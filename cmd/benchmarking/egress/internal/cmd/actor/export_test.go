// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actor

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
)

// fakeCollector is an OTLP/gRPC metrics receiver on a loopback listener
// that keeps every export it was sent.
type fakeCollector struct {
	collectormetrics.UnimplementedMetricsServiceServer
	addr    string
	mu      sync.Mutex
	exports []*collectormetrics.ExportMetricsServiceRequest
}

func (f *fakeCollector) Export(_ context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exports = append(f.exports, req)
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

// received returns the exports so far.
func (f *fakeCollector) received() []*collectormetrics.ExportMetricsServiceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.exports)
}

func startFakeCollector(t *testing.T) *fakeCollector {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCollector{addr: lis.Addr().String()}
	srv := grpc.NewServer()
	collectormetrics.RegisterMetricsServiceServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return f
}

// exportEnv points the SDK at f with a short interval, as the template's
// environment points the actor at the collector. The environment is process
// wide, so a test that calls it cannot be parallel.
func exportEnv(t *testing.T, f *fakeCollector) {
	t.Helper()
	t.Setenv(endpointEnv, "http://"+f.addr)
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100")
	t.Setenv("OTEL_METRIC_EXPORT_TIMEOUT", "2000")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name="+serviceName)
}

// attr returns the string attribute key of kvs, "" when absent.
func attr(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value.GetStringValue()
		}
	}
	return ""
}

// metricsOf flattens one export into its metrics by name, checking the
// resource and the scope on the way.
func metricsOf(t *testing.T, req *collectormetrics.ExportMetricsServiceRequest) map[string]*metricspb.Metric {
	t.Helper()
	out := map[string]*metricspb.Metric{}
	for _, rm := range req.ResourceMetrics {
		if name := attr(rm.Resource.Attributes, "service.name"); name != serviceName {
			t.Errorf("resource service.name = %q, want %q", name, serviceName)
		}
		if attr(rm.Resource.Attributes, "service.instance.id") == "" {
			t.Errorf("resource has no service.instance.id: %v", rm.Resource.Attributes)
		}
		for _, sm := range rm.ScopeMetrics {
			if sm.Scope.Name != meterName {
				t.Errorf("scope %q, want %q", sm.Scope.Name, meterName)
			}
			for _, m := range sm.Metrics {
				out[m.Name] = m
			}
		}
	}
	return out
}

// instanceOf is the service.instance.id of one export.
func instanceOf(req *collectormetrics.ExportMetricsServiceRequest) string {
	if len(req.ResourceMetrics) == 0 {
		return ""
	}
	return attr(req.ResourceMetrics[0].Resource.Attributes, "service.instance.id")
}

// sumOf adds the integer points of a counter.
func sumOf(m *metricspb.Metric) int64 {
	var n int64
	for _, p := range m.GetSum().GetDataPoints() {
		n += p.GetAsInt()
	}
	return n
}

// localhostTLSTarget is an HTTPS target with a certificate for localhost, so a
// loop that names it does a lookup and a handshake on every open.
func localhostTLSTarget(t *testing.T, h http.HandlerFunc) (host string, cert *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}}}
	srv.Config.ErrorLog = quietLog
	srv.StartTLS()
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return "localhost:" + u.Port(), cert
}

// TestExportReachesTheCollector runs a loop against a fake OTLP receiver, as
// the actor runs against the benchmark's collector, and pins what arrives:
// the resource, the eleven names, the three attributes on every point,
// cumulative temporality, and a final export after /stop that agrees with
// the counters /stop answered.
func TestExportReachesTheCollector(t *testing.T) {
	f := startFakeCollector(t)
	exportEnv(t, f)
	okHost, okCert := localhostTLSTarget(t, okHandler)
	failHost, failCert := localhostTLSTarget(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "denied", http.StatusServiceUnavailable) })
	roots := x509.NewCertPool()
	roots.AddCert(okCert)
	roots.AddCert(failCert)
	h := serveTLS(roots, okHost, failHost)

	req := contract.StartRequest{Endpoints: 2, Scheme: contract.SchemeHTTPS, IntervalMs: 2}
	final := runLoop(t, h, req, func(s *contract.Stats) bool { return s.Rounds >= 3 && s.Errors["HTTP 503"] > 0 })
	exports := f.received()
	if len(exports) == 0 {
		t.Fatal("the collector received nothing")
	}
	last := metricsOf(t, exports[len(exports)-1])
	for _, name := range metricNames {
		if _, ok := last[name]; !ok {
			t.Errorf("the final export lacks %s; it has %v", name, slices.Sorted(maps.Keys(last)))
		}
	}
	for name, m := range last {
		var points []*commonpb.KeyValue
		var temporality metricspb.AggregationTemporality
		switch d := m.Data.(type) {
		case *metricspb.Metric_Sum:
			temporality = d.Sum.AggregationTemporality
			for _, p := range d.Sum.DataPoints {
				points = append(points, p.Attributes...)
			}
		case *metricspb.Metric_Histogram:
			temporality = d.Histogram.AggregationTemporality
			for _, p := range d.Histogram.DataPoints {
				points = append(points, p.Attributes...)
				if len(p.ExplicitBounds) != len(contract.Bounds) || len(p.BucketCounts) != len(contract.Bounds)+1 {
					t.Errorf("%s point has %d bounds and %d buckets, want %d and %d", name, len(p.ExplicitBounds), len(p.BucketCounts), len(contract.Bounds), len(contract.Bounds)+1)
				}
			}
		case *metricspb.Metric_Gauge:
			temporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
			for _, p := range d.Gauge.DataPoints {
				points = append(points, p.Attributes...)
			}
		default:
			t.Errorf("%s arrived as %T", name, m.Data)
		}
		if temporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
			t.Errorf("%s arrived with temporality %v, want cumulative", name, temporality)
		}
		if a, c, r := attr(points, "actor"), attr(points, "cell"), attr(points, "run"); a != identity.Actor || c != identity.Cell || r != identity.Run {
			t.Errorf("%s points carry actor %q cell %q run %q, want %q %q %q", name, a, c, r, identity.Actor, identity.Cell, identity.Run)
		}
	}
	if got := sumOf(last[requestsName]); got != final.Requests {
		t.Errorf("final export: requests %d, /stop answered %d", got, final.Requests)
	}
	if got := sumOf(last[requestErrorsName]); got != final.Requests-final.Successes {
		t.Errorf("final export: request_errors %d, /stop answered %d failures", got, final.Requests-final.Successes)
	}
	if got := sumOf(last[endpointsOpenedName]); got != final.Opened {
		t.Errorf("final export: endpoints_opened %d, /stop answered %d", got, final.Opened)
	}

	// A second loop in the same process is a new instance from zero.
	first := instanceOf(exports[len(exports)-1])
	before := len(exports)
	runLoop(t, h, req, atLeast(1))
	exports = f.received()
	if len(exports) <= before {
		t.Fatal("the second loop exported nothing")
	}
	if second := instanceOf(exports[len(exports)-1]); second == first || second == "" {
		t.Errorf("second loop exported as instance %q, first as %q; want two ids", second, first)
	}
}

// TestNoEndpointMeansNoExport pins the actor without the template's
// environment: /start works, the loop runs, and nothing is pushed anywhere.
func TestNoEndpointMeansNoExport(t *testing.T) {
	f := startFakeCollector(t)
	exportEnv(t, f)
	t.Setenv(endpointEnv, "")
	h := serveTargets(t, 2)
	final := runLoop(t, h, contract.StartRequest{Endpoints: 2}, atLeast(5))
	if final.Requests < 5 || final.Successes != final.Requests {
		t.Errorf("loop without an endpoint: %d requests, %d successes", final.Requests, final.Successes)
	}
	if got := f.received(); len(got) != 0 {
		t.Errorf("the collector received %d exports with the endpoint env unset", len(got))
	}
}
