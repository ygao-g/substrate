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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/targetcert"
)

// target is an endpoint that counts the TCP connections opened to it.
type target struct {
	*httptest.Server
	conns    atomic.Int64
	requests atomic.Int64
}

func newTarget(t *testing.T, h http.HandlerFunc) *target {
	t.Helper()
	return newTargetConnState(t, h, nil)
}

// newTargetConnState is newTarget with onState called on every connection
// state change, after the connection count.
func newTargetConnState(t *testing.T, h http.HandlerFunc, onState func(net.Conn, http.ConnState)) *target {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
		if onState != nil {
			onState(c, s)
		}
	}
	tg.Start()
	t.Cleanup(tg.Close)
	return tg
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }

// quietLog discards a test server's own error log.
var quietLog = log.New(io.Discard, "", 0)

// host is the target's host:port.
func (tg *target) host() string { return tg.Listener.Addr().String() }

// fixture is the actor's handler with a reader on every loop it starts, so
// a test reads the instruments the actor exports beside the counters it
// answers.
type fixture struct {
	http.Handler
	mu      sync.Mutex
	readers []*finalReader // one per /start, the current loop's last
}

func newFixture(s *server) *fixture {
	f := &fixture{}
	s.readers = func() []sdkmetric.Reader {
		r := &finalReader{ManualReader: sdkmetric.NewManualReader()}
		f.mu.Lock()
		f.readers = append(f.readers, r)
		f.mu.Unlock()
		return []sdkmetric.Reader{r}
	}
	f.Handler = s.handler()
	return f
}

// metrics is the current loop's instruments: collected now while it runs,
// as of its Shutdown once it is stopped.
func (f *fixture) metrics(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	f.mu.Lock()
	r := f.readers[len(f.readers)-1]
	f.mu.Unlock()
	rm, err := r.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return rm
}

// finalReader is a manual reader that keeps the collection of its Shutdown,
// which the loop calls once its goroutines have exited, so a test reads the
// instruments as they stood when the final Stats were answered.
type finalReader struct {
	*sdkmetric.ManualReader
	mu    sync.Mutex
	final *metricdata.ResourceMetrics
}

func (r *finalReader) Shutdown(ctx context.Context) error {
	var rm metricdata.ResourceMetrics
	err := r.ManualReader.Collect(ctx, &rm)
	r.mu.Lock()
	r.final = &rm
	r.mu.Unlock()
	return errors.Join(err, r.ManualReader.Shutdown(ctx))
}

func (r *finalReader) read(ctx context.Context) (metricdata.ResourceMetrics, error) {
	r.mu.Lock()
	final := r.final
	r.mu.Unlock()
	if final != nil {
		return *final, nil
	}
	var rm metricdata.ResourceMetrics
	err := r.ManualReader.Collect(ctx, &rm)
	return rm, err
}

// serve returns the actor with endpoint i mapped to hosts[i], a host:port,
// in place of the in-cluster Service names. The URL keeps the scheme the
// loop asks for.
func serve(hosts ...string) *fixture {
	return serveTLS(nil, hosts...)
}

// serveTLS is serve, verifying HTTPS endpoints against roots.
func serveTLS(roots *x509.CertPool, hosts ...string) *fixture {
	s := newServer()
	s.rootCAs = roots
	s.endpointURL = func(i int, scheme string) string { return scheme + "://" + hosts[i] + "/" }
	return newFixture(s)
}

// serveTargets is serve over n fresh targets, one per endpoint. Endpoints
// that share one host would share the transport's single idle connection per
// host and redial on every request.
func serveTargets(t *testing.T, n int) *fixture {
	t.Helper()
	hosts := make([]string, n)
	for i := range hosts {
		hosts[i] = newTarget(t, okHandler).host()
	}
	return serve(hosts...)
}

// newTLSTarget is newTarget serving HTTPS with httptest's certificate, which
// the returned pool trusts.
func newTLSTarget(t *testing.T, h http.HandlerFunc) (*target, *x509.CertPool) {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.Config.ErrorLog = quietLog // failed handshakes are what some tests expect
	tg.StartTLS()
	t.Cleanup(tg.Close)
	roots := x509.NewCertPool()
	roots.AddCert(tg.Certificate())
	return tg, roots
}

func post(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, &buf))
	return rec
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// decodeStats reads a 200 answer's Stats.
func decodeStats(t *testing.T, what string, rec *httptest.ResponseRecorder) *contract.Stats {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("%s = %d %s", what, rec.Code, rec.Body)
	}
	var s contract.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("%s: decoding %s: %v", what, rec.Body, err)
	}
	return &s
}

// The poll and the stop of a running loop, each with the per-endpoint counts.
const (
	statsPath = contract.StatsRoute + "?endpoints=1"
	stopPath  = contract.StopRoute + "?endpoints=1"
)

// start posts /start with req, identified.
func start(t *testing.T, h http.Handler, req contract.StartRequest) *httptest.ResponseRecorder {
	t.Helper()
	return post(t, h, contract.StartRoute, identified(req))
}

// readStats polls the counters.
func readStats(t *testing.T, h http.Handler) *contract.Stats {
	t.Helper()
	return decodeStats(t, "GET /stats", get(h, statsPath))
}

// stopLoop stops the loop and returns its final Stats.
func stopLoop(t *testing.T, h http.Handler) *contract.Stats {
	t.Helper()
	return decodeStats(t, "POST /stop", post(t, h, stopPath, nil))
}

// runLoop starts a loop with req, lets it run until cond holds on the live
// counters (or fails after 10s), then stops it and returns the final Stats.
func runLoop(t *testing.T, h http.Handler, req contract.StartRequest, cond func(*contract.Stats) bool) *contract.Stats {
	t.Helper()
	if rec := start(t, h, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	waitFor(t, h, cond)
	return stopLoop(t, h)
}

// waitFor polls the counters until cond holds, or fails after 10s.
func waitFor(t *testing.T, h http.Handler, cond func(*contract.Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := readStats(t, h)
		if cond(s) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached; last stats %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func atLeast(n int64) func(*contract.Stats) bool {
	return func(s *contract.Stats) bool { return s.Requests >= n }
}

// histogram is the named distribution's data point in the current loop's
// instruments; ok is false when the loop recorded nothing in it.
func histogram(t *testing.T, f *fixture, name contract.Distribution) (p metricdata.HistogramDataPoint[float64], ok bool) {
	t.Helper()
	for _, sm := range f.metrics(t).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			h, isHist := m.Data.(metricdata.Histogram[float64])
			if !isHist || len(h.DataPoints) != 1 {
				t.Fatalf("%s is %+v, want a histogram with one point", name, m.Data)
			}
			return h.DataPoints[0], true
		}
	}
	return p, false
}

// samples is the count of a distribution in the current loop, 0 when absent.
func samples(t *testing.T, f *fixture, name contract.Distribution) uint64 {
	t.Helper()
	p, _ := histogram(t, f, name)
	return p.Count
}

func TestKeepAliveReusesOneConnectionPerEndpoint(t *testing.T) {
	t.Parallel()
	targets := []*target{newTarget(t, okHandler), newTarget(t, okHandler), newTarget(t, okHandler)}
	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.host())
	}
	h := serve(hosts...)

	s := runLoop(t, h, contract.StartRequest{Endpoints: len(hosts)}, func(s *contract.Stats) bool { return s.Rounds >= 10 })
	if s.Scheme != contract.SchemeHTTP || samples(t, h, contract.TLS) != 0 || samples(t, h, contract.DNS) != 0 {
		t.Errorf("a start with no scheme against IP hosts ran over %q with %d TLS handshakes and %d DNS lookups, want http with neither", s.Scheme, samples(t, h, contract.TLS), samples(t, h, contract.DNS))
	}

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v", s.Successes, s.Requests, s.Errors)
	}
	for i, tg := range targets {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("target %d saw %d connections, want 1 kept alive", i, got)
		}
	}
	if s.NewConns != int64(len(targets)) {
		t.Errorf("NewConns = %d, want %d (one per endpoint)", s.NewConns, len(targets))
	}
	if opening, latency := samples(t, h, contract.Opening), samples(t, h, contract.Latency); opening != uint64(len(targets)) || int64(opening+latency) != s.Successes {
		t.Errorf("opening %d and latency %d samples for %d successes; want one opening per endpoint and the rest latency", opening, latency, s.Successes)
	}
	for i, e := range s.Endpoints {
		if e.Requests < 10 {
			t.Errorf("endpoint %d got %d requests, want at least the %d rounds the loop was stopped at", i, e.Requests, 10)
		}
	}
}

func TestStopDuringOpeningDoesNotCountTheCancelledOpen(t *testing.T) {
	t.Parallel()
	first := newTarget(t, okHandler)
	blocked := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // holds the second open until the client gives up
	})
	h := serve(first.host(), blocked.host())

	s := runLoop(t, h, contract.StartRequest{Endpoints: 2}, func(s *contract.Stats) bool { return s.Opened >= 1 })
	if s.Opened != 1 {
		t.Errorf("Opened = %d after stopping during the second open, want 1", s.Opened)
	}
	if s.Rounds != 0 || s.OpeningDone != 0 || s.Endpoints[1].Requests != 0 {
		t.Errorf("Rounds = %d, OpeningDone = %v, endpoint 1 requests = %d during the opening, want 0, 0 and 0", s.Rounds, s.OpeningDone, s.Endpoints[1].Requests)
	}
}

func TestRequestTimeoutIsClassified(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	slow := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	h := serve(slow.host())

	s := runLoop(t, h, contract.StartRequest{Endpoints: 1, RequestTimeoutMs: 20}, atLeast(2))

	if s.Errors["timeout"] != s.Requests || s.Successes != 0 {
		t.Errorf("errors %v, successes %d for %d requests; want every request a timeout", s.Errors, s.Successes, s.Requests)
	}
	if n := samples(t, h, contract.Latency); n != 0 {
		t.Errorf("failed requests recorded %d latency samples, want 0", n)
	}
	if s.Endpoints[0].Errors != s.Requests {
		t.Errorf("endpoint errors = %d, want %d", s.Endpoints[0].Errors, s.Requests)
	}
}

func TestNon200IsAnError(t *testing.T) {
	t.Parallel()
	ok := newTarget(t, okHandler)
	failing := newTarget(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "egress denied", http.StatusServiceUnavailable)
	})
	h := serve(ok.host(), failing.host())

	s := runLoop(t, h, contract.StartRequest{Endpoints: 2}, atLeast(10))

	if s.Errors["HTTP 503"] == 0 {
		t.Errorf("errors = %v, want HTTP 503 counted", s.Errors)
	}
	if s.Endpoints[0].Errors != 0 || s.Endpoints[1].Errors != s.Endpoints[1].Requests {
		t.Errorf("endpoints = %+v, want only the second one failing", s.Endpoints)
	}
}

func TestStartStopLifecycle(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())
	req := contract.StartRequest{Endpoints: 1}

	if rec := get(h, contract.StatsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /stats before start = %d, want 404", rec.Code)
	}
	if rec := post(t, h, contract.StopRoute, nil); rec.Code != http.StatusNotFound {
		t.Errorf("POST /stop before start = %d, want 404", rec.Code)
	}

	first := runLoop(t, h, req, atLeast(5))
	time.Sleep(20 * time.Millisecond)
	// A request the stop cut short can still reach the target after /stop
	// returns; nothing else may.
	if extra := tg.requests.Load() - first.Requests; extra > 1 {
		t.Errorf("target got %d requests the stopped loop did not count, want at most the one cut short by the stop", extra)
	}

	if rec := start(t, h, req); rec.Code != http.StatusNoContent {
		t.Fatalf("restart = %d %s", rec.Code, rec.Body)
	}
	if rec := start(t, h, req); rec.Code != http.StatusConflict {
		t.Errorf("second /start while running = %d, want 409", rec.Code)
	}
	second := stopLoop(t, h)
	// The target saw every request of both runs (plus any cut short by a
	// stop), so a second run that started from zero counts no more than the
	// target saw after the first run.
	if sinceRestart := tg.requests.Load() - first.Requests; second.Requests > sinceRestart {
		t.Errorf("restarted loop counted %d requests, more than the %d sent since the restart: the old counts were kept", second.Requests, sinceRestart)
	}
}

func TestStartRejectsBadRequests(t *testing.T) {
	t.Parallel()
	h := newServer().handler()
	const named = `"actor":"benchmark-workloads/egress-0","cell":"cell-1","run":"run-1"`
	for _, tc := range []struct{ name, body string }{
		{"not json", `not json`},
		{"empty object", `{}`},
		{"invalid", `{"endpoints":1,"scheme":"ftp",` + named + `}`},
		{"no identity", `{"endpoints":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, contract.StartRoute, strings.NewReader(tc.body)))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("POST /start %s = %d, want 400", tc.body, rec.Code)
			}
		})
	}
	if rec := get(h, contract.ReadyzRoute); rec.Code != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", rec.Code)
	}
}

func TestDefaultURLsAreTheEndpointServices(t *testing.T) {
	t.Parallel()
	s := newServer()
	for _, i := range []int{0, 9, 255} {
		for _, scheme := range []string{contract.SchemeHTTP, contract.SchemeHTTPS} {
			if got, want := s.endpointURL(i, scheme), contract.EndpointURL(i, scheme); got != want {
				t.Errorf("endpoint %d over %s = %q, want %q", i, scheme, got, want)
			}
		}
	}
	if s.rootCAs != nil {
		t.Error("the actor's roots are set by default; they must stay nil so SSL_CERT_FILE applies")
	}
	if s.dial != nil {
		t.Error("the actor dials through a hook by default; it must stay nil so a plain dialer is used")
	}
}

func TestHTTPSFailuresAreClassified(t *testing.T) {
	t.Parallel()
	bundle, err := targetcert.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpointCert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	endpointRoots := x509.NewCertPool()
	endpointRoots.AppendCertsFromPEM(bundle.CA)
	// Serves the endpoints' certificate, which names no IP address.
	wrongName := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	wrongName.TLS = &tls.Config{Certificates: []tls.Certificate{endpointCert}}
	wrongName.Config.ErrorLog = quietLog
	wrongName.StartTLS()
	t.Cleanup(wrongName.Close)

	untrusted, _ := newTLSTarget(t, okHandler)
	plain := newTarget(t, okHandler)

	// Accepts, reads the ClientHello so closing sends a FIN rather than a
	// reset, and closes: a peer that refuses the handshake, as the gateway
	// does for an SNI no rule allows.
	closer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closer.Close() })
	go func() {
		for {
			conn, err := closer.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Read(make([]byte, 4096))
			conn.Close()
		}
	}()

	for _, tc := range []struct {
		name  string
		roots *x509.CertPool
		host  string
		want  string
	}{
		{"untrusted CA", x509.NewCertPool(), untrusted.host(), "tls: unknown authority"},
		{"hostname mismatch", endpointRoots, wrongName.host(), "tls: hostname mismatch"},
		{"plain HTTP server", x509.NewCertPool(), plain.host(), "tls: not a TLS server"},
		{"peer closes during handshake", x509.NewCertPool(), closer.Addr().String(), "tls handshake: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := serveTLS(tc.roots, tc.host)
			s := runLoop(t, h, contract.StartRequest{Endpoints: 1, Scheme: contract.SchemeHTTPS}, atLeast(2))
			var matched int64
			for class, n := range s.Errors {
				if strings.HasPrefix(class, tc.want) {
					matched += n
				}
			}
			if matched != s.Requests || s.Successes != 0 || samples(t, h, contract.TLS) != 0 {
				t.Errorf("errors %v, %d successes, %d handshakes for %d requests; want every request classed %q", s.Errors, s.Successes, samples(t, h, contract.TLS), s.Requests, tc.want)
			}
		})
	}
}

// dialTo returns a dial function that connects host:port to addrs[host]
// instead, so a loop can request the in-cluster endpoint names against local
// listeners. TLS still verifies the name the loop asked for.
func dialTo(addrs map[string]string) dialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		real, ok := addrs[host]
		if !ok {
			return nil, fmt.Errorf("no local listener for %s", addr)
		}
		return (&net.Dialer{}).DialContext(ctx, network, real)
	}
}

// targetCertServer is an HTTPS target serving the endpoints' certificate.
func targetCertServer(t *testing.T, bundle *targetcert.Bundle) *target {
	t.Helper()
	cert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	tg.StartTLS()
	t.Cleanup(tg.Close)
	return tg
}

// TestEndpointsByNameOverBothSchemes runs the loop as the actor does in the
// cluster: the default endpoint URLs, eight endpoints, and over HTTPS the
// target's own certificate verified under each endpoint's name.
func TestEndpointsByNameOverBothSchemes(t *testing.T) {
	t.Parallel()
	const c = 8
	bundle, err := targetcert.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle.CA) {
		t.Fatal("no CA in the bundle")
	}
	for _, scheme := range []string{contract.SchemeHTTP, contract.SchemeHTTPS} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()
			addrs := make(map[string]string, c)
			targets := make([]*target, c)
			for i := range c {
				if scheme == contract.SchemeHTTPS {
					targets[i] = targetCertServer(t, bundle)
				} else {
					targets[i] = newTarget(t, okHandler)
				}
				addrs[contract.EndpointHost(i)] = targets[i].host()
			}
			s := newServer()
			s.rootCAs = roots
			s.dial = dialTo(addrs)
			h := newFixture(s)

			const interval = 2 * time.Millisecond
			st := runLoop(t, h, contract.StartRequest{Endpoints: c, Scheme: scheme, IntervalMs: int64(interval / time.Millisecond)},
				func(s *contract.Stats) bool { return s.Rounds >= 3 })

			if st.Scheme != scheme || st.Opened != c || st.Rounds < 3 {
				t.Errorf("Scheme = %q, Opened = %d, Rounds = %d, want %s, %d and at least 3", st.Scheme, st.Opened, st.Rounds, scheme, c)
			}
			if st.Successes != st.Requests || len(st.Errors) != 0 {
				t.Errorf("successes %d of %d requests, errors %v", st.Successes, st.Requests, st.Errors)
			}
			if st.NewConns != c {
				t.Errorf("NewConns = %d, want one per endpoint (%d)", st.NewConns, c)
			}
			wantTLS := uint64(0)
			if scheme == contract.SchemeHTTPS {
				wantTLS = c
			}
			if n := samples(t, h, contract.TLS); n != wantTLS {
				t.Errorf("TLS handshakes = %d, want %d", n, wantTLS)
			}
			for i, tg := range targets {
				if got := tg.conns.Load(); got != 1 {
					t.Errorf("endpoint %d saw %d connections, want 1 kept alive", i, got)
				}
			}
			// Every endpoint is requested once per period of C intervals; a gap
			// is about one period, never anywhere near Elapsed.
			if period := c * interval; st.LongestGap < period/2 || st.LongestGap > 5*time.Second {
				t.Errorf("LongestGap = %v, want about one period (%v) and well under Elapsed", st.LongestGap, period)
			}
		})
	}
}

// TestResetConnectionIsRedialedWithoutAnError pins how a tunnel the gateway
// reset shows up: the loop redials, net/http retries the GET, and NewConns
// grows by one while Errors stays empty.
func TestResetConnectionIsRedialedWithoutAnError(t *testing.T) {
	t.Parallel()
	var cutOnce atomic.Bool
	cut := newTargetConnState(t, okHandler, func(c net.Conn, s http.ConnState) {
		// Closes the server side once the first response is out, as the
		// gateway does to an idle tunnel.
		if s == http.StateIdle && cutOnce.CompareAndSwap(false, true) {
			c.Close()
		}
	})
	steady := []*target{newTarget(t, okHandler), newTarget(t, okHandler)}
	h := serve(steady[0].host(), cut.host(), steady[1].host())

	s := runLoop(t, h, contract.StartRequest{Endpoints: 3, IntervalMs: 2}, func(s *contract.Stats) bool { return s.Rounds >= 2 })

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v; want the reset absorbed", s.Successes, s.Requests, s.Errors)
	}
	if s.NewConns != 4 {
		t.Errorf("NewConns = %d, want 4: one per endpoint plus the redial", s.NewConns)
	}
	if got := cut.conns.Load(); got != 2 {
		t.Errorf("the cut endpoint saw %d connections, want 2", got)
	}
	for i, tg := range steady {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("steady endpoint %d saw %d connections, want 1", i, got)
		}
	}
}

// pipeNet serves a handler over in-memory connections, so a loop can run
// inside a synctest bubble, where a real socket would not count as blocked.
type pipeNet struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
	srv   *http.Server
}

func newPipeNet(h http.Handler) *pipeNet {
	p := &pipeNet{conns: make(chan net.Conn), done: make(chan struct{}), srv: &http.Server{Handler: h, ErrorLog: quietLog}}
	go p.srv.Serve(p)
	return p
}

func (p *pipeNet) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case p.conns <- server:
		return client, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeNet) Accept() (net.Conn, error) {
	select {
	case c := <-p.conns:
		return c, nil
	case <-p.done:
		return nil, net.ErrClosed
	}
}

func (p *pipeNet) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

func (p *pipeNet) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// startPipeLoop serves the actor over pn, with endpoint i named endpoint-i,
// and starts a loop with req. It returns the handler and a counters reader
// that waits for the bubble to settle first.
func startPipeLoop(t *testing.T, pn *pipeNet, req contract.StartRequest) (*fixture, func() *contract.Stats) {
	t.Helper()
	s := newServer()
	s.dial = pn.dial
	s.endpointURL = func(i int, scheme string) string { return fmt.Sprintf("%s://endpoint-%d/", scheme, i) }
	h := newFixture(s)
	if rec := start(t, h, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	return h, func() *contract.Stats {
		synctest.Wait()
		return readStats(t, h)
	}
}

// TestPacingAndGapsAreExact runs the loop on a fake clock with instant
// endpoints, so the schedule can be checked to the millisecond: endpoint i
// opens i intervals in, each endpoint is then requested once per period of C
// intervals, and so every gap is one period. At each step the instruments
// hold one opening per opened endpoint and the rest latency.
func TestPacingAndGapsAreExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(okHandler))
		defer pn.srv.Close()
		const c, interval = 4, 100 * time.Millisecond
		const period = c * interval
		start := time.Now()
		h, stats := startPipeLoop(t, pn, contract.StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})
		for _, tc := range []struct {
			at                       time.Duration
			requests, opened, rounds int64
			openingDone, longestGap  time.Duration
			why                      string
		}{
			{interval / 2, 1, 1, 0, 0, 0, "endpoint 0 opens at once, the others wait their turn"},
			{period - interval/2, c, c, 1, (c - 1) * interval, 0, "one open per interval; the last completes at (C-1) intervals"},
			{2*period - interval/2, 2 * c, c, 2, (c - 1) * interval, period, "each endpoint's second request comes one period after its first"},
			{3*period - interval/2, 3 * c, c, 3, (c - 1) * interval, period, "steady state: every gap is one period"},
		} {
			time.Sleep(tc.at - time.Since(start))
			st := stats()
			if st.Requests != tc.requests || st.Opened != tc.opened || st.Rounds != tc.rounds || st.OpeningDone != tc.openingDone || st.LongestGap != tc.longestGap {
				t.Errorf("at %v: requests %d, opened %d, rounds %d, opening done %v, longest gap %v; want %d, %d, %d, %v, %v (%s)",
					tc.at, st.Requests, st.Opened, st.Rounds, st.OpeningDone, st.LongestGap, tc.requests, tc.opened, tc.rounds, tc.openingDone, tc.longestGap, tc.why)
			}
			opening, latency := samples(t, h, contract.Opening), samples(t, h, contract.Latency)
			if int64(opening) != tc.opened || int64(opening+latency) != st.Successes {
				t.Errorf("at %v: %d opening and %d latency samples for %d successes; want one opening per opened endpoint (%d) and the rest latency",
					tc.at, opening, latency, st.Successes, tc.opened)
			}
		}
		synctest.Wait()
		for i, e := range readStats(t, h).Endpoints {
			if e.LongestGap != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v)", i, e.LongestGap, period)
			}
		}
		stopLoop(t, h)
	})
}

// TestOffsetShiftsEveryEndpoint pins that offsetMs delays the whole schedule
// and nothing else: endpoint 0 opens at the offset, the last endpoint at the
// offset plus C-1 intervals, and every gap is still one period.
func TestOffsetShiftsEveryEndpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(okHandler))
		defer pn.srv.Close()
		const c, interval, offset = 4, 100 * time.Millisecond, 50 * time.Millisecond
		const period = c * interval
		start := time.Now()
		h, stats := startPipeLoop(t, pn, contract.StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond), OffsetMs: int64(offset / time.Millisecond)})
		for _, tc := range []struct {
			at                       time.Duration
			requests, opened, rounds int64
			openingDone, longestGap  time.Duration
			why                      string
		}{
			{offset / 2, 0, 0, 0, 0, 0, "nothing opens before the offset"},
			{offset + interval/2, 1, 1, 0, 0, 0, "endpoint 0 opens at the offset"},
			{offset + period - interval/2, c, c, 1, offset + (c-1)*interval, 0, "the last open completes at the offset plus C-1 intervals"},
			{offset + 2*period - interval/2, 2 * c, c, 2, offset + (c-1)*interval, period, "each repeat comes one period after the open it follows"},
		} {
			time.Sleep(tc.at - time.Since(start))
			st := stats()
			if st.Requests != tc.requests || st.Opened != tc.opened || st.Rounds != tc.rounds || st.OpeningDone != tc.openingDone || st.LongestGap != tc.longestGap {
				t.Errorf("at %v: requests %d, opened %d, rounds %d, opening done %v, longest gap %v; want %d, %d, %d, %v, %v (%s)",
					tc.at, st.Requests, st.Opened, st.Rounds, st.OpeningDone, st.LongestGap, tc.requests, tc.opened, tc.rounds, tc.openingDone, tc.longestGap, tc.why)
			}
		}
		for i, e := range stats().Endpoints {
			if e.LongestGap != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v)", i, e.LongestGap, period)
			}
		}
		stopLoop(t, h)
	})
}

// TestSlowEndpointDelaysOnlyItself pins that a request hanging until its
// timeout on one endpoint leaves the other endpoints' gaps at one period, and
// that the hung endpoint's own gap is the timeout: its next request starts as
// soon as the slow one returns.
func TestSlowEndpointDelaysOnlyItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "endpoint-0" {
				<-r.Context().Done()
				return
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		const c, interval, timeout = 3, 50 * time.Millisecond, 300 * time.Millisecond
		const period = c * interval
		h, stats := startPipeLoop(t, pn, contract.StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond), RequestTimeoutMs: int64(timeout / time.Millisecond)})

		time.Sleep(time.Second + interval/2)
		st := stats()
		for i := 1; i < c; i++ {
			if got := st.Endpoints[i].LongestGap; got != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v) with endpoint 0 hung", i, got, period)
			}
			if st.Endpoints[i].Errors != 0 {
				t.Errorf("endpoint %d: %d errors, want 0", i, st.Endpoints[i].Errors)
			}
		}
		if st.Endpoints[0].LongestGap != timeout || st.LongestGap != timeout {
			t.Errorf("endpoint 0 longest gap %v, overall %v; want the timeout (%v) for both", st.Endpoints[0].LongestGap, st.LongestGap, timeout)
		}
		if st.Errors["timeout"] == 0 || st.Errors["timeout"] != st.Endpoints[0].Errors {
			t.Errorf("errors %v, endpoint 0 errors %d; want every timeout on endpoint 0", st.Errors, st.Endpoints[0].Errors)
		}
		if st.Opened != c || st.OpeningDone != timeout {
			t.Errorf("Opened = %d, OpeningDone = %v; want %d and %v: the hung open counts once it times out, and it completes last", st.Opened, st.OpeningDone, c, timeout)
		}
		stopLoop(t, h)
	})
}

// TestSlowOpensDoNotDelayOpenedEndpoints pins the schedule when opens are
// slow, as they are while a gateway works through a fleet's handshakes: an
// endpoint already opened is requested one period after its open while the
// others are still opening, and a slow open stretches only its own
// endpoint's first gap, to the open's latency.
func TestSlowOpensDoNotDelayOpenedEndpoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const c, interval, openLatency = 4, 100 * time.Millisecond, 500 * time.Millisecond
		const period = c * interval
		var mu sync.Mutex
		opened := map[string]bool{}
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			first := !opened[r.Host]
			opened[r.Host] = true
			mu.Unlock()
			if first && r.Host != "endpoint-0" {
				select {
				case <-time.After(openLatency):
				case <-r.Context().Done():
					return
				}
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		start := time.Now()
		h, stats := startPipeLoop(t, pn, contract.StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})

		// endpoint 0 opened at once; its second request is due at one period,
		// while endpoints 1 to 3 are still inside their first.
		time.Sleep(period + interval/2)
		st := stats()
		if st.Opened != 1 || st.Endpoints[0].Requests != 2 || st.Endpoints[0].LongestGap != period || st.Requests != 2 {
			t.Errorf("at %v: opened %d, endpoint 0 %d requests with gap %v, %d requests in all; want 1, 2, %v, 2: the opened endpoint keeps its period while the others open",
				time.Since(start), st.Opened, st.Endpoints[0].Requests, st.Endpoints[0].LongestGap, st.Requests, period)
		}

		// The last open, endpoint 3's, started at 3 intervals and took
		// openLatency; after that each slow endpoint's next request came
		// at once, since its period had already passed.
		time.Sleep(3*period + interval/2 - time.Since(start))
		st = stats()
		if st.Opened != c || st.OpeningDone != (c-1)*interval+openLatency {
			t.Errorf("Opened = %d, OpeningDone = %v; want %d and %v", st.Opened, st.OpeningDone, c, (c-1)*interval+openLatency)
		}
		if st.Endpoints[0].LongestGap != period || st.Endpoints[0].Requests != 4 {
			t.Errorf("endpoint 0: longest gap %v, %d requests; want %v and 4: the slow opens of the others did not touch it", st.Endpoints[0].LongestGap, st.Endpoints[0].Requests, period)
		}
		for i := 1; i < c; i++ {
			if got := st.Endpoints[i].LongestGap; got != openLatency {
				t.Errorf("endpoint %d: longest gap %v, want its own open latency (%v)", i, got, openLatency)
			}
		}
		stopLoop(t, h)
		op, _ := histogram(t, h, contract.Opening)
		if opMax, _ := op.Max.Value(); st.LongestGap != openLatency || op.Count != c || opMax != openLatency.Seconds() {
			t.Errorf("LongestGap = %v, opening samples %d with max %v; want %v, %d and %v", st.LongestGap, op.Count, opMax, openLatency, c, openLatency.Seconds())
		}
	})
}

// TestSlowRequestResumesOnTheNextTick pins what a request longer than the
// period does to its own endpoint: the next request starts as soon as the
// slow one returns, so the gap is the request's latency, not latency plus a
// period.
func TestSlowRequestResumesOnTheNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const period, latency = 50 * time.Millisecond, 120 * time.Millisecond
		var mu sync.Mutex
		var starts []time.Duration
		start := time.Now()
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			starts = append(starts, time.Since(start))
			mu.Unlock()
			select {
			case <-time.After(latency):
				okHandler(w, r)
			case <-r.Context().Done():
			}
		}))
		defer pn.srv.Close()
		h, stats := startPipeLoop(t, pn, contract.StartRequest{Endpoints: 1, IntervalMs: int64(period / time.Millisecond)})

		time.Sleep(4*latency + period/2)
		st := stats()
		mu.Lock()
		got := slices.Clone(starts)
		mu.Unlock()
		want := []time.Duration{0, latency, 2 * latency, 3 * latency, 4 * latency}
		if !slices.Equal(got, want) {
			t.Errorf("request starts = %v, want %v: back to back once each outlasts the period", got, want)
		}
		if st.LongestGap != latency {
			t.Errorf("LongestGap = %v, want the request latency (%v)", st.LongestGap, latency)
		}
		stopLoop(t, h)
	})
}

// TestStopWithRequestsInFlightReturnsAtOnce pins that a stop neither waits
// for the in-flight requests' timeouts nor leaks their goroutines.
func TestStopWithRequestsInFlightReturnsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer pn.srv.Close()
		const c = 8
		h, _ := startPipeLoop(t, pn, contract.StartRequest{Endpoints: c, RequestTimeoutMs: 5000})
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()

		before := time.Now()
		st := stopLoop(t, h)
		if waited := time.Since(before); waited != 0 {
			t.Errorf("/stop took %v with %d requests in flight, want 0: the stop must not wait for a timeout", waited, c)
		}
		if _, recorded := histogram(t, h, contract.Opening); st.Requests != 0 || st.Opened != 0 || recorded {
			t.Errorf("Requests = %d, Opened = %d, opening recorded %v after stopping mid-open, want 0, 0 and nothing", st.Requests, st.Opened, recorded)
		}
		if rec := get(h, contract.StatsRoute); rec.Code != http.StatusNotFound {
			t.Errorf("GET /stats after stop = %d, want 404", rec.Code)
		}
	})
}

// TestDNSAndTLSStopMovingOnceOpened pins the emptiness the driver checks in
// a settled window: once every tunnel is open, the lookup and handshake
// counts do not move while the latency count does.
func TestDNSAndTLSStopMovingOnceOpened(t *testing.T) {
	t.Parallel()
	byName := newTarget(t, okHandler)
	u, err := url.Parse(byName.URL)
	if err != nil {
		t.Fatal(err)
	}
	tlsTarget, roots := newTLSTarget(t, okHandler)
	for _, tc := range []struct {
		name string
		h    *fixture
		req  contract.StartRequest
		dist contract.Distribution
	}{
		{"dns over http by name", serve("localhost:" + u.Port()), contract.StartRequest{Endpoints: 1}, contract.DNS},
		{"tls over https", serveTLS(roots, tlsTarget.host()), contract.StartRequest{Endpoints: 1, Scheme: contract.SchemeHTTPS}, contract.TLS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if rec := start(t, tc.h, tc.req); rec.Code != http.StatusNoContent {
				t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
			}
			waitFor(t, tc.h, atLeast(2))
			opened, first := samples(t, tc.h, tc.dist), samples(t, tc.h, contract.Latency)
			if opened == 0 {
				t.Errorf("the open recorded no %s sample", tc.dist)
			}
			// Two more requests after a count taken after the first read.
			waitFor(t, tc.h, atLeast(readStats(t, tc.h).Requests+2))
			stopLoop(t, tc.h)
			if later, second := samples(t, tc.h, tc.dist), samples(t, tc.h, contract.Latency); later != opened || second <= first {
				t.Errorf("%s went %d -> %d and latency %d -> %d over the settled requests; want %s flat and latency up", tc.dist, opened, later, first, second, tc.dist)
			}
		})
	}
}

func TestStopReturnsFinalStats(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())
	for _, tc := range []struct {
		name string
		rec  *httptest.ResponseRecorder
	}{
		{"GET /stats", get(h, contract.StatsRoute)},
		{"POST /stop", post(t, h, contract.StopRoute, nil)},
	} {
		if tc.rec.Code != http.StatusNotFound || strings.TrimSpace(tc.rec.Body.String()) != contract.NoLoopMessage {
			t.Errorf("%s with no loop = %d %q, want 404 %q", tc.name, tc.rec.Code, tc.rec.Body, contract.NoLoopMessage)
		}
	}
	if rec := start(t, h, contract.StartRequest{Endpoints: 1}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	waitFor(t, h, atLeast(1))
	rec := post(t, h, contract.StopRoute, nil)
	final := decodeStats(t, "POST /stop", rec)
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" || final.Scheme != contract.SchemeHTTP || final.Endpoints != nil || final.Requests == 0 {
		t.Errorf("POST /stop answered %q with scheme %q, endpoints %v and %d requests; want JSON, http, no endpoints unless asked, and the requests made", ct, final.Scheme, final.Endpoints, final.Requests)
	}
	if rec := get(h, contract.StatsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /stats after stop = %d, want 404", rec.Code)
	}

	// The final report is kept: a /stop the driver did not hear is answered
	// again, byte for byte, with or without the endpoints, until a new loop
	// replaces it.
	if again := post(t, h, contract.StopRoute, nil); again.Code != http.StatusOK || !bytes.Equal(again.Body.Bytes(), rec.Body.Bytes()) {
		t.Errorf("second POST /stop = %d %s; want 200 and the first answer %s", again.Code, again.Body, rec.Body)
	}
	if withEP := decodeStats(t, "third POST /stop", post(t, h, stopPath, nil)); len(withEP.Endpoints) != 1 || withEP.Requests != final.Requests {
		t.Errorf("POST /stop?endpoints=1 after stop: %d endpoints, %d requests; want 1 and %d", len(withEP.Endpoints), withEP.Requests, final.Requests)
	}
	if rec := start(t, h, contract.StartRequest{Endpoints: 1, IntervalMs: 60_000, OffsetMs: 60_000}); rec.Code != http.StatusNoContent {
		t.Fatalf("second POST /start = %d %s", rec.Code, rec.Body)
	}
	if next := stopLoop(t, h); next.Requests != 0 {
		t.Errorf("the stop after a new /start reports %d requests; want 0, the new loop's own report", next.Requests)
	}
}

// TestRequestsAtOpeningDone pins the count the blended window prices: the
// requests this actor had finished when its last endpoint opened, repeats on
// the endpoints already open included, and fixed from then on.
func TestRequestsAtOpeningDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const c, interval, openLatency = 2, 100 * time.Millisecond, 250 * time.Millisecond
		var mu sync.Mutex
		opened := map[string]bool{}
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			first := !opened[r.Host]
			opened[r.Host] = true
			mu.Unlock()
			if first && r.Host == "endpoint-1" {
				select {
				case <-time.After(openLatency):
				case <-r.Context().Done():
					return
				}
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		start := time.Now()
		h, stats := startPipeLoop(t, pn, contract.StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})

		// Endpoint 0 opens at 0 and repeats at 200; endpoint 1 opens at 100
		// and finishes at 350: the third request. Its first repeat follows at
		// once, since its period passed during the open.
		time.Sleep(interval + openLatency - time.Since(start))
		st := stats()
		if st.Opened != c || st.OpeningDone != interval+openLatency || st.RequestsAtOpeningDone != 3 || st.Requests != 4 {
			t.Errorf("at %v: opened %d at %v with %d requests, %d requests now; want %d at %v with 3, 4", time.Since(start), st.Opened, st.OpeningDone, st.RequestsAtOpeningDone, st.Requests, c, interval+openLatency)
		}
		time.Sleep(time.Second - time.Since(start))
		if st = stats(); st.RequestsAtOpeningDone != 3 || st.Requests <= 4 {
			t.Errorf("at %v: requestsAtOpeningDone %d with %d requests; want still 3 with more than 4", time.Since(start), st.RequestsAtOpeningDone, st.Requests)
		}
		if final := stopLoop(t, h); final.RequestsAtOpeningDone != 3 {
			t.Errorf("final requestsAtOpeningDone = %d, want 3", final.RequestsAtOpeningDone)
		}
	})
}

// TestFreshRecorderPerLoop pins that a loop starts from nothing: after
// start-stop-start the new loop's counters and instruments are empty.
func TestFreshRecorderPerLoop(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 2)
	first := runLoop(t, h, contract.StartRequest{Endpoints: 2}, atLeast(10))
	if samples(t, h, contract.Latency) == 0 {
		t.Fatalf("first loop recorded no latency over %d requests", first.Requests)
	}
	// Nothing in the new loop fires for a minute, so its instruments hold
	// only what the previous loop might have left behind.
	if rec := start(t, h, contract.StartRequest{Endpoints: 2, IntervalMs: 60_000, OffsetMs: 60_000}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	second := readStats(t, h)
	if _, recorded := histogram(t, h, contract.Latency); second.Requests != 0 || second.Opened != 0 || recorded {
		t.Errorf("second loop: %d requests, %d opened, latency recorded %v; want nothing", second.Requests, second.Opened, recorded)
	}
	stopLoop(t, h)
}

// TestInstrumentsTrackCountersUnderRace reads the instruments while requests
// land: every success records exactly one opening or latency sample under
// the same lock as the counters, so a histogram count read between two
// /stats polls lies between their Successes, every /stats poll holds
// successes + errors = requests, and the final instruments equal the final
// counters.
func TestInstrumentsTrackCountersUnderRace(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 8)
	if rec := start(t, h, contract.StartRequest{Endpoints: 8}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	var wg sync.WaitGroup
	var badReads atomic.Int64
	for range 4 {
		wg.Go(func() {
			for range 50 {
				st := readStats(t, h)
				var failed int64
				for _, n := range st.Errors {
					failed += n
				}
				if st.Successes+failed != st.Requests {
					badReads.Add(1)
				}
			}
		})
	}
	var outOfOrder int
	for range 200 {
		before := readStats(t, h)
		got := samples(t, h, contract.Opening) + samples(t, h, contract.Latency)
		after := readStats(t, h)
		if int64(got) < before.Successes || int64(got) > after.Successes {
			outOfOrder++
		}
	}
	wg.Wait()
	final := stopLoop(t, h)
	opening, latency := samples(t, h, contract.Opening), samples(t, h, contract.Latency)
	if badReads.Load() > 0 || outOfOrder > 0 {
		t.Errorf("%d reads broke successes + errors = requests, %d instrument reads fell outside the polls around them", badReads.Load(), outOfOrder)
	}
	if int64(opening+latency) != final.Successes || opening != 8 || final.Successes == 0 {
		t.Errorf("final instruments hold %d opening and %d latency samples for %d successes over 8 endpoints", opening, latency, final.Successes)
	}
}

// TestStatsDuringStopIsNever500 races many reads against a stop: each one
// answers 200 while the loop runs or 404 once it is stopped, never a 500.
func TestStatsDuringStopIsNever500(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 8)
	if rec := start(t, h, contract.StartRequest{Endpoints: 8}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	var wg sync.WaitGroup
	var ok, gone, other atomic.Int64
	stopped := make(chan struct{})
	for range 16 {
		wg.Go(func() {
			for {
				rec := get(h, contract.StatsRoute)
				switch {
				case rec.Code == http.StatusOK:
					ok.Add(1)
				case rec.Code == http.StatusNotFound && strings.TrimSpace(rec.Body.String()) == contract.NoLoopMessage:
					gone.Add(1)
				default:
					other.Add(1)
					t.Errorf("answered %d %s", rec.Code, rec.Body)
				}
				select {
				case <-stopped:
					return
				default:
				}
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	stopLoop(t, h)
	close(stopped)
	wg.Wait()
	if ok.Load() == 0 || gone.Load() == 0 || other.Load() != 0 {
		t.Errorf("%d answers were 200, %d 404, %d something else", ok.Load(), gone.Load(), other.Load())
	}
}

// TestEveryRouteCarriesProtocolHeader checks the header a driver reads to
// refuse an actor that speaks another contract, on every answer the actor
// gives, errors and unknown routes included.
func TestEveryRouteCarriesProtocolHeader(t *testing.T) {
	t.Parallel()
	if contract.ProtocolVersion != "1" {
		t.Errorf("ProtocolVersion = %q, want 1; a driver requires it", contract.ProtocolVersion)
	}
	h := newServer().handler()
	for _, tc := range []struct {
		name string
		rec  *httptest.ResponseRecorder
		want int
	}{
		{"readyz", get(h, contract.ReadyzRoute), http.StatusOK},
		{"start rejected", post(t, h, contract.StartRoute, map[string]int{"endpoints": 0}), http.StatusBadRequest},
		{"stats without a loop", get(h, contract.StatsRoute), http.StatusNotFound},
		{"stop without a loop", post(t, h, contract.StopRoute, nil), http.StatusNotFound},
		{"unknown route", get(h, "/metrics"), http.StatusNotFound},
	} {
		if tc.rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, tc.rec.Code, tc.want)
		}
		if got := tc.rec.Header().Get(contract.ProtocolHeader); got != contract.ProtocolVersion {
			t.Errorf("%s: %s = %q, want %q", tc.name, contract.ProtocolHeader, got, contract.ProtocolVersion)
		}
	}
}

// TestPayloadBudget pins the answer sizes at the largest C, since the driver
// polls every actor: /stats stays under 1 KiB, and the per-endpoint counts
// come only when asked for, under 80 KiB.
func TestPayloadBudget(t *testing.T) {
	t.Parallel()
	const c = contract.MaxEndpoints
	run, err := newLoopRun(context.Background(), identified(contract.StartRequest{Endpoints: c, Scheme: contract.SchemeHTTPS}), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.rec.Shutdown(context.Background()) })
	run.stats.Requests, run.stats.Successes, run.stats.NewConns, run.stats.Opened = 12345*c, 12339*c, c, c
	run.stats.OpeningDone, run.stats.LongestGap = 102400*time.Millisecond, 12345678*time.Microsecond
	run.stats.Errors = map[string]int64{"timeout": 3 * c, "HTTP 503": 3 * c}
	run.rounds = 12345
	for i := range c {
		run.stats.Endpoints[i] = contract.Endpoint{Requests: 12345, Errors: 6, LongestGap: 12345678 * time.Microsecond}
	}
	s := newServer()
	s.run = run
	h := s.handler()

	stats := get(h, contract.StatsRoute)
	statsEndpoints := get(h, statsPath)
	t.Logf("at C=%d: /stats %d B, /stats?endpoints=1 %d B", c, stats.Body.Len(), statsEndpoints.Body.Len())
	for _, tc := range []struct {
		name      string
		rec       *httptest.ResponseRecorder
		budget    int
		endpoints bool
	}{
		{"/stats", stats, 1 << 10, false},
		{"/stats?endpoints=1", statsEndpoints, 80 << 10, true},
	} {
		if tc.rec.Code != http.StatusOK {
			t.Fatalf("%s = %d %s", tc.name, tc.rec.Code, tc.rec.Body)
		}
		if tc.rec.Body.Len() >= tc.budget {
			t.Errorf("%s = %d B, budget %d B", tc.name, tc.rec.Body.Len(), tc.budget)
		}
		var st contract.Stats
		if err := json.Unmarshal(tc.rec.Body.Bytes(), &st); err != nil {
			t.Fatal(err)
		}
		if (len(st.Endpoints) == c) != tc.endpoints || st.Rounds != 12345 {
			t.Errorf("%s carries %d endpoints and rounds %d; want endpoints %v and rounds 12345", tc.name, len(st.Endpoints), st.Rounds, tc.endpoints)
		}
	}
}
