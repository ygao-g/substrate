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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	bucketsPerDecade = 20
	// maxBucketSeconds is the largest finite bucket bound, 60s. Slower
	// samples land in the +Inf bucket.
	maxBucketSeconds = 60
)

// bucketBounds are the explicit bucket boundaries, in seconds, of every
// latency histogram: 10^(k/20) microseconds for k = 0, 1, ... up to the first
// bound at or past maxBucketSeconds. Neighboring bounds differ by about 12%,
// which is the resolution of every quantile read from a scrape. Every actor
// records with the same bounds, so scrapes from many actors add bucket by
// bucket.
var bucketBounds = func() []float64 {
	var bounds []float64
	for k := 0; ; k++ {
		b := math.Pow(10, float64(k)/bucketsPerDecade-6)
		// Pow is not bit-identical across architectures (Exp and Log have
		// assembly ports), and every actor and driver must emit the same
		// bound and the same le label, so each bound is rounded once to
		// four significant digits.
		b, _ = strconv.ParseFloat(strconv.FormatFloat(b, 'g', 4, 64), 64)
		bounds = append(bounds, b)
		if b >= maxBucketSeconds {
			return bounds
		}
	}
}()

// metricsRoute serves the running loop's families as a Prometheus
// exposition; ?endpoints=1 adds the per-endpoint ones.
const metricsRoute = "/metrics"

// The metric families a scrape of GET /metrics carries, named as the OTel
// Prometheus exporter emits them: counters end in _total, families with a
// unit carry it. The driver reads them by these names.
const (
	// famRequests counts finished requests by result, success or error.
	famRequests = "requests_total"
	// famRequestErrors counts failed requests by classify's class.
	famRequestErrors = "request_errors_total"
	// famConnectionsOpened counts requests sent on a freshly dialed
	// connection: one per endpoint is the tunnel, more are redials.
	famConnectionsOpened = "connections_opened_total"
	// famEndpointsOpened counts endpoints whose first request completed.
	famEndpointsOpened = "endpoints_opened_total"
	// famOpeningDone is the loop's elapsed time when its last endpoint
	// opened; 0 until then.
	famOpeningDone = "opening_done_seconds"
	// famRounds is the request count of the least-visited endpoint.
	famRounds = "rounds"
	// famLongestGap is the longest time between two consecutive requests
	// on one endpoint.
	famLongestGap = "longest_gap_seconds"
	// famElapsed is the time since the loop started.
	famElapsed = "loop_elapsed_seconds"
	// famLoopInfo is 1, with the scheme the loop requests endpoints over.
	famLoopInfo = "loop_info"
	// The four latency histograms, in seconds: first requests, repeats,
	// lookups and handshakes.
	famOpening = "opening_seconds"
	famLatency = "latency_seconds"
	famDNS     = "dns_seconds"
	famTLS     = "tls_seconds"
	// The per-endpoint families, served only on GET /metrics?endpoints=1:
	// requests by endpoint and result, and each endpoint's longest gap.
	famEndpointRequests   = "endpoint_requests_total"
	famEndpointLongestGap = "endpoint_longest_gap_seconds"
)

// Label names and the values of result.
const (
	labelResult   = "result"
	labelClass    = "class"
	labelScheme   = "scheme"
	labelEndpoint = "endpoint"
	resultSuccess = "success"
	resultError   = "error"
)

// loopMetrics are one loop's instruments. The OTel SDK aggregates them and
// the OTel Prometheus exporter exposes them through two registries: base,
// scraped every time, and endpoints, whose per-endpoint families a scrape
// has to ask for. No exporter runs behind either; GET /metrics gathers them.
type loopMetrics struct {
	base, endpoints *prometheus.Registry
	providers       []*sdkmetric.MeterProvider

	requests, requestErrors, connectionsOpened, endpointsOpened metric.Int64Counter
	openingDone, longestGap, elapsed                            metric.Float64Gauge
	rounds, loopInfo                                            metric.Int64Gauge
	opening, latency, dns, tls                                  metric.Float64Histogram
	endpointRequests                                            metric.Int64Counter
	endpointLongestGap                                          metric.Float64Gauge

	success, failure metric.MeasurementOption
	// endpointResult[i] holds endpoint i's success and error attribute
	// sets; endpointOnly[i] the endpoint label alone.
	endpointResult [][2]metric.MeasurementOption
	endpointOnly   []metric.MeasurementOption
	classes        map[string]metric.MeasurementOption
}

// newLoopMetrics creates the instruments of a loop over c endpoints and
// touches every counter and gauge once, so an empty loop exposes them at zero.
func newLoopMetrics(c int, scheme string) (*loopMetrics, error) {
	m := &loopMetrics{classes: map[string]metric.MeasurementOption{}}
	baseMeter, err := m.newRegistry(&m.base)
	if err != nil {
		return nil, err
	}
	// The SDK caps the attribute sets of one instrument, keeping one slot for
	// its overflow set, and its default of 2000 folds the last of the 2 x C
	// endpoint_requests sets into otel_metric_overflow at C = 1000.
	endpointMeter, err := m.newRegistry(&m.endpoints, sdkmetric.WithCardinalityLimit(2*c+1))
	if err != nil {
		return nil, err
	}
	var errs []error
	counter := func(meter metric.Meter, name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		errs = append(errs, err)
		return c
	}
	seconds := func(meter metric.Meter, name, desc string) metric.Float64Gauge {
		g, err := meter.Float64Gauge(name, metric.WithUnit("s"), metric.WithDescription(desc))
		errs = append(errs, err)
		return g
	}
	histogram := func(name, desc string) metric.Float64Histogram {
		h, err := baseMeter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(bucketBounds...))
		errs = append(errs, err)
		return h
	}
	m.requests = counter(baseMeter, "requests", "Requests finished, by result.")
	m.requestErrors = counter(baseMeter, "request_errors", "Failed requests, by class.")
	m.connectionsOpened = counter(baseMeter, "connections_opened", "Requests sent on a freshly dialed connection.")
	m.endpointsOpened = counter(baseMeter, "endpoints_opened", "Endpoints whose first request has completed.")
	m.openingDone = seconds(baseMeter, "opening_done", "Loop time when the last endpoint opened; 0 until then.")
	m.longestGap = seconds(baseMeter, "longest_gap", "Longest time between two consecutive requests on one endpoint.")
	m.elapsed = seconds(baseMeter, "loop_elapsed", "Time since the loop started.")
	m.rounds, err = baseMeter.Int64Gauge("rounds", metric.WithDescription("Requests completed by the least-visited endpoint."))
	errs = append(errs, err)
	m.loopInfo, err = baseMeter.Int64Gauge("loop_info", metric.WithDescription("1, with the scheme the loop requests endpoints over."))
	errs = append(errs, err)
	m.opening = histogram("opening", "End-to-end time of each endpoint's first request, when it succeeded.")
	m.latency = histogram("latency", "End-to-end time of successful requests after an endpoint's first.")
	m.dns = histogram("dns", "Lookup time of requests that did a lookup.")
	m.tls = histogram("tls", "Handshake time of handshakes that completed.")
	m.endpointRequests = counter(endpointMeter, "endpoint_requests", "Requests finished, by endpoint and result.")
	m.endpointLongestGap = seconds(endpointMeter, "endpoint_longest_gap", "Longest time between two consecutive requests, by endpoint.")
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("creating instruments: %w", err)
	}

	m.success = metric.WithAttributeSet(attribute.NewSet(attribute.String(labelResult, resultSuccess)))
	m.failure = metric.WithAttributeSet(attribute.NewSet(attribute.String(labelResult, resultError)))
	ctx := context.Background()
	m.requests.Add(ctx, 0, m.success)
	m.requests.Add(ctx, 0, m.failure)
	m.connectionsOpened.Add(ctx, 0)
	m.endpointsOpened.Add(ctx, 0)
	m.openingDone.Record(ctx, 0)
	m.longestGap.Record(ctx, 0)
	m.elapsed.Record(ctx, 0)
	m.rounds.Record(ctx, 0)
	m.loopInfo.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String(labelScheme, scheme))))
	m.endpointResult = make([][2]metric.MeasurementOption, c)
	m.endpointOnly = make([]metric.MeasurementOption, c)
	for i := range c {
		ep := attribute.String(labelEndpoint, strconv.Itoa(i))
		m.endpointResult[i] = [2]metric.MeasurementOption{
			metric.WithAttributeSet(attribute.NewSet(ep, attribute.String(labelResult, resultSuccess))),
			metric.WithAttributeSet(attribute.NewSet(ep, attribute.String(labelResult, resultError))),
		}
		m.endpointOnly[i] = metric.WithAttributeSet(attribute.NewSet(ep))
		m.endpointRequests.Add(ctx, 0, m.endpointResult[i][0])
		m.endpointRequests.Add(ctx, 0, m.endpointResult[i][1])
		m.endpointLongestGap.Record(ctx, 0, m.endpointOnly[i])
	}
	return m, nil
}

// newRegistry backs a fresh registry with an OTel Prometheus exporter and
// returns the meter that records into it. Scope and target info are off:
// one process, one scope, nothing to tell apart.
func (m *loopMetrics) newRegistry(reg **prometheus.Registry, opts ...sdkmetric.Option) (metric.Meter, error) {
	*reg = prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(*reg), otelprom.WithoutScopeInfo(), otelprom.WithoutTargetInfo())
	if err != nil {
		return nil, fmt.Errorf("creating the Prometheus exporter: %w", err)
	}
	opts = append([]sdkmetric.Option{sdkmetric.WithReader(exporter), sdkmetric.WithResource(resource.Empty())}, opts...)
	provider := sdkmetric.NewMeterProvider(opts...)
	m.providers = append(m.providers, provider)
	return provider.Meter("benchmark-egress"), nil
}

// class returns the attribute set for an error class, made once per class.
func (m *loopMetrics) class(name string) metric.MeasurementOption {
	opt, ok := m.classes[name]
	if !ok {
		opt = metric.WithAttributeSet(attribute.NewSet(attribute.String(labelClass, name)))
		m.classes[name] = opt
	}
	return opt
}

// gather collects the base families and, when asked, the per-endpoint ones.
// The caller holds the loop's lock, so one scrape is one consistent cut.
func (m *loopMetrics) gather(withEndpoints bool) ([]*dto.MetricFamily, error) {
	mfs, err := m.base.Gather()
	if err != nil {
		return nil, err
	}
	if withEndpoints {
		more, err := m.endpoints.Gather()
		if err != nil {
			return nil, err
		}
		mfs = append(mfs, more...)
	}
	return mfs, nil
}

func (m *loopMetrics) shutdown(ctx context.Context) error {
	var errs []error
	for _, p := range m.providers {
		errs = append(errs, p.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

// writeExposition encodes the families in the format the request's Accept
// header negotiates: the Prometheus protobuf encoding when asked, text
// otherwise.
func writeExposition(w http.ResponseWriter, r *http.Request, mfs []*dto.MetricFamily) {
	format := expfmt.Negotiate(r.Header)
	w.Header().Set("Content-Type", string(format))
	enc := expfmt.NewEncoder(w, format)
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			return // the client went away; nothing to do
		}
	}
	if closer, ok := enc.(expfmt.Closer); ok {
		_ = closer.Close()
	}
}

// decodeFamilies parses an exposition in format, as writeExposition and any
// Prometheus endpoint produce it, into families by name.
func decodeFamilies(r io.Reader, format expfmt.Format) (map[string]*dto.MetricFamily, error) {
	if format.FormatType() != expfmt.TypeProtoDelim {
		parser := expfmt.NewTextParser(model.LegacyValidation)
		return parser.TextToMetricFamilies(r)
	}
	dec := expfmt.NewDecoder(r, format)
	fams := map[string]*dto.MetricFamily{}
	for {
		mf := &dto.MetricFamily{}
		if err := dec.Decode(mf); err != nil {
			if errors.Is(err, io.EOF) {
				return fams, nil
			}
			return nil, err
		}
		fams[mf.GetName()] = mf
	}
}
