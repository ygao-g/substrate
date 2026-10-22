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

// Package actor is the workload of the egress scale test: the subcommand
// that holds one tunnel per endpoint open through the egress gateway and
// counts what happened.
package actor

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
)

// The names of the loop's instruments, the contract with the queries that
// read them once the collector has served them to Prometheus: a counter
// gains _total there, a unit of seconds _seconds.
const (
	meterName           = "benchmark-egress"
	requestsName        = "requests"
	requestErrorsName   = "request_errors"
	connsOpenedName     = "connections_opened"
	endpointsOpenedName = "endpoints_opened"
	roundsName          = "rounds"
	openingDoneName     = "opening_done"
	longestGapName      = "longest_gap"
)

// Recorder holds one loop's instruments on a private MeterProvider: the
// counters that mirror its Stats and the four latency histograms. Without a
// reader every method is a cheap no-op.
type Recorder struct {
	provider *sdkmetric.MeterProvider
	hists    map[contract.Distribution]metric.Float64Histogram

	requests, requestErrors, connsOpened, endpointsOpened metric.Int64Counter
	rounds                                                metric.Int64Gauge
	openingDone, longestGap                               metric.Float64Gauge

	// attrs is on every point; success and failure add the result.
	attrs, success, failure metric.MeasurementOption
}

// NewRecorder creates the instruments for the loop req starts, on a private
// MeterProvider with readers, which tests use to read the instruments.
func NewRecorder(ctx context.Context, req contract.StartRequest, readers ...sdkmetric.Reader) (*Recorder, error) {
	opts := make([]sdkmetric.Option, 0, len(readers))
	for _, rd := range readers {
		opts = append(opts, sdkmetric.WithReader(rd))
	}
	r := &Recorder{provider: sdkmetric.NewMeterProvider(opts...), hists: make(map[contract.Distribution]metric.Float64Histogram, len(contract.DistributionNames))}
	withResult := func(result string) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(attribute.String("result", result)))
	}
	r.attrs, r.success, r.failure = metric.WithAttributeSet(attribute.NewSet()), withResult("success"), withResult("error")
	if err := r.instruments(r.provider.Meter(meterName)); err != nil {
		_ = r.provider.Shutdown(context.Background())
		return nil, err
	}
	return r, nil
}

// instruments creates the eleven instruments on meter.
func (r *Recorder) instruments(meter metric.Meter) error {
	var err error
	counter := func(name, desc string) metric.Int64Counter {
		c, cerr := meter.Int64Counter(name, metric.WithDescription(desc))
		err = errors.Join(err, cerr)
		return c
	}
	r.requests = counter(requestsName, "Requests the loop finished, by result.")
	r.requestErrors = counter(requestErrorsName, "Failed requests, by class.")
	r.connsOpened = counter(connsOpenedName, "Requests sent on a freshly dialed connection.")
	r.endpointsOpened = counter(endpointsOpenedName, "Endpoints whose first request has completed.")
	var gerr error
	r.rounds, gerr = meter.Int64Gauge(roundsName, metric.WithDescription("Requests completed by the least-visited endpoint."))
	err = errors.Join(err, gerr)
	r.openingDone, gerr = meter.Float64Gauge(openingDoneName, metric.WithUnit("s"), metric.WithDescription("Seconds from /start to the last endpoint's open; zero until then."))
	err = errors.Join(err, gerr)
	r.longestGap, gerr = meter.Float64Gauge(longestGapName, metric.WithUnit("s"), metric.WithDescription("Longest gap between two consecutive requests on one endpoint."))
	err = errors.Join(err, gerr)
	for _, name := range contract.DistributionNames {
		h, herr := meter.Float64Histogram(string(name), metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(contract.Bounds...))
		err = errors.Join(err, herr)
		r.hists[name] = h
	}
	if err != nil {
		return fmt.Errorf("creating the instruments: %w", err)
	}
	return nil
}

// Record adds one sample, in seconds, to the named distribution.
func (r *Recorder) Record(ctx context.Context, name contract.Distribution, seconds float64) {
	r.hists[name].Record(ctx, seconds, r.attrs)
}

// Request counts one finished request by result.
func (r *Recorder) Request(ctx context.Context, ok bool) {
	if ok {
		r.requests.Add(ctx, 1, r.success)
	} else {
		r.requests.Add(ctx, 1, r.failure)
	}
}

// RequestError counts one failed request under its class.
func (r *Recorder) RequestError(ctx context.Context, class string) {
	r.requestErrors.Add(ctx, 1, r.attrs, metric.WithAttributes(attribute.String("class", class)))
}

// ConnectionOpened counts one request sent on a freshly dialed connection.
func (r *Recorder) ConnectionOpened(ctx context.Context) { r.connsOpened.Add(ctx, 1, r.attrs) }

// EndpointOpened counts one endpoint whose first request completed.
func (r *Recorder) EndpointOpened(ctx context.Context) { r.endpointsOpened.Add(ctx, 1, r.attrs) }

// Gauges records the three derived values of the Stats.
func (r *Recorder) Gauges(ctx context.Context, rounds int64, openingDone, longestGap float64) {
	r.rounds.Record(ctx, rounds, r.attrs)
	r.openingDone.Record(ctx, openingDone, r.attrs)
	r.longestGap.Record(ctx, longestGap, r.attrs)
}

// ForceFlush exports what every reader holds, now.
func (r *Recorder) ForceFlush(ctx context.Context) error { return r.provider.ForceFlush(ctx) }

// Shutdown exports once more and releases the provider. Every method is a
// no-op afterwards.
func (r *Recorder) Shutdown(ctx context.Context) error { return r.provider.Shutdown(ctx) }
