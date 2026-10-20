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
	"math"
	"slices"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	bucketsPerDecade = 20
	// maxBucketMicros is the largest finite bucket bound, 60s. Slower samples
	// land in the overflow bucket.
	maxBucketMicros = 60_000_000
	// latencyUnit is the unit of every latency instrument. In microseconds
	// every sample, sum and bound is an integer, so the JSON stays exact.
	// Nothing exports these instruments, so seconds would buy nothing.
	latencyUnit = "us"
)

// bucketBounds are the explicit bucket boundaries, in microseconds, of every
// latency instrument: 10^(k/20) for k = 0, 1, ... up to the first bound at or
// past maxBucketMicros. Neighboring bounds differ by about 12%, which is the
// resolution of every quantile read from a histogram. Every actor records
// with the same bounds, so histograms from many actors merge exactly.
var bucketBounds = func() []float64 {
	var bounds []float64
	for k := 0; ; k++ {
		b := math.Pow(10, float64(k)/bucketsPerDecade)
		bounds = append(bounds, b)
		if b >= maxBucketMicros {
			return bounds
		}
	}
}()

// Names of the latency instruments, and of the stats fields that carry them.
const (
	openingInstrument = "opening"
	latencyInstrument = "latency"
	dnsInstrument     = "dns"
	tlsInstrument     = "tls"
)

// latencyInstruments are one loop's histograms. The OTel SDK buckets the
// samples, and a ManualReader with no exporter behind it reads them back.
type latencyInstruments struct {
	provider                   *sdkmetric.MeterProvider
	reader                     *sdkmetric.ManualReader
	opening, latency, dns, tls metric.Float64Histogram
}

func newLatencyInstruments() (*latencyInstruments, error) {
	reader := sdkmetric.NewManualReader()
	li := &latencyInstruments{provider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), reader: reader}
	meter := li.provider.Meter("benchmark-egress")
	for _, inst := range []struct {
		name, desc string
		into       *metric.Float64Histogram
	}{
		{openingInstrument, "End-to-end time of each endpoint's first request, when it succeeded.", &li.opening},
		{latencyInstrument, "End-to-end time of successful requests after an endpoint's first.", &li.latency},
		{dnsInstrument, "Lookup time of requests that did a lookup.", &li.dns},
		{tlsInstrument, "Handshake time of handshakes that completed.", &li.tls},
	} {
		h, err := meter.Float64Histogram(inst.name,
			metric.WithUnit(latencyUnit),
			metric.WithDescription(inst.desc),
			metric.WithExplicitBucketBoundaries(bucketBounds...))
		if err != nil {
			return nil, fmt.Errorf("creating the %s instrument: %w", inst.name, err)
		}
		*inst.into = h
	}
	return li, nil
}

// observe records one sample.
func observe(h metric.Float64Histogram, d time.Duration) {
	h.Record(context.Background(), float64(max(d.Microseconds(), 0)))
}

// collect reads every instrument's cumulative data point, keyed by instrument
// name. An instrument with no samples yet has no data point and no entry.
func (li *latencyInstruments) collect(ctx context.Context) (map[string]histogram, error) {
	var rm metricdata.ResourceMetrics
	if err := li.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	hs := make(map[string]histogram)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			data, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				return nil, fmt.Errorf("%s is %T, want a float64 histogram", m.Name, m.Data)
			}
			// The instruments record no attributes, so each has one data point.
			for _, dp := range data.DataPoints {
				hs[m.Name] = histogramOf(dp)
			}
		}
	}
	return hs, nil
}

func (li *latencyInstruments) shutdown(ctx context.Context) error {
	return li.provider.Shutdown(ctx)
}

// histogram is the wire form of a metricdata.HistogramDataPoint[float64]:
// the fields a reader needs to merge it and read quantiles from it. The data
// point itself does not survive encoding/json, since its attribute set,
// extrema and exemplars have no UnmarshalJSON. The zero value is empty.
type histogram struct {
	// Bounds are the inclusive upper bounds of the finite buckets, in
	// latencyUnit. BucketCounts has one entry per bound plus a final overflow
	// bucket. Both are nil until the first sample.
	Bounds       []float64 `json:"bounds,omitempty"`
	BucketCounts []uint64  `json:"counts,omitempty"`
	Count        uint64    `json:"count"`
	Sum          float64   `json:"sumMicros"`
	Max          float64   `json:"maxMicros"`
}

func histogramOf(dp metricdata.HistogramDataPoint[float64]) histogram {
	h := histogram{
		Bounds:       slices.Clone(dp.Bounds),
		BucketCounts: slices.Clone(dp.BucketCounts),
		Count:        dp.Count,
		Sum:          dp.Sum,
	}
	h.Max, _ = dp.Max.Value()
	return h
}

func (h *histogram) empty() bool {
	return h.Count == 0 && h.Bounds == nil && h.BucketCounts == nil
}

// merge adds every sample of o to h. It refuses, leaving h as it was, a
// histogram whose buckets are not h's: one from an actor built with other
// bounds, or one whose counts were dropped in transit. An empty h takes o's
// bounds.
func (h *histogram) merge(o histogram) error {
	if o.empty() {
		return nil
	}
	if err := checkBuckets(o); err != nil {
		return err
	}
	if h.empty() {
		*h = o.clone()
		return nil
	}
	if err := checkBuckets(*h); err != nil {
		return err
	}
	if !slices.Equal(h.Bounds, o.Bounds) {
		return errors.New("histogram bounds differ")
	}
	for i, c := range o.BucketCounts {
		h.BucketCounts[i] += c
	}
	h.Count += o.Count
	h.Sum += o.Sum
	h.Max = max(h.Max, o.Max)
	return nil
}

// checkBuckets reports whether h has one count per bound plus the overflow.
func checkBuckets(h histogram) error {
	if len(h.BucketCounts) != len(h.Bounds)+1 {
		return fmt.Errorf("histogram has %d bucket counts for %d bounds, want %d", len(h.BucketCounts), len(h.Bounds), len(h.Bounds)+1)
	}
	return nil
}

// quantile returns the upper bound of the bucket holding the q-th sample,
// capped at the largest sample seen. It returns 0 for an empty histogram.
func (h *histogram) quantile(q float64) time.Duration {
	if h.Count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.Count)))
	rank = min(max(rank, 1), h.Count)
	var seen uint64
	for i, c := range h.BucketCounts {
		seen += c
		if seen < rank {
			continue
		}
		if i == len(h.Bounds) {
			break
		}
		return duration(min(math.Ceil(h.Bounds[i]), h.Max))
	}
	return duration(h.Max)
}

// mean returns the average sample, or 0 for an empty histogram.
func (h *histogram) mean() time.Duration {
	if h.Count == 0 {
		return 0
	}
	return duration(math.Floor(h.Sum / float64(h.Count)))
}

// max returns the largest sample.
func (h *histogram) max() time.Duration {
	return duration(h.Max)
}

func (h *histogram) clone() histogram {
	c := *h
	c.Bounds = slices.Clone(h.Bounds)
	c.BucketCounts = slices.Clone(h.BucketCounts)
	return c
}

// duration converts a value in latencyUnit.
func duration(us float64) time.Duration {
	return time.Duration(us) * time.Microsecond
}
