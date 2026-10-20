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
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// recorded returns the histogram of ds, recorded through a fresh instrument
// and read back through its reader.
func recorded(t *testing.T, ds ...time.Duration) histogram {
	t.Helper()
	li, err := newLatencyInstruments()
	if err != nil {
		t.Fatal(err)
	}
	defer li.shutdown(context.Background())
	for _, d := range ds {
		observe(li.latency, d)
	}
	hs, err := li.collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return hs[latencyInstrument]
}

// withinBucket reports whether got is no lower than want and at most one
// bucket width (~12%) above it.
func withinBucket(got, want time.Duration) bool {
	return got >= want && float64(got) <= float64(want)*1.13
}

func TestHistogramQuantiles(t *testing.T) {
	// 1ms, 2ms, ..., 1000ms: the q-th quantile is q*1000ms.
	var ds []time.Duration
	for i := 1; i <= 1000; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	h := recorded(t, ds...)
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{
		{0.5, 500 * time.Millisecond},
		{0.9, 900 * time.Millisecond},
		{0.99, 990 * time.Millisecond},
	} {
		if got := h.quantile(tc.q); !withinBucket(got, tc.want) {
			t.Errorf("quantile(%v) = %v, want within one bucket above %v", tc.q, got, tc.want)
		}
	}
	if got := h.quantile(1); got != time.Second {
		t.Errorf("quantile(1) = %v, want the max sample 1s", got)
	}
	if got, want := h.mean(), 500500*time.Microsecond; got != want {
		t.Errorf("mean() = %v, want %v", got, want)
	}
	if h.Count != 1000 {
		t.Errorf("Count = %d, want 1000", h.Count)
	}
}

func TestCollectKeysByInstrumentName(t *testing.T) {
	li, err := newLatencyInstruments()
	if err != nil {
		t.Fatal(err)
	}
	defer li.shutdown(context.Background())
	for _, h := range []metric.Float64Histogram{li.opening, li.latency, li.dns, li.tls} {
		observe(h, time.Millisecond)
	}
	hs, err := li.collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{openingInstrument, latencyInstrument, dnsInstrument, tlsInstrument} {
		if hs[name].Count != 1 {
			t.Errorf("collect()[%q].Count = %d, want 1", name, hs[name].Count)
		}
	}
	if len(hs) != 4 {
		t.Errorf("collect() returned %d histograms, want 4: %v", len(hs), slices.Sorted(maps.Keys(hs)))
	}
}

func TestHistogramUsesTheSharedBounds(t *testing.T) {
	h := recorded(t, time.Millisecond)
	if !slices.Equal(h.Bounds, bucketBounds) {
		t.Errorf("Bounds = %v, want bucketBounds", h.Bounds)
	}
	if len(h.BucketCounts) != len(bucketBounds)+1 {
		t.Errorf("%d bucket counts for %d bounds, want one per bound plus overflow", len(h.BucketCounts), len(bucketBounds))
	}
	if h.Sum != 1000 || h.Max != 1000 {
		t.Errorf("Sum = %v, Max = %v for one 1ms sample, want 1000 %s each", h.Sum, h.Max, latencyUnit)
	}
}

func TestHistogramEmpty(t *testing.T) {
	var h histogram
	if h.quantile(0.5) != 0 || h.mean() != 0 || h.max() != 0 {
		t.Errorf("empty histogram: quantile=%v mean=%v max=%v, want all 0", h.quantile(0.5), h.mean(), h.max())
	}
	if err := h.merge(histogram{}); err != nil {
		t.Fatal(err)
	}
	if h.BucketCounts != nil || h.Bounds != nil {
		t.Errorf("merging an empty histogram allocated buckets")
	}
	if got := recorded(t); !got.empty() {
		t.Errorf("an instrument with no samples collected %+v, want an empty histogram", got)
	}
}

func TestHistogramOverflow(t *testing.T) {
	h := recorded(t, 2*time.Minute)
	if got := h.BucketCounts[len(h.BucketCounts)-1]; got != 1 {
		t.Fatalf("overflow bucket = %d, want 1", got)
	}
	if got := h.quantile(0.5); got != 2*time.Minute {
		t.Errorf("quantile(0.5) = %v, want the overflowed sample 2m", got)
	}
}

func TestHistogramMergeMatchesSingleRecorder(t *testing.T) {
	var evens, odds, every []time.Duration
	for i := 1; i <= 500; i++ {
		d := time.Duration(i*i) * time.Microsecond
		every = append(every, d)
		if i%2 == 0 {
			evens = append(evens, d)
		} else {
			odds = append(odds, d)
		}
	}
	all := recorded(t, every...)
	var merged histogram
	for _, part := range []histogram{recorded(t, evens...), recorded(t, odds...)} {
		if err := merged.merge(part); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []float64{0.1, 0.5, 0.9, 0.99, 1} {
		if merged.quantile(q) != all.quantile(q) {
			t.Errorf("quantile(%v): merged %v, single recorder %v", q, merged.quantile(q), all.quantile(q))
		}
	}
	if merged.Count != all.Count || merged.Sum != all.Sum || merged.Max != all.Max || !slices.Equal(merged.BucketCounts, all.BucketCounts) {
		t.Errorf("merged totals %+v differ from %+v", merged, all)
	}
}

func TestHistogramMergeRefusesMismatchedBuckets(t *testing.T) {
	rec := recorded(t, time.Millisecond)
	decode := func(raw string) histogram {
		var h histogram
		if err := json.Unmarshal([]byte(raw), &h); err != nil {
			t.Fatal(err)
		}
		return h
	}
	otherBounds := rec.clone()
	otherBounds.Bounds[0] *= 2
	for name, tc := range map[string]struct {
		into histogram
		from histogram
	}{
		"more buckets than bounds": {rec.clone(), decode(`{"counts":[` + repeat("1,", 200) + `1],"count":201}`)},
		"ours decoded short":       {decode(`{"counts":[1],"count":1}`), rec},
		"counts missing":           {rec.clone(), histogram{Count: 99}},
		"other bounds":             {rec.clone(), otherBounds},
	} {
		t.Run(name, func(t *testing.T) {
			before := tc.into.clone()
			err := tc.into.merge(tc.from)
			if err == nil {
				t.Fatalf("merge accepted mismatched buckets: %+v", tc.into)
			}
			if tc.into.Count != before.Count || !slices.Equal(tc.into.BucketCounts, before.BucketCounts) {
				t.Errorf("a refused merge changed the receiver: %+v, was %+v", tc.into, before)
			}
		})
	}
	var empty histogram
	if err := empty.merge(histogram{}); err != nil {
		t.Errorf("merging two empty histograms: %v", err)
	}
	if err := empty.merge(rec); err != nil || empty.Count != 1 || !slices.Equal(empty.Bounds, rec.Bounds) {
		t.Errorf("merging into an empty histogram: %v, %+v", err, empty)
	}
}

func TestHistogramJSONRoundTrip(t *testing.T) {
	rec := recorded(t, time.Microsecond, time.Millisecond, time.Second, 2*time.Minute)
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var back histogram
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(back.Bounds, rec.Bounds) || !slices.Equal(back.BucketCounts, rec.BucketCounts) ||
		back.Count != rec.Count || back.Sum != rec.Sum || back.Max != rec.Max {
		t.Errorf("JSON round trip changed the histogram:\n got %+v\nwant %+v", back, rec)
	}
	if err := back.merge(rec); err != nil {
		t.Errorf("a decoded histogram does not merge with the one it was encoded from: %v", err)
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}
