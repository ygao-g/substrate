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

package egresstunnels

import (
	"testing"
	"time"
)

// withinBucket reports whether got is no lower than want and at most one
// bucket width (~12%) above it.
func withinBucket(got, want time.Duration) bool {
	return got >= want && float64(got) <= float64(want)*1.13
}

func TestHistogramQuantiles(t *testing.T) {
	var h Histogram
	// 1ms, 2ms, ..., 1000ms: the q-th quantile is q*1000ms.
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{
		{0.5, 500 * time.Millisecond},
		{0.9, 900 * time.Millisecond},
		{0.99, 990 * time.Millisecond},
	} {
		if got := h.Quantile(tc.q); !withinBucket(got, tc.want) {
			t.Errorf("Quantile(%v) = %v, want within one bucket above %v", tc.q, got, tc.want)
		}
	}
	if got := h.Quantile(1); got != time.Second {
		t.Errorf("Quantile(1) = %v, want the max sample 1s", got)
	}
	if got, want := h.Mean(), 500500*time.Microsecond; got != want {
		t.Errorf("Mean() = %v, want %v", got, want)
	}
	if h.Count != 1000 {
		t.Errorf("Count = %d, want 1000", h.Count)
	}
}

func TestHistogramEmpty(t *testing.T) {
	var h Histogram
	if h.Quantile(0.5) != 0 || h.Mean() != 0 || h.Max() != 0 {
		t.Errorf("empty histogram: Quantile=%v Mean=%v Max=%v, want all 0", h.Quantile(0.5), h.Mean(), h.Max())
	}
	h.Merge(Histogram{})
	if h.Counts != nil {
		t.Errorf("merging an empty histogram allocated buckets")
	}
}

func TestHistogramOverflow(t *testing.T) {
	var h Histogram
	h.Record(2 * time.Minute)
	if got := h.Counts[len(h.Counts)-1]; got != 1 {
		t.Fatalf("overflow bucket = %d, want 1", got)
	}
	if got := h.Quantile(0.5); got != 2*time.Minute {
		t.Errorf("Quantile(0.5) = %v, want the overflowed sample 2m", got)
	}
}

func TestHistogramMergeMatchesSingleRecorder(t *testing.T) {
	var a, b, all Histogram
	for i := 1; i <= 500; i++ {
		d := time.Duration(i*i) * time.Microsecond
		all.Record(d)
		if i%2 == 0 {
			a.Record(d)
		} else {
			b.Record(d)
		}
	}
	var merged Histogram
	merged.Merge(a)
	merged.Merge(b)
	for _, q := range []float64{0.1, 0.5, 0.9, 0.99, 1} {
		if merged.Quantile(q) != all.Quantile(q) {
			t.Errorf("Quantile(%v): merged %v, single recorder %v", q, merged.Quantile(q), all.Quantile(q))
		}
	}
	if merged.Count != all.Count || merged.SumMicros != all.SumMicros || merged.MaxMicros != all.MaxMicros {
		t.Errorf("merged totals %+v differ from %+v", merged, all)
	}
}
