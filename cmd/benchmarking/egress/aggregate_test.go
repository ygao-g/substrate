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
	"math"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// fixtureScrape records samples on fresh instruments and returns the scrape
// as the driver would decode it, with the endpoint families.
func fixtureScrape(t *testing.T, c int, record func(m *loopMetrics)) families {
	t.Helper()
	m, err := newLoopMetrics(c, "http")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.shutdown(context.Background()) })
	record(m)
	mfs, err := m.gather(true)
	if err != nil {
		t.Fatal(err)
	}
	f := families{}
	for _, mf := range mfs {
		f[mf.GetName()] = mf
	}
	return f
}

func recordLatencies(ds ...time.Duration) func(m *loopMetrics) {
	return func(m *loopMetrics) {
		ctx := context.Background()
		for _, d := range ds {
			m.requests.Add(ctx, 1, m.success)
			m.latency.Record(ctx, d.Seconds())
		}
	}
}

// CONTRACT G29.
func TestAggregateEqualsOneRecorder(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var all []time.Duration
	parts := make([][]time.Duration, 3)
	for i := range 900 {
		d := time.Duration(math.Exp(math.Log(50)+r.Float64()*math.Log(60_000))) * time.Microsecond
		all = append(all, d)
		parts[i%3] = append(parts[i%3], d)
	}
	one := fixtureScrape(t, 2, recordLatencies(all...))
	sum := families{}
	for i, p := range parts {
		if err := sum.add(fixtureScrape(t, 2, recordLatencies(p...)), "actor-"+string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	want, got := one.histogram(famLatency), sum.histogram(famLatency)
	if got.GetSampleCount() != want.GetSampleCount() || math.Abs(got.GetSampleSum()-want.GetSampleSum()) > 1e-9 {
		t.Errorf("sum: count %d sum %v; one recorder: count %d sum %v", got.GetSampleCount(), got.GetSampleSum(), want.GetSampleCount(), want.GetSampleSum())
	}
	wb, gb := finiteBuckets(want), finiteBuckets(got)
	for i := range wb {
		if wb[i].GetCumulativeCount() != gb[i].GetCumulativeCount() {
			t.Errorf("bucket le=%v: sum %d, one recorder %d", wb[i].GetUpperBound(), gb[i].GetCumulativeCount(), wb[i].GetCumulativeCount())
		}
	}
	if sum.counter(famRequests) != 900 || sum.counter(famRequests, labelResult, resultSuccess) != 900 {
		t.Errorf("requests %d (success %d), want 900", sum.counter(famRequests), sum.counter(famRequests, labelResult, resultSuccess))
	}
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999, 1} {
		if quantile(got, q) != quantile(want, q) {
			t.Errorf("quantile(%v): sum %v, one recorder %v", q, quantile(got, q), quantile(want, q))
		}
	}
}

// CONTRACT R7.
func TestAggregateRefusesOtherLe(t *testing.T) {
	sum := families{}
	if err := sum.add(fixtureScrape(t, 1, recordLatencies(time.Millisecond)), "egress-0"); err != nil {
		t.Fatal(err)
	}
	before := sum.clone()
	other := fixtureScrape(t, 1, recordLatencies(2*time.Millisecond))
	// An actor built with 10 buckets per decade: every other bound.
	h := other.histogram(famLatency)
	var kept []*dto.Bucket
	for i, b := range h.GetBucket() {
		if i%2 == 0 {
			kept = append(kept, b)
		}
	}
	h.Bucket = kept
	err := sum.add(other, "egress-7")
	if err == nil || !strings.Contains(err.Error(), "egress-7") || !strings.Contains(err.Error(), famLatency) {
		t.Fatalf("add() = %v, want a refusal naming egress-7 and %s", err, famLatency)
	}
	if !proto.Equal(sum[famLatency], before[famLatency]) || sum.counter(famRequests) != before.counter(famRequests) {
		t.Errorf("a refused add changed the receiver")
	}
}

// CONTRACT G24.
func TestMissingInfBucketIsSampleCount(t *testing.T) {
	text := fixtureScrape(t, 1, recordLatencies(time.Millisecond, time.Second, 2*time.Minute))
	// The protobuf encoding has no +Inf bucket; the text one does.
	noInf := text.clone()
	h := noInf.histogram(famLatency)
	h.Bucket = finiteBuckets(h)
	sum := families{}
	for _, f := range []families{text, noInf} {
		if err := sum.add(f, "x"); err != nil {
			t.Fatal(err)
		}
	}
	got := sum.histogram(famLatency)
	if got.GetSampleCount() != 6 || quantile(got, 1) != math.Inf(1) || quantile(got, 0.5) != boundAtOrAbove(time.Millisecond) && quantile(got, 0.5) != boundAtOrAbove(time.Second) {
		t.Errorf("count %d, p100 %v, p50 %v", got.GetSampleCount(), quantile(got, 1), quantile(got, 0.5))
	}
	if n := len(finiteBuckets(got)); n != len(bucketBounds) {
		t.Errorf("%d finite buckets, want %d", n, len(bucketBounds))
	}
}

// CONTRACT G25.
func TestWindowAndReset(t *testing.T) {
	m, err := newLoopMetrics(1, "http")
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown(context.Background())
	snap := func() families {
		mfs, err := m.gather(false)
		if err != nil {
			t.Fatal(err)
		}
		f := families{}
		for _, mf := range mfs {
			f[mf.GetName()] = proto.Clone(mf).(*dto.MetricFamily)
		}
		return f
	}
	recordLatencies(10*time.Millisecond, 10*time.Millisecond)(m)
	first := snap()
	recordLatencies(100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)(m)
	second := snap()
	d, reset := histogramDelta(second.histogram(famLatency), first.histogram(famLatency))
	if reset || d.GetSampleCount() != 3 || math.Abs(d.GetSampleSum()-0.3) > 1e-9 {
		t.Fatalf("window: reset %v, count %d, sum %v; want 3 samples of 100ms", reset, d.GetSampleCount(), d.GetSampleSum())
	}
	if q := quantile(d, 0.5); q != boundAtOrAbove(100*time.Millisecond) {
		t.Errorf("window p50 = %v, want the 100ms bucket %v: the window holds none of the 10ms samples", q, boundAtOrAbove(100*time.Millisecond))
	}
	if reqs := second.counter(famRequests) - first.counter(famRequests); reqs != 3 {
		t.Errorf("window requests = %d, want 3", reqs)
	}
	// A restarted actor's later scrape is smaller than the earlier one.
	if _, reset := histogramDelta(first.histogram(famLatency), second.histogram(famLatency)); !reset {
		t.Errorf("a later scrape with fewer samples was not seen as a reset")
	}
}

// TestQuantileTable replays the stage-0 oracle's events through the new path
// and compares counts and quantiles with what the old code answered, except
// where the old code capped a quantile at the recorded max.
// CONTRACT G31.
func TestQuantileTable(t *testing.T) {
	raw, err := os.ReadFile("/tmp/pr2716-cobra/expo-baseline/events.json")
	if err != nil {
		t.Skipf("no oracle: %v", err)
	}
	var events []struct {
		Endpoint  int    `json:"endpoint"`
		LatencyUs int64  `json:"latencyMicros"`
		Status    int    `json:"status"`
		Err       string `json:"err"`
	}
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	var want map[string]struct {
		Count          uint64
		P50, P99, P999 int64
	}
	rawQ, _ := os.ReadFile("/tmp/pr2716-cobra/expo-baseline/quantiles.json")
	if err := json.Unmarshal(rawQ, &want); err != nil {
		t.Fatal(err)
	}
	f := fixtureScrape(t, 8, func(m *loopMetrics) {
		ctx := context.Background()
		seen := make([]bool, 8)
		for _, ev := range events {
			// As the actor does: an endpoint's first request is its first
			// whatever the outcome; only successes land in a histogram.
			first := !seen[ev.Endpoint]
			seen[ev.Endpoint] = true
			if ev.Status != 200 || ev.Err != "" {
				continue
			}
			d := time.Duration(ev.LatencyUs) * time.Microsecond
			if first {
				m.opening.Record(ctx, d.Seconds())
			} else {
				m.latency.Record(ctx, d.Seconds())
			}
		}
	})
	for _, name := range []string{"opening", "latency"} {
		h := f.histogram(name + "_seconds")
		w := want[name]
		if h.GetSampleCount() != w.Count {
			t.Errorf("%s: count %d, oracle %d", name, h.GetSampleCount(), w.Count)
		}
		for _, tc := range []struct {
			q      float64
			oracle int64
		}{{0.5, w.P50}, {0.99, w.P99}, {0.999, w.P999}} {
			got := quantile(h, tc.q)
			gotUs := math.Ceil(got * 1e6)
			if math.IsInf(got, 1) {
				gotUs = math.Inf(1)
			}
			if gotUs == float64(tc.oracle) {
				continue
			}
			// The bounds were rounded to four significant digits after the
			// oracle was taken, so the same bucket now reads up to 0.05% off
			// its old bound.
			if math.Abs(gotUs-float64(tc.oracle)) <= float64(tc.oracle)*5e-4+1 {
				continue
			}
			// The old code capped at the recorded max; the new one returns the
			// bucket bound. Only that case may differ, and the oracle's value
			// must then lie in the bucket.
			lower := 0.0
			for _, b := range bucketBounds {
				if b >= got {
					break
				}
				lower = b
			}
			if float64(tc.oracle) <= lower*1e6 || float64(tc.oracle) > gotUs {
				t.Errorf("%s q=%v: new %v µs, oracle %d µs, not a cap-at-max difference", name, tc.q, gotUs, tc.oracle)
			} else {
				t.Logf("%s q=%v: new %v µs (bucket bound), oracle %d µs (capped at max)", name, tc.q, gotUs, tc.oracle)
			}
		}
	}
}
