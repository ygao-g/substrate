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
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// families is one scrape, or the sum of many: the metric families of
// GET /metrics by name. It is the Prometheus data model as expfmt decodes it;
// nothing of ours stands between the exposition and the aggregate.
type families map[string]*dto.MetricFamily

// gaugeRules says how a gauge family aggregates across actors: the latest
// time wins for the done and elapsed times and the quietest tunnel for the
// gaps, rounds add up so the total says how many requests the fleet swept,
// and loop_info is one actor's.
var gaugeRules = map[string]string{
	famOpeningDone:        "max",
	famLongestGap:         "max",
	famElapsed:            "max",
	famEndpointLongestGap: "max",
	famRounds:             "sum",
	famLoopInfo:           "first",
}

// labelKey identifies a metric within its family by its labels, le excluded.
func labelKey(m *dto.Metric) string {
	parts := make([]string, 0, len(m.GetLabel()))
	for _, l := range m.GetLabel() {
		parts = append(parts, l.GetName()+"="+l.GetValue())
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// clone deep-copies f.
func (f families) clone() families {
	c := make(families, len(f))
	for name, mf := range f {
		c[name] = proto.Clone(mf).(*dto.MetricFamily)
	}
	return c
}

// add merges the scrape of actor into f: counters and histogram buckets
// add per label set and gauges follow gaugeRules. A histogram whose bucket
// bounds are not bucketBounds, the fleet's, is refused with the actor named
// and f left as it was, whatever order the scrapes arrive in. An empty f
// takes a copy of the scrape.
func (f families) add(o families, actor string) error {
	// Check every histogram before touching anything, so a refusal leaves
	// f whole.
	for name, mf := range o {
		if mf.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		for _, m := range mf.GetMetric() {
			if err := fleetBounds(m.GetHistogram()); err != nil {
				return fmt.Errorf("%s: %s: %w", actor, name, err)
			}
		}
	}
	if len(f) == 0 {
		for name, mf := range o {
			f[name] = proto.Clone(mf).(*dto.MetricFamily)
		}
		return nil
	}
	for name, mf := range o {
		have, ok := f[name]
		if !ok {
			f[name] = proto.Clone(mf).(*dto.MetricFamily)
			continue
		}
		for _, m := range mf.GetMetric() {
			into := findMetric(have, m)
			if into == nil {
				have.Metric = append(have.Metric, proto.Clone(m).(*dto.Metric))
				continue
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				into.Counter.Value = proto.Float64(into.GetCounter().GetValue() + m.GetCounter().GetValue())
			case dto.MetricType_GAUGE:
				v := m.GetGauge().GetValue()
				switch gaugeRules[name] {
				case "max":
					v = max(into.GetGauge().GetValue(), v)
				case "sum":
					v += into.GetGauge().GetValue()
				default: // first
					v = into.GetGauge().GetValue()
				}
				into.Gauge.Value = proto.Float64(v)
			case dto.MetricType_HISTOGRAM:
				addHistogram(into.GetHistogram(), m.GetHistogram())
			}
		}
	}
	return nil
}

// findMetric returns the metric of mf with m's labels, or nil.
func findMetric(mf *dto.MetricFamily, m *dto.Metric) *dto.Metric {
	key := labelKey(m)
	for _, have := range mf.GetMetric() {
		if labelKey(have) == key {
			return have
		}
	}
	return nil
}

// fleetBounds reports whether h's finite bucket bounds are bucketBounds,
// the ones every actor built from this source records with. The +Inf bucket
// is optional: the protobuf encoding leaves it out.
func fleetBounds(h *dto.Histogram) error {
	if got := finiteBounds(h); !slices.Equal(got, bucketBounds) {
		return fmt.Errorf("histogram has %d bucket bounds, the fleet's has %d, or they differ", len(got), len(bucketBounds))
	}
	return nil
}

// sameBounds reports whether b's finite bucket bounds are a's.
func sameBounds(a, b *dto.Histogram) error {
	if !slices.Equal(finiteBounds(a), finiteBounds(b)) {
		return errors.New("histogram bucket bounds differ")
	}
	return nil
}

func finiteBounds(h *dto.Histogram) []float64 {
	var bounds []float64
	for _, b := range h.GetBucket() {
		if !math.IsInf(b.GetUpperBound(), 1) {
			bounds = append(bounds, b.GetUpperBound())
		}
	}
	return bounds
}

// addHistogram adds o's samples to h, bucket by bucket; both have the same
// finite bounds. h keeps no +Inf bucket: sample_count is it.
func addHistogram(h, o *dto.Histogram) {
	h.SampleCount = proto.Uint64(h.GetSampleCount() + o.GetSampleCount())
	h.SampleSum = proto.Float64(h.GetSampleSum() + o.GetSampleSum())
	hb, ob := finiteBuckets(h), finiteBuckets(o)
	for i := range hb {
		hb[i].CumulativeCount = proto.Uint64(hb[i].GetCumulativeCount() + ob[i].GetCumulativeCount())
	}
	h.Bucket = hb
}

func finiteBuckets(h *dto.Histogram) []*dto.Bucket {
	return slices.DeleteFunc(slices.Clone(h.GetBucket()), func(b *dto.Bucket) bool { return math.IsInf(b.GetUpperBound(), 1) })
}

// counter sums a counter family, over the metrics whose labels include
// every name=value pair of labels; no labels sums the whole family.
func (f families) counter(name string, labels ...string) int64 {
	var n float64
	for _, m := range f[name].GetMetric() {
		if hasLabels(m, labels) {
			n += m.GetCounter().GetValue()
		}
	}
	return int64(n)
}

// gauge is the value of a gauge family's one metric, or of the metric with
// labels; 0 when absent.
func (f families) gauge(name string, labels ...string) float64 {
	for _, m := range f[name].GetMetric() {
		if hasLabels(m, labels) {
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

// histogram is a histogram family's one data point, or nil before any sample.
func (f families) histogram(name string) *dto.Histogram {
	ms := f[name].GetMetric()
	if len(ms) == 0 {
		return nil
	}
	return ms[0].GetHistogram()
}

// errorCounts is request_errors_total by class.
func (f families) errorCounts() map[string]int64 {
	out := map[string]int64{}
	for _, m := range f[famRequestErrors].GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == labelClass {
				out[l.GetValue()] += int64(m.GetCounter().GetValue())
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func hasLabels(m *dto.Metric, want []string) bool {
	for i := 0; i+1 < len(want); i += 2 {
		found := false
		for _, l := range m.GetLabel() {
			if l.GetName() == want[i] && l.GetValue() == want[i+1] {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// quantile reads the q-th quantile from a cumulative histogram: the upper
// bound of the bucket holding the rank, in seconds; +Inf when the rank falls
// past the last finite bound, and 0 for an empty histogram.
func quantile(h *dto.Histogram, q float64) float64 {
	count := h.GetSampleCount()
	if count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(count)))
	rank = min(max(rank, 1), count)
	for _, b := range h.GetBucket() {
		if b.GetCumulativeCount() >= rank {
			return b.GetUpperBound()
		}
	}
	return math.Inf(1)
}

// mean is the average sample in seconds, or 0 for an empty histogram.
func mean(h *dto.Histogram) float64 {
	if h.GetSampleCount() == 0 {
		return 0
	}
	return h.GetSampleSum() / float64(h.GetSampleCount())
}

// histogramDelta is the samples later holds beyond earlier, for cumulative
// scrapes. reset reports a count that went down: the actor restarted, so
// there is no window and the caller re-anchors on later.
func histogramDelta(later, earlier *dto.Histogram) (d *dto.Histogram, reset bool) {
	if later == nil {
		return nil, earlier != nil && earlier.GetSampleCount() > 0
	}
	if earlier == nil {
		return proto.Clone(later).(*dto.Histogram), false
	}
	if later.GetSampleCount() < earlier.GetSampleCount() || sameBounds(later, earlier) != nil {
		return nil, true
	}
	d = proto.Clone(later).(*dto.Histogram)
	d.SampleCount = proto.Uint64(later.GetSampleCount() - earlier.GetSampleCount())
	d.SampleSum = proto.Float64(later.GetSampleSum() - earlier.GetSampleSum())
	db, eb := finiteBuckets(d), finiteBuckets(earlier)
	for i := range db {
		if db[i].GetCumulativeCount() < eb[i].GetCumulativeCount() {
			return nil, true
		}
		db[i].CumulativeCount = proto.Uint64(db[i].GetCumulativeCount() - eb[i].GetCumulativeCount())
	}
	d.Bucket = db
	return d, false
}
