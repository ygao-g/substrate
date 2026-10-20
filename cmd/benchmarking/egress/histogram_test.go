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
	"flag"
	"fmt"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata goldens")

// scrape GETs the exposition in the text format and parses it. withEndpoints
// asks for the per-endpoint families.
func scrape(t *testing.T, h http.Handler, withEndpoints bool) map[string]*dto.MetricFamily {
	t.Helper()
	rec := getText(h, metricsPath(withEndpoints))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d %s", metricsPath(withEndpoints), rec.Code, rec.Body)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	fams, err := parser.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("parsing the exposition: %v\n%s", err, rec.Body)
	}
	return fams
}

// scrapeProto is scrape over the protobuf encoding.
func scrapeProto(t *testing.T, h http.Handler, withEndpoints bool) map[string]*dto.MetricFamily {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, metricsPath(withEndpoints), nil)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeProtoDelim)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s (protobuf) = %d %s", metricsPath(withEndpoints), rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/vnd.google.protobuf") {
		t.Fatalf("protobuf scrape answered Content-Type %q", ct)
	}
	fams, err := decodeFamilies(rec.Body, expfmt.ResponseFormat(rec.Header()))
	if err != nil {
		t.Fatalf("decoding the protobuf exposition: %v", err)
	}
	return fams
}

// getText GETs path from h in the text format.
func getText(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func metricsPath(withEndpoints bool) string {
	if withEndpoints {
		return metricsRoute + "?endpoints=1"
	}
	return metricsRoute
}

// loopView is what the old JSON body carried, read back from a scrape, so the
// loop tests keep their assertions.
type loopView struct {
	Scheme                                string
	Elapsed, OpeningDone, LongestGap      time.Duration
	Requests, Successes, NewConns, Opened int64
	Rounds                                int64
	Errors                                map[string]int64
	Endpoints                             []endpointView
	Opening, Latency, DNS, TLS            histView
}

type endpointView struct {
	Requests, Errors int64
	LongestGap       time.Duration
}

// histView is one histogram family's single data point.
type histView struct {
	Count   uint64
	Sum     float64 // seconds
	Buckets []*dto.Bucket
}

// bucketOf returns the cumulative count of the bucket with upper bound le;
// the +Inf bucket is the sample count.
func (h histView) bucketOf(le float64) uint64 {
	if math.IsInf(le, 1) {
		return h.Count
	}
	for _, b := range h.Buckets {
		if b.GetUpperBound() == le {
			return b.GetCumulativeCount()
		}
	}
	return 0
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func viewOf(t *testing.T, fams map[string]*dto.MetricFamily) *loopView {
	t.Helper()
	v := &loopView{Errors: map[string]int64{}}
	counter := func(name string) int64 {
		var n int64
		for _, m := range fams[name].GetMetric() {
			n += int64(m.GetCounter().GetValue())
		}
		return n
	}
	gauge := func(name string) float64 {
		ms := fams[name].GetMetric()
		if len(ms) == 0 {
			return 0
		}
		return ms[0].GetGauge().GetValue()
	}
	secs := func(f float64) time.Duration { return time.Duration(math.Round(f * 1e9)) }
	hist := func(name string) histView {
		ms := fams[name].GetMetric()
		if len(ms) == 0 {
			return histView{}
		}
		h := ms[0].GetHistogram()
		// The text encoding spells the +Inf bucket out and the protobuf one
		// leaves it to sample_count; the view keeps the finite buckets only.
		buckets := slices.DeleteFunc(slices.Clone(h.GetBucket()), func(b *dto.Bucket) bool { return math.IsInf(b.GetUpperBound(), 1) })
		return histView{Count: h.GetSampleCount(), Sum: h.GetSampleSum(), Buckets: buckets}
	}
	for _, m := range fams[famRequests].GetMetric() {
		n := int64(m.GetCounter().GetValue())
		v.Requests += n
		if labelValue(m, "result") == "success" {
			v.Successes += n
		}
	}
	for _, m := range fams[famRequestErrors].GetMetric() {
		v.Errors[labelValue(m, "class")] += int64(m.GetCounter().GetValue())
	}
	if len(v.Errors) == 0 {
		v.Errors = nil
	}
	v.NewConns, v.Opened = counter(famConnectionsOpened), counter(famEndpointsOpened)
	v.Rounds = int64(gauge(famRounds))
	v.OpeningDone, v.LongestGap, v.Elapsed = secs(gauge(famOpeningDone)), secs(gauge(famLongestGap)), secs(gauge(famElapsed))
	if ms := fams[famLoopInfo].GetMetric(); len(ms) > 0 {
		v.Scheme = labelValue(ms[0], "scheme")
	}
	v.Opening, v.Latency, v.DNS, v.TLS = hist(famOpening), hist(famLatency), hist(famDNS), hist(famTLS)
	if reqs := fams[famEndpointRequests]; reqs != nil {
		for _, m := range reqs.GetMetric() {
			i, err := strconv.Atoi(labelValue(m, "endpoint"))
			if err != nil {
				t.Fatalf("endpoint label %q: %v", labelValue(m, "endpoint"), err)
			}
			for len(v.Endpoints) <= i {
				v.Endpoints = append(v.Endpoints, endpointView{})
			}
			n := int64(m.GetCounter().GetValue())
			v.Endpoints[i].Requests += n
			if labelValue(m, "result") == "error" {
				v.Endpoints[i].Errors += n
			}
		}
		for _, m := range fams[famEndpointLongestGap].GetMetric() {
			i, _ := strconv.Atoi(labelValue(m, "endpoint"))
			if i < len(v.Endpoints) {
				v.Endpoints[i].LongestGap = secs(m.GetGauge().GetValue())
			}
		}
	}
	return v
}

// scrapeView is viewOf(scrape()), with the endpoint families.
func scrapeView(t *testing.T, h http.Handler) *loopView {
	t.Helper()
	return viewOf(t, scrape(t, h, true))
}

// metricsHandlerOf serves one scrape of m, as the loop's handler does once
// it has set the derived gauges.
func metricsHandlerOf(m *loopMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mfs, err := m.gather(r.URL.Query().Get("endpoints") == "1")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeExposition(w, r, mfs)
	})
}

// scriptedMetrics records a fixed sequence on fresh instruments, so a scrape
// has a known shape: 60 requests over c endpoints, each endpoint opened with
// a lookup and a handshake, a few 503s and timeouts.
func scriptedMetrics(t *testing.T, c int) *loopMetrics {
	t.Helper()
	m, err := newLoopMetrics(c, "https")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.shutdown(context.Background()) })
	ctx := context.Background()
	for n := range 60 {
		i := n % c
		if n < c {
			m.connectionsOpened.Add(ctx, 1)
			m.dns.Record(ctx, (time.Duration(100+n) * time.Microsecond).Seconds())
			m.tls.Record(ctx, (time.Duration(5+n) * time.Millisecond).Seconds())
			m.endpointsOpened.Add(ctx, 1)
		}
		latency := (time.Duration(1+n) * time.Millisecond).Seconds()
		switch class := ""; {
		case n%17 == 3:
			class = "HTTP 503"
			fallthrough
		case n%23 == 5:
			if class == "" {
				class = "timeout"
			}
			m.requests.Add(ctx, 1, m.failure)
			m.requestErrors.Add(ctx, 1, m.class(class))
			m.endpointRequests.Add(ctx, 1, m.endpointResult[i][1])
		default:
			m.requests.Add(ctx, 1, m.success)
			m.endpointRequests.Add(ctx, 1, m.endpointResult[i][0])
			if n < c {
				m.opening.Record(ctx, latency)
			} else {
				m.latency.Record(ctx, latency)
			}
		}
	}
	m.rounds.Record(ctx, int64(60/c))
	m.longestGap.Record(ctx, 0.4)
	m.endpointLongestGap.Record(ctx, 0.4, m.endpointOnly[0])
	return m
}

func TestScrapeGolden(t *testing.T) {
	fams := scrape(t, metricsHandlerOf(scriptedMetrics(t, 4)), true)
	var lines []string
	for _, name := range slices.Sorted(maps.Keys(fams)) {
		f := fams[name]
		for _, m := range f.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				if l.GetName() == "le" {
					continue
				}
				labels = append(labels, l.GetName()+"="+l.GetValue())
			}
			lines = append(lines, fmt.Sprintf("%s %s {%s}", name, f.GetType(), strings.Join(labels, ",")))
		}
	}
	got := strings.Join(slices.Compact(lines), "\n") + "\n"
	path := filepath.Join("testdata", "scrape.txt")
	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("scrape shape differs from %s (run with -update to accept):\n%s", path, got)
	}
}

func TestOneSampleLandsInOneBucket(t *testing.T) {
	m, err := newLoopMetrics(1, "http")
	if err != nil {
		t.Fatal(err)
	}
	// 1 ms is a bound: the sample goes in the bucket whose le is 0.001, not
	// the one above it.
	m.opening.Record(context.Background(), time.Millisecond.Seconds())
	v := scrapeView(t, metricsHandlerOf(m))
	h := v.Opening
	if h.Count != 1 || h.Sum != 0.001 {
		t.Fatalf("one 1ms sample: count %d sum %v, want 1 and 0.001", h.Count, h.Sum)
	}
	le := bucketBounds[60]
	if le != 0.001 {
		t.Fatalf("bucketBounds[60] = %v, want 0.001", le)
	}
	if h.bucketOf(le) != 1 || h.bucketOf(bucketBounds[59]) != 0 || h.bucketOf(bucketBounds[61]) != 1 {
		t.Errorf("buckets around 1ms: le=%v -> %d, le=%v -> %d, le=%v -> %d; want 0, 1, 1",
			bucketBounds[59], h.bucketOf(bucketBounds[59]), le, h.bucketOf(le), bucketBounds[61], h.bucketOf(bucketBounds[61]))
	}
	if got := len(h.Buckets); got != len(bucketBounds) {
		t.Errorf("%d finite buckets, want %d", got, len(bucketBounds))
	}
	if h.bucketOf(math.Inf(1)) != 1 {
		t.Errorf("+Inf bucket = %d, want the sample count 1", h.bucketOf(math.Inf(1)))
	}
}

func TestEmptyLoopExposesCountersAndGaugesAtZero(t *testing.T) {
	m, err := newLoopMetrics(2, "http")
	if err != nil {
		t.Fatal(err)
	}
	fams := scrape(t, metricsHandlerOf(m), true)
	for _, name := range []string{famRequests, famConnectionsOpened, famEndpointsOpened, famOpeningDone, famRounds, famLongestGap, famElapsed, famLoopInfo, famEndpointRequests, famEndpointLongestGap} {
		if fams[name] == nil {
			t.Errorf("%s is absent from an empty loop's scrape", name)
		}
	}
	v := viewOf(t, fams)
	if v.Requests != 0 || v.Successes != 0 || v.NewConns != 0 || v.Opened != 0 || v.Rounds != 0 || v.OpeningDone != 0 || v.LongestGap != 0 || v.Scheme != "http" || len(v.Endpoints) != 2 {
		t.Errorf("empty loop view = %+v, want zeros, scheme http and 2 endpoints", v)
	}
	// Histogram families appear with their first sample: the SDK emits no
	// data point for an instrument nothing was recorded on.
	for _, name := range []string{famOpening, famLatency, famDNS, famTLS} {
		if fams[name] != nil {
			t.Errorf("%s present before any sample", name)
		}
	}
}

func TestExporterOptionsPinned(t *testing.T) {
	rec := getText(metricsHandlerOf(scriptedMetrics(t, 2)), metricsPath(true))
	body := rec.Body.String()
	for _, bad := range []string{"otel_scope_", "target_info", "otel_scope_info"} {
		if strings.Contains(body, bad) {
			t.Errorf("scrape carries %q", bad)
		}
	}
	for _, want := range []string{famRequests + "{", famOpening + "_bucket{", famOpening + "_sum ", famOpening + "_count ", famLongestGap + " ", famEndpointRequests + "{"} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape lacks %q", want)
		}
	}
	if strings.Contains(body, "_seconds_seconds") || strings.Contains(body, "_total_total") {
		t.Errorf("doubled suffix in scrape")
	}
}

// TestBoundsAreReproducible pins the bounds to what every build emits: the
// four-digit rounding is a fixed point, the labels are short, and the table
// has the shape the README states.
func TestBoundsAreReproducible(t *testing.T) {
	if n := len(bucketBounds); n != 157 {
		t.Fatalf("%d bounds, want 157", n)
	}
	if first, last := bucketBounds[0], bucketBounds[len(bucketBounds)-1]; first != 1e-6 || last != 63.1 {
		t.Errorf("bounds run %v to %v, want 1e-06 to 63.1", first, last)
	}
	for i, b := range bucketBounds {
		again, _ := strconv.ParseFloat(strconv.FormatFloat(b, 'g', 4, 64), 64)
		if again != b {
			t.Errorf("bound %d = %v is not a fixed point of the rounding (%v)", i, b, again)
		}
		label := strconv.FormatFloat(b, 'g', -1, 64)
		if digits := len(strings.TrimLeft(strings.ReplaceAll(strings.SplitN(label, "e", 2)[0], ".", ""), "0")); digits > 4 {
			t.Errorf("bound %d label %q has %d significant digits, want at most 4", i, label, digits)
		}
		if i > 0 && b <= bucketBounds[i-1] {
			t.Errorf("bound %d = %v does not exceed bound %d = %v", i, b, i-1, bucketBounds[i-1])
		}
	}
}

func TestLeLabelsAreTheBounds(t *testing.T) {
	rec := getText(metricsHandlerOf(scriptedMetrics(t, 1)), metricsPath(false))
	var les []string
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, famLatency+"_bucket{le=") {
			les = append(les, strings.TrimSuffix(strings.TrimPrefix(line, famLatency+"_bucket{le=\""), "\"} "+strings.Fields(line)[1]))
		}
	}
	want := make([]string, 0, len(bucketBounds)+1)
	for _, b := range bucketBounds {
		want = append(want, strconv.FormatFloat(b, 'g', -1, 64))
	}
	want = append(want, "+Inf")
	for i := range want {
		if i >= len(les) || les[i] != want[i] {
			t.Fatalf("le label %d = %q, want %q (of %d)", i, les, want[i], len(want))
		}
	}
	if len(les) != len(want) {
		t.Errorf("%d le labels, want %d", len(les), len(want))
	}
}

func TestEndpointFamiliesOnlyOnRequest(t *testing.T) {
	h := metricsHandlerOf(scriptedMetrics(t, 3))
	plain := scrape(t, h, false)
	if plain[famEndpointRequests] != nil || plain[famEndpointLongestGap] != nil {
		t.Errorf("per-endpoint families present without ?endpoints=1")
	}
	with := scrape(t, h, true)
	if n := len(with[famEndpointRequests].GetMetric()); n != 6 {
		t.Errorf("%d endpoint_requests series, want 3 endpoints x 2 results", n)
	}
	if n := len(with[famEndpointLongestGap].GetMetric()); n != 3 {
		t.Errorf("%d endpoint_longest_gap series, want 3", n)
	}
	if plain[famRequests] == nil || with[famRequests] == nil {
		t.Errorf("requests family missing")
	}
}

func TestEndpointFamiliesAreCompleteAtLargeC(t *testing.T) {
	// 1000 endpoints need 2000 endpoint_requests sets, the SDK's default
	// cardinality limit; 1024 is the most endpoints the actor accepts.
	for _, c := range []int{1000, 1024} {
		t.Run(strconv.Itoa(c), func(t *testing.T) {
			m, err := newLoopMetrics(c, "https")
			if err != nil {
				t.Fatal(err)
			}
			defer m.shutdown(context.Background())
			fams := scrape(t, metricsHandlerOf(m), true)
			if n := len(fams[famEndpointRequests].GetMetric()); n != 2*c {
				t.Errorf("%d endpoint_requests series, want %d", n, 2*c)
			}
			if n := len(fams[famEndpointLongestGap].GetMetric()); n != c {
				t.Errorf("%d endpoint_longest_gap series, want %d", n, c)
			}
			for name, mf := range fams {
				for _, metric := range mf.GetMetric() {
					for _, l := range metric.GetLabel() {
						if strings.Contains(l.GetName(), "overflow") {
							t.Errorf("%s carries an overflow series: %v", name, metric.GetLabel())
						}
					}
				}
			}
		})
	}
}

func TestBothEncodingsAgree(t *testing.T) {
	h := metricsHandlerOf(scriptedMetrics(t, 3))
	text, proto := viewOf(t, scrape(t, h, true)), viewOf(t, scrapeProto(t, h, true))
	text.Elapsed, proto.Elapsed = 0, 0 // the one gauge that moves between two scrapes
	if fmt.Sprintf("%+v", text) != fmt.Sprintf("%+v", proto) {
		t.Errorf("text and protobuf scrapes differ:\n%+v\n%+v", text, proto)
	}
}
