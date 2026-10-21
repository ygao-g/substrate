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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/atenet"
)

// TestLeStringsAgree pins the one agreement the aggregation depends on: the
// bucket bounds the actor exposes, after a round trip through the text
// encoding, are the float64 values the driver matches on, and their strings
// are the ones every actor built from this source prints.
// CONTRACT R7.
func TestLeStringsAgree(t *testing.T) {
	m, err := newLoopMetrics(1, "http")
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown(context.Background())
	m.latency.Record(context.Background(), 0.001)
	rec := getText(metricsHandlerOf(m), metricsRoute)
	fams, err := decodeFamilies(rec.Body, "text/plain; version=0.0.4")
	if err != nil {
		t.Fatal(err)
	}
	got := finiteBounds(families(fams).histogram(famLatency))
	if len(got) != len(bucketBounds) {
		t.Fatalf("%d bounds after the text round trip, want %d", len(got), len(bucketBounds))
	}
	for i := range got {
		if got[i] != bucketBounds[i] {
			t.Errorf("bound %d: text round trip gives %v, the driver expects %v (%s)", i, got[i], bucketBounds[i], strconv.FormatFloat(bucketBounds[i], 'g', -1, 64))
		}
	}
	// And the driver refuses nothing the actor emits: adding the scrape to
	// a fixture built from the same bounds is fine.
	sum := families{}
	if err := sum.add(families(fams), "actor"); err != nil {
		t.Fatal(err)
	}
	if err := sum.add(fixtureScrape(t, 1, recordLatencies(time.Millisecond)), "fixture"); err != nil {
		t.Errorf("the actor's scrape and a fixture from bucketBounds do not add up: %v", err)
	}
}

// routedActors stands in for the atenet router in front of real actor
// handlers: it picks the actor named by the target header.
func routedActors(t *testing.T, actors map[string]http.Handler, textOnly bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, name, _ := strings.Cut(r.Header.Get(atenet.TargetActorHeader), "/")
		h, ok := actors[name]
		if !ok {
			http.Error(w, "no such actor", http.StatusBadGateway)
			return
		}
		if textOnly {
			r.Header.Del("Accept")
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// contractCycle runs real actors against a counting target through the real
// scrape client: start, polls, a final scrape with endpoints, stop. It returns
// the driver's aggregate and what the target counted per endpoint.
func contractCycle(t *testing.T, textOnly bool) (families, []int64) {
	t.Helper()
	const actors, c = 3, 4
	var perEndpoint [c]atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		perEndpoint[i].Add(1)
		w.Write([]byte("ok"))
	}))
	t.Cleanup(target.Close)
	// Each endpoint is its own host, as in the cluster, so each gets its own
	// connection; they all dial the one target.
	addrs := map[string]string{}
	for i := range c {
		addrs[fmt.Sprintf("endpoint-%d", i)] = strings.TrimPrefix(target.URL, "http://")
	}
	handlers := map[string]http.Handler{}
	for a := range actors {
		s := newServer()
		s.dial = dialTo(addrs)
		s.endpointURL = func(i int, _ string) string { return fmt.Sprintf("http://endpoint-%d/%d", i, i) }
		handlers[actorName(a)] = s.handler()
	}
	router := routedActors(t, handlers, textOnly)
	rc := newRouterClient(router.URL, "egress-tests", actors)
	ctx := t.Context()
	for a := range actors {
		if err := rc.probeProtocol(ctx, actorName(a)); err != nil {
			t.Fatalf("probe %s: %v", actorName(a), err)
		}
		if err := rc.do(ctx, http.MethodPost, actorName(a), startRoute, startRequest{Endpoints: c, IntervalMs: 2}, nil); err != nil {
			t.Fatalf("start %s: %v", actorName(a), err)
		}
	}
	// Poll until every loop has swept twice.
	deadline := time.Now().Add(10 * time.Second)
	for {
		swept := 0
		for a := range actors {
			f, err := rc.scrape(ctx, actorName(a), false)
			if err != nil {
				t.Fatalf("scrape %s: %v", actorName(a), err)
			}
			if f.gauge(famRounds) >= 2 {
				swept++
			}
		}
		if swept == actors || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The driver's order: final scrape with the endpoint families, then stop.
	sum := families{}
	for a := range actors {
		f, err := rc.scrape(ctx, actorName(a), true)
		if err != nil {
			t.Fatalf("final scrape %s: %v", actorName(a), err)
		}
		if err := rc.do(ctx, http.MethodPost, actorName(a), stopRoute, nil, nil); err != nil {
			t.Fatalf("stop %s: %v", actorName(a), err)
		}
		if err := sum.add(f, actorName(a)); err != nil {
			t.Fatalf("add %s: %v", actorName(a), err)
		}
	}
	counted := make([]int64, c)
	for i := range counted {
		counted[i] = perEndpoint[i].Load()
	}
	return sum, counted
}

// CONTRACT G29.
func TestContractCycleMatchesTheTarget(t *testing.T) {
	sum, counted := contractCycle(t, false)
	var total int64
	for _, n := range counted {
		total += n
	}
	// Each endpoint goroutine can have one request at the target that the
	// actor has not counted yet when the final scrape lands, and the stop
	// cuts it short: the target may count up to one more per endpoint per
	// actor, never less.
	const inFlight = 3 * 4
	if got := sum.counter(famRequests); got > total || got < total-inFlight {
		t.Errorf("driver counted %d requests, target saw %d", got, total)
	}
	if got := sum.counter(famRequests, labelResult, resultSuccess); got != sum.counter(famRequests) {
		t.Errorf("%d of %d requests succeeded, want all", got, sum.counter(famRequests))
	}
	for _, e := range endpointCounts(sum) {
		if e.requests > counted[e.index] || e.requests < counted[e.index]-3 || e.errors != 0 { // one in flight per actor
			t.Errorf("endpoint %d: driver %d requests %d errors, target saw %d", e.index, e.requests, e.errors, counted[e.index])
		}
	}
	if sum.counter(famEndpointsOpened) != 12 || sum.counter(famConnectionsOpened) != 12 {
		t.Errorf("opened %d endpoints over %d connections, want 12 and 12 (3 actors x 4 endpoints, one tunnel each)", sum.counter(famEndpointsOpened), sum.counter(famConnectionsOpened))
	}
	opening, latency := sum.histogram(famOpening), sum.histogram(famLatency)
	if int64(opening.GetSampleCount())+int64(latency.GetSampleCount()) != sum.counter(famRequests, labelResult, resultSuccess) || opening.GetSampleCount() != 12 {
		t.Errorf("opening %d + latency %d samples for %d successes; want 12 openings and the rest latency", opening.GetSampleCount(), latency.GetSampleCount(), sum.counter(famRequests, labelResult, resultSuccess))
	}
	if sum.gauge(famRounds) < 6 {
		t.Errorf("rounds summed over 3 actors = %v, want at least 6", sum.gauge(famRounds))
	}
}

// CONTRACT G23.
func TestBothEncodingsAgreeAcrossTheSeam(t *testing.T) {
	proto, _ := contractCycle(t, false)
	text, _ := contractCycle(t, true)
	// Two separate runs cannot have equal counts; the shape and the
	// per-family arithmetic must. Every family of one is in the other, with
	// the same label sets and, for histograms, the same bounds.
	for name, pf := range proto {
		tf, ok := text[name]
		if !ok {
			t.Errorf("%s in the protobuf aggregate only", name)
			continue
		}
		if pf.GetType() != tf.GetType() || len(pf.GetMetric()) != len(tf.GetMetric()) {
			t.Errorf("%s: protobuf %v x%d, text %v x%d", name, pf.GetType(), len(pf.GetMetric()), tf.GetType(), len(tf.GetMetric()))
		}
		if pf.GetType() == 4 { // HISTOGRAM
			if err := sameBounds(pf.GetMetric()[0].GetHistogram(), tf.GetMetric()[0].GetHistogram()); err != nil {
				t.Errorf("%s: %v", name, err)
			}
		}
	}
	for name := range text {
		if _, ok := proto[name]; !ok {
			t.Errorf("%s in the text aggregate only", name)
		}
	}
	// The invariant the actor keeps holds in both aggregates.
	for enc, f := range map[string]families{"protobuf": proto, "text": text} {
		if int64(f.histogram(famOpening).GetSampleCount()+f.histogram(famLatency).GetSampleCount()) != f.counter(famRequests, labelResult, resultSuccess) {
			t.Errorf("%s: opening + latency != successes", enc)
		}
	}
}

// TestLaterProtocolRefused: an actor that speaks a protocol version this
// driver does not know is refused the same way, naming both versions.
// CONTRACT R6.
func TestLaterProtocolRefused(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.protocol["egress-0"] = "2"
	_, err := r.run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "egress-0") || !strings.Contains(err.Error(), protocolHeader+" 2") || !strings.Contains(err.Error(), "driver "+protocolVersion) {
		t.Fatalf("run = %v, want a failure naming egress-0 and both versions\n%s", err, out)
	}
	if len(fr.starts) != 0 {
		t.Errorf("loops started on %d actors, want none: %v", len(fr.starts), fr.starts)
	}
}

// CONTRACT R6.
func TestOldActorFailsFast(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.oldActor["egress-1"] = true
	rep, err := r.run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "egress-1") || !strings.Contains(err.Error(), protocolHeader) || !strings.Contains(err.Error(), "rebuild") {
		t.Fatalf("run = %v, want a failure naming egress-1 and the missing %s\n%s", err, protocolHeader, out)
	}
	if len(fr.starts) != 0 {
		t.Errorf("loops started on %d actors, want none: %v", len(fr.starts), fr.starts)
	}
	if rep.Start.Attempted != cfg.Parallel || rep.Start.Failed != cfg.Parallel || rep.Stop.Failed != 0 || rep.Suspend.Failed != 0 {
		t.Errorf("start %+v, stop %+v, suspend %+v; want every start failed and everything suspended", rep.Start, rep.Stop, rep.Suspend)
	}
	_ = fmt.Sprint(rep)
}
