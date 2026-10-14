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
	"bytes"
	"context"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// loopFamilies is a final scrape with requests over elapsed, for the rate.
func loopFamilies(requests int64, elapsed time.Duration) families {
	return mustFamilies(func(m *loopMetrics) {
		ctx := context.Background()
		m.requests.Add(ctx, requests, m.success)
		m.elapsed.Record(ctx, elapsed.Seconds())
	})
}

// openedFamilies is an actor's final scrape with n endpoints opened.
func openedFamilies(n int64) families {
	return mustFamilies(func(m *loopMetrics) { m.endpointsOpened.Add(context.Background(), n) })
}

// mustFamilies records on fresh instruments and returns the scrape.
func mustFamilies(record func(m *loopMetrics)) families {
	m, err := newLoopMetrics(1, "http")
	if err != nil {
		panic(err)
	}
	defer m.shutdown(context.Background())
	record(m)
	mfs, err := m.gather(true)
	if err != nil {
		panic(err)
	}
	f := families{}
	for _, mf := range mfs {
		f[mf.GetName()] = mf
	}
	return f
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// series builds cAdvisor samples of one container at the given times, with
// CPU rising at cores.
func series(component, pod, container string, cores float64, at ...time.Duration) []cadvisorSample {
	t0 := time.Unix(1000, 0)
	var out []cadvisorSample
	for _, d := range at {
		out = append(out, cadvisorSample{T: t0.Add(d), Component: component, Pod: pod, Container: container, CPUSeconds: cores * d.Seconds(), WorkingSetBytes: 64 << 20})
	}
	return out
}

// liveSeries builds one process's reads every second from 0 to end, with
// the steady marks at begin and stop.
func liveSeries(component, container, pod string, cores float64, begin, stop, end time.Duration) []liveSample {
	t0 := time.Unix(1000, 0)
	var out []liveSample
	for d := time.Duration(0); d <= end; d += time.Second {
		l := liveSample{T: t0.Add(d), Component: component, Container: container, Pod: pod, ProcessCPUSeconds: cores * d.Seconds()}
		switch d {
		case begin:
			l.Label = markSteadyBegin
		case stop:
			l.Label = markSteadyEnd
		}
		out = append(out, l)
	}
	return out
}

// envoyAt is one gateway read at t0+d with the given counters.
func envoyAt(d time.Duration, counters map[string]float64) envoySample {
	return envoySample{T: time.Unix(1000, 0).Add(d), Pod: "gw", Counters: counters}
}

// testUsageReport is a 60 s steady window at 1000 req/s with cAdvisor
// readings 16 s apart: gateway envoy at 1 core and ext-proc at 0.5, two
// workers at 0.25 each, and kube-dns read once inside steady. The gateway
// held 100 tunnels on 10 connections.
func testUsageReport() *report {
	t0 := time.Unix(1000, 0)
	s := time.Second
	every16 := []time.Duration{0, 16 * s, 32 * s, 48 * s, 64 * s, 80 * s}
	var samples []cadvisorSample
	samples = append(samples, series("gateway", "gw", "envoy", 1, every16...)...)
	samples = append(samples, series("gateway", "gw", "ext-proc", 0.5, every16...)...)
	samples = append(samples, series("gateway", "gw", "POD", 1.5, every16...)...)
	samples = append(samples, series("workers", "w1", "ateom", 0.25, every16...)...)
	samples = append(samples, series("workers", "w2", "ateom", 0.25, every16...)...)
	samples = append(samples, series("dns", "d1", "coredns", 0.01, 0, 40*s, 90*s)...)
	return &report{
		Phases: []phaseMark{{Name: "steady", Start: t0.Add(10 * s), End: t0.Add(70 * s)}},
		Loop:   loopFamilies(60000, 60*s),
		Usage: &usageReport{
			Cadvisor: samples,
			Live:     liveSeries("gateway", "ext-proc", "gw", 0.4, 10*s, 70*s, 80*s),
			Driver:   driverUsage{Steady: usageWindow{CPUSeconds: 1.2, WallSeconds: 60, Cores: 0.02}},
			Envoy: []envoySample{
				envoyAt(10*s, map[string]float64{envoyTunnelsOpened: 40, envoyTunnels: 40, envoyTunnelConns: 40, envoyActorConns: 10, envoyStreams: 40,
					envoyHTTP2Conns: 10, envoyHeapAllocated: 100 << 20}),
				envoyAt(70*s, map[string]float64{envoyTunnelsOpened: 100, envoyTunnels: 100, envoyTunnelConns: 102, envoyActorConns: 10, envoyStreams: 100,
					envoyHTTP2Conns: 10, envoyHeapAllocated: 200 << 20, "cluster.egress_forward_proxy_cleartext.upstream_cx_active": 7}),
			},
		},
	}
}

func TestSummarizeUsage(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	summarizeUsage(rep)
	c := rep.Usage.Components
	gw := c["gateway"]
	if gw == nil {
		t.Fatal("no gateway summary")
	}
	for _, tc := range []struct {
		name      string
		got, want float64
	}{
		{"gateway mean", gw.Steady.CPUCores.Mean, 1.5},
		{"gateway max", gw.Steady.CPUCores.Max, 1.5},
		{"gateway cores per 1000 req/s", gw.Steady.CoresPerKrps, 1.5},
		{"gateway coverage", gw.Steady.Coverage, 48.0 / 60},
		{"envoy mean", gw.Containers["envoy"].CPUCores.Mean, 1},
		{"ext-proc exact", *gw.Containers["ext-proc"].LiveCPUCores, 0.4},
		{"workers mean", c["workers"].Steady.CPUCores.Mean, 0.5},
		{"workers working set", c["workers"].Steady.WorkingSetBytes, 2 * 64 << 20},
		{"driver", c["driver"].Steady.CPUCores.Mean, 0.02},
		{"tunnels opened", rep.Usage.Gateway.TunnelsOpened, 60},
		{"heap per tunnel", rep.Usage.Gateway.HeapPerTunnel, 2 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !near(tc.got, tc.want) {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
	if gw.Pods != 1 || c["workers"].Pods != 2 {
		t.Errorf("pods: gateway %d workers %d, want 1 and 2", gw.Pods, c["workers"].Pods)
	}
	if dns := c["dns"]; !dns.Steady.Insufficient || dns.Steady.CoresPerKrps != 0 {
		t.Errorf("dns steady = %+v, want insufficient with no per-request figure", dns.Steady)
	}
	if _, ok := gw.Containers[podContainer]; ok {
		t.Error("the pod cgroup row is summarized as a container")
	}

	var out bytes.Buffer
	rep.printUsage(&out)
	for _, want := range []string{
		"cpu        steady cores mean/max: gateway 1.50/1.50 workers 0.50/0.50 (cAdvisor: insufficient for dns)",
		"cores per 1000 req/s: gateway 1.50 workers 0.50 (cAdvisor coverage 80%)",
		"steady cores, exact: gateway/ext-proc 0.400 driver 0.020",
		"memory     steady max working set: gateway 128Mi workers 128Mi dns 64Mi; envoy heap allocated max 200Mi (2Mi per tunnel at 100 tunnels)",
		"gateway    tunnels opened +60, open max 100 (tunnel connections max 102); actor connections max 10, streams per connection max 10; h2 connections +0, http1 +0",
		"upstream connections max: mitm_internal 102, egress_forward_proxy 0, egress_forward_proxy_cleartext 7",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("printed usage lacks %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "!! usage") || strings.Contains(out.String(), "refusals") {
		t.Errorf("printed usage flags a clean run:\n%s", out.String())
	}
}

func TestSummarizeGateway(t *testing.T) {
	t.Parallel()
	s := time.Second
	clean := []envoySample{
		envoyAt(0, map[string]float64{envoyTunnelsOpened: 10, envoyHTTP2Conns: 1}),
		envoyAt(10*s, map[string]float64{envoyTunnelsOpened: 110, envoyTunnels: 100, envoyActorConns: 1, envoyStreams: 100, envoyHTTP2Conns: 2}),
	}
	with := func(base []envoySample, last map[string]float64) []envoySample {
		out := slices.Clone(base)
		counters := map[string]float64{}
		for k, v := range out[len(out)-1].Counters {
			counters[k] = v
		}
		for k, v := range last {
			counters[k] = v
		}
		out[len(out)-1] = envoyAt(10*s, counters)
		return out
	}
	for _, tc := range []struct {
		name      string
		samples   []envoySample
		want      gatewaySummary
		wantFlags []string
	}{
		{
			name:    "clean h2 run",
			samples: clean,
			want:    gatewaySummary{TunnelsOpened: 100, TunnelsMax: 100, ActorConnsMax: 1, StreamsPerConnMax: 100, HTTP2Conns: 1, PoolConnsMax: map[string]float64{"mitm_internal": 0, "egress_forward_proxy": 0, "egress_forward_proxy_cleartext": 0}},
		},
		{
			name:      "stream idle timeout cuts are flagged",
			samples:   with(clean, map[string]float64{envoyStreamIdleCuts: 7, envoyConnIdleCuts: 2}),
			want:      gatewaySummary{TunnelsOpened: 100, TunnelsMax: 100, ActorConnsMax: 1, StreamsPerConnMax: 100, HTTP2Conns: 1, StreamIdleCuts: 7, ConnIdleCuts: 2, PoolConnsMax: map[string]float64{"mitm_internal": 0, "egress_forward_proxy": 0, "egress_forward_proxy_cleartext": 0}},
			wantFlags: []string{"the stream idle timeout cut 7 tunnels (http.egress_connect.downstream_rq_idle_timeout)"},
		},
		{
			name:      "more streams per connection than allowed",
			samples:   with(clean, map[string]float64{envoyStreams: 150}),
			want:      gatewaySummary{TunnelsOpened: 100, TunnelsMax: 100, ActorConnsMax: 1, StreamsPerConnMax: 150, HTTP2Conns: 1, PoolConnsMax: map[string]float64{"mitm_internal": 0, "egress_forward_proxy": 0, "egress_forward_proxy_cleartext": 0}},
			wantFlags: []string{"streams per connection reached 150, above the 100 the gateway allows"},
		},
		{
			name: "refusals and an open breaker",
			samples: with(clean, map[string]float64{"cluster.mitm_internal.upstream_cx_overflow": 30, "cluster.mitm_internal.circuit_breakers.default.cx_open": 1,
				"cluster.egress_forward_proxy_cleartext.upstream_rq_active_overflow": 4, "cluster.egress_forward_proxy_cleartext.circuit_breakers.default.rq_open": 1,
				"cluster.egress_forward_proxy_cleartext.upstream_cx_active": 1024}),
			want: gatewaySummary{TunnelsOpened: 100, TunnelsMax: 100, ActorConnsMax: 1, StreamsPerConnMax: 100, HTTP2Conns: 1,
				PoolConnsMax:   map[string]float64{"mitm_internal": 0, "egress_forward_proxy": 0, "egress_forward_proxy_cleartext": 1024},
				Refusals:       map[string]float64{"cluster.mitm_internal.upstream_cx_overflow": 30, "cluster.egress_forward_proxy_cleartext.upstream_rq_active_overflow": 4},
				BreakersOpened: []string{"mitm_internal cx", "egress_forward_proxy_cleartext rq"}},
			wantFlags: []string{"breaker opened: mitm_internal cx, egress_forward_proxy_cleartext rq"},
		},
		{
			name: "counters summed over pods, reads out of order",
			samples: []envoySample{
				{T: time.Unix(1010, 0), Pod: "a", Counters: map[string]float64{envoyTunnelsOpened: 20}},
				{T: time.Unix(1000, 0), Pod: "a", Counters: map[string]float64{envoyTunnelsOpened: 5}},
				{T: time.Unix(1000, 0), Pod: "b", Counters: map[string]float64{envoyTunnelsOpened: 100, envoyTunnels: 3}},
				{T: time.Unix(1010, 0), Pod: "b", Counters: map[string]float64{envoyTunnelsOpened: 107}},
			},
			want: gatewaySummary{TunnelsOpened: 22, TunnelsMax: 3, PoolConnsMax: map[string]float64{"mitm_internal": 0, "egress_forward_proxy": 0, "egress_forward_proxy_cleartext": 0}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, flags := summarizeGateway(tc.samples)
			if got == nil {
				t.Fatal("no summary")
			}
			if !slices.Equal(flags, tc.wantFlags) {
				t.Errorf("flags = %q, want %q", flags, tc.wantFlags)
			}
			for _, f := range []struct {
				name      string
				got, want float64
			}{
				{"TunnelsOpened", got.TunnelsOpened, tc.want.TunnelsOpened},
				{"TunnelsMax", got.TunnelsMax, tc.want.TunnelsMax},
				{"ActorConnsMax", got.ActorConnsMax, tc.want.ActorConnsMax},
				{"StreamsPerConnMax", got.StreamsPerConnMax, tc.want.StreamsPerConnMax},
				{"HTTP2Conns", got.HTTP2Conns, tc.want.HTTP2Conns},
				{"HTTP1Conns", got.HTTP1Conns, tc.want.HTTP1Conns},
				{"StreamIdleCuts", got.StreamIdleCuts, tc.want.StreamIdleCuts},
				{"ConnIdleCuts", got.ConnIdleCuts, tc.want.ConnIdleCuts},
				{"ProtocolErrors", got.ProtocolErrors, tc.want.ProtocolErrors},
			} {
				if f.got != f.want {
					t.Errorf("%s = %v, want %v", f.name, f.got, f.want)
				}
			}
			if !mapsEqualFloat(got.PoolConnsMax, tc.want.PoolConnsMax) || !mapsEqualFloat(got.Refusals, tc.want.Refusals) || !slices.Equal(got.BreakersOpened, tc.want.BreakersOpened) {
				t.Errorf("pools %v refusals %v breakers %v, want %v %v %v", got.PoolConnsMax, got.Refusals, got.BreakersOpened, tc.want.PoolConnsMax, tc.want.Refusals, tc.want.BreakersOpened)
			}
		})
	}
	if g, flags := summarizeGateway(nil); g != nil || flags != nil {
		t.Errorf("summarizeGateway(nil) = %+v, %v, want nothing", g, flags)
	}
}

func mapsEqualFloat(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func TestPrintUsageFlagsAndRefusals(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	rep.Usage.Envoy = append(rep.Usage.Envoy, envoyAt(80*time.Second, map[string]float64{envoyTunnelsOpened: 100, envoyStreamIdleCuts: 3,
		"cluster.mitm_internal.upstream_cx_overflow": 12, "cluster.mitm_internal.circuit_breakers.default.cx_open": 1}))
	summarizeUsage(rep)
	var out bytes.Buffer
	rep.printUsage(&out)
	for _, want := range []string{
		"cut by the stream idle timeout +3, by the connection idle timeout +0, protocol errors +0",
		"refusals: cluster.mitm_internal.upstream_cx_overflow +12",
		"!! usage   the stream idle timeout cut 3 tunnels (http.egress_connect.downstream_rq_idle_timeout)",
		"!! usage   breaker opened: mitm_internal cx",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("printed usage lacks %q:\n%s", want, out.String())
		}
	}
}

func TestSourceWarnings(t *testing.T) {
	t.Parallel()
	sources := []sourceInfo{{Name: "live"}, {Name: "envoy"}, {Name: "cadvisor"}}
	for _, tc := range []struct {
		name   string
		reads  map[string]int
		errors map[string]int
		want   string
	}{
		{name: "all answered", reads: map[string]int{"live/a": 100, "envoy/gw": 100, "cadvisor/n1": 20}},
		{name: "a target that answered nothing", reads: map[string]int{"live/a": 100, "cadvisor/n1": 20}, errors: map[string]int{"envoy/gw": 100}, want: "envoy/gw 0 reads, 100 failed"},
		{name: "a source never read", reads: map[string]int{"live/a": 100, "envoy/gw": 100}, want: "cadvisor no reads"},
		{name: "some reads failed", reads: map[string]int{"live/a": 95, "envoy/gw": 100, "cadvisor/n1": 20}, errors: map[string]int{"live/a": 5}, want: "live/a 95 reads, 5 failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sourceWarnings(&usageReport{Sources: sources, Reads: tc.reads, Errors: tc.errors}); got != tc.want {
				t.Errorf("sourceWarnings = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMaxSum(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(0, 0)
	s := time.Second
	steady := phaseMark{Start: t0, End: t0.Add(20 * s)}
	a := []segment{{from: t0, to: t0.Add(10 * s), cores: 1}, {from: t0.Add(10 * s), to: t0.Add(20 * s), cores: 3}}
	b := []segment{{from: t0.Add(5 * s), to: t0.Add(15 * s), cores: 2}}
	if got := maxSum([][]segment{a, b}, steady); got != 5 {
		t.Errorf("maxSum = %v, want 5: 3 cores from a and 2 from b overlap in [10s, 15s)", got)
	}
}

func TestSettledOf(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	steady := phaseMark{Name: "steady", Start: t0, End: t0.Add(60 * s)}
	// polls builds one poll per 5 s, with new connections and requests at
	// each; carried is what the loops had already counted before steady.
	pollsFrom := func(carried int64, conns []int64) []loopPoint {
		var out []loopPoint
		for i, c := range conns {
			out = append(out, loopPoint{T: t0.Add(time.Duration(5*(i+1)) * s), NewConns: c, Requests: carried + int64(500*(i+1))})
		}
		return out
	}
	polls := func(conns []int64) []loopPoint { return pollsFrom(0, conns) }
	full := []int64{400, 800, 1000, 1000, 1001, 1001, 1001, 1001, 1001, 1001, 1001, 1001}
	for _, tc := range []struct {
		name        string
		cfg         runConfig
		timeline    []loopPoint
		wantOK      bool
		wantStart   time.Duration
		wantRule    string
		wantRate    float64
		wantFloored bool
	}{
		{
			name:      "plateau, one poll after it",
			cfg:       runConfig{Endpoints: 100, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  polls(full),
			wantOK:    true,
			wantStart: 20 * s,
			wantRule:  settledByPlateau,
			wantRate:  100,
		},
		{
			name:        "polls coarser than a round, floored at 5 s",
			cfg:         runConfig{Endpoints: 10, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:    polls(full),
			wantOK:      true,
			wantStart:   5 * s,
			wantRule:    settledByRounds,
			wantRate:    100,
			wantFloored: true,
		},
		{
			name:      "rounds rule past the floor",
			cfg:       runConfig{Endpoints: 40, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  polls(full),
			wantOK:    true,
			wantStart: 6 * s,
			wantRule:  settledByRounds,
			wantRate:  100,
		},
		{
			name:      "no plateau",
			cfg:       runConfig{Endpoints: 100, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  polls([]int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200}),
			wantOK:    true,
			wantStart: 15 * s,
			wantRule:  settledByRounds,
			wantRate:  100,
		},
		{
			// After --settle the counts are far from zero when steady begins,
			// and the window opens between two polls; the rate is still the
			// slope between the polls inside it.
			name:      "requests carried in from the settle wait",
			cfg:       runConfig{Endpoints: 40, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s},
			timeline:  pollsFrom(1710, full),
			wantOK:    true,
			wantStart: 6 * s,
			wantRule:  settledByRounds,
			wantRate:  100,
		},
		{
			name:     "no plateau and no pacing",
			cfg:      runConfig{Endpoints: 100, ProgressInterval: 5 * s},
			timeline: polls([]int64{100, 200, 300}),
		},
		{
			name:     "round outlasts steady",
			cfg:      runConfig{Endpoints: 100, RequestInterval: time.Second, ProgressInterval: 120 * s},
			timeline: polls(full),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w, ok := settledOf(tc.cfg, steady, tc.timeline)
			if ok != tc.wantOK {
				t.Fatalf("settledOf() ok = %v, want %v (window %+v)", ok, tc.wantOK, w)
			}
			if !ok {
				return
			}
			if w.Floored != tc.wantFloored {
				t.Errorf("floored = %v, want %v", w.Floored, tc.wantFloored)
			}
			if got := w.Start.Sub(t0); got != tc.wantStart || w.Rule != tc.wantRule || !near(w.ReqPerS, tc.wantRate) {
				t.Errorf("settled from +%v by %q at %.3f req/s, want +%v by %q at %.3f req/s",
					got, w.Rule, w.ReqPerS, tc.wantStart, tc.wantRule, tc.wantRate)
			}
		})
	}
}

func TestSummarizeUsageSettled(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	t0 := time.Unix(1000, 0)
	s := time.Second
	// A round over 10 endpoints at 1 s takes 10 s; polls every 20 s are
	// too coarse, so the window starts 15 s into steady, at t0+25 s.
	rep.Config = runConfig{Endpoints: 10, RequestInterval: s, ProgressInterval: 20 * s}
	rep.LoopTimeline = []loopPoint{
		{T: t0.Add(30 * s), Requests: 20000, NewConns: 10},
		{T: t0.Add(50 * s), Requests: 40000, NewConns: 10},
		{T: t0.Add(70 * s), Requests: 60000, NewConns: 10},
	}
	summarizeUsage(rep)
	w := rep.Usage.Settled
	if w == nil || !w.Start.Equal(t0.Add(25*s)) || w.Rule != settledByRounds || !near(w.ReqPerS, 1000) {
		t.Fatalf("settled window = %+v, want from t0+25s by %q at 1000 req/s", w, settledByRounds)
	}
	gw := rep.Usage.Components["gateway"].Settled
	if gw == nil || !near(gw.CPUCores.Mean, 1.5) || !near(gw.CoresPerKrps, 1.5) || !near(gw.Coverage, 32.0/45) {
		t.Errorf("gateway settled = %+v, want 1.5 cores, 1.5 cores/krps, coverage 32/45", gw)
	}
	var out bytes.Buffer
	rep.printUsage(&out)
	want := "cores per 1000 req/s, settled: gateway 1.50 workers 0.50 (from steady +15.0s by 1.5 rounds, 1000.0 req/s)"
	if !strings.Contains(out.String(), want) {
		t.Errorf("printed usage lacks %q:\n%s", want, out.String())
	}
}

func TestSummarizeUsageBaseline(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	for _, tc := range []struct {
		name    string
		preIdle time.Duration
		want    map[string]float64 // key -> mean cores; nil keys must be insufficient
		line    string
	}{
		// Pre-idle covers the readings at 0 and 16 s, before steady at
		// 10 to 70 s would start in a real run; the window is all that
		// matters here.
		{name: "two readings", preIdle: 16 * s, want: map[string]float64{"gateway": 1.5, "gateway/envoy": 1, "gateway/ext-proc": 0.5, "workers": 0.5},
			line: "pre-idle baseline cores: gateway 1.500 workers 0.500"},
		{name: "too short for a second reading", preIdle: 5 * s, line: "pre-idle baseline cores:  (insufficient for gateway,workers,dns)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rep := testUsageReport()
			rep.Phases = append([]phaseMark{{Name: "pre-idle", Start: t0, End: t0.Add(tc.preIdle)}}, rep.Phases...)
			summarizeUsage(rep)
			b := rep.Usage.Baseline
			for key, want := range tc.want {
				if b[key] == nil || !near(b[key].CPUCores.Mean, want) {
					t.Errorf("baseline[%s] = %+v, want %v cores", key, b[key], want)
				}
			}
			if _, ok := b["gateway/POD"]; ok {
				t.Error("baseline holds the pod cgroup as a container")
			}
			var out bytes.Buffer
			rep.printUsage(&out)
			if !strings.Contains(out.String(), tc.line) {
				t.Errorf("printed usage lacks %q:\n%s", tc.line, out.String())
			}
		})
	}
	none := testUsageReport()
	summarizeUsage(none)
	if none.Usage.Baseline != nil {
		t.Errorf("baseline without a pre-idle phase = %v, want nil", none.Usage.Baseline)
	}
}

// TestSettledLatency checks that the settled p50 and p99 count only the
// requests after the window starts: the first round's slow requests, with
// their DNS lookups and connects, stay out.
func TestSettledLatency(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	steady := phaseMark{Name: "steady", Start: t0, End: t0.Add(60 * s)}
	cfg := runConfig{Endpoints: 100, RequestInterval: 100 * time.Millisecond, ProgressInterval: 5 * s}
	// One actor's instruments stand in for the fleet; each poll is a copy of
	// its cumulative latency histogram so far.
	m, err := newLoopMetrics(1, "http")
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown(context.Background())
	var polls []loopPoint
	conns := []int64{400, 800, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000}
	for i, c := range conns {
		// 500 requests per poll: 50 ms each while connections open, 3 ms after.
		d := 3 * time.Millisecond
		if i < 3 {
			d = 50 * time.Millisecond
		}
		for range 500 {
			m.latency.Record(context.Background(), d.Seconds())
		}
		mfs, err := m.gather(false)
		if err != nil {
			t.Fatal(err)
		}
		f := families{}
		for _, mf := range mfs {
			f[mf.GetName()] = proto.Clone(mf).(*dto.MetricFamily)
		}
		polls = append(polls, loopPoint{T: t0.Add(time.Duration(5*(i+1)) * s), NewConns: c, Requests: int64(500 * (i + 1)), Latency: f.histogram(famLatency), Polled: 10, Answered: 10})
	}
	w, ok := settledOf(cfg, steady, polls)
	if !ok {
		t.Fatal("no settled window")
	}
	// The window starts at the 20 s poll, after the 50 ms requests.
	if w.P99 < 3*time.Millisecond || w.P99 > 4*time.Millisecond || w.P50 > w.P99 {
		t.Errorf("settled p50 %v p99 %v, want both in the 3 ms bucket", w.P50, w.P99)
	}
	if whole := durationOf(quantile(polls[len(polls)-1].Latency, 0.99)); whole < 40*time.Millisecond {
		t.Errorf("whole-run p99 %v, want the first round's 50 ms to show there", whole)
	}

	// A poll that missed an actor holds less than the whole, so a delta
	// against it would count that actor's earlier requests, first round
	// included. The window must open on the next complete poll instead.
	partial := slices.Clone(polls)
	partial[3].Answered = 9
	partial[3].Requests -= 200
	partial[3].Latency = proto.Clone(polls[0].Latency).(*dto.Histogram)
	pw, ok := settledOf(cfg, steady, partial)
	if !ok {
		t.Fatal("no settled window with a partial poll")
	}
	if pw.P99 < 3*time.Millisecond || pw.P99 > 4*time.Millisecond || !near(pw.ReqPerS, w.ReqPerS) {
		t.Errorf("settled p99 %v, %.1f req/s with a partial poll, want the 3 ms bucket and %.1f req/s as without it", pw.P99, pw.ReqPerS, w.ReqPerS)
	}
}

func TestSteadyRequestRateUsesTheSteadyPolls(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	rep := &report{
		Phases: []phaseMark{{Name: "settle", Start: t0.Add(-90 * s), End: t0}, {Name: "steady", Start: t0, End: t0.Add(60 * s)}},
		// 1710 requests during the settle wait, then 100 req/s.
		Loop: loopFamilies(1710+6000, 150*s),
	}
	for i := range 12 {
		rep.LoopTimeline = append(rep.LoopTimeline, loopPoint{T: t0.Add(time.Duration(5*(i+1)) * s), Requests: 1710 + int64(500*(i+1)), Polled: 2, Answered: 2})
	}
	if got := steadyRequestRate(rep); !near(got, 100) {
		t.Errorf("steadyRequestRate() = %.2f, want 100 from the steady polls, not %.2f over the whole loop", got, float64(rep.Loop.counter(famRequests))/150)
	}
	rep.LoopTimeline = rep.LoopTimeline[:1]
	if got, want := steadyRequestRate(rep), float64(7710)/150; !near(got, want) {
		t.Errorf("steadyRequestRate() with one poll = %.2f, want the whole-loop fallback %.2f", got, want)
	}
}

// TestSettledRateStartsAtTheFirstPollInTheWindow uses the shape a --settle
// run has: 30 s polls, counts far from zero when steady begins, and a
// window that opens before the first poll inside steady. The rate must be
// the slope between the polls in the window, not a line from zero.
func TestSettledRateStartsAtTheFirstPollInTheWindow(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1000, 0)
	s := time.Second
	steady := phaseMark{Name: "steady", Start: t0, End: t0.Add(120 * s)}
	cfg := runConfig{Endpoints: 20, RequestInterval: 100 * time.Millisecond, ProgressInterval: 30 * s}
	var polls []loopPoint
	for i := 1; i <= 4; i++ { // 18 req/s on top of 1710 requests from the settle wait
		polls = append(polls, loopPoint{T: t0.Add(time.Duration(30*i) * s), NewConns: 40, Requests: 1710 + int64(540*i), Polled: 2, Answered: 2})
	}
	w, ok := settledOf(cfg, steady, polls)
	if !ok {
		t.Fatal("no settled window")
	}
	if w.Start.After(polls[0].T) {
		t.Fatalf("window starts at +%v, after the first poll; the test needs it before", w.Start.Sub(t0))
	}
	if !near(w.ReqPerS, 18) {
		t.Errorf("settled rate = %.2f req/s, want 18 (the slope between the polls in the window)", w.ReqPerS)
	}
}

func TestSummarizeUsageOpening(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	t0 := time.Unix(1000, 0)
	s := time.Second
	// Two batches opened 100 tunnels over 64 s, the first batch's 20 of
	// them alone in the first 32 s.
	rep.Phases = append(rep.Phases,
		phaseMark{Name: "opening", Start: t0, End: t0.Add(64 * s)},
		phaseMark{Name: "opening-pure", Start: t0, End: t0.Add(32 * s)})
	rep.Opening = &openingResult{Start: t0, End: t0.Add(64 * s), PureEnd: t0.Add(32 * s), Opens: 100}
	rep.Start.Batches = []batchMark{{Index: 0, Actors: 1, At: t0}, {Index: 1, Actors: 1, At: t0.Add(32 * s)}}
	rep.Actors = []actorResult{
		{Name: "egress-0", Batch: 0, StartedAt: t0, Metrics: openedFamilies(20)},
		{Name: "egress-1", Batch: 1, StartedAt: t0.Add(32 * s), Metrics: openedFamilies(80)},
	}
	summarizeUsage(rep)
	gw := rep.Usage.Components["gateway"]
	if gw.Opening == nil || gw.OpeningPure == nil {
		t.Fatalf("gateway = %+v, want opening and pure-opening summaries", gw)
	}
	for _, tc := range []struct {
		name      string
		got, want float64
	}{
		{"opening mean", gw.Opening.CPUCores.Mean, 1.5},
		{"opening coverage", gw.Opening.Coverage, 1},
		{"opening ms per open", gw.Opening.MsPerOpen, 1.5 * 64 / 100 * 1000},
		{"pure mean", gw.OpeningPure.CPUCores.Mean, 1.5},
		{"pure ms per open", gw.OpeningPure.MsPerOpen, 1.5 * 32 / 20 * 1000},
		{"workers opening mean", rep.Usage.Components["workers"].Opening.CPUCores.Mean, 0.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !near(tc.got, tc.want) {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
	var out bytes.Buffer
	rep.printUsage(&out)
	for _, want := range []string{
		"cores over opening: gateway 1.50 workers 0.50 dns 0.01 (1m4s, 100 opens; gateway 960.0 ms per tunnel opened)",
		"cores over the pure opening window: gateway 1.50 workers 0.50 (32s, 20 opens; gateway 2400.0 ms per tunnel opened)",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("printed usage lacks %q:\n%s", want, out.String())
		}
	}
}

func TestSummarizeUsagePurity(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	t0 := time.Unix(1000, 0)
	s := time.Second
	rep.Phases = append(rep.Phases,
		phaseMark{Name: "opening", Start: t0, End: t0.Add(64 * s)},
		phaseMark{Name: "opening-pure", Start: t0, End: t0.Add(32 * s)})
	rep.Opening = &openingResult{Start: t0, End: t0.Add(64 * s), PureEnd: t0.Add(32 * s), Opens: 100}
	rep.Start.Batches = []batchMark{{Index: 0, Actors: 1, At: t0}, {Index: 1, Actors: 1, At: t0.Add(32 * s)}}
	rep.Actors = []actorResult{
		{Name: "egress-0", Batch: 0, StartedAt: t0, Metrics: openedFamilies(20)},
		{Name: "egress-1", Batch: 1, StartedAt: t0.Add(32 * s), Metrics: openedFamilies(80)},
	}
	// The poll nearest the pure window's end saw 25 requests, 20 of them
	// opens; the later poll is further from it.
	rep.LoopTimeline = []loopPoint{
		{T: t0.Add(31 * s), Requests: 25, Opened: 20, Polled: 1, Answered: 1},
		{T: t0.Add(40 * s), Requests: 200, Opened: 60, Polled: 1, Answered: 1},
	}
	summarizeUsage(rep)
	gw := rep.Usage.Components["gateway"]
	if gw.OpeningPure == nil || !near(gw.OpeningPure.Purity, 0.8) {
		t.Errorf("pure window = %+v, want a purity of 0.8", gw.OpeningPure)
	}
	if gw.Opening == nil || gw.Opening.Purity != 0 {
		t.Errorf("opening = %+v, want no purity outside the pure window", gw.Opening)
	}
	var out bytes.Buffer
	rep.printUsage(&out)
	if want := "cores over the pure opening window: gateway 1.50 workers 0.50 (32s, 20 opens, 80% opens; gateway 2400.0 ms per tunnel opened)"; !strings.Contains(out.String(), want) {
		t.Errorf("printed usage lacks %q:\n%s", want, out.String())
	}
}

func TestPrintUsageSaysWhenTheOpeningIsTooShortToCost(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	t0 := time.Unix(1000, 0)
	// The samples are 16 s apart, so neither window holds two of them.
	rep.Phases = append(rep.Phases,
		phaseMark{Name: "opening", Start: t0, End: t0.Add(1900 * time.Millisecond)},
		phaseMark{Name: "opening-pure", Start: t0, End: t0.Add(900 * time.Millisecond)})
	rep.Opening = &openingResult{Start: t0, End: t0.Add(1900 * time.Millisecond), PureEnd: t0.Add(900 * time.Millisecond), Opens: 100}
	rep.Start.Batches = []batchMark{{Index: 0, Actors: 1, At: t0}, {Index: 1, Actors: 1, At: t0.Add(time.Second)}}
	summarizeUsage(rep)
	gw := rep.Usage.Components["gateway"]
	if gw.Opening == nil || !gw.Opening.Insufficient || gw.Opening.MsPerOpen != 0 {
		t.Errorf("gateway opening = %+v, want it insufficient with no cost", gw.Opening)
	}
	var out bytes.Buffer
	rep.printUsage(&out)
	for _, want := range []string{
		"cores over opening: window 1.9s too short for two cAdvisor readings, cost per tunnel opened not measured",
		"cores over the pure opening window: window 900ms too short for two cAdvisor readings, cost per tunnel opened not measured",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("printed usage lacks %q:\n%s", want, out.String())
		}
	}
}

func TestSummarizeUsageWithoutOpeningMarks(t *testing.T) {
	t.Parallel()
	rep := testUsageReport()
	summarizeUsage(rep)
	if gw := rep.Usage.Components["gateway"]; gw.Opening != nil || gw.OpeningPure != nil {
		t.Errorf("gateway = %+v, want no opening summaries without the marks", gw)
	}
	var out bytes.Buffer
	rep.printUsage(&out)
	if strings.Contains(out.String(), "opening") {
		t.Errorf("printed usage mentions the opening without its marks:\n%s", out.String())
	}
}
