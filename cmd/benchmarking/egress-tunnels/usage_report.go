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
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"
)

// componentOrder is the order components are printed in.
var componentOrder = []string{"gateway", "workers", "ateapi", "targets", "dns", "router"}

// componentSummary is what one component used over the run's windows.
type componentSummary struct {
	Pods   int           `json:"pods"`
	Steady windowSummary `json:"steady"`
	// Settled is the same summary over the settled window.
	Settled *windowSummary `json:"settled,omitempty"`
	// Opening and OpeningPure are the same over the opening and, with
	// batched starts, the first batch's window of opens alone.
	Opening     *windowSummary            `json:"opening,omitempty"`
	OpeningPure *windowSummary            `json:"openingPure,omitempty"`
	Containers  map[string]*windowSummary `json:"containers,omitempty"`
}

// windowSummary is a component's or container's use over one window.
type windowSummary struct {
	CPUCores     meanMax `json:"cpuCores"`
	CoresPerKrps float64 `json:"coresPerKrps,omitempty"`
	// MsPerOpen is the CPU spent per tunnel opened over an opening window:
	// mean cores times the window's seconds over its opens, in ms.
	MsPerOpen      float64 `json:"msPerOpen,omitempty"`
	ThrottledRatio float64 `json:"throttledRatio"`
	// WorkingSetBytes is the sum over pods of each one's maximum in the
	// window.
	WorkingSetBytes float64 `json:"workingSetBytes"`
	// Coverage is the shortest fraction of the window any series' readings
	// span. Insufficient is set when a series has fewer than two inside it.
	Coverage     float64 `json:"coverage"`
	Insufficient bool    `json:"insufficient,omitempty"`
	// LiveCPUCores is the exact mean from the process counters read at the
	// steady marks; only the Go processes and the driver have it.
	LiveCPUCores *float64 `json:"liveCpuCores,omitempty"`
}

type meanMax struct {
	Mean float64 `json:"mean"`
	Max  float64 `json:"max"`
}

// segment is the usage between two consecutive readings of one series.
type segment struct {
	from, to          time.Time
	cores             float64
	periods, throttle float64
}

// Rules that can start the settled window.
const (
	settledByPlateau = "new-connection plateau"
	settledByRounds  = "1.5 rounds"
)

// settledWindow is steady minus the first full round over the endpoints,
// whose connection setup and DNS lookups inflate per-request cost.
type settledWindow struct {
	Start time.Time `json:"start"`
	// Rule says which rule started the window.
	Rule    string  `json:"rule"`
	ReqPerS float64 `json:"reqPerS"`
	// P50 and P99 are the latency quantiles of the requests between the
	// first progress poll at or after Start and the last one, by the loop
	// histograms' buckets.
	P50 time.Duration `json:"p50,omitempty"`
	P99 time.Duration `json:"p99,omitempty"`
	// Floored is set when the rule's start fell before settledFloor and the
	// window starts at the floor instead.
	Floored bool `json:"floored,omitempty"`
}

// settledFloor is the earliest the settled window starts: at small C the
// rounds rule lands inside the first seconds of steady, while B actors are
// still opening their connections at once.
const settledFloor = 5 * time.Second

// settledPlateauTolerance is the growth in new connections, as a fraction
// of those already open, that still counts as a plateau; reconnects after
// a timeout add a few.
const settledPlateauTolerance = 0.01

// settledOf finds the settled window. It starts one progress poll after
// the first poll after which new connections stop growing. When polls come
// further apart than one round over the endpoints, or no plateau shows,
// it starts 1.5 rounds into steady instead.
func settledOf(cfg runConfig, steady phaseMark, timeline []loopPoint) (settledWindow, bool) {
	// Only complete polls: one that missed an actor holds less than the
	// whole, and a delta against it would count that actor's earlier
	// requests, first round included.
	var polls []loopPoint
	for _, p := range timeline {
		if !p.T.Before(steady.Start) && !p.T.After(steady.End) && p.Answered == p.Polled {
			polls = append(polls, p)
		}
	}
	round := time.Duration(cfg.Endpoints) * cfg.RequestInterval
	var w settledWindow
	if cfg.ProgressInterval <= round {
		for i := 0; i+1 < len(polls); i++ {
			grew := polls[i+1].NewConns - polls[i].NewConns
			if polls[i].NewConns > 0 && float64(grew) <= settledPlateauTolerance*float64(polls[i].NewConns) {
				w = settledWindow{Start: polls[i+1].T, Rule: settledByPlateau}
				break
			}
		}
	}
	if w.Rule == "" {
		if round <= 0 {
			return w, false
		}
		w = settledWindow{Start: steady.Start.Add(round * 3 / 2), Rule: settledByRounds}
	}
	if floor := steady.Start.Add(settledFloor); w.Start.Before(floor) {
		w.Start, w.Floored = floor, true
	}
	if len(polls) == 0 {
		return w, false
	}
	// The rate and the quantiles come from the first complete poll inside
	// the window to the last one. The counts are cumulative from the loop
	// start, not from steady, so only a delta between two polls is a rate.
	first, last, ok := pollSpan(polls, w.Start)
	if !ok {
		return w, false
	}
	w.ReqPerS = float64(last.Requests-first.Requests) / last.T.Sub(first.T).Seconds()
	if d := histogramDelta(last.Latency, first.Latency); d.Count > 0 {
		w.P50, w.P99 = d.Quantile(0.5), d.Quantile(0.99)
	}
	return w, true
}

// pollSpan returns the first poll at or after start and the last poll, when
// they are two different polls.
func pollSpan(polls []loopPoint, start time.Time) (first, last loopPoint, ok bool) {
	for _, p := range polls {
		if !p.T.Before(start) {
			last = polls[len(polls)-1]
			return p, last, last.T.After(p.T)
		}
	}
	return first, last, false
}

// steadyRequestRate is the loops' merged request rate over the steady
// phase, from its complete polls. With fewer than two it falls back to the
// whole loop, which also spans the start and settle phases.
func steadyRequestRate(rep *report) float64 {
	steady := rep.phase("steady")
	var polls []loopPoint
	for _, p := range rep.LoopTimeline {
		if !p.T.Before(steady.Start) && !p.T.After(steady.End) && p.Answered == p.Polled {
			polls = append(polls, p)
		}
	}
	if first, last, ok := pollSpan(polls, steady.Start); ok {
		return float64(last.Requests-first.Requests) / last.T.Sub(first.T).Seconds()
	}
	if rep.Loop == nil || rep.Loop.Elapsed <= 0 {
		return 0
	}
	return float64(rep.Loop.Requests) / rep.Loop.Elapsed.Seconds()
}

// summarizeUsage fills the summaries of rep.Usage from its raw readings.
func summarizeUsage(rep *report) {
	res := rep.Usage
	steady := rep.phase("steady")
	preIdle := rep.phase("pre-idle")
	res.Components = map[string]*componentSummary{}
	res.Baseline = nil
	if !preIdle.End.IsZero() {
		res.Baseline = map[string]*windowSummary{}
	}
	var settled phaseMark
	if !steady.End.IsZero() {
		if w, ok := settledOf(rep.Config, steady, rep.LoopTimeline); ok {
			res.Settled = &w
			settled = phaseMark{Name: "settled", Start: w.Start, End: steady.End}
		}
	}
	opening, pure := rep.phase("opening"), rep.phase("opening-pure")
	bySeries := map[string][]cadvisorSample{}
	for _, s := range res.Cadvisor {
		bySeries[s.key()] = append(bySeries[s.key()], s)
	}
	type compAcc struct {
		segs, settledSegs, openingSegs, pureSegs [][]segment
		pods                                     map[string]bool
		periods, throttle                        float64
	}
	acc := map[string]*compAcc{}
	for _, key := range slices.Sorted(maps.Keys(bySeries)) {
		ss := bySeries[key]
		slices.SortFunc(ss, func(a, b cadvisorSample) int { return a.T.Compare(b.T) })
		comp, ctr := ss[0].Component, ss[0].Container
		var segs []segment
		for i := 1; i < len(ss); i++ {
			a, b := ss[i-1], ss[i]
			dt := b.T.Sub(a.T).Seconds()
			if dt <= 0 {
				continue
			}
			segs = append(segs, segment{from: a.T, to: b.T, cores: (b.CPUSeconds - a.CPUSeconds) / dt,
				periods: b.CFSPeriods - a.CFSPeriods, throttle: b.CFSThrottled - a.CFSThrottled})
		}
		// The pod cgroup row is the sum of the containers; the containers
		// carry the summaries.
		if steady.End.IsZero() || ctr == podContainer {
			continue
		}
		if res.Baseline != nil {
			b := windowOf(ss, segs, preIdle).windowSummary
			for _, k := range []string{comp, comp + "/" + ctr} {
				if res.Baseline[k] == nil {
					res.Baseline[k] = &windowSummary{Coverage: 1}
				}
				addTo(res.Baseline[k], b)
			}
		}
		sum := windowOf(ss, segs, steady)
		cs := res.Components[comp]
		if cs == nil {
			cs = &componentSummary{Containers: map[string]*windowSummary{}, Steady: windowSummary{Coverage: 1}}
			res.Components[comp] = cs
			acc[comp] = &compAcc{pods: map[string]bool{}}
		}
		a := acc[comp]
		a.pods[ss[0].Pod] = true
		a.segs = append(a.segs, inside(segs, steady))
		a.periods += sum.periods
		a.throttle += sum.throttle
		addTo(&cs.Steady, sum.windowSummary)
		if res.Settled != nil {
			if cs.Settled == nil {
				cs.Settled = &windowSummary{Coverage: 1}
			}
			a.settledSegs = append(a.settledSegs, inside(segs, settled))
			addTo(cs.Settled, windowOf(ss, segs, settled).windowSummary)
		}
		if !opening.End.IsZero() {
			if cs.Opening == nil {
				cs.Opening = &windowSummary{Coverage: 1}
			}
			a.openingSegs = append(a.openingSegs, inside(segs, opening))
			addTo(cs.Opening, windowOf(ss, segs, opening).windowSummary)
		}
		if !pure.End.IsZero() {
			if cs.OpeningPure == nil {
				cs.OpeningPure = &windowSummary{Coverage: 1}
			}
			a.pureSegs = append(a.pureSegs, inside(segs, pure))
			addTo(cs.OpeningPure, windowOf(ss, segs, pure).windowSummary)
		}
		if cs.Containers[ctr] == nil {
			cs.Containers[ctr] = &windowSummary{Coverage: 1}
		}
		addTo(cs.Containers[ctr], sum.windowSummary)
	}
	for comp, cs := range res.Components {
		a := acc[comp]
		if a == nil {
			continue
		}
		cs.Pods = len(a.pods)
		cs.Steady.CPUCores.Max = maxSum(a.segs, steady)
		if cs.Settled != nil {
			cs.Settled.CPUCores.Max = maxSum(a.settledSegs, settled)
		}
		if cs.Opening != nil {
			cs.Opening.CPUCores.Max = maxSum(a.openingSegs, opening)
		}
		if cs.OpeningPure != nil {
			cs.OpeningPure.CPUCores.Max = maxSum(a.pureSegs, pure)
		}
		if a.periods > 0 {
			cs.Steady.ThrottledRatio = a.throttle / a.periods
		}
	}
	addLive(rep)
	if rps := steadyRequestRate(rep); rps > 0 {
		for _, cs := range res.Components {
			if !cs.Steady.Insufficient {
				cs.Steady.CoresPerKrps = cs.Steady.CPUCores.Mean / (rps / 1000)
			}
		}
	}
	if res.Settled != nil && res.Settled.ReqPerS > 0 {
		for _, cs := range res.Components {
			if cs.Settled != nil && !cs.Settled.Insufficient {
				cs.Settled.CoresPerKrps = cs.Settled.CPUCores.Mean / (res.Settled.ReqPerS / 1000)
			}
		}
	}
	if o := rep.Opening; o != nil {
		for _, cs := range res.Components {
			setMsPerOpen(cs.Opening, opening, o.Opens)
			setMsPerOpen(cs.OpeningPure, pure, rep.openedInBatch(0))
		}
	}
	res.Gateway, res.Flags = summarizeGateway(res.Envoy)
}

// setMsPerOpen fills w's CPU per tunnel opened over window.
func setMsPerOpen(w *windowSummary, window phaseMark, opens int64) {
	if w == nil || w.Insufficient || opens <= 0 {
		return
	}
	w.MsPerOpen = w.CPUCores.Mean * window.End.Sub(window.Start).Seconds() / float64(opens) * 1000
}

// openedInBatch is how many endpoints the loops started in batch b opened.
func (rep *report) openedInBatch(b int) int64 {
	var n int64
	for _, a := range rep.Actors {
		if a.Batch == b && !a.StartedAt.IsZero() && a.Stats != nil {
			n += a.Stats.Opened
		}
	}
	return n
}

type seriesWindow struct {
	windowSummary
	periods, throttle float64
}

// windowOf summarizes one series over the readings inside window.
func windowOf(ss []cadvisorSample, segs []segment, window phaseMark) seriesWindow {
	var in []cadvisorSample
	for _, s := range ss {
		if !s.T.Before(window.Start) && !s.T.After(window.End) {
			in = append(in, s)
		}
	}
	var out seriesWindow
	for _, s := range in {
		out.WorkingSetBytes = max(out.WorkingSetBytes, s.WorkingSetBytes)
	}
	if len(in) < 2 {
		out.Insufficient = true
		return out
	}
	first, last := in[0], in[len(in)-1]
	span := last.T.Sub(first.T)
	if span <= 0 {
		out.Insufficient = true
		return out
	}
	out.CPUCores.Mean = (last.CPUSeconds - first.CPUSeconds) / span.Seconds()
	out.Coverage = span.Seconds() / window.End.Sub(window.Start).Seconds()
	out.periods, out.throttle = last.CFSPeriods-first.CFSPeriods, last.CFSThrottled-first.CFSThrottled
	if out.periods > 0 {
		out.ThrottledRatio = out.throttle / out.periods
	}
	for _, sg := range inside(segs, window) {
		out.CPUCores.Max = max(out.CPUCores.Max, sg.cores)
	}
	return out
}

// inside keeps the segments that lie wholly within window.
func inside(segs []segment, window phaseMark) []segment {
	var out []segment
	for _, s := range segs {
		if !s.from.Before(window.Start) && !s.to.After(window.End) {
			out = append(out, s)
		}
	}
	return out
}

// addTo adds one series' summary into a sum over series.
func addTo(dst *windowSummary, s windowSummary) {
	dst.CPUCores.Mean += s.CPUCores.Mean
	dst.CPUCores.Max += s.CPUCores.Max
	dst.WorkingSetBytes += s.WorkingSetBytes
	dst.Insufficient = dst.Insufficient || s.Insufficient
	if !s.Insufficient {
		dst.Coverage = min(dst.Coverage, s.Coverage)
	} else {
		dst.Coverage = 0
	}
	if s.ThrottledRatio > dst.ThrottledRatio {
		dst.ThrottledRatio = s.ThrottledRatio
	}
}

// maxSum is the highest summed rate over every second of window, where
// each series contributes the rate of the segment covering that second.
func maxSum(series [][]segment, window phaseMark) float64 {
	var best float64
	for t := window.Start; !t.After(window.End); t = t.Add(time.Second) {
		var sum float64
		for _, segs := range series {
			for _, s := range segs {
				if !t.Before(s.from) && t.Before(s.to) {
					sum += s.cores
					break
				}
			}
		}
		best = max(best, sum)
	}
	return best
}

// addLive adds the Go processes' exact steady CPU, from the reads forced at
// the steady marks, and the driver's own.
func addLive(rep *report) {
	res := rep.Usage
	byKey := map[string][]liveSample{}
	for _, l := range res.Live {
		k := l.Component + "/" + l.Container + "@" + l.Pod
		byKey[k] = append(byKey[k], l)
	}
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		ls := byKey[k]
		cores, ok := liveSteadyCores(ls)
		if !ok {
			continue
		}
		comp, ctr := ls[0].Component, ls[0].Container
		cs := res.Components[comp]
		if cs == nil {
			cs = &componentSummary{Containers: map[string]*windowSummary{}, Steady: windowSummary{Insufficient: true}}
			res.Components[comp] = cs
		}
		c := cs.Containers[ctr]
		if c == nil {
			c = &windowSummary{Insufficient: true}
			cs.Containers[ctr] = c
		}
		c.LiveCPUCores = addPtr(c.LiveCPUCores, cores)
	}
	if d := res.Driver.Steady; d.WallSeconds > 0 {
		cores := d.Cores
		res.Components["driver"] = &componentSummary{Steady: windowSummary{CPUCores: meanMax{Mean: cores, Max: cores}, Coverage: 1, LiveCPUCores: &cores}}
	}
}

func addPtr(p *float64, v float64) *float64 {
	if p != nil {
		v += *p
	}
	return &v
}

// liveSteadyCores is a process's mean CPU between the steady marks.
func liveSteadyCores(ls []liveSample) (float64, bool) {
	var a, b *liveSample
	for i := range ls {
		switch ls[i].Label {
		case markSteadyBegin:
			a = &ls[i]
		case markSteadyEnd:
			b = &ls[i]
		}
	}
	if a == nil || b == nil || !b.T.After(a.T) {
		return 0, false
	}
	return (b.ProcessCPUSeconds - a.ProcessCPUSeconds) / b.T.Sub(a.T).Seconds(), true
}

// maxStreamsPerConn is the gateway's max_concurrent_streams: the most
// CONNECT streams one actor connection carries.
const maxStreamsPerConn = 100

// gatewaySummary is what the gateway's counters say about the run, summed
// over the gateway's pods: counters as their rise from the first read to
// the last, gauges as their peak.
type gatewaySummary struct {
	// TunnelsOpened is the rise of mitm_internal.upstream_cx_total: the
	// CONNECTs that got a tunnel.
	TunnelsOpened float64 `json:"tunnelsOpened"`
	// TunnelsMax is the peak of open tunnels (mitm_internal.upstream_rq_active).
	// TunnelConnsMax is the peak of their connections (upstream_cx_active),
	// which can run a little higher while released connections linger idle.
	TunnelsMax     float64 `json:"tunnelsMax"`
	TunnelConnsMax float64 `json:"tunnelConnsMax"`
	// ActorConnsMax is the peak of actor-to-gateway connections;
	// StreamsPerConnMax the peak ratio of CONNECT streams to them.
	ActorConnsMax     float64 `json:"actorConnsMax"`
	StreamsPerConnMax float64 `json:"streamsPerConnMax"`
	// HTTP2Conns and HTTP1Conns count the actor connections negotiated as
	// each protocol.
	HTTP2Conns float64 `json:"http2Conns"`
	HTTP1Conns float64 `json:"http1Conns"`
	// StreamIdleCuts counts tunnels the stream idle timeout reset,
	// ConnIdleCuts actor connections the connection idle timeout closed, and
	// ProtocolErrors actor connections closed on a protocol error.
	StreamIdleCuts float64 `json:"streamIdleCuts"`
	ConnIdleCuts   float64 `json:"connIdleCuts"`
	ProtocolErrors float64 `json:"protocolErrors"`
	// PoolConnsMax is each cluster's peak of open upstream connections.
	PoolConnsMax map[string]float64 `json:"poolConnsMax"`
	// Refusals holds the overflow counters that rose, by report key.
	Refusals map[string]float64 `json:"refusals,omitempty"`
	// BreakersOpened names the clusters whose connection or request breaker
	// was open at a read, as "<cluster> cx" or "<cluster> rq".
	BreakersOpened []string `json:"breakersOpened,omitempty"`
	// HeapAllocatedMax is the peak of Envoy's allocated heap. HeapPerTunnel
	// is that heap per open tunnel at the read with the most tunnels.
	HeapAllocatedMax float64 `json:"heapAllocatedMax"`
	HeapPerTunnel    float64 `json:"heapPerTunnel,omitempty"`
}

// summarizeGateway reduces the Envoy reads to the gateway summary and the
// flags they raise: tunnels cut by the stream idle timeout, more streams
// per connection than the gateway allows, and an open breaker.
func summarizeGateway(samples []envoySample) (*gatewaySummary, []string) {
	if len(samples) == 0 {
		return nil, nil
	}
	g := &gatewaySummary{
		TunnelsOpened:    envoyDelta(samples, envoyTunnelsOpened),
		TunnelsMax:       envoyMax(samples, envoyTunnels),
		TunnelConnsMax:   envoyMax(samples, envoyTunnelConns),
		ActorConnsMax:    envoyMax(samples, envoyActorConns),
		HTTP2Conns:       envoyDelta(samples, envoyHTTP2Conns),
		HTTP1Conns:       envoyDelta(samples, envoyHTTP1Conns),
		StreamIdleCuts:   envoyDelta(samples, envoyStreamIdleCuts),
		ConnIdleCuts:     envoyDelta(samples, envoyConnIdleCuts),
		ProtocolErrors:   envoyDelta(samples, envoyProtocolErrors),
		PoolConnsMax:     map[string]float64{},
		HeapAllocatedMax: envoyMax(samples, envoyHeapAllocated),
	}
	var tunnels float64
	for _, s := range samples {
		if conns := s.Counters[envoyActorConns]; conns > 0 {
			g.StreamsPerConnMax = max(g.StreamsPerConnMax, s.Counters[envoyStreams]/conns)
		}
		if n := s.Counters[envoyTunnels]; n > tunnels {
			tunnels, g.HeapPerTunnel = n, s.Counters[envoyHeapAllocated]/n
		}
	}
	for _, c := range envoyClusters {
		g.PoolConnsMax[c] = envoyMax(samples, clusterKey(c, envoyStatCxActive))
		for _, stat := range envoyOverflowStats {
			if n := envoyDelta(samples, clusterKey(c, stat)); n > 0 {
				if g.Refusals == nil {
					g.Refusals = map[string]float64{}
				}
				g.Refusals[clusterKey(c, stat)] = n
			}
		}
		if envoyMax(samples, clusterKey(c, envoyStatCxBreaker)) > 0 {
			g.BreakersOpened = append(g.BreakersOpened, c+" cx")
		}
		if envoyMax(samples, clusterKey(c, envoyStatRqBreaker)) > 0 {
			g.BreakersOpened = append(g.BreakersOpened, c+" rq")
		}
	}
	var flags []string
	if g.StreamIdleCuts > 0 {
		flags = append(flags, fmt.Sprintf("the stream idle timeout cut %.0f tunnels (%s)", g.StreamIdleCuts, envoyStreamIdleCuts))
	}
	if g.StreamsPerConnMax > maxStreamsPerConn {
		flags = append(flags, fmt.Sprintf("streams per connection reached %.0f, above the %d the gateway allows", g.StreamsPerConnMax, maxStreamsPerConn))
	}
	if len(g.BreakersOpened) > 0 {
		flags = append(flags, "breaker opened: "+strings.Join(g.BreakersOpened, ", "))
	}
	return g, flags
}

// envoyDelta is the counter's rise from each pod's first read to its last,
// summed over pods.
func envoyDelta(samples []envoySample, counter string) float64 {
	type span struct {
		first, last time.Time
		from, to    float64
	}
	byPod := map[string]*span{}
	for _, s := range samples {
		sp := byPod[s.Pod]
		if sp == nil {
			sp = &span{first: s.T, last: s.T, from: s.Counters[counter], to: s.Counters[counter]}
			byPod[s.Pod] = sp
			continue
		}
		if s.T.Before(sp.first) {
			sp.first, sp.from = s.T, s.Counters[counter]
		}
		if !s.T.Before(sp.last) {
			sp.last, sp.to = s.T, s.Counters[counter]
		}
	}
	var total float64
	for _, sp := range byPod {
		total += sp.to - sp.from
	}
	return total
}

// envoyMax is the gauge's peak over every read of every pod.
func envoyMax(samples []envoySample, gauge string) float64 {
	var m float64
	for _, s := range samples {
		m = max(m, s.Counters[gauge])
	}
	return m
}

func bytesIEC(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGi", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.0fMi", b/(1<<20))
	default:
		return fmt.Sprintf("%.0fKi", b/(1<<10))
	}
}

// sourceWarnings names the targets that answered nothing or failed reads.
func sourceWarnings(res *usageReport) string {
	keys := map[string]bool{}
	for k := range res.Reads {
		keys[k] = true
	}
	for k := range res.Errors {
		keys[k] = true
	}
	var parts []string
	seen := map[string]bool{}
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		source, _, _ := strings.Cut(k, "/")
		seen[source] = true
		if res.Reads[k] == 0 || res.Errors[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d reads, %d failed", k, res.Reads[k], res.Errors[k]))
		}
	}
	for _, src := range res.Sources {
		if !seen[src.Name] {
			parts = append(parts, src.Name+" no reads")
		}
	}
	return strings.Join(parts, "; ")
}

// printUsage writes the cpu, memory and gateway lines, and the flags.
func (rep *report) printUsage(w io.Writer) {
	res := rep.Usage
	if res == nil {
		return
	}
	if warn := sourceWarnings(res); warn != "" {
		fmt.Fprintf(w, "%-10s %s\n", "sources", warn)
	}
	rep.printComponents(w)
	if g := res.Gateway; g != nil {
		fmt.Fprintf(w, "%-10s tunnels opened +%.0f, open max %.0f (tunnel connections max %.0f); actor connections max %.0f, streams per connection max %.0f; h2 connections +%.0f, http1 +%.0f\n",
			"gateway", g.TunnelsOpened, g.TunnelsMax, g.TunnelConnsMax, g.ActorConnsMax, g.StreamsPerConnMax, g.HTTP2Conns, g.HTTP1Conns)
		fmt.Fprintf(w, "%-10s cut by the stream idle timeout +%.0f, by the connection idle timeout +%.0f, protocol errors +%.0f\n",
			"", g.StreamIdleCuts, g.ConnIdleCuts, g.ProtocolErrors)
		var pools []string
		for _, c := range envoyClusters {
			pools = append(pools, fmt.Sprintf("%s %.0f", c, g.PoolConnsMax[c]))
		}
		fmt.Fprintf(w, "%-10s upstream connections max: %s\n", "", strings.Join(pools, ", "))
		if len(g.Refusals) > 0 {
			var parts []string
			for _, k := range slices.Sorted(maps.Keys(g.Refusals)) {
				parts = append(parts, fmt.Sprintf("%s +%.0f", k, g.Refusals[k]))
			}
			fmt.Fprintf(w, "%-10s refusals: %s\n", "", strings.Join(parts, " "))
		}
	}
	for _, f := range res.Flags {
		fmt.Fprintf(w, "%-10s %s\n", "!! usage", f)
	}
}

// printOpeningCores writes each component's mean cores over an opening
// window and the gateway's CPU per tunnel opened in it.
func (rep *report) printOpeningCores(w io.Writer, label string, window phaseMark, opens int64, pick func(*componentSummary) *windowSummary) {
	var cores []string
	perOpen := ""
	for _, name := range componentOrder {
		cs, ok := rep.Usage.Components[name]
		if !ok || pick(cs) == nil || pick(cs).Insufficient {
			continue
		}
		cores = append(cores, fmt.Sprintf("%s %.2f", name, pick(cs).CPUCores.Mean))
		if name == "gateway" && pick(cs).MsPerOpen > 0 {
			perOpen = fmt.Sprintf("; gateway %.1f ms per tunnel opened", pick(cs).MsPerOpen)
		}
	}
	if len(cores) == 0 {
		return
	}
	fmt.Fprintf(w, "%-10s %s: %s (%v, %d opens%s)\n", "", label, strings.Join(cores, " "), round(window.End.Sub(window.Start)), opens, perOpen)
}

// printComponents writes each component's cores over steady and settled,
// the exact figures of the Go processes, and the working sets.
func (rep *report) printComponents(w io.Writer) {
	res := rep.Usage
	if len(res.Components) == 0 {
		return
	}
	var cpu, perK, settledK, mem, insufficient []string
	minCov := 1.0
	for _, name := range componentOrder {
		cs, ok := res.Components[name]
		if !ok {
			continue
		}
		if cs.Steady.Insufficient {
			insufficient = append(insufficient, name)
		} else {
			cpu = append(cpu, fmt.Sprintf("%s %.2f/%.2f", name, cs.Steady.CPUCores.Mean, cs.Steady.CPUCores.Max))
			minCov = min(minCov, cs.Steady.Coverage)
		}
		if cs.Steady.CoresPerKrps > 0 {
			perK = append(perK, fmt.Sprintf("%s %.2f", name, cs.Steady.CoresPerKrps))
		}
		if cs.Settled != nil && cs.Settled.CoresPerKrps > 0 {
			settledK = append(settledK, fmt.Sprintf("%s %.2f", name, cs.Settled.CoresPerKrps))
		}
		if cs.Steady.WorkingSetBytes > 0 {
			mem = append(mem, fmt.Sprintf("%s %s", name, bytesIEC(cs.Steady.WorkingSetBytes)))
		}
	}
	line := strings.Join(cpu, " ")
	if len(insufficient) > 0 {
		line += " (cAdvisor: insufficient for " + strings.Join(insufficient, ",") + ")"
	}
	fmt.Fprintf(w, "%-10s steady cores mean/max: %s\n", "cpu", line)
	if len(perK) > 0 {
		fmt.Fprintf(w, "%-10s cores per 1000 req/s: %s (cAdvisor coverage %.0f%%)\n", "", strings.Join(perK, " "), 100*minCov)
	}
	if st := res.Settled; st != nil && len(settledK) > 0 {
		rule := st.Rule
		if st.Floored {
			rule += ", floored"
		}
		latency := ""
		if st.P99 > 0 {
			latency = fmt.Sprintf(", p50 %v p99 %v", round(st.P50), round(st.P99))
		}
		fmt.Fprintf(w, "%-10s cores per 1000 req/s, settled: %s (from steady +%.1fs by %s, %.1f req/s%s)\n", "",
			strings.Join(settledK, " "), st.Start.Sub(rep.phase("steady").Start).Seconds(), rule, st.ReqPerS, latency)
	}
	if o := rep.Opening; o != nil {
		rep.printOpeningCores(w, "cores over opening", rep.phase("opening"), o.Opens, func(cs *componentSummary) *windowSummary { return cs.Opening })
		if !o.PureEnd.IsZero() {
			rep.printOpeningCores(w, "cores over the pure opening window", rep.phase("opening-pure"), rep.openedInBatch(0), func(cs *componentSummary) *windowSummary { return cs.OpeningPure })
		}
	}
	if len(res.Baseline) > 0 {
		var base, short []string
		for _, name := range componentOrder {
			if b := res.Baseline[name]; b != nil && !b.Insufficient {
				base = append(base, fmt.Sprintf("%s %.3f", name, b.CPUCores.Mean))
			} else if b != nil {
				short = append(short, name)
			}
		}
		line := strings.Join(base, " ")
		if len(short) > 0 {
			line += " (insufficient for " + strings.Join(short, ",") + ")"
		}
		fmt.Fprintf(w, "%-10s pre-idle baseline cores: %s\n", "", line)
	}
	var live []string
	for _, name := range componentOrder {
		cs, ok := res.Components[name]
		if !ok {
			continue
		}
		for _, ctr := range slices.Sorted(maps.Keys(cs.Containers)) {
			if c := cs.Containers[ctr]; c.LiveCPUCores != nil {
				live = append(live, fmt.Sprintf("%s/%s %.3f", name, ctr, *c.LiveCPUCores))
			}
		}
	}
	if d, ok := res.Components["driver"]; ok && d.Steady.LiveCPUCores != nil {
		live = append(live, fmt.Sprintf("driver %.3f", *d.Steady.LiveCPUCores))
	}
	if len(live) > 0 {
		fmt.Fprintf(w, "%-10s steady cores, exact: %s\n", "", strings.Join(live, " "))
	}
	var memory []string
	if len(mem) > 0 {
		memory = append(memory, "steady max working set: "+strings.Join(mem, " "))
	}
	if g := res.Gateway; g != nil && g.HeapAllocatedMax > 0 {
		heap := "envoy heap allocated max " + bytesIEC(g.HeapAllocatedMax)
		if g.HeapPerTunnel > 0 {
			heap += fmt.Sprintf(" (%s per tunnel at %.0f tunnels)", bytesIEC(g.HeapPerTunnel), g.TunnelsMax)
		}
		memory = append(memory, heap)
	}
	if len(memory) > 0 {
		fmt.Fprintf(w, "%-10s %s\n", "memory", strings.Join(memory, "; "))
	}
}
