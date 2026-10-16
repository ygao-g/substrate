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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/egresstunnels"
)

// report is everything one run measured. --output writes it as JSON;
// durations are in nanoseconds.
type report struct {
	Config      runConfig            `json:"config"`
	Started     time.Time            `json:"started"`
	Finished    time.Time            `json:"finished"`
	Interrupted bool                 `json:"interrupted"`
	Create      createResult         `json:"create"`
	Resume      resumeResult         `json:"resume"`
	Start       startResult          `json:"start"`
	Stop        phaseResult          `json:"stop"`
	Suspend     phaseResult          `json:"suspend"`
	Loop        *egresstunnels.Stats `json:"loop,omitempty"`
	// Opening is the opening of every tunnel, once every loop reported it
	// complete; nil otherwise.
	Opening *openingResult `json:"opening,omitempty"`
	// Settle is set when the run waited for the loops to settle.
	Settle *settleResult `json:"settle,omitempty"`
	// Steady summarizes the measured window alone; Loop covers the whole
	// run, opening and settle wait included. Nil with fewer than two
	// complete polls in the window.
	Steady *windowResult `json:"steady,omitempty"`
	Actors []actorResult `json:"actors,omitempty"`
	// Phases holds when each phase ran, on the driver's clock.
	Phases []phaseMark `json:"phases,omitempty"`
	// LoopTimeline holds the merged loop stats of each progress poll.
	LoopTimeline []loopPoint `json:"loopTimeline,omitempty"`
	// Usage is what the egress path spent, with --usage.
	Usage *usageReport `json:"usage,omitempty"`
}

// phase returns the named phase, or the zero mark when it did not run.
func (rep *report) phase(name string) phaseMark {
	for _, p := range rep.Phases {
		if p.Name == name {
			return p
		}
	}
	return phaseMark{}
}

type phaseMark struct {
	Name  string    `json:"name"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func (rep *report) addPhase(name string, start time.Time) {
	rep.Phases = append(rep.Phases, phaseMark{Name: name, Start: start, End: time.Now()})
}

// openingResult is the opening: from the first loop start until the last
// loop had its first answer from its last endpoint.
type openingResult struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// PureEnd is when the first batch had opened: until then the gateway
	// saw opens only. Zero when the loops started in one batch.
	PureEnd time.Time `json:"pureEnd,omitzero"`
	// Opens counts the endpoints opened, over every loop.
	Opens int64 `json:"opens"`
	// Redials is the redial count at the first poll after End; nil when no
	// poll came after it.
	Redials *int64 `json:"redials,omitempty"`
}

// markOpening sets rep.Opening and the opening phase marks from the started
// loops, once every one of them has reported when it finished opening.
func (rep *report) markOpening(started []*actorRun, timeline []loopPoint) {
	if len(started) == 0 {
		return
	}
	o := &openingResult{Start: rep.Start.Batches[0].At}
	for _, a := range started {
		if a.openingDone == 0 {
			return
		}
		end := a.startedAt.Add(a.openingDone)
		if end.After(o.End) {
			o.End = end
		}
		if a.batch == 0 && len(rep.Start.Batches) > 1 && end.After(o.PureEnd) {
			o.PureEnd = end
		}
		if a.stats != nil {
			o.Opens += a.stats.Opened
		}
	}
	for _, p := range timeline {
		if !p.T.Before(o.End) {
			o.Redials = &p.Redials
			break
		}
	}
	rep.Opening = o
	rep.Phases = append(rep.Phases, phaseMark{Name: "opening", Start: o.Start, End: o.End})
	if !o.PureEnd.IsZero() {
		rep.Phases = append(rep.Phases, phaseMark{Name: "opening-pure", Start: o.Start, End: o.PureEnd})
	}
	slices.SortStableFunc(rep.Phases, func(a, b phaseMark) int { return a.Start.Compare(b.Start) })
}

// loopPoint is the cumulative stats of all running loops at one poll.
type loopPoint struct {
	T         time.Time `json:"t"`
	Requests  int64     `json:"requests"`
	Successes int64     `json:"successes"`
	NewConns  int64     `json:"newConns"`
	Opened    int64     `json:"opened"`
	// OpeningDone is the latest Stats.OpeningDone over the loops; zero until
	// every one of them has opened.
	OpeningDone time.Duration `json:"openingDone"`
	Rounds      int64         `json:"rounds"`
	LongestGap  time.Duration `json:"longestGap"`
	// Redials sums, per loop, connections opened beyond one per endpoint.
	Redials int64                   `json:"redials"`
	Latency egresstunnels.Histogram `json:"latency"`
	// Polled and Answered count the actors asked for their stats and those
	// that answered. A point with Answered below Polled covers only the
	// actors that answered, so its totals undercount the run.
	Polled   int `json:"polled"`
	Answered int `json:"answered"`
}

// settleResult says whether and when the loops settled: every loop opened
// all its endpoints and requested each of them twice, and no connection was
// redialed for Config.Settle. Times are since the loops started.
type settleResult struct {
	Settled   bool          `json:"settled"`
	Waited    time.Duration `json:"waited"`
	AllOpened time.Duration `json:"allOpened"`
	AllSwept  time.Duration `json:"allSwept"`
	// Redials is how many connections were opened beyond one per endpoint
	// per loop when the wait ended: tunnels the gateway had cut.
	Redials    int64         `json:"redials"`
	LongestGap time.Duration `json:"longestGap"`
	// LongestGapNearTimer is set when LongestGap is within 10% of
	// Config.StreamIdleTimeout: the interval rule left little room for
	// request latency.
	LongestGapNearTimer bool `json:"longestGapNearTimer,omitempty"`
	// Reason says why the wait ended unsettled; Unanswered names loops that
	// did not answer the last poll, Unmergeable those whose answer the
	// driver could not merge, with the reason.
	Reason      string   `json:"reason,omitempty"`
	Unanswered  []string `json:"unanswered,omitempty"`
	Unmergeable []string `json:"unmergeable,omitempty"`
}

// windowResult is the measured window alone, as the difference between its
// first complete poll, taken when it opened, and its last.
type windowResult struct {
	Start     time.Time     `json:"start"`
	End       time.Time     `json:"end"`
	Polls     int           `json:"polls"`
	Requests  int64         `json:"requests"`
	Successes int64         `json:"successes"`
	Redials   int64         `json:"redials"`
	ReqPerS   float64       `json:"reqPerS"`
	P50       time.Duration `json:"p50"`
	P99       time.Duration `json:"p99"`
	P999      time.Duration `json:"p999"`
}

// histogramDelta is the samples b holds beyond a, for cumulative snapshots.
func histogramDelta(b, a egresstunnels.Histogram) egresstunnels.Histogram {
	d := egresstunnels.Histogram{Count: b.Count - a.Count, SumMicros: b.SumMicros - a.SumMicros, MaxMicros: b.MaxMicros}
	if d.Count <= 0 {
		return egresstunnels.Histogram{}
	}
	d.Counts = slices.Clone(b.Counts)
	for i := range min(len(a.Counts), len(d.Counts)) {
		d.Counts[i] -= a.Counts[i]
	}
	return d
}

type phaseResult struct {
	Attempted int           `json:"attempted"`
	Succeeded int           `json:"succeeded"`
	Failed    int           `json:"failed"`
	Duration  time.Duration `json:"duration"`
	// Errors counts failures by kind: the gRPC code, or the error itself
	// when it is not a gRPC status.
	Errors map[string]int `json:"errors,omitempty"`
	// Examples keeps the first few failures in full.
	Examples []string `json:"examples,omitempty"`
}

type createResult struct {
	phaseResult
	ActorsReused    int `json:"actorsReused"`
	PoliciesUpdated int `json:"policiesUpdated"`
}

type startResult struct {
	phaseResult
	// Batches records each batch of loop starts; one entry without
	// Config.StartBatch.
	Batches []batchMark `json:"batches,omitempty"`
}

// batchMark is one batch of loop starts.
type batchMark struct {
	Index  int       `json:"index"`
	Actors int       `json:"actors"`
	At     time.Time `json:"at"`
}

type resumeResult struct {
	phaseResult
	ResumeLatency latencySummary `json:"resumeLatency"`
	ReadyLatency  latencySummary `json:"readyLatency"`
	// Sources counts the ready actors by the snapshot they resumed from.
	Sources map[string]int `json:"sources,omitempty"`
}

type actorResult struct {
	Name          string        `json:"name"`
	ResumeLatency time.Duration `json:"resumeLatency"`
	ReadyLatency  time.Duration `json:"readyLatency"`
	ResumeSource  string        `json:"resumeSource,omitempty"`
	// StartedAt and Batch say when the loop started and in which batch;
	// zero when it did not start.
	StartedAt time.Time            `json:"startedAt,omitzero"`
	Batch     int                  `json:"batch"`
	Error     string               `json:"error,omitempty"`
	Stats     *egresstunnels.Stats `json:"stats,omitempty"`
}

func (a *actorRun) result() actorResult {
	res := actorResult{Name: a.name, ResumeLatency: a.resumeLatency, ReadyLatency: a.readyLatency,
		ResumeSource: a.resumeSource, StartedAt: a.startedAt, Batch: a.batch, Stats: a.stats}
	if a.err != nil {
		res.Error = a.err.Error()
	}
	return res
}

// maxExamples bounds phaseResult.Examples.
const maxExamples = 3

// phase counts the outcomes of one phase from many goroutines.
type phase struct {
	mu        sync.Mutex
	succeeded int
	failed    int
	errors    map[string]int
	examples  []string
}

func newPhase() *phase { return &phase{errors: make(map[string]int)} }

func (p *phase) succeed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.succeeded++
}

func (p *phase) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed++
	p.errors[errorKind(err)]++
	if len(p.examples) < maxExamples {
		p.examples = append(p.examples, err.Error())
	}
}

func (p *phase) result(attempted int, d time.Duration) phaseResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := phaseResult{Attempted: attempted, Succeeded: p.succeeded, Failed: p.failed, Duration: d, Examples: p.examples}
	if len(p.errors) > 0 {
		res.Errors = maps.Clone(p.errors)
	}
	return res
}

// errorKind groups errors without the actor names they carry: a failure that
// hits thousands of actors is one kind, not thousands.
func errorKind(err error) string {
	var kind string
	var se *statusError
	if errors.As(err, &se) {
		kind = fmt.Sprintf("HTTP %d", se.code)
	} else if s, ok := status.FromError(err); ok {
		kind = s.Code().String()
	} else {
		// Wrapping adds context in front; the cause is the last part.
		kind = err.Error()
		if i := strings.LastIndex(kind, ": "); i >= 0 {
			kind = kind[i+2:]
		}
	}
	if errors.Is(err, context.DeadlineExceeded) && kind != codes.DeadlineExceeded.String() {
		kind = "gave up retrying " + kind
	}
	return kind
}

type latencySummary struct {
	Count int           `json:"count"`
	P50   time.Duration `json:"p50"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

func (s latencySummary) String() string {
	if s.Count == 0 {
		return "no samples"
	}
	return fmt.Sprintf("p50 %v p99 %v max %v", round(s.P50), round(s.P99), round(s.Max))
}

// summarize returns nearest-rank percentiles of durs.
func summarize(durs []time.Duration) latencySummary {
	if len(durs) == 0 {
		return latencySummary{}
	}
	sorted := slices.Sorted(slices.Values(durs))
	at := func(q float64) time.Duration {
		i := int(math.Ceil(q*float64(len(sorted)))) - 1
		return sorted[min(max(i, 0), len(sorted)-1)]
	}
	return latencySummary{Count: len(sorted), P50: at(0.5), P99: at(0.99), Max: sorted[len(sorted)-1]}
}

// round trims a duration to three significant digits for printing.
func round(d time.Duration) time.Duration {
	switch {
	case d >= 10*time.Second:
		return d.Round(100 * time.Millisecond)
	case d >= time.Second:
		return d.Round(10 * time.Millisecond)
	case d >= 10*time.Millisecond:
		return d.Round(100 * time.Microsecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}

func printPhase(w io.Writer, name string, p phaseResult, extra string) {
	fmt.Fprintf(w, "%-10s %d/%d ok in %v%s\n", name, p.Succeeded, p.Attempted, round(p.Duration), extra)
	if p.Failed == 0 {
		return
	}
	fmt.Fprintf(w, "%-10s %d failed: %s\n", "", p.Failed, formatCounts(p.Errors))
	for _, ex := range p.Examples {
		fmt.Fprintf(w, "%-10s   e.g. %s\n", "", ex)
	}
}

// formatCounts prints counts largest first.
func formatCounts[V int | int64](counts map[string]V) string {
	keys := slices.SortedFunc(maps.Keys(counts), func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), cmp.Compare(a, b))
	})
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, counts[k])
	}
	return strings.Join(parts, " ")
}

// print writes the human-readable summary of the run.
func (rep *report) print(w io.Writer) {
	c := rep.Config
	fmt.Fprintf(w, "\n== egress-tunnels: actors=%d parallel=%d endpoints=%d scheme=%s conn-mode=%s request-interval=%v duration=%v\n",
		c.Actors, c.Parallel, c.Endpoints, c.Scheme, c.ConnMode, c.RequestInterval, c.Duration)
	if rep.Interrupted {
		fmt.Fprintln(w, "!! interrupted: the numbers below cover only part of the run")
	}
	printPhase(w, "create", rep.Create.phaseResult,
		fmt.Sprintf(" (%d actors reused, %d policies updated)", rep.Create.ActorsReused, rep.Create.PoliciesUpdated))
	printPhase(w, "resume", rep.Resume.phaseResult, "")
	fmt.Fprintf(w, "%-10s resume %s; ready %s\n", "", rep.Resume.ResumeLatency, rep.Resume.ReadyLatency)
	if len(rep.Resume.Sources) > 0 {
		var src []string
		for _, k := range slices.Sorted(maps.Keys(rep.Resume.Sources)) {
			src = append(src, fmt.Sprintf("%s %d", k, rep.Resume.Sources[k]))
		}
		fmt.Fprintf(w, "%-10s resumed from: %s\n", "", strings.Join(src, ", "))
	}
	printPhase(w, "start", rep.Start.phaseResult, "")
	if o := rep.Opening; o != nil {
		line := fmt.Sprintf("%-10s %v: %d opens", "opening", round(o.End.Sub(o.Start)), o.Opens)
		if o.Redials != nil {
			line += fmt.Sprintf(", %d redials", *o.Redials)
		}
		if !o.PureEnd.IsZero() {
			line += fmt.Sprintf(" (pure window %v, %d actors)", round(o.PureEnd.Sub(o.Start)), rep.Start.Batches[0].Actors)
		}
		fmt.Fprintln(w, line)
	}
	if s := rep.Settle; s != nil {
		if s.Settled {
			near := ""
			if s.LongestGapNearTimer {
				near = fmt.Sprintf(" (within 10%% of the %v stream idle timeout)", c.StreamIdleTimeout)
			}
			fmt.Fprintf(w, "%-10s settled after %v: all opened at %v, all swept at %v, %d redials, longest gap %v%s\n", "settle",
				round(s.Waited), round(s.AllOpened), round(s.AllSwept), s.Redials, round(s.LongestGap), near)
		} else {
			fmt.Fprintf(w, "%-10s NOT settled after %v: %s\n", "settle", round(s.Waited), s.Reason)
			if s.AllOpened > 0 {
				fmt.Fprintf(w, "%-10s all opened at %v\n", "", round(s.AllOpened))
			}
			if s.AllSwept > 0 {
				fmt.Fprintf(w, "%-10s all swept at %v, %d redials, longest gap %v\n", "", round(s.AllSwept), s.Redials, round(s.LongestGap))
			}
		}
	}

	if s := rep.Loop; s != nil && s.Requests > 0 {
		secs := s.Elapsed.Seconds()
		fmt.Fprintf(w, "%-10s %v: %d requests, %.1f req/s, %.3f%% success, %d new connections\n",
			"whole run", round(s.Elapsed), s.Requests, float64(s.Requests)/secs, 100*float64(s.Successes)/float64(s.Requests), s.NewConns)
		if sw := rep.Steady; sw != nil && sw.Requests > 0 {
			fmt.Fprintf(w, "%-10s %v window, %d polls: %d requests, %.1f req/s, %.3f%% success, %d redials; p50 %v p99 %v p99.9 %v\n",
				"steady", round(sw.End.Sub(sw.Start)), sw.Polls, sw.Requests, sw.ReqPerS, 100*float64(sw.Successes)/float64(sw.Requests), sw.Redials,
				round(sw.P50), round(sw.P99), round(sw.P999))
		}
		if s.Opened > 0 {
			fmt.Fprintf(w, "%-10s %d endpoints opened, %d rounds in all, longest gap %v\n", "", s.Opened, s.Rounds, round(s.LongestGap))
		}
		fmt.Fprintf(w, "%-10s p50 %v p90 %v p99 %v p99.9 %v max %v (mean %v)\n", "latency",
			round(s.Latency.Quantile(0.5)), round(s.Latency.Quantile(0.9)), round(s.Latency.Quantile(0.99)),
			round(s.Latency.Quantile(0.999)), round(s.Latency.Max()), round(s.Latency.Mean()))
		if s.DNS.Count > 0 {
			fmt.Fprintf(w, "%-10s %d lookups, p50 %v p99 %v max %v\n", "dns",
				s.DNS.Count, round(s.DNS.Quantile(0.5)), round(s.DNS.Quantile(0.99)), round(s.DNS.Max()))
		}
		if s.TLS.Count > 0 {
			fmt.Fprintf(w, "%-10s %d handshakes, p50 %v p99 %v max %v\n", "tls",
				s.TLS.Count, round(s.TLS.Quantile(0.5)), round(s.TLS.Quantile(0.99)), round(s.TLS.Max()))
		}
		if len(s.Errors) > 0 {
			fmt.Fprintf(w, "%-10s %s\n", "errors", formatCounts(s.Errors))
		}
		rep.printPerActor(w)
		printWorstEndpoints(w, s)
	} else {
		fmt.Fprintf(w, "%-10s no requests measured\n", "loop")
	}
	printPhase(w, "stop", rep.Stop, "")
	printPhase(w, "suspend", rep.Suspend, "")
	rep.printUsage(w)
}

func (rep *report) printPerActor(w io.Writer) {
	var rates []float64
	for _, a := range rep.Actors {
		if a.Stats != nil && a.Stats.Elapsed > 0 {
			rates = append(rates, float64(a.Stats.Requests)/a.Stats.Elapsed.Seconds())
		}
	}
	if len(rates) == 0 {
		return
	}
	slices.Sort(rates)
	fmt.Fprintf(w, "%-10s req/s per actor: min %.1f median %.1f max %.1f\n", "",
		rates[0], rates[len(rates)/2], rates[len(rates)-1])
}

// printWorstEndpoints lists the endpoints with the most errors, if any failed.
func printWorstEndpoints(w io.Writer, s *egresstunnels.Stats) {
	idx := make([]int, 0, len(s.Endpoints))
	for i, e := range s.Endpoints {
		if e.Errors > 0 {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return
	}
	slices.SortFunc(idx, func(a, b int) int {
		return cmp.Or(cmp.Compare(s.Endpoints[b].Errors, s.Endpoints[a].Errors), cmp.Compare(a, b))
	})
	var parts []string
	for _, i := range idx[:min(len(idx), 5)] {
		parts = append(parts, fmt.Sprintf("%s (%d/%d)", egresstunnels.ServiceName(i), s.Endpoints[i].Errors, s.Endpoints[i].Requests))
	}
	fmt.Fprintf(w, "%-10s most errors: %s\n", "", strings.Join(parts, ", "))
}
