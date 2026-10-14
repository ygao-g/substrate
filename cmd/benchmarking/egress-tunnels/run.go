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
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/egresstunnels"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	connModeKeepAlive = "keepalive"
	connModeNewConn   = "new-conn"
)

// runConfig is what one run does. It is echoed into the report.
type runConfig struct {
	Actors            int           `json:"actors"`
	Parallel          int           `json:"parallel"`
	Endpoints         int           `json:"endpoints"`
	Duration          time.Duration `json:"duration"`
	ConnMode          string        `json:"connMode"`
	RequestTimeout    time.Duration `json:"requestTimeout"`
	RequestInterval   time.Duration `json:"requestInterval"`
	CreateConcurrency int           `json:"createConcurrency"`
	ResumeTimeout     time.Duration `json:"resumeTimeout"`
	ProgressInterval  time.Duration `json:"progressInterval"`
	// StreamIdleTimeout is the gateway's CONNECT stream idle timeout. A
	// keep-alive run must open all its endpoints within half of it, or the
	// gateway resets the first tunnels before their first sweep. Zero skips
	// the check.
	StreamIdleTimeout time.Duration `json:"streamIdleTimeout"`
	// Settle, when positive, delays the measured window until every loop has
	// opened all its endpoints and swept them once, and no connection has
	// been redialed for this long. SettleTimeout bounds the wait.
	Settle        time.Duration `json:"settle"`
	SettleTimeout time.Duration `json:"settleTimeout"`
	Atespace      string        `json:"atespace"`
	Template      string        `json:"template"`
	Scheme        string        `json:"scheme"`
}

func (c *runConfig) validate() error {
	switch {
	case c.Actors < 1:
		return fmt.Errorf("--actors must be at least 1, got %d", c.Actors)
	case c.Parallel < 1 || c.Parallel > c.Actors:
		return fmt.Errorf("--parallel must be between 1 and --actors (%d), got %d", c.Actors, c.Parallel)
	case c.Endpoints < 1 || c.Endpoints > egresstunnels.MaxEndpoints:
		return fmt.Errorf("--endpoints must be between 1 and %d, got %d", egresstunnels.MaxEndpoints, c.Endpoints)
	case c.Duration <= 0:
		return fmt.Errorf("--duration must be positive, got %v", c.Duration)
	case c.ConnMode != connModeKeepAlive && c.ConnMode != connModeNewConn:
		return fmt.Errorf("--conn-mode must be %s or %s, got %q", connModeKeepAlive, connModeNewConn, c.ConnMode)
	case c.RequestTimeout < time.Millisecond || c.RequestTimeout%time.Millisecond != 0:
		return fmt.Errorf("--request-timeout must be a positive whole number of milliseconds, got %v", c.RequestTimeout)
	case c.RequestInterval < 0 || c.RequestInterval%time.Millisecond != 0:
		return fmt.Errorf("--request-interval must be zero or a whole number of milliseconds, got %v", c.RequestInterval)
	case c.CreateConcurrency < 1:
		return fmt.Errorf("--create-concurrency must be at least 1, got %d", c.CreateConcurrency)
	case c.ResumeTimeout <= 0:
		return fmt.Errorf("--resume-timeout must be positive, got %v", c.ResumeTimeout)
	case c.ProgressInterval < 0:
		return fmt.Errorf("--progress-interval cannot be negative, got %v", c.ProgressInterval)
	case c.StreamIdleTimeout < 0:
		return fmt.Errorf("--stream-idle-timeout cannot be negative, got %v", c.StreamIdleTimeout)
	case c.Settle < 0:
		return fmt.Errorf("--settle cannot be negative, got %v", c.Settle)
	case c.Settle > 0 && c.SettleTimeout <= 0:
		return fmt.Errorf("--settle-timeout must be positive with --settle, got %v", c.SettleTimeout)
	case c.Settle > 0 && c.ConnMode != connModeKeepAlive:
		return fmt.Errorf("--settle needs --conn-mode %s: redials mean nothing with a new connection per request", connModeKeepAlive)
	case c.Settle > 0 && c.ProgressInterval <= 0:
		return errors.New("--settle needs --progress-interval above 0: the polls are what it watches")
	case c.Settle > 0 && c.Settle < time.Duration(c.Endpoints)*c.RequestInterval:
		return fmt.Errorf("--settle (%v) must cover one sweep of the endpoints (%d x %v = %v), or a cut tunnel can go unseen",
			c.Settle, c.Endpoints, c.RequestInterval, time.Duration(c.Endpoints)*c.RequestInterval)
	case c.Scheme != egresstunnels.SchemeHTTP && c.Scheme != egresstunnels.SchemeHTTPS:
		return fmt.Errorf("--scheme must be %s or %s, got %q", egresstunnels.SchemeHTTP, egresstunnels.SchemeHTTPS, c.Scheme)
	case c.Atespace == "" || c.Template == "":
		return errors.New("--atespace and --template are required")
	}
	// The first endpoint waits (2C-1) intervals from its open to its first
	// sweep, so C intervals must fit in half the timer.
	if opening := time.Duration(c.Endpoints) * c.RequestInterval; c.ConnMode == connModeKeepAlive && c.StreamIdleTimeout > 0 && opening > c.StreamIdleTimeout/2 {
		return fmt.Errorf("--endpoints x --request-interval = %v is more than half the gateway's stream idle timeout (%v): the gateway would reset the first tunnels before their first sweep", opening, c.StreamIdleTimeout)
	}
	return nil
}

// actorName is the name of actor i. Names are stable across runs, so a rerun
// reuses the actors an earlier run created.
func actorName(i int) string {
	return fmt.Sprintf("egress-%d", i)
}

// runner carries out one run against a cluster.
type runner struct {
	cfg    runConfig
	api    ateapipb.ControlClient
	k8s    kubernetes.Interface
	router *routerClient
	out    io.Writer

	backoff backoff
	// createTimeout bounds the retries of one actor's create and policy.
	createTimeout time.Duration
	// pollInterval paces the readiness polls after a resume.
	pollInterval time.Duration
	// cleanupTimeout bounds stopping the loops and suspending the actors,
	// which still happen after the run is interrupted.
	cleanupTimeout time.Duration
}

func newRunner(cfg runConfig, api ateapipb.ControlClient, k8s kubernetes.Interface, router *routerClient, out io.Writer) *runner {
	return &runner{
		cfg:            cfg,
		api:            api,
		k8s:            k8s,
		router:         router,
		out:            out,
		backoff:        defaultBackoff,
		createTimeout:  2 * time.Minute,
		pollInterval:   200 * time.Millisecond,
		cleanupTimeout: 3 * time.Minute,
	}
}

func (r *runner) logf(format string, args ...any) {
	fmt.Fprintf(r.out, "%s  "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

// run creates the actors, resumes Parallel of them, runs their loops for
// Duration, and suspends them again. When ctx ends early, the loops that
// started are still stopped and collected and the resumed actors suspended;
// the partial report comes back with ctx's error.
func (r *runner) run(ctx context.Context) (*report, error) {
	rep := &report{Config: r.cfg, Started: time.Now()}
	defer func() { rep.Finished = time.Now() }()

	if err := preflight(ctx, r.k8s, r.cfg.Endpoints, r.cfg.Scheme); err != nil {
		return nil, err
	}
	phaseStart := time.Now()
	created := r.createActors(ctx, rep)
	rep.addPhase("create", phaseStart)
	if ctx.Err() != nil {
		rep.Interrupted = true
		return rep, ctx.Err()
	}

	names := r.pickActors(created)
	if len(names) < r.cfg.Parallel {
		r.logf("only %d actors were created; resuming %d instead of %d", len(names), len(names), r.cfg.Parallel)
	}

	phaseStart = time.Now()
	actors := r.resumeActors(ctx, names, rep)
	rep.addPhase("resume", phaseStart)

	var ready []*actorRun
	for _, a := range actors {
		if a.ready {
			ready = append(ready, a)
		}
	}
	if ctx.Err() == nil {
		phaseStart = time.Now()
		started := r.startLoops(ctx, ready, rep)
		rep.addPhase("start", phaseStart)
		w := r.watchLoops(started)
		if r.cfg.Settle > 0 && len(started) > 0 {
			phaseStart = time.Now()
			rep.Settle = w.settle(ctx)
			rep.addPhase("settle", phaseStart)
		}
		phaseStart = time.Now()
		w.steady(ctx)
		rep.addPhase("steady", phaseStart)
		rep.LoopTimeline = w.timeline
		rep.Steady = w.window()
	}

	// Everything resumed is put back, even after an interrupt, under its own
	// deadline that starts only now, whatever the run's duration.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cleanupTimeout)
	defer cancel()
	// Every ready actor gets a stop: an interrupt can land after an actor
	// started its loop but before the driver saw the answer.
	phaseStart = time.Now()
	r.stopLoops(cleanupCtx, ready, rep)
	rep.addPhase("stop", phaseStart)
	phaseStart = time.Now()
	r.suspendActors(cleanupCtx, actors, rep)
	rep.addPhase("suspend", phaseStart)
	if ctx.Err() != nil {
		rep.Interrupted = true
		return rep, ctx.Err()
	}
	if rep.Stop.Failed > 0 || rep.Suspend.Failed > 0 {
		return rep, fmt.Errorf("%d loops were not stopped and %d actors were not suspended; they may still be sending traffic, so suspend them or run cleanup",
			rep.Stop.Failed, rep.Suspend.Failed)
	}
	return rep, nil
}

// createActors creates actors 0 through Actors-1 and gives each the egress
// policy. Actors and policies that already exist are reused. It returns, per
// actor, whether it is ready to resume.
func (r *runner) createActors(ctx context.Context, rep *report) []bool {
	n := r.cfg.Actors
	want := buildPolicy(r.cfg.Atespace, r.cfg.Endpoints)
	ok := make([]bool, n)
	res := newPhase()
	var reused, updated, done atomic.Int64

	stopProgress := r.progress(ctx, func() { r.logf("create: %d/%d", done.Load(), n) })
	start := time.Now()
	forEach(n, r.cfg.CreateConcurrency, func(i int) {
		defer done.Add(1)
		if ctx.Err() != nil {
			return
		}
		actorCtx, cancel := context.WithTimeout(ctx, r.createTimeout)
		defer cancel()
		existed, err := r.ensureActor(actorCtx, actorName(i))
		if err != nil {
			res.fail(fmt.Errorf("create actor: %w", err))
			return
		}
		if existed {
			reused.Add(1)
		}
		changed, err := r.ensurePolicy(actorCtx, actorName(i), want)
		if err != nil {
			res.fail(fmt.Errorf("egress policy: %w", err))
			return
		}
		if changed {
			updated.Add(1)
		}
		res.succeed()
		ok[i] = true
	})
	stopProgress()

	rep.Create = createResult{
		phaseResult:     res.result(n, time.Since(start)),
		ActorsReused:    int(reused.Load()),
		PoliciesUpdated: int(updated.Load()),
	}
	r.logf("create: %d/%d ready in %v (%d reused, %d policies updated)",
		rep.Create.Succeeded, n, rep.Create.Duration.Round(time.Millisecond), rep.Create.ActorsReused, rep.Create.PoliciesUpdated)
	return ok
}

// ensureActor creates the actor and reports whether it already existed. An
// AlreadyExists after an attempt whose outcome is unknown may be that
// attempt's own commit, so it does not count as existing.
func (r *runner) ensureActor(ctx context.Context, name string) (existed bool, err error) {
	var maybeCreated bool
	err = r.backoff.retry(ctx, func(ctx context.Context) error {
		_, err := r.api.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: r.cfg.Atespace, Name: name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: r.cfg.Template},
			},
		})
		if status.Code(err) == codes.AlreadyExists {
			existed = !maybeCreated
			return nil
		}
		if outcomeUnknown(err) {
			maybeCreated = true
		}
		return err
	})
	return existed, err
}

// ensurePolicy gives the actor exactly the want policy and reports whether it
// had to replace a different one.
func (r *runner) ensurePolicy(ctx context.Context, name string, want *ateapipb.EgressPolicy) (changed bool, err error) {
	ref := &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: name}
	var exists bool
	err = r.backoff.retry(ctx, func(ctx context.Context) error {
		_, err := r.api.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: ref, EgressPolicy: want})
		if status.Code(err) == codes.AlreadyExists {
			exists = true
			return nil
		}
		return err
	})
	if err != nil || !exists {
		return false, err
	}

	// Each attempt reads the policy again: the update needs the current UID
	// and version as preconditions, and a version conflict (Aborted) means
	// another write, possibly an earlier attempt's, moved them.
	err = r.backoff.retry(ctx, func(ctx context.Context) error {
		existing, err := r.api.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref, Name: policyName})
		if err != nil {
			return err
		}
		if samePolicyRules(existing, want) {
			return nil
		}
		// Set before the call: an update that commits but loses its answer
		// finds matching rules on the next attempt.
		changed = true
		update := &ateapipb.EgressPolicy{Metadata: existing.GetMetadata(), Rules: want.GetRules()}
		_, err = r.api.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{Actor: ref, EgressPolicy: update})
		return err
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

// pickActors returns the names of the first Parallel actors among the created
// ones, in index order, so a warm-up run of Parallel actors gives every later
// run of at most that many their own snapshots. It returns all of them if
// fewer were created.
func (r *runner) pickActors(created []bool) []string {
	var idx []int
	for i, ok := range created {
		if ok {
			idx = append(idx, i)
		}
	}
	idx = idx[:min(len(idx), r.cfg.Parallel)]
	names := make([]string, len(idx))
	for i, n := range idx {
		names[i] = actorName(n)
	}
	return names
}

// actorRun is one resumed actor and what happened to it.
type actorRun struct {
	name string
	// resumeLatency runs from the first ResumeActor attempt to its success,
	// including retries while the worker pool was full.
	resumeLatency time.Duration
	// readyLatency runs from the resume to the first answer through the router.
	readyLatency time.Duration
	// resumeSource is the snapshot the resume restored from, read just
	// before it: resumeFromOwn, resumeFromTag or resumeFromNone.
	resumeSource string
	resumed      bool
	// mayBeRunning is set when a resume failed in a way that may still have
	// committed, such as a timeout, so the actor is suspended anyway.
	mayBeRunning bool
	ready        bool
	started      bool
	err          error
	stats        *egresstunnels.Stats
}

// resumeActors resumes every named actor at once and waits until each answers
// through the router.
func (r *runner) resumeActors(ctx context.Context, names []string, rep *report) []*actorRun {
	actors := make([]*actorRun, len(names))
	// Read every actor before the burst, so that the reads do not compete
	// with the resumes at ateapi and the resumes start together.
	forEach(len(names), len(names), func(i int) {
		a := &actorRun{name: names[i], resumeSource: resumeFromUnknown}
		actors[i] = a
		readCtx, cancel := context.WithTimeout(ctx, r.cfg.ResumeTimeout)
		defer cancel()
		if got, err := r.api.GetActor(readCtx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: a.name},
		}); err == nil {
			a.resumeSource = resumeSource(got)
		}
	})
	res := newPhase()
	start := time.Now()
	forEach(len(names), len(names), func(i int) {
		a := actors[i]
		actorCtx, cancel := context.WithTimeout(ctx, r.cfg.ResumeTimeout)
		defer cancel()

		resumeStart := time.Now()
		err := r.backoff.retry(actorCtx, func(ctx context.Context) error {
			_, err := r.api.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: a.name},
			})
			if outcomeUnknown(err) {
				a.mayBeRunning = true
			}
			return err
		})
		if err != nil {
			a.err = fmt.Errorf("resume: %w", err)
			res.fail(a.err)
			return
		}
		a.resumed = true
		a.resumeLatency = time.Since(resumeStart)

		readyStart := time.Now()
		if err := r.waitReady(actorCtx, a.name); err != nil {
			a.err = fmt.Errorf("waiting for %s through the router: %w", egresstunnels.ReadyzRoute, err)
			res.fail(a.err)
			return
		}
		a.ready = true
		a.readyLatency = time.Since(readyStart)
		res.succeed()
	})

	rep.Resume = resumeResult{phaseResult: res.result(len(names), time.Since(start))}
	var resumeLat, readyLat []time.Duration
	for _, a := range actors {
		if a.ready {
			resumeLat = append(resumeLat, a.resumeLatency)
			readyLat = append(readyLat, a.readyLatency)
		}
	}
	rep.Resume.ResumeLatency = summarize(resumeLat)
	rep.Resume.ReadyLatency = summarize(readyLat)
	rep.Resume.Sources = map[string]int{}
	for _, a := range actors {
		if a.ready {
			rep.Resume.Sources[a.resumeSource]++
		}
	}
	r.logf("resume: %d/%d ready in %v (resume %s; ready %s)", rep.Resume.Succeeded, len(names),
		rep.Resume.Duration.Round(time.Millisecond), rep.Resume.ResumeLatency, rep.Resume.ReadyLatency)
	return actors
}

// Snapshots a resume can restore from.
const (
	// resumeFromOwn is a snapshot the actor took at its last suspend.
	resumeFromOwn = "own"
	// resumeFromTag is a tag's snapshot, such as the template's golden
	// one, borrowed until the actor's first suspend.
	resumeFromTag = "tag"
	// resumeFromNone means the actor has no snapshot and boots fresh.
	resumeFromNone = "none"
	// resumeFromRunning means the actor was already running, so the resume
	// restored nothing; an earlier run left it that way.
	resumeFromRunning = "running"
	resumeFromUnknown = "unknown"
)

// resumeSource classifies the durable snapshot an actor would resume from:
// the newest completed generation, which is the actor's own when the actor
// took it and a tag's when the actor was created from one. A running actor
// resumes from nothing.
func resumeSource(a *ateapipb.Actor) string {
	if a.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return resumeFromRunning
	}
	var latest *ateapipb.Snapshot
	for _, s := range a.GetStatus().GetSnapshots() {
		if s.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
			continue
		}
		if latest == nil || s.GetGeneration() > latest.GetGeneration() {
			latest = s
		}
	}
	if latest == nil {
		return resumeFromNone
	}
	switch latest.GetOwner() {
	case ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR:
		return resumeFromOwn
	case ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG:
		return resumeFromTag
	}
	return resumeFromUnknown
}

// waitReady polls the actor's readiness route through the router: right after
// a resume, the route to the actor can take a moment to settle.
func (r *runner) waitReady(ctx context.Context, name string) error {
	for {
		err := r.router.do(ctx, http.MethodGet, name, egresstunnels.ReadyzRoute, nil, nil)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		case <-time.After(r.pollInterval):
		}
	}
}

// startLoops starts the loops of the ready actors at the same moment and
// returns the actors whose loop started.
func (r *runner) startLoops(ctx context.Context, ready []*actorRun, rep *report) []*actorRun {
	req := egresstunnels.StartRequest{
		Endpoints:         r.cfg.Endpoints,
		NewConnPerRequest: r.cfg.ConnMode == connModeNewConn,
		RequestTimeoutMs:  r.cfg.RequestTimeout.Milliseconds(),
		IntervalMs:        r.cfg.RequestInterval.Milliseconds(),
		Scheme:            r.cfg.Scheme,
	}
	res := newPhase()
	start := time.Now()
	forEach(len(ready), len(ready), func(i int) {
		a := ready[i]
		err := r.router.do(ctx, http.MethodPost, a.name, egresstunnels.StartRoute, req, nil)
		var se *statusError
		if errors.As(err, &se) && se.code == http.StatusConflict {
			// A loop left running by an interrupted run: stop it and start over.
			if err = r.router.do(ctx, http.MethodPost, a.name, egresstunnels.StopRoute, nil, nil); err == nil {
				err = r.router.do(ctx, http.MethodPost, a.name, egresstunnels.StartRoute, req, nil)
			}
		}
		if err != nil {
			a.err = fmt.Errorf("start: %w", err)
			res.fail(a.err)
			return
		}
		a.started = true
		res.succeed()
	})

	var started []*actorRun
	for _, a := range ready {
		if a.started {
			started = append(started, a)
		}
	}
	rep.Start = res.result(len(ready), time.Since(start))
	r.logf("start: %d/%d loops running against %d endpoints (%s, %s)", len(started), len(ready), r.cfg.Endpoints, r.cfg.Scheme, r.cfg.ConnMode)
	return started
}

// loopWatch polls the running loops, logs progress and keeps the timeline,
// through the settle wait and the measured window alike.
type loopWatch struct {
	r       *runner
	started []*actorRun
	start   time.Time

	last     egresstunnels.Stats
	lastAt   time.Time
	timeline []loopPoint
	// steadyFrom indexes the first timeline point of the measured window,
	// polled as the window opens; -1 before that.
	steadyFrom int
}

func (r *runner) watchLoops(started []*actorRun) *loopWatch {
	now := time.Now()
	return &loopWatch{r: r, started: started, start: now, lastAt: now, steadyFrom: -1}
}

// pollResult is one poll of every running loop.
type pollResult struct {
	merged   *egresstunnels.Stats
	answered int
	// opened and swept count the answering loops that have opened every
	// endpoint with at least as many successes, and that have completed a
	// sweep.
	opened, swept int
	// redials sums, per loop, the connections opened beyond one per
	// endpoint: tunnels that were cut and dialed again.
	redials int64
	// missing counts the loops that did not answer; unanswered names the
	// first few.
	missing    int
	unanswered []string
}

// poll reads every loop, logs the rate since the previous poll and appends
// the point to the timeline. Loops that do not answer are left out.
func (w *loopWatch) poll(ctx context.Context) (pollResult, time.Time) {
	var mu sync.Mutex
	res := pollResult{merged: &egresstunnels.Stats{}}
	c := int64(w.r.cfg.Endpoints)
	forEach(len(w.started), len(w.started), func(i int) {
		var s egresstunnels.Stats
		err := w.r.router.do(ctx, http.MethodGet, w.started[i].name, egresstunnels.StatsRoute, nil, &s)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			res.missing++
			if len(res.unanswered) < maxExamples {
				res.unanswered = append(res.unanswered, w.started[i].name)
			}
			return
		}
		res.merged.Merge(&s)
		res.answered++
		if s.Opened >= c && s.Successes >= c {
			res.opened++
		}
		if s.Sweeps >= 1 {
			res.swept++
		}
		res.redials += max(0, s.NewConns-s.Opened)
	})
	cur, now := res.merged, time.Now()
	w.timeline = append(w.timeline, loopPoint{T: now, Requests: cur.Requests, Successes: cur.Successes,
		NewConns: cur.NewConns, Opened: cur.Opened, Sweeps: cur.Sweeps, LongestGap: cur.LongestGap, Redials: res.redials,
		Latency: cur.Latency, Polled: len(w.started), Answered: res.answered})
	secs := now.Sub(w.lastAt).Seconds()
	reqs, errs := cur.Requests-w.last.Requests, (cur.Requests-cur.Successes)-(w.last.Requests-w.last.Successes)
	line := fmt.Sprintf("loop: %v elapsed, %.1f req/s, %.1f errors/s, p99 %v (cumulative); opened %d/%d, swept %d/%d, %d redials, longest gap %v",
		now.Sub(w.start).Round(time.Second), float64(reqs)/secs, float64(errs)/secs, cur.Latency.Quantile(0.99),
		res.opened, res.answered, res.swept, res.answered, res.redials, cur.LongestGap.Round(time.Second))
	if res.missing > 0 {
		line += fmt.Sprintf("; %d loops did not answer (%s)", res.missing, strings.Join(res.unanswered, ", "))
	}
	w.r.logf("%s", line)
	w.last, w.lastAt = *cur, now
	return res, now
}

// until polls every ProgressInterval until done returns true or limit
// passes, and reports which. pollFirst takes a poll at once, before the
// first tick. With progress off it only waits; validate keeps --settle from
// getting here without progress.
func (w *loopWatch) until(ctx context.Context, limit time.Duration, pollFirst bool, done func(pollResult, time.Time) bool) bool {
	interval := w.r.cfg.ProgressInterval
	if interval <= 0 {
		select {
		case <-ctx.Done():
		case <-time.After(limit):
		}
		return false
	}
	deadline := time.After(limit)
	if pollFirst {
		if res, now := w.poll(ctx); done != nil && done(res, now) {
			return true
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-ticker.C:
			if res, now := w.poll(ctx); done != nil && done(res, now) {
				return true
			}
		}
	}
}

// steady is the measured window: a poll as it opens, then Duration of polls.
func (w *loopWatch) steady(ctx context.Context) {
	if len(w.started) == 0 {
		return
	}
	w.steadyFrom = len(w.timeline)
	w.until(ctx, w.r.cfg.Duration, true, nil)
}

// window summarizes the measured window from its complete polls, the first
// one taken as the window opened to the last. It is nil with fewer than two.
func (w *loopWatch) window() *windowResult {
	if w.steadyFrom < 0 {
		return nil
	}
	var polls []loopPoint
	for _, p := range w.timeline[w.steadyFrom:] {
		if p.Answered == p.Polled {
			polls = append(polls, p)
		}
	}
	if len(polls) < 2 {
		return nil
	}
	first, last := polls[0], polls[len(polls)-1]
	res := &windowResult{Start: first.T, End: last.T, Polls: len(polls),
		Requests: last.Requests - first.Requests, Successes: last.Successes - first.Successes, Redials: last.Redials - first.Redials}
	if secs := last.T.Sub(first.T).Seconds(); secs > 0 {
		res.ReqPerS = float64(res.Requests) / secs
	}
	if d := histogramDelta(last.Latency, first.Latency); d.Count > 0 {
		res.P50, res.P99, res.P999 = d.Quantile(0.5), d.Quantile(0.99), d.Quantile(0.999)
	}
	return res
}

// settle waits until every loop answers, has opened all its endpoints with
// a success each and swept them once, and the redial count has not moved
// for Settle, or until SettleTimeout passes.
func (w *loopWatch) settle(ctx context.Context) *settleResult {
	res := &settleResult{}
	interval := w.r.cfg.ProgressInterval
	var flatSince time.Time
	lastRedials := int64(-1)
	res.Settled = w.until(ctx, w.r.cfg.SettleTimeout, false, func(p pollResult, now time.Time) bool {
		res.Unanswered = p.unanswered
		if p.missing > 0 {
			return false
		}
		if p.opened == p.answered && res.AllOpened == 0 {
			res.AllOpened = now.Sub(w.start)
		}
		if p.swept == p.answered && res.AllSwept == 0 {
			res.AllSwept = now.Sub(w.start)
		}
		if res.AllOpened == 0 || res.AllSwept == 0 {
			return false
		}
		if p.redials != lastRedials {
			lastRedials, flatSince = p.redials, now
		}
		res.Redials = p.redials
		res.LongestGap = p.merged.LongestGap
		// Polls land a few ms after their tick, so half an interval of slack
		// keeps a --settle that is a multiple of the interval from costing one
		// more poll.
		return now.Sub(flatSince)+interval/2 >= w.r.cfg.Settle
	})
	res.Waited = time.Since(w.start)
	if t := w.r.cfg.StreamIdleTimeout; t > 0 && res.LongestGap > t*9/10 {
		res.LongestGapNearTimer = true
	}
	switch {
	case res.Settled:
		near := ""
		if res.LongestGapNearTimer {
			near = fmt.Sprintf("; within 10%% of the %v stream idle timeout", w.r.cfg.StreamIdleTimeout)
		}
		w.r.logf("settle: settled after %v (all opened at %v, all swept at %v, %d redials, longest gap %v%s)",
			res.Waited.Round(time.Second), res.AllOpened.Round(time.Second), res.AllSwept.Round(time.Second), res.Redials, res.LongestGap.Round(time.Second), near)
	case ctx.Err() != nil:
		res.Reason = "interrupted"
		w.r.logf("settle: interrupted after %v", res.Waited.Round(time.Second))
	default:
		switch {
		case len(res.Unanswered) > 0:
			res.Reason = "loops did not answer: " + strings.Join(res.Unanswered, ", ")
		case res.AllOpened == 0:
			res.Reason = "not every loop opened all its endpoints with a success each"
		case res.AllSwept == 0:
			res.Reason = "not every loop completed a sweep"
		default:
			res.Reason = fmt.Sprintf("redials kept changing (%d so far)", res.Redials)
		}
		w.r.logf("settle: NOT settled after %v: %s; measuring anyway", res.Waited.Round(time.Second), res.Reason)
	}
	return res
}

// stopLoops stops the loop of every ready actor and keeps its final stats.
// An actor whose start was never confirmed may have no loop, which is fine;
// one that did start and has no loop lost it, which is a failure.
func (r *runner) stopLoops(ctx context.Context, ready []*actorRun, rep *report) {
	res := newPhase()
	start := time.Now()
	forEach(len(ready), len(ready), func(i int) {
		a := ready[i]
		var s egresstunnels.Stats
		err := r.router.do(ctx, http.MethodPost, a.name, egresstunnels.StopRoute, nil, &s)
		var se *statusError
		switch {
		case err == nil:
			a.stats = &s
			res.succeed()
		case !a.started && errors.As(err, &se) && se.code == http.StatusNotFound:
			res.succeed()
		default:
			a.err = fmt.Errorf("stop: %w", err)
			res.fail(a.err)
		}
	})
	rep.Stop = res.result(len(ready), time.Since(start))

	merged := &egresstunnels.Stats{}
	for _, a := range ready {
		if a.stats != nil {
			merged.Merge(a.stats)
		}
		rep.Actors = append(rep.Actors, a.result())
	}
	rep.Loop = merged
}

// outcomeUnknown reports whether a failed call may still have committed on the
// server: it timed out, lost its connection, or failed with no status code,
// rather than being refused.
func outcomeUnknown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable, codes.Unknown:
		return true
	}
	return false
}

// suspendActors suspends every actor that resumed, or may have, so the next
// run finds them with no loop running. Suspending an actor that is already
// suspended succeeds.
func (r *runner) suspendActors(ctx context.Context, actors []*actorRun, rep *report) {
	var toSuspend []*actorRun
	for _, a := range actors {
		if a.resumed || a.mayBeRunning {
			toSuspend = append(toSuspend, a)
		}
	}
	res := newPhase()
	start := time.Now()
	forEach(len(toSuspend), len(toSuspend), func(i int) {
		err := r.backoff.retry(ctx, func(ctx context.Context) error {
			_, err := r.api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: toSuspend[i].name},
			})
			return err
		})
		if err != nil {
			res.fail(fmt.Errorf("suspend: %w", err))
			return
		}
		res.succeed()
	})
	rep.Suspend = res.result(len(toSuspend), time.Since(start))
	r.logf("suspend: %d/%d resumed or possibly resumed actors suspended in %v", rep.Suspend.Succeeded, len(toSuspend), rep.Suspend.Duration.Round(time.Millisecond))
}

// progress calls report every ProgressInterval until the returned stop func
// is called. A zero interval disables it.
func (r *runner) progress(ctx context.Context, report func()) (stop func()) {
	if r.cfg.ProgressInterval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(r.cfg.ProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				report()
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
	}
}
