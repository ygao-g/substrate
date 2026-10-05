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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testServiceBin  = "fake_service"
	testAdaptiveBin = "fake_adaptive"
	testServiceAddr = "127.0.0.1:18443"
)

// fakeProcess exits with code, or err, once release is closed.
type fakeProcess struct {
	code    int
	err     error
	release chan struct{}

	mu      sync.Mutex
	stopped bool
}

func (p *fakeProcess) Wait() (int, error) {
	<-p.release
	return p.code, p.err
}

func (p *fakeProcess) Stop(time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stopped {
		p.stopped = true
		select {
		case <-p.release:
		default:
			close(p.release)
		}
	}
}

func (p *fakeProcess) wasStopped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped
}

// fakeProgram scripts what one binary does when started.
type fakeProgram struct {
	startErr error
	code     int
	waitErr  error
	// log is written to the process's output at start.
	log string
	// output, when non-empty, is written to the --output-file argument.
	output string
	// block keeps the process running until Stop.
	block bool
}

type startCall struct {
	name string
	args []string
	// spec is the --spec-file contents at start, when the flag is present.
	spec string
}

type fakeRunner struct {
	programs map[string]fakeProgram

	mu    sync.Mutex
	calls []startCall
	procs map[string]*fakeProcess
}

func (r *fakeRunner) Start(name string, args []string, out io.Writer) (process, error) {
	prog := r.programs[name]
	call := startCall{name: name, args: args}
	if i := slices.Index(args, "--spec-file"); i >= 0 {
		b, err := os.ReadFile(args[i+1])
		if err != nil {
			return nil, err
		}
		call.spec = string(b)
	}
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.mu.Unlock()
	if prog.startErr != nil {
		return nil, prog.startErr
	}
	if prog.log != "" {
		_, _ = io.WriteString(out, prog.log)
	}
	if i := slices.Index(args, "--output-file"); i >= 0 && prog.output != "" {
		if err := os.WriteFile(args[i+1], []byte(prog.output), 0o600); err != nil {
			return nil, err
		}
	}
	p := &fakeProcess{code: prog.code, err: prog.waitErr, release: make(chan struct{})}
	if !prog.block {
		close(p.release)
	}
	r.mu.Lock()
	if r.procs == nil {
		r.procs = map[string]*fakeProcess{}
	}
	r.procs[name] = p
	r.mu.Unlock()
	return p, nil
}

func (r *fakeRunner) proc(name string) *fakeProcess {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.procs[name]
}

func (r *fakeRunner) startCalls() []startCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func readyOK(context.Context, string) error { return nil }

func readyNever(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func newTestController(t *testing.T, r runner, ready readyFunc) *controller {
	t.Helper()
	c := newController(config{
		workDir:        t.TempDir(),
		serviceBin:     testServiceBin,
		adaptiveBin:    testAdaptiveBin,
		serviceAddress: testServiceAddr,
	}, r, ready)
	c.readyTimeout = 50 * time.Millisecond
	t.Cleanup(c.close)
	return c
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func runBody(t *testing.T, id, spec string) string {
	t.Helper()
	b, err := json.Marshal(runRequest{RunID: id, Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// waitState polls /status until the run leaves stateRunning.
func waitState(t *testing.T, h http.Handler) statusResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var s statusResponse
		rec := do(t, h, http.MethodGet, "/status", "")
		if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
			t.Fatalf("decode /status %q: %v", rec.Body.String(), err)
		}
		if s.State != stateRunning {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("run still active after 5s")
	return statusResponse{}
}

func getResults(t *testing.T, h http.Handler) resultsResponse {
	t.Helper()
	rec := do(t, h, http.MethodGet, "/results", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /results = %d %q, want %d", rec.Code, rec.Body.String(), http.StatusOK)
	}
	var res resultsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode /results: %v", err)
	}
	return res
}

func TestRunLifecycle(t *testing.T) {
	t.Parallel()
	const spec = "nighthawk_traffic_template { uri: \"http://origin/\" }\n"
	tests := []struct {
		name     string
		service  fakeProgram
		adaptive fakeProgram
		ready    readyFunc
		// wantState, wantExit (nil for no exit code) and wantErr (a
		// substring, empty for none) describe the finished run.
		wantState    runState
		wantExit     *int
		wantErr      string
		wantOutput   string
		wantStarts   []string
		wantService  string
		wantAdaptive string
	}{
		{
			name:         "adaptive client exits 0",
			service:      fakeProgram{block: true, log: "service up\n"},
			adaptive:     fakeProgram{output: "output {}\n", log: "stage 1\n"},
			ready:        readyOK,
			wantState:    stateDone,
			wantExit:     ptr(0),
			wantOutput:   "output {}\n",
			wantStarts:   []string{testServiceBin, testAdaptiveBin},
			wantService:  "service up\n",
			wantAdaptive: "stage 1\n",
		},
		{
			name:         "adaptive client exits nonzero keeps partial output",
			service:      fakeProgram{block: true},
			adaptive:     fakeProgram{code: 3, output: "partial\n", log: "did not converge\n"},
			ready:        readyOK,
			wantState:    stateFailed,
			wantExit:     ptr(3),
			wantOutput:   "partial\n",
			wantStarts:   []string{testServiceBin, testAdaptiveBin},
			wantAdaptive: "did not converge\n",
		},
		{
			name:       "adaptive client exit code unknown",
			service:    fakeProgram{block: true},
			adaptive:   fakeProgram{waitErr: errors.New("wait failed")},
			ready:      readyOK,
			wantState:  stateFailed,
			wantErr:    "wait failed",
			wantStarts: []string{testServiceBin, testAdaptiveBin},
		},
		{
			name:       "service fails to start",
			service:    fakeProgram{startErr: errors.New("no such file")},
			ready:      readyOK,
			wantState:  stateFailed,
			wantErr:    "start fake_service: no such file",
			wantStarts: []string{testServiceBin},
		},
		{
			name:       "service never listens",
			service:    fakeProgram{block: true},
			ready:      readyNever,
			wantState:  stateFailed,
			wantErr:    "not listening on " + testServiceAddr,
			wantStarts: []string{testServiceBin},
		},
		{
			name:       "adaptive client fails to start",
			service:    fakeProgram{block: true},
			adaptive:   fakeProgram{startErr: errors.New("exec format error")},
			ready:      readyOK,
			wantState:  stateFailed,
			wantErr:    "start fake_adaptive: exec format error",
			wantStarts: []string{testServiceBin, testAdaptiveBin},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &fakeRunner{programs: map[string]fakeProgram{
				testServiceBin:  tc.service,
				testAdaptiveBin: tc.adaptive,
			}}
			h := newTestController(t, r, tc.ready).handler()

			rec := do(t, h, http.MethodPost, "/run", runBody(t, "run-1", spec))
			if rec.Code != http.StatusAccepted {
				t.Fatalf("POST /run = %d %q, want %d", rec.Code, rec.Body.String(), http.StatusAccepted)
			}
			s := waitState(t, h)
			if s.State != tc.wantState {
				t.Errorf("state = %q, want %q", s.State, tc.wantState)
			}
			if got, want := derefOr(s.ExitCode, -1), derefOr(tc.wantExit, -1); got != want {
				t.Errorf("exitCode = %d, want %d (-1 is null)", got, want)
			}
			if tc.wantErr == "" && s.Error != "" {
				t.Errorf("error = %q, want none", s.Error)
			}
			if !strings.Contains(s.Error, tc.wantErr) {
				t.Errorf("error = %q, want it to contain %q", s.Error, tc.wantErr)
			}
			if s.FinishedAt == nil {
				t.Errorf("finishedAt = nil, want a time")
			}

			res := getResults(t, h)
			if res.RunID != "run-1" {
				t.Errorf("results runId = %q, want %q", res.RunID, "run-1")
			}
			if res.Output != tc.wantOutput {
				t.Errorf("results output = %q, want %q", res.Output, tc.wantOutput)
			}
			if res.OutputBytes != len(tc.wantOutput) {
				t.Errorf("results outputBytes = %d, want %d", res.OutputBytes, len(tc.wantOutput))
			}
			if res.ServiceLog != tc.wantService {
				t.Errorf("results serviceLog = %q, want %q", res.ServiceLog, tc.wantService)
			}
			if res.AdaptiveLog != tc.wantAdaptive {
				t.Errorf("results adaptiveLog = %q, want %q", res.AdaptiveLog, tc.wantAdaptive)
			}

			var started []string
			for _, c := range r.startCalls() {
				started = append(started, c.name)
			}
			if !slices.Equal(started, tc.wantStarts) {
				t.Errorf("started %v, want %v", started, tc.wantStarts)
			}
			if p := r.proc(testServiceBin); p != nil && !p.wasStopped() {
				t.Errorf("nighthawk_service still running after the run ended, want stopped")
			}
		})
	}
}

func TestRunCommandLines(t *testing.T) {
	t.Parallel()
	const spec = "convergence_deadline { seconds: 600 }\n"
	r := &fakeRunner{programs: map[string]fakeProgram{
		testServiceBin:  {block: true},
		testAdaptiveBin: {},
	}}
	h := newTestController(t, r, readyOK).handler()
	if rec := do(t, h, http.MethodPost, "/run", runBody(t, "run-args", spec)); rec.Code != http.StatusAccepted {
		t.Fatalf("POST /run = %d %q, want %d", rec.Code, rec.Body.String(), http.StatusAccepted)
	}
	waitState(t, h)

	calls := r.startCalls()
	if len(calls) != 2 {
		t.Fatalf("got %d process starts, want 2", len(calls))
	}
	tests := []struct {
		name     string
		got      startCall
		wantName string
		// wantFlags maps each flag to its value; "" means any non-empty value.
		wantFlags map[string]string
		wantSpec  string
	}{
		{
			name:      "nighthawk_service",
			got:       calls[0],
			wantName:  testServiceBin,
			wantFlags: map[string]string{"--listen": testServiceAddr},
		},
		{
			name:     "adaptive client",
			got:      calls[1],
			wantName: testAdaptiveBin,
			wantFlags: map[string]string{
				"--spec-file":                 "",
				"--output-file":               "",
				"--nighthawk-service-address": testServiceAddr,
			},
			wantSpec: spec,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got.name != tc.wantName {
				t.Errorf("binary = %q, want %q", tc.got.name, tc.wantName)
			}
			if got, want := len(tc.got.args), 2*len(tc.wantFlags); got != want {
				t.Errorf("args %v has %d elements, want %d", tc.got.args, got, want)
			}
			for flag, want := range tc.wantFlags {
				i := slices.Index(tc.got.args, flag)
				if i < 0 || i+1 >= len(tc.got.args) {
					t.Errorf("args %v lack %s", tc.got.args, flag)
					continue
				}
				got := tc.got.args[i+1]
				if (want == "" && got == "") || (want != "" && got != want) {
					t.Errorf("%s = %q, want %q", flag, got, want)
				}
			}
			if tc.got.spec != tc.wantSpec {
				t.Errorf("spec file = %q, want %q", tc.got.spec, tc.wantSpec)
			}
		})
	}
}

func TestRunRejectsBadRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
	}{
		{name: "GET is not allowed", method: http.MethodGet, wantCode: http.StatusMethodNotAllowed},
		{name: "empty body", method: http.MethodPost, body: "", wantCode: http.StatusBadRequest},
		{name: "not JSON", method: http.MethodPost, body: "spec {}", wantCode: http.StatusBadRequest},
		{name: "missing spec", method: http.MethodPost, body: `{"runId":"r"}`, wantCode: http.StatusBadRequest},
		{name: "missing runId", method: http.MethodPost, body: `{"spec":"s"}`, wantCode: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &fakeRunner{}
			h := newTestController(t, r, readyOK).handler()
			rec := do(t, h, tc.method, "/run", tc.body)
			if rec.Code != tc.wantCode {
				t.Errorf("%s /run = %d, want %d", tc.method, rec.Code, tc.wantCode)
			}
			if got := len(r.startCalls()); got != 0 {
				t.Errorf("started %d processes, want 0", got)
			}
		})
	}
}

func TestIdleEndpoints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
		wantBody string
	}{
		{name: "readyz", method: http.MethodGet, path: "/readyz", wantCode: http.StatusOK, wantBody: "ok\n"},
		{name: "status before any run", method: http.MethodGet, path: "/status", wantCode: http.StatusOK, wantBody: `{"state":"idle","exitCode":null,"serviceLogBytes":0,"adaptiveLogBytes":0,"outputBytes":0}` + "\n"},
		{name: "results before any run", method: http.MethodGet, path: "/results", wantCode: http.StatusNotFound, wantBody: "no run\n"},
		{name: "readyz rejects POST", method: http.MethodPost, path: "/readyz", wantCode: http.StatusMethodNotAllowed},
		{name: "unknown path", method: http.MethodGet, path: "/nope", wantCode: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestController(t, &fakeRunner{}, readyOK).handler()
			rec := do(t, h, tc.method, tc.path, "")
			if rec.Code != tc.wantCode {
				t.Errorf("%s %s = %d, want %d", tc.method, tc.path, rec.Code, tc.wantCode)
			}
			if tc.wantBody != "" && rec.Body.String() != tc.wantBody {
				t.Errorf("%s %s body = %q, want %q", tc.method, tc.path, rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func TestActiveRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// during runs against the handler while the adaptive client is
		// blocked; it returns the HTTP status to check.
		during   func(t *testing.T, h http.Handler) int
		wantCode int
	}{
		{
			name: "second run is refused",
			during: func(t *testing.T, h http.Handler) int {
				return do(t, h, http.MethodPost, "/run", runBody(t, "run-2", "s")).Code
			},
			wantCode: http.StatusConflict,
		},
		{
			name: "results are not ready",
			during: func(t *testing.T, h http.Handler) int {
				return do(t, h, http.MethodGet, "/results", "").Code
			},
			wantCode: http.StatusConflict,
		},
		{
			name: "status reports running",
			during: func(t *testing.T, h http.Handler) int {
				var s statusResponse
				rec := do(t, h, http.MethodGet, "/status", "")
				if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
					t.Fatalf("decode /status: %v", err)
				}
				if s.State != stateRunning || s.RunID != "run-1" {
					t.Errorf("status = %q/%q, want %q/%q", s.RunID, s.State, "run-1", stateRunning)
				}
				return rec.Code
			},
			wantCode: http.StatusOK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &fakeRunner{programs: map[string]fakeProgram{
				testServiceBin:  {block: true},
				testAdaptiveBin: {block: true},
			}}
			h := newTestController(t, r, readyOK).handler()
			if rec := do(t, h, http.MethodPost, "/run", runBody(t, "run-1", "s")); rec.Code != http.StatusAccepted {
				t.Fatalf("POST /run = %d, want %d", rec.Code, http.StatusAccepted)
			}
			waitStarted(t, r, testAdaptiveBin)

			if got := tc.during(t, h); got != tc.wantCode {
				t.Errorf("status code = %d, want %d", got, tc.wantCode)
			}

			r.proc(testAdaptiveBin).Stop(0)
			if s := waitState(t, h); s.State != stateDone || s.RunID != "run-1" {
				t.Errorf("after release: %q/%q, want %q/%q", s.RunID, s.State, "run-1", stateDone)
			}
		})
	}
}

func TestNextRunReplacesResults(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{programs: map[string]fakeProgram{
		testServiceBin:  {block: true},
		testAdaptiveBin: {output: "out\n"},
	}}
	h := newTestController(t, r, readyOK).handler()
	for _, id := range []string{"run-1", "run-2"} {
		if rec := do(t, h, http.MethodPost, "/run", runBody(t, id, "s")); rec.Code != http.StatusAccepted {
			t.Fatalf("POST /run %s = %d, want %d", id, rec.Code, http.StatusAccepted)
		}
		waitState(t, h)
		// Results survive repeated reads until the next run.
		for i := range 2 {
			if got := getResults(t, h).RunID; got != id {
				t.Errorf("read %d: results runId = %q, want %q", i, got, id)
			}
		}
	}
}

func TestCloseStopsActiveRun(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{programs: map[string]fakeProgram{
		testServiceBin:  {block: true},
		testAdaptiveBin: {block: true, code: 143},
	}}
	c := newTestController(t, r, readyOK)
	h := c.handler()
	if rec := do(t, h, http.MethodPost, "/run", runBody(t, "run-1", "s")); rec.Code != http.StatusAccepted {
		t.Fatalf("POST /run = %d, want %d", rec.Code, http.StatusAccepted)
	}
	waitStarted(t, r, testAdaptiveBin)
	c.close()

	s := waitState(t, h)
	if s.State != stateFailed {
		t.Errorf("state = %q, want %q", s.State, stateFailed)
	}
	if want := "stopped by actor shutdown"; !strings.Contains(s.Error, want) {
		t.Errorf("error = %q, want it to contain %q", s.Error, want)
	}
	for _, bin := range []string{testServiceBin, testAdaptiveBin} {
		if !r.proc(bin).wasStopped() {
			t.Errorf("%s not stopped, want stopped", bin)
		}
	}
}

func TestLogBuffer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		max         int
		writes      []string
		wantString  string
		wantDropped int
	}{
		{name: "under the cap", max: 10, writes: []string{"abc", "de"}, wantString: "abcde"},
		{name: "write crosses the cap", max: 4, writes: []string{"abc", "def"}, wantString: "abcd", wantDropped: 2},
		{name: "writes after the cap", max: 3, writes: []string{"abc", "def", "g"}, wantString: "abc", wantDropped: 4},
		{name: "zero cap", max: 0, writes: []string{"abc"}, wantString: "", wantDropped: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newLogBuffer(tc.max)
			for _, w := range tc.writes {
				n, err := b.Write([]byte(w))
				if n != len(w) || err != nil {
					t.Errorf("Write(%q) = %d, %v, want %d, nil", w, n, err, len(w))
				}
			}
			if got := b.String(); got != tc.wantString {
				t.Errorf("String() = %q, want %q", got, tc.wantString)
			}
			if got := b.Len(); got != len(tc.wantString) {
				t.Errorf("Len() = %d, want %d", got, len(tc.wantString))
			}
			if got := b.Dropped(); got != tc.wantDropped {
				t.Errorf("Dropped() = %d, want %d", got, tc.wantDropped)
			}
		})
	}
}

// waitStarted waits until the fake runner has started bin.
func waitStarted(t *testing.T, r *fakeRunner, bin string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.proc(bin) != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s not started after 5s", bin)
}

func ptr[T any](v T) *T { return &v }

func derefOr(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}
