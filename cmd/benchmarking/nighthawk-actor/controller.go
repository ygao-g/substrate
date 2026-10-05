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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	maxRunBody = 4 << 20
	// maxLogBytes caps each process's captured output; bytes past it are
	// counted and dropped so a chatty process cannot exhaust actor memory.
	maxLogBytes = 64 << 20
	// serviceReadyTimeout matches the ingress runner's wait for
	// nighthawk_service to listen.
	serviceReadyTimeout = 30 * time.Second
	stopGrace           = 10 * time.Second
)

type runState string

const (
	stateIdle    runState = "idle"
	stateRunning runState = "running"
	stateDone    runState = "done"
	stateFailed  runState = "failed"
)

type config struct {
	workDir        string
	serviceBin     string
	adaptiveBin    string
	serviceAddress string
}

// process is a started child process.
type process interface {
	// Wait blocks until the process exits and returns its exit code. The
	// error is non-nil only when the exit code could not be determined.
	// Safe to call more than once.
	Wait() (int, error)
	// Stop asks the process to exit, kills it after grace, and returns once
	// it has exited.
	Stop(grace time.Duration)
}

// runner starts child processes; tests replace it with a fake.
type runner interface {
	Start(name string, args []string, out io.Writer) (process, error)
}

// readyFunc blocks until addr accepts connections or ctx ends.
type readyFunc func(ctx context.Context, addr string) error

type runRequest struct {
	RunID string `json:"runId"`
	// Spec is the nighthawk.adaptive_load.AdaptiveLoadSessionSpec textproto
	// passed to the adaptive client as --spec-file.
	Spec string `json:"spec"`
}

type statusResponse struct {
	RunID            string     `json:"runId,omitempty"`
	State            runState   `json:"state"`
	ExitCode         *int       `json:"exitCode"`
	Error            string     `json:"error,omitempty"`
	ServiceLogBytes  int        `json:"serviceLogBytes"`
	AdaptiveLogBytes int        `json:"adaptiveLogBytes"`
	OutputBytes      int        `json:"outputBytes"`
	StartedAt        *time.Time `json:"startedAt,omitempty"`
	FinishedAt       *time.Time `json:"finishedAt,omitempty"`
}

type resultsResponse struct {
	statusResponse
	// Output is the adaptive client's --output-file, an
	// AdaptiveLoadSessionOutput textproto.
	Output          string `json:"output"`
	ServiceLog      string `json:"serviceLog"`
	AdaptiveLog     string `json:"adaptiveLog"`
	LogBytesDropped int    `json:"logBytesDropped"`
}

// run is one session. Fields other than the log buffers are guarded by
// controller.mu.
type run struct {
	id          string
	state       runState
	exitCode    *int
	err         string
	output      []byte
	serviceLog  *logBuffer
	adaptiveLog *logBuffer
	startedAt   time.Time
	finishedAt  time.Time
}

// controller owns at most one session at a time and keeps the last one's
// results until the next POST /run replaces them.
type controller struct {
	cfg          config
	runner       runner
	ready        readyFunc
	readyTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu  sync.Mutex
	cur *run
}

func newController(cfg config, r runner, ready readyFunc) *controller {
	ctx, cancel := context.WithCancel(context.Background())
	return &controller{
		cfg:          cfg,
		runner:       r,
		ready:        ready,
		readyTimeout: serviceReadyTimeout,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// close stops a running session and waits for its processes to exit.
func (c *controller) close() {
	c.cancel()
	c.wg.Wait()
}

func (c *controller) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("POST /run", c.handleRun)
	mux.HandleFunc("GET /status", c.handleStatus)
	mux.HandleFunc("GET /results", c.handleResults)
	return mux
}

func (c *controller) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRunBody)).Decode(&req); err != nil {
		http.Error(w, "decode run request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.RunID == "" || req.Spec == "" {
		http.Error(w, "runId and spec are required", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	if c.cur != nil && c.cur.state == stateRunning {
		id := c.cur.id
		c.mu.Unlock()
		http.Error(w, fmt.Sprintf("run %q is active", id), http.StatusConflict)
		return
	}
	cur := &run{
		id:          req.RunID,
		state:       stateRunning,
		serviceLog:  newLogBuffer(maxLogBytes),
		adaptiveLog: newLogBuffer(maxLogBytes),
		startedAt:   time.Now(),
	}
	c.cur = cur
	status := c.statusLocked()
	c.wg.Add(1)
	c.mu.Unlock()

	go func() {
		defer c.wg.Done()
		code, output, err := c.session(cur, req.Spec)
		c.finish(cur, code, output, err)
	}()
	writeJSON(w, http.StatusAccepted, status)
}

func (c *controller) handleStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	status := c.statusLocked()
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, status)
}

func (c *controller) handleResults(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.cur == nil:
		http.Error(w, "no run", http.StatusNotFound)
		return
	case c.cur.state == stateRunning:
		http.Error(w, fmt.Sprintf("run %q is active", c.cur.id), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, resultsResponse{
		statusResponse:  c.statusLocked(),
		Output:          string(c.cur.output),
		ServiceLog:      c.cur.serviceLog.String(),
		AdaptiveLog:     c.cur.adaptiveLog.String(),
		LogBytesDropped: c.cur.serviceLog.Dropped() + c.cur.adaptiveLog.Dropped(),
	})
}

func (c *controller) statusLocked() statusResponse {
	if c.cur == nil {
		return statusResponse{State: stateIdle}
	}
	s := statusResponse{
		RunID:            c.cur.id,
		State:            c.cur.state,
		ExitCode:         c.cur.exitCode,
		Error:            c.cur.err,
		ServiceLogBytes:  c.cur.serviceLog.Len(),
		AdaptiveLogBytes: c.cur.adaptiveLog.Len(),
		OutputBytes:      len(c.cur.output),
		StartedAt:        &c.cur.startedAt,
	}
	if !c.cur.finishedAt.IsZero() {
		s.FinishedAt = &c.cur.finishedAt
	}
	return s
}

func (c *controller) finish(cur *run, code *int, output []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur.exitCode = code
	cur.output = output
	cur.finishedAt = time.Now()
	switch {
	case err != nil:
		cur.state = stateFailed
		cur.err = err.Error()
	case code == nil || *code != 0:
		cur.state = stateFailed
	default:
		cur.state = stateDone
	}
}

// session runs nighthawk_service and the adaptive client for one spec, the
// sequence of the ingress runner's start_service and run_adaptive_client.
// It returns the adaptive client's exit code, when it ran, and its output
// file, when it wrote one.
func (c *controller) session(cur *run, spec string) (*int, []byte, error) {
	dir, err := os.MkdirTemp(c.cfg.workDir, "run-")
	if err != nil {
		return nil, nil, fmt.Errorf("create run dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	specPath := filepath.Join(dir, "spec.textproto")
	outputPath := filepath.Join(dir, "output.textproto")
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		return nil, nil, fmt.Errorf("write spec: %w", err)
	}

	svc, err := c.runner.Start(c.cfg.serviceBin, []string{"--listen", c.cfg.serviceAddress}, cur.serviceLog)
	if err != nil {
		return nil, nil, fmt.Errorf("start %s: %w", c.cfg.serviceBin, err)
	}
	defer svc.Stop(stopGrace)

	readyCtx, cancel := context.WithTimeout(c.ctx, c.readyTimeout)
	err = c.ready(readyCtx, c.cfg.serviceAddress)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("%s not listening on %s: %w", c.cfg.serviceBin, c.cfg.serviceAddress, err)
	}

	client, err := c.runner.Start(c.cfg.adaptiveBin, []string{
		"--spec-file", specPath,
		"--output-file", outputPath,
		"--nighthawk-service-address", c.cfg.serviceAddress,
	}, cur.adaptiveLog)
	if err != nil {
		return nil, nil, fmt.Errorf("start %s: %w", c.cfg.adaptiveBin, err)
	}

	type exit struct {
		code int
		err  error
	}
	exited := make(chan exit, 1)
	go func() {
		code, err := client.Wait()
		exited <- exit{code, err}
	}()
	var res exit
	select {
	case res = <-exited:
	case <-c.ctx.Done():
		client.Stop(stopGrace)
		res = <-exited
		if res.err == nil {
			res.err = errors.New("stopped by actor shutdown")
		}
	}
	if res.err != nil {
		return nil, readOutput(outputPath), fmt.Errorf("%s: %w", c.cfg.adaptiveBin, res.err)
	}
	return &res.code, readOutput(outputPath), nil
}

// readOutput returns the output file's contents, or nil when the adaptive
// client wrote none.
func readOutput(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return b
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// logBuffer is a concurrency-safe, size-capped io.Writer. Writes never fail,
// so a full buffer never blocks or kills the child process writing to it.
type logBuffer struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	max     int
	dropped int
}

func newLogBuffer(max int) *logBuffer {
	return &logBuffer{max: max}
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keep := min(len(p), b.max-b.buf.Len())
	b.buf.Write(p[:keep])
	b.dropped += len(p) - keep
	return len(p), nil
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *logBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *logBuffer) Dropped() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}
