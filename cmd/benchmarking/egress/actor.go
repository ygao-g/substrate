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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// actorCommand is the workload of the egress scale test. Once told to start,
// it opens every endpoint, one per interval, then on one goroutine per
// endpoint sends it an HTTP GET once per period of C intervals, and counts
// what happened under one lock.
//
// The loop starts on POST /start rather than at boot: every actor restores
// from a golden snapshot taken after the template's warmup, so a loop started
// at boot would run in the golden actor, which has no egress policy, and be
// frozen into every actor's snapshot, failed requests included.
func actorCommand() *cobra.Command {
	var listen string
	cmd := &cobra.Command{
		Use:   "actor",
		Short: "Hold one tunnel per endpoint open through the gateway and count what happened",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runActor(cmd.Context(), listen)
		},
	}
	cmd.Flags().StringVar(&listen, "listen", ":80", "Address to serve the start, stats and stop routes on.")
	return cmd
}

// runActor serves the actor's routes on listen until ctx ends.
func runActor(ctx context.Context, listen string) error {
	srv := &http.Server{
		Addr:              listen,
		Handler:           newServer().handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("egress-tunnels actor listening", "addr", listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}

// maxBodyBytes bounds what the actor reads, from a /start body or from an
// endpoint's response.
const maxBodyBytes = 1 << 20

// server serves the actor routes. At most one loop runs at a time.
type server struct {
	// endpointURL maps an endpoint number and scheme to the URL the loop
	// requests. Tests point it at local servers.
	endpointURL func(i int, scheme string) string
	// rootCAs verifies HTTPS endpoints; nil uses the system roots, which
	// SSL_CERT_FILE extends with the gateway's MITM CA. Tests set their own.
	rootCAs *x509.CertPool
	// dial opens the loop's connections; nil uses a plain net.Dialer. Tests
	// send the in-cluster endpoint names to local listeners through it.
	dial dialFunc

	mu  sync.Mutex
	run *loopRun // nil when no loop is running
}

func newServer() *server {
	return &server{endpointURL: endpointURL}
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+readyzRoute, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST "+startRoute, s.handleStart)
	mux.HandleFunc("GET "+metricsRoute, s.handleMetrics)
	mux.HandleFunc("POST "+stopRoute, s.handleStop)
	return mux
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		http.Error(w, "decoding start request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := req.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run != nil {
		http.Error(w, "a loop is already running; POST "+stopRoute+" first", http.StatusConflict)
		return
	}
	urls := make([]string, req.Endpoints)
	for i := range urls {
		urls[i] = s.endpointURL(i, req.urlScheme())
	}
	run, err := startLoop(req, urls, s.rootCAs, s.dial)
	if err != nil {
		http.Error(w, "starting loop: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.run = run
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run == nil {
		http.Error(w, noLoopMessage, http.StatusNotFound)
		return
	}
	run.serveMetrics(w, r)
}

func (s *server) handleStop(w http.ResponseWriter, r *http.Request) {
	// Held across the wait so a /start cannot slip in before the loop exits.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run == nil {
		http.Error(w, noLoopMessage, http.StatusNotFound)
		return
	}
	err := s.run.stop(r.Context())
	s.run = nil
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// loopRun is one started loop. What it measures lives in its instruments;
// it keeps only what the next measurement needs.
type loopRun struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup // one goroutine per endpoint
	started   time.Time
	transport *http.Transport
	metrics   *loopMetrics

	mu sync.Mutex
	// requests counts each endpoint's finished requests, for rounds and to
	// tell an endpoint's first request from its repeats.
	requests []int64
	opened   int
	// longestGap and gaps hold the longest gap seen overall and per
	// endpoint, so the gauges only ever move up.
	longestGap time.Duration
	gaps       []time.Duration
	last       []time.Time // start of each endpoint's most recent request
}

// newLoopRun is a loop over c endpoints with fresh instruments and no
// goroutines yet; startLoop runs it.
func newLoopRun(c int, scheme string) (*loopRun, error) {
	m, err := newLoopMetrics(c, scheme)
	if err != nil {
		return nil, err
	}
	return &loopRun{
		started:  time.Now(),
		metrics:  m,
		requests: make([]int64, c),
		gaps:     make([]time.Duration, c),
		last:     make([]time.Time, c),
	}, nil
}

func startLoop(req startRequest, urls []string, roots *x509.CertPool, dial dialFunc) (*loopRun, error) {
	timeoutMs := req.RequestTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = defaultRequestTimeoutMs
	}
	r, err := newLoopRun(len(urls), req.urlScheme())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.transport = cancel, newTransport(req.NewConnPerRequest, roots, dial)
	client := &http.Client{
		Transport: r.transport,
		// Count a redirect as the endpoint's answer rather than following it
		// somewhere the egress policy may not allow.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond
	interval := time.Duration(req.IntervalMs) * time.Millisecond
	period := interval * time.Duration(len(urls))
	for i, u := range urls {
		r.wg.Go(func() { r.endpoint(ctx, client, i, u, timeout, interval*time.Duration(i), period) })
	}
	return r, nil
}

// dialFunc opens one connection, as net.Dialer.DialContext does.
type dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// newTransport keeps one connection alive per endpoint, or none at all when
// newConnPerRequest is set, and verifies HTTPS endpoints against roots. It
// ignores proxy environment variables: the sandbox's egress capture is the
// only proxy under test.
func newTransport(newConnPerRequest bool, roots *x509.CertPool, dial dialFunc) *http.Transport {
	if dial == nil {
		dial = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	return &http.Transport{
		DialContext:         dial,
		TLSClientConfig:     &tls.Config{RootCAs: roots},
		ForceAttemptHTTP2:   false, // HTTP/1.1 over either scheme: one request per connection at a time
		MaxIdleConns:        0,     // no total cap, so C endpoints keep C connections
		MaxIdleConnsPerHost: 1,     // one request in flight per endpoint, so one per endpoint is enough
		// No IdleConnTimeout: a kept-alive connection the client closed for
		// idling would redial like one the gateway reset, and the gateway's
		// stream idle timeout is what the run measures.
		IdleConnTimeout:   0,
		DisableKeepAlives: newConnPerRequest,
		// No TLSHandshakeTimeout: the request's own timeout bounds the
		// handshake, so a stalled one is counted as a timeout like any other.
	}
}

// endpoint waits offset, opens endpoint i, then requests it once per period
// until ctx ends. The opens are one interval apart across endpoints, so C
// handshakes do not reach the gateway at once. At most one request is in
// flight per endpoint, and the period is counted from the start of the open:
// a request that outlasts the period delays only this endpoint, whose next
// request starts as soon as the slow one returns.
func (r *loopRun) endpoint(ctx context.Context, client *http.Client, i int, u string, timeout, offset, period time.Duration) {
	if !pause(ctx, offset) {
		return
	}
	var tick <-chan time.Time
	if period > 0 {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		tick = ticker.C
	}
	r.request(ctx, client, i, u, timeout)
	if ctx.Err() != nil {
		return // stopped mid-open: not an open
	}
	r.mu.Lock()
	r.opened++
	r.metrics.endpointsOpened.Add(context.Background(), 1)
	if r.opened == len(r.requests) {
		r.metrics.openingDone.Record(context.Background(), time.Since(r.started).Seconds())
	}
	r.mu.Unlock()
	for ctx.Err() == nil {
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-tick:
			}
		}
		r.request(ctx, client, i, u, timeout)
	}
}

// pause waits d, or until ctx ends, and reports whether to go on.
func pause(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// requestTrace collects what httptrace reports about one request. The dial,
// and with it the DNS callbacks, can still be running on another goroutine
// after a timed-out request returns, hence the lock.
type requestTrace struct {
	mu         sync.Mutex
	gotConn    bool
	newConn    bool
	dnsStart   time.Time
	dns        time.Duration
	didDNS     bool
	tlsStart   time.Time
	tls        time.Duration
	tlsStarted bool
	didTLS     bool
}

func (t *requestTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.gotConn, t.newConn = true, !info.Reused
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dnsStart = time.Now()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dns, t.didDNS = time.Since(t.dnsStart), true
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.tlsStart, t.tlsStarted = time.Now(), true
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if err == nil {
				t.tls, t.didTLS = time.Since(t.tlsStart), true
			}
		},
	}
}

func (r *loopRun) request(ctx context.Context, client *http.Client, i int, u string, timeout time.Duration) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	trace := &requestTrace{}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(reqCtx, trace.clientTrace()), http.MethodGet, u, nil)
	if err != nil {
		r.record(i, 0, 0, err, trace)
		return
	}

	start := time.Now()
	r.mu.Lock()
	if last := r.last[i]; !last.IsZero() {
		gap := start.Sub(last)
		if gap > r.longestGap {
			r.longestGap = gap
			r.metrics.longestGap.Record(ctx, gap.Seconds())
		}
		if gap > r.gaps[i] {
			r.gaps[i] = gap
			r.metrics.endpointLongestGap.Record(ctx, gap.Seconds(), r.metrics.endpointOnly[i])
		}
	}
	r.last[i] = start
	r.mu.Unlock()
	status := 0
	resp, err := client.Do(req)
	if err == nil {
		status = resp.StatusCode
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		resp.Body.Close()
	}
	latency := time.Since(start)
	if ctx.Err() != nil {
		// Stopped mid-request: the failure is the stop, not the egress path.
		return
	}
	r.record(i, latency, status, err, trace)
}

func (r *loopRun) record(i int, latency time.Duration, status int, err error, trace *requestTrace) {
	trace.mu.Lock()
	newConn := trace.gotConn && trace.newConn
	didDNS, dns := trace.didDNS, trace.dns
	tlsStarted, didTLS, tlsTime := trace.tlsStarted, trace.didTLS, trace.tls
	trace.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	m, ctx := r.metrics, context.Background()
	first := r.requests[i] == 0
	r.requests[i]++
	if newConn {
		m.connectionsOpened.Add(ctx, 1)
	}
	if didDNS {
		m.dns.Record(ctx, dns.Seconds())
	}
	if didTLS {
		m.tls.Record(ctx, tlsTime.Seconds())
	}
	class := classify(err, status)
	if class != "" && tlsStarted && !didTLS && !strings.HasPrefix(class, "tls: ") {
		// A failure inside the handshake, such as the gateway closing a
		// connection whose SNI no rule allows, is told apart from the same
		// error in an HTTP exchange.
		class = "tls handshake: " + class
	}
	if class == "" {
		m.requests.Add(ctx, 1, m.success)
		m.endpointRequests.Add(ctx, 1, m.endpointResult[i][0])
		if first {
			m.opening.Record(ctx, latency.Seconds())
		} else {
			m.latency.Record(ctx, latency.Seconds())
		}
		return
	}
	m.requests.Add(ctx, 1, m.failure)
	m.requestErrors.Add(ctx, 1, m.class(class))
	m.endpointRequests.Add(ctx, 1, m.endpointResult[i][1])
}

// serveMetrics answers GET /metrics: the loop's families, plus the
// per-endpoint ones with ?endpoints=1. The gauges that are derived rather
// than recorded are set first, under the lock, which is held for the gather
// only, never for the write.
func (r *loopRun) serveMetrics(w http.ResponseWriter, req *http.Request) {
	withEndpoints := req.URL.Query().Get("endpoints") == "1"
	ctx := context.Background()
	r.mu.Lock()
	rounds := int64(0)
	if len(r.requests) > 0 {
		rounds = slices.Min(r.requests)
	}
	r.metrics.rounds.Record(ctx, rounds)
	r.metrics.elapsed.Record(ctx, time.Since(r.started).Seconds())
	mfs, err := r.metrics.gather(withEndpoints)
	r.mu.Unlock()
	if err != nil {
		http.Error(w, "gathering metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeExposition(w, req, mfs)
}

// stop cancels the loop, waits for every endpoint's goroutine to exit, and
// releases its instruments.
func (r *loopRun) stop(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	if r.transport != nil {
		r.transport.CloseIdleConnections()
	}
	// Nothing is attached to the providers, so there is nothing to flush and
	// a failed shutdown loses nothing.
	_ = r.metrics.shutdown(ctx)
	return nil
}
