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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/common/expfmt"
)

// target is an endpoint that counts the TCP connections opened to it.
type target struct {
	*httptest.Server
	conns    atomic.Int64
	requests atomic.Int64
}

func newTarget(t *testing.T, h http.HandlerFunc) *target {
	t.Helper()
	return newTargetConnState(t, h, nil)
}

// newTargetConnState is newTarget with onState called on every connection
// state change, after the connection count.
func newTargetConnState(t *testing.T, h http.HandlerFunc, onState func(net.Conn, http.ConnState)) *target {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
		if onState != nil {
			onState(c, s)
		}
	}
	tg.Start()
	t.Cleanup(tg.Close)
	return tg
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }

// quietLog discards a test server's own error log.
var quietLog = log.New(io.Discard, "", 0)

// host is the target's host:port.
func (tg *target) host() string { return tg.Listener.Addr().String() }

// serve returns the actor's handler with endpoint i mapped to hosts[i], a
// host:port, in place of the in-cluster Service names. The URL keeps the
// scheme the loop asks for.
func serve(hosts ...string) http.Handler {
	return serveTLS(nil, hosts...)
}

// serveTLS is serve, verifying HTTPS endpoints against roots.
func serveTLS(roots *x509.CertPool, hosts ...string) http.Handler {
	s := newServer()
	s.rootCAs = roots
	s.endpointURL = func(i int, scheme string) string { return scheme + "://" + hosts[i] + "/" }
	return s.handler()
}

// newTLSTarget is newTarget serving HTTPS with httptest's certificate, which
// the returned pool trusts.
func newTLSTarget(t *testing.T, h http.HandlerFunc) (*target, *x509.CertPool) {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.Config.ErrorLog = quietLog // failed handshakes are what some tests expect
	tg.StartTLS()
	t.Cleanup(tg.Close)
	roots := x509.NewCertPool()
	roots.AddCert(tg.Certificate())
	return tg, roots
}

func post(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, &buf))
	return rec
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// runLoop starts a loop with req, lets it run until cond holds on a live
// scrape (or fails after 10s), then stops it and returns the last scrape
// before the stop, as the driver reads it.
func runLoop(t *testing.T, h http.Handler, req startRequest, cond func(*loopView) bool) *loopView {
	t.Helper()
	if rec := post(t, h, startRoute, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := scrapeView(t, h)
		if cond(s) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached; last scrape %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return stopLoop(t, h)
}

func atLeast(n int64) func(*loopView) bool {
	return func(s *loopView) bool { return s.Requests >= n }
}

func TestKeepAliveReusesOneConnectionPerEndpoint(t *testing.T) {
	t.Parallel()
	targets := []*target{newTarget(t, okHandler), newTarget(t, okHandler), newTarget(t, okHandler)}
	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.host())
	}
	h := serve(hosts...)

	s := runLoop(t, h, startRequest{Endpoints: len(hosts)}, func(s *loopView) bool { return s.Rounds >= 10 })
	if s.Scheme != schemeHTTP || s.TLS.Count != 0 {
		t.Errorf("a start with no scheme ran over %q with %d TLS handshakes, want http with none", s.Scheme, s.TLS.Count)
	}

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v", s.Successes, s.Requests, s.Errors)
	}
	for i, tg := range targets {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("target %d saw %d connections, want 1 kept alive", i, got)
		}
	}
	if s.NewConns != int64(len(targets)) {
		t.Errorf("NewConns = %d, want %d (one per endpoint)", s.NewConns, len(targets))
	}
	if s.Opening.Count != uint64(len(targets)) || int64(s.Opening.Count+s.Latency.Count) != s.Successes {
		t.Errorf("opening %d and latency %d samples for %d successes; want one opening per endpoint and the rest latency", s.Opening.Count, s.Latency.Count, s.Successes)
	}
	for i, e := range s.Endpoints {
		if e.Requests < 10 {
			t.Errorf("endpoint %d got %d requests, want at least the %d rounds the loop was stopped at", i, e.Requests, 10)
		}
	}
}

func TestEveryEndpointIsOpenedAndRevisited(t *testing.T) {
	t.Parallel()
	targets := []*target{newTarget(t, okHandler), newTarget(t, okHandler), newTarget(t, okHandler)}
	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.host())
	}
	h := serve(hosts...)

	s := runLoop(t, h, startRequest{Endpoints: len(hosts), IntervalMs: 2}, func(s *loopView) bool { return s.Rounds >= 2 })

	if s.Opened != int64(len(hosts)) {
		t.Errorf("Opened = %d, want %d", s.Opened, len(hosts))
	}
	if s.OpeningDone <= 0 || s.OpeningDone > s.Elapsed {
		t.Errorf("OpeningDone = %v, want within (0, %v]", s.OpeningDone, s.Elapsed)
	}
	for i, e := range s.Endpoints {
		if e.Requests < 2 || e.LongestGap <= 0 {
			t.Errorf("endpoint %d: %d requests, longest gap %v; want at least 2 and a gap between them", i, e.Requests, e.LongestGap)
		}
	}
	if s.LongestGap <= 0 {
		t.Errorf("LongestGap = %v, want the gap between two requests of one endpoint", s.LongestGap)
	}
	if s.Opening.Count != uint64(len(hosts)) || int64(s.Opening.Count+s.Latency.Count) != s.Successes {
		t.Errorf("opening %d and latency %d samples for %d successes; want one opening per endpoint and the rest latency", s.Opening.Count, s.Latency.Count, s.Successes)
	}
}

func TestStopDuringOpeningDoesNotCountTheCancelledOpen(t *testing.T) {
	t.Parallel()
	first := newTarget(t, okHandler)
	blocked := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // holds the second open until the client gives up
	})
	h := serve(first.host(), blocked.host())

	s := runLoop(t, h, startRequest{Endpoints: 2}, func(s *loopView) bool { return s.Opened >= 1 })
	if s.Opened != 1 {
		t.Errorf("Opened = %d after stopping during the second open, want 1", s.Opened)
	}
	if s.Rounds != 0 || s.OpeningDone != 0 || s.Endpoints[1].Requests != 0 {
		t.Errorf("Rounds = %d, OpeningDone = %v, endpoint 1 requests = %d during the opening, want 0, 0 and 0", s.Rounds, s.OpeningDone, s.Endpoints[1].Requests)
	}
}

func TestNewConnPerRequest(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())

	s := runLoop(t, h, startRequest{Endpoints: 1, NewConnPerRequest: true}, atLeast(10))

	if s.NewConns != s.Requests {
		t.Errorf("NewConns = %d, want one per request (%d)", s.NewConns, s.Requests)
	}
	if got := tg.conns.Load(); got < s.Requests {
		t.Errorf("target saw %d connections for %d requests, want at least one each", got, s.Requests)
	}
}

func TestRequestTimeoutIsClassified(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	slow := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	h := serve(slow.host())

	s := runLoop(t, h, startRequest{Endpoints: 1, RequestTimeoutMs: 20}, atLeast(2))

	if s.Errors["timeout"] != s.Requests || s.Successes != 0 {
		t.Errorf("errors %v, successes %d for %d requests; want every request a timeout", s.Errors, s.Successes, s.Requests)
	}
	if s.Latency.Count != 0 {
		t.Errorf("failed requests recorded %d latency samples, want 0", s.Latency.Count)
	}
	if s.Endpoints[0].Errors != s.Requests {
		t.Errorf("endpoint errors = %d, want %d", s.Endpoints[0].Errors, s.Requests)
	}
}

func TestNon200IsAnError(t *testing.T) {
	t.Parallel()
	ok := newTarget(t, okHandler)
	failing := newTarget(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "egress denied", http.StatusServiceUnavailable)
	})
	h := serve(ok.host(), failing.host())

	s := runLoop(t, h, startRequest{Endpoints: 2}, atLeast(10))

	if s.Errors["HTTP 503"] == 0 {
		t.Errorf("errors = %v, want HTTP 503 counted", s.Errors)
	}
	if s.Endpoints[0].Errors != 0 || s.Endpoints[1].Errors != s.Endpoints[1].Requests {
		t.Errorf("endpoints = %+v, want only the second one failing", s.Endpoints)
	}
}

func TestIntervalPacesRequests(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())

	start := time.Now()
	s := runLoop(t, h, startRequest{Endpoints: 1, IntervalMs: 20}, atLeast(5))
	elapsed := time.Since(start)

	// n requests on one endpoint, one per 20ms period, cannot finish in under (n-1)*20ms.
	if minimum := time.Duration(s.Requests-1) * 20 * time.Millisecond; elapsed < minimum {
		t.Errorf("%d requests took %v, want at least %v with a 20ms interval", s.Requests, elapsed, minimum)
	}
}

func TestDNSRecordedOnlyForHostnames(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	u, err := url.Parse(tg.URL)
	if err != nil {
		t.Fatal(err)
	}
	byIP := runLoop(t, serve(tg.host()), startRequest{Endpoints: 1}, atLeast(3))
	if byIP.DNS.Count != 0 {
		t.Errorf("an IP target recorded %d DNS lookups, want 0", byIP.DNS.Count)
	}

	byName := serve("localhost:" + u.Port())
	s := runLoop(t, byName, startRequest{Endpoints: 1, NewConnPerRequest: true}, atLeast(3))
	if s.DNS.Count == 0 {
		t.Errorf("a hostname target with a new connection per request recorded no DNS lookups (stats %+v)", s)
	}
}

func TestStartStopLifecycle(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())
	req := startRequest{Endpoints: 1}

	if rec := get(h, metricsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /metrics before start = %d, want 404", rec.Code)
	}
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNotFound {
		t.Errorf("POST /stop before start = %d, want 404", rec.Code)
	}

	if rec := post(t, h, startRoute, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	for deadline := time.Now().Add(10 * time.Second); scrapeView(t, h).Requests < 5; {
		if time.Now().After(deadline) {
			t.Fatal("the loop did not reach 5 requests")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /stop = %d %s", rec.Code, rec.Body)
	}
	atStop := tg.requests.Load()
	time.Sleep(20 * time.Millisecond)
	// A request the stop cut short can still reach the target after /stop
	// returns; nothing else may.
	if extra := tg.requests.Load() - atStop; extra > 1 {
		t.Errorf("target got %d requests after the stop, want at most the one it cut short", extra)
	}

	if rec := post(t, h, startRoute, req); rec.Code != http.StatusNoContent {
		t.Fatalf("restart = %d %s", rec.Code, rec.Body)
	}
	if rec := post(t, h, startRoute, req); rec.Code != http.StatusConflict {
		t.Errorf("second /start while running = %d, want 409", rec.Code)
	}
	second := scrapeView(t, h)
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNoContent {
		t.Errorf("stop after restart = %d, want 204", rec.Code)
	}
	// The target saw every request of both runs (plus any cut short by a
	// stop), so a second run that started from zero counts no more than the
	// target saw after the first run.
	if sinceRestart := tg.requests.Load() - atStop; second.Requests > sinceRestart {
		t.Errorf("restarted loop counted %d requests, more than the %d sent since the restart: the old counts were kept", second.Requests, sinceRestart)
	}
}

func TestStartRejectsBadRequests(t *testing.T) {
	t.Parallel()
	h := newServer().handler()
	for _, tc := range []struct{ name, body string }{
		{"not json", `not json`},
		{"empty object", `{}`},
		{"zero endpoints", `{"endpoints":0}`},
		{"too many endpoints", `{"endpoints":1025}`},
		{"unknown scheme", `{"endpoints":1,"scheme":"ftp"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, startRoute, strings.NewReader(tc.body)))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("POST /start %s = %d, want 400", tc.body, rec.Code)
			}
		})
	}
	if rec := get(h, readyzRoute); rec.Code != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", rec.Code)
	}
}

func TestDefaultURLsAreTheEndpointServices(t *testing.T) {
	t.Parallel()
	s := newServer()
	for _, i := range []int{0, 9, 255} {
		for _, scheme := range []string{schemeHTTP, schemeHTTPS} {
			if got, want := s.endpointURL(i, scheme), endpointURL(i, scheme); got != want {
				t.Errorf("endpoint %d over %s = %q, want %q", i, scheme, got, want)
			}
		}
	}
	if s.rootCAs != nil {
		t.Error("the actor's roots are set by default; they must stay nil so SSL_CERT_FILE applies")
	}
	if s.dial != nil {
		t.Error("the actor dials through a hook by default; it must stay nil so a plain dialer is used")
	}
}

func TestHTTPSKeepAliveHandshakesOncePerEndpoint(t *testing.T) {
	t.Parallel()
	a, _ := newTLSTarget(t, okHandler)
	b, _ := newTLSTarget(t, okHandler)
	roots := x509.NewCertPool()
	roots.AddCert(a.Certificate())
	roots.AddCert(b.Certificate())
	h := serveTLS(roots, a.host(), b.host())

	s := runLoop(t, h, startRequest{Endpoints: 2, Scheme: schemeHTTPS}, atLeast(20))

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v", s.Successes, s.Requests, s.Errors)
	}
	if s.Scheme != schemeHTTPS {
		t.Errorf("Scheme = %q, want https", s.Scheme)
	}
	if s.TLS.Count != 2 || s.NewConns != 2 {
		t.Errorf("%d TLS handshakes and %d new connections, want one of each per endpoint (2)", s.TLS.Count, s.NewConns)
	}
	for i, tg := range []*target{a, b} {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("target %d saw %d connections, want 1 kept alive", i, got)
		}
	}
}

func TestHTTPSNewConnHandshakesEveryRequest(t *testing.T) {
	t.Parallel()
	tg, roots := newTLSTarget(t, okHandler)
	h := serveTLS(roots, tg.host())

	s := runLoop(t, h, startRequest{Endpoints: 1, Scheme: schemeHTTPS, NewConnPerRequest: true}, atLeast(10))

	if s.Successes != s.Requests || int64(s.TLS.Count) != s.Requests {
		t.Errorf("%d successes and %d TLS handshakes for %d requests, want one handshake per successful request", s.Successes, s.TLS.Count, s.Requests)
	}
}

func TestHTTPSFailuresAreClassified(t *testing.T) {
	t.Parallel()
	bundle, err := generateTargetCert(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpointCert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	endpointRoots := x509.NewCertPool()
	endpointRoots.AppendCertsFromPEM(bundle.CA)
	// Serves the endpoints' certificate, which names no IP address.
	wrongName := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	wrongName.TLS = &tls.Config{Certificates: []tls.Certificate{endpointCert}}
	wrongName.Config.ErrorLog = quietLog
	wrongName.StartTLS()
	t.Cleanup(wrongName.Close)

	untrusted, _ := newTLSTarget(t, okHandler)
	plain := newTarget(t, okHandler)

	// Accepts, reads the ClientHello so closing sends a FIN rather than a
	// reset, and closes: a peer that refuses the handshake, as the gateway
	// does for an SNI no rule allows.
	closer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closer.Close() })
	go func() {
		for {
			conn, err := closer.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Read(make([]byte, 4096))
			conn.Close()
		}
	}()

	for _, tc := range []struct {
		name  string
		roots *x509.CertPool
		host  string
		want  string
	}{
		{"untrusted CA", x509.NewCertPool(), untrusted.host(), "tls: unknown authority"},
		{"hostname mismatch", endpointRoots, wrongName.host(), "tls: hostname mismatch"},
		{"plain HTTP server", x509.NewCertPool(), plain.host(), "tls: not a TLS server"},
		{"peer closes during handshake", x509.NewCertPool(), closer.Addr().String(), "tls handshake: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := serveTLS(tc.roots, tc.host)
			s := runLoop(t, h, startRequest{Endpoints: 1, Scheme: schemeHTTPS}, atLeast(2))
			var matched int64
			for class, n := range s.Errors {
				if strings.HasPrefix(class, tc.want) {
					matched += n
				}
			}
			if matched != s.Requests || s.Successes != 0 || s.TLS.Count != 0 {
				t.Errorf("errors %v, %d successes, %d handshakes for %d requests; want every request classed %q", s.Errors, s.Successes, s.TLS.Count, s.Requests, tc.want)
			}
		})
	}
}

// dialTo returns a dial function that connects host:port to addrs[host]
// instead, so a loop can request the in-cluster endpoint names against local
// listeners. TLS still verifies the name the loop asked for.
func dialTo(addrs map[string]string) dialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		real, ok := addrs[host]
		if !ok {
			return nil, fmt.Errorf("no local listener for %s", addr)
		}
		return (&net.Dialer{}).DialContext(ctx, network, real)
	}
}

// targetCertServer is an HTTPS target serving the endpoints' certificate.
func targetCertServer(t *testing.T, bundle *targetCertBundle) *target {
	t.Helper()
	cert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	tg.StartTLS()
	t.Cleanup(tg.Close)
	return tg
}

// TestEndpointsByNameOverBothSchemes runs the loop as the actor does in the
// cluster: the default endpoint URLs, eight endpoints, and over HTTPS the
// target's own certificate verified under each endpoint's name.
func TestEndpointsByNameOverBothSchemes(t *testing.T) {
	t.Parallel()
	const c = 8
	bundle, err := generateTargetCert(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle.CA) {
		t.Fatal("no CA in the bundle")
	}
	for _, scheme := range []string{schemeHTTP, schemeHTTPS} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()
			addrs := make(map[string]string, c)
			targets := make([]*target, c)
			for i := range c {
				if scheme == schemeHTTPS {
					targets[i] = targetCertServer(t, bundle)
				} else {
					targets[i] = newTarget(t, okHandler)
				}
				addrs[endpointHost(i)] = targets[i].host()
			}
			s := newServer()
			s.rootCAs = roots
			s.dial = dialTo(addrs)
			h := s.handler()

			const interval = 2 * time.Millisecond
			st := runLoop(t, h, startRequest{Endpoints: c, Scheme: scheme, IntervalMs: int64(interval / time.Millisecond)},
				func(s *loopView) bool { return s.Rounds >= 3 })

			if st.Opened != c || st.Rounds < 3 {
				t.Errorf("Opened = %d, Rounds = %d, want %d and at least 3", st.Opened, st.Rounds, c)
			}
			if st.Successes != st.Requests || len(st.Errors) != 0 {
				t.Errorf("successes %d of %d requests, errors %v", st.Successes, st.Requests, st.Errors)
			}
			if st.NewConns != c {
				t.Errorf("NewConns = %d, want one per endpoint (%d)", st.NewConns, c)
			}
			wantTLS := int64(0)
			if scheme == schemeHTTPS {
				wantTLS = c
			}
			if int64(st.TLS.Count) != wantTLS {
				t.Errorf("TLS handshakes = %d, want %d", st.TLS.Count, wantTLS)
			}
			for i, tg := range targets {
				if got := tg.conns.Load(); got != 1 {
					t.Errorf("endpoint %d saw %d connections, want 1 kept alive", i, got)
				}
			}
			// Every endpoint is requested once per period of C intervals; a gap
			// is about one period, never anywhere near Elapsed.
			if period := c * interval; st.LongestGap < period/2 || st.LongestGap > 5*time.Second {
				t.Errorf("LongestGap = %v, want about one period (%v) and well under Elapsed", st.LongestGap, period)
			}
		})
	}
}

// TestResetConnectionIsRedialedWithoutAnError pins how a tunnel the gateway
// reset shows up: the loop redials, net/http retries the GET, and NewConns
// grows by one while Errors stays empty.
func TestResetConnectionIsRedialedWithoutAnError(t *testing.T) {
	t.Parallel()
	var cutOnce atomic.Bool
	cut := newTargetConnState(t, okHandler, func(c net.Conn, s http.ConnState) {
		// Closes the server side once the first response is out, as the
		// gateway does to an idle tunnel.
		if s == http.StateIdle && cutOnce.CompareAndSwap(false, true) {
			c.Close()
		}
	})
	steady := []*target{newTarget(t, okHandler), newTarget(t, okHandler)}
	h := serve(steady[0].host(), cut.host(), steady[1].host())

	s := runLoop(t, h, startRequest{Endpoints: 3, IntervalMs: 2}, func(s *loopView) bool { return s.Rounds >= 2 })

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v; want the reset absorbed", s.Successes, s.Requests, s.Errors)
	}
	if s.NewConns != 4 {
		t.Errorf("NewConns = %d, want 4: one per endpoint plus the redial", s.NewConns)
	}
	if got := cut.conns.Load(); got != 2 {
		t.Errorf("the cut endpoint saw %d connections, want 2", got)
	}
	for i, tg := range steady {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("steady endpoint %d saw %d connections, want 1", i, got)
		}
	}
}

// pipeNet serves a handler over in-memory connections, so a loop can run
// inside a synctest bubble, where a real socket would not count as blocked.
type pipeNet struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
	srv   *http.Server
}

func newPipeNet(h http.Handler) *pipeNet {
	p := &pipeNet{conns: make(chan net.Conn), done: make(chan struct{}), srv: &http.Server{Handler: h, ErrorLog: quietLog}}
	go p.srv.Serve(p)
	return p
}

func (p *pipeNet) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case p.conns <- server:
		return client, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeNet) Accept() (net.Conn, error) {
	select {
	case c := <-p.conns:
		return c, nil
	case <-p.done:
		return nil, net.ErrClosed
	}
}

func (p *pipeNet) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

func (p *pipeNet) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// startPipeLoop serves the actor over pn, with endpoint i named endpoint-i,
// and starts a loop with req. It returns the handler and a stats reader that
// waits for the bubble to settle first.
func startPipeLoop(t *testing.T, pn *pipeNet, req startRequest) (http.Handler, func() *loopView) {
	t.Helper()
	s := newServer()
	s.dial = pn.dial
	s.endpointURL = func(i int, scheme string) string { return fmt.Sprintf("%s://endpoint-%d/", scheme, i) }
	h := s.handler()
	if rec := post(t, h, startRoute, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	return h, func() *loopView {
		synctest.Wait()
		return scrapeView(t, h)
	}
}

// stopLoop scrapes the loop one last time, with the endpoint families, then
// stops it, in the order the driver does.
func stopLoop(t *testing.T, h http.Handler) *loopView {
	t.Helper()
	st := scrapeView(t, h)
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /stop = %d %s", rec.Code, rec.Body)
	}
	return st
}

// TestPacingAndGapsAreExact runs the loop on a fake clock with instant
// endpoints, so the schedule can be checked to the millisecond: endpoint i
// opens i intervals in, each endpoint is then requested once per period of C
// intervals, and so every gap is one period.
func TestPacingAndGapsAreExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(okHandler))
		defer pn.srv.Close()
		const c, interval = 4, 100 * time.Millisecond
		const period = c * interval
		start := time.Now()
		h, stats := startPipeLoop(t, pn, startRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})
		for _, tc := range []struct {
			at                       time.Duration
			requests, opened, rounds int64
			openingDone, longestGap  time.Duration
			why                      string
		}{
			{interval / 2, 1, 1, 0, 0, 0, "endpoint 0 opens at once, the others wait their turn"},
			{period - interval/2, c, c, 1, (c - 1) * interval, 0, "one open per interval; the last completes at (C-1) intervals"},
			{2*period - interval/2, 2 * c, c, 2, (c - 1) * interval, period, "each endpoint's second request comes one period after its first"},
			{3*period - interval/2, 3 * c, c, 3, (c - 1) * interval, period, "steady state: every gap is one period"},
		} {
			time.Sleep(tc.at - time.Since(start))
			st := stats()
			if st.Requests != tc.requests || st.Opened != tc.opened || st.Rounds != tc.rounds || st.OpeningDone != tc.openingDone || st.LongestGap != tc.longestGap {
				t.Errorf("at %v: requests %d, opened %d, rounds %d, opening done %v, longest gap %v; want %d, %d, %d, %v, %v (%s)",
					tc.at, st.Requests, st.Opened, st.Rounds, st.OpeningDone, st.LongestGap, tc.requests, tc.opened, tc.rounds, tc.openingDone, tc.longestGap, tc.why)
			}
			if int64(st.Opening.Count) != tc.opened || int64(st.Opening.Count+st.Latency.Count) != st.Successes {
				t.Errorf("at %v: opening %d and latency %d samples for %d successes; want one opening per opened endpoint (%d) and the rest latency",
					tc.at, st.Opening.Count, st.Latency.Count, st.Successes, tc.opened)
			}
		}
		st := stats()
		for i, e := range st.Endpoints {
			if e.LongestGap != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v)", i, e.LongestGap, period)
			}
		}
		stopLoop(t, h)
	})
}

// TestSlowEndpointDelaysOnlyItself pins that a request hanging until its
// timeout on one endpoint leaves the other endpoints' gaps at one period, and
// that the hung endpoint's own gap is the timeout: its next request starts as
// soon as the slow one returns.
func TestSlowEndpointDelaysOnlyItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "endpoint-0" {
				<-r.Context().Done()
				return
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		const c, interval, timeout = 3, 50 * time.Millisecond, 300 * time.Millisecond
		const period = c * interval
		h, stats := startPipeLoop(t, pn, startRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond), RequestTimeoutMs: int64(timeout / time.Millisecond)})

		time.Sleep(time.Second + interval/2)
		st := stats()
		for i := 1; i < c; i++ {
			if got := st.Endpoints[i].LongestGap; got != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v) with endpoint 0 hung", i, got, period)
			}
			if st.Endpoints[i].Errors != 0 {
				t.Errorf("endpoint %d: %d errors, want 0", i, st.Endpoints[i].Errors)
			}
		}
		if st.Endpoints[0].LongestGap != timeout || st.LongestGap != timeout {
			t.Errorf("endpoint 0 longest gap %v, overall %v; want the timeout (%v) for both", st.Endpoints[0].LongestGap, st.LongestGap, timeout)
		}
		if st.Errors["timeout"] == 0 || st.Errors["timeout"] != st.Endpoints[0].Errors {
			t.Errorf("errors %v, endpoint 0 errors %d; want every timeout on endpoint 0", st.Errors, st.Endpoints[0].Errors)
		}
		if st.Opened != c || st.OpeningDone != timeout {
			t.Errorf("Opened = %d, OpeningDone = %v; want %d and %v: the hung open counts once it times out, and it completes last", st.Opened, st.OpeningDone, c, timeout)
		}
		stopLoop(t, h)
	})
}

// TestSlowOpensDoNotDelayOpenedEndpoints pins the schedule when opens are
// slow, as they are while a gateway works through a fleet's handshakes: an
// endpoint already opened is requested one period after its open while the
// others are still opening, and a slow open stretches only its own
// endpoint's first gap, to the open's latency.
func TestSlowOpensDoNotDelayOpenedEndpoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const c, interval, openLatency = 4, 100 * time.Millisecond, 500 * time.Millisecond
		const period = c * interval
		var mu sync.Mutex
		opened := map[string]bool{}
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			first := !opened[r.Host]
			opened[r.Host] = true
			mu.Unlock()
			if first && r.Host != "endpoint-0" {
				select {
				case <-time.After(openLatency):
				case <-r.Context().Done():
					return
				}
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		start := time.Now()
		h, stats := startPipeLoop(t, pn, startRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})

		// endpoint 0 opened at once; its second request is due at one period,
		// while endpoints 1 to 3 are still inside their first.
		time.Sleep(period + interval/2)
		st := stats()
		if st.Opened != 1 || st.Endpoints[0].Requests != 2 || st.Endpoints[0].LongestGap != period || st.Requests != 2 {
			t.Errorf("at %v: opened %d, endpoint 0 %d requests with gap %v, %d requests in all; want 1, 2, %v, 2: the opened endpoint keeps its period while the others open",
				time.Since(start), st.Opened, st.Endpoints[0].Requests, st.Endpoints[0].LongestGap, st.Requests, period)
		}

		// The last open, endpoint 3's, started at 3 intervals and took
		// openLatency; after that each slow endpoint's next request came
		// at once, since its period had already passed.
		time.Sleep(3*period + interval/2 - time.Since(start))
		st = stats()
		if st.Opened != c || st.OpeningDone != (c-1)*interval+openLatency {
			t.Errorf("Opened = %d, OpeningDone = %v; want %d and %v", st.Opened, st.OpeningDone, c, (c-1)*interval+openLatency)
		}
		if st.Endpoints[0].LongestGap != period || st.Endpoints[0].Requests != 4 {
			t.Errorf("endpoint 0: longest gap %v, %d requests; want %v and 4: the slow opens of the others did not touch it", st.Endpoints[0].LongestGap, st.Endpoints[0].Requests, period)
		}
		for i := 1; i < c; i++ {
			if got := st.Endpoints[i].LongestGap; got != openLatency {
				t.Errorf("endpoint %d: longest gap %v, want its own open latency (%v)", i, got, openLatency)
			}
		}
		if st.LongestGap != openLatency || st.Opening.Count != c || st.Opening.bucketOf(boundAtOrAbove(openLatency)) != c {
			t.Errorf("LongestGap = %v, opening samples %d, %d of them at or under %v; want %v, %d and all of them", st.LongestGap, st.Opening.Count, st.Opening.bucketOf(boundAtOrAbove(openLatency)), openLatency, openLatency, c)
		}
		stopLoop(t, h)
	})
}

// TestSlowRequestResumesOnTheNextTick pins what a request longer than the
// period does to its own endpoint: the next request starts as soon as the
// slow one returns, so the gap is the request's latency, not latency plus a
// period.
func TestSlowRequestResumesOnTheNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const period, latency = 50 * time.Millisecond, 120 * time.Millisecond
		var mu sync.Mutex
		var starts []time.Duration
		start := time.Now()
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			starts = append(starts, time.Since(start))
			mu.Unlock()
			select {
			case <-time.After(latency):
				okHandler(w, r)
			case <-r.Context().Done():
			}
		}))
		defer pn.srv.Close()
		h, stats := startPipeLoop(t, pn, startRequest{Endpoints: 1, IntervalMs: int64(period / time.Millisecond)})

		time.Sleep(4*latency + period/2)
		st := stats()
		mu.Lock()
		got := slices.Clone(starts)
		mu.Unlock()
		want := []time.Duration{0, latency, 2 * latency, 3 * latency, 4 * latency}
		if !slices.Equal(got, want) {
			t.Errorf("request starts = %v, want %v: back to back once each outlasts the period", got, want)
		}
		if st.LongestGap != latency {
			t.Errorf("LongestGap = %v, want the request latency (%v)", st.LongestGap, latency)
		}
		stopLoop(t, h)
	})
}

// TestStopWithRequestsInFlightReturnsAtOnce pins that a stop neither waits
// for the in-flight requests' timeouts nor leaks their goroutines.
func TestStopWithRequestsInFlightReturnsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer pn.srv.Close()
		const c = 8
		h, _ := startPipeLoop(t, pn, startRequest{Endpoints: c, RequestTimeoutMs: 5000})
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()

		before := time.Now()
		st := stopLoop(t, h)
		if waited := time.Since(before); waited != 0 {
			t.Errorf("/stop took %v with %d requests in flight, want 0: the stop must not wait for a timeout", waited, c)
		}
		if st.Requests != 0 || st.Opened != 0 {
			t.Errorf("Requests = %d, Opened = %d after stopping mid-open, want 0 and 0", st.Requests, st.Opened)
		}
		if rec := get(h, metricsRoute); rec.Code != http.StatusNotFound {
			t.Errorf("GET /metrics after stop = %d, want 404", rec.Code)
		}
	})
}

func TestStatsGoneStopIs204(t *testing.T) {
	tg := newTarget(t, okHandler)
	s := newServer()
	s.endpointURL = func(int, string) string { return "http://" + tg.host() + "/" }
	h := s.handler()
	if rec := get(h, "/stats"); rec.Code != http.StatusNotFound {
		t.Errorf("GET /stats = %d, want 404", rec.Code)
	}
	if rec := get(h, metricsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /metrics with no loop = %d, want 404", rec.Code)
	}
	if rec := post(t, h, startRoute, startRequest{Endpoints: 1}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Errorf("POST /stop = %d with %d body bytes, want 204 and no body", rec.Code, rec.Body.Len())
	}
	if rec := get(h, metricsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /metrics after stop = %d, want 404", rec.Code)
	}
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNotFound {
		t.Errorf("second POST /stop = %d, want 404", rec.Code)
	}
}

func TestFreshRegistryPerLoop(t *testing.T) {
	tg := newTarget(t, okHandler)
	s := newServer()
	s.endpointURL = func(int, string) string { return "http://" + tg.host() + "/" }
	h := s.handler()
	first := runLoop(t, h, startRequest{Endpoints: 2}, atLeast(10))
	if first.Requests < 10 {
		t.Fatalf("first loop made %d requests, want at least 10", first.Requests)
	}
	// A new loop starts from zero: scrape it before its first interval.
	if rec := post(t, h, startRoute, startRequest{Endpoints: 2, IntervalMs: 60_000}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	second := scrapeView(t, h)
	if second.Requests > 1 || second.Opened > 1 {
		t.Errorf("second loop's first scrape: %d requests, %d opened; want a fresh registry", second.Requests, second.Opened)
	}
	post(t, h, stopRoute, nil)
}

func TestScrapeInvariantUnderRace(t *testing.T) {
	tg := newTarget(t, okHandler)
	s := newServer()
	s.endpointURL = func(int, string) string { return "http://" + tg.host() + "/" }
	h := s.handler()
	if rec := post(t, h, startRoute, startRequest{Endpoints: 8}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	var wg sync.WaitGroup
	var bad atomic.Int64
	for range 4 {
		wg.Go(func() {
			for range 50 {
				v := scrapeView(t, h)
				if int64(v.Opening.Count+v.Latency.Count) != v.Successes {
					bad.Add(1)
				}
			}
		})
	}
	wg.Wait()
	final := stopLoop(t, h)
	if bad.Load() > 0 {
		t.Errorf("%d scrapes broke opening+latency = successes", bad.Load())
	}
	if int64(final.Opening.Count+final.Latency.Count) != final.Successes || final.Successes == 0 {
		t.Errorf("final: opening %d + latency %d != successes %d", final.Opening.Count, final.Latency.Count, final.Successes)
	}
}

// boundAtOrAbove is the first bucket bound at or above d, the bucket d lands in.
func boundAtOrAbove(d time.Duration) float64 {
	for _, b := range bucketBounds {
		if b >= d.Seconds() {
			return b
		}
	}
	return math.Inf(1)
}

// TestScrapeDuringStopIsNever500 races scrapes against a stop: each one
// answers 200 while the loop runs or 404 once it is stopped, never a 500
// from gathering a shut-down reader.
func TestScrapeDuringStopIsNever500(t *testing.T) {
	tg := newTarget(t, okHandler)
	s := newServer()
	s.endpointURL = func(int, string) string { return "http://" + tg.host() + "/" }
	h := s.handler()
	if rec := post(t, h, startRoute, startRequest{Endpoints: 8}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	var wg sync.WaitGroup
	var ok, gone, other atomic.Int64
	stopped := make(chan struct{})
	for range 16 {
		wg.Go(func() {
			for {
				rec := get(h, metricsRoute)
				switch {
				case rec.Code == http.StatusOK:
					ok.Add(1)
				case rec.Code == http.StatusNotFound && strings.TrimSpace(rec.Body.String()) == noLoopMessage:
					gone.Add(1)
				default:
					other.Add(1)
					t.Errorf("GET /metrics = %d %s", rec.Code, rec.Body)
				}
				select {
				case <-stopped:
					return
				default:
				}
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	if rec := post(t, h, stopRoute, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /stop = %d %s", rec.Code, rec.Body)
	}
	close(stopped)
	wg.Wait()
	if rec := get(h, metricsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /metrics after stop = %d, want 404", rec.Code)
	}
	if ok.Load() == 0 || other.Load() != 0 {
		t.Errorf("%d scrapes answered 200, %d 404, %d something else", ok.Load(), gone.Load(), other.Load())
	}
}

// TestEveryRouteCarriesProtocolHeader checks the header a driver reads to
// tell this actor from an image built before the exposition, on every
// answer the actor gives, errors and unknown routes included.
func TestEveryRouteCarriesProtocolHeader(t *testing.T) {
	h := newServer().handler()
	for _, tc := range []struct {
		name string
		rec  *httptest.ResponseRecorder
		want int
	}{
		{"readyz", get(h, readyzRoute), http.StatusOK},
		{"start rejected", post(t, h, startRoute, map[string]int{"endpoints": 0}), http.StatusBadRequest},
		{"metrics without a loop", get(h, metricsRoute), http.StatusNotFound},
		{"stop without a loop", post(t, h, stopRoute, nil), http.StatusNotFound},
		{"unknown route", get(h, "/stats"), http.StatusNotFound},
	} {
		if tc.rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, tc.rec.Code, tc.want)
		}
		if got := tc.rec.Header().Get(protocolHeader); got != protocolVersion {
			t.Errorf("%s: %s = %q, want %q", tc.name, protocolHeader, got, protocolVersion)
		}
	}
}

// TestPayloadBudget pins the scrape size at the largest C, since the driver
// polls every actor: without the endpoint families a scrape must stay small
// in both encodings, and the README's claim that those families outweigh the
// rest of the scrape more than five to one must hold.
func TestPayloadBudget(t *testing.T) {
	m, err := newLoopMetrics(maxEndpoints, "https")
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown(context.Background())
	ctx := context.Background()
	// Every bucket non-zero, so no encoding gets to skip anything.
	for _, b := range bucketBounds {
		m.opening.Record(ctx, b)
		m.latency.Record(ctx, b)
		m.dns.Record(ctx, b)
		m.tls.Record(ctx, b)
	}
	for i := range maxEndpoints {
		m.endpointRequests.Add(ctx, 12345, m.endpointResult[i][0])
		m.endpointRequests.Add(ctx, 6, m.endpointResult[i][1])
		m.endpointLongestGap.Record(ctx, 12.345678, m.endpointOnly[i])
	}
	size := func(withEndpoints bool, accept string) int {
		path := metricsRoute
		if withEndpoints {
			path += "?endpoints=1"
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", accept)
		rec := httptest.NewRecorder()
		metricsHandlerOf(m).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d", path, rec.Code)
		}
		return rec.Body.Len()
	}
	proto := string(expfmt.NewFormat(expfmt.TypeProtoDelim))
	text, protobuf := size(false, "text/plain"), size(false, proto)
	textEndpoints, protoEndpoints := size(true, "text/plain"), size(true, proto)
	t.Logf("scrape at C=%d: text %d B, protobuf %d B; with endpoint families: text %d B, protobuf %d B", maxEndpoints, text, protobuf, textEndpoints, protoEndpoints)
	for _, tc := range []struct {
		name   string
		got    int
		budget int
	}{
		{"text without endpoint families", text, 64 << 10},
		{"protobuf without endpoint families", protobuf, 20 << 10},
		{"text with endpoint families", textEndpoints, 400 << 10},
		{"protobuf with endpoint families", protoEndpoints, 200 << 10},
	} {
		if tc.got >= tc.budget {
			t.Errorf("%s = %d B, budget %d B", tc.name, tc.got, tc.budget)
		}
	}
	if ratio := float64(textEndpoints-text) / float64(text); ratio < 5 {
		t.Errorf("endpoint families are %.1fx the rest of the text scrape, README says more than five to one", ratio)
	}
}
