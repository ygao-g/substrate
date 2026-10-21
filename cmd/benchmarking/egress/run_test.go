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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// fakeAPI is an in-memory ateapi holding just enough state for the driver.
// Methods the driver does not call panic through the nil embedded interface.
type fakeAPI struct {
	ateapipb.ControlClient

	mu       sync.Mutex
	actors   map[string]string // name -> state: "suspended" or "running"
	policies map[string]*ateapipb.EgressPolicy
	calls    map[string][]string // RPC -> actor names, in call order
	// resumeErrs are returned by ResumeActor, one per call, before it succeeds.
	resumeErrs []error
	// suspendErr, if set, is what SuspendActor returns.
	suspendErr error
	// The lostAnswer counters make that many calls commit and then return
	// DeadlineExceeded, as when the answer is lost after the server wrote.
	createLostAnswer int
	updateLostAnswer int
	// resumeLostAnswer makes every ResumeActor commit and then time out.
	resumeLostAnswer bool
	nextUID          int
	// snapshots maps an actor to the owner of its one completed durable
	// snapshot; GetActor reports actor <name> with UID uid-<name>.
	snapshots map[string]ateapipb.SnapshotOwner
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		actors:   make(map[string]string),
		policies: make(map[string]*ateapipb.EgressPolicy),
		calls:    make(map[string][]string),
	}
}

func (f *fakeAPI) record(rpc, name string) {
	f.calls[rpc] = append(f.calls[rpc], name)
}

func (f *fakeAPI) callCount(rpc string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls[rpc])
}

func (f *fakeAPI) callNames(rpc string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(slices.Values(f.calls[rpc]))
}

func (f *fakeAPI) isRunning(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actors[name] == "running"
}

func (f *fakeAPI) GetActor(_ context.Context, in *ateapipb.GetActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("GetActor", name)
	if _, ok := f.actors[name]; !ok {
		return nil, status.Error(codes.NotFound, "no actor")
	}
	a := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name, Uid: "uid-" + name}, Status: &ateapipb.ActorStatus{}}
	if owner, ok := f.snapshots[name]; ok {
		a.Status.Snapshots = []*ateapipb.Snapshot{completedSnapshot(1, owner)}
	}
	return a, nil
}

func (f *fakeAPI) CreateActor(_ context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetMetadata().GetName()
	f.record("CreateActor", name)
	if _, ok := f.actors[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	f.actors[name] = "suspended"
	if f.createLostAnswer > 0 {
		f.createLostAnswer--
		return nil, status.Error(codes.DeadlineExceeded, "answer lost")
	}
	return in.GetActor(), nil
}

func (f *fakeAPI) CreateActorEgressPolicy(_ context.Context, in *ateapipb.CreateActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("CreateActorEgressPolicy", name)
	if _, ok := f.policies[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "policy exists")
	}
	f.nextUID++
	p := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: in.GetActor().GetAtespace(), Name: "default", Uid: fmt.Sprint(f.nextUID), Version: 1},
		Rules:    in.GetEgressPolicy().GetRules(),
	}
	f.policies[name] = p
	return p, nil
}

func (f *fakeAPI) GetActorEgressPolicy(_ context.Context, in *ateapipb.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("GetActorEgressPolicy", in.GetActor().GetName())
	if in.GetName() != "default" {
		return nil, status.Error(codes.InvalidArgument, "name: Required value")
	}
	p, ok := f.policies[in.GetActor().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no policy")
	}
	return p, nil
}

func (f *fakeAPI) UpdateActorEgressPolicy(_ context.Context, in *ateapipb.UpdateActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("UpdateActorEgressPolicy", name)
	cur, ok := f.policies[name]
	if !ok {
		return nil, status.Error(codes.NotFound, "no policy")
	}
	if in.GetEgressPolicy().GetMetadata().GetUid() != cur.GetMetadata().GetUid() {
		return nil, status.Error(codes.FailedPrecondition, "uid precondition failed")
	}
	if in.GetEgressPolicy().GetMetadata().GetVersion() != cur.GetMetadata().GetVersion() {
		return nil, status.Error(codes.Aborted, "EgressPolicy version conflict")
	}
	updated := proto.Clone(in.GetEgressPolicy()).(*ateapipb.EgressPolicy)
	updated.Metadata.Version = cur.GetMetadata().GetVersion() + 1
	f.policies[name] = updated
	if f.updateLostAnswer > 0 {
		f.updateLostAnswer--
		return nil, status.Error(codes.DeadlineExceeded, "answer lost")
	}
	return updated, nil
}

func (f *fakeAPI) ResumeActor(_ context.Context, in *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("ResumeActor", name)
	if len(f.resumeErrs) > 0 {
		err := f.resumeErrs[0]
		f.resumeErrs = f.resumeErrs[1:]
		return nil, err
	}
	if _, ok := f.actors[name]; !ok {
		return nil, status.Error(codes.NotFound, "no actor")
	}
	f.actors[name] = "running"
	if f.resumeLostAnswer {
		return nil, status.Error(codes.DeadlineExceeded, "answer lost")
	}
	return &ateapipb.ResumeActorResponse{Resumed: true}, nil
}

func (f *fakeAPI) SuspendActor(_ context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("SuspendActor", name)
	if f.suspendErr != nil {
		return nil, f.suspendErr
	}
	f.actors[name] = "suspended"
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeAPI) DeleteActor(_ context.Context, in *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("DeleteActor", name)
	if _, ok := f.actors[name]; !ok {
		return nil, status.Error(codes.NotFound, "no actor")
	}
	delete(f.actors, name)
	delete(f.policies, name)
	return &ateapipb.Actor{}, nil
}

// fakeRouter stands in for the atenet router and the actors behind it: it
// answers the egressapi routes for running actors of fakeAPI.
type fakeRouter struct {
	t   *testing.T
	api *fakeAPI

	mu     sync.Mutex
	starts map[string]startRequest
	// startedAt is when each loop's start was accepted.
	startedAt map[string]time.Time
	running   map[string]bool // actor -> loop running
	stops     []string
	// stats, when set, is what an actor's scrape says; n counts its polls
	// so far, from 1. nil answers loopStats.
	stats func(name string, n int) *fakeLoop
	polls map[string]int
	// statsFault makes GET /metrics answer 500 for an actor.
	statsFault map[string]bool
	// oldActor makes an actor answer as an image from before the exposition
	// would: no protocol header, and a plain 404 on /metrics.
	oldActor map[string]bool
	// protocol, when set for an actor, is the protocol header value it sends
	// instead of protocolVersion.
	protocol map[string]string
	// startFault makes /start fail for an actor: "lost-answer" starts the
	// loop but answers 502, "refuse" answers 500 without starting it.
	startFault map[string]string
}

func newFakeRouter(t *testing.T, api *fakeAPI) (*fakeRouter, *httptest.Server) {
	fr := &fakeRouter{t: t, api: api, starts: make(map[string]startRequest), startedAt: make(map[string]time.Time), running: make(map[string]bool), startFault: make(map[string]string), polls: make(map[string]int), oldActor: map[string]bool{}, protocol: map[string]string{}, statsFault: make(map[string]bool)}
	srv := httptest.NewServer(fr)
	t.Cleanup(srv.Close)
	return fr, srv
}

// loopStats is what every fake actor reports: 100 requests to endpoint 0,
// one of which timed out.
// fakeLoop is what a fake actor's scrape says, in the terms the old JSON
// body used. exposition renders it through the actor's own instruments, so
// the driver reads a real exposition.
type fakeLoop struct {
	Elapsed, OpeningDone, LongestGap      time.Duration
	Requests, Successes, NewConns, Opened int64
	Rounds                                int64
	Errors                                map[string]int64
	Endpoints                             []fakeEndpoint
	LatencySamples, OpeningSamples        []time.Duration
	// OtherBounds renders the latency histogram with every other bucket, as
	// an actor built with other bounds would expose it.
	OtherBounds bool
}

type fakeEndpoint struct {
	Requests, Errors int64
}

func repeatDur(n int, d time.Duration) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

func loopStats() *fakeLoop {
	return &fakeLoop{
		Elapsed:        time.Second,
		Requests:       100,
		Successes:      99,
		NewConns:       1,
		Errors:         map[string]int64{"timeout": 1},
		Endpoints:      []fakeEndpoint{{Requests: 100, Errors: 1}},
		LatencySamples: repeatDur(99, time.Millisecond),
	}
}

// exposition renders fl over c endpoints, with the per-endpoint families
// when withEndpoints.
func (fl *fakeLoop) exposition(c int, withEndpoints bool) ([]*dto.MetricFamily, error) {
	m, err := newLoopMetrics(c, "http")
	if err != nil {
		return nil, err
	}
	defer m.shutdown(context.Background())
	ctx := context.Background()
	m.requests.Add(ctx, fl.Successes, m.success)
	m.requests.Add(ctx, fl.Requests-fl.Successes, m.failure)
	for class, n := range fl.Errors {
		m.requestErrors.Add(ctx, n, m.class(class))
	}
	m.connectionsOpened.Add(ctx, fl.NewConns)
	m.endpointsOpened.Add(ctx, fl.Opened)
	m.openingDone.Record(ctx, fl.OpeningDone.Seconds())
	m.longestGap.Record(ctx, fl.LongestGap.Seconds())
	m.elapsed.Record(ctx, fl.Elapsed.Seconds())
	m.rounds.Record(ctx, fl.Rounds)
	for i, e := range fl.Endpoints {
		if i < c {
			m.endpointRequests.Add(ctx, e.Requests-e.Errors, m.endpointResult[i][0])
			m.endpointRequests.Add(ctx, e.Errors, m.endpointResult[i][1])
		}
	}
	for _, d := range fl.LatencySamples {
		m.latency.Record(ctx, d.Seconds())
	}
	for _, d := range fl.OpeningSamples {
		m.opening.Record(ctx, d.Seconds())
	}
	mfs, err := m.gather(withEndpoints)
	if err != nil {
		return nil, err
	}
	if fl.OtherBounds {
		for _, mf := range mfs {
			if mf.GetName() != famLatency {
				continue
			}
			for _, metric := range mf.GetMetric() {
				var kept []*dto.Bucket
				for i, b := range metric.GetHistogram().GetBucket() {
					if i%2 == 0 {
						kept = append(kept, b)
					}
				}
				metric.Histogram.Bucket = kept
			}
		}
	}
	return mfs, nil
}

func (fr *fakeRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get(atenet.TargetActorHeader)
	atespace, name, ok := strings.Cut(target, "/")
	if !ok || atespace != "egress-tests" {
		http.Error(w, "bad "+atenet.TargetActorHeader+" header: "+target, http.StatusBadRequest)
		return
	}
	if !fr.api.isRunning(name) {
		http.Error(w, "actor not running", http.StatusServiceUnavailable)
		return
	}
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if !fr.oldActor[name] {
		w.Header().Set(protocolHeader, cmp.Or(fr.protocol[name], protocolVersion))
	}
	switch r.Method + " " + r.URL.Path {
	case "GET " + readyzRoute:
		w.Write([]byte("ok"))
	case "POST " + startRoute:
		if fr.running[name] {
			http.Error(w, "already running", http.StatusConflict)
			return
		}
		var req startRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if fr.startFault[name] == "refuse" {
			http.Error(w, "refused", http.StatusInternalServerError)
			return
		}
		fr.starts[name] = req
		fr.startedAt[name] = time.Now()
		fr.running[name] = true
		if fr.startFault[name] == "lost-answer" {
			http.Error(w, "upstream reset", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "GET " + metricsRoute:
		if fr.oldActor[name] {
			http.NotFound(w, r)
			return
		}
		if !fr.running[name] {
			http.Error(w, noLoopMessage, http.StatusNotFound)
			return
		}
		fr.polls[name]++
		if fr.statsFault[name] {
			http.Error(w, "metrics broken", http.StatusInternalServerError)
			return
		}
		state := loopStats()
		if fr.stats != nil {
			state = fr.stats(name, fr.polls[name])
		}
		c := max(fr.starts[name].Endpoints, len(state.Endpoints), 1)
		mfs, err := state.exposition(c, r.URL.Query().Get("endpoints") == "1")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeExposition(w, r, mfs)
	case "POST " + stopRoute:
		if !fr.running[name] {
			http.Error(w, noLoopMessage, http.StatusNotFound)
			return
		}
		fr.running[name] = false
		fr.stops = append(fr.stops, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

// services returns the Services of endpoints 0 through n-1, exposing ports.
func services(n int, ports ...int32) []runtime.Object {
	var objs []runtime.Object
	for i := range n {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName(i), Namespace: targetNamespace}}
		for _, p := range ports {
			svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Port: p})
		}
		objs = append(objs, svc)
	}
	return objs
}

// gatewaySetup is what deploy.sh --deploy --https leaves behind: the target's
// TLS Secret, the CA ConfigMap and a gateway patched for it and rolled out.
type gatewaySetup struct {
	secret *corev1.Secret
	cm     *corev1.ConfigMap
	dep    *appsv1.Deployment
}

func newGatewaySetup(t *testing.T) *gatewaySetup {
	t.Helper()
	bundle, err := generateTargetCert(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(bundle.CA)
	replicas := int32(1)
	return &gatewaySetup{
		secret: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: targetTLSSecret, Namespace: targetNamespace},
			Data:       map[string][]byte{caFile: bundle.CA, certFile: bundle.Cert, keyFile: bundle.Key},
		},
		cm: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: gatewayCAConfigMap, Namespace: installdefaults.SystemNamespace},
			Data:       map[string]string{caFile: string(bundle.CA)},
		},
		dep: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: gatewayDeployment, Namespace: installdefaults.SystemNamespace, Generation: 2},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{gatewayCAAnnotation: hex.EncodeToString(sum[:])}},
					Spec: corev1.PodSpec{InitContainers: []corev1.Container{
						{Name: gatewayTrustInitContainer}, {Name: "sdsmint"},
					}},
				},
			},
			Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1},
		},
	}
}

func (g *gatewaySetup) objects() []runtime.Object {
	var objs []runtime.Object
	for _, o := range []runtime.Object{g.secret, g.cm, g.dep} {
		if !reflect.ValueOf(o).IsNil() {
			objs = append(objs, o)
		}
	}
	return objs
}

func testConfig() runConfig {
	return runConfig{
		Actors:            12,
		Parallel:          3,
		Endpoints:         4,
		Duration:          30 * time.Millisecond,
		ConnMode:          connModeKeepAlive,
		RequestTimeout:    2 * time.Second,
		CreateConcurrency: 4,
		ResumeTimeout:     5 * time.Second,
		ProgressInterval:  10 * time.Millisecond,
		Atespace:          "egress-tests",
		Template:          "egress",
		Scheme:            schemeHTTP,
	}
}

func newTestRunner(t *testing.T, cfg runConfig, api *fakeAPI, deployed int) (*runner, *fakeRouter, *bytes.Buffer) {
	t.Helper()
	return newTestRunnerWith(t, cfg, api, services(deployed)...)
}

// newTestRunnerWith is newTestRunner with the cluster holding exactly objs.
func newTestRunnerWith(t *testing.T, cfg runConfig, api *fakeAPI, objs ...runtime.Object) (*runner, *fakeRouter, *bytes.Buffer) {
	t.Helper()
	fr, srv := newFakeRouter(t, api)
	out := &bytes.Buffer{}
	r := newRunner(cfg, api, fake.NewSimpleClientset(objs...), newRouterClient(srv.URL, cfg.Atespace, cfg.Parallel), out)
	r.backoff = backoff{initial: time.Millisecond, max: 5 * time.Millisecond, rpcTimeout: time.Second}
	r.pollInterval = time.Millisecond
	return r, fr, out
}

func wantHosts(n int) []string {
	var hosts []string
	for i := range n {
		hosts = append(hosts, fmt.Sprintf("egress-target-%d.egress-tests-targets.svc.cluster.local", i))
	}
	return hosts
}

func TestRun(t *testing.T) {
	for _, mode := range []string{connModeKeepAlive, connModeNewConn} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig()
			cfg.ConnMode = mode
			api := newFakeAPI()
			api.snapshots = map[string]ateapipb.SnapshotOwner{
				"egress-0": ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
				"egress-1": ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
			}
			r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)

			rep, err := r.run(t.Context())
			if err != nil {
				t.Fatalf("run: %v\n%s", err, out)
			}
			if want := map[string]int{resumeFromOwn: 1, resumeFromTag: 1, resumeFromNone: 1}; !maps.Equal(rep.Resume.Sources, want) {
				t.Errorf("resume sources = %v, want %v", rep.Resume.Sources, want)
			}
			for _, a := range rep.Actors {
				if want := map[string]string{"egress-0": resumeFromOwn, "egress-1": resumeFromTag, "egress-2": resumeFromNone}[a.Name]; a.ResumeSource != want {
					t.Errorf("%s resumed from %q, want %q", a.Name, a.ResumeSource, want)
				}
			}

			if got := api.callCount("CreateActor"); got != cfg.Actors {
				t.Errorf("CreateActor called %d times, want %d", got, cfg.Actors)
			}
			if len(api.policies) != cfg.Actors {
				t.Errorf("%d egress policies, want one per actor (%d)", len(api.policies), cfg.Actors)
			}
			for name, p := range api.policies {
				if len(p.GetRules()) != 2 {
					t.Errorf("policy of %s has %d rules, want an http and an https rule", name, len(p.GetRules()))
					continue
				}
				if hosts := p.GetRules()[0].GetHttp().GetHostnames(); !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
					t.Errorf("http rule of %s allows %v, want %v", name, hosts, wantHosts(cfg.Endpoints))
				}
				if hosts := p.GetRules()[1].GetHttps().GetHostnames(); !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
					t.Errorf("https rule of %s allows %v, want %v", name, hosts, wantHosts(cfg.Endpoints))
				}
			}

			resumed := api.callNames("ResumeActor")
			if len(resumed) != cfg.Parallel || len(slices.Compact(slices.Clone(resumed))) != cfg.Parallel {
				t.Errorf("resumed %v, want %d distinct actors", resumed, cfg.Parallel)
			}
			for _, name := range resumed {
				if _, ok := api.actors[name]; !ok {
					t.Errorf("resumed %s, which the run did not create", name)
				}
			}
			for _, name := range resumed {
				req, ok := fr.starts[name]
				if !ok {
					t.Errorf("loop of %s never started", name)
					continue
				}
				if req.Endpoints != cfg.Endpoints {
					t.Errorf("%s started with %d endpoints, want %d", name, req.Endpoints, cfg.Endpoints)
				}
				if req.NewConnPerRequest != (mode == connModeNewConn) || req.RequestTimeoutMs != 2000 {
					t.Errorf("%s started with %+v, want new-conn=%v and a 2000ms timeout", name, req, mode == connModeNewConn)
				}
			}
			if got := slices.Sorted(slices.Values(fr.stops)); !slices.Equal(got, resumed) {
				t.Errorf("stopped %v, want every resumed actor %v", got, resumed)
			}
			if got := api.callNames("SuspendActor"); !slices.Equal(got, resumed) {
				t.Errorf("suspended %v, want every resumed actor %v", got, resumed)
			}

			if rep.Create.Succeeded != cfg.Actors || rep.Resume.Succeeded != 3 || rep.Start.Succeeded != 3 || rep.Suspend.Succeeded != 3 {
				t.Errorf("report phases: create %+v resume %+v start %+v suspend %+v", rep.Create, rep.Resume, rep.Start, rep.Suspend)
			}
			if rep.Loop.counter(famRequests) != 300 || rep.Loop.counter(famRequests, labelResult, resultSuccess) != 297 || rep.Loop.errorCounts()["timeout"] != 3 {
				t.Errorf("merged loop stats = %+v, want 3 actors x 100 requests", rep.Loop)
			}
			if len(rep.Actors) != 3 || rep.Resume.ResumeLatency.Count != 3 {
				t.Errorf("report has %d actors and %d resume samples, want 3 each", len(rep.Actors), rep.Resume.ResumeLatency.Count)
			}

			var printed bytes.Buffer
			rep.print(&printed)
			for _, want := range []string{"scheme=http ", "request-interval=0s", "300 requests", "timeout=3", "egress-target-0 (3/300)",
				"resumed from: none 1, own 1, tag 1"} {
				if !strings.Contains(printed.String(), want) {
					t.Errorf("printed report lacks %q:\n%s", want, printed.String())
				}
			}
			if _, err := json.Marshal(rep); err != nil {
				t.Errorf("report does not marshal: %v", err)
			}
		})
	}
}

func TestRunReusesActorsAndUpdatesPolicies(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()

	// A first run with fewer endpoints leaves actors and narrower policies.
	small := cfg
	small.Endpoints = 2
	r, _, out := newTestRunner(t, small, api, cfg.Endpoints)
	if _, err := r.run(t.Context()); err != nil {
		t.Fatalf("first run: %v\n%s", err, out)
	}

	r, _, out = newTestRunner(t, cfg, api, cfg.Endpoints)
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("second run: %v\n%s", err, out)
	}
	if rep.Create.ActorsReused != cfg.Actors || rep.Create.PoliciesUpdated != cfg.Actors {
		t.Errorf("second run reused %d actors and updated %d policies, want %d of each", rep.Create.ActorsReused, rep.Create.PoliciesUpdated, cfg.Actors)
	}
	for name, p := range api.policies {
		if hosts := p.GetRules()[0].GetHttp().GetHostnames(); !slices.Equal(hosts, wantHosts(cfg.Endpoints)) {
			t.Errorf("policy of %s allows %v after the rerun, want %v", name, hosts, wantHosts(cfg.Endpoints))
		}
	}

	// A third run with the same endpoints changes nothing.
	r, _, out = newTestRunner(t, cfg, api, cfg.Endpoints)
	updatesBefore := api.callCount("UpdateActorEgressPolicy")
	rep, err = r.run(t.Context())
	if err != nil {
		t.Fatalf("third run: %v\n%s", err, out)
	}
	if rep.Create.PoliciesUpdated != 0 || api.callCount("UpdateActorEgressPolicy") != updatesBefore {
		t.Errorf("an unchanged rerun updated %d policies", rep.Create.PoliciesUpdated)
	}
}

func TestPreflightFailsOnMissingServices(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, _, _ := newTestRunner(t, cfg, api, 2)

	_, err := r.run(t.Context())
	if err == nil {
		t.Fatal("run succeeded with 2 of 4 endpoint Services deployed")
	}
	for _, want := range []string{"egress-target-2", "egress-target-3", "--endpoints 4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if n := api.callCount("CreateActor"); n != 0 {
		t.Errorf("CreateActor called %d times before the preflight passed", n)
	}
}

func TestResumeRetriesFullPool(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel = 1
	api := newFakeAPI()
	api.resumeErrs = []error{
		status.Error(codes.ResourceExhausted, "no worker has capacity"),
		status.Error(codes.Aborted, "concurrent update conflict"),
	}
	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Resume.Succeeded != 1 || api.callCount("ResumeActor") != 3 {
		t.Errorf("resume succeeded %d times after %d calls, want 1 after 3", rep.Resume.Succeeded, api.callCount("ResumeActor"))
	}
}

func TestResumeGivesUpOnCrash(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel = 1
	api := newFakeAPI()
	api.resumeErrs = []error{status.Error(codes.Aborted, "actor entered ACTOR_STATE_CRASHED")}
	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Resume.Failed != 1 || rep.Resume.Errors["Aborted"] != 1 || api.callCount("ResumeActor") != 1 {
		t.Errorf("resume = %+v after %d calls, want one Aborted failure and no retry", rep.Resume, api.callCount("ResumeActor"))
	}
	if api.callCount("SuspendActor") != 0 {
		t.Errorf("suspended an actor that never resumed")
	}
}

func TestStartRecoversLeftoverLoop(t *testing.T) {
	cfg := testConfig()
	cfg.Actors, cfg.Parallel = 1, 1 // so the run resumes egress-0
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	// An interrupted run left egress-0's loop running.
	fr.running["egress-0"] = true

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Start.Succeeded != 1 {
		t.Errorf("start = %+v, want the leftover loop stopped and restarted", rep.Start)
	}
	if len(fr.stops) != 2 {
		t.Errorf("stops = %v, want two: the leftover loop and the final collect", fr.stops)
	}
}

func TestStopReachesLoopsWhoseStartFailed(t *testing.T) {
	cfg := testConfig()
	cfg.Actors = cfg.Parallel // so the run resumes egress-0 to egress-2
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.startFault["egress-1"] = "lost-answer"
	fr.startFault["egress-2"] = "refuse"

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Start.Succeeded != 1 || rep.Start.Failed != 2 {
		t.Errorf("start = %+v, want 1 ok and 2 failed", rep.Start)
	}
	// egress-1's loop runs although its start failed; the stop phase must
	// still reach it. egress-2 has no loop, which is not a stop failure.
	if got := slices.Sorted(slices.Values(fr.stops)); !slices.Equal(got, []string{"egress-0", "egress-1"}) {
		t.Errorf("stopped %v, want egress-0 and egress-1", got)
	}
	if fr.running["egress-1"] {
		t.Errorf("egress-1's loop is still running after the run")
	}
	if rep.Stop.Succeeded != 3 || rep.Stop.Failed != 0 {
		t.Errorf("stop = %+v, want 3 ok", rep.Stop)
	}
	if rep.Loop.counter(famRequests) != 200 {
		t.Errorf("merged requests = %d, want the 200 of the two loops that ran", rep.Loop.counter(famRequests))
	}
}

func TestCleanupDeadlineStartsAfterTheLoops(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = 100 * time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	// Shorter than the run: a deadline counted from the resume would be spent
	// before the loops are stopped.
	r.cleanupTimeout = 50 * time.Millisecond

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if len(fr.stops) != cfg.Parallel || rep.Suspend.Succeeded != cfg.Parallel {
		t.Errorf("%d loops stopped and %d actors suspended, want %d each", len(fr.stops), rep.Suspend.Succeeded, cfg.Parallel)
	}
	if rep.Loop.counter(famRequests) != 300 {
		t.Errorf("merged requests = %d, want 300", rep.Loop.counter(famRequests))
	}
}

func TestRunFailsWhenActorsAreLeftRunning(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	api.suspendErr = status.Error(codes.FailedPrecondition, "cannot suspend")
	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "3 actors were not suspended") {
		t.Fatalf("run = %v, want an error naming the 3 unsuspended actors\n%s", err, out)
	}
	if rep == nil || rep.Suspend.Failed != cfg.Parallel {
		t.Errorf("report = %+v, want it returned with the suspend failures", rep)
	}
}

func TestPickActorsFirst(t *testing.T) {
	cfg := testConfig()
	cfg.Actors, cfg.Parallel = 20, 5
	for _, tc := range []struct {
		name    string
		created func(i int) bool
		want    []string
	}{
		{"all created", func(int) bool { return true }, []string{"egress-0", "egress-1", "egress-2", "egress-3", "egress-4"}},
		{"egress-3 not created", func(i int) bool { return i != 3 }, []string{"egress-0", "egress-1", "egress-2", "egress-4", "egress-5"}},
		{"fewer created than wanted", func(i int) bool { return i == 7 || i == 11 }, []string{"egress-7", "egress-11"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created := make([]bool, cfg.Actors)
			for i := range created {
				created[i] = tc.created(i)
			}
			if got := newRunner(cfg, nil, nil, nil, io.Discard).pickActors(created); !slices.Equal(got, tc.want) {
				t.Errorf("picked %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRunInterruptedStillStopsAndSuspends(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = time.Hour
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		// Interrupt once every loop is running.
		for {
			fr.mu.Lock()
			n := len(fr.starts)
			fr.mu.Unlock()
			if n == cfg.Parallel {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	rep, err := r.run(ctx)
	if err == nil || !rep.Interrupted {
		t.Fatalf("run = %v, interrupted %v; want the cancellation reported\n%s", err, rep.Interrupted, out)
	}
	if len(fr.stops) != cfg.Parallel || api.callCount("SuspendActor") != cfg.Parallel {
		t.Errorf("after the interrupt: %d loops stopped, %d actors suspended; want %d each", len(fr.stops), api.callCount("SuspendActor"), cfg.Parallel)
	}
	if rep.Loop.counter(famRequests) != 300 {
		t.Errorf("interrupted run lost the loop stats: %+v", rep.Loop)
	}
}

func TestCleanup(t *testing.T) {
	api := newFakeAPI()
	for i := range 5 {
		api.actors[actorName(i)] = "running"
	}
	b := backoff{initial: time.Millisecond, max: time.Millisecond, rpcTimeout: time.Second}

	res := cleanup(t.Context(), api, b, "egress-tests", 7, 3, io.Discard)

	if res.Succeeded != 7 || res.NotFound != 2 || res.Failed != 0 {
		t.Errorf("cleanup = %+v, want 7 done of which 2 did not exist", res)
	}
	if len(api.actors) != 0 {
		t.Errorf("actors left after cleanup: %v", api.actors)
	}
}

func TestBuildPolicy(t *testing.T) {
	p := buildPolicy("ns", 3)
	if p.GetMetadata().GetName() != "default" || p.GetMetadata().GetAtespace() != "ns" {
		t.Errorf("policy metadata = %v, want default in ns", p.GetMetadata())
	}
	rules := p.GetRules()
	if len(rules) != 2 || rules[0].GetHttp() == nil || rules[1].GetHttps() == nil {
		t.Fatalf("policy rules = %v, want an http rule and an https rule", rules)
	}
	if rules[0].GetHttp().GetPorts() != nil || rules[1].GetHttps().GetPorts() != nil {
		t.Errorf("policy rules = %v, want both on their default ports", rules)
	}
	if !slices.Equal(rules[0].GetHttp().GetHostnames(), wantHosts(3)) || !slices.Equal(rules[1].GetHttps().GetHostnames(), wantHosts(3)) {
		t.Errorf("policy hostnames = %v", rules)
	}

	for _, tc := range []struct {
		n, wantRules int
	}{{256, 1}, {257, 2}, {1000, 4}, {maxEndpoints, 4}} {
		p := buildPolicy("ns", tc.n)
		var http, https []string
		var httpRules, httpsRules int
		for _, r := range p.GetRules() {
			switch {
			case r.GetHttp() != nil:
				httpRules++
				http = append(http, r.GetHttp().GetHostnames()...)
				if n := len(r.GetHttp().GetHostnames()); n > maxRuleHostnames {
					t.Errorf("C=%d: an http rule holds %d hostnames, want at most %d", tc.n, n, maxRuleHostnames)
				}
			case r.GetHttps() != nil:
				httpsRules++
				https = append(https, r.GetHttps().GetHostnames()...)
				if n := len(r.GetHttps().GetHostnames()); n > maxRuleHostnames {
					t.Errorf("C=%d: an https rule holds %d hostnames, want at most %d", tc.n, n, maxRuleHostnames)
				}
			}
		}
		if httpRules != tc.wantRules || httpsRules != tc.wantRules {
			t.Errorf("C=%d: got %d http and %d https rules, want %d of each", tc.n, httpRules, httpsRules, tc.wantRules)
		}
		if !slices.Equal(http, wantHosts(tc.n)) || !slices.Equal(https, wantHosts(tc.n)) {
			t.Errorf("C=%d: the rules' hostnames are not endpoints 0 through %d, each once", tc.n, tc.n-1)
		}
		if !samePolicyRules(buildPolicy("ns", tc.n), p) {
			t.Errorf("C=%d: samePolicyRules told apart two builds of the same policy", tc.n)
		}
	}

	reordered := buildPolicy("ns", 3)
	slices.Reverse(reordered.Rules[0].Http.Hostnames)
	slices.Reverse(reordered.Rules)
	httpOnly := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Http: &ateapipb.HttpRule{Hostnames: wantHosts(3)}}}}
	switch {
	case !samePolicyRules(reordered, p):
		t.Error("samePolicyRules told apart policies that differ only in order")
	case samePolicyRules(buildPolicy("ns", 2), p):
		t.Error("samePolicyRules missed a different set of hostnames")
	case samePolicyRules(httpOnly, p):
		t.Error("samePolicyRules took an http-only policy for one that also allows https")
	}
}

// Policies an earlier driver created allow only HTTP; a run brings each up to
// both rules once and then leaves them alone.
func TestRunUpgradesHTTPOnlyPolicies(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	for i := range cfg.Actors {
		name := actorName(i)
		api.actors[name] = "suspended"
		api.nextUID++
		api.policies[name] = &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: cfg.Atespace, Name: "default", Uid: fmt.Sprint(api.nextUID)},
			Rules:    []*ateapipb.EgressRule{{Http: &ateapipb.HttpRule{Hostnames: wantHosts(cfg.Endpoints)}}},
		}
	}

	r, _, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Create.PoliciesUpdated != cfg.Actors {
		t.Errorf("updated %d policies, want all %d http-only ones", rep.Create.PoliciesUpdated, cfg.Actors)
	}
	for name, p := range api.policies {
		if len(p.GetRules()) != 2 {
			t.Errorf("policy of %s has %d rules after the run, want 2", name, len(p.GetRules()))
		}
	}

	r, _, out = newTestRunner(t, cfg, api, cfg.Endpoints)
	if rep, err = r.run(t.Context()); err != nil {
		t.Fatalf("second run: %v\n%s", err, out)
	}
	if rep.Create.PoliciesUpdated != 0 {
		t.Errorf("second run updated %d policies, want none", rep.Create.PoliciesUpdated)
	}
}

func TestRunHTTPS(t *testing.T) {
	cfg := testConfig()
	cfg.Scheme = schemeHTTPS
	api := newFakeAPI()
	objs := append(services(cfg.Endpoints, 80, httpsPort), newGatewaySetup(t).objects()...)
	r, fr, out := newTestRunnerWith(t, cfg, api, objs...)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if len(fr.starts) != cfg.Parallel {
		t.Fatalf("%d loops started, want %d", len(fr.starts), cfg.Parallel)
	}
	for name, req := range fr.starts {
		if req.Scheme != schemeHTTPS {
			t.Errorf("%s started over %q, want https", name, req.Scheme)
		}
	}
	var printed bytes.Buffer
	rep.print(&printed)
	if !strings.Contains(printed.String(), "scheme=https ") {
		t.Errorf("report header does not say https:\n%s", printed.String())
	}
}

func TestPreflightHTTPS(t *testing.T) {
	const n = 3
	for _, tc := range []struct {
		name    string
		scheme  string
		ports   []int32
		mutate  func(*gatewaySetup)
		wantErr string // "" means preflight passes
	}{
		{name: "patched gateway", scheme: schemeHTTPS, ports: []int32{80, httpsPort}},
		{name: "http needs no gateway setup", scheme: schemeHTTP, mutate: func(g *gatewaySetup) { *g = gatewaySetup{} }},
		{name: "service without https port", scheme: schemeHTTPS, ports: []int32{80}, wantErr: "--https"},
		{name: "no target secret", scheme: schemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.secret = nil }, wantErr: "--deploy --https"},
		{name: "no CA configmap", scheme: schemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.cm = nil }, wantErr: "--patch-gateway"},
		{name: "gateway trusts another CA", scheme: schemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) {
				g.cm.Data[caFile] = string(newGatewaySetup(t).secret.Data[caFile])
			}, wantErr: "--patch-gateway"},
		{name: "gateway not patched", scheme: schemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) {
				g.dep.Spec.Template.Spec.InitContainers = g.dep.Spec.Template.Spec.InitContainers[1:]
			}, wantErr: "--patch-gateway"},
		{name: "patch annotation is stale", scheme: schemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.dep.Spec.Template.Annotations[gatewayCAAnnotation] = "0000" }, wantErr: "--patch-gateway"},
		{name: "rollout in progress", scheme: schemeHTTPS, ports: []int32{80, httpsPort},
			mutate: func(g *gatewaySetup) { g.dep.Status.UpdatedReplicas = 0 }, wantErr: "rollout status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGatewaySetup(t)
			if tc.mutate != nil {
				tc.mutate(g)
			}
			k8s := fake.NewSimpleClientset(append(services(n, tc.ports...), g.objects()...)...)
			err := preflight(t.Context(), k8s, n, tc.scheme)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("preflight = %v, want it to pass", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("preflight = %v, want an error mentioning %q", err, tc.wantErr)
			}
		})
	}
}

func TestConfigValidateAccepts(t *testing.T) {
	for name, mutate := range map[string]func(*runConfig){
		"opening exactly half the timer": func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.StreamIdleTimeout = 1000, 150*time.Millisecond, 5*time.Minute
		},
		"long opening with new-conn": func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.StreamIdleTimeout, c.ConnMode = 1000, time.Second, 5*time.Minute, connModeNewConn
		},
		"long opening with the check off": func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.StreamIdleTimeout = 1000, time.Second, 0
		},
		"settle covering one sweep": func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.Settle, c.SettleTimeout = 4, 100*time.Millisecond, 400*time.Millisecond, time.Minute
		},
		"batches with a gap":  func(c *runConfig) { c.StartBatch, c.StartGap = 2, time.Second },
		"batches with no gap": func(c *runConfig) { c.StartBatch = c.Parallel },
		"opens budget met":    func(c *runConfig) { c.RequestInterval, c.OpensBudget = 100*time.Millisecond, 30 },
		"opens budget off":    func(c *runConfig) { c.RequestInterval, c.OpensBudget = 100*time.Millisecond, 0 },
		"opens budget with unpaced requests": func(c *runConfig) {
			c.RequestInterval, c.OpensBudget = 0, 1
		},
		"batches under the opens budget": func(c *runConfig) {
			c.RequestInterval, c.StartBatch, c.OpensBudget = 100*time.Millisecond, 1, 10
		},
		"overlapping batches under the opens budget": func(c *runConfig) {
			// Period 400ms, gap 200ms: two batches open at once, 20 opens/s.
			c.RequestInterval, c.StartBatch, c.StartGap, c.OpensBudget = 100*time.Millisecond, 1, 200*time.Millisecond, 20
		},
	} {
		c := testConfig()
		mutate(&c)
		if err := c.validate(); err != nil {
			t.Errorf("%s: validate rejected %+v: %v", name, c, err)
		}
	}
}

func TestConfigValidateMessages(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*runConfig)
		want   string
	}{
		"opening past half the timer": {func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.StreamIdleTimeout = 1000, 200*time.Millisecond, 5*time.Minute
		}, "half the gateway's stream idle timeout"},
		"settle without progress": {func(c *runConfig) { c.Settle, c.SettleTimeout, c.ProgressInterval = time.Second, time.Minute, 0 }, "--progress-interval"},
		"settle shorter than a sweep": {func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.Settle, c.SettleTimeout = 4, 100*time.Millisecond, 100*time.Millisecond, time.Minute
		}, "one period"},
		"opening past half the timer names the period": {func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.StreamIdleTimeout = 1000, 200*time.Millisecond, 5*time.Minute
		}, "per-endpoint period"},
		"gap without batch":     {func(c *runConfig) { c.StartGap = time.Second }, "--start-gap needs --start-batch"},
		"negative opens budget": {func(c *runConfig) { c.OpensBudget = -1 }, "--opens-budget cannot be negative"},
		"all at once over the opens budget": {func(c *runConfig) {
			c.RequestInterval, c.OpensBudget = 100*time.Millisecond, 20
		}, "starting 3 actors together asks the gateway for 30 opens/s, over --opens-budget 20/s: use --start-batch 2 or less"},
		"no batch fits the opens budget": {func(c *runConfig) {
			c.RequestInterval, c.OpensBudget = 100*time.Millisecond, 5
		}, "over --opens-budget 5/s: even one actor at a time is over it; use --request-interval 200ms or more"},
		"overlapping batches over the opens budget": {func(c *runConfig) {
			// Period 1s, gap 200ms: five batches open at once. One actor per
			// batch would still ask for 50/s, so only a wider gap fits.
			c.Parallel, c.Endpoints, c.RequestInterval, c.StartBatch, c.StartGap, c.OpensBudget = 4, 10, 100*time.Millisecond, 2, 200*time.Millisecond, 20
		}, "starting 2 actors in batches 200ms apart, 5 of them inside one 1s period, asks the gateway for 100 opens/s, over --opens-budget 20/s: use --start-gap 1s or more"},
		"overlapping batches with two ways to fit": {func(c *runConfig) {
			c.Parallel, c.Endpoints, c.RequestInterval, c.StartBatch, c.StartGap, c.OpensBudget = 4, 10, 100*time.Millisecond, 2, 500*time.Millisecond, 30
		}, "asks the gateway for 40 opens/s, over --opens-budget 30/s: use --start-batch 1 or less, or --start-gap 1s or more"},
	} {
		c := testConfig()
		tc.mutate(&c)
		err := c.validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: validate() = %v, want an error mentioning %q", name, err, tc.want)
		}
	}
}

func TestStartLogPrintsTheOpeningDemand(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*runConfig)
		want   string
	}{
		"all at once, unchecked": {func(c *runConfig) { c.RequestInterval = 100 * time.Millisecond },
			"(http, keepalive): 30 opens/s over a 400ms opening window (budget unchecked)"},
		"all at once, budget": {func(c *runConfig) { c.RequestInterval, c.OpensBudget = 100*time.Millisecond, 100 },
			"(http, keepalive): 30 opens/s over a 400ms opening window against a budget of 100/s"},
		"batches, budget": {func(c *runConfig) {
			c.RequestInterval, c.StartBatch, c.StartGap, c.OpensBudget = 100*time.Millisecond, 1, 20*time.Millisecond, 200
		}, "in 3 batches of 1, 20ms apart: 10 opens/s per batch, 20 batches inside one 400ms period: 200 opens/s against a budget of 200/s"},
		"batches one window apart, unchecked": {func(c *runConfig) {
			c.RequestInterval, c.StartBatch, c.StartGap, c.Endpoints = 10*time.Millisecond, 1, 20*time.Millisecond, 2
		}, "in 3 batches of 1, 20ms apart: 100 opens/s per batch over a 20ms opening window (budget unchecked)"},
		"unpaced, budget": {func(c *runConfig) { c.RequestInterval, c.OpensBudget = 0, 100 },
			"(http, keepalive): unpaced opens (budget unchecked)"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			tc.mutate(&cfg)
			r, _, out := newTestRunner(t, cfg, newFakeAPI(), cfg.Endpoints)
			if _, err := r.run(t.Context()); err != nil {
				t.Fatalf("run: %v\n%s", err, out)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Errorf("start log lacks %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	valid := testConfig()
	if err := valid.validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	for name, mutate := range map[string]func(*runConfig){
		"parallel above actors": func(c *runConfig) { c.Parallel = c.Actors + 1 },
		"no endpoints":          func(c *runConfig) { c.Endpoints = 0 },
		"too many endpoints":    func(c *runConfig) { c.Endpoints = maxEndpoints + 1 },
		"bad conn mode":         func(c *runConfig) { c.ConnMode = "pooled" },
		"zero duration":         func(c *runConfig) { c.Duration = 0 },
		"negative interval":     func(c *runConfig) { c.RequestInterval = -time.Second },
		"bad scheme":            func(c *runConfig) { c.Scheme = "ftp" },
		"sub-ms interval":       func(c *runConfig) { c.RequestInterval = 500 * time.Microsecond },
		"fractional interval":   func(c *runConfig) { c.RequestInterval = 1500 * time.Microsecond },
		"sub-ms timeout":        func(c *runConfig) { c.RequestTimeout = 500 * time.Microsecond },
		"opening past half the idle timer": func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.StreamIdleTimeout = 1000, 200*time.Millisecond, 5*time.Minute
		},
		"negative settle":         func(c *runConfig) { c.Settle = -time.Second },
		"settle without timeout":  func(c *runConfig) { c.Settle, c.SettleTimeout = time.Second, 0 },
		"settle with new-conn":    func(c *runConfig) { c.Settle, c.SettleTimeout, c.ConnMode = time.Second, time.Minute, connModeNewConn },
		"settle without progress": func(c *runConfig) { c.Settle, c.SettleTimeout, c.ProgressInterval = time.Second, time.Minute, 0 },
		"settle shorter than a sweep": func(c *runConfig) {
			c.Endpoints, c.RequestInterval, c.Settle, c.SettleTimeout = 4, 100*time.Millisecond, 100*time.Millisecond, time.Minute
		},
		"batch above parallel": func(c *runConfig) { c.StartBatch = c.Parallel + 1 },
		"negative batch":       func(c *runConfig) { c.StartBatch = -1 },
		"negative gap":         func(c *runConfig) { c.StartBatch, c.StartGap = 1, -time.Second },
		"gap without batch":    func(c *runConfig) { c.StartGap = time.Second },
	} {
		c := testConfig()
		mutate(&c)
		if c.validate() == nil {
			t.Errorf("%s: validate accepted %+v", name, c)
		}
	}
}

func TestErrorKind(t *testing.T) {
	deadline := fmt.Errorf("resume: %w", fmt.Errorf("%w (last error: %w)", context.DeadlineExceeded, status.Error(codes.ResourceExhausted, "pool full for egress-7")))
	for _, tc := range []struct {
		err  error
		want string
	}{
		{status.Error(codes.NotFound, "actor egress-1 not found"), "NotFound"},
		{fmt.Errorf("create actor: %w", status.Error(codes.Internal, "x")), "Internal"},
		{deadline, "gave up retrying ResourceExhausted"},
		{&statusError{code: 503, body: "no healthy upstream"}, "HTTP 503"},
		{fmt.Errorf("start: %w", fmt.Errorf("dial tcp 127.0.0.1:1: connect: connection refused")), "connection refused"},
	} {
		if got := errorKind(tc.err); got != tc.want {
			t.Errorf("errorKind(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestOutcomeUnknown(t *testing.T) {
	t.Parallel()
	gaveUp := func(ctxErr, last error) error {
		return fmt.Errorf("%w (last error: %w)", ctxErr, last)
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"FailedPrecondition", status.Error(codes.FailedPrecondition, "x"), false},
		{"AlreadyExists", status.Error(codes.AlreadyExists, "x"), false},
		{"NotFound", status.Error(codes.NotFound, "x"), false},
		{"ResourceExhausted", status.Error(codes.ResourceExhausted, "x"), false},
		{"Aborted", status.Error(codes.Aborted, "x"), false},
		{"DeadlineExceeded code", status.Error(codes.DeadlineExceeded, "x"), true},
		{"Canceled code", status.Error(codes.Canceled, "x"), true},
		{"Unavailable code", status.Error(codes.Unavailable, "x"), true},
		{"Unknown code", status.Error(codes.Unknown, "x"), true},
		{"error with no status", fmt.Errorf("connection reset"), true},
		{"wrapped Unavailable", fmt.Errorf("resume: %w", status.Error(codes.Unavailable, "x")), true},
		{"wrapped context.DeadlineExceeded", fmt.Errorf("resume: %w", context.DeadlineExceeded), true},
		{"wrapped context.Canceled", fmt.Errorf("resume: %w", context.Canceled), true},
		{"gave up after refusal", gaveUp(context.DeadlineExceeded, status.Error(codes.ResourceExhausted, "x")), true},
		{"canceled after refusal", gaveUp(context.Canceled, status.Error(codes.FailedPrecondition, "x")), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := outcomeUnknown(tc.err); got != tc.want {
				t.Errorf("outcomeUnknown(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestLostAnswers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		setup func(api *fakeAPI)
		check func(t *testing.T, r *runner, api *fakeAPI)
	}{
		{
			name:  "policy update committed before its answer was lost",
			setup: func(api *fakeAPI) { api.updateLostAnswer = 1 },
			check: func(t *testing.T, r *runner, api *fakeAPI) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				if _, err := api.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
					Actor: &ateapipb.ObjectRef{Atespace: "egress-tests", Name: "egress-0"}, EgressPolicy: buildPolicy("egress-tests", 1),
				}); err != nil {
					t.Fatal(err)
				}
				want := buildPolicy("egress-tests", 3)
				changed, err := r.ensurePolicy(ctx, "egress-0", want)
				if err != nil || !changed {
					t.Fatalf("ensurePolicy = %v, %v; want true, nil", changed, err)
				}
				if got := api.policies["egress-0"].GetRules()[0].GetHttp().GetHostnames(); !slices.Equal(got, wantHosts(3)) {
					t.Errorf("policy hosts = %v, want %v", got, wantHosts(3))
				}
				if got := api.callCount("UpdateActorEgressPolicy"); got != 1 {
					t.Errorf("UpdateActorEgressPolicy called %d times, want 1: the retry should see the policy already updated", got)
				}
			},
		},
		{
			name:  "create committed before its answer was lost",
			setup: func(api *fakeAPI) { api.createLostAnswer = 1 },
			check: func(t *testing.T, r *runner, api *fakeAPI) {
				existed, err := r.ensureActor(t.Context(), "egress-0")
				if err != nil || existed {
					t.Errorf("ensureActor = %v, %v; want false, nil", existed, err)
				}
			},
		},
		{
			name:  "resume committed but every answer timed out",
			setup: func(api *fakeAPI) { api.resumeLostAnswer = true },
			check: func(t *testing.T, r *runner, api *fakeAPI) {
				r.cfg.Actors, r.cfg.Parallel = 1, 1 // so the run resumes egress-0
				r.cfg.ResumeTimeout = 50 * time.Millisecond
				rep, err := r.run(t.Context())
				if err != nil {
					t.Fatalf("run: %v", err)
				}
				if rep.Resume.Failed != 1 {
					t.Errorf("resume = %+v, want one failure", rep.Resume)
				}
				if api.isRunning("egress-0") {
					t.Errorf("egress-0 left running after a resume whose answer was lost")
				}
				if got := api.callNames("SuspendActor"); !slices.Equal(got, []string{"egress-0"}) {
					t.Errorf("suspended %v, want [egress-0]", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := newFakeAPI()
			tc.setup(api)
			r, _, _ := newTestRunner(t, testConfig(), api, testConfig().Endpoints)
			tc.check(t, r, api)
		})
	}
}

func TestResumeSource(t *testing.T) {
	t.Parallel()
	own := ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
	tag := ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG
	pending := completedSnapshot(2, own)
	pending.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	for _, tc := range []struct {
		name      string
		snapshots []*ateapipb.Snapshot
		running   bool
		want      string
	}{
		{name: "own snapshot", snapshots: []*ateapipb.Snapshot{completedSnapshot(1, own)}, want: resumeFromOwn},
		{name: "own snapshot but already running", snapshots: []*ateapipb.Snapshot{completedSnapshot(1, own)}, running: true, want: resumeFromRunning},
		{name: "golden tag, borrowed", snapshots: []*ateapipb.Snapshot{completedSnapshot(1, tag)}, want: resumeFromTag},
		{name: "newest completed generation wins", snapshots: []*ateapipb.Snapshot{completedSnapshot(1, tag), completedSnapshot(2, own)}, want: resumeFromOwn},
		{name: "a pending upload does not count", snapshots: []*ateapipb.Snapshot{completedSnapshot(1, tag), pending}, want: resumeFromTag},
		{name: "no snapshot", want: resumeFromNone},
		{name: "unknown owner", snapshots: []*ateapipb.Snapshot{completedSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_UNSPECIFIED)}, want: resumeFromUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Uid: "uid-a"}, Status: &ateapipb.ActorStatus{Snapshots: tc.snapshots}}
			if tc.running {
				a.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			}
			if got := resumeSource(a); got != tc.want {
				t.Errorf("resumeSource() = %q, want %q", got, tc.want)
			}
		})
	}
}

// completedSnapshot is a durable snapshot generation that finished uploading.
func completedSnapshot(gen int32, owner ateapipb.SnapshotOwner) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Uuid:       fmt.Sprintf("s%d", gen),
		Generation: gen,
		Owner:      owner,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
			Object: &ateapipb.ObjectSnapshot{SnapshotUri: fmt.Sprintf("gs://b/r/snapshots/s%d", gen)},
		},
	}
}

// settlingStats returns a stats function whose loops open their endpoints
// over the first two polls, finish a sweep on the third, and then hold
// steady, with one redial on the fourth poll when redial is set.
func settlingStats(c int, redial bool) func(name string, n int) *fakeLoop {
	return func(_ string, n int) *fakeLoop {
		s := loopStats()
		s.Requests, s.Successes = int64(100*n), int64(100*n-1)
		s.Opened = int64(min(n*c/2, c))
		s.NewConns = s.Opened
		if s.Opened == int64(c) {
			s.OpeningDone = 300 * time.Millisecond
		}
		// Rounds reaches 2, the settle's bar, on the fourth poll.
		if n >= 3 {
			s.Rounds = int64(n - 2)
			s.LongestGap = time.Duration(c) * 100 * time.Millisecond
		}
		if redial && n >= 4 {
			s.NewConns++
		}
		return s
	}
}

func TestRunSettlesBeforeMeasuring(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 5*time.Second
	cfg.StreamIdleTimeout = 430 * time.Millisecond // the loops' 400 ms gap is within 10% of it
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = settlingStats(cfg.Endpoints, false)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	s := rep.Settle
	if s == nil || !s.Settled {
		t.Fatalf("settle = %+v, want settled\n%s", s, out)
	}
	if s.AllOpened <= 0 || s.AllSwept <= s.AllOpened || s.Redials != 0 || s.LongestGap != 400*time.Millisecond {
		t.Errorf("settle = %+v, want opened before swept, no redials, the loops' longest gap", s)
	}
	// Rounds reaches 1 on the third poll and 2 on the fourth; only the
	// fourth counts as swept.
	for _, name := range api.callNames("ResumeActor") {
		if fr.polls[name] < 4 {
			t.Errorf("%s was polled %d times before settling, want at least 4: one round is not swept", name, fr.polls[name])
		}
	}
	// Half a poll interval of slack keeps jitter from costing a whole poll.
	if s.Waited < s.AllSwept+cfg.Settle-cfg.ProgressInterval/2 {
		t.Errorf("waited %v, want at least %v: all swept plus a flat --settle", s.Waited, s.AllSwept+cfg.Settle-cfg.ProgressInterval/2)
	}
	var names []string
	for _, p := range rep.Phases {
		names = append(names, p.Name)
		if p.Name == "steady" && p.End.Sub(p.Start) < cfg.Duration {
			t.Errorf("steady phase lasted %v, want the full --duration %v after settling", p.End.Sub(p.Start), cfg.Duration)
		}
	}
	if want := []string{"create", "resume", "start", "opening", "settle", "steady", "stop", "suspend"}; !slices.Equal(names, want) {
		t.Errorf("phases = %v, want %v", names, want)
	}
	if last := rep.LoopTimeline[len(rep.LoopTimeline)-1]; last.Opened != int64(3*cfg.Endpoints) || last.Rounds < 3*2 || last.OpeningDone != 300*time.Millisecond || last.LongestGap != 400*time.Millisecond {
		t.Errorf("last timeline point = %+v, want every endpoint opened, the rounds, opening done and the gap carried", last)
	}
	if !s.LongestGapNearTimer {
		t.Errorf("settle = %+v, want the near-timer flag with a 400 ms gap against a 430 ms timer", s)
	}
	if sw := rep.Steady; sw == nil || sw.Polls < 2 || sw.Requests <= 0 || sw.ReqPerS <= 0 || sw.Redials != 0 || sw.End.Sub(sw.Start) < cfg.Duration/2 {
		t.Errorf("steady window = %+v, want at least two polls over the window with a positive rate and no redials", sw)
	} else if whole := float64(rep.Loop.counter(famRequests)) / rep.Loop.gauge(famElapsed); sw.ReqPerS <= whole {
		t.Errorf("steady rate %.1f req/s is not above the whole-run rate %.1f: the window still counts the settle wait", sw.ReqPerS, whole)
	}
	var printed bytes.Buffer
	rep.print(&printed)
	for _, want := range []string{"settle     settled after", "0 redials", "within 10% of the 430ms stream idle timeout", "whole run ", "steady     ", "opening    ", "rounds in all"} {
		if !strings.Contains(printed.String(), want) {
			t.Errorf("printed report lacks %q:\n%s", want, printed.String())
		}
	}
}

func TestRunSettleNeedsSuccessfulOpens(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 60*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = func(_ string, n int) *fakeLoop {
		s := settlingStats(cfg.Endpoints, false)("", n)
		s.Successes, s.Errors = 0, map[string]int64{"HTTP 503": s.Requests} // every open failed
		return s
	}
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if s := rep.Settle; s == nil || s.Settled || !strings.Contains(s.Reason, "success") || s.AllOpened != 0 {
		t.Errorf("settle = %+v, want not settled because the opens had no successes", s)
	}
}

func TestRunSettleNamesLoopsThatDoNotAnswer(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 60*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = settlingStats(cfg.Endpoints, false)
	fr.statsFault["egress-1"] = true
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	s := rep.Settle
	if s == nil || s.Settled || !slices.Equal(s.Unanswered, []string{"egress-1"}) || !strings.Contains(s.Reason, "egress-1") {
		t.Errorf("settle = %+v, want not settled with egress-1 named as unanswered", s)
	}
	if !strings.Contains(out.String(), "did not answer (egress-1)") {
		t.Errorf("progress log never named the silent loop:\n%s", out)
	}
	var printed bytes.Buffer
	rep.print(&printed)
	if !strings.Contains(printed.String(), "NOT settled after") || !strings.Contains(printed.String(), "egress-1") {
		t.Errorf("printed report lacks the reason:\n%s", printed.String())
	}
}

func TestRunSettleWaitsForRedialsToStop(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 5*time.Second
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = settlingStats(cfg.Endpoints, true)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if s := rep.Settle; s == nil || !s.Settled || s.Redials != 3 {
		t.Fatalf("settle = %+v, want settled with 3 redials (one per loop)\n%s", s, out)
	}
	// The redial and the second round both land on the fourth poll, so the
	// flat wait starts there.
	for _, name := range api.callNames("ResumeActor") {
		if fr.polls[name] < 4+3 {
			t.Errorf("%s was polled %d times, want at least 7: the redial on poll 4 restarts the %v flat wait", name, fr.polls[name], cfg.Settle)
		}
	}
}

func TestRunMeasuresAnywayWhenSettleTimesOut(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 60*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = func(_ string, n int) *fakeLoop {
		s := loopStats()
		s.Opened = int64(cfg.Endpoints) // opened, never swept
		return s
	}

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	s := rep.Settle
	if s == nil || s.Settled || s.Waited < cfg.SettleTimeout || s.AllOpened == 0 || s.AllSwept != 0 || !strings.Contains(s.Reason, "twice") {
		t.Fatalf("settle = %+v, want not settled after the timeout, opened but never swept\n%s", s, out)
	}
	if len(rep.Loop) == 0 || rep.Loop.counter(famRequests) != 300 {
		t.Errorf("loop = %+v, want the measured window to have run anyway", rep.Loop)
	}
	var printed bytes.Buffer
	rep.print(&printed)
	if !strings.Contains(printed.String(), "NOT settled") {
		t.Errorf("printed report lacks the NOT settled line:\n%s", printed.String())
	}
}

// batchesOf groups the fake router's start times by batch: starts within a
// quarter of gap of each other are one batch.
func batchesOf(startedAt map[string]time.Time, gap time.Duration) [][]time.Time {
	times := slices.SortedFunc(maps.Values(startedAt), time.Time.Compare)
	var batches [][]time.Time
	for _, t := range times {
		if n := len(batches); n > 0 && t.Sub(batches[n-1][0]) < gap/4 {
			batches[n-1] = append(batches[n-1], t)
			continue
		}
		batches = append(batches, []time.Time{t})
	}
	return batches
}

func TestStartLoopsInBatches(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel, cfg.StartBatch, cfg.StartGap = 6, 2, 50*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	batches := batchesOf(fr.startedAt, cfg.StartGap)
	if len(batches) != 3 {
		t.Fatalf("loops started in %d batches %v, want 3 of 2", len(batches), batches)
	}
	for i, b := range batches {
		if len(b) != 2 {
			t.Errorf("batch %d started %d loops, want 2", i, len(b))
		}
		if i > 0 {
			if apart := b[0].Sub(batches[i-1][0]); apart < cfg.StartGap {
				t.Errorf("batches %d and %d started %v apart, want at least the %v gap", i-1, i, apart, cfg.StartGap)
			}
		}
	}
	if rep.Start.Succeeded != 6 || len(rep.Start.Batches) != 3 {
		t.Fatalf("start = %+v, want 6 started in 3 recorded batches", rep.Start)
	}
	// Two gaps between three batches, and none after the last.
	var startPhase phaseMark
	for _, p := range rep.Phases {
		if p.Name == "start" {
			startPhase = p
		}
	}
	if rep.Start.Duration < 2*cfg.StartGap || startPhase.End.Sub(rep.Start.Batches[2].At) >= cfg.StartGap {
		t.Errorf("start phase took %v and ended %v after the last batch began; want at least two gaps (%v) and no gap after the last batch",
			rep.Start.Duration, startPhase.End.Sub(rep.Start.Batches[2].At), 2*cfg.StartGap)
	}
	for i, b := range rep.Start.Batches {
		if b.Index != i || b.Actors != 2 || b.At.After(batches[i][0]) || (i > 0 && b.At.Sub(rep.Start.Batches[i-1].At) < cfg.StartGap) {
			t.Errorf("recorded batch %d = %+v, want index %d, 2 actors, at or before its first start %v, a gap after the previous", i, b, i, batches[i][0])
		}
	}
	for _, a := range rep.Actors {
		if a.StartedAt.IsZero() || a.Batch < 0 || a.Batch > 2 {
			t.Errorf("actor %s started at %v in batch %d, want a start time and a batch", a.Name, a.StartedAt, a.Batch)
		}
	}
	if !strings.Contains(out.String(), "3 batches of 2") {
		t.Errorf("start log does not describe the batches:\n%s", out)
	}
}

func TestInterruptBetweenBatchesStillStopsAndSuspends(t *testing.T) {
	cfg := testConfig()
	cfg.Parallel, cfg.StartBatch, cfg.StartGap = 6, 2, time.Hour
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		// Interrupt during the gap after the first batch.
		for {
			fr.mu.Lock()
			n := len(fr.starts)
			fr.mu.Unlock()
			if n == cfg.StartBatch {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	rep, err := r.run(ctx)
	if err == nil || !rep.Interrupted {
		t.Fatalf("run = %v, interrupted %v; want the cancellation reported\n%s", err, rep.Interrupted, out)
	}
	if len(fr.starts) != 2 || len(fr.stops) != 2 {
		t.Errorf("%d loops started and %d stopped, want the first batch's 2 each", len(fr.starts), len(fr.stops))
	}
	if rep.Stop.Succeeded != 6 || api.callCount("SuspendActor") != 6 {
		t.Errorf("stop reached %d of 6 ready actors and %d were suspended, want 6 each after an interrupt between batches", rep.Stop.Succeeded, api.callCount("SuspendActor"))
	}
	if len(rep.Start.Batches) != 1 {
		t.Errorf("recorded batches = %+v, want only the one that started", rep.Start.Batches)
	}
}

func TestRunMarksTheOpeningPhase(t *testing.T) {
	cfg := testConfig()
	cfg.StartBatch, cfg.StartGap = 1, 20*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	// Loop i takes (i+1) x 5 ms, on the actor's clock, to open.
	const unit = 5 * time.Millisecond
	openingDone := map[string]time.Duration{"egress-0": unit, "egress-1": 2 * unit, "egress-2": 3 * unit}
	// Each loop opened every endpoint, one of them with a failed first
	// request, and the others in 10ms; one request per loop timed out.
	fr.stats = func(name string, _ int) *fakeLoop {
		s := loopStats()
		s.Opened, s.OpeningDone = int64(cfg.Endpoints), openingDone[name]
		s.OpeningSamples = repeatDur(cfg.Endpoints-1, 10*time.Millisecond)
		return s
	}

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	startedAt := map[string]time.Time{}
	for _, a := range rep.Actors {
		startedAt[a.Name] = a.StartedAt
	}
	phases := map[string]phaseMark{}
	for _, p := range rep.Phases {
		phases[p.Name] = p
	}
	o := rep.Opening
	if o == nil {
		t.Fatalf("report has no opening; phases %+v\n%s", rep.Phases, out)
	}
	if !o.Start.Equal(rep.Start.Batches[0].At) || !o.End.Equal(startedAt["egress-2"].Add(3*unit)) || !o.PureEnd.Equal(startedAt["egress-0"].Add(unit)) {
		t.Errorf("opening = %+v, want from the first batch (%v) to the last loop's start + 15ms (%v), pure until the first loop's start + 5ms (%v)",
			o, rep.Start.Batches[0].At, startedAt["egress-2"].Add(3*unit), startedAt["egress-0"].Add(unit))
	}
	if o.Opens != int64(3*cfg.Endpoints) || o.Redials == nil || *o.Redials != 0 {
		t.Errorf("opening = %+v, want %d opens and 0 redials at the first poll after it", o, 3*cfg.Endpoints)
	}
	if o.FailedOpens != 3 || o.OpenP99 != 10*time.Millisecond || o.Timeouts != 3 {
		t.Errorf("opening = %+v, want 3 failed opens, an open p99 of 10ms and 3 timeouts at the first poll after it", o)
	}
	js, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"failedOpens":3`, `"openP99":10000000`, `"timeouts":3`} {
		if !strings.Contains(string(js), want) {
			t.Errorf("JSON report lacks %s", want)
		}
	}
	if p, ok := phases["opening"]; !ok || !p.Start.Equal(o.Start) || !p.End.Equal(o.End) {
		t.Errorf("opening phase = %+v, want %v to %v", p, o.Start, o.End)
	}
	if p, ok := phases["opening-pure"]; !ok || !p.Start.Equal(o.Start) || !p.End.Equal(o.PureEnd) {
		t.Errorf("opening-pure phase = %+v, want %v to %v", p, o.Start, o.PureEnd)
	}
	var names []string
	for _, p := range rep.Phases {
		names = append(names, p.Name)
	}
	if want := []string{"create", "resume", "start", "opening", "opening-pure", "steady", "stop", "suspend"}; !slices.Equal(names, want) {
		t.Errorf("phases = %v, want %v", names, want)
	}
	var printed bytes.Buffer
	rep.print(&printed)
	if want := fmt.Sprintf("opening    %v: %d opens, 3 failed, open p99 10ms, 3 timeouts, 0 redials (pure window %v, 1 actors)", round(o.End.Sub(o.Start)), 3*cfg.Endpoints, round(o.PureEnd.Sub(o.Start))); !strings.Contains(printed.String(), want) {
		t.Errorf("printed report lacks %q:\n%s", want, printed.String())
	}
}

func TestRunWithoutBatchesHasNoPureWindow(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = func(string, int) *fakeLoop {
		s := loopStats()
		s.Opened, s.OpeningDone = int64(cfg.Endpoints), time.Second
		return s
	}
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if o := rep.Opening; o == nil || !o.PureEnd.IsZero() || len(rep.Start.Batches) != 1 {
		t.Errorf("opening = %+v with batches %+v; want an opening with no pure window and one batch of all the loops", o, rep.Start.Batches)
	}
	for _, p := range rep.Phases {
		if p.Name == "opening-pure" {
			t.Errorf("phases %+v hold opening-pure, want it only with --start-batch", rep.Phases)
		}
	}
}

func TestRunOpeningUnknownUntilEveryLoopOpened(t *testing.T) {
	cfg := testConfig()
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = func(name string, _ int) *fakeLoop {
		s := loopStats()
		if name != "egress-1" {
			s.Opened, s.OpeningDone = int64(cfg.Endpoints), time.Second
		}
		return s
	}
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if rep.Opening != nil {
		t.Errorf("opening = %+v, want none while egress-1 has not opened", rep.Opening)
	}
	for _, p := range rep.Phases {
		if strings.HasPrefix(p.Name, "opening") {
			t.Errorf("phases %+v hold %s, want no opening mark", rep.Phases, p.Name)
		}
	}
}

func TestRunSettleNeedsTwoRounds(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 60*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = func(_ string, n int) *fakeLoop {
		s := settlingStats(cfg.Endpoints, false)("", n)
		s.Rounds = min(s.Rounds, 1) // every endpoint requested once, none twice
		return s
	}
	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if s := rep.Settle; s == nil || s.Settled || s.AllOpened == 0 || s.AllSwept != 0 || !strings.Contains(s.Reason, "twice") {
		t.Errorf("settle = %+v, want not settled: one round is not swept", s)
	}
}

// unmergeableStats is a scrape whose latency histogram has other bucket
// bounds, as an actor built from other source would send.
func unmergeableStats() *fakeLoop {
	s := loopStats()
	s.OtherBounds = true
	return s
}

func TestStatsTheDriverCannotMergeAreNamedAndLeftOut(t *testing.T) {
	cfg := testConfig()
	cfg.Settle, cfg.SettleTimeout = 3*cfg.ProgressInterval, 60*time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	fr.stats = func(name string, n int) *fakeLoop {
		if name == "egress-1" {
			return unmergeableStats()
		}
		return settlingStats(cfg.Endpoints, false)(name, n)
	}

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if !strings.Contains(out.String(), "cannot add up (egress-1: latency_seconds: histogram has") {
		t.Errorf("progress log never named the loop whose stats cannot be merged:\n%s", out)
	}
	if s := rep.Settle; s == nil || s.Settled || !strings.Contains(s.Reason, "egress-1") || !strings.Contains(s.Reason, "cannot add up") {
		t.Errorf("settle = %+v, want not settled, naming egress-1 as unmergeable", s)
	}
	if len(rep.Loop) == 0 || rep.Loop.counter(famRequests) != 2*rep.Actors[0].Metrics.counter(famRequests) {
		t.Errorf("merged loop stats = %+v, want the two mergeable actors only", rep.Loop)
	}
	var named bool
	for _, a := range rep.Actors {
		if a.Name == "egress-1" && strings.Contains(a.Error, "cannot be added up") {
			named = true
		}
	}
	if !named {
		t.Errorf("actors = %+v, want egress-1 carrying the merge error", rep.Actors)
	}
	if rep.Stop.Succeeded != cfg.Parallel {
		t.Errorf("stop = %+v, want every loop stopped even when its stats cannot be merged", rep.Stop)
	}
}
