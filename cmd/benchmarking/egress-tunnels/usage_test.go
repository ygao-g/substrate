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
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// Server gauges the fake gateway always serves.
const (
	fakeHeapAllocated = 11492288
	fakeHeapSize      = 16777216
)

// envoyBody renders /stats/prometheus text with every stat the sampler
// reads, taking values from values and 0 elsewhere.
func envoyBody(values map[string]float64) []byte {
	var b strings.Builder
	for _, metric := range slices.Sorted(maps.Keys(envoyClusterStats)) {
		for _, c := range envoyClusters {
			fmt.Fprintf(&b, "%s{envoy_cluster_name=%q} %g\n", metric, c, values[clusterKey(c, envoyClusterStats[metric])])
		}
	}
	for _, metric := range slices.Sorted(maps.Keys(envoyConnectStats)) {
		fmt.Fprintf(&b, "%s{envoy_http_conn_manager_prefix=%q} %g\n", metric, envoyConnectPrefix, values["http."+envoyConnectPrefix+"."+envoyConnectStats[metric]])
	}
	server := map[string]float64{envoyHeapAllocated: fakeHeapAllocated, "server.memory_heap_size": fakeHeapSize, "server.memory_physical_size": 19852094}
	for _, metric := range slices.Sorted(maps.Keys(envoyServerStats)) {
		stat := envoyServerStats[metric]
		v, ok := values[stat]
		if !ok {
			v = server[stat]
		}
		fmt.Fprintf(&b, "%s{} %g\n", metric, v)
	}
	return []byte(b.String())
}

// fakeGetter answers raw API server paths. Each Go process's CPU counter
// rises by 0.01 s per read; the gateway's stats come from envoy.
type fakeGetter struct {
	mu    sync.Mutex
	reads map[string]int
	fail  map[string]bool // path -> always fail
	envoy func() map[string]float64
	// envoyPartial answers the Envoy read with one series instead of all.
	envoyPartial bool
	// block holds the paths whose reads wait until ctx ends.
	block map[string]bool
}

func newFakeGetter() *fakeGetter {
	return &fakeGetter{reads: map[string]int{}, fail: map[string]bool{}, block: map[string]bool{},
		envoy: func() map[string]float64 { return map[string]float64{envoyTunnelsOpened: 50, envoyTunnels: 3} }}
}

func (g *fakeGetter) GetRaw(ctx context.Context, path string, params url.Values) ([]byte, error) {
	g.mu.Lock()
	g.reads[path]++
	n := g.reads[path]
	fail, block := g.fail[path], g.block[path]
	g.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if fail {
		return nil, errors.New("proxy error")
	}
	switch {
	case strings.HasSuffix(path, ":9090/proxy/metrics"):
		return fmt.Appendf(nil, "# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total %.2f\nprocess_resident_memory_bytes 5e+07\n", 100+0.01*float64(n)), nil
	case strings.HasSuffix(path, "/proxy/metrics/cadvisor"):
		// cAdvisor refreshes every third read, 15 s apart.
		ts := 1_700_000_000_000 + int64((n-1)/3)*15_000
		cpu := 10 + float64((n-1)/3)
		return fmt.Appendf(nil, `container_cpu_usage_seconds_total{container="envoy",cpu="total",image="envoy",namespace="ate-system",pod="atenet-egress-a"} %v %d
container_memory_working_set_bytes{container="envoy",image="envoy",namespace="ate-system",pod="atenet-egress-a"} 6e+07 %d
container_cpu_usage_seconds_total{container="ateom",cpu="total",image="ateom",namespace="egress-tests",pod="worker-a"} %v %d
`, cpu, ts, ts, 2*cpu, ts), nil
	case strings.HasSuffix(path, ":15090/proxy/stats/prometheus"):
		if params.Get("filter") != envoyStatsFilter {
			return nil, fmt.Errorf("unexpected filter %q", params.Get("filter"))
		}
		if g.envoyPartial {
			return []byte("envoy_cluster_upstream_cx_total{envoy_cluster_name=\"mitm_internal\"} 1\n"), nil
		}
		return envoyBody(g.envoy()), nil
	}
	return nil, fmt.Errorf("unexpected path %s", path)
}

func (g *fakeGetter) readsOf(path string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reads[path]
}

// addSystemPods creates the pods the sampler looks for: the gateway with
// its sdsmint sidecar, two ateapi replicas, and one worker of the
// egress-tests atespace, all on node-a.
func addSystemPods(t *testing.T, k8s kubernetes.Interface) {
	t.Helper()
	always := corev1.ContainerRestartPolicyAlways
	pods := []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "atenet-egress-a", Namespace: "ate-system", Labels: map[string]string{"app": "atenet-egress"}},
			Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "sdsmint", RestartPolicy: &always}},
				Containers: []corev1.Container{{Name: "envoy"}, {Name: "ext-proc"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "ate-api-server-a", Namespace: "ate-system", Labels: map[string]string{"app": "ate-api-server"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ate-api-server"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "ate-api-server-b", Namespace: "ate-system", Labels: map[string]string{"app": "ate-api-server"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ate-api-server"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "egress-tests", Labels: map[string]string{workerPoolLabel: "benchmark-ateom"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ateom"}}}},
	}
	for _, pod := range pods {
		pod.Spec.NodeName = "node-a"
		pod.Status.Phase = corev1.PodRunning
		if _, err := k8s.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

// readsOf sums a source's answered reads over its targets.
func readsOf(counts map[string]int, source string) int {
	n := 0
	for k, v := range counts {
		if strings.HasPrefix(k, source+"/") {
			n += v
		}
	}
	return n
}

// newFastSampler is a sampler over the fake cluster that polls in
// milliseconds.
func newFastSampler(t *testing.T, get rawGetter) *usageSampler {
	t.Helper()
	k8s := fake.NewSimpleClientset()
	addSystemPods(t, k8s)
	s := newUsageSampler(get, k8s, "egress-tests")
	s.interval = 5 * time.Millisecond
	s.cadvisorInterval = 2 * time.Millisecond
	return s
}

func TestUsageSampler(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		get := newFakeGetter()
		broken := podProxyPath("ate-system", "ate-api-server-b", 9090, "/metrics")
		get.fail[broken] = true
		s := newFastSampler(t, get)
		if err := s.start(t.Context()); err != nil {
			t.Fatal(err)
		}
		s.mark(t.Context(), markSteadyBegin)
		time.Sleep(40 * time.Millisecond)
		s.mark(t.Context(), markSteadyEnd)
		rep := s.stop()
		if again := s.stop(); len(again.Live) != len(rep.Live) {
			t.Errorf("second stop changed the samples: %d then %d", len(rep.Live), len(again.Live))
		}

		pods := map[string]int{}
		labeled := 0
		for _, l := range rep.Live {
			pods[l.Pod]++
			if l.Label != "" {
				labeled++
			}
		}
		if pods["atenet-egress-a"] < 3 || pods["ate-api-server-a"] < 3 {
			t.Errorf("live reads per pod = %v, want at least 3 for each working pod", pods)
		}
		if pods["ate-api-server-b"] != 0 || rep.Errors["live/ate-api-server-b"] != get.readsOf(broken) {
			t.Errorf("broken pod: %d samples and %d errors after %d reads, want 0 samples and one error per read",
				pods["ate-api-server-b"], rep.Errors["live/ate-api-server-b"], get.readsOf(broken))
		}
		if labeled != 4 {
			t.Errorf("%d labeled live reads, want 2 marks x 2 working pods", labeled)
		}
		if readsOf(rep.Reads, "live") != len(rep.Live) || readsOf(rep.Reads, "envoy") != len(rep.Envoy) || readsOf(rep.Reads, "cadvisor") == 0 {
			t.Errorf("reads %v, want live=%d envoy=%d and cadvisor reads", rep.Reads, len(rep.Live), len(rep.Envoy))
		}
		if len(rep.Envoy) < 3 || len(rep.Sources) != 3 {
			t.Errorf("%d envoy reads and sources %+v, want at least 3 reads and 3 sources", len(rep.Envoy), rep.Sources)
		}
		if got := rep.Envoy[0].Counters[envoyHeapAllocated]; got != fakeHeapAllocated {
			t.Errorf("first envoy read's heap = %v, want %v", got, fakeHeapAllocated)
		}
		cadvisorReads := get.readsOf("/api/v1/nodes/node-a/proxy/metrics/cadvisor")
		if want := 2 * ((cadvisorReads + 2) / 3); len(rep.Cadvisor) != want || cadvisorReads < 6 {
			t.Errorf("%d cAdvisor samples after %d reads, want one per container per new timestamp (%d)", len(rep.Cadvisor), cadvisorReads, want)
		}
		if !slices.Equal(s.missing, []string{"router", "targets", "dns"}) {
			t.Errorf("missing components = %v, want the three with no pods in the fake", s.missing)
		}
		if rep.Driver.WholeRun.WallSeconds <= 0 || rep.Driver.Steady.WallSeconds <= 0 {
			t.Errorf("driver usage %+v, want whole-run and steady windows", rep.Driver)
		}
	})
}

// A read whose body lacks any of the expected series is a failed read, not
// a sample of zeros: Envoy serves every stat of a cluster from the start.
func TestReadEnvoyRejectsPartialBody(t *testing.T) {
	t.Parallel()
	get := newFakeGetter()
	get.envoyPartial = true
	s := newUsageSampler(get, fake.NewSimpleClientset(), "egress-tests")
	s.readEnvoy(t.Context(), "atenet-egress-a", "")
	get.envoyPartial = false
	s.readEnvoy(t.Context(), "atenet-egress-a", "")
	rep := s.stop()
	if len(rep.Envoy) != 1 || rep.Errors["envoy/atenet-egress-a"] != 1 {
		t.Errorf("%d envoy samples and %d errors, want the partial body counted as an error and the full one kept", len(rep.Envoy), rep.Errors["envoy/atenet-egress-a"])
	}
}

// A read that stop() cuts off is not a failure of the source.
func TestStopDoesNotCountCancelledReads(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		get := newFakeGetter()
		envoy := podProxyPath(envoyTarget.namespace, "atenet-egress-a", envoyTarget.port, envoyTarget.path)
		get.block[envoy] = true
		s := newFastSampler(t, get)
		if err := s.start(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		rep := s.stop()
		if get.readsOf(envoy) == 0 {
			t.Fatal("the blocked Envoy read was never attempted")
		}
		if rep.Errors["envoy/atenet-egress-a"] != 0 || readsOf(rep.Reads, "live") == 0 {
			t.Errorf("errors %v reads %v, want no envoy error from the read stop() cut off, and live reads", rep.Errors, rep.Reads)
		}
		if warn := sourceWarnings(rep); warn != "envoy no reads" {
			t.Errorf("sourceWarnings = %q, want the envoy source named as never read", warn)
		}
	})
}

// TestReadProcessStampsMidpoint checks that a live read is stamped halfway
// through its round trip and keeps the round trip.
func TestReadProcessStampsMidpoint(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		rtt  time.Duration
	}{
		{name: "fast", rtt: 20 * time.Millisecond},
		{name: "slow", rtt: 2400 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newUsageSampler(newFakeGetter(), fake.NewSimpleClientset(), "egress-tests")
			calls := 0
			s.now = func() time.Time {
				calls++
				if calls == 1 {
					return start
				}
				return start.Add(tc.rtt)
			}
			s.readProcess(t.Context(), liveTargets[0], "atenet-egress-a", "")
			if len(s.rep.Live) != 1 {
				t.Fatalf("%d live samples, want 1", len(s.rep.Live))
			}
			got := s.rep.Live[0]
			if want := start.Add(tc.rtt / 2); !got.T.Equal(want) || got.RTT != tc.rtt {
				t.Errorf("sample at %v with RTT %v, want %v with RTT %v", got.T, got.RTT, want, tc.rtt)
			}
		})
	}
}

func TestPodContainers(t *testing.T) {
	t.Parallel()
	always := corev1.ContainerRestartPolicyAlways
	for _, tc := range []struct {
		name string
		spec corev1.PodSpec
		want []string
	}{
		{name: "containers only", spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "envoy"}, {Name: "ext-proc"}}},
			want: []string{"envoy", "ext-proc"}},
		{name: "native sidecar counts, plain init does not", spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "trust"}, {Name: "sdsmint", RestartPolicy: &always}},
			Containers:     []corev1.Container{{Name: "envoy"}, {Name: "ext-proc"}},
		}, want: []string{"sdsmint", "envoy", "ext-proc"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := podContainers(&corev1.Pod{Spec: tc.spec}); !slices.Equal(got, tc.want) {
				t.Errorf("podContainers = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSamplerConfigHasATimeout pins the per-read timeout: proxy requests
// are exempt from the API server's request timeout, so without it a read
// that never answers holds the run.
func TestSamplerConfigHasATimeout(t *testing.T) {
	t.Parallel()
	cfg := &rest.Config{}
	tuneSamplerConfig(cfg)
	if cfg.Timeout != samplerTimeout || cfg.Timeout <= 0 {
		t.Errorf("sampler client timeout = %v, want %v", cfg.Timeout, samplerTimeout)
	}
	if cfg.QPS != samplerQPS || cfg.Burst != samplerBurst {
		t.Errorf("sampler rate limit = %v/%v, want %v/%v", cfg.QPS, cfg.Burst, samplerQPS, samplerBurst)
	}
}

func TestRunWithUsage(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.Usage = true
	cfg.PreIdle = 30 * time.Millisecond
	api := newFakeAPI()
	r, fr, out := newTestRunner(t, cfg, api, cfg.Endpoints)
	addSystemPods(t, r.k8s)
	get := newFakeGetter()
	// Each started loop holds one tunnel on the one actor connection the
	// fake gateway reports; the tunnel counter starts at 50 from earlier runs.
	get.envoy = func() map[string]float64 {
		fr.mu.Lock()
		defer fr.mu.Unlock()
		started := float64(len(fr.starts))
		return map[string]float64{envoyTunnelsOpened: 50 + started, envoyTunnels: started, envoyTunnelConns: started,
			envoyStreams: started, envoyActorConns: min(started, 1), envoyHTTP2Conns: min(started, 1)}
	}
	r.usage = newUsageSampler(get, r.k8s, cfg.Atespace)
	r.usage.interval = 5 * time.Millisecond
	r.usage.cadvisorInterval = 2 * time.Millisecond

	rep, err := r.run(t.Context())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	var names []string
	for i, p := range rep.Phases {
		names = append(names, p.Name)
		if p.End.Before(p.Start) || (i > 0 && p.Start.Before(rep.Phases[i-1].End)) {
			t.Errorf("phase %s runs %v to %v, out of order", p.Name, p.Start, p.End)
		}
	}
	if got, want := strings.Join(names, ","), "pre-idle,create,resume,start,steady,stop,suspend"; got != want {
		t.Errorf("phases = %s, want %s", got, want)
	}
	if rep.Usage == nil || rep.Usage.Gateway == nil {
		t.Fatalf("usage = %+v, want a section with a gateway summary", rep.Usage)
	}
	g := rep.Usage.Gateway
	want := gatewaySummary{TunnelsOpened: 3, TunnelsMax: 3, TunnelConnsMax: 3, ActorConnsMax: 1, StreamsPerConnMax: 3, HTTP2Conns: 1,
		PoolConnsMax:     map[string]float64{"mitm_internal": 3, "egress_forward_proxy": 0, "egress_forward_proxy_cleartext": 0},
		HeapAllocatedMax: fakeHeapAllocated, HeapPerTunnel: fakeHeapAllocated / 3.0}
	if g.TunnelsOpened != want.TunnelsOpened || g.TunnelsMax != want.TunnelsMax || g.ActorConnsMax != want.ActorConnsMax ||
		g.StreamsPerConnMax != want.StreamsPerConnMax || g.HTTP2Conns != want.HTTP2Conns || !maps.Equal(g.PoolConnsMax, want.PoolConnsMax) ||
		g.HeapAllocatedMax != want.HeapAllocatedMax || g.HeapPerTunnel != want.HeapPerTunnel || g.StreamIdleCuts != 0 || len(g.BreakersOpened) != 0 {
		t.Errorf("gateway summary = %+v, want %+v", g, want)
	}
	if len(rep.Usage.Flags) != 0 {
		t.Errorf("flags = %v, want none on a clean run", rep.Usage.Flags)
	}
	gw := rep.Usage.Components["gateway"]
	if gw == nil || gw.Containers["ext-proc"] == nil || gw.Containers["ext-proc"].LiveCPUCores == nil {
		t.Errorf("gateway component = %+v, want ext-proc's exact steady cores from the marks", gw)
	}
	if d := rep.Usage.Components["driver"]; d == nil || d.Steady.LiveCPUCores == nil {
		t.Errorf("driver component = %+v, want its steady cores", d)
	}
	if len(rep.LoopTimeline) == 0 || rep.LoopTimeline[len(rep.LoopTimeline)-1].Requests == 0 {
		t.Errorf("loop timeline = %+v, want the progress polls", rep.LoopTimeline)
	}
	var printed strings.Builder
	rep.print(&printed)
	for _, line := range []string{
		"gateway    tunnels opened +3, open max 3 (tunnel connections max 3); actor connections max 1, streams per connection max 3; h2 connections +1, http1 +0",
		"cut by the stream idle timeout +0, by the connection idle timeout +0, protocol errors +0",
		"upstream connections max: mitm_internal 3, egress_forward_proxy 0, egress_forward_proxy_cleartext 0",
		"memory     envoy heap allocated max 11Mi (4Mi per tunnel at 3 tunnels)",
	} {
		if !strings.Contains(printed.String(), line) {
			t.Errorf("report lacks %q:\n%s", line, printed.String())
		}
	}
}

// The usage section is absent from the JSON of a run without --usage, so
// readers of the report need not know it.
func TestReportJSONOmitsUsageWithoutTheFlag(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		usage *usageReport
		want  bool
	}{
		{"without", nil, false},
		{"with", &usageReport{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(&report{Usage: tc.usage})
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if _, ok := fields["usage"]; ok != tc.want {
				t.Errorf("usage key present = %v, want %v", ok, tc.want)
			}
		})
	}
}
