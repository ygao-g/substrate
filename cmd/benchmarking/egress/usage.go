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
	"maps"
	"net/url"
	"slices"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/installdefaults"
)

// rawGetter fetches a raw API server path, such as a pod or node proxy URL.
// It is the seam the usage tests replace.
type rawGetter interface {
	GetRaw(ctx context.Context, path string, params url.Values) ([]byte, error)
}

type restRawGetter struct{ c rest.Interface }

// The sampler's own client settings. One poll round reads a few pods every
// second and every node every 5 s; client-go's default of 5 requests per
// second would space the one-second reads out. The API server applies no
// request timeout to proxy requests, so without samplerTimeout a pod that
// accepts the connection and never answers would hold a read, and the
// phase-boundary reads with it, for as long as TCP allows.
const (
	samplerQPS     = 50
	samplerBurst   = 100
	samplerTimeout = 30 * time.Second
	// liveInterval paces the process and Envoy reads.
	liveInterval = time.Second
)

// tuneSamplerConfig sets the sampler's rate limit and per-read timeout.
func tuneSamplerConfig(cfg *rest.Config) {
	cfg.QPS, cfg.Burst = samplerQPS, samplerBurst
	cfg.Timeout = samplerTimeout
}

// newSamplerGetter builds a rawGetter with its own client and rate limit, so
// sampling neither waits behind the run's own API calls nor delays them.
func newSamplerGetter(kubeconfig, kubeContext string) (rawGetter, error) {
	cfg, err := ateclient.LoadKubeConfig(kubeconfig, kubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	tuneSamplerConfig(cfg)
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating the sampler's Kubernetes client: %w", err)
	}
	return restRawGetter{cs.CoreV1().RESTClient()}, nil
}

func (g restRawGetter) GetRaw(ctx context.Context, path string, params url.Values) ([]byte, error) {
	req := g.c.Get().AbsPath(path)
	for k, vs := range params {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	return req.DoRaw(ctx)
}

func podProxyPath(ns, pod string, port int, path string) string {
	return fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy%s", ns, pod, port, path)
}

// liveTarget is a Go component whose process counters are read on each poll.
type liveTarget struct {
	component, namespace, selector, container string
	port                                      int
}

var liveTargets = []liveTarget{
	{component: "gateway", namespace: installdefaults.SystemNamespace, selector: "app=" + gatewayDeployment, container: "ext-proc", port: 9090},
	{component: "ateapi", namespace: installdefaults.SystemNamespace, selector: "app=ate-api-server", container: "ate-api-server", port: 9090},
}

// envoyTarget is the gateway Envoy whose stats are read on each poll, through
// its envoy_metrics listener. The admin API itself listens on loopback only,
// which the API server's pod proxy cannot reach.
var envoyTarget = struct {
	namespace, selector string
	port                int
	path                string
}{installdefaults.SystemNamespace, "app=" + gatewayDeployment, 15090, "/stats/prometheus"}

// Phase marks that force a read of every live source, so the exact process
// counters bracket the steady window.
const (
	markSteadyBegin = "steady:begin"
	markSteadyEnd   = "steady:end"
)

// usageReport is the usage section of the report: the raw readings, the
// summaries over the run's windows, and what the gateway's counters say.
type usageReport struct {
	Sources []sourceInfo  `json:"sources"`
	Live    []liveSample  `json:"live,omitempty"`
	Envoy   []envoySample `json:"envoy,omitempty"`
	// Cadvisor holds the kubelet's readings, one per container per new
	// cAdvisor timestamp.
	Cadvisor []cadvisorSample `json:"cadvisor,omitempty"`
	// Components summarizes each component over the steady window and,
	// when there is one, the settled window.
	Components map[string]*componentSummary `json:"components,omitempty"`
	// Baseline is each component's and container's use over the pre-idle
	// phase, keyed "component" and "component/container".
	Baseline map[string]*windowSummary `json:"baseline,omitempty"`
	// Settled is the window the components' settled summaries cover.
	Settled *settledWindow  `json:"settledWindow,omitempty"`
	Gateway *gatewaySummary `json:"gateway,omitempty"`
	Driver  driverUsage     `json:"driver"`
	// Reads and Errors count answered and failed reads per source and
	// target, keyed source/target: the pod for live and envoy, the node for
	// cadvisor.
	Reads  map[string]int `json:"reads,omitempty"`
	Errors map[string]int `json:"errors,omitempty"`
	// Flags name what the gateway's counters say went wrong during the run.
	// They do not fail the run.
	Flags []string `json:"flags,omitempty"`
}

// sourceInfo says how often a source is read and whose clock stamps it.
type sourceInfo struct {
	Name     string        `json:"name"`
	Interval time.Duration `json:"interval"`
	Clock    string        `json:"clock"`
}

// liveSample is one read of a Go process's counters. Label names the phase
// mark that forced the read, if any.
type liveSample struct {
	T                 time.Time `json:"t"`
	Label             string    `json:"label,omitempty"`
	Component         string    `json:"component"`
	Pod               string    `json:"pod"`
	Container         string    `json:"container"`
	ProcessCPUSeconds float64   `json:"processCpuSeconds"`
	RSSBytes          float64   `json:"rssBytes"`
	// RTT is the read's round trip; T is its midpoint.
	RTT time.Duration `json:"rttNs,omitempty"`
}

// envoySample is one read of a gateway Envoy's stats, keyed by admin name.
type envoySample struct {
	T        time.Time          `json:"t"`
	Label    string             `json:"label,omitempty"`
	Pod      string             `json:"pod"`
	Counters map[string]float64 `json:"counters"`
}

// driverUsage is the driver's own CPU, from getrusage, over the whole run and
// over the steady window.
type driverUsage struct {
	WholeRun usageWindow `json:"wholeRun"`
	Steady   usageWindow `json:"steady"`
}

type usageWindow struct {
	CPUSeconds  float64 `json:"cpuSeconds"`
	WallSeconds float64 `json:"wallSeconds"`
	Cores       float64 `json:"cores"`
}

// rusageMark is the driver's CPU time at one moment.
type rusageMark struct {
	at  time.Time
	cpu float64
}

func selfCPU() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

func usageBetween(a, b rusageMark) usageWindow {
	w := usageWindow{CPUSeconds: b.cpu - a.cpu, WallSeconds: b.at.Sub(a.at).Seconds()}
	if w.WallSeconds > 0 {
		w.Cores = w.CPUSeconds / w.WallSeconds
	}
	return w
}

// usageSampler reads the CPU and memory of the components on the egress
// path while a run goes on. Each source polls on its own cadence; the
// steady marks force an extra read of the live sources so the window lines
// up with them.
type usageSampler struct {
	get              rawGetter
	k8s              kubernetes.Interface
	atespace         string
	interval         time.Duration
	cadvisorInterval time.Duration
	now              func() time.Time

	mu     sync.Mutex
	rep    usageReport
	live   map[liveTarget][]string // target -> pod names
	envoys []string
	// cadvisorPods is keyed by namespace/name; nodes are where they run.
	cadvisorPods map[string]cadvisorPod
	nodes        []string
	lastTS       map[string]time.Time // series key -> newest cAdvisor timestamp kept
	// missing lists every cAdvisor component that matched no Running pod.
	missing []string
	rusage  map[string]rusageMark
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped bool
}

func newUsageSampler(get rawGetter, k8s kubernetes.Interface, atespace string) *usageSampler {
	return &usageSampler{
		get:              get,
		k8s:              k8s,
		atespace:         atespace,
		interval:         liveInterval,
		cadvisorInterval: cadvisorPollInterval,
		now:              time.Now,
		lastTS:           map[string]time.Time{},
		rusage:           map[string]rusageMark{},
		rep:              usageReport{Reads: map[string]int{}, Errors: map[string]int{}},
	}
}

// start finds the target pods and begins polling. Polling runs until stop,
// independent of ctx's cancellation, so cleanup after an interrupt is still
// measured.
func (s *usageSampler) start(ctx context.Context) error {
	s.live = map[liveTarget][]string{}
	for _, t := range liveTargets {
		pods, err := runningPods(ctx, s.k8s, t.namespace, t.selector)
		if err != nil {
			return err
		}
		s.live[t] = pods
	}
	envoys, err := runningPods(ctx, s.k8s, envoyTarget.namespace, envoyTarget.selector)
	if err != nil {
		return err
	}
	s.envoys = envoys
	if err := s.resolveCadvisor(ctx); err != nil {
		return err
	}
	s.rep.Sources = []sourceInfo{
		{Name: "live", Interval: s.interval, Clock: "driver"},
		{Name: "envoy", Interval: s.interval, Clock: "driver"},
		{Name: "cadvisor", Interval: s.cadvisorInterval, Clock: "kubelet"},
	}
	s.markRusage("run:begin")

	// Each source polls on its own goroutine, so a slow cAdvisor fetch never
	// delays the one-second sources.
	pollCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.every(pollCtx, s.interval, func(ctx context.Context) { s.pollLive(ctx, "") })
	s.every(pollCtx, s.cadvisorInterval, s.pollCadvisor)
	return nil
}

// every calls poll at once and then every interval until ctx ends.
func (s *usageSampler) every(ctx context.Context, interval time.Duration, poll func(context.Context)) {
	s.wg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			poll(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
}

// podContainers names the containers that run for a pod's lifetime: its
// containers and its native sidecars, the init containers that restart
// always. Ordinary init containers have exited by the time it runs.
func podContainers(p *corev1.Pod) []string {
	var out []string
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			out = append(out, c.Name)
		}
	}
	for _, c := range p.Spec.Containers {
		out = append(out, c.Name)
	}
	return out
}

// resolveCadvisor finds the pods of every cAdvisor target and their nodes.
func (s *usageSampler) resolveCadvisor(ctx context.Context) error {
	s.cadvisorPods = map[string]cadvisorPod{}
	nodes := map[string]bool{}
	for _, t := range cadvisorTargets(s.atespace) {
		list, err := s.k8s.CoreV1().Pods(t.namespace).List(ctx, metav1.ListOptions{LabelSelector: t.selector})
		if err != nil {
			return fmt.Errorf("listing pods %s in %s: %w", t.selector, t.namespace, err)
		}
		found := false
		for _, p := range list.Items {
			if p.Status.Phase != corev1.PodRunning {
				continue
			}
			found = true
			have := podContainers(&p)
			var containers []string
			for _, c := range t.containers {
				if slices.Contains(have, c) {
					containers = append(containers, c)
				}
			}
			s.cadvisorPods[p.Namespace+"/"+p.Name] = cadvisorPod{component: t.component, containers: containers}
			if p.Spec.NodeName != "" {
				nodes[p.Spec.NodeName] = true
			}
		}
		if !found {
			s.missing = append(s.missing, t.component)
		}
	}
	s.nodes = slices.Sorted(maps.Keys(nodes))
	return nil
}

// pollCadvisor reads every node's cAdvisor and keeps the readings whose
// timestamp is new for their series.
func (s *usageSampler) pollCadvisor(ctx context.Context) {
	var wg sync.WaitGroup
	for _, node := range s.nodes {
		wg.Go(func() {
			body, err := s.get.GetRaw(ctx, "/api/v1/nodes/"+node+"/proxy/metrics/cadvisor", nil)
			if err != nil {
				s.fail(err, "cadvisor", node)
				return
			}
			rows, err := parseCadvisor(body, s.cadvisorPods)
			if err != nil {
				s.fail(err, "cadvisor", node)
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			s.rep.Reads[sourceKey("cadvisor", node)]++
			for _, r := range rows {
				if r.T.IsZero() || !r.T.After(s.lastTS[r.key()]) {
					continue
				}
				s.lastTS[r.key()] = r.T
				s.rep.Cadvisor = append(s.rep.Cadvisor, r)
			}
		})
	}
	wg.Wait()
}

// mark records a phase boundary: it reads every live source once, labeled,
// and notes the driver's CPU time.
func (s *usageSampler) mark(ctx context.Context, label string) {
	s.markRusage(label)
	s.pollLive(context.WithoutCancel(ctx), label)
}

func (s *usageSampler) markRusage(label string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rusage[label] = rusageMark{at: s.now(), cpu: selfCPU()}
}

// stop ends polling and returns the collected section. It is safe to call
// more than once.
func (s *usageSampler) stop() *usageReport {
	s.mu.Lock()
	stopped := s.stopped
	s.stopped = true
	s.mu.Unlock()
	if !stopped {
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		s.markRusage("run:end")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.Driver.WholeRun = usageBetween(s.rusage["run:begin"], s.rusage["run:end"])
	if a, ok := s.rusage[markSteadyBegin]; ok {
		if b, ok := s.rusage[markSteadyEnd]; ok {
			s.rep.Driver.Steady = usageBetween(a, b)
		}
	}
	rep := s.rep
	return &rep
}

// pollLive reads every Go process and Envoy once, in parallel.
func (s *usageSampler) pollLive(ctx context.Context, label string) {
	var wg sync.WaitGroup
	for t, pods := range s.live {
		for _, pod := range pods {
			wg.Go(func() { s.readProcess(ctx, t, pod, label) })
		}
	}
	for _, pod := range s.envoys {
		wg.Go(func() { s.readEnvoy(ctx, pod, label) })
	}
	wg.Wait()
}

func (s *usageSampler) readProcess(ctx context.Context, t liveTarget, pod, label string) {
	sent := s.now()
	body, err := s.get.GetRaw(ctx, podProxyPath(t.namespace, pod, t.port, "/metrics"), nil)
	rtt := s.now().Sub(sent)
	if err == nil {
		var cpu, rss float64
		if cpu, rss, err = parseProcessMetrics(body); err == nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.rep.Live = append(s.rep.Live, liveSample{T: sent.Add(rtt / 2), Label: label, Component: t.component, Pod: pod,
				Container: t.container, ProcessCPUSeconds: cpu, RSSBytes: rss, RTT: rtt})
			s.rep.Reads[sourceKey("live", pod)]++
			return
		}
	}
	s.fail(err, "live", pod)
}

func (s *usageSampler) readEnvoy(ctx context.Context, pod, label string) {
	body, err := s.get.GetRaw(ctx, podProxyPath(envoyTarget.namespace, pod, envoyTarget.port, envoyTarget.path),
		url.Values{"filter": {envoyStatsFilter}})
	at := s.now()
	var counters map[string]float64
	if err == nil {
		counters = parseEnvoyStats(body)
	}
	// Envoy serves every stat of a cluster from the start, zeros included, so
	// a body missing one means a renamed metric or cluster, not an idle
	// gateway. Such a read is a failure, or the gaps would pass unnoticed.
	if err == nil && len(counters) < envoyStatCount {
		err = fmt.Errorf("%d of %d stats in the body", len(counters), envoyStatCount)
	}
	if err != nil {
		s.fail(err, "envoy", pod)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.Envoy = append(s.rep.Envoy, envoySample{T: at, Label: label, Pod: pod, Counters: counters})
	s.rep.Reads[sourceKey("envoy", pod)]++
}

// sourceKey names one target of a source in Reads and Errors.
func sourceKey(source, target string) string {
	return source + "/" + target
}

// fail counts a failed read of source at target. A read that its context
// cut off, as stop() does to the polls in flight, is not the source's fault
// and is not counted.
func (s *usageSampler) fail(err error, source, target string) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.Errors[sourceKey(source, target)]++
}

// runningPods lists the names of the Running pods that match selector.
func runningPods(ctx context.Context, k8s kubernetes.Interface, ns, selector string) ([]string, error) {
	list, err := k8s.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("listing pods %s in %s: %w", selector, ns, err)
	}
	var names []string
	for _, p := range list.Items {
		if p.Status.Phase == corev1.PodRunning {
			names = append(names, p.Name)
		}
	}
	return names, nil
}
