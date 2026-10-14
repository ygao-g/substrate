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
	"testing"
	"time"
)

// cadvisorFixture is a trimmed /metrics/cadvisor body: two gateway
// containers, the pod cgroup, the pause container, a container the test
// does not target, and a pod outside the targets.
const cadvisorFixture = `# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed in seconds.
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="",cpu="total",id="/kubepods.slice/p.slice",image="",name="",namespace="ate-system",pod="gw"} 30.5 1791289219021
container_cpu_usage_seconds_total{container="",cpu="total",id="/kubepods.slice/p.slice/cri-containerd-a.scope",image="registry.k8s.io/pause:3.8",name="a",namespace="ate-system",pod="gw"} 0.01 1791289219021
container_cpu_usage_seconds_total{container="envoy",cpu="total",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 20.25 1791289219021
container_cpu_usage_seconds_total{container="ext-proc",cpu="total",id="/y",image="atenet",name="c",namespace="ate-system",pod="gw"} 10.24 1791289221000
container_cpu_usage_seconds_total{container="other",cpu="total",id="/z",image="o",name="d",namespace="ate-system",pod="gw"} 99 1791289219021
container_cpu_usage_seconds_total{container="envoy",cpu="total",id="/w",image="envoy",name="e",namespace="ate-system",pod="router"} 5 1791289219021
# TYPE container_cpu_cfs_periods_total counter
container_cpu_cfs_periods_total{container="envoy",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 1000 1791289219021
# TYPE container_cpu_cfs_throttled_periods_total counter
container_cpu_cfs_throttled_periods_total{container="envoy",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 25 1791289219021
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{container="envoy",id="/x",image="envoy",name="b",namespace="ate-system",pod="gw"} 6.2e+07 1791289219021
`

func TestParseCadvisor(t *testing.T) {
	t.Parallel()
	pods := map[string]cadvisorPod{"ate-system/gw": {component: "gateway", containers: []string{"envoy", "ext-proc"}}}
	rows, err := parseCadvisor([]byte(cadvisorFixture), pods)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]cadvisorSample{}
	for _, r := range rows {
		got[r.Container] = r
	}
	if len(got) != 3 {
		t.Fatalf("parsed containers %v, want POD, envoy and ext-proc only", rows)
	}
	for _, tc := range []struct {
		container string
		want      cadvisorSample
	}{
		{"POD", cadvisorSample{T: time.UnixMilli(1791289219021), Component: "gateway", Pod: "gw", Container: "POD", CPUSeconds: 30.5}},
		{"envoy", cadvisorSample{T: time.UnixMilli(1791289219021), Component: "gateway", Pod: "gw", Container: "envoy",
			CPUSeconds: 20.25, CFSPeriods: 1000, CFSThrottled: 25, WorkingSetBytes: 6.2e7}},
		{"ext-proc", cadvisorSample{T: time.UnixMilli(1791289221000), Component: "gateway", Pod: "gw", Container: "ext-proc", CPUSeconds: 10.24}},
	} {
		t.Run(tc.container, func(t *testing.T) {
			t.Parallel()
			if g := got[tc.container]; g != tc.want {
				t.Errorf("got %+v, want %+v", g, tc.want)
			}
		})
	}
	if _, err := parseCadvisor([]byte("not { prometheus"), pods); err == nil {
		t.Errorf("parseCadvisor accepted malformed text")
	}
}

// The workers are the WorkerPool pods in the actors' atespace; every other
// target has a fixed namespace.
func TestCadvisorTargetsFollowTheAtespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		component string
		wantNS    string
	}{
		{"workers", "bench"},
		{"gateway", "ate-system"},
		{"targets", "egress-tests-targets"},
	} {
		t.Run(tc.component, func(t *testing.T) {
			t.Parallel()
			for _, target := range cadvisorTargets("bench") {
				if target.component == tc.component && target.namespace != tc.wantNS {
					t.Errorf("%s target namespace = %s, want %s", tc.component, target.namespace, tc.wantNS)
				}
			}
		})
	}
}
