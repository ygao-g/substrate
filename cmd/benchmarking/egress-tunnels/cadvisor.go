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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/agent-substrate/substrate/internal/egresstunnels"
	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// cadvisorPollInterval paces the cAdvisor polls. The kubelet refreshes each
// container every 12 to 20 s, so faster polls only find the same readings.
const cadvisorPollInterval = 5 * time.Second

// podContainer names the pod cgroup's own row, the sum of its containers.
const podContainer = "POD"

// workerPoolLabel is on every pod a WorkerPool runs.
const workerPoolLabel = "ate.dev/worker-pool"

// cadvisorTarget is a component whose containers cAdvisor reports.
type cadvisorTarget struct {
	component, namespace, selector string
	containers                     []string
}

// cadvisorTargets are the components on the egress path. The workers are
// the WorkerPool pods in the actors' atespace; a worker pod is the smallest
// unit cAdvisor sees, and it holds the actors, gVisor, atunnel and the
// sandbox DNS relay together.
func cadvisorTargets(atespace string) []cadvisorTarget {
	system := installdefaults.SystemNamespace
	return []cadvisorTarget{
		{"workers", atespace, workerPoolLabel, []string{"ateom"}},
		{"gateway", system, "app=" + gatewayDeployment, []string{"envoy", "ext-proc", "sdsmint"}},
		{"ateapi", system, "app=ate-api-server", []string{"ate-api-server"}},
		{"router", system, "app=atenet-router", []string{"atenet-router", "envoy"}},
		{"targets", egresstunnels.TargetNamespace, "app=egress-target", []string{"target"}},
		{"dns", "kube-system", "k8s-app=kube-dns", []string{"coredns", "kubedns", "dnsmasq"}},
	}
}

// cadvisorSample is one container's counters at the time cAdvisor read them.
type cadvisorSample struct {
	T               time.Time `json:"t"`
	Component       string    `json:"component"`
	Pod             string    `json:"pod"`
	Container       string    `json:"container"`
	CPUSeconds      float64   `json:"cpuSeconds"`
	CFSPeriods      float64   `json:"cfsPeriods,omitempty"`
	CFSThrottled    float64   `json:"cfsThrottled,omitempty"`
	WorkingSetBytes float64   `json:"workingSetBytes"`
}

func (s cadvisorSample) key() string { return s.Component + "/" + s.Container + "@" + s.Pod }

// cadvisorPod is a target pod: its component and the containers to keep.
type cadvisorPod struct {
	component  string
	containers []string
}

var cadvisorMetrics = []string{
	"container_cpu_usage_seconds_total",
	"container_cpu_cfs_periods_total",
	"container_cpu_cfs_throttled_periods_total",
	"container_memory_working_set_bytes",
}

// parseCadvisor returns the target containers' readings from one node's
// /metrics/cadvisor. pods is keyed by namespace/name. The pod cgroup row,
// which has no container and no image, is kept as container POD.
func parseCadvisor(body []byte, pods map[string]cadvisorPod) ([]cadvisorSample, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parsing cadvisor metrics: %w", err)
	}
	type rowKey struct{ ns, pod, container string }
	rows := map[rowKey]*cadvisorSample{}
	for _, name := range cadvisorMetrics {
		for _, m := range families[name].GetMetric() {
			l := labelMap(m)
			p, ok := pods[l["namespace"]+"/"+l["pod"]]
			if !ok {
				continue
			}
			container := l["container"]
			switch {
			case container == "" && l["image"] == "":
				container = podContainer
			case !slices.Contains(p.containers, container):
				continue
			}
			k := rowKey{l["namespace"], l["pod"], container}
			row := rows[k]
			if row == nil {
				row = &cadvisorSample{Component: p.component, Pod: l["pod"], Container: container}
				rows[k] = row
			}
			if ts := time.UnixMilli(m.GetTimestampMs()); m.GetTimestampMs() > 0 && ts.After(row.T) {
				row.T = ts
			}
			v, _ := sampleValue(m)
			switch name {
			case "container_cpu_usage_seconds_total":
				row.CPUSeconds = v
			case "container_cpu_cfs_periods_total":
				row.CFSPeriods = v
			case "container_cpu_cfs_throttled_periods_total":
				row.CFSThrottled = v
			case "container_memory_working_set_bytes":
				row.WorkingSetBytes = v
			}
		}
	}
	out := make([]cadvisorSample, 0, len(rows))
	for _, r := range rows {
		out = append(out, *r)
	}
	slices.SortFunc(out, func(a, b cadvisorSample) int {
		return cmp.Or(a.T.Compare(b.T), strings.Compare(a.key(), b.key()))
	})
	return out, nil
}
