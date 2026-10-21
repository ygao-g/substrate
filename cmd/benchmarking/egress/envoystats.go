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
	"fmt"
	"regexp"
	"slices"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// Process metrics every Go component serves on its Prometheus port. They are
// read from /proc at scrape time, so each scrape is current.
const (
	metricProcessCPU = "process_cpu_seconds_total"
	metricProcessRSS = "process_resident_memory_bytes"
)

// envoyClusters are the gateway clusters the sampler reads. mitm_internal
// carries one TCP connection and one request per tunnel; the two forward
// proxy clusters pool the gateway's connections to the targets, TLS and
// cleartext.
var envoyClusters = []string{"mitm_internal", "egress_forward_proxy", "egress_forward_proxy_cleartext"}

// envoyClusterStats maps each cluster metric the sampler reads to its admin
// stat; the report keys them cluster.<cluster>.<stat>. The three overflow
// counters and the two breaker gauges say when a cluster refused: a
// request past max_connections waits in the pending queue (cx_overflow
// counts the holds, pending_active is the depth) and is refused when that
// queue is full too (pending_overflow); one past max_requests is refused on
// attach (rq_active_overflow); the gauges are 1 while a breaker is open.
var envoyClusterStats = map[string]string{
	"envoy_cluster_upstream_cx_total":                "upstream_cx_total",
	"envoy_cluster_upstream_cx_active":               "upstream_cx_active",
	"envoy_cluster_upstream_rq_active":               "upstream_rq_active",
	"envoy_cluster_upstream_cx_overflow":             "upstream_cx_overflow",
	"envoy_cluster_upstream_rq_pending_active":       "upstream_rq_pending_active",
	"envoy_cluster_upstream_rq_pending_overflow":     "upstream_rq_pending_overflow",
	"envoy_cluster_upstream_rq_cancelled":            "upstream_rq_cancelled",
	"envoy_cluster_upstream_rq_active_overflow":      "upstream_rq_active_overflow",
	"envoy_cluster_circuit_breakers_default_cx_open": "circuit_breakers.default.cx_open",
	"envoy_cluster_circuit_breakers_default_rq_open": "circuit_breakers.default.rq_open",
}

// envoyConnectPrefix is the stat prefix of the HTTP connection manager that
// accepts the actors' CONNECTs.
const envoyConnectPrefix = "egress_connect"

// envoyConnectStats maps the egress_connect metrics the sampler reads to
// their admin stats; the report keys them http.egress_connect.<stat>. They
// are the downstream view the cluster stats lack: the actors' connections,
// the CONNECT streams multiplexed on them, the protocol each connection
// negotiated, and the timeouts and errors that cut a stream or a connection.
var envoyConnectStats = map[string]string{
	"envoy_http_downstream_cx_active":         "downstream_cx_active",
	"envoy_http_downstream_rq_active":         "downstream_rq_active",
	"envoy_http_downstream_cx_http1_total":    "downstream_cx_http1_total",
	"envoy_http_downstream_cx_http2_total":    "downstream_cx_http2_total",
	"envoy_http_downstream_rq_idle_timeout":   "downstream_rq_idle_timeout",
	"envoy_http_downstream_cx_idle_timeout":   "downstream_cx_idle_timeout",
	"envoy_http_downstream_cx_protocol_error": "downstream_cx_protocol_error",
}

// envoyServerStats maps the server gauges the sampler reads to their admin
// names: the allocator's view of Envoy's heap, which the cgroup's working
// set mixes with page cache and freed pages.
var envoyServerStats = map[string]string{
	"envoy_server_memory_allocated":     "server.memory_allocated",
	"envoy_server_memory_heap_size":     "server.memory_heap_size",
	"envoy_server_memory_physical_size": "server.memory_physical_size",
}

// Report keys of the stats the summary reads.
const (
	// envoyTunnels is the open tunnel count; envoyTunnelConns can run a
	// little higher while released connections linger idle.
	envoyTunnels        = "cluster.mitm_internal.upstream_rq_active"
	envoyTunnelConns    = "cluster.mitm_internal.upstream_cx_active"
	envoyTunnelsOpened  = "cluster.mitm_internal.upstream_cx_total"
	envoyActorConns     = "http.egress_connect.downstream_cx_active"
	envoyStreams        = "http.egress_connect.downstream_rq_active"
	envoyHTTP1Conns     = "http.egress_connect.downstream_cx_http1_total"
	envoyHTTP2Conns     = "http.egress_connect.downstream_cx_http2_total"
	envoyStreamIdleCuts = "http.egress_connect.downstream_rq_idle_timeout"
	envoyConnIdleCuts   = "http.egress_connect.downstream_cx_idle_timeout"
	envoyProtocolErrors = "http.egress_connect.downstream_cx_protocol_error"
	envoyHeapAllocated  = "server.memory_allocated"
	// Per-cluster stats the summary reads through clusterKey.
	envoyStatCxActive  = "upstream_cx_active"
	envoyStatCxBreaker = "circuit_breakers.default.cx_open"
	envoyStatRqBreaker = "circuit_breakers.default.rq_open"
)

// envoyOverflowStats are the counters that rise when a cluster refuses.
var envoyOverflowStats = []string{"upstream_cx_overflow", "upstream_rq_pending_overflow", "upstream_rq_active_overflow"}

// clusterKey is the report key of a cluster's stat.
func clusterKey(cluster, stat string) string {
	return "cluster." + cluster + "." + stat
}

// envoyStatsFilter selects the sampler's stats on /stats/prometheus, which
// filters on the admin stat name.
var envoyStatsFilter = envoyFilter()

func envoyFilter() string {
	alternatives := func(m map[string]string) string {
		out := make([]string, 0, len(m))
		for _, stat := range m {
			out = append(out, regexp.QuoteMeta(stat))
		}
		slices.Sort(out)
		return strings.Join(out, "|")
	}
	return `^(cluster\.(` + strings.Join(envoyClusters, "|") + `)\.(` + alternatives(envoyClusterStats) + `)|` +
		`http\.` + envoyConnectPrefix + `\.(` + alternatives(envoyConnectStats) + `)|` +
		alternatives(envoyServerStats) + `)$`
}

// envoyStatCount is how many stats a complete read of one gateway yields.
var envoyStatCount = len(envoyClusters)*len(envoyClusterStats) + len(envoyConnectStats) + len(envoyServerStats)

// parseText decodes a Prometheus text body with the decoder the actor
// scrapes share, so every exposition the driver reads goes through one parser.
func parseText(body []byte) (families, error) {
	return decodeFamilies(bytes.NewReader(body), expfmt.NewFormat(expfmt.TypeTextPlain))
}

// parseProcessMetrics reads the process CPU and RSS from Prometheus text.
func parseProcessMetrics(body []byte) (cpuSeconds, rssBytes float64, err error) {
	families, err := parseText(body)
	if err != nil {
		return 0, 0, err
	}
	value := func(name string) (float64, bool) {
		for _, m := range families[name].GetMetric() {
			if v, ok := sampleValue(m); ok {
				return v, true
			}
		}
		return 0, false
	}
	cpu, ok := value(metricProcessCPU)
	if !ok {
		return 0, 0, fmt.Errorf("no %s in the metrics", metricProcessCPU)
	}
	rss, _ := value(metricProcessRSS)
	return cpu, rss, nil
}

// sampleValue is a counter's, gauge's or untyped sample's value.
func sampleValue(m *dto.Metric) (float64, bool) {
	switch {
	case m.Counter != nil:
		return m.Counter.GetValue(), true
	case m.Gauge != nil:
		return m.Gauge.GetValue(), true
	case m.Untyped != nil:
		return m.Untyped.GetValue(), true
	}
	return 0, false
}

func labelMap(m *dto.Metric) map[string]string {
	out := make(map[string]string, len(m.GetLabel()))
	for _, lp := range m.GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// parseEnvoyStats decodes the gateway's /stats/prometheus text into the
// stats of the three tables, keyed by their admin names. Other families,
// clusters and connection managers are dropped. A body that is not
// Prometheus text yields nothing, which readEnvoy counts as a failed read.
func parseEnvoyStats(body []byte) map[string]float64 {
	out := map[string]float64{}
	families, err := parseText(body)
	if err != nil {
		return out
	}
	for metric, stat := range envoyClusterStats {
		for _, m := range families[metric].GetMetric() {
			cluster := labelMap(m)["envoy_cluster_name"]
			if !slices.Contains(envoyClusters, cluster) {
				continue
			}
			if v, ok := sampleValue(m); ok {
				out[clusterKey(cluster, stat)] = v
			}
		}
	}
	for metric, stat := range envoyConnectStats {
		for _, m := range families[metric].GetMetric() {
			if labelMap(m)["envoy_http_conn_manager_prefix"] != envoyConnectPrefix {
				continue
			}
			if v, ok := sampleValue(m); ok {
				out["http."+envoyConnectPrefix+"."+stat] = v
			}
		}
	}
	for metric, stat := range envoyServerStats {
		for _, m := range families[metric].GetMetric() {
			if v, ok := sampleValue(m); ok {
				out[stat] = v
			}
		}
	}
	return out
}
