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
	"maps"
	"os"
	"regexp"
	"testing"
)

func TestParseProcessMetrics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		body             string
		wantCPU, wantRSS float64
		wantErr          bool
	}{
		{name: "both", body: "# HELP x\nprocess_cpu_seconds_total 127.37\nprocess_resident_memory_bytes 5.8896384e+07\n", wantCPU: 127.37, wantRSS: 58896384},
		{name: "with timestamp", body: "process_cpu_seconds_total 1.5 1791287695035\n", wantCPU: 1.5},
		{name: "lookalike ignored", body: "process_cpu_seconds_total_x 9\nprocess_cpu_seconds_total 2\n", wantCPU: 2},
		{name: "missing cpu", body: "process_resident_memory_bytes 1\n", wantErr: true},
		{name: "bad value", body: "process_cpu_seconds_total abc\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cpu, rss, err := parseProcessMetrics([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if cpu != tc.wantCPU || rss != tc.wantRSS {
				t.Errorf("got cpu %v rss %v, want %v %v", cpu, rss, tc.wantCPU, tc.wantRSS)
			}
		})
	}
}

func TestParseEnvoyStats(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body string
		want map[string]float64
	}{
		{
			name: "cluster stats keyed by admin name, other clusters dropped",
			body: `# HELP envoy_cluster_upstream_cx_total Multiline...
# TYPE envoy_cluster_upstream_cx_total counter
envoy_cluster_upstream_cx_total{envoy_cluster_name="mitm_internal"} 42
envoy_cluster_upstream_cx_total{envoy_cluster_name="egress_forward_proxy_cleartext"} 7 1791287695035
envoy_cluster_upstream_cx_total{envoy_cluster_name="ext_proc_server"} 9
envoy_cluster_upstream_rq_active{envoy_cluster_name="mitm_internal"} 40
envoy_cluster_circuit_breakers_default_cx_open{envoy_cluster_name="mitm_internal"} 1
envoy_cluster_circuit_breakers_high_cx_open{envoy_cluster_name="mitm_internal"} 1
envoy_cluster_upstream_cx_total_x{envoy_cluster_name="mitm_internal"} 1
envoy_cluster_upstream_cx_length_ms_bucket{envoy_cluster_name="mitm_internal",le="0.5"} 0
`,
			want: map[string]float64{
				envoyTunnelsOpened: 42,
				"cluster.egress_forward_proxy_cleartext.upstream_cx_total": 7,
				envoyTunnels: 40,
				"cluster.mitm_internal.circuit_breakers.default.cx_open": 1,
			},
		},
		{
			name: "connection manager stats of egress_connect only",
			body: `envoy_http_downstream_cx_active{envoy_http_conn_manager_prefix="egress_connect"} 100
envoy_http_downstream_rq_active{envoy_http_conn_manager_prefix="egress_connect"} 10000
envoy_http_downstream_cx_active{envoy_http_conn_manager_prefix="mitm_http"} 5
envoy_http_downstream_rq_idle_timeout{envoy_http_conn_manager_prefix="egress_connect"} 2
envoy_http_downstream_rq_total{envoy_http_conn_manager_prefix="egress_connect"} 10000
`,
			want: map[string]float64{envoyActorConns: 100, envoyStreams: 10000, envoyStreamIdleCuts: 2},
		},
		{
			name: "server gauges",
			body: "envoy_server_memory_allocated{} 550899088\nenvoy_server_memory_heap_size{} 570425344\nenvoy_server_uptime{} 12\n",
			want: map[string]float64{envoyHeapAllocated: 550899088, "server.memory_heap_size": 570425344},
		},
		{
			// An error page served with 200 yields nothing rather than
			// whatever lines happened to parse.
			name: "junk body",
			body: "<html>busy</html>\nenvoy_server_memory_allocated{} 1\n",
			want: map[string]float64{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := parseEnvoyStats([]byte(tc.body)); !maps.Equal(got, tc.want) {
				t.Errorf("parseEnvoyStats = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnvoyStatsFilterMatchesTables pins the filter sent to Envoy to the
// stats the parser reads, so neither can drift from the other.
func TestEnvoyStatsFilterMatchesTables(t *testing.T) {
	t.Parallel()
	filter := regexp.MustCompile(envoyStatsFilter)
	var want []string
	for _, c := range envoyClusters {
		for _, stat := range envoyClusterStats {
			want = append(want, clusterKey(c, stat))
		}
	}
	for _, stat := range envoyConnectStats {
		want = append(want, "http."+envoyConnectPrefix+"."+stat)
	}
	for _, stat := range envoyServerStats {
		want = append(want, stat)
	}
	if len(want) != envoyStatCount {
		t.Errorf("%d stats across the tables, want envoyStatCount %d", len(want), envoyStatCount)
	}
	for _, name := range want {
		if !filter.MatchString(name) {
			t.Errorf("envoyStatsFilter does not select %s", name)
		}
	}
	for _, name := range []string{"cluster.ext_proc_server.upstream_cx_total", "cluster.mitm_internal.upstream_cx_total_x",
		"cluster.mitm_internal.circuit_breakers.high.cx_open", "http.mitm_http.downstream_cx_active", "server.memory_allocated_x"} {
		if filter.MatchString(name) {
			t.Errorf("envoyStatsFilter selects %s", name)
		}
	}
}

// TestEnvoyStatsNamesAreReal reads a dump captured from a gateway with
// 10,000 tunnels open on 100 HTTP/2 connections, so the Prometheus names in
// the tables are the ones Envoy serves and the values mean what the summary
// takes them for.
func TestEnvoyStatsNamesAreReal(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile("testdata/envoy-stats-prometheus.txt")
	if err != nil {
		t.Fatal(err)
	}
	got := parseEnvoyStats(body)
	if len(got) != envoyStatCount {
		t.Errorf("parsed %d stats from the dump, want %d", len(got), envoyStatCount)
	}
	for _, tc := range []struct {
		key  string
		want float64
	}{
		{envoyTunnels, 10000},
		{envoyTunnelConns, 10000},
		{envoyTunnelsOpened, 10000},
		{envoyActorConns, 100},
		{envoyStreams, 10000},
		{envoyHTTP2Conns, 100},
		{envoyHTTP1Conns, 0},
		{envoyStreamIdleCuts, 0},
		{envoyConnIdleCuts, 0},
		{envoyProtocolErrors, 0},
		{envoyHeapAllocated, 550899088},
		{"cluster.egress_forward_proxy_cleartext.upstream_cx_active", 747},
		{"cluster.egress_forward_proxy.upstream_cx_active", 0},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			v, ok := got[tc.key]
			if !ok || v != tc.want {
				t.Errorf("dump[%s] = %v (present %v), want %v", tc.key, v, ok, tc.want)
			}
		})
	}
}
