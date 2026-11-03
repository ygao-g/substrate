# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for cAdvisor resource sampler and Envoy/ext-proc stats scraper."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import envoy_stats
import resource_sampler
import resources_common as rc


def test_cadvisor_two_scrape_rate_and_pin_fraction():
    class DummyClient:
        def list_pods(self, ns, sel):
            return []

    sampler = resource_sampler.Sampler(DummyClient())
    node = "node-1"
    target = resource_sampler.Target("workers", "benchmark-workloads", "ate.dev/worker-pool", ("worker",))
    item = {"metadata": {"name": "worker-1"}}
    targets = [(target, item)]

    # Pod with CPU limit pinned to 4 cores
    sampler._container_limits[("worker-1", "worker")] = 4.0

    # Scrape 1 at t=1000.0, cpu=10.0s
    t1 = """
container_cpu_usage_seconds_total{pod="worker-1",container="worker"} 10.0 1000000
container_memory_working_set_bytes{pod="worker-1",container="worker"} 104857600 1000000
"""
    sampler._process_cadvisor_text(node, t1, targets)
    assert len(sampler._samples) == 1  # First scrape records memory, establishes cpu baseline
    assert "cpu_cores" not in sampler._samples[0].values
    assert sampler._samples[0].values["working_set_bytes"] == 104857600.0

    # Scrape 2 at t=1002.0, cpu=14.0s (delta 4.0s over 2.0s -> 2.0 cores rate)
    t2 = """
container_cpu_usage_seconds_total{pod="worker-1",container="worker"} 14.0 1002000
container_memory_working_set_bytes{pod="worker-1",container="worker"} 104857600 1002000
"""
    sampler._process_cadvisor_text(node, t2, targets)
    assert len(sampler._samples) == 2
    sample = sampler._samples[1]
    assert sample.values["cpu_cores"] == 2.0
    assert sample.values["limit_cores"] == 4.0

    # Test by_stage computes cpu_fraction_of_pin correctly
    from datetime import timedelta
    stage = rc.Stage(
        name="test_stage",
        start=sample.timestamp - timedelta(seconds=10),
        end=sample.timestamp + timedelta(seconds=10),
        total_rps=1000.0,
        failed_thresholds=[],
    )
    result = rc.by_stage([sample], [stage])
    container_entry = result[0]["containers"]["workers/worker@worker-1"]
    assert abs(container_entry["cpu_fraction_of_pin"]["mean"] - 0.5) < 1e-6

    # When unpinned (no limit), limit_cores is None and cpu_fraction_of_pin is None
    sampler._samples.clear()
    sampler._prev.clear()
    del sampler._container_limits[("worker-1", "worker")]

    sampler._process_cadvisor_text(node, t1, targets)
    sampler._process_cadvisor_text(node, t2, targets)
    sample_unpinned = sampler._samples[0]
    assert sample_unpinned.values["limit_cores"] is None
    result_unpinned = rc.by_stage([sample_unpinned], [stage])
    assert result_unpinned[0]["containers"]["workers/worker@worker-1"]["cpu_fraction_of_pin"] is None


def test_parse_envoy_prometheus_metrics():
    prom_text = """
# TYPE envoy_http_downstream_rq_2xx counter
envoy_http_downstream_rq_2xx{envoy_http_conn_manager_prefix="egress_connect"} 12345
# TYPE envoy_listener_ssl_handshake counter
envoy_listener_ssl_handshake{envoy_listener_address="0.0.0.0_443"} 5432
envoy_listener_ssl_handshake{envoy_listener_address="127.0.0.1_15001"} 1200
# TYPE envoy_cluster_upstream_rq_total counter
envoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"} 12345
# TYPE envoy_cluster_upstream_cx_overflow counter
envoy_cluster_upstream_cx_overflow{envoy_cluster_name="egress_forward_proxy"} 0
# TYPE envoy_server_memory_allocated gauge
envoy_server_memory_allocated 104857600
"""
    parsed = envoy_stats.parse_prometheus_stats(prom_text)
    assert parsed["http.egress_connect.downstream_rq_2xx"] == 12345.0
    assert parsed["listener.0.0.0.0_443.ssl_handshake"] == 5432.0
    assert parsed["listener.127.0.0.1_15001.ssl_handshake"] == 1200.0
    assert parsed["cluster.ext_proc_server.upstream_rq_total"] == 12345.0
    assert parsed["cluster.egress_forward_proxy.upstream_cx_overflow"] == 0.0
    assert parsed["server.memory_allocated"] == 104857600.0


def test_parse_ext_proc_metrics():
    ext_proc_prom = """
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 45.67
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 268435456
"""
    parsed = envoy_stats.parse_ext_proc_metrics(ext_proc_prom)
    assert parsed["ext_proc.process_cpu_seconds_total"] == 45.67
    assert parsed["ext_proc.process_resident_memory_bytes"] == 268435456.0


def test_envoy_stats_deltas_skips_missing_and_resets():
    start_rec = {
        "stage": "stage_001",
        "label": "start",
        "pods": {
            "pod-1": {
                "http.egress_connect.downstream_rq_2xx": 100.0,
                "listener.0.0.0.0_443.ssl_handshake": 10.0,
                "server.memory_allocated": 200.0,
                "cluster.reset_counter.upstream_rq_total": 500.0,
            }
        },
    }
    end_rec = {
        "stage": "stage_001",
        "label": "end",
        "pods": {
            "pod-1": {
                "http.egress_connect.downstream_rq_2xx": 1100.0,
                "listener.0.0.0.0_443.ssl_handshake": 60.0,
                # Gauge metric decreased: should be preserved
                "server.memory_allocated": 150.0,
                # Counter reset (500 -> 50): should be skipped
                "cluster.reset_counter.upstream_rq_total": 50.0,
                # Key absent in start: should be skipped
                "cluster.new_cluster.upstream_rq_total": 20.0,
            }
        },
    }
    deltas = envoy_stats.compute_stage_deltas([start_rec, end_rec])
    assert len(deltas) == 1
    assert deltas[0]["stage"] == "stage_001"
    pod1_delta = deltas[0]["pods"]["pod-1"]
    assert pod1_delta["http.egress_connect.downstream_rq_2xx"] == 1000.0
    assert pod1_delta["listener.0.0.0.0_443.ssl_handshake"] == 50.0
    assert pod1_delta["server.memory_allocated"] == -50.0
    assert "cluster.reset_counter.upstream_rq_total" not in pod1_delta
    assert "cluster.new_cluster.upstream_rq_total" not in pod1_delta


def test_stages_from_stats_attempted_rps_fallback():
    line = json.dumps(
        {
            "timestamp": "2026-11-01T20:00:00.000Z",
            "metric": "nighthawk_adjusting_001",
            "measurements": {
                "metric_nighthawk.builtin_attempted_rps": 5000.0,
                "duration_s": 20.0,
            },
        }
    )
    stages, _, _ = rc.stages_from_stats([line])
    assert len(stages) == 1
    assert stages[0].total_rps == 5000.0


def test_by_stage_envoy_and_ext_proc_cpu_normalization():
    t0 = datetime(2026, 11, 1, 20, 0, 0, tzinfo=timezone.utc)
    stage = rc.Stage(
        name="adjusting_001",
        start=t0,
        end=datetime(2026, 11, 1, 20, 0, 20, tzinfo=timezone.utc),
        total_rps=1000.0,
        failed_thresholds=[],
    )
    s_envoy = rc.Sample(
        timestamp=datetime(2026, 11, 1, 20, 0, 10, tzinfo=timezone.utc),
        window_s=2.0,
        component="egress",
        pod="atenet-egress-xyz",
        container="envoy",
        source="cadvisor",
        values={"cpu_cores": 1.6},
    )
    s_ext_proc = rc.Sample(
        timestamp=datetime(2026, 11, 1, 20, 0, 10, tzinfo=timezone.utc),
        window_s=2.0,
        component="egress",
        pod="atenet-egress-xyz",
        container="ext-proc",
        source="cadvisor",
        values={"cpu_cores": 2.0},
    )
    s_sdsmint = rc.Sample(
        timestamp=datetime(2026, 11, 1, 20, 0, 10, tzinfo=timezone.utc),
        window_s=2.0,
        component="egress",
        pod="atenet-egress-xyz",
        container="sdsmint",
        source="cadvisor",
        values={"cpu_cores": 0.1},
    )

    result = rc.by_stage(
        [s_envoy, s_ext_proc, s_sdsmint],
        [stage],
        envoy_cpu=2.0,
        ext_proc_cpu=4.0,
    )
    containers = result[0]["containers"]
    envoy_entry = containers["egress/envoy@atenet-egress-xyz"]
    ext_proc_entry = containers["egress/ext-proc@atenet-egress-xyz"]
    sdsmint_entry = containers["egress/sdsmint@atenet-egress-xyz"]

    assert abs(envoy_entry["cpu_fraction_of_pin"]["mean"] - 0.8) < 1e-6
    assert abs(ext_proc_entry["cpu_fraction_of_pin"]["mean"] - 0.5) < 1e-6
    assert sdsmint_entry["cpu_fraction_of_pin"] is None


def test_parse_cpu_cores():
    assert resource_sampler.parse_cpu_cores("4") == 4.0
    assert resource_sampler.parse_cpu_cores("2000m") == 2.0
    assert resource_sampler.parse_cpu_cores("500m") == 0.5
    assert resource_sampler.parse_cpu_cores("1.5") == 1.5
    assert resource_sampler.parse_cpu_cores("") is None
    assert resource_sampler.parse_cpu_cores(None) is None


def test_by_stage_worker_and_server_limit_cores():
    t0 = datetime(2026, 11, 1, 20, 0, 0, tzinfo=timezone.utc)
    stage = rc.Stage(
        name="nighthawk_testing",
        start=t0,
        end=datetime(2026, 11, 1, 20, 0, 20, tzinfo=timezone.utc),
        total_rps=5000.0,
        failed_thresholds=[],
    )
    s_worker = rc.Sample(
        timestamp=datetime(2026, 11, 1, 20, 0, 10, tzinfo=timezone.utc),
        window_s=2.0,
        component="workers",
        pod="worker-pod-1",
        container="worker",
        source="cadvisor",
        values={"cpu_cores": 1.5, "limit_cores": 6.0},
    )
    s_server = rc.Sample(
        timestamp=datetime(2026, 11, 1, 20, 0, 10, tzinfo=timezone.utc),
        window_s=2.0,
        component="server",
        pod="server-pod-1",
        container="nighthawk-server",
        source="cadvisor",
        values={"cpu_cores": 1.2, "limit_cores": 8.0},
    )

    result = rc.by_stage([s_worker, s_server], [stage])
    containers = result[0]["containers"]
    worker_entry = containers["workers/worker@worker-pod-1"]
    server_entry = containers["server/nighthawk-server@server-pod-1"]

    assert abs(worker_entry["cpu_fraction_of_pin"]["mean"] - 0.25) < 1e-6
    assert abs(server_entry["cpu_fraction_of_pin"]["mean"] - 0.15) < 1e-6
