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

"""Scrapes atenet-egress Envoy :15090 and ext-proc :9090 stats at stage boundaries."""

import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import kubeapi
import resources_common as rc

ENVOY_PORT = 15090
EXT_PROC_PORT = 9090
NAMESPACE = "ate-system"
SELECTOR = "app=atenet-egress"

GAUGE_METRICS = frozenset(
    {"server.memory_allocated", "ext_proc.process_resident_memory_bytes"}
)


def parse_prometheus_stats(text: str) -> dict[str, float]:
    """Parse Envoy Prometheus metrics from :15090/stats/prometheus."""
    out = {}
    for name, labels, val, _ in rc.parse_prometheus_text(text):
        if name == "envoy_http_downstream_rq_2xx":
            prefix = labels.get("envoy_http_conn_manager_prefix", "egress_connect")
            out[f"http.{prefix}.downstream_rq_2xx"] = val
        elif name == "envoy_listener_ssl_handshake":
            addr = labels.get("envoy_listener_address", "unknown")
            out[f"listener.{addr}.ssl_handshake"] = val
        elif name == "envoy_cluster_upstream_rq_total":
            cluster = labels.get("envoy_cluster_name", "")
            if cluster:
                out[f"cluster.{cluster}.upstream_rq_total"] = val
        elif name == "envoy_cluster_upstream_cx_overflow":
            cluster = labels.get("envoy_cluster_name", "")
            if cluster:
                out[f"cluster.{cluster}.upstream_cx_overflow"] = val
        elif name == "envoy_server_memory_allocated":
            out["server.memory_allocated"] = val
    return out


def parse_ext_proc_metrics(text: str) -> dict[str, float]:
    """Parse ext-proc Go process metrics from :9090/metrics."""
    out = {}
    for name, _, val, _ in rc.parse_prometheus_text(text):
        if name == "process_cpu_seconds_total":
            out["ext_proc.process_cpu_seconds_total"] = val
        elif name == "process_resident_memory_bytes":
            out["ext_proc.process_resident_memory_bytes"] = val
    return out


def egress_pod_ips(client: kubeapi.KubeClient) -> list[tuple[str, str]]:
    """Return [(pod_name, ip), ...] for atenet-egress pods."""
    pods = client.list_pods(NAMESPACE, SELECTOR)
    res = []
    for p in pods:
        name = p.get("metadata", {}).get("name", "")
        ip = p.get("status", {}).get("podIP", "")
        if name and ip:
            res.append((name, ip))
    return sorted(res)


def http_get(url: str, timeout_s: float = 10.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as r:
        return r.read().decode()


def scrape_boundary(
    client: kubeapi.KubeClient,
    stage: str,
    label: str = "end",
    log=print,
) -> dict:
    """Scrape all atenet-egress pods and aggregate their boundary counters."""
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "stage": stage,
        "label": label,
        "timestamp": now,
        "pods": {},
    }
    pod_ips = egress_pod_ips(client)
    for pod_name, ip in pod_ips:
        host = f"[{ip}]" if ":" in ip else ip
        pod_stats = {}
        # Envoy :15090
        try:
            envoy_text = http_get(f"http://{host}:{ENVOY_PORT}/stats/prometheus")
            pod_stats.update(parse_prometheus_stats(envoy_text))
        except Exception as e:
            log(f"scrape envoy stats on {pod_name} ({ip}): {e}")

        # ext-proc :9090
        try:
            ext_proc_text = http_get(f"http://{host}:{EXT_PROC_PORT}/metrics")
            pod_stats.update(parse_ext_proc_metrics(ext_proc_text))
        except Exception as e:
            log(f"scrape ext-proc metrics on {pod_name} ({ip}): {e}")

        record["pods"][pod_name] = pod_stats
    return record


def compute_stage_deltas(records: list[dict]) -> list[dict]:
    """Compute per-stage counter deltas between start and end boundary scrapes."""
    by_stage: dict[str, dict[str, dict]] = {}
    for r in records:
        if not r or "stage" not in r:
            continue
        by_stage.setdefault(r["stage"], {})[r.get("label", "")] = r

    deltas = []
    for stage, labels in by_stage.items():
        start_rec = labels.get("start")
        end_rec = labels.get("end")
        if not start_rec or not end_rec:
            continue
        stage_delta = {"stage": stage, "pods": {}}
        for pod, end_stats in end_rec.get("pods", {}).items():
            start_stats = start_rec.get("pods", {}).get(pod, {})
            pod_delta = {}
            for k, ev in end_stats.items():
                if k not in start_stats:
                    continue
                sv = start_stats[k]
                delta = ev - sv
                if k not in GAUGE_METRICS and delta < 0:
                    continue
                pod_delta[k] = delta
            stage_delta["pods"][pod] = pod_delta
        deltas.append(stage_delta)
    return deltas


def write_envoy_stats_by_stage(records: list[dict], path: Path) -> None:
    data = {
        "snapshots": records,
        "per_stage_deltas": compute_stage_deltas(records),
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
