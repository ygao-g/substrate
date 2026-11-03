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

"""In-run cAdvisor sampler: CPU and memory of egress benchmark pods every N seconds."""

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import kubeapi
import resources_common as rc

KEEP = frozenset(
    {
        "container_cpu_usage_seconds_total",
        "container_memory_working_set_bytes",
        "machine_cpu_cores",
    }
)


def parse_cpu_cores(val: str | None) -> float | None:
    if not val:
        return None
    val = str(val).strip()
    if val.endswith("m"):
        try:
            return float(val[:-1]) / 1000.0
        except ValueError:
            return None
    try:
        return float(val)
    except ValueError:
        return None


@dataclass(frozen=True)
class Target:
    component: str
    namespace: str
    selector: str
    containers: tuple[str, ...]
    pod: str = ""


DEFAULT_TARGETS = (
    Target("egress", "ate-system", "app=atenet-egress", ("envoy", "ext-proc", "sdsmint")),
    Target("atelet", "ate-system", "app=atelet", ("atelet", "objectstore-plugin")),
    Target("runner", "benchmarking", "app=substrate-benchmark-runner", ("runner",)),
    Target("workers", "benchmark-workloads", "ate.dev/worker-pool", ("worker",)),
    Target("server", "nighthawk", "app=nighthawk-server", ("nighthawk-server",)),
)


def parse_cadvisor(
    text: str,
) -> Iterable[tuple[str, dict[str, str], float, float | None]]:
    for name, labels, val, ts in rc.parse_prometheus_text(text):
        if name in KEEP:
            yield name, labels, val, ts


@dataclass(frozen=True)
class _Key:
    component: str
    pod: str
    container: str
    metric: str


class Sampler:
    def __init__(
        self,
        client: kubeapi.KubeClient,
        targets: tuple[Target, ...] = DEFAULT_TARGETS,
        interval_s: float = 2.0,
        log: Callable[[str], None] = print,
    ):
        self.client = client
        self.targets = targets
        self.interval_s = interval_s
        self.log = log
        self._samples: list[rc.Sample] = []
        self._prev: dict[_Key, tuple[float, float]] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._container_limits: dict[tuple[str, str], float] = {}
        self._node_cores: dict[str, float] = {}

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> list[rc.Sample]:
        self._stop.set()
        if self._thread:
            self._thread.join()
        return list(self._samples)

    def _run(self) -> None:
        while not self._stop.is_set():
            start = time.time()
            try:
                self.poll_once()
            except Exception as e:
                self.log(f"sampler poll error: {e}")
            elapsed = time.time() - start
            time.sleep(max(0.1, self.interval_s - elapsed))

    def poll_once(self) -> None:
        nodes = set()
        pods_by_node: dict[str, list[tuple[Target, dict]]] = {}
        for target in self.targets:
            for item in self.client.list_pods(target.namespace, target.selector):
                pod_name = item.get("metadata", {}).get("name", "")
                if target.pod and pod_name != target.pod:
                    continue
                node = item.get("spec", {}).get("nodeName", "")
                if node:
                    nodes.add(node)
                    pods_by_node.setdefault(node, []).append((target, item))
                # Store container CPU limits from pod spec if specified
                for c in item.get("spec", {}).get("containers", []):
                    c_name = c.get("name", "")
                    cpu_raw = c.get("resources", {}).get("limits", {}).get("cpu")
                    limit = parse_cpu_cores(cpu_raw)
                    if limit is not None:
                        self._container_limits[(pod_name, c_name)] = limit

        for node in sorted(nodes):
            try:
                text = self.client.get_raw(f"/api/v1/nodes/{node}/proxy/metrics/cadvisor")
            except Exception as e:
                self.log(f"cadvisor {node}: {e}")
                continue
            self._process_cadvisor_text(node, text, pods_by_node.get(node, []))

    def _process_cadvisor_text(
        self, node: str, text: str, targets: list[tuple[Target, dict]]
    ) -> None:
        pod_targets = {
            item.get("metadata", {}).get("name", ""): (t, item)
            for t, item in targets
        }
        now = datetime.now(timezone.utc)
        current_rates: dict[tuple[str, str, str], dict[str, float]] = {}

        for metric, labels, val, ts in parse_cadvisor(text):
            if metric == "machine_cpu_cores":
                self._node_cores[node] = val
                continue

            pod = labels.get("pod", "")
            if pod not in pod_targets:
                continue
            target, item = pod_targets[pod]
            container = labels.get("container", "")
            if not container:
                continue
            if target.containers and container not in target.containers:
                continue

            key = _Key(target.component, pod, container, metric)
            sample_time = ts if ts else now.timestamp()

            if metric == "container_cpu_usage_seconds_total":
                if key in self._prev:
                    prev_val, prev_time = self._prev[key]
                    dt = sample_time - prev_time
                    if dt > 0:
                        cores = max(0.0, (val - prev_val) / dt)
                        current_rates.setdefault((target.component, pod, container), {})["cpu_cores"] = cores
                self._prev[key] = (val, sample_time)

            elif metric == "container_memory_working_set_bytes":
                current_rates.setdefault((target.component, pod, container), {})["working_set_bytes"] = val

        for (comp, pod, container), values in current_rates.items():
            limit = self._container_limits.get((pod, container))
            values["limit_cores"] = limit
            self._samples.append(
                rc.Sample(
                    timestamp=now,
                    window_s=self.interval_s,
                    component=comp,
                    pod=pod,
                    container=container,
                    source="cadvisor",
                    values=values,
                )
            )


def write_resources_by_stage(
    samples: list[rc.Sample],
    stages: list[rc.Stage],
    envoy_cpu: float | None,
    path: Path,
    ext_proc_cpu: float | None = None,
) -> None:
    data = rc.by_stage(samples, stages, envoy_cpu, ext_proc_cpu=ext_proc_cpu)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def write_raw_samples(samples: list[rc.Sample], path: Path) -> None:
    records = (
        {
            "timestamp": rc.format_time(s.timestamp),
            "window_s": s.window_s,
            "component": s.component,
            "pod": s.pod,
            "container": s.container,
            "source": s.source,
            "values": s.values,
        }
        for s in samples
    )
    rc.write_jsonl(records, path)
