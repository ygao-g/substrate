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

"""In-run cAdvisor sampler: CPU and memory of selected pods every N seconds.

Polls each node's kubelet cAdvisor endpoint through the API server proxy
(/api/v1/nodes/<node>/proxy/metrics/cadvisor), keeps the series that
benchmarking/monitoring.yaml keeps, and turns counters into per-second
rates between consecutive polls. runner.py starts it behind
--sample-resources; it also runs standalone:

  python3 resource_sampler.py --duration 60 --interval 2 --out /tmp/rs \\
    [--stats stats.jsonl] [--envoy-cpu 2] [--context <kubecontext>]

Output: resources.jsonl, and resources-by-stage.json when stage windows
are known.
"""

import argparse
import json
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import kubeapi
import resources_common as rc

# Keep regex from benchmarking/monitoring.yaml's cadvisor job.
KEEP = re.compile(
    r"container_((.*pressure.*)|network_(receive|transmit)_bytes_total"
    r"|fs_(reads|writes)_bytes_total|memory_rss|memory_working_set_bytes"
    r"|cpu_usage_seconds_total|cpu_cfs_.*|spec_memory_limit_bytes)"
)
# monitoring.yaml drops these at container="": the pod-level cgroup is the
# sum of its containers, and keeping both double counts.
ROLLUP_DROP = frozenset(
    {"container_cpu_usage_seconds_total", "container_memory_working_set_bytes"}
)
# Pod-cgroup rows (container="" and name="") are reported as this container.
POD_CONTAINER = "POD"

LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+"
    r"(?P<value>\S+)(?:\s+(?P<ts>-?\d+))?\s*$"
)
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


@dataclass(frozen=True)
class Target:
    component: str
    namespace: str
    selector: str
    containers: tuple[str, ...]  # empty: every container
    pod: str = ""  # set: only this pod


DEFAULT_TARGETS = (
    Target("router", "ate-system", "app=atenet-router", ("envoy", "atenet-router")),
    Target("runner", "benchmarking", "app=substrate-benchmark-runner", ("runner",)),
    Target("workers", "benchmark-workloads", "ate.dev/worker-pool", ("ateom",)),
)


def default_targets(own_pod: str = "") -> tuple[Target, ...]:
    """DEFAULT_TARGETS, with the runner narrowed to own_pod when given.

    Inside the runner Job, HOSTNAME is the pod name; without it every
    running runner pod in the namespace matches.
    """
    return tuple(
        Target(t.component, t.namespace, t.selector, t.containers, own_pod)
        if t.component == "runner" and own_pod
        else t
        for t in DEFAULT_TARGETS
    )


def parse_target(spec: str) -> Target:
    """component=namespace:selector[:c1,c2]"""
    comp, _, rest = spec.partition("=")
    ns, _, rest = rest.partition(":")
    selector, _, containers = rest.partition(":")
    if not comp or not ns:
        raise ValueError(f"bad --target {spec!r}; want component=namespace:selector[:c1,c2]")
    return Target(comp, ns, selector, tuple(c for c in containers.split(",") if c))


def parse_cadvisor(text: str) -> Iterable[tuple[str, dict[str, str], float, float | None]]:
    """Yield (name, labels, value, timestamp_s or None) for kept series."""
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = LINE.match(line)
        if not m or not KEEP.fullmatch(m.group("name")):
            continue
        labels = dict(LABEL.findall(m.group("labels") or ""))
        try:
            value = float(m.group("value"))
        except ValueError:
            continue
        ts = int(m.group("ts")) / 1000.0 if m.group("ts") else None
        yield m.group("name"), labels, value, ts


def container_of(name: str, labels: dict[str, str]) -> str | None:
    """Container name, POD for pod-level rows, None to drop the row.

    kubelet reports pod network counters on the pause container's row
    (container="", name set); the pod cgroup row (container="", name="")
    carries PSI, rss and limits. Both map to POD; per-interface dedup in
    Sampler.ingest keeps a counter reported on both from counting twice.
    """
    c = labels.get("container", "")
    if c:
        return c
    if name.startswith("container_network_"):
        return POD_CONTAINER
    if labels.get("name"):
        return None  # pause container's own CPU and memory
    if name in ROLLUP_DROP:
        return None
    return POD_CONTAINER


def value_name(metric: str) -> str:
    base = metric[len("container_"):]
    return base[: -len("_total")] + "_rate" if base.endswith("_total") else base


@dataclass
class Reading:
    value: float
    ts: float


class RateTracker:
    """Per-series previous counter reading; yields rates on the next poll."""

    def __init__(self):
        self.prev: dict[tuple, Reading] = {}

    def rate(self, key: tuple, value: float, ts: float) -> tuple[float, float] | None:
        """(rate per second, dt) or None on first sight, reset, or stale ts."""
        prev = self.prev.get(key)
        self.prev[key] = Reading(value, ts)
        if prev is None or ts <= prev.ts or value < prev.value:
            return None
        dt = ts - prev.ts
        return (value - prev.value) / dt, dt


class PollFailed(RuntimeError):
    """Every node in one poll failed; the per-node errors are already counted."""


class Sampler:
    """Polls cAdvisor on a background thread; samples accumulate in `samples`.

    Read `samples` through snapshot() while the thread may still run. The
    thread stops on its own after max_failed_polls consecutive failed polls,
    so a missing RBAC grant costs a few log lines, not one per interval.
    """

    def __init__(
        self,
        client: kubeapi.KubeClient,
        targets: Iterable[Target] = DEFAULT_TARGETS,
        interval_s: float = 2.0,
        clock: Callable[[], float] = time.time,
        log: Callable[[str], None] = print,
        max_failed_polls: int = 5,
    ):
        self.client = client
        self.targets = list(targets)
        self.interval_s = interval_s
        self.clock = clock
        self.log = log
        self.max_failed_polls = max_failed_polls
        self.samples: list[rc.Sample] = []
        self.errors = 0
        self._lock = threading.Lock()
        self.rates = RateTracker()
        self.gauge_ts: dict[tuple, float] = {}  # last ts, or value if unstamped
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def resolve(self) -> dict[tuple[str, str], tuple[Target, str]]:
        """(namespace, pod) -> (target, node) for running target pods."""
        out = {}
        for t in self.targets:
            for pod in self.client.list_pods(t.namespace, t.selector):
                node = pod.get("spec", {}).get("nodeName")
                if t.pod and pod["metadata"]["name"] != t.pod:
                    continue
                if node and pod.get("status", {}).get("phase") == "Running":
                    out[(t.namespace, pod["metadata"]["name"])] = (t, node)
        return out

    def poll_once(self) -> int:
        """One pass over every node hosting a target pod. Returns samples added.

        Raises PollFailed when every node failed, so the loop can count
        consecutive failures without logging each node each time.
        """
        pods = self.resolve()
        nodes = sorted({n for _, n in pods.values()})
        added = 0
        failures: list[str] = []
        for node in nodes:
            if self._stop.is_set():
                break
            try:
                text = self.client.get_raw(f"/api/v1/nodes/{node}/proxy/metrics/cadvisor")
            except Exception as e:
                self.errors += 1
                failures.append(f"{node}: {e}")
                continue
            added += self.ingest(text, pods, self.clock())
        if nodes and len(failures) == len(nodes):
            raise PollFailed(f"all {len(nodes)} node(s) failed, last: {failures[-1]}")
        for f in failures:
            self.log(f"resource_sampler: {f}")
        return added

    def ingest(self, text: str, pods: dict, now: float) -> int:
        values: dict[tuple, dict[str, float]] = {}
        windows: dict[tuple, dict[str, tuple[float, float]]] = {}
        network: dict[tuple, dict[tuple[str, str], float]] = {}
        comps: dict[tuple, str] = {}
        for name, labels, value, ts in parse_cadvisor(text):
            hit = pods.get((labels.get("namespace", ""), labels.get("pod", "")))
            if hit is None:
                continue
            target, _ = hit
            container = container_of(name, labels)
            if container is None:
                continue
            if target.containers and container not in target.containers and container != POD_CONTAINER:
                continue
            stamped = ts is not None
            ts = ts or now
            key = (labels["namespace"], labels["pod"], container)
            comps[key] = target.component
            vname = value_name(name)
            if name.endswith("_total"):
                iface = labels.get("interface", "")
                r = self.rates.rate(key + (name, iface, labels.get("id", "")), value, ts)
                if r is None:
                    continue  # first sight, reset, or cAdvisor has not refreshed
                v, dt = r
                windows.setdefault(key, {})[vname] = (dt, ts)
                if iface:
                    network.setdefault(key, {})[(vname, iface)] = v
                    continue
            else:
                gkey = key + (name, labels.get("id", ""))
                # cAdvisor has not refreshed the series; spec_* rows carry no
                # timestamp, so for them an unchanged value means the same.
                seen = ts if stamped else value
                if gkey in self.gauge_ts and self.gauge_ts[gkey] == seen:
                    continue
                self.gauge_ts[gkey] = seen
                v = value
            values.setdefault(key, {})[vname] = v
        for key, per_iface in network.items():
            vals = values.setdefault(key, {})
            for (vname, _), v in per_iface.items():
                vals[vname] = vals.get(vname, 0.0) + v
        added = 0
        for key, vals in values.items():
            if not vals:
                continue
            win = windows.get(key, {})
            # The window is the CPU counter's delta when there is one.
            dt, ts = win.get("cpu_usage_seconds_rate") or (
                max(win.values()) if win else (self.interval_s, now)
            )
            if "cpu_usage_seconds_rate" in vals:
                vals["cpu_cores"] = vals["cpu_usage_seconds_rate"]
            if "memory_working_set_bytes" in vals:
                vals["working_set_bytes"] = vals["memory_working_set_bytes"]
            if "memory_rss" in vals:
                vals["rss_bytes"] = vals["memory_rss"]
            periods = vals.get("cpu_cfs_periods_rate")
            if periods:
                vals["cfs_throttled_ratio"] = vals.get("cpu_cfs_throttled_periods_rate", 0.0) / periods
            sample = rc.Sample(
                timestamp=datetime.fromtimestamp(ts, timezone.utc),
                window_s=dt,
                component=comps[key],
                pod=key[1],
                container=key[2],
                source="cadvisor-live",
                values=vals,
            )
            with self._lock:
                self.samples.append(sample)
            added += 1
        return added

    def snapshot(self) -> list[rc.Sample]:
        """A copy of the samples, safe while the thread is still appending."""
        with self._lock:
            return list(self.samples)

    def next_deadline(self, next_at: float) -> float:
        """One interval after the last deadline, or now if a poll ran long.

        Resyncing to the clock keeps a slow poll from being followed by a
        burst of back-to-back polls.
        """
        return max(next_at + self.interval_s, self.clock())

    def _run(self) -> None:
        next_at = self.clock()
        failed = 0
        while not self._stop.is_set():
            try:
                self.poll_once()
                failed = 0
            except Exception as e:
                failed += 1
                if not isinstance(e, PollFailed):
                    self.errors += 1
                if self._stop.is_set():
                    return
                if failed >= self.max_failed_polls:
                    self.log(f"resource_sampler: {failed} polls failed in a row, stopping: {e}")
                    return
                if failed == 1:
                    self.log(f"resource_sampler: poll failed: {e}")
            next_at = self.next_deadline(next_at)
            self._stop.wait(max(0.0, next_at - self.clock()))

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 30)

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()


def write_outputs(
    samples: list[rc.Sample],
    out_dir: Path,
    stats_path: Path | None,
    envoy_cpu: float | None,
    tag: str = "",
    test_name: str = "",
) -> list[Path]:
    """resources.jsonl always; resources-by-stage.json if stats.jsonl exists."""
    stages: list[rc.Stage] = []
    if stats_path is not None and stats_path.exists():
        stages, stag, sname = rc.stages_from_stats(stats_path.read_text().splitlines())
        tag, test_name = tag or stag, test_name or sname
    out_dir.mkdir(parents=True, exist_ok=True)
    res = out_dir / "resources.jsonl"
    rc.write_jsonl(rc.sample_records(samples, stages, tag, test_name), res)
    paths = [res]
    if stages:
        by = out_dir / "resources-by-stage.json"
        by.write_text(
            json.dumps(
                {
                    "tag": tag,
                    "test_name": test_name,
                    "envoy_cpu": envoy_cpu,
                    "source": "cadvisor-live",
                    "stages": rc.by_stage(
                        samples,
                        stages,
                        envoy_cpu,
                        ("cpu_cores", "working_set_bytes", "cfs_throttled_ratio", "rss_bytes"),
                    ),
                },
                indent=2,
            )
        )
        paths.append(by)
    return paths


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="resource_sampler",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--duration", type=float, default=60, help="Seconds to sample")
    p.add_argument("--interval", type=float, default=2, help="Seconds between polls")
    p.add_argument("--out", required=True, help="Output directory")
    p.add_argument("--stats", help="stats.jsonl to align samples to")
    p.add_argument("--envoy-cpu", type=float, help="Router CPU pin, for cpu_fraction_of_pin")
    p.add_argument("--context", help="kubectl context (default: in-cluster, else current)")
    p.add_argument(
        "--target",
        action="append",
        help="component=namespace:selector[:c1,c2]; repeatable, replaces the defaults",
    )
    args = p.parse_args(argv)
    targets = [parse_target(t) for t in args.target] if args.target else DEFAULT_TARGETS
    sampler = Sampler(kubeapi.default_client(args.context), targets, args.interval)
    sampler.start()
    time.sleep(args.duration)
    sampler.stop()
    paths = write_outputs(
        sampler.snapshot(), Path(args.out), Path(args.stats) if args.stats else None, args.envoy_cpu
    )
    print(f"{len(sampler.samples)} samples, {sampler.errors} errors -> {', '.join(map(str, paths))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
