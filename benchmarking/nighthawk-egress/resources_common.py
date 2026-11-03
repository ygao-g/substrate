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

"""Common structures and alignment math for resource samples and stages."""

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

# Components whose containers run under pinned requests=limits.
PINNED_COMPONENTS = frozenset({"egress"})

LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+"
    r"(?P<value>\S+)(?:\s+(?P<ts>-?\d+))?\s*$"
)
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prometheus_text(
    text: str,
) -> Iterable[tuple[str, dict[str, str], float, float | None]]:
    """Parse Prometheus text-format lines into (name, labels, value, timestamp)."""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = LINE.match(line)
        if not m:
            continue
        name = m.group("name")
        labels = dict(LABEL.findall(m.group("labels") or ""))
        try:
            val = float(m.group("value"))
        except ValueError:
            continue
        ts = int(m.group("ts")) / 1000.0 if m.group("ts") else None
        yield name, labels, val, ts


def parse_time(s: str) -> datetime:
    dt = datetime.fromisoformat(s.strip())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class Stage:
    name: str
    start: datetime
    end: datetime
    total_rps: float | None
    failed_thresholds: list[str]


def stages_from_stats(
    lines: Iterable[str], log: Callable[[str], None] | None = None
) -> tuple[list[Stage], str, str]:
    stages: list[Stage] = []
    tag = test_name = ""
    skipped = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if not rec.get("timestamp"):
            skipped += 1
            continue
        m = rec.get("measurements", {})
        start = parse_time(rec["timestamp"])
        if m.get("end_time"):
            end = parse_time(m["end_time"])
        else:
            end = datetime.fromtimestamp(
                start.timestamp() + float(m.get("duration_s", 0)), timezone.utc
            )
        rps = None
        if m.get("per_worker_rps") is not None and m.get("concurrency"):
            rps = float(m["per_worker_rps"]) * float(m["concurrency"])
        elif m.get("metric_nighthawk.builtin_attempted_rps") is not None:
            rps = float(m["metric_nighthawk.builtin_attempted_rps"])
        stages.append(
            Stage(
                name=rec["metric"],
                start=start,
                end=end,
                total_rps=rps,
                failed_thresholds=list(m.get("failed_thresholds") or []),
            )
        )
        tag = tag or rec.get("tag", "")
        test_name = test_name or rec.get("test_name", "")
    stages.sort(key=lambda s: s.start)
    if skipped and log:
        log(f"stats.jsonl: skipped {skipped} record(s) without a timestamp")
    return stages, tag, test_name


@dataclass
class Sample:
    timestamp: datetime
    window_s: float
    component: str
    pod: str
    container: str
    source: str
    values: dict[str, float | None] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.component}/{self.container}@{self.pod}"


def overlapping_stages(sample: Sample, stages: list[Stage]) -> list[Stage]:
    lo = sample.timestamp.timestamp() - sample.window_s
    hi = sample.timestamp.timestamp()
    return [
        s for s in stages if lo < s.end.timestamp() and hi > s.start.timestamp()
    ]


def percentile(values: list[float], q: float) -> float:
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * q / 100.0
    lo = math.floor(pos)
    hi = math.ceil(pos)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def summarize(values: list[float]) -> dict | None:
    if not values:
        return None
    return {
        "mean": sum(values) / len(values),
        "max": max(values),
        "p95": percentile(values, 95),
        "n": len(values),
    }


def by_stage(
    samples: list[Sample],
    stages: list[Stage],
    envoy_cpu: float | None = None,
    series: Iterable[str] = ("cpu_cores", "working_set_bytes"),
    ext_proc_cpu: float | None = None,
) -> list[dict]:
    series = list(series)
    out = []
    for st in stages:
        per_key: dict[str, list[Sample]] = {}
        for s in samples:
            if st in overlapping_stages(s, [st]):
                per_key.setdefault(s.key, []).append(s)
        containers = {}
        for key in sorted(per_key):
            group = per_key[key]
            first = group[0]
            entry: dict = {
                "component": first.component,
                "pod": first.pod,
                "container": first.container,
                "source": first.source,
                "window_s": first.window_s,
            }
            for i, name in enumerate(series):
                vals = [
                    g.values[name] for g in group if g.values.get(name) is not None
                ]
                if vals or i < 2:
                    entry[name] = summarize(vals)
            cpu = entry.get("cpu_cores")
            pin_cpu = first.values.get("limit_cores")
            if pin_cpu is None and first.component in PINNED_COMPONENTS:
                if first.container == "envoy":
                    pin_cpu = envoy_cpu
                elif first.container == "ext-proc":
                    pin_cpu = ext_proc_cpu

            if cpu and pin_cpu:
                entry["cpu_fraction_of_pin"] = {
                    k: (v / pin_cpu if k != "n" else v) for k, v in cpu.items()
                }
            else:
                entry["cpu_fraction_of_pin"] = None
            containers[key] = entry
        out.append(
            {
                "stage": st.name,
                "start": format_time(st.start),
                "end": format_time(st.end),
                "total_rps": st.total_rps,
                "failed_thresholds": st.failed_thresholds,
                "containers": containers,
            }
        )
    return out


def write_jsonl(records: Iterable[dict], path: Path) -> None:
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
