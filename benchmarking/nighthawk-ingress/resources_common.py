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

"""Stage windows, sample records, and per-stage rollups for the resource
tools (fetch_run_resources.py).

A sample is one reading of one container at one time. It covers the window
(timestamp - window_s, timestamp]: for a Cloud Monitoring rate point that is
the rate window, 60 s or 120 s. A sample counts toward every stage whose
[start, end] that window overlaps.
"""

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

# Components whose containers run under the envoyCpu requests=limits pin.
PINNED_COMPONENTS = frozenset({"router"})


def parse_time(s: str) -> datetime:
    """RFC3339 with Z or offset, any fractional precision."""
    s = s.strip().replace("Z", "+00:00")
    if "." in s:
        head, _, rest = s.partition(".")
        frac = ""
        tz = ""
        for i, ch in enumerate(rest):
            if not ch.isdigit():
                tz = rest[i:]
                break
            frac += ch
        s = f"{head}.{(frac + '000000')[:6]}{tz}"
    dt = datetime.fromisoformat(s)
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
    """Parse stats.jsonl into stages plus the run's (tag, test_name).

    timestamp is the stage start; measurements.end_time the end. Total RPS
    is per_worker_rps * concurrency, the rate Nighthawk was asked to send.
    Records without a timestamp are skipped and counted to log.
    """
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
    """One reading of one container. Values may be None: not reported."""

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


def sample_records(
    samples: Iterable[Sample], stages: list[Stage], tag: str, test_name: str
) -> list[dict]:
    """stats.jsonl envelope, one record per sample per container."""
    out = []
    for s in sorted(samples, key=lambda x: (x.timestamp, x.key)):
        out.append(
            {
                "timestamp": format_time(s.timestamp),
                "tag": tag,
                "test_name": test_name,
                "metric": f"resources/{s.component}/{s.container}",
                "measurements": {
                    "component": s.component,
                    "pod": s.pod,
                    "container": s.container,
                    "source": s.source,
                    "window_s": s.window_s,
                    "stages": [st.name for st in overlapping_stages(s, stages)],
                    **s.values,
                },
            }
        )
    return out


def percentile(values: list[float], q: float) -> float:
    """Linear interpolation between closest ranks, q in [0, 100]."""
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
    envoy_cpu: float | None,
    series: Iterable[str] = ("cpu_cores", "working_set_bytes"),
) -> list[dict]:
    """Per stage per container: mean/max/p95 of each series.

    cpu_fraction_of_pin is cpu_cores / envoy_cpu for pinned components and
    None elsewhere. The first two series are None when no overlapping
    sample carries them; later series are omitted instead.
    """
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
                # The first two series always appear, null if absent.
                if vals or i < 2:
                    entry[name] = summarize(vals)
            cpu = entry.get("cpu_cores")
            if cpu and envoy_cpu and first.component in PINNED_COMPONENTS:
                entry["cpu_fraction_of_pin"] = {
                    k: (v / envoy_cpu if k != "n" else v) for k, v in cpu.items()
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


def missing_containers(
    samples: Iterable[Sample], expected: Iterable[tuple[str, str]]
) -> list[str]:
    """Expected component/container pairs that produced no sample at all."""
    seen = {(s.component, s.container) for s in samples}
    return [f"{c}/{k}" for c, k in expected if (c, k) not in seen]


def write_jsonl(records: Iterable[dict], path: Path) -> None:
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
