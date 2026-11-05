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

"""Turn an atunnel-bench run into the benchmark pipeline's rows.

results.json is a list of scenario reports (internal/report). Every row here
is flat, named uniquely, and carries the run's rig and configuration, so a
reader that keeps the last row per metric name loses nothing.
"""

import json
import re
from pathlib import Path
from typing import Any

RIG = "pod"

# RIG-LIMITED means the generator or the sink set the pace (internal/report):
# atunnel's figures there are a floor, so the rung counts neither as OK nor
# as atunnel falling short.
RIG_LIMITED = "RIG-LIMITED"


def flatten(prefix: str, obj: Any) -> dict[str, Any]:
    """Flatten nested dicts into prefix_key names. Lists and nulls are dropped:
    they hold samples and file names, not measurements."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            out.update(flatten(f"{prefix}_{k}" if prefix else k, v))
        return out
    if isinstance(obj, list) or obj is None:
        return {}
    return {prefix: obj}


def rung_rows(report: dict, config: dict[str, Any]) -> list[dict]:
    """One row per rung: atunnel_<Scenario>_<offered>."""
    rows = []
    scenario = report["scenario"]
    seen: dict[str, int] = {}
    for i, rung in enumerate(report.get("rungs", [])):
        offered = rung.get("load", {}).get("offered", 0)
        # Two rungs can round to the same rate; the second gets a suffix so
        # a reader that keeps the last row per metric sees both.
        metric = f"atunnel_{scenario}_{int(offered)}"
        if metric in seen:
            seen[metric] += 1
            metric = f"{metric}_{seen[metric]}"
        else:
            seen[metric] = 0
        measurements = {
            "rig": RIG,
            "scenario": scenario,
            "rung": i,
            "verdict": rung.get("verdict", ""),
            "reason": rung.get("reason", ""),
            "unit": report.get("unit", ""),
            "step_ns": report.get("step_ns", 0),
            "warmup_ns": report.get("warmup_ns", 0),
            **config,
        }
        for key in ("load", "tunnel", "generator", "sink"):
            # A DOWN rung has no tunnel to measure; its zeros would read as
            # measurements.
            if key == "tunnel" and rung.get("verdict") == "DOWN":
                continue
            measurements.update(flatten(key, rung.get(key, {})))
        rows.append({"metric": metric, "measurements": measurements})
    return rows


def summary_row(report: dict) -> dict:
    """One row per scenario: the highest offered rate atunnel kept up with,
    the lowest it did not, and how often the rig or a death got in the way."""
    rungs = report.get("rungs", [])
    measurements: dict[str, Any] = {
        "rig": RIG,
        "rungs": len(rungs),
        "down_rungs": sum(1 for r in rungs if r.get("verdict") == "DOWN"),
        "rig_limited_rungs": sum(1 for r in rungs if r.get("verdict") == RIG_LIMITED),
    }
    offered = lambda r: r["load"]["offered"]  # noqa: E731
    ok = [r for r in rungs if r.get("verdict") == "OK"]
    if ok:
        max_ok = max(ok, key=offered)
        measurements["max_ok_offered"] = offered(max_ok)
        us = max_ok.get("tunnel", {}).get("cpu_us_per_unit")
        if us is not None:
            measurements["tunnel_cpu_us_per_unit_at_max_ok"] = us
    not_ok = [r for r in rungs if r.get("verdict") not in ("OK", RIG_LIMITED)]
    if not_ok:
        first = min(not_ok, key=offered)
        measurements["first_not_ok_offered"] = offered(first)
        measurements["first_not_ok_verdict"] = first.get("verdict", "")
    return {"metric": f"atunnel_{report['scenario']}", "measurements": measurements}


def rows(reports: list[dict], config: dict[str, Any]) -> list[dict]:
    """Every row of a run: per rung, per scenario, and the sink's counters per
    scenario (their keys are the sink's own, so they get a row of their own)."""
    out = []
    for report in reports:
        out.extend(rung_rows(report, config))
        out.append(summary_row(report))
        counters = report.get("sink_counters")
        if counters:
            out.append(
                {
                    "metric": f"atunnel_{report['scenario']}_sink_counters",
                    "measurements": {"rig": RIG, "scenario": report["scenario"], **flatten("", counters)},
                }
            )
    return out


def stats_records(rows: list[dict], tag: str, test_name: str, timestamp: str) -> list[dict]:
    """The pipeline's five-key stats.jsonl records. results.json has no time
    per rung, so every record carries the run's start."""
    return [
        {
            "timestamp": timestamp,
            "tag": tag,
            "test_name": test_name,
            "metric": row["metric"],
            "measurements": row["measurements"],
        }
        for row in rows
    ]


def death_counts(path: Path) -> tuple[int, int]:
    """Deaths and OOM kills from the worker's tunnel-deaths.log, one line per
    atunnel exit ending in 'OOM kills: N'. (0, 0) without the file."""
    if not path.exists():
        return 0, 0
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    ooms = sum(1 for line in lines if re.search(r"OOM kills: [1-9]", line))
    return len(lines), ooms


def death_line(deaths: int, oom_kills: int, tunnel_memory: str) -> str:
    """The summary line run-docker.sh appends to results.txt."""
    return f"atunnel died {deaths} time(s), {oom_kills} of them OOM-killed at its {tunnel_memory} limit"


def status(
    reports: list[dict],
    load_exit_code: int,
    deaths: int,
    oom_kills: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    """status.json: whether load ran, what it saw, and the rig's shape.
    Verdicts, deaths and OOM kills are data, not failures. Without a usable
    results.json the counts are zero and the runner's exit code says so."""
    verdicts: dict[str, int] = {}
    rungs = 0
    for report in reports:
        for rung in report.get("rungs", []):
            rungs += 1
            v = rung.get("verdict", "")
            verdicts[v] = verdicts.get(v, 0) + 1
    return {
        "rig": RIG,
        "load_exit_code": load_exit_code,
        "scenarios": len(reports),
        "rungs": rungs,
        "verdicts": verdicts,
        "tunnel_deaths": deaths,
        "oom_kills": oom_kills,
        **config,
    }


def write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
