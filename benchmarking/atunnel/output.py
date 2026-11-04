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

"""Converts atunnel-bench results.json into stats.jsonl and status.json."""

import json
import re
from pathlib import Path

_OOM_RE = re.compile(r"OOM kills: [1-9]")


def flatten(prefix: str, obj: dict) -> dict:
    """Flattens nested dicts with '_', keeping scalars and dropping lists."""
    out: dict = {}
    for k, v in obj.items():
        key = f"{prefix}_{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(key, v))
        elif isinstance(v, list) or v is None:
            continue
        else:
            out[key] = v
    return out


def rows(reports: list[dict], config: dict | None = None) -> list[dict]:
    """Builds per-rung, per-scenario summary, and sink_counters rows."""
    cfg = dict(config or {})
    out: list[dict] = []
    for report in reports:
        scenario = report.get("scenario", "")
        rungs = report.get("rungs", [])
        for idx, rung in enumerate(rungs):
            offered = int(rung.get("load", {}).get("offered", 0))
            verdict = rung.get("verdict", "")
            rung_to_flatten = {
                k: v
                for k, v in rung.items()
                if not (k == "tunnel" and verdict == "DOWN")
            }
            measurements = {
                "rig": "pod",
                "scenario": scenario,
                "rung": idx,
                "verdict": verdict,
                "reason": rung.get("reason", ""),
                "unit": report.get("unit", ""),
                "step_ns": report.get("step_ns", 0),
                "warmup_ns": report.get("warmup_ns", 0),
            }
            measurements.update(cfg)
            measurements.update(flatten("", rung_to_flatten))
            out.append(
                {
                    "metric": f"atunnel_{scenario}_{offered}",
                    "measurements": measurements,
                }
            )

        ok_rungs = [r for r in rungs if r.get("verdict") == "OK"]
        not_ok_rungs = [r for r in rungs if r.get("verdict") != "OK"]
        summary: dict = {
            "rig": "pod",
            "rungs": len(rungs),
            "down_rungs": sum(1 for r in rungs if r.get("verdict") == "DOWN"),
        }
        if ok_rungs:
            max_ok = max(
                ok_rungs, key=lambda r: r.get("load", {}).get("offered", 0.0)
            )
            summary["max_ok_offered"] = max_ok.get("load", {}).get(
                "offered", 0.0
            )
            summary["tunnel_cpu_us_per_unit_at_max_ok"] = max_ok.get(
                "tunnel", {}
            ).get("cpu_us_per_unit", 0.0)
        if not_ok_rungs:
            first_not_ok = not_ok_rungs[0]
            summary["first_not_ok_offered"] = first_not_ok.get("load", {}).get(
                "offered", 0.0
            )
            summary["first_not_ok_verdict"] = first_not_ok.get("verdict", "")
        else:
            summary["first_not_ok_verdict"] = ""
        out.append({"metric": f"atunnel_{scenario}", "measurements": summary})

        counters = report.get("sink_counters")
        if counters:
            out.append(
                {
                    "metric": f"atunnel_{scenario}_sink_counters",
                    "measurements": dict(counters),
                }
            )
    return out


def stats_records(
    row_list: list[dict], tag: str, test_name: str, timestamp: str
) -> list[dict]:
    """Formats rows as five-key stats.jsonl records."""
    return [
        {
            "timestamp": timestamp,
            "tag": tag,
            "test_name": test_name,
            "metric": r["metric"],
            "measurements": r["measurements"],
        }
        for r in row_list
    ]


def death_counts(path: str | Path) -> tuple[int, int]:
    """Returns (tunnel_deaths, oom_kills) from tunnel-deaths.log."""
    p = Path(path)
    if not p.exists():
        return 0, 0
    lines = p.read_text().splitlines()
    deaths = len(lines)
    ooms = sum(1 for line in lines if _OOM_RE.search(line))
    return deaths, ooms


def status(
    reports: list[dict],
    load_exit_code: int = 0,
    tunnel_deaths: int = 0,
    oom_kills: int = 0,
    *,
    tunnel_cpus: int | float = 1,
    tunnel_memory: str = "1g",
    actors: int = 10,
) -> dict:
    """Builds the status.json summary for an atunnel benchmark run."""
    verdicts: dict[str, int] = {}
    total_rungs = 0
    for report in reports:
        for rung in report.get("rungs", []):
            total_rungs += 1
            v = rung.get("verdict", "")
            if v:
                verdicts[v] = verdicts.get(v, 0) + 1
    return {
        "rig": "pod",
        "load_exit_code": load_exit_code,
        "scenarios": len(reports),
        "rungs": total_rungs,
        "verdicts": verdicts,
        "tunnel_deaths": tunnel_deaths,
        "oom_kills": oom_kills,
        "tunnel_cpus": tunnel_cpus,
        "tunnel_memory": tunnel_memory,
        "actors": actors,
    }


def write_jsonl(records: list[dict], path: str | Path) -> None:
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
