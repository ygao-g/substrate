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

"""Unit tests for results.json -> stats.jsonl rows and status.json."""

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import output as output_mod

CONFIG = {"tunnel_cpus": 1, "tunnel_memory": "1g", "actors": 10, "body_bytes": 16}


def rung(offered, verdict="OK", reason="", tunnel=True):
    r = {
        "load": {
            "offered": offered,
            "measured_seconds": 8,
            "scheduled": 10 * offered,
            "skipped": 0,
            "completed": 8 * offered,
            "errors": 0,
            "achieved": float(offered),
            "latency": {"p50_ms": 0.9, "p90_ms": 2.0, "p99_ms": 3.1, "max_ms": 9.0, "mean_ms": 1.0},
            "send_lag": {"p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.1, "max_ms": 0.2, "mean_ms": 0.0},
            "bytes": 16 * 8 * offered,
            "mbytes_per_sec": 0.016,
            "error_samples": ["connection reset"],
        },
        "generator": {"cpu_seconds": 9.6, "cores": 1.2, "cpu_us_per_unit": 300.0, "gomaxprocs": 8},
        "sink": {"cpu_seconds": 2.4, "cores": 0.3, "cpu_us_per_unit": 75.0, "gomaxprocs": 8},
        "verdict": verdict,
        "profiles": ["cpu.pb.gz"],
    }
    if reason:
        r["reason"] = reason
    # A DOWN rung has no tunnel figures: the process was gone (omitempty).
    r["tunnel"] = (
        {
            "cpu_seconds": 5.2,
            "cores": 0.65,
            "cpu_us_per_unit": 162.0,
            "gomaxprocs": 2,
            "rss_max_bytes": 60000000,
            "cgroup_cores": 0.65,
            "cgroup_mem_max_bytes": 70000000,
            "cgroup_cpu_limit_cores": 1.0,
        }
        if tunnel
        else {"cpu_seconds": 0, "cores": 0, "cpu_us_per_unit": 0, "gomaxprocs": 0}
    )
    return r


REPORTS = [
    {
        "scenario": "IngressH1",
        "description": "router-style HTTPS requests",
        "unit": "request",
        "step_ns": 10000000000,
        "warmup_ns": 2000000000,
        "rungs": [rung(1000), rung(2000), rung(4000), rung(8000, "SATURATED", "atunnel at its CPU limit")],
        "sink_counters": {"ingress_actor_requests": 123456, "gateway_connections": 10},
    },
    {
        "scenario": "EgressIdleConns",
        "description": "held connections",
        "unit": "held connection",
        "step_ns": 10000000000,
        "warmup_ns": 0,
        "rungs": [rung(4000), rung(8000, "DOWN", "atunnel stopped answering", tunnel=False)],
    },
    {
        "scenario": "EgressTCP",
        "description": "4 connections write as fast as they can",
        "unit": "write",
        "step_ns": 10000000000,
        "warmup_ns": 2000000000,
        "rungs": [rung(1000, "RIG-LIMITED", "the sink was at its CPU limit"), rung(2000), rung(4000, "SHORTFALL", "fell behind")],
    },
]


def by_metric(rows):
    return {r["metric"]: r["measurements"] for r in rows}


def test_three_row_kinds_with_unique_names():
    rows = output_mod.rows(REPORTS, CONFIG)
    names = [r["metric"] for r in rows]
    assert len(names) == len(set(names))
    assert names == [
        "atunnel_IngressH1_1000",
        "atunnel_IngressH1_2000",
        "atunnel_IngressH1_4000",
        "atunnel_IngressH1_8000",
        "atunnel_IngressH1",
        "atunnel_IngressH1_sink_counters",
        "atunnel_EgressIdleConns_4000",
        "atunnel_EgressIdleConns_8000",
        "atunnel_EgressIdleConns",
        "atunnel_EgressTCP_1000",
        "atunnel_EgressTCP_2000",
        "atunnel_EgressTCP_4000",
        "atunnel_EgressTCP",
    ]


def test_rung_row_is_flat_and_named_after_the_json():
    m = by_metric(output_mod.rows(REPORTS, CONFIG))["atunnel_IngressH1_4000"]
    assert m["rig"] == "pod"
    assert m["scenario"] == "IngressH1"
    assert m["rung"] == 2
    assert m["verdict"] == "OK"
    assert m["reason"] == ""
    assert m["unit"] == "request"
    assert m["tunnel_cpus"] == 1 and m["tunnel_memory"] == "1g" and m["actors"] == 10 and m["body_bytes"] == 16
    assert m["load_offered"] == 4000
    assert m["load_latency_p99_ms"] == 3.1
    assert m["load_send_lag_p99_ms"] == 0.1
    assert m["tunnel_cpu_us_per_unit"] == 162.0
    assert m["tunnel_cgroup_mem_max_bytes"] == 70000000
    assert m["generator_cores"] == 1.2
    assert m["sink_cores"] == 0.3
    assert not any(isinstance(v, (dict, list)) for v in m.values())


def test_rungs_rounding_to_the_same_rate_get_distinct_names():
    report = {"scenario": "S", "rungs": [rung(1000.4), rung(1000.6), rung(1000.9, "SATURATED")]}
    assert [r["metric"] for r in output_mod.rows([report], CONFIG)] == [
        "atunnel_S_1000",
        "atunnel_S_1000_1",
        "atunnel_S_1000_2",
        "atunnel_S",
    ]


def test_lists_and_nulls_are_dropped():
    reports = copy.deepcopy(REPORTS)
    reports[0]["rungs"][0]["tunnel"]["rss_max_bytes"] = None
    m = by_metric(output_mod.rows(reports, CONFIG))["atunnel_IngressH1_1000"]
    assert "load_error_samples" not in m
    assert "profiles" not in m
    assert "tunnel_rss_max_bytes" not in m


def test_down_rung_has_no_tunnel_figures_but_its_reason():
    m = by_metric(output_mod.rows(REPORTS, CONFIG))["atunnel_EgressIdleConns_8000"]
    assert m["verdict"] == "DOWN"
    assert m["reason"] == "atunnel stopped answering"
    # The dead tunnel's zeros would read as measurements; only the rig's
    # configuration keeps its tunnel_ prefix.
    assert [k for k in m if k.startswith("tunnel_")] == ["tunnel_cpus", "tunnel_memory"]
    assert m["load_offered"] == 8000
    assert m["generator_cores"] == 1.2


def test_unknown_key_passes_through():
    reports = copy.deepcopy(REPORTS)
    reports[0]["rungs"][0]["tunnel"]["cgroup_sock_max_bytes"] = 2572288
    m = by_metric(output_mod.rows(reports, CONFIG))["atunnel_IngressH1_1000"]
    assert m["tunnel_cgroup_sock_max_bytes"] == 2572288


def test_summary_rows():
    rows = by_metric(output_mod.rows(REPORTS, CONFIG))
    assert rows["atunnel_IngressH1"] == {
        "rig": "pod",
        "rungs": 4,
        "down_rungs": 0,
        "rig_limited_rungs": 0,
        "max_ok_offered": 4000,
        "tunnel_cpu_us_per_unit_at_max_ok": 162.0,
        "first_not_ok_offered": 8000,
        "first_not_ok_verdict": "SATURATED",
    }
    assert rows["atunnel_EgressIdleConns"]["down_rungs"] == 1
    assert rows["atunnel_EgressIdleConns"]["first_not_ok_verdict"] == "DOWN"


def test_summary_skips_rig_limited_rungs():
    m = by_metric(output_mod.rows(REPORTS, CONFIG))["atunnel_EgressTCP"]
    assert m["rig_limited_rungs"] == 1
    assert m["max_ok_offered"] == 2000
    assert m["first_not_ok_offered"] == 4000
    assert m["first_not_ok_verdict"] == "SHORTFALL"


def test_summary_without_ok_or_not_ok_rungs():
    only_down = [{"scenario": "S", "rungs": [rung(1000, "DOWN", tunnel=False)]}]
    m = by_metric(output_mod.rows(only_down, CONFIG))["atunnel_S"]
    assert "max_ok_offered" not in m and m["first_not_ok_offered"] == 1000
    all_ok = [{"scenario": "S", "rungs": [rung(1000), rung(2000)]}]
    m = by_metric(output_mod.rows(all_ok, CONFIG))["atunnel_S"]
    assert m["max_ok_offered"] == 2000 and "first_not_ok_offered" not in m


def test_sink_counters_row():
    rows = by_metric(output_mod.rows(REPORTS, CONFIG))
    assert rows["atunnel_IngressH1_sink_counters"] == {
        "rig": "pod",
        "scenario": "IngressH1",
        "ingress_actor_requests": 123456,
        "gateway_connections": 10,
    }
    assert "atunnel_EgressIdleConns_sink_counters" not in rows


def test_stats_records_have_five_keys():
    records = output_mod.stats_records(output_mod.rows(REPORTS, CONFIG), "abc123", "atunnel_1cpu_1g", "2026-11-03T03:12:00Z")
    assert len(records) == 13
    for rec in records:
        assert set(rec) == {"timestamp", "tag", "test_name", "metric", "measurements"}
        assert rec["timestamp"] == "2026-11-03T03:12:00Z"
        assert rec["tag"] == "abc123"
        assert rec["test_name"] == "atunnel_1cpu_1g"


def test_death_counts(tmp_path):
    log = tmp_path / "tunnel-deaths.log"
    log.write_text(
        "12:00:01 atunnel exited (signal: killed); OOM kills: 1\n"
        "12:00:09 atunnel exited (exit status 1); OOM kills: 0\n"
        "12:00:20 atunnel exited (signal: killed); OOM kills: 2\n"
    )
    assert output_mod.death_counts(log) == (3, 2)
    assert output_mod.death_counts(tmp_path / "missing") == (0, 0)


def test_status():
    assert output_mod.status(REPORTS, 0, 1, 1, CONFIG) == {
        "rig": "pod",
        "load_exit_code": 0,
        "scenarios": 3,
        "rungs": 9,
        "verdicts": {"OK": 5, "SATURATED": 1, "DOWN": 1, "RIG-LIMITED": 1, "SHORTFALL": 1},
        "tunnel_deaths": 1,
        "oom_kills": 1,
        "tunnel_cpus": 1,
        "tunnel_memory": "1g",
        "actors": 10,
        "body_bytes": 16,
    }


def test_death_line():
    assert output_mod.death_line(1, 1, "256m") == "atunnel died 1 time(s), 1 of them OOM-killed at its 256m limit"


def test_write_jsonl(tmp_path):
    path = tmp_path / "stats.jsonl"
    output_mod.write_jsonl(path, [{"a": 1}, {"b": "x"}])
    assert path.read_text() == '{"a": 1}\n{"b": "x"}\n'
