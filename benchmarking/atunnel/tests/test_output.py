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

"""Unit tests for atunnel benchmark output conversion."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import output as output_mod

CONFIG = {"tunnel_cpus": 1, "tunnel_memory": "1g", "actors": 10}

REPORTS = [
    {
        "scenario": "IngressH1",
        "description": "router-style HTTPS requests over HTTP/1.1 keep-alive",
        "unit": "request",
        "step_ns": 10_000_000_000,
        "warmup_ns": 2_000_000_000,
        "rungs": [
            {
                "load": {
                    "offered": 4000.0,
                    "measured_seconds": 8.0,
                    "scheduled": 32000,
                    "skipped": 0,
                    "completed": 32000,
                    "errors": 0,
                    "achieved": 4000.0,
                    "latency": {
                        "p50_ms": 0.9,
                        "p90_ms": 1.8,
                        "p99_ms": 3.1,
                        "max_ms": 12.0,
                        "mean_ms": 1.1,
                    },
                    "send_lag": {
                        "p50_ms": 0.01,
                        "p90_ms": 0.05,
                        "p99_ms": 0.1,
                        "max_ms": 0.5,
                        "mean_ms": 0.02,
                    },
                    "bytes": 512000,
                    "mbytes_per_sec": 0.064,
                    "error_samples": ["sample error that must be dropped"],
                    "future_scalar": 42,
                },
                "tunnel": {
                    "cpu_seconds": 5.2,
                    "cores": 0.65,
                    "cpu_us_per_unit": 162.0,
                    "gomaxprocs": 1,
                    "rss_max_bytes": 60_000_000,
                    "cgroup_mem_max_bytes": 70_000_000,
                    "cgroup_cpu_limit_cores": 1.0,
                },
                "generator": {
                    "cpu_seconds": 9.6,
                    "cores": 1.2,
                    "cpu_us_per_unit": 300.0,
                    "gomaxprocs": 8,
                },
                "sink": {
                    "cpu_seconds": 2.4,
                    "cores": 0.3,
                    "cpu_us_per_unit": 75.0,
                    "gomaxprocs": 8,
                },
                "verdict": "OK",
                "profiles": [
                    "/tmp/profiles/IngressH1-4000.cpu.pb.gz",
                    "/tmp/profiles/IngressH1-4000.heap.pb.gz",
                ],
            },
            {
                "load": {
                    "offered": 8000.0,
                    "measured_seconds": 8.0,
                    "scheduled": 64000,
                    "skipped": 0,
                    "completed": 50000,
                    "errors": 0,
                    "achieved": 6250.0,
                    "latency": {
                        "p50_ms": 2.5,
                        "p90_ms": 6.0,
                        "p99_ms": 15.0,
                        "max_ms": 40.0,
                        "mean_ms": 3.5,
                    },
                    "send_lag": {
                        "p50_ms": 0.1,
                        "p90_ms": 0.5,
                        "p99_ms": 2.0,
                        "max_ms": 10.0,
                        "mean_ms": 0.3,
                    },
                    "bytes": 800000,
                    "mbytes_per_sec": 0.1,
                },
                "tunnel": {
                    "cpu_seconds": 7.9,
                    "cores": 0.99,
                    "cpu_us_per_unit": 158.0,
                    "gomaxprocs": 1,
                    "rss_max_bytes": 65_000_000,
                    "cgroup_mem_max_bytes": 75_000_000,
                    "cgroup_cpu_limit_cores": 1.0,
                },
                "generator": {
                    "cpu_seconds": 11.2,
                    "cores": 1.4,
                    "cpu_us_per_unit": 224.0,
                    "gomaxprocs": 8,
                },
                "sink": {
                    "cpu_seconds": 3.2,
                    "cores": 0.4,
                    "cpu_us_per_unit": 64.0,
                    "gomaxprocs": 8,
                },
                "verdict": "SATURATED",
                "reason": "atunnel used 0.99 of its 1.00 core limit",
            },
            {
                "load": {
                    "offered": 16000.0,
                    "measured_seconds": 0.0,
                    "scheduled": 0,
                    "skipped": 0,
                    "completed": 0,
                    "errors": 0,
                    "achieved": 0.0,
                    "latency": {
                        "p50_ms": 0.0,
                        "p90_ms": 0.0,
                        "p99_ms": 0.0,
                        "max_ms": 0.0,
                        "mean_ms": 0.0,
                    },
                    "send_lag": {
                        "p50_ms": 0.0,
                        "p90_ms": 0.0,
                        "p99_ms": 0.0,
                        "max_ms": 0.0,
                        "mean_ms": 0.0,
                    },
                    "bytes": 0,
                    "mbytes_per_sec": 0.0,
                },
                "tunnel": {
                    "cpu_seconds": 0.0,
                    "cores": 0.0,
                    "cpu_us_per_unit": 0.0,
                    "gomaxprocs": 0,
                },
                "generator": {
                    "cpu_seconds": 0.0,
                    "cores": 0.0,
                    "cpu_us_per_unit": 0.0,
                    "gomaxprocs": 0,
                },
                "sink": {
                    "cpu_seconds": 0.0,
                    "cores": 0.0,
                    "cpu_us_per_unit": 0.0,
                    "gomaxprocs": 0,
                },
                "verdict": "DOWN",
                "reason": "atunnel stopped answering: EOF",
            },
        ],
        "sink_counters": {
            "ingress_actor_requests": 82000,
            "gateway_connections": 0,
        },
    },
    {
        "scenario": "EgressTCP",
        "description": "4 streams writing 64 KiB chunks",
        "unit": "write",
        "step_ns": 10_000_000_000,
        "warmup_ns": 2_000_000_000,
        "rungs": [
            {
                "load": {
                    "offered": 0.0,
                    "measured_seconds": 8.0,
                    "scheduled": 0,
                    "skipped": 0,
                    "completed": 16000,
                    "errors": 0,
                    "achieved": 2000.0,
                    "latency": {
                        "p50_ms": 1.0,
                        "p90_ms": 1.5,
                        "p99_ms": 2.0,
                        "max_ms": 5.0,
                        "mean_ms": 1.1,
                    },
                    "send_lag": {
                        "p50_ms": 0.0,
                        "p90_ms": 0.0,
                        "p99_ms": 0.0,
                        "max_ms": 0.0,
                        "mean_ms": 0.0,
                    },
                    "bytes": 1_048_576_000,
                    "mbytes_per_sec": 131.072,
                },
                "tunnel": {
                    "cpu_seconds": 6.4,
                    "cores": 0.8,
                    "cpu_us_per_unit": 400.0,
                    "gomaxprocs": 1,
                },
                "generator": {
                    "cpu_seconds": 1.6,
                    "cores": 0.2,
                    "cpu_us_per_unit": 100.0,
                    "gomaxprocs": 8,
                },
                "sink": {
                    "cpu_seconds": 1.6,
                    "cores": 0.2,
                    "cpu_us_per_unit": 100.0,
                    "gomaxprocs": 8,
                },
                "verdict": "OK",
            }
        ],
    },
]


def test_rows_and_stats_records_shape():
    rows = output_mod.rows(REPORTS, CONFIG)
    records = output_mod.stats_records(
        rows,
        tag="deadbeef",
        test_name="atunnel_1cpu_1g",
        timestamp="2026-11-03T03:12:00Z",
    )
    metrics = [r["metric"] for r in records]
    assert metrics == [
        "atunnel_IngressH1_4000",
        "atunnel_IngressH1_8000",
        "atunnel_IngressH1_16000",
        "atunnel_IngressH1",
        "atunnel_IngressH1_sink_counters",
        "atunnel_EgressTCP_0",
        "atunnel_EgressTCP",
    ]
    assert len(metrics) == len(set(metrics))
    for rec in records:
        assert set(rec.keys()) == {
            "timestamp",
            "tag",
            "test_name",
            "metric",
            "measurements",
        }
        assert rec["timestamp"] == "2026-11-03T03:12:00Z"
        assert rec["tag"] == "deadbeef"
        assert rec["test_name"] == "atunnel_1cpu_1g"


def test_per_rung_flattening_and_filtering():
    by_metric = {
        r["metric"]: r["measurements"]
        for r in output_mod.rows(REPORTS, CONFIG)
    }

    r4000 = by_metric["atunnel_IngressH1_4000"]
    assert r4000["rig"] == "pod"
    assert r4000["scenario"] == "IngressH1"
    assert r4000["rung"] == 0
    assert r4000["verdict"] == "OK"
    assert r4000["reason"] == ""
    assert r4000["unit"] == "request"
    assert r4000["step_ns"] == 10_000_000_000
    assert r4000["warmup_ns"] == 2_000_000_000
    assert r4000["tunnel_cpus"] == 1
    assert r4000["tunnel_memory"] == "1g"
    assert r4000["actors"] == 10
    assert r4000["load_offered"] == 4000.0
    assert r4000["load_latency_p99_ms"] == 3.1
    assert r4000["load_send_lag_p99_ms"] == 0.1
    assert r4000["tunnel_cores"] == 0.65
    assert r4000["tunnel_cpu_us_per_unit"] == 162.0
    assert r4000["generator_cores"] == 1.2
    assert r4000["sink_cores"] == 0.3
    assert r4000["load_future_scalar"] == 42
    assert "profiles" not in r4000
    assert "load_error_samples" not in r4000

    down = by_metric["atunnel_IngressH1_16000"]
    assert down["verdict"] == "DOWN"
    assert down["rung"] == 2
    assert not any(k.startswith("tunnel_") and k not in CONFIG for k in down)


def test_scenario_summary_and_sink_counters():
    by_metric = {
        r["metric"]: r["measurements"]
        for r in output_mod.rows(REPORTS, CONFIG)
    }

    summary = by_metric["atunnel_IngressH1"]
    assert summary["rig"] == "pod"
    assert summary["rungs"] == 3
    assert summary["max_ok_offered"] == 4000.0
    assert summary["first_not_ok_offered"] == 8000.0
    assert summary["first_not_ok_verdict"] == "SATURATED"
    assert summary["down_rungs"] == 1
    assert summary["tunnel_cpu_us_per_unit_at_max_ok"] == 162.0

    counters = by_metric["atunnel_IngressH1_sink_counters"]
    assert counters == {
        "ingress_actor_requests": 82000,
        "gateway_connections": 0,
    }


def test_death_counts(tmp_path):
    missing = tmp_path / "missing.log"
    assert output_mod.death_counts(missing) == (0, 0)

    log = tmp_path / "tunnel-deaths.log"
    log.write_text(
        "03:12:01 atunnel exited (signal: killed); OOM kills: 1\n"
        "03:12:15 atunnel exited (exit status 1); OOM kills: 0\n"
        "03:12:30 atunnel exited (signal: killed); OOM kills: 2\n"
    )
    assert output_mod.death_counts(log) == (3, 2)


def test_status():
    st = output_mod.status(
        REPORTS,
        load_exit_code=0,
        tunnel_deaths=3,
        oom_kills=2,
        tunnel_cpus=1,
        tunnel_memory="1g",
        actors=10,
    )
    assert st == {
        "rig": "pod",
        "load_exit_code": 0,
        "scenarios": 2,
        "rungs": 4,
        "verdicts": {"OK": 2, "SATURATED": 1, "DOWN": 1},
        "tunnel_deaths": 3,
        "oom_kills": 2,
        "tunnel_cpus": 1,
        "tunnel_memory": "1g",
        "actors": 10,
    }


def test_runner_parse_args():
    import runner as runner_mod

    args = runner_mod.parse_args(
        [
            "--name",
            "atunnel_1cpu_1g",
            "--tag",
            "abc123",
            "--dest",
            "gs://bucket/path",
            "--tunnel-cpus",
            "2",
            "--tunnel-memory",
            "2g",
            "--actors",
            "10",
            "--scenarios",
            "IngressH1",
            "--rungs",
            "1000,2000",
        ]
    )
    assert args.tunnel_cpus == 2
    assert isinstance(args.tunnel_cpus, int)
    assert args.extra == ["--scenarios", "IngressH1", "--rungs", "1000,2000"]

    frac = runner_mod.parse_args(
        [
            "--name",
            "t",
            "--tag",
            "sha",
            "--dest",
            "/tmp/out",
            "--tunnel-cpus",
            "0.5",
        ]
    )
    assert frac.tunnel_cpus == 0.5
    assert isinstance(frac.tunnel_cpus, float)


if __name__ == "__main__":
    for fn_name, fn in sorted(dict(globals()).items()):
        if fn_name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {fn_name}")
