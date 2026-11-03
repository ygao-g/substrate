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

"""Unit tests for Nighthawk egress output parsing, exact pooled variance, and capacity summary."""

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import output as out_mod


def sample_stage_output(
    requests_sent=10000,
    http_2xx=9998,
    duration_s=20.0,
    p99_ms=12.5,
    mean_ms=5.0,
    pstdev_ms=1.0,
):
    return {
        "results": [
            {
                "name": "global",
                "execution_duration": f"{duration_s}s",
                "counters": [
                    {"name": "upstream_rq_total", "value": requests_sent},
                    {"name": "benchmark.http_2xx", "value": http_2xx},
                    {"name": "benchmark.http_5xx", "value": requests_sent - http_2xx},
                ],
                "statistics": [
                    {
                        "id": out_mod.LATENCY_STAT_ID,
                        "mean": f"{mean_ms / 1000.0}s",
                        "pstdev": f"{pstdev_ms / 1000.0}s",
                        "percentiles": [
                            {"percentile": 0.5, "duration": f"{mean_ms / 1000.0}s"},
                            {"percentile": 0.95, "duration": f"{(mean_ms * 1.5) / 1000.0}s"},
                            {"percentile": 0.99, "duration": f"{p99_ms / 1000.0}s"},
                        ],
                    }
                ],
            }
        ]
    }


def test_single_actor_stage_metrics():
    raw = sample_stage_output(requests_sent=10000, http_2xx=9990, duration_s=20.0, mean_ms=5.0, pstdev_ms=1.2)
    row = out_mod.stage_row_from_json("adjusting_001", raw, per_worker_rps=500, concurrency=1)
    assert row["stage"] == "adjusting_001"
    assert row["duration_s"] == 20.0
    assert abs(row["metric_nighthawk.builtin_achieved_rps"] - 500.0) < 1e-6
    assert abs(row["metric_nighthawk.builtin_success_rate"] - 0.999) < 1e-6
    assert abs(row["error_rate"] - 0.001) < 1e-6
    assert row["latency_mean_ms"] == 5.0
    assert row["latency_pstdev_ms"] == 1.2
    assert row["latency_mean_plus_2stdev_ms"] == round(5.0 + 2.0 * 1.2, 3)


def test_missing_or_non_positive_duration_raises():
    raw_missing = {"results": [{"name": "global"}]}
    with pytest.raises(ValueError, match="missing or non-positive execution_duration"):
        out_mod.stage_row_from_json("stage_1", raw_missing)

    raw_zero = {"results": [{"name": "global", "execution_duration": "0s"}]}
    with pytest.raises(ValueError, match="missing or non-positive execution_duration"):
        out_mod.stage_row_from_json("stage_1", raw_zero)


def test_m1_pooled_variance_equivalence():
    raw = sample_stage_output(requests_sent=10000, http_2xx=10000, duration_s=20.0, mean_ms=4.5, pstdev_ms=1.1)
    single_row = out_mod.stage_row_from_json("stage_1", raw)
    agg = out_mod.aggregate_multi_actor_stage("stage_1", [raw], total_attempted_rps=500.0)

    # In M=1, pooled mean and variance must match single actor identically
    assert agg["latency_mean_ms"] == single_row["latency_mean_ms"]
    assert agg["latency_pstdev_ms"] == single_row["latency_pstdev_ms"]
    assert agg["latency_mean_plus_2stdev_ms"] == single_row["latency_mean_plus_2stdev_ms"]
    assert agg["max_actor_mean_plus_2stdev_ms"] == single_row["latency_mean_plus_2stdev_ms"]


def test_m4_exact_pooled_variance():
    # 4 actors with unequal sample sizes, means, and pstdevs
    actors = [
        sample_stage_output(requests_sent=1000, mean_ms=4.0, pstdev_ms=0.5),
        sample_stage_output(requests_sent=2000, mean_ms=5.0, pstdev_ms=1.0),
        sample_stage_output(requests_sent=3000, mean_ms=6.0, pstdev_ms=1.5),
        sample_stage_output(requests_sent=4000, mean_ms=7.0, pstdev_ms=2.0),
    ]

    # Ground truth calculation:
    counts = [1000.0, 2000.0, 3000.0, 4000.0]
    means = [4.0, 5.0, 6.0, 7.0]
    sigmas = [0.5, 1.0, 1.5, 2.0]
    total_n = sum(counts)
    expected_mean = sum(n * mu for n, mu in zip(counts, means)) / total_n
    expected_var = sum(
        n * (sig**2 + (mu - expected_mean)**2)
        for n, mu, sig in zip(counts, means, sigmas)
    ) / total_n
    expected_pooled_plus_2 = round(expected_mean + 2.0 * math.sqrt(expected_var), 3)

    actor_mean_plus_2 = [mu + 2.0 * sig for mu, sig in zip(means, sigmas)]
    expected_max_actor = round(max(actor_mean_plus_2), 3)

    agg = out_mod.aggregate_multi_actor_stage("testing", actors, total_attempted_rps=10000 / 20.0)
    assert abs(agg["latency_mean_ms"] - round(expected_mean, 3)) < 1e-3
    assert abs(agg["latency_mean_plus_2stdev_ms"] - expected_pooled_plus_2) < 1e-3
    assert abs(agg["max_actor_mean_plus_2stdev_ms"] - expected_max_actor) < 1e-3
    assert agg["actors"] == 4


def test_capacity_summary_verdict_and_ext_proc_cpu():
    clean_stage = {
        "stage": "testing",
        "metric_nighthawk.builtin_achieved_rps": 9500.0,
        "metric_nighthawk.builtin_success_rate": 0.999,
        "failed_thresholds": [],
        "latency_mean_plus_2stdev_ms": 18.5,
    }
    resources = {
        "envoy_cpu_util": 0.88,
        "ext_proc_cpu_util": 0.82,
        "worker_max_cpu_util": 0.35,
        "origin_max_cpu_util": 0.25,
    }
    summary = out_mod.capacity_summary(
        [clean_stage],
        envoy_cpu=2,
        ext_proc_cpu=2,
        actors=4,
        client_concurrency=4,
        tail_latency_slo_ms=25.0,
        stage_resources=resources,
    )
    assert summary["verdict"] == "pass"
    assert summary["envoy_cpu"] == 2
    assert summary["ext_proc_cpu"] == 2
    assert summary["gateway_saturated"] is True
    assert summary["bottleneck_component"] == "envoy+ext-proc"


def test_capacity_summary_failed_testing_stage():
    failed_stage = {
        "stage": "testing",
        "metric_nighthawk.builtin_achieved_rps": 9500.0,
        "metric_nighthawk.builtin_success_rate": 0.98,  # Below 0.998
        "failed_thresholds": ["success-rate"],
    }
    summary = out_mod.capacity_summary(
        [failed_stage],
        envoy_cpu=2,
        ext_proc_cpu=2,
        actors=4,
    )
    assert summary["verdict"] == "fail"
    assert summary["converged"] is False
    assert summary["testing_passed"] is False


def test_capacity_summary_marginal_testing_stage_with_clean_adjusting():
    clean_adjusting = {
        "stage": "adjusting_006",
        "metric_nighthawk.builtin_achieved_rps": 14000.0,
        "metric_nighthawk.builtin_success_rate": 0.999,
        "failed_thresholds": [],
    }
    marginal_testing = {
        "stage": "testing",
        "metric_nighthawk.builtin_achieved_rps": 14000.0,
        "metric_nighthawk.builtin_success_rate": 0.999,
        "failed_thresholds": ["latency-ns-mean-plus-2stdev"],
    }
    summary = out_mod.capacity_summary(
        [clean_adjusting, marginal_testing],
        envoy_cpu=2,
        ext_proc_cpu=2,
        actors=4,
    )
    assert summary["converged"] is True
    assert summary["testing_passed"] is False
    assert summary["slo_max_rps"] == 14000.0
    assert summary["slo_max_stage"] == "adjusting_006"
    assert summary["verdict"] == "pass"


def test_aggregate_multi_actor_stage_none_values(monkeypatch):
    # Pass sparse/None actor row metrics to ensure coalescing handles None safely
    rows_with_none = [
        {
            "stage": "adjusting_001",
            "duration_s": 20.0,
            "upstream_rq_total": None,
            "http_2xx": None,
            "http_5xx": None,
            "p50_ms": None,
            "p90_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "latency_mean_ms": None,
            "latency_pstdev_ms": None,
            "latency_mean_plus_2stdev_ms": None,
        },
        {
            "stage": "adjusting_001",
            "duration_s": 20.0,
            "upstream_rq_total": 100,
            "http_2xx": 100,
            "http_5xx": 0,
            "p50_ms": 2.0,
            "p90_ms": 4.0,
            "p95_ms": 5.0,
            "p99_ms": 8.0,
            "latency_mean_ms": 2.0,
            "latency_pstdev_ms": 0.5,
            "latency_mean_plus_2stdev_ms": 3.0,
        },
    ]

    monkeypatch.setattr(out_mod, "stage_row_from_json", lambda s, out: out)
    agg = out_mod.aggregate_multi_actor_stage("adjusting_001", rows_with_none, total_attempted_rps=100.0)
    assert agg["stage"] == "adjusting_001"
    assert agg["upstream_rq_total"] == 100
    assert agg["p99_ms"] == 8.0
    assert agg["latency_mean_ms"] == 2.0
    assert agg["latency_pstdev_ms"] == 0.5
    assert agg["latency_mean_plus_2stdev_ms"] == 3.0
