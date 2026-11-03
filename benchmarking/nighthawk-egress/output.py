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

"""Transforms Nighthawk output into stats records and capacity summaries for nighthawk-egress."""

import json
import math

LATENCY_STAT_ID = "benchmark_http_client.request_to_response"
PERCENTILE_TARGETS = {"p50": 0.5, "p90": 0.9, "p95": 0.95, "p99": 0.99}

COUNTERS_OF_INTEREST = (
    "benchmark.http_2xx",
    "benchmark.http_3xx",
    "benchmark.http_4xx",
    "benchmark.http_5xx",
    "benchmark.pool_overflow",
    "benchmark.pool_connection_failure",
    "benchmark.stream_resets",
    "upstream_rq_total",
)

BUILTIN_PREFIX = "metric_nighthawk.builtin_"


def _builtin_key(metric_name: str) -> str:
    return BUILTIN_PREFIX + metric_name.replace("-", "_")


def _duration_seconds(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).rstrip("s") or 0)


def _global_result(nighthawk_output: dict) -> dict:
    results = nighthawk_output.get("results", [])
    for r in results:
        if r.get("name") == "global":
            return r
    return results[0] if results else {}


def _counters(result: dict) -> dict:
    return {
        c.get("name", ""): int(c.get("value", 0))
        for c in result.get("counters", [])
    }


def _latency_ms(result: dict) -> dict:
    out: dict = {}
    for stat in result.get("statistics", []):
        if stat.get("id") != LATENCY_STAT_ID:
            continue
        mean_s = _duration_seconds(stat.get("mean"))
        pstdev_s = _duration_seconds(stat.get("pstdev"))
        mean_ms = round(mean_s * 1000.0, 3)
        pstdev_ms = round(pstdev_s * 1000.0, 3)
        mean_plus_2stdev_ms = round((mean_s + 2.0 * pstdev_s) * 1000.0, 3)

        out["latency_mean_ms"] = mean_ms
        out["latency_pstdev_ms"] = pstdev_ms
        out["latency_min_ms"] = _duration_seconds(stat.get("min")) * 1000.0
        out["latency_max_ms"] = _duration_seconds(stat.get("max")) * 1000.0
        out["latency_mean_plus_2stdev_ms"] = mean_plus_2stdev_ms

        percentiles = stat.get("percentiles", [])
        for label, target in PERCENTILE_TARGETS.items():
            for p in percentiles:
                if float(p.get("percentile", 0)) >= target:
                    out[f"{label}_ms"] = _duration_seconds(p.get("duration")) * 1000.0
                    break
        break
    return out


def stage_row_from_json(
    stage: str,
    nh_output: dict,
    per_worker_rps: int = 0,
    concurrency: int = 1,
    start_time: str = "",
    end_time: str = "",
) -> dict:
    result = _global_result(nh_output)
    counters = _counters(result)
    options = nh_output.get("options", {})
    duration_s = _duration_seconds(result.get("execution_duration"))
    if duration_s <= 0.0:
        duration_s = _duration_seconds(options.get("duration"))
    if duration_s <= 0.0:
        raise ValueError("missing or non-positive execution_duration")

    if not per_worker_rps:
        per_worker_rps = int(options.get("requests_per_second", 0) or 0)
    if concurrency <= 1 and options.get("concurrency"):
        concurrency = int(options.get("concurrency") or 1)

    sent = counters.get("upstream_rq_total", 0)
    http_2xx = counters.get("benchmark.http_2xx", 0)
    achieved_rps = float(sent) / duration_s if duration_s > 0 else 0.0
    attempted_rps = float(per_worker_rps * concurrency)

    row = {
        "stage": stage,
        "start_time": start_time,
        "end_time": end_time,
        "duration_s": duration_s,
        "per_worker_rps": per_worker_rps,
        "concurrency": concurrency,
        _builtin_key("attempted-rps"): attempted_rps,
        _builtin_key("achieved-rps"): achieved_rps,
        _builtin_key("send-rate"): (achieved_rps / attempted_rps) if attempted_rps > 0 else 0.0,
        _builtin_key("success-rate"): (float(http_2xx) / sent) if sent > 0 else 0.0,
        "error_rate": (float(sent - http_2xx) / sent) if sent > 0 else 0.0,
    }

    for name in COUNTERS_OF_INTEREST:
        row[name.replace("benchmark.", "")] = counters.get(name, 0)
    row.update(_latency_ms(result))
    row["failed_thresholds"] = []
    return row


def aggregate_multi_actor_stage(
    stage: str,
    actor_outputs: list[dict],
    total_attempted_rps: float = 0.0,
    start_time: str = "",
    end_time: str = "",
) -> dict:
    """Aggregate parallel stage outputs from M actors with exact pooled variance."""
    if not actor_outputs:
        return {"stage": stage, "actors": 0}

    rows = [stage_row_from_json(stage, out) for out in actor_outputs]
    duration_s = max(((r.get("duration_s") or 0.0) for r in rows), default=0.0)
    if duration_s <= 0.0:
        raise ValueError("missing or non-positive execution_duration")
    total_sent = sum((r.get("upstream_rq_total") or 0) for r in rows)
    total_2xx = sum((r.get("http_2xx") or 0) for r in rows)

    achieved_rps = float(total_sent) / duration_s if duration_s > 0 else 0.0
    success_rate = (float(total_2xx) / total_sent) if total_sent > 0 else 0.0
    error_rate = (float(total_sent - total_2xx) / total_sent) if total_sent > 0 else 0.0
    send_rate = (achieved_rps / total_attempted_rps) if total_attempted_rps > 0 else 0.0

    # Max-per-actor conservative percentiles
    p99_ms = max(((r.get("p99_ms") or 0.0) for r in rows), default=0.0)
    p95_ms = max(((r.get("p95_ms") or 0.0) for r in rows), default=0.0)
    p90_ms = max(((r.get("p90_ms") or 0.0) for r in rows), default=0.0)

    # Exact pooled variance across M actors weighted by achieved_count_i = achieved_rps_i * duration_s
    # falling back to equal weights if total count == 0.
    counts = [float(r.get("upstream_rq_total") or 0) for r in rows]
    total_n = sum(counts)
    if total_n <= 0.0:
        counts = [1.0 for _ in rows]
        total_n = float(len(rows))

    mean_pool = sum(n * (r.get("latency_mean_ms") or 0.0) for n, r in zip(counts, rows)) / total_n
    var_pool = sum(
        n * (((r.get("latency_pstdev_ms") or 0.0) ** 2) + (((r.get("latency_mean_ms") or 0.0) - mean_pool) ** 2))
        for n, r in zip(counts, rows)
    ) / total_n
    pooled_pstdev_ms = round(math.sqrt(max(0.0, var_pool)), 3)
    pooled_mean_plus_2stdev_ms = round(mean_pool + 2.0 * math.sqrt(max(0.0, var_pool)), 3)
    max_actor_mean_plus_2stdev_ms = round(
        max(((r.get("latency_mean_plus_2stdev_ms") or 0.0) for r in rows), default=0.0), 3
    )

    if total_sent > 0:
        p50_ms = sum((r.get("p50_ms") or 0.0) * (r.get("upstream_rq_total") or 0) for r in rows) / total_sent
    else:
        p50_ms = 0.0

    row = {
        "stage": stage,
        "actors": len(actor_outputs),
        "start_time": start_time,
        "end_time": end_time,
        "duration_s": duration_s,
        _builtin_key("attempted-rps"): total_attempted_rps,
        _builtin_key("achieved-rps"): achieved_rps,
        _builtin_key("send-rate"): send_rate,
        _builtin_key("success-rate"): success_rate,
        "error_rate": error_rate,
        "upstream_rq_total": total_sent,
        "http_2xx": total_2xx,
        "http_5xx": sum((r.get("http_5xx") or 0) for r in rows),
        "p50_ms": p50_ms,
        "p90_ms": p90_ms,
        "p95_ms": p95_ms,
        "p99_ms": p99_ms,
        "latency_mean_ms": round(mean_pool, 3),
        "latency_pstdev_ms": pooled_pstdev_ms,
        "latency_mean_plus_2stdev_ms": pooled_mean_plus_2stdev_ms,
        "max_actor_mean_plus_2stdev_ms": max_actor_mean_plus_2stdev_ms,
        "failed_thresholds": [],
    }
    return row


def stats_records(rows: list[dict], tag: str, test_name: str) -> list[dict]:
    records = []
    for row in rows:
        measurements = {
            k: v for k, v in row.items() if k not in ("stage", "start_time")
        }
        records.append(
            {
                "timestamp": row.get("start_time", ""),
                "tag": tag,
                "test_name": test_name,
                "metric": f"nighthawk_{row['stage']}",
                "measurements": measurements,
            }
        )
    return records


def capacity_summary(
    rows: list[dict],
    *,
    envoy_cpu: int,
    ext_proc_cpu: int | None = None,
    actors: int,
    client_concurrency: int | None = None,
    tail_latency_slo_ms: float | None = None,
    stage_resources: dict | None = None,
) -> dict:
    """Build capacity.json enriched with saturation verdicts and bottleneck diagnosis."""
    rows = list(rows)

    if ext_proc_cpu is None:
        ext_proc_cpu = envoy_cpu

    testing = next((r for r in rows if r.get("stage") == "testing"), None)
    clean = [r for r in rows if not r.get("failed_thresholds")]
    best_clean = max(
        clean, key=lambda r: r.get(_builtin_key("achieved-rps"), 0.0), default=None
    )

    # The search converges to the failure boundary, so the testing stage
    # can be marginal; best_clean provides the conservative slo_max_rps counterpart.
    testing_passed = bool(testing is not None and not testing.get("failed_thresholds"))
    converged = bool(testing is not None and best_clean is not None)
    verdict = "pass" if converged else "fail"

    summary = {
        "verdict": verdict,
        "envoy_cpu": envoy_cpu,
        "ext_proc_cpu": ext_proc_cpu,
        "actors": actors,
        "client_concurrency": client_concurrency,
        "tail_latency_slo_ms": tail_latency_slo_ms,
        "slo_max_rps": (
            best_clean.get(_builtin_key("achieved-rps")) if best_clean else None
        ),
        "slo_max_stage": best_clean["stage"] if best_clean else None,
        "converged": converged,
        "testing_passed": testing_passed,
        "adjusting_stages": sum(1 for r in rows if r.get("stage") != "testing"),
    }

    if testing:
        for name in ("attempted-rps", "achieved-rps", "send-rate", "success-rate"):
            summary[_builtin_key(name)] = testing.get(_builtin_key(name))
        for name in ("p50_ms", "p95_ms", "p99_ms", "latency_mean_plus_2stdev_ms", "max_actor_mean_plus_2stdev_ms"):
            if name in testing:
                summary[name] = testing.get(name)

    # Resource saturation gating
    res = stage_resources or {}
    envoy_util = res.get("envoy_cpu_util")
    ext_proc_util = res.get("ext_proc_cpu_util")
    worker_util = res.get("worker_max_cpu_util")
    origin_util = res.get("origin_max_cpu_util")

    summary["envoy_cpu_util"] = envoy_util
    summary["ext_proc_cpu_util"] = ext_proc_util
    summary["worker_max_cpu_util"] = worker_util
    summary["origin_max_cpu_util"] = origin_util

    # Saturated if gateway (envoy or ext-proc) >= 0.80 while non-gateway < 0.80
    max_gw = max(filter(lambda x: x is not None, [envoy_util, ext_proc_util]), default=0.0)
    max_non_gw = max(filter(lambda x: x is not None, [worker_util, origin_util]), default=0.0)

    gateway_saturated = bool(max_gw >= 0.80 and max_non_gw < 0.80)
    summary["gateway_saturated"] = gateway_saturated

    # Bottleneck diagnosis
    if worker_util is not None and worker_util >= 0.80:
        bottleneck = "worker"
    elif origin_util is not None and origin_util >= 0.80:
        bottleneck = "origin"
    elif envoy_util is not None and ext_proc_util is not None and envoy_util >= 0.80 and ext_proc_util >= 0.80:
        bottleneck = "envoy+ext-proc"
    elif envoy_util is not None and envoy_util >= 0.80:
        bottleneck = "envoy"
    elif ext_proc_util is not None and ext_proc_util >= 0.80:
        bottleneck = "ext-proc"
    else:
        bottleneck = "none"

    summary["bottleneck_component"] = bottleneck
    return summary


def write_jsonl(records: list[dict], path) -> None:
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
