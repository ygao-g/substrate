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

"""Unit tests for resources_common.py and fetch_run_resources.py.

The HTTP layer is replaced with a fake that answers by URL and query
substring, in the shapes the Monitoring v3 and GMP PromQL APIs return."""

import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fetch_run_resources as frr
import resources_common as rc

TAG = "12cf471c287a4d5c674f6139b5b8be67a570b38e"
NAME = "ingress_routercap_envoy_2cpu"


def stats_line(metric: str, start: str, end: str, per_worker_rps: int, failed=()) -> str:
    return json.dumps(
        {
            "timestamp": start,
            "tag": TAG,
            "test_name": NAME,
            "metric": metric,
            "measurements": {
                "end_time": end,
                "duration_s": 10.0,
                "per_worker_rps": per_worker_rps,
                "concurrency": 16,
                "failed_thresholds": list(failed),
            },
        }
    )


STATS = [
    stats_line("nighthawk_adjusting_001", "2026-09-30T17:06:00.000Z", "2026-09-30T17:06:10.000Z", 250),
    stats_line("nighthawk_adjusting_000", "2026-09-30T17:05:30.5Z", "2026-09-30T17:05:40.123456789Z", 125),
    stats_line(
        "nighthawk_adjusting_002",
        "2026-09-30T17:07:00.000Z",
        "2026-09-30T17:07:10.000Z",
        500,
        failed=["latency-ns-mean-plus-2stdev"],
    ),
]


def t(s: str) -> datetime:
    return rc.parse_time(s)


class ParseTest(unittest.TestCase):
    def test_parse_time(self):
        tests = [
            ("Z seconds", "2026-09-30T17:05:38Z", datetime(2026, 9, 30, 17, 5, 38, tzinfo=timezone.utc)),
            ("millis", "2026-09-30T17:05:38.423Z", datetime(2026, 9, 30, 17, 5, 38, 423000, tzinfo=timezone.utc)),
            ("nanos", "2026-09-30T17:05:38.123456789Z", datetime(2026, 9, 30, 17, 5, 38, 123456, tzinfo=timezone.utc)),
            ("offset", "2026-09-30T19:05:38+02:00", datetime(2026, 9, 30, 17, 5, 38, tzinfo=timezone.utc)),
        ]
        for name, in_, want in tests:
            with self.subTest(name):
                got = rc.parse_time(in_)
                self.assertEqual(got, want, f"parse_time({in_!r}) = {got}, want {want}")

    def test_stages_from_stats(self):
        stages, tag, name = rc.stages_from_stats(STATS + [""])
        got = [(s.name, s.total_rps, s.failed_thresholds) for s in stages]
        want = [
            ("nighthawk_adjusting_000", 2000.0, []),
            ("nighthawk_adjusting_001", 4000.0, []),
            ("nighthawk_adjusting_002", 8000.0, ["latency-ns-mean-plus-2stdev"]),
        ]
        self.assertEqual(got, want)
        self.assertEqual((tag, name), (TAG, NAME))

    def test_stages_skip_missing_timestamp(self):
        logs = []
        lines = STATS + [json.dumps({"metric": "broken", "measurements": {}})]
        stages, _, _ = rc.stages_from_stats(lines, log=logs.append)
        self.assertEqual(len(stages), len(STATS))
        self.assertEqual(logs, ["stats.jsonl: skipped 1 record(s) without a timestamp"])

    def test_stage_end_from_duration(self):
        line = json.dumps(
            {"timestamp": "2026-09-30T17:00:00Z", "metric": "m", "measurements": {"duration_s": 60}}
        )
        stages, _, _ = rc.stages_from_stats([line])
        got, want = stages[0].end, t("2026-09-30T17:01:00Z")
        self.assertEqual(got, want)


class AlignmentTest(unittest.TestCase):
    def setUp(self):
        self.stages, _, _ = rc.stages_from_stats(STATS)

    def sample(self, ts: str, window: float, **values) -> rc.Sample:
        return rc.Sample(t(ts), window, "router", "atenet-router-x", "envoy", "test", values)

    def test_overlapping_stages(self):
        tests = [
            ("inside one stage", "2026-09-30T17:06:05Z", 5, ["nighthawk_adjusting_001"]),
            ("between stages", "2026-09-30T17:06:30Z", 5, []),
            ("window spans two", "2026-09-30T17:07:05Z", 60, ["nighthawk_adjusting_001", "nighthawk_adjusting_002"]),
            ("ends exactly at start", "2026-09-30T17:06:00Z", 5, []),
            ("starts exactly at end", "2026-09-30T17:06:15Z", 5, []),
        ]
        for name, ts, window, want in tests:
            with self.subTest(name):
                got = [s.name for s in rc.overlapping_stages(self.sample(ts, window), self.stages)]
                self.assertEqual(got, want)

    def test_percentile(self):
        tests = [
            ("single", [3.0], 95, 3.0),
            ("interpolated", [1.0, 2.0, 3.0, 4.0, 5.0], 95, 4.8),
            ("median even", [1.0, 2.0, 3.0, 4.0], 50, 2.5),
            ("unsorted", [5.0, 1.0, 3.0], 0, 1.0),
        ]
        for name, vals, q, want in tests:
            with self.subTest(name):
                got = rc.percentile(vals, q)
                self.assertAlmostEqual(got, want, msg=f"percentile({vals}, {q}) = {got}, want {want}")

    def test_by_stage_pin_fraction_and_nulls(self):
        samples = [
            self.sample("2026-09-30T17:07:05Z", 5, cpu_cores=1.0, working_set_bytes=100.0),
            self.sample("2026-09-30T17:07:10Z", 5, cpu_cores=2.0, working_set_bytes=None),
            rc.Sample(t("2026-09-30T17:07:05Z"), 5, "runner", "runner-p", "runner", "test", {"cpu_cores": 4.0}),
        ]
        rows = rc.by_stage(samples, self.stages, envoy_cpu=2, series=("cpu_cores", "working_set_bytes", "route_rps"))
        got_names = [r["stage"] for r in rows]
        self.assertEqual(got_names, [s.name for s in self.stages])
        self.assertEqual(rows[0]["containers"], {}, "stage with no samples should have no containers")

        c = rows[2]["containers"]
        env = c["router/envoy@atenet-router-x"]
        self.assertEqual(env["cpu_cores"], {"mean": 1.5, "max": 2.0, "p95": 1.95, "n": 2})
        self.assertEqual(env["cpu_fraction_of_pin"], {"mean": 0.75, "max": 1.0, "p95": 0.975, "n": 2})
        self.assertEqual(env["working_set_bytes"], {"mean": 100.0, "max": 100.0, "p95": 100.0, "n": 1})
        self.assertNotIn("route_rps", env, "absent optional series should be omitted")

        run = c["runner/runner@runner-p"]
        self.assertIsNone(run["cpu_fraction_of_pin"], "runner is not pinned")
        self.assertIsNone(run["working_set_bytes"], "absent primary series should be null")

    def test_sample_records_envelope(self):
        recs = rc.sample_records(
            [self.sample("2026-09-30T17:07:05Z", 5, cpu_cores=1.0)], self.stages, TAG, NAME
        )
        want = {
            "timestamp": "2026-09-30T17:07:05.000Z",
            "tag": TAG,
            "test_name": NAME,
            "metric": "resources/router/envoy",
            "measurements": {
                "component": "router",
                "pod": "atenet-router-x",
                "container": "envoy",
                "source": "test",
                "window_s": 5,
                "stages": ["nighthawk_adjusting_002"],
                "cpu_cores": 1.0,
            },
        }
        self.assertEqual(recs, [want])

    def test_missing_containers(self):
        samples = [self.sample("2026-09-30T17:07:05Z", 5, cpu_cores=1.0)]
        got = rc.missing_containers(samples, [("router", "envoy"), ("router", "atenet-router")])
        self.assertEqual(got, ["router/atenet-router"])


class SelectorTest(unittest.TestCase):
    def test_tag_sha7(self):
        for tag, want in [
            (TAG, "12cf471"),
            ("quick-12cf471-205858", "12cf471"),
            ("quick-12cf471-dirty-205858", "12cf471"),
            ("12cf471", "12cf471"),
        ]:
            with self.subTest(tag=tag):
                self.assertEqual(frr.tag_sha7(tag), want)

    def test_atelet_controller_regex(self):
        pat = re.compile(frr.atelet_controller_regex("quick-12cf471-205858"))
        for name in ["atelet-12cf471", "atelet-12cf471c", "atelet-v0-2-0-54-g12cf471", "atelet-v0-2-0-54-g12cf471-dirty"]:
            self.assertIsNotNone(pat.fullmatch(name), name)
        for name in ["atelet-1240749", "atelet-quick-1", "atelet-12cf471-x"]:
            self.assertIsNone(pat.fullmatch(name), name)

    def test_runner_prefix_override(self):
        sel = frr.component_selectors(NAME, TAG, "ns", "ateom", "runner-custom-")
        self.assertEqual([s["pod_prefix"] for s in sel if s["component"] == "runner"], ["runner-custom-"])

    def test_runner_pod_prefix(self):
        self.assertEqual(frr.runner_pod_prefix(NAME, "quick-12cf471-205858"), frr.runner_pod_prefix(NAME, TAG))
        got = frr.runner_pod_prefix("ingress_routercap_envoy_2cpu", TAG)
        want = "runner-ingress-routercap-envoy-2cpu-12cf471-"
        self.assertEqual(got, want)

    def test_k8s_filter(self):
        sel = frr.component_selectors(NAME, TAG, "benchmark-workloads", "ateom")[0]
        got = frr.k8s_filter("kubernetes.io/container/cpu/core_usage_time", "", "c1", sel)
        for want in (
            'metric.type="kubernetes.io/container/cpu/core_usage_time"',
            'resource.labels.cluster_name="c1"',
            'resource.labels.namespace_name="ate-system"',
            'resource.labels.container_name=one_of("envoy","atenet-router")',
            'resource.labels.pod_name=starts_with("atenet-router-")',
        ):
            self.assertIn(want, got)

    def test_workers_filter_has_no_pod_clause(self):
        sel = frr.component_selectors(NAME, TAG, "benchmark-workloads", "ateom")[2]
        got = frr.k8s_filter("m", "", "c1", sel)
        self.assertNotIn("pod_name", got)

    def test_select_router_instances(self):
        atelet_info = {"a1": {"pod": "atelet-12cf471-x", "node": "n1"}}
        router_info = {
            "r1": {"pod": "atenet-router-abc", "node": "n1"},
            "r2": {"pod": "atenet-router-def", "node": "n9"},
            "e1": {"pod": "atenet-egress-ghi", "node": "n1"},
        }
        tests = [
            ("mapped by node", {"a1"}, atelet_info, router_info, {"r1"}),
            ("no atelet: every router, no egress", set(), atelet_info, router_info, {"r1", "r2"}),
            ("no target_info", set(), {}, {}, None),
        ]
        for name, inst, ai, ri, want in tests:
            with self.subTest(name):
                got = frr.select_router_instances(inst, ai, ri)
                self.assertEqual(got, want)


class FakeHttp:
    """Answers by (URL suffix, substring of the query or filter)."""

    def __init__(self, routes: list[tuple[str, str, dict]]):
        self.routes = routes
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, url: str, params: list[tuple[str, str]]) -> dict:
        p = dict(params)
        self.calls.append((url, p))
        text = p.get("query") or p.get("filter") or ""
        for suffix, needle, resp in self.routes:
            if url.endswith(suffix) and needle in text:
                if callable(resp):
                    return resp(p)
                return resp
        if url.endswith("/timeSeries"):
            return {}
        return {"status": "success", "data": {"resultType": "matrix", "result": []}}


def k8s_series(pod: str, container: str, points: list[tuple[str, float]]) -> dict:
    return {
        "resource": {"labels": {"pod_name": pod, "container_name": container}},
        "points": [{"interval": {"endTime": ts}, "value": {"doubleValue": v}} for ts, v in points],
    }


def epoch(s: str) -> float:
    return t(s).timestamp()


class FetchTest(unittest.TestCase):
    def setUp(self):
        self.stages, _, _ = rc.stages_from_stats(STATS)

    def test_k8s_paging_and_merge(self):
        page1 = {
            "timeSeries": [k8s_series("atenet-router-a", "envoy", [("2026-09-30T17:07:10Z", 1.5)])],
            "nextPageToken": "p2",
        }
        page2 = {"timeSeries": [k8s_series("atenet-router-a", "atenet-router", [("2026-09-30T17:07:10Z", 0.4)])]}

        def cpu(p):
            return page2 if p.get("pageToken") == "p2" else page1

        mem = {"timeSeries": [k8s_series("atenet-router-a", "envoy", [("2026-09-30T17:07:10Z", 5e7)])]}
        http = FakeHttp(
            [
                ("/timeSeries", "core_usage_time", cpu),
                ("/timeSeries", "memory/used_bytes", mem),
            ]
        )
        samples, queries = frr.fetch(
            http, "p", "c1", self.stages, TAG, NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=lambda m: None, system_metrics=True,
        )
        router = sorted(
            (s.container, s.values.get("cpu_cores"), s.values.get("working_set_bytes"))
            for s in samples
            if s.component == "router" and s.source == "k8s"
        )
        want = [("atenet-router", 0.4, None), ("envoy", 1.5, 5e7)]
        self.assertEqual(router, want)
        cpu_q = [q for q in queries if q["component"] == "router" and q["value"] == "cpu_cores" and q["source"] == "k8s"]
        self.assertEqual(cpu_q[0]["series"], 2)

    def test_otlp_router_selection(self):
        ts = epoch("2026-09-30T17:07:10Z")
        info_atelet = {
            "status": "success",
            "data": {"result": [{"metric": {"instance": "a1", "k8s.pod.name": "atelet-12cf471-x", "k8s.node.name": "n1"}}]},
        }
        info_router = {
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"instance": "r1", "k8s.pod.name": "atenet-router-ours", "k8s.node.name": "n1"}},
                    {"metric": {"instance": "r2", "k8s.pod.name": "atenet-router-other", "k8s.node.name": "n2"}},
                ]
            },
        }

        def matrix(rows):
            return {"status": "success", "data": {"result": rows}}

        http = FakeHttp(
            [
                ("/query", 'job="atelet"', info_atelet),
                ("/query", 'job="atenet-router"', info_router),
                ("/query_range", "ate.actor.stats.cpu.time", matrix([{"metric": {"instance": "a1"}, "values": [[ts, "0.7"]]}])),
                (
                    "/query_range",
                    "route.duration_count",
                    matrix(
                        [
                            {"metric": {"instance": "r1"}, "values": [[ts, "5000"]]},
                            {"metric": {"instance": "r2"}, "values": [[ts, "9999"]]},
                        ]
                    ),
                ),
            ]
        )
        samples, _ = frr.fetch(
            http, "p", "c1", self.stages, TAG, NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=lambda m: None,
        )
        rps = [(s.pod, s.values["route_rps"]) for s in samples if "route_rps" in s.values]
        self.assertEqual(rps, [("atenet-router-ours", 5000.0)])
        actors = [(s.pod, s.values["cpu_cores"]) for s in samples if s.component == "actors"]
        self.assertEqual(actors, [("atelet-12cf471-x", 0.7)])

    def test_atelet_override_and_zero_series_warnings(self):
        http = FakeHttp([])
        logs = []
        frr.fetch(
            http, "p", "c1", self.stages, "quick-12cf471-205858", NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=logs.append, atelet="atelet-v0-2-0-54-g12cf471",
        )
        queries = [p.get("query", "") for _, p in http.calls]
        self.assertTrue(any('top_level_controller_name="atelet-v0-2-0-54-g12cf471"' in q for q in queries), queries)
        self.assertEqual(sum("no actor series" in m for m in logs), 1, logs)
        self.assertEqual(sum("router rps unfiltered" in m for m in logs), 1, logs)

        logs.clear()
        frr.fetch(
            http, "p", "c1", self.stages, "quick-12cf471-205858", NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=logs.append,
        )
        queries = [p.get("query", "") for _, p in http.calls]
        self.assertTrue(any('top_level_controller_name=~"atelet-(.*-g)?12cf471[0-9a-f]*(-dirty)?"' in q for q in queries), queries)

    def test_system_metrics_off_by_default(self):
        http = FakeHttp([])
        _, queries = frr.fetch(
            http, "p", "c1", self.stages, TAG, NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=lambda m: None,
        )
        got = sorted({q["source"] for q in queries})
        self.assertEqual(got, ["otlp"], f"sources = {got}, want only otlp")
        self.assertFalse(any(u.endswith("/timeSeries") for u, _ in http.calls))

    def test_workers_aggregate(self):
        mk = lambda pod, v: k8s_series(pod, "ateom", [("2026-09-30T17:07:10Z", v)])  # noqa: E731
        cpu = {"timeSeries": [mk("w1", 0.1), mk("w2", 0.3), mk("w3", 0.2)]}
        http = FakeHttp([("/timeSeries", 'namespace_name="benchmark-workloads"', cpu)])
        samples, _ = frr.fetch(
            http, "p", "c1", self.stages, TAG, NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=lambda m: None, system_metrics=True,
        )
        workers = [s for s in samples if s.component == "workers"]
        self.assertEqual(len(workers), 1, "per_pod=False should emit only the aggregate")
        w = workers[0].values
        self.assertEqual((w["pods"], w["cpu_cores_p50"], w["cpu_cores_max"]), (3.0, 0.2, 0.3))

    def test_errors_are_recorded_not_raised(self):
        def boom(p):
            raise RuntimeError("HTTP 403")

        http = FakeHttp([("/timeSeries", "", boom)])
        samples, queries = frr.fetch(
            http, "p", "c1", self.stages, TAG, NAME, "benchmark-workloads", "ateom",
            per_pod=False, log=lambda m: None, system_metrics=True,
        )
        errs = [q for q in queries if q["source"] == "k8s"]
        self.assertTrue(errs and all(str(q["series"]).startswith("error") for q in errs))
        self.assertEqual(samples, [])

    def test_project_and_cluster_from_flags_or_env(self):
        env = {"PROJECT_ID": "env-p", "CLUSTER_NAME": "env-c"}
        with mock.patch.dict(os.environ, env, clear=False):
            a = frr.parse_args(["gs://b/run"])
            self.assertEqual((a.project, a.cluster), ("env-p", "env-c"))
            a = frr.parse_args(["gs://b/run", "--project", "p2", "--cluster", "c2"])
            self.assertEqual((a.project, a.cluster), ("p2", "c2"))
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as cm, mock.patch("sys.stderr"):
                frr.parse_args(["gs://b/run", "--cluster", "c"])
            self.assertEqual(cm.exception.code, 2)

    def test_main_local_run(self):
        with tempfile.TemporaryDirectory() as d:
            run = Path(d) / "run"
            run.mkdir()
            (run / "stats.jsonl").write_text("\n".join(STATS) + "\n")
            (run / "capacity.json").write_text(json.dumps({"envoy_cpu": 2}))
            out = Path(d) / "out"
            orig_token, orig_http = frr.access_token, frr.make_http_get
            frr.access_token = lambda f: "tok"
            frr.make_http_get = lambda tok: FakeHttp(
                [("/timeSeries", "core_usage_time", {"timeSeries": [k8s_series("atenet-router-a", "envoy", [("2026-09-30T17:07:10Z", 1.8)])]})]
            )
            try:
                rc_ = frr.main([
                    str(run), "--out", str(out), "--system-metrics",
                    "--project", "p1", "--cluster", "c1",
                ])
            finally:
                frr.access_token, frr.make_http_get = orig_token, orig_http
            self.assertEqual(rc_, 0)
            by_stage = json.loads((out / "resources-by-stage.json").read_text())
            self.assertEqual(by_stage["envoy_cpu"], 2)
            self.assertIn("router/atenet-router", by_stage["missing"])
            last = by_stage["stages"][-1]["containers"]["router/envoy@atenet-router-a"]
            self.assertAlmostEqual(last["cpu_fraction_of_pin"]["max"], 0.9)
            lines = (out / "resources.jsonl").read_text().splitlines()
            self.assertEqual(json.loads(lines[0])["metric"], "resources/router/envoy")


if __name__ == "__main__":
    unittest.main()
