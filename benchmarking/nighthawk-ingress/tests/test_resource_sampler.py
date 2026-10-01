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

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import kubeapi  # noqa: E402
import resource_sampler as rs  # noqa: E402

POD = 'namespace="ate-system",pod="atenet-router-x"'


def cadvisor(t_ms: int, envoy_cpu: float, periods: float, throttled: float, rx: float) -> str:
    return "\n".join(
        [
            "# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed",
            "# TYPE container_cpu_usage_seconds_total counter",
            f'container_cpu_usage_seconds_total{{container="envoy",cpu="total",id="/k/e",image="i",name="e",{POD}}} {envoy_cpu} {t_ms}',
            f'container_cpu_usage_seconds_total{{container="atenet-router",cpu="total",id="/k/a",image="i",name="a",{POD}}} {envoy_cpu / 10} {t_ms}',
            # Pod rollup: dropped, else it double counts.
            f'container_cpu_usage_seconds_total{{container="",cpu="total",id="/k/pod.slice",image="",name="",{POD}}} {envoy_cpu * 2} {t_ms}',
            f'container_memory_working_set_bytes{{container="envoy",id="/k/e",image="i",name="e",{POD}}} 1.5e+08 {t_ms}',
            f'container_memory_working_set_bytes{{container="",id="/k/pod.slice",image="",name="",{POD}}} 9e+08 {t_ms}',
            f'container_memory_rss{{container="envoy",id="/k/e",image="i",name="e",{POD}}} 1.2e+08 {t_ms}',
            f'container_cpu_cfs_periods_total{{container="envoy",id="/k/e",image="i",name="e",{POD}}} {periods} {t_ms}',
            f'container_cpu_cfs_throttled_periods_total{{container="envoy",id="/k/e",image="i",name="e",{POD}}} {throttled} {t_ms}',
            # Pod-level network, two interfaces summed.
            f'container_network_receive_bytes_total{{container="",id="/k/pod.slice",image="",interface="eth0",name="",{POD}}} {rx} {t_ms}',
            f'container_network_receive_bytes_total{{container="",id="/k/pod.slice",image="",interface="lo",name="",{POD}}} {rx} {t_ms}',
            # Pause container: its CPU is dropped; the same eth0 counter
            # under another id must not count twice.
            f'container_cpu_usage_seconds_total{{container="",cpu="total",id="/k/pause",image="pause",name="p",{POD}}} {envoy_cpu} {t_ms}',
            f'container_network_receive_bytes_total{{container="",id="/k/pause",image="pause",interface="eth0",name="p",{POD}}} {rx} {t_ms}',
            # spec_* rows carry no timestamp.
            f'container_spec_memory_limit_bytes{{container="envoy",id="/k/e",image="i",name="e",{POD}}} 0',
            # Not in the keep-list.
            f'container_memory_cache{{container="envoy",id="/k/e",image="i",name="e",{POD}}} 5 {t_ms}',
            # Not a target pod.
            f'container_cpu_usage_seconds_total{{container="envoy",cpu="total",id="/o",image="i",name="o",namespace="ate-system",pod="other"}} 1 {t_ms}',
            "",
        ]
    )


class FakeClient(kubeapi.KubeClient):
    def __init__(self, pages: list[str]):
        self.pages = pages
        self.paths: list[str] = []

    def list_pods(self, namespace, selector):
        if namespace != "ate-system":
            return []
        return [
            {
                "metadata": {"name": "atenet-router-x"},
                "spec": {"nodeName": "node-a"},
                "status": {"phase": "Running"},
            },
            {
                "metadata": {"name": "atenet-router-old"},
                "spec": {"nodeName": "node-b"},
                "status": {"phase": "Succeeded"},
            },
        ]

    def get_raw(self, path, timeout_s=20):
        self.paths.append(path)
        return self.pages.pop(0)


def by_key(samples):
    return {s.container: s for s in samples}


class ParseTest(unittest.TestCase):
    def test_keep_list(self):
        names = {n for n, _, _, _ in rs.parse_cadvisor(cadvisor(1000, 1, 1, 0, 1))}
        self.assertNotIn("container_memory_cache", names)
        self.assertIn("container_cpu_cfs_periods_total", names)

    def test_timestamp_ms(self):
        _, _, v, ts = next(iter(rs.parse_cadvisor(cadvisor(1500, 2, 1, 0, 1))))
        self.assertEqual((v, ts), (2.0, 1.5))

    def test_container_of(self):
        tests = [
            ("named", "container_cpu_usage_seconds_total", {"container": "envoy"}, "envoy"),
            ("pod rollup cpu", "container_cpu_usage_seconds_total", {"container": "", "name": ""}, None),
            ("pod network", "container_network_receive_bytes_total", {"container": "", "name": ""}, "POD"),
            ("pause network", "container_network_receive_bytes_total", {"container": "", "name": "p"}, "POD"),
            ("pause cpu", "container_cpu_usage_seconds_total", {"container": "", "name": "p"}, None),
        ]
        for name, metric, labels, want in tests:
            with self.subTest(name):
                got = rs.container_of(metric, labels)
                self.assertEqual(got, want, f"container_of = {got}, want {want}")

    def test_parse_target(self):
        got = rs.parse_target("router=ate-system:app=atenet-router:envoy,atenet-router")
        want = rs.Target("router", "ate-system", "app=atenet-router", ("envoy", "atenet-router"))
        self.assertEqual(got, want)
        with self.assertRaises(ValueError):
            rs.parse_target("router")


class TargetsTest(unittest.TestCase):
    def test_default_targets(self):
        tests = [("no hostname", "", ""), ("own pod", "runner-x-abc", "runner-x-abc")]
        for name, own, want in tests:
            with self.subTest(name):
                got = {t.component: t.pod for t in rs.default_targets(own)}
                self.assertEqual(got, {"router": "", "runner": want, "workers": ""})

    def test_resolve_own_pod_only(self):
        class Runners(FakeClient):
            def list_pods(self, namespace, selector):
                if namespace != "benchmarking":
                    return []
                return [
                    {"metadata": {"name": n}, "spec": {"nodeName": "node-" + n}, "status": {"phase": "Running"}}
                    for n in ("runner-mine", "runner-other")
                ]

        s = rs.Sampler(Runners([]), rs.default_targets("runner-mine"), log=lambda m: None)
        got = sorted(pod for _, pod in s.resolve())
        self.assertEqual(got, ["runner-mine"])


class RateTest(unittest.TestCase):
    def test_rate(self):
        tests = [
            ("first sight", [(10, 1.0)], None),
            ("rate", [(10, 1.0), (20, 6.0)], (2.0, 5.0)),
            ("counter reset", [(10, 1.0), (5, 6.0)], None),
            ("same timestamp", [(10, 1.0), (20, 1.0)], None),
        ]
        for name, points, want in tests:
            with self.subTest(name):
                t = rs.RateTracker()
                got = None
                for v, ts in points:
                    got = t.rate(("k",), v, ts)
                self.assertEqual(got, want, f"rate = {got}, want {want}")


class SamplerTest(unittest.TestCase):
    def test_two_polls(self):
        client = FakeClient(
            [cadvisor(1_000_000, 100.0, 1000, 10, 1e6), cadvisor(1_005_000, 108.0, 1050, 20, 2e6)]
        )
        clock = iter([1000.2, 1005.2])
        s = rs.Sampler(client, rs.DEFAULT_TARGETS, 5, clock=lambda: next(clock), log=lambda m: None)

        # First poll: gauges only, so only envoy's working set appears.
        self.assertEqual(s.poll_once(), 1)
        first = by_key(s.samples)
        self.assertEqual(set(first), {"envoy"})
        self.assertNotIn("cpu_cores", first["envoy"].values)
        self.assertEqual(first["envoy"].values["working_set_bytes"], 1.5e8)
        self.assertEqual(first["envoy"].values["rss_bytes"], 1.2e8)

        s.samples.clear()
        s.poll_once()
        second = by_key(s.samples)
        self.assertEqual(client.paths, ["/api/v1/nodes/node-a/proxy/metrics/cadvisor"] * 2)
        env = second["envoy"].values
        self.assertAlmostEqual(env["cpu_cores"], 1.6)
        self.assertAlmostEqual(env["cfs_throttled_ratio"], 0.2)
        self.assertEqual(second["envoy"].window_s, 5.0)
        self.assertAlmostEqual(second["atenet-router"].values["cpu_cores"], 0.16)
        # eth0 + lo; eth0 on two rows counts once.
        self.assertAlmostEqual(second["POD"].values["network_receive_bytes_rate"], 4e5)
        self.assertNotIn("working_set_bytes", second["POD"].values)
        self.assertEqual(second["envoy"].component, "router")

    def test_unrefreshed_series_skipped(self):
        page = cadvisor(1_000_000, 100.0, 1000, 10, 1e6)
        clock = iter([1000.0, 1005.0])
        s = rs.Sampler(FakeClient([page, page]), rs.DEFAULT_TARGETS, 5, clock=lambda: next(clock), log=lambda m: None)
        s.poll_once()
        got = s.poll_once()
        self.assertEqual(got, 0, f"second poll of an unchanged page added {got} samples, want 0")

    def test_node_error_counted(self):
        class Boom(FakeClient):
            def get_raw(self, path, timeout_s=20):
                raise RuntimeError("forbidden")

        s = rs.Sampler(Boom([]), rs.DEFAULT_TARGETS, 5, log=lambda m: None)
        with self.assertRaises(rs.PollFailed):
            s.poll_once()
        self.assertEqual(s.errors, 1)

    def test_loop_stops_after_consecutive_failures(self):
        class Boom(FakeClient):
            def get_raw(self, path, timeout_s=20):
                raise RuntimeError("HTTP Error 403: Forbidden")

        logs = []
        s = rs.Sampler(Boom([]), rs.DEFAULT_TARGETS, 0.001, log=logs.append, max_failed_polls=3)
        s.start()
        s._thread.join(timeout=5)
        self.assertFalse(s.running)
        self.assertEqual(s.errors, 3)
        self.assertEqual(len(logs), 2, logs)
        self.assertIn("poll failed", logs[0])
        self.assertIn("3 polls failed in a row, stopping", logs[1])

    def test_next_deadline_resyncs_after_slow_poll(self):
        now = [100.0]
        s = rs.Sampler(FakeClient([]), rs.DEFAULT_TARGETS, 2, clock=lambda: now[0], log=lambda m: None)
        self.assertEqual(s.next_deadline(100.0), 102.0)
        now[0] = 110.0  # the poll took 10 s
        self.assertEqual(s.next_deadline(100.0), 110.0)

    def test_snapshot_is_a_copy(self):
        client = FakeClient([cadvisor(1_000_000, 100.0, 1000, 10, 1e6)])
        s = rs.Sampler(client, rs.DEFAULT_TARGETS, 5, clock=lambda: 1000.0, log=lambda m: None)
        s.poll_once()
        snap = s.snapshot()
        self.assertEqual(len(snap), 1)
        self.assertIsNot(snap, s.samples)

    def test_write_outputs(self):
        client = FakeClient(
            [cadvisor(1_000_000, 100.0, 1000, 10, 1e6), cadvisor(1_005_000, 108.0, 1050, 20, 2e6)]
        )
        clock = iter([1000.0, 1005.0])
        s = rs.Sampler(client, rs.DEFAULT_TARGETS, 5, clock=lambda: next(clock), log=lambda m: None)
        s.poll_once()
        s.poll_once()
        stats = {
            "timestamp": "1970-01-01T00:16:42Z",
            "tag": "abcdef0123",
            "test_name": "t",
            "metric": "adj_000",
            "measurements": {
                "end_time": "1970-01-01T00:16:44Z",
                "per_worker_rps": 10,
                "concurrency": 16,
            },
        }
        with tempfile.TemporaryDirectory() as d:
            sp = Path(d) / "stats.jsonl"
            sp.write_text(json.dumps(stats) + "\n")
            paths = rs.write_outputs(s.samples, Path(d) / "out", sp, 2.0)
            self.assertEqual([p.name for p in paths], ["resources.jsonl", "resources-by-stage.json"])
            recs = [json.loads(l) for l in paths[0].read_text().splitlines()]
            self.assertEqual(recs[0]["tag"], "abcdef0123")
            by = json.loads(paths[1].read_text())
            env = by["stages"][0]["containers"]["router/envoy@atenet-router-x"]
            self.assertAlmostEqual(env["cpu_cores"]["mean"], 1.6)
            self.assertAlmostEqual(env["cpu_fraction_of_pin"]["mean"], 0.8)

    def test_write_outputs_without_stats(self):
        with tempfile.TemporaryDirectory() as d:
            paths = rs.write_outputs([], Path(d), None, None)
            self.assertEqual([p.name for p in paths], ["resources.jsonl"])


if __name__ == "__main__":
    unittest.main()
