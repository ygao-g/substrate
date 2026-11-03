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
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import envoy_stats as es  # noqa: E402
import kubeapi  # noqa: E402

STATS = """\
cluster.ate-cluster.upstream_cx_total: 94
cluster.ate-cluster.upstream_rq_total: 500
http.ingress_http.downstream_rq_total: 1243
http.ingress_http.downstream_rq_2xx: 1200
http.ingress_http.downstream_rq_time: P0(nan,1) P25(nan,2)
listener.0.0.0.0_8080.downstream_cx_total: 1243
listener.0.0.0.0_8080.worker_3.downstream_cx_active: 2
server.memory_allocated: 27543312
server.worker_0.watchdog_miss: 0
server.uptime: 100
runtime.load_success: 1
"""


class FakeClient(kubeapi.KubeClient):
    def __init__(self):
        self.paths = []

    def get_raw(self, path, timeout_s=20):
        self.paths.append(path)
        if "endpointslices" in path:
            return json.dumps(
                {
                    "items": [
                        {
                            "endpoints": [
                                {"addresses": ["10.0.0.5"], "targetRef": {"name": "atenet-router-a"}},
                                {"addresses": ["fd00::7"], "targetRef": {"name": "atenet-router-b"}},
                            ]
                        },
                        # Dual-stack: the same pod in a second slice.
                        {"endpoints": [{"addresses": ["fd00::5"], "targetRef": {"name": "atenet-router-a"}}]},
                    ]
                }
            )
        if path.startswith("/api/v1/namespaces/ate-system/pods?"):
            return json.dumps(
                {
                    "items": [
                        {"metadata": {"name": "atenet-router-a"}, "status": {"phase": "Running"}},
                        {"metadata": {"name": "atenet-router-z"}, "status": {"phase": "Pending"}},
                    ]
                }
            )
        if path.endswith(":9901/proxy/stats"):
            return STATS
        raise AssertionError(f"unexpected path {path}")


class ParseTest(unittest.TestCase):
    def test_keep(self):
        got = es.parse_stats(STATS)
        want = {
            "cluster.ate-cluster.upstream_cx_total": 94.0,
            "http.ingress_http.downstream_rq_total": 1243.0,
            "http.ingress_http.downstream_rq_2xx": 1200.0,
            "listener.0.0.0.0_8080.downstream_cx_total": 1243.0,
            "listener.0.0.0.0_8080.worker_3.downstream_cx_active": 2.0,
            "server.memory_allocated": 27543312.0,
            "server.worker_0.watchdog_miss": 0.0,
        }
        self.assertEqual(got, want)

    def test_admin_url(self):
        tests = [("v4", "10.0.0.5", "http://10.0.0.5:9901/stats"), ("v6", "fd00::7", "http://[fd00::7]:9901/stats")]
        for name, addr, want in tests:
            with self.subTest(name):
                got = es.admin_url(addr)
                self.assertEqual(got, want, f"admin_url({addr}) = {got}, want {want}")


class SnapshotTest(unittest.TestCase):
    def test_direct(self):
        urls = []

        def get(url):
            urls.append(url)
            if "fd00" in url:
                raise OSError("connection refused")
            return STATS

        snaps = es.snapshot(FakeClient(), "direct", get)
        self.assertEqual(urls, ["http://10.0.0.5:9901/stats", "http://[fd00::7]:9901/stats"])
        self.assertEqual([p for p, _, _ in snaps], ["atenet-router-a", "atenet-router-b"])
        self.assertEqual(snaps[0][1]["server.memory_allocated"], 27543312.0)
        self.assertIsNone(snaps[1][1])
        self.assertIn("refused", snaps[1][2])

    def test_proxy(self):
        c = FakeClient()
        snaps = es.snapshot(c, "proxy")
        self.assertEqual([p for p, _, _ in snaps], ["atenet-router-a"])
        self.assertIn("/api/v1/namespaces/ate-system/pods/atenet-router-a:9901/proxy/stats", c.paths)

    def test_append(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "envoy-stats.jsonl"
            now = lambda: datetime(2026, 9, 30, 1, 2, 3, tzinfo=timezone.utc)  # noqa: E731
            es.append(FakeClient(), out, "before", "proxy", "abc", "t", now=now)
            es.append(FakeClient(), out, "after", "proxy", "abc", "t", now=now)
            recs = [json.loads(l) for l in out.read_text().splitlines()]
        self.assertEqual([r["metric"] for r in recs], ["envoy-stats/before", "envoy-stats/after"])
        r = recs[0]
        self.assertEqual(r["timestamp"], "2026-09-30T01:02:03.000Z")
        self.assertEqual((r["tag"], r["test_name"]), ("abc", "t"))
        self.assertEqual(r["measurements"]["pod"], "atenet-router-a")
        self.assertIsNone(r["measurements"]["error"])


if __name__ == "__main__":
    unittest.main()
