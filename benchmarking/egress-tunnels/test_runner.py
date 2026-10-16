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

"""Unit tests for runner.py: python3 -m pytest benchmarking/egress-tunnels"""

import argparse
import unittest
from datetime import datetime, timezone
from pathlib import Path

import runner


class JudgeTest(unittest.TestCase):
    def test_meets_threshold(self):
        v = runner.judge({"loop": {"requests": 1000, "successes": 999}}, 0.999)
        self.assertTrue(v["ok"])
        self.assertAlmostEqual(v["successRate"], 0.999)

    def test_below_threshold(self):
        self.assertFalse(runner.judge({"loop": {"requests": 1000, "successes": 998}}, 0.999)["ok"])

    def test_no_loop_fails(self):
        v = runner.judge({"create": {"succeeded": 0}}, 0.5)
        self.assertFalse(v["ok"])
        self.assertEqual(v["requests"], 0)

    def test_interrupted_fails(self):
        v = runner.judge({"interrupted": True, "loop": {"requests": 10, "successes": 10}}, 0.5)
        self.assertFalse(v["ok"])


class ArgsTest(unittest.TestCase):
    def args(self, *extra):
        return runner.parse_args([
            "--name", "n", "--tag", "t", "--dest", "/d",
            "--actors", "100", "--parallel", "50", "--endpoints", "10", "--duration", "5m",
            *extra,
        ])

    def test_run_args_carry_every_knob_and_the_in_cluster_addresses(self):
        cmd = runner.run_args(self.args("--scheme", "https"), Path("/tmp/r.json"))
        self.assertEqual(cmd[:2], [runner.DEFAULT_DRIVER, "run"])
        for flag, want in {
            "--api-endpoint": runner.DEFAULT_API_ENDPOINT,
            "--api-token-file": runner.DEFAULT_TOKEN_FILE,
            "--router-url": runner.DEFAULT_ROUTER_URL,
            "--actors": "100", "--parallel": "50", "--endpoints": "10",
            "--duration": "5m", "--scheme": "https", "--output": "/tmp/r.json",
        }.items():
            self.assertEqual(cmd[cmd.index(flag) + 1], want, flag)

    def test_extra_args_pass_through(self):
        cmd = runner.run_args(self.args("--create-concurrency", "8"), Path("/r"))
        self.assertEqual(cmd[-2:], ["--create-concurrency", "8"])

    def test_cleanup_targets_every_created_actor(self):
        cmd = runner.cleanup_args(self.args())
        self.assertEqual(cmd[1], "cleanup")
        self.assertEqual(cmd[cmd.index("--actors") + 1], "100")
        self.assertNotIn("--router-url", cmd)


class PrefixTest(unittest.TestCase):
    def test_hive_layout(self):
        now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(
            runner.run_prefix("gs://b/p/", "smoke", "abc", now),
            f"gs://b/p/runs/smoke/run_date=2026-10-14/run_ts={int(now.timestamp())}/run_tag=abc",
        )


if __name__ == "__main__":
    unittest.main()
