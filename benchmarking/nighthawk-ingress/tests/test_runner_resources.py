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

"""runner.py's --sample-resources hook, without gRPC or a cluster."""

import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# actors imports generated ateapi protos; the hook does not touch it.
if "actors" not in sys.modules:
    try:
        import actors  # noqa: F401
    except ImportError:
        sys.modules["actors"] = types.SimpleNamespace(ATESPACE="default")

import runner  # noqa: E402

BASE = ["runner.py", "--name", "n", "--tag", "t", "--dest", "gs://b", "--envoy-cpu", "2"]


class ArgsTest(unittest.TestCase):
    def test_default_off(self):
        with mock.patch.object(sys, "argv", BASE):
            args = runner.parse_args()
        self.assertFalse(args.sample_resources)
        self.assertEqual(args.sample_interval, 2)

    def test_on(self):
        with mock.patch.object(sys, "argv", BASE + ["--sample-resources", "--sample-interval", "0.5"]):
            args = runner.parse_args()
        self.assertTrue(args.sample_resources)
        self.assertEqual(args.sample_interval, 0.5)


class HookTest(unittest.TestCase):
    def test_start_stop_snapshots_and_never_raises(self):
        args = types.SimpleNamespace(tag="t", name="n", sample_interval=0.01)
        labels = []

        def append(client, path, label, *a, **k):
            labels.append(label)
            raise RuntimeError("admin port unreachable")

        class Client:
            def list_pods(self, ns, sel):
                return []

        logs = []
        with mock.patch.object(runner.kubeapi, "default_client", return_value=Client()), mock.patch.object(
            runner.envoy_stats, "append", side_effect=append
        ):
            s = runner.start_resource_sampling(args, Path("/nonexistent/e.jsonl"), logs.append)
            runner.stop_resource_sampling(s, args, Path("/nonexistent/e.jsonl"), logs.append)
            s.stop()  # the finally block stops again
        self.assertEqual(labels, ["before", "after"])
        self.assertEqual(sum("snapshot failed" in m for m in logs), 2, logs)
        self.assertEqual(s.errors, 0)

    def test_start_returns_none_when_thread_start_fails(self):
        args = types.SimpleNamespace(tag="t", name="n", sample_interval=0.01)
        logs = []
        with mock.patch.object(runner.kubeapi, "default_client", return_value=object()), mock.patch.object(
            runner.envoy_stats, "append"
        ), mock.patch.object(runner.resource_sampler.Sampler, "start", side_effect=RuntimeError("can't start new thread")):
            s = runner.start_resource_sampling(args, Path("/nonexistent/e.jsonl"), logs.append)
        self.assertIsNone(s)
        self.assertEqual(sum("disabled" in m for m in logs), 1, logs)

    def test_start_returns_none_when_no_client(self):
        args = types.SimpleNamespace(tag="t", name="n", sample_interval=0.01)
        logs = []
        with mock.patch.object(
            runner.kubeapi, "default_client", side_effect=RuntimeError("no ca.crt")
        ):
            s = runner.start_resource_sampling(args, Path("/nonexistent/e.jsonl"), logs.append)
        self.assertIsNone(s)
        self.assertEqual(sum("disabled" in m for m in logs), 1, logs)


if __name__ == "__main__":
    unittest.main()
