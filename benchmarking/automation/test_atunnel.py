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

"""Unit tests for testtypes/atunnel.py and its Job template:
python3 -m pytest benchmarking/automation/test_atunnel.py"""

import os
import unittest

import yaml

import orchestrator
from testtypes import atunnel

TMPL = os.path.join(os.path.dirname(__file__), "manifests", "atunnel-runner-job.yaml.tmpl")


def entry(**fields):
    return {"name": "t", "type": "atunnel", "duration": "15m", **fields}


class ValidateTest(unittest.TestCase):
    def test_defaults(self):
        atunnel.validate(entry())

    def test_unknown_knob(self):
        with self.assertRaisesRegex(ValueError, "unknown knob"):
            atunnel.validate(entry(atunnel={"tunnelCpu": 1}))

    def test_missing_duration(self):
        test = entry()
        del test["duration"]
        with self.assertRaisesRegex(ValueError, "duration"):
            atunnel.validate(test)

    def test_bad_values(self):
        for block in (
            {"tunnelMemory": "1.5g"},
            {"tunnelMemory": 1},
            {"tunnelCpus": 0},
            {"tunnelCpus": "1"},
            {"tunnelCpus": float("inf")},
            {"actors": 0},
            {"bodyBytes": -1},
            {"rigCpu": 0},
        ):
            with self.subTest(block=block), self.assertRaises(ValueError):
                atunnel.validate(entry(atunnel=block))

    def test_bad_duration(self):
        with self.assertRaisesRegex(ValueError, "duration"):
            atunnel.validate(entry(duration="15x"))

    def test_empty_block(self):
        atunnel.validate(entry(atunnel=None))
        self.assertEqual(atunnel.job_subs(entry(atunnel=None))["ACTORS"], 10)
        with self.assertRaisesRegex(ValueError, "mapping"):
            atunnel.validate(entry(atunnel=[1]))


class JobSubsTest(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(
            atunnel.job_subs(entry()),
            {
                "TUNNEL_CPUS": 1,
                "TUNNEL_MEMORY": "1g",
                "ACTORS": 10,
                "BODY_BYTES": 16,
                "RIG_CPU": 4,
                "WORKER_CPU": 2,
                "WORKER_MEMORY": "2048Mi",
                "ACTIVE_DEADLINE": 15 * 60 + 600,
            },
        )

    def test_small_tunnel(self):
        subs = atunnel.job_subs(entry(atunnel={"tunnelMemory": "256m", "tunnelCpus": 0.5}))
        self.assertEqual(subs["WORKER_MEMORY"], "1280Mi")
        self.assertEqual(subs["WORKER_CPU"], 1.5)


class RenderTest(unittest.TestCase):
    FLAGS = ["--scenarios", "IngressH1", "--rungs", "1000"]

    def render(self, test=None, flags=()):
        test = test or entry()
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "0123456", "NAME": "n", "DEST": "d"}
        subs.update(atunnel.job_subs(test))
        text = orchestrator.render_template(TMPL, subs, flags)
        self.assertNotIn("${", text)
        return list(yaml.safe_load_all(text))

    def job(self, **kw):
        return next(d for d in self.render(**kw) if d and d.get("kind") == "Job")

    def pod(self, **kw):
        return self.job(**kw)["spec"]["template"]

    def test_every_command_and_arg_is_a_string(self):
        spec = self.pod(flags=self.FLAGS)["spec"]
        for c in spec["initContainers"] + spec["containers"]:
            with self.subTest(container=c["name"]):
                for a in c["command"] + c.get("args", []):
                    self.assertIsInstance(a, str, a)

    def test_flags_follow_the_runners_own_args(self):
        args = self.pod(flags=self.FLAGS)["spec"]["containers"][0]["args"]
        self.assertEqual(args[-4:], self.FLAGS)
        self.assertEqual(args[:2], ["--name", "n"])
        self.assertEqual(args[args.index("--body-bytes") + 1], "16")
        self.assertIn("--tunnel-memory", args)

    def test_worker_sidecar(self):
        spec = self.pod()["spec"]
        worker = next(c for c in spec["initContainers"] if c["name"] == "worker")
        self.assertEqual(worker["restartPolicy"], "Always")
        self.assertIs(worker["securityContext"]["privileged"], False)
        self.assertNotIn("limits", worker["resources"])
        self.assertEqual(worker["resources"]["requests"], {"cpu": "2", "memory": "2048Mi"})
        for c in spec["initContainers"] + spec["containers"]:
            self.assertNotIn("limits", c["resources"], c["name"])

    def test_pod_shape(self):
        job = self.job()
        self.assertEqual(job["spec"]["activeDeadlineSeconds"], 1500)
        pod = job["spec"]["template"]
        self.assertEqual(pod["metadata"]["labels"]["app"], "substrate-benchmark-runner")
        self.assertEqual(
            {s["name"]: s["value"] for s in pod["spec"]["securityContext"]["sysctls"]},
            {
                "net.ipv4.ip_unprivileged_port_start": "0",
                "net.ipv4.ip_local_port_range": "1024 65535",
                "net.ipv4.ip_local_reserved_ports": "8443,9090,9091,9443,15001",
            },
        )
        self.assertEqual(pod["spec"]["hostAliases"], [{"ip": "127.0.0.1", "hostnames": ["atunnel-worker", "atunnel-sink"]}])
        self.assertEqual(pod["spec"]["nodeSelector"], {"ate.dev/benchmarking": "true"})
        self.assertEqual(pod["spec"]["tolerations"][0]["key"], "ate.dev/benchmarking")

    def test_no_substrate_rbac(self):
        for doc in self.render():
            if doc and doc["kind"] == "RoleBinding":
                self.assertNotEqual(doc["metadata"]["namespace"], "ate-system")
        kinds = [d["kind"] for d in self.render() if d]
        self.assertEqual(kinds, ["Namespace", "ServiceAccount", "Role", "RoleBinding", "Job"])


if __name__ == "__main__":
    unittest.main()
