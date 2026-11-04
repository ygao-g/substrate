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

"""Unit tests for testtypes/atunnel.py:
python3 benchmarking/automation/test_atunnel.py"""

import os
import unittest
from unittest import mock

import yaml

import orchestrator
from testtypes import atunnel


def entry(**fields):
    base = {
        "name": "atunnel_1cpu_1g",
        "type": "atunnel",
        "targetCluster": "dev",
        "duration": "15m",
        "atunnel": {
            "tunnelCpus": 1,
            "tunnelMemory": "1g",
            "actors": 10,
            "bodyBytes": 16,
            "rigCpu": 4,
        },
    }
    base.update(fields)
    return base


class ValidateTest(unittest.TestCase):
    def test_valid_defaults_and_overrides(self):
        atunnel.validate(entry(atunnel={}))
        for cpus in (1, 2, 0.5, 1.5):
            with self.subTest(tunnelCpus=cpus):
                atunnel.validate(entry(atunnel={"tunnelCpus": cpus}))
        for mem in ("512m", "512M", "1g", "2G", "1024k", "1024K", "1073741824"):
            with self.subTest(tunnelMemory=mem):
                atunnel.validate(entry(atunnel={"tunnelMemory": mem}))

    def test_missing_duration(self):
        test = entry()
        del test["duration"]
        with self.assertRaisesRegex(ValueError, "missing 'duration'"):
            atunnel.validate(test)

    def test_unknown_knob(self):
        with self.assertRaisesRegex(ValueError, "unknown knob"):
            atunnel.validate(entry(atunnel={"bogus": 1}))

    def test_invalid_knobs(self):
        cases = [
            ({"tunnelMemory": "1.5g"}, "tunnelMemory"),
            ({"tunnelMemory": "1Gi"}, "tunnelMemory"),
            ({"tunnelMemory": "0g"}, "tunnelMemory"),
            ({"actors": 0}, "actors"),
            ({"actors": True}, "actors"),
            ({"rigCpu": 0}, "rigCpu"),
            ({"rigCpu": False}, "rigCpu"),
            ({"bodyBytes": -1}, "bodyBytes"),
            ({"bodyBytes": True}, "bodyBytes"),
            ({"tunnelCpus": 0}, "tunnelCpus"),
            ({"tunnelCpus": -1}, "tunnelCpus"),
            ({"tunnelCpus": True}, "tunnelCpus"),
        ]
        for block, needle in cases:
            with self.subTest(block=block):
                with self.assertRaisesRegex(ValueError, needle):
                    atunnel.validate(entry(atunnel=block))


class BuildImageTest(unittest.TestCase):
    def test_build_image_uses_atunnel_bench_test_tag(self):
        with mock.patch.dict(os.environ, {"KO_DOCKER_REPO": "gcr.io/p"}), \
             mock.patch.object(atunnel, "build_and_push", return_value="gcr.io/p/atunnel-bench-test:abc") as bp:
            img = atunnel.build_image("abc")
        self.assertEqual(img, "gcr.io/p/atunnel-bench-test:abc")
        bp.assert_called_once_with(
            "gcr.io/p/atunnel-bench-test:abc",
            "benchmarking/atunnel/Dockerfile",
        )


class RenderTest(unittest.TestCase):
    HERE = os.path.dirname(__file__)

    def render_docs(self, test):
        subs = {
            "JOB_NAME": "runner-atunnel-1cpu-1g-ac41c06-123456",
            "NAME": test["name"],
            "TAG": "ac41c06deadbeef",
            "IMAGE": "gcr.io/p/atunnel-bench-test:ac41c06deadbeef",
            "DEST": "gs://bucket/prefix",
            **atunnel.job_subs(test),
        }
        tmpl = atunnel.job_tmpl(os.path.join(self.HERE, "manifests"))
        text = orchestrator.render_template(tmpl, subs, test.get("flags", []))
        self.assertNotIn("${", text)
        return [d for d in yaml.safe_load_all(text) if d]

    def test_job_subs_values(self):
        subs = atunnel.job_subs(
            entry(
                duration="15m",
                atunnel={
                    "tunnelCpus": 1.5,
                    "tunnelMemory": "1g",
                    "actors": 10,
                    "bodyBytes": 16,
                    "rigCpu": 4,
                },
            )
        )
        self.assertEqual(subs["TUNNEL_CPUS"], 1.5)
        self.assertEqual(subs["TUNNEL_MEMORY"], "1g")
        self.assertEqual(subs["ACTORS"], 10)
        self.assertEqual(subs["BODY_BYTES"], 16)
        self.assertEqual(subs["RIG_CPU"], 4)
        self.assertEqual(subs["WORKER_CPU"], 2.5)
        # Valid Kubernetes memory quantity (integer bytes or binary/decimal SI).
        self.assertRegex(
            str(subs["WORKER_MEMORY"]),
            r"^\d+([EPTGMK]i?)?$",
        )
        self.assertEqual(int(subs["WORKER_MEMORY"]), 2 * (1 << 30))
        self.assertEqual(subs["ACTIVE_DEADLINE"], 15 * 60 + 600)

    def test_rendered_manifest_contract(self):
        test = entry(flags=["--scenarios", "IngressH1", "--rungs", "1000"])
        docs = self.render_docs(test)

        # Self-contained benchmark: no RoleBinding into ate-system, and a
        # pods/log Role + RoleBinding in benchmarking.
        for d in docs:
            if d.get("kind") == "RoleBinding":
                self.assertNotEqual(
                    d.get("metadata", {}).get("namespace"), "ate-system"
                )
        roles = [d for d in docs if d.get("kind") == "Role"]
        self.assertEqual(len(roles), 1)
        self.assertEqual(roles[0]["metadata"]["namespace"], "benchmarking")
        self.assertEqual(
            roles[0]["rules"],
            [{"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]}],
        )

        jobs = [d for d in docs if d.get("kind") == "Job"]
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job["spec"]["activeDeadlineSeconds"], 1500)

        tmpl = job["spec"]["template"]
        self.assertEqual(
            tmpl["metadata"]["labels"].get("app"),
            "substrate-benchmark-runner",
        )
        self.assertEqual(
            tmpl["metadata"]["annotations"].get(
                "kubectl.kubernetes.io/default-container"
            ),
            "runner",
        )

        spec = tmpl["spec"]
        self.assertEqual(
            spec["hostAliases"],
            [{"ip": "127.0.0.1", "hostnames": ["atunnel-worker", "atunnel-sink"]}],
        )
        self.assertEqual(
            spec["securityContext"]["sysctls"],
            [
                {"name": "net.ipv4.ip_unprivileged_port_start", "value": "0"},
                {"name": "net.ipv4.ip_local_port_range", "value": "1024 65535"},
                {
                    "name": "net.ipv4.ip_local_reserved_ports",
                    "value": "8443,9090,9091,9443,15001",
                },
            ],
        )

        self.assertEqual(
            [c["name"] for c in spec["initContainers"]],
            ["pki", "sink", "worker"],
        )
        pki, sink, worker = spec["initContainers"]
        self.assertEqual(pki["securityContext"]["runAsUser"], 65532)
        self.assertEqual(sink["restartPolicy"], "Always")
        self.assertEqual(sink["securityContext"]["runAsUser"], 65532)
        self.assertEqual(
            sink["startupProbe"]["httpGet"], {"path": "/healthz", "port": 9091}
        )
        self.assertNotIn("limits", sink["resources"])

        self.assertEqual(worker["restartPolicy"], "Always")
        wsec = worker["securityContext"]
        self.assertFalse(wsec["privileged"])
        self.assertFalse(wsec["allowPrivilegeEscalation"])
        self.assertEqual(wsec["appArmorProfile"], {"type": "Unconfined"})
        self.assertEqual(wsec["seccompProfile"], {"type": "Unconfined"})
        self.assertEqual(wsec["capabilities"]["drop"], ["ALL"])
        self.assertEqual(
            worker["startupProbe"]["httpGet"], {"path": "/healthz", "port": 9090}
        )
        self.assertNotIn("limits", worker["resources"])

        for container in [*spec["initContainers"], *spec["containers"]]:
            for field in ("command", "args"):
                for item in container.get(field, []):
                    self.assertIsInstance(
                        item,
                        str,
                        f"container {container['name']!r} {field} has non-str {item!r}",
                    )

        runner = spec["containers"][0]
        self.assertNotIn("limits", runner["resources"])
        self.assertEqual(
            runner["args"],
            [
                "--name",
                "atunnel_1cpu_1g",
                "--tag",
                "ac41c06deadbeef",
                "--dest",
                "gs://bucket/prefix",
                "--tunnel-cpus",
                "1",
                "--tunnel-memory",
                "1g",
                "--actors",
                "10",
                "--scenarios",
                "IngressH1",
                "--rungs",
                "1000",
            ],
        )


if __name__ == "__main__":
    unittest.main()
