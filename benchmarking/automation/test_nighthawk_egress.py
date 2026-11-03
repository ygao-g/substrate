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

"""Unit tests for testtypes/nighthawk_egress.py:
PYTHONPATH=benchmarking/automation pytest benchmarking/automation/test_nighthawk_egress.py"""

import json
import os
import unittest
from unittest import mock

import yaml

import orchestrator
from testtypes import nighthawk_egress


class ValidationTest(unittest.TestCase):
    def valid_test(self, **overrides):
        nh = {
            "mode": "http",
            "connMode": "keepalive",
            "envoyCpu": 2,
            "extProcCpu": 2,
            "actors": 4,
        }
        nh.update(overrides.pop("nighthawk_overrides", {}))
        test = {
            "name": "egress_test",
            "type": "nighthawk-egress",
            "targetCluster": "dev",
            "duration": "30m",
            "workerCount": 4,
            "nighthawk-egress": nh,
        }
        test.update(overrides)
        return test

    def test_valid_http_and_https_modes(self):
        t1 = self.valid_test()
        nighthawk_egress.validate(t1)

        t2 = self.valid_test(nighthawk_overrides={"mode": "https-mitm", "connMode": "churn"})
        nighthawk_egress.validate(t2)

    def test_missing_duration(self):
        t = self.valid_test()
        del t["duration"]
        with self.assertRaisesRegex(ValueError, "missing 'duration'"):
            nighthawk_egress.validate(t)

    def test_unknown_knobs(self):
        t = self.valid_test(nighthawk_overrides={"bogusKnob": 123})
        with self.assertRaisesRegex(ValueError, "unknown knob"):
            nighthawk_egress.validate(t)

    def test_invalid_mode(self):
        for bad in ("grpc", "tls-passthrough", ""):
            with self.subTest(mode=bad):
                t = self.valid_test(nighthawk_overrides={"mode": bad})
                with self.assertRaisesRegex(ValueError, "mode must be one of"):
                    nighthawk_egress.validate(t)

    def test_invalid_conn_mode(self):
        for bad in ("streaming", "persistent", ""):
            with self.subTest(connMode=bad):
                t = self.valid_test(nighthawk_overrides={"connMode": bad})
                with self.assertRaisesRegex(ValueError, "connMode must be one of"):
                    nighthawk_egress.validate(t)

    def test_invalid_cpu_knobs(self):
        for knob in ("envoyCpu", "extProcCpu"):
            for bad in (0, -1, "2", None):
                with self.subTest(knob=knob, bad=bad):
                    t = self.valid_test(nighthawk_overrides={knob: bad})
                    with self.assertRaisesRegex(ValueError, f"needs {nighthawk_egress.TEST_TYPE}.{knob}"):
                        nighthawk_egress.validate(t)

    def test_worker_count_and_actors(self):
        t = self.valid_test(workerCount=2, nighthawk_overrides={"actors": 4})
        with self.assertRaisesRegex(ValueError, "workerCount .* must be >= actors"):
            nighthawk_egress.validate(t)

        t2 = self.valid_test()
        del t2["workerCount"]
        with self.assertRaisesRegex(ValueError, "needs workerCount"):
            nighthawk_egress.validate(t2)

    def test_invalid_durations(self):
        for field in ("measuringPeriod", "testingStageDuration", "convergenceDeadline"):
            t = self.valid_test(nighthawk_overrides={field: "invalid"})
            with self.assertRaises(ValueError):
                nighthawk_egress.validate(t)

    def test_invalid_dns_labels(self):
        t = self.valid_test(nighthawk_overrides={"atespace": "BAD_NAME!"})
        with self.assertRaisesRegex(ValueError, "must be a DNS label"):
            nighthawk_egress.validate(t)


class WorkloadTemplatesTest(unittest.TestCase):
    def test_workload_templates(self):
        test = {"nighthawk-egress": {}}
        self.assertEqual(nighthawk_egress.workload_templates(test), ["traffic-driver"])


class PreTestTest(unittest.TestCase):
    @mock.patch("testtypes.nighthawk_egress.run")
    @mock.patch("testtypes.nighthawk_egress.pin_deployment")
    def test_pre_test_http_mode(self, mock_pin, mock_run):
        test = {
            "nighthawk-egress": {
                "mode": "http",
                "envoyCpu": 2,
                "extProcCpu": 2,
            }
        }
        nighthawk_egress.pre_test(test)

        # 1. Namespace applied
        calls = [c.args[0] for c in mock_run.call_args_list]
        self.assertTrue(
            any(
                c[:3] == ["kubectl", "apply", "-f"] and c[3].endswith("benchmarking/nighthawk-egress/namespace.yaml")
                for c in calls
            )
        )

        # 2. Server rollout status checked
        self.assertIn(
            ["kubectl", "-n", "nighthawk", "rollout", "status", "deployment/nighthawk-server", "--timeout=180s"],
            calls,
        )

        # 3. Deployment pinned with replicas=1, single rollout
        mock_pin.assert_called_once()
        name, ns, patch = mock_pin.call_args.args[:3]
        self.assertEqual(name, "atenet-egress")
        self.assertEqual(ns, "ate-system")
        self.assertEqual(patch["spec"]["replicas"], 1)
        containers = patch["spec"]["template"]["spec"]["containers"]
        envoy = next(c for c in containers if c["name"] == "envoy")
        ext_proc = next(c for c in containers if c["name"] == "ext-proc")
        self.assertEqual(envoy["resources"]["requests"]["cpu"], "2")
        self.assertIn("--concurrency", envoy["args"])
        self.assertEqual(ext_proc["resources"]["requests"]["cpu"], "2")

        # No separate scale and no resource-sampler-rbac applied in pre_test
        calls = [c.args[0] for c in mock_run.call_args_list]
        self.assertFalse(any("scale" in c for c in calls))
        self.assertFalse(any("resource-sampler-rbac" in str(c) for c in calls))

    @mock.patch("testtypes.nighthawk_egress.run")
    @mock.patch("testtypes.nighthawk_egress.pin_deployment")
    def test_pre_test_https_mitm_mode(self, mock_pin, mock_run):
        def fake_run(cmd, *args, **kwargs):
            if "get" in cmd and "atenet-egress" in cmd:
                return mock.Mock(stdout="envoyproxy/envoy:test-image\n")
            return mock.Mock()

        mock_run.side_effect = fake_run

        test = {
            "nighthawk-egress": {
                "mode": "https-mitm",
                "envoyCpu": 4,
                "extProcCpu": 2,
            }
        }
        nighthawk_egress.pre_test(test)

        # Single pin_deployment call with merged patch
        mock_pin.assert_called_once()
        name, ns, patch = mock_pin.call_args.args[:3]
        self.assertEqual(name, "atenet-egress")
        self.assertEqual(ns, "ate-system")
        self.assertEqual(patch["spec"]["replicas"], 1)
        pod_spec = patch["spec"]["template"]["spec"]
        self.assertTrue(any(c["name"] == "egress-upstream-roots" for c in pod_spec.get("initContainers", [])))
        self.assertTrue(any(v["name"] == "egress-upstream-roots" for v in pod_spec.get("volumes", [])))
        envoy = next(c for c in pod_spec["containers"] if c["name"] == "envoy")
        self.assertTrue(any(vm["name"] == "egress-upstream-roots" for vm in envoy.get("volumeMounts", [])))
        self.assertEqual(envoy["resources"]["requests"]["cpu"], "4")

        # No separate scale or rollout commands
        calls = [c.args[0] for c in mock_run.call_args_list]
        self.assertFalse(any("scale" in c for c in calls))
        self.assertFalse(any("resource-sampler-rbac" in str(c) for c in calls))

    def test_merge_patch_preserves_extra_fields(self):
        base = {
            "spec": {
                "replicas": 1,
                "template": {
                    "spec": {
                        "containers": [
                            {"name": "envoy", "resources": {"limits": {"cpu": "2"}}},
                            {"name": "ext-proc", "resources": {"limits": {"cpu": "2"}}},
                        ]
                    }
                }
            }
        }
        extra = {
            "spec": {
                "template": {
                    "metadata": {"annotations": {"sidecar.istio.io/inject": "false"}},
                    "spec": {
                        "initContainers": [{"name": "init-1"}],
                        "containers": [
                            {"name": "envoy", "volumeMounts": [{"name": "v1", "mountPath": "/p"}]}
                        ],
                        "nodeSelector": {"disktype": "ssd"},
                    }
                }
            }
        }
        merged = nighthawk_egress._merge_patch(base, extra)
        self.assertEqual(merged["spec"]["replicas"], 1)
        self.assertEqual(
            merged["spec"]["template"]["metadata"]["annotations"],
            {"sidecar.istio.io/inject": "false"},
        )
        self.assertEqual(
            merged["spec"]["template"]["spec"]["nodeSelector"],
            {"disktype": "ssd"},
        )
        self.assertEqual(
            merged["spec"]["template"]["spec"]["initContainers"],
            [{"name": "init-1"}],
        )
        envoy = next(c for c in merged["spec"]["template"]["spec"]["containers"] if c["name"] == "envoy")
        ext_proc = next(c for c in merged["spec"]["template"]["spec"]["containers"] if c["name"] == "ext-proc")
        self.assertEqual(envoy["resources"]["limits"]["cpu"], "2")
        self.assertEqual(envoy["volumeMounts"], [{"name": "v1", "mountPath": "/p"}])
        self.assertEqual(ext_proc["resources"]["limits"]["cpu"], "2")


class PostTestTest(unittest.TestCase):
    @mock.patch("testtypes.nighthawk_egress.run_no_check")
    def test_post_test(self, mock_run_no_check):
        test = {"nighthawk-egress": {"mode": "http"}}
        nighthawk_egress.post_test(test)

        calls = [c.args[0] for c in mock_run_no_check.call_args_list]
        self.assertIn(
            ["kubectl", "delete", "clusterrole,clusterrolebinding", "benchmark-runner-resource-sampler", "--ignore-not-found"],
            calls,
        )
        self.assertIn(
            ["kubectl", "delete", "namespace", "nighthawk", "--ignore-not-found"],
            calls,
        )
        # Should not patch atenet-egress or wait for rollout in post_test
        self.assertFalse(any("patch" in c and "atenet-egress" in c for c in calls))
        self.assertFalse(any("rollout" in c for c in calls))


class JobSubsAndTemplateTest(unittest.TestCase):
    HERE = os.path.dirname(__file__)

    def test_job_subs_and_render_template(self):
        test = {
            "name": "egress_test",
            "workerCount": 4,
            "duration": "30m",
            "nighthawk-egress": {
                "mode": "https-mitm",
                "connMode": "churn",
                "envoyCpu": 2,
                "extProcCpu": 2,
                "actors": 4,
                "clientConcurrency": 4,
                "connections": 1000,
                "maxPendingRequests": 1000,
                "initialTotalRps": 1000,
                "exponentialFactor": 2.0,
                "measuringPeriod": "20s",
                "testingStageDuration": "60s",
                "convergenceDeadline": "600s",
                "successRateThreshold": 0.998,
                "sendRateThreshold": 0.9,
                "tailLatencySloMs": 50.0,
            },
        }
        subs = {
            "JOB_NAME": "runner-egress-test-123456",
            "IMAGE": "gcr.io/test/runner:tag",
            "TAG": "tag123",
            "NAME": test["name"],
            "DEST": "gs://bucket/results",
        }
        subs.update(nighthawk_egress.job_subs(test))
        self.assertEqual(subs["MODE"], "https-mitm")
        self.assertEqual(subs["CONN_MODE"], "churn")
        self.assertEqual(subs["MEASURING_PERIOD_S"], 20)
        self.assertEqual(subs["TESTING_STAGE_DURATION_S"], 60)
        self.assertEqual(subs["CONVERGENCE_DEADLINE_S"], 600)
        self.assertEqual(subs["RUNNER_CPU"], 2)
        self.assertEqual(subs["ATE_ATEAPI_CLIENT_AUTH"], "cert")

        tmpl_path = nighthawk_egress.job_tmpl(os.path.join(self.HERE, "manifests"))
        rendered = orchestrator.render_template(tmpl_path, subs)
        self.assertNotIn("${", rendered)

        docs = list(yaml.safe_load_all(rendered))
        job = next(d for d in docs if d and d.get("kind") == "Job")
        container = job["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["image"], "gcr.io/test/runner:tag")
        self.assertEqual(container["resources"]["requests"]["cpu"], "2")
        self.assertIn("--mode", container["args"])
        self.assertIn("https-mitm", container["args"])
        self.assertIn("--conn-mode", container["args"])
        self.assertIn("churn", container["args"])

        # Embedded resource-sampler RBAC objects
        cluster_roles = [d for d in docs if d and d.get("kind") == "ClusterRole"]
        self.assertTrue(any(cr["metadata"]["name"] == "benchmark-runner-resource-sampler" for cr in cluster_roles))
        cluster_role_bindings = [d for d in docs if d and d.get("kind") == "ClusterRoleBinding"]
        self.assertTrue(any(crb["metadata"]["name"] == "benchmark-runner-resource-sampler" for crb in cluster_role_bindings))
        roles = [d for d in docs if d and d.get("kind") == "Role" and d["metadata"]["name"] == "benchmark-runner-resource-sampler"]
        role_namespaces = {r["metadata"]["namespace"] for r in roles}
        self.assertEqual(role_namespaces, {"ate-system", "benchmarking", "benchmark-workloads", "nighthawk"})


class TestsYamlTest(unittest.TestCase):
    def test_tests_yaml_contains_valid_nighthawk_egress_entries(self):
        tests_path = os.path.join(os.path.dirname(__file__), "tests.yaml")
        raw = yaml.safe_load(open(tests_path).read())
        tests = raw.get("tests", [])
        egress_tests = [t for t in tests if t.get("type") == "nighthawk-egress"]
        self.assertGreaterEqual(len(egress_tests), 3)
        modes = {t["nighthawk-egress"]["mode"] for t in egress_tests}
        self.assertEqual(modes, {"http", "https-mitm"})
        conn_modes = {t["nighthawk-egress"]["connMode"] for t in egress_tests}
        self.assertEqual(conn_modes, {"keepalive", "churn"})

        # Validate that all tests in tests.yaml pass validation
        orchestrator.validate_and_normalize_tests(tests)


if __name__ == "__main__":
    unittest.main()
