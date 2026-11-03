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

"""The `type: nighthawk-egress` hooks (see the package docstring for the
lifecycle).

Measures atenet-egress capacity across HTTP (:80) and HTTPS MITM (:443):
pre_test deploys nighthawk-server, pins atenet-egress (envoy + ext-proc CPU,
concurrency), applies the servicedns upstream trust patch for https-mitm mode,
and runs the adaptive search runner Job against it.
"""

import os
from pathlib import Path
import re
from typing import Any

import yaml

from util import (
    build_and_push,
    parse_duration_seconds,
    pin_deployment,
    run,
    run_no_check,
)

TEST_TYPE = "nighthawk-egress"
WORKLOAD_TEMPLATE = "traffic-driver"

MODES = ("http", "https-mitm")
CONN_MODES = ("keepalive", "churn")

DEFAULTS = {
    "mode": "http",
    "connMode": "keepalive",
    "actors": 4,
    "clientConcurrency": 4,
    "connections": None,
    "maxPendingRequests": 1000,
    "initialTotalRps": 1000,
    "exponentialFactor": 2.0,
    "measuringPeriod": "20s",
    "testingStageDuration": "60s",
    "convergenceDeadline": "600s",
    "successRateThreshold": 0.998,
    "sendRateThreshold": 0.9,
    "tailLatencySloMs": 25.0,
    "atespace": "benchmark-workloads",
}


def config(test: dict[str, Any]) -> dict[str, Any]:
    """The test's `nighthawk-egress:` block merged over DEFAULTS."""
    merged = {**DEFAULTS, **test.get(TEST_TYPE, {})}
    if merged.get("connections") is None:
        merged["connections"] = 32 if merged.get("connMode") == "keepalive" else 250
    return merged


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "nighthawk-egress-runner-job.yaml.tmpl")


def validate(test: dict[str, Any]) -> None:
    """Validate a nighthawk-egress test configuration.
    Raises ValueError on malformed entries."""
    name = test.get("name")
    if "duration" not in test:
        raise ValueError(f"{TEST_TYPE} test {name!r} missing 'duration'")
    parse_duration_seconds(str(test["duration"]))

    allowed = set(DEFAULTS) | {"envoyCpu", "extProcCpu"}
    unknown = set(test.get(TEST_TYPE, {})) - allowed
    if unknown:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} has unknown knob(s) {sorted(unknown)}; "
            f"allowed: {sorted(allowed)}"
        )

    nh = config(test)
    envoy_cpu = nh.get("envoyCpu")
    if not isinstance(envoy_cpu, int) or envoy_cpu < 1:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} needs {TEST_TYPE}.envoyCpu (int >= 1)"
        )
    ext_proc_cpu = nh.get("extProcCpu", envoy_cpu)
    if not isinstance(ext_proc_cpu, int) or ext_proc_cpu < 1:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} needs {TEST_TYPE}.extProcCpu (int >= 1)"
        )

    mode = nh.get("mode")
    if mode not in MODES:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} mode must be one of {MODES}, got {mode!r}"
        )
    conn_mode = nh.get("connMode")
    if conn_mode not in CONN_MODES:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} connMode must be one of {CONN_MODES}, got {conn_mode!r}"
        )

    for field in ("actors", "clientConcurrency", "connections", "maxPendingRequests", "initialTotalRps"):
        val = nh.get(field)
        if not isinstance(val, int) or val < 1:
            raise ValueError(
                f"{TEST_TYPE} test {name!r} needs {field} (int >= 1), got {val!r}"
            )

    exp_factor = nh.get("exponentialFactor")
    if not isinstance(exp_factor, (int, float)) or exp_factor <= 1.0:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} exponentialFactor must be > 1.0, got {exp_factor!r}"
        )

    rate = nh.get("successRateThreshold")
    if not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} successRateThreshold must be in [0, 1], got {rate!r}"
        )
    send_rate = nh.get("sendRateThreshold")
    if not isinstance(send_rate, (int, float)) or not 0 <= send_rate <= 1:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} sendRateThreshold must be in [0, 1], got {send_rate!r}"
        )

    slo = nh.get("tailLatencySloMs")
    if not isinstance(slo, (int, float)) or slo < 0:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} tailLatencySloMs must be >= 0, got {slo!r}"
        )

    worker_count = test.get("workerCount")
    if not isinstance(worker_count, int) or worker_count < 1:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} needs workerCount (int >= 1): the "
            f"benchmark warms actors across ready workers"
        )
    if worker_count < nh["actors"]:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} workerCount ({worker_count}) must be >= "
            f"actors ({nh['actors']})"
        )

    for field in ("measuringPeriod", "convergenceDeadline", "testingStageDuration"):
        parse_duration_seconds(str(nh[field]))

    atespace = str(nh["atespace"])
    if not re.fullmatch(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?", atespace):
        raise ValueError(
            f"{TEST_TYPE} test {name!r} atespace {atespace!r} must be a DNS label"
        )


def build_image(commit: str) -> str:
    """Build & push the nighthawk-egress capacity-benchmark runner image."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/nighthawk-egress-test:{commit}",
        "benchmarking/nighthawk-egress/Dockerfile",
    )


def _repo_path(rel: str) -> Path:
    """Resolve rel against the current working directory, falling back to
    the repository root so tests running from benchmarking/automation/ find
    benchmarking/nighthawk-egress assets."""
    p = Path(rel)
    if p.exists():
        return p
    root = Path(__file__).resolve().parents[3]
    candidate = root / rel
    if candidate.exists():
        return candidate
    return p


def workload_templates(test: dict[str, Any]) -> list[str]:
    """The actor templates the workloads deploy must add."""
    return [WORKLOAD_TEMPLATE]


def _merge_patch(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge extra into base.
    Dicts are merged key-by-key.
    Lists of dicts with 'name' (such as containers) are merged element-by-element matching on 'name'.
    Other lists are replaced by extra.
    """
    merged = dict(base)
    for k, v in extra.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _merge_patch(merged[k], v)
        elif k in merged and isinstance(merged[k], list) and isinstance(v, list):
            base_by_name = {
                item["name"]: item
                for item in merged[k]
                if isinstance(item, dict) and "name" in item
            }
            if base_by_name:
                out_list = []
                seen_names = set()
                for item in v:
                    if isinstance(item, dict) and "name" in item and item["name"] in base_by_name:
                        out_list.append(_merge_patch(base_by_name[item["name"]], item))
                        seen_names.add(item["name"])
                    else:
                        out_list.append(item)
                for item in merged[k]:
                    if not (isinstance(item, dict) and "name" in item and item["name"] in seen_names):
                        out_list.append(item)
                merged[k] = out_list
            else:
                merged[k] = list(v)
        else:
            merged[k] = v
    return merged


def pre_test(test: dict[str, Any]) -> None:
    """Shape the cluster for nighthawk-egress: deploy nighthawk-server,
    pin atenet-egress (replicas, envoy + ext-proc CPU, concurrency), and apply
    servicedns upstream trust patch for https-mitm mode in a single rollout."""
    cfg = config(test)
    envoy_cpu = cfg["envoyCpu"]
    ext_proc_cpu = cfg.get("extProcCpu", envoy_cpu)
    mode = cfg["mode"]

    # 1. Deploy nighthawk namespace and nighthawk-server
    run(["kubectl", "apply", "-f", str(_repo_path("benchmarking/nighthawk-egress/namespace.yaml"))])
    server_replicas = max(envoy_cpu, 2)
    server_text = _repo_path("benchmarking/nighthawk-egress/nighthawk-server.yaml").read_text()
    server_text = re.sub(r"replicas:\s*1", f"replicas: {server_replicas}", server_text, count=1)
    run(["kubectl", "apply", "-f", "-"], input=server_text, text=True)
    run(["kubectl", "-n", "nighthawk", "rollout", "status", "deployment/nighthawk-server", "--timeout=180s"])

    # 2. Pin atenet-egress with replicas=1, CPU/concurrency, and optional trust patch
    envoy_container: dict[str, Any] = {
        "name": "envoy",
        "resources": {
            "requests": {"cpu": str(envoy_cpu), "memory": "4Gi"},
            "limits": {"cpu": str(envoy_cpu), "memory": "4Gi"},
        },
        "args": [
            "-c",
            "/etc/envoy/envoy.yaml",
            "--service-node",
            "atenet-egress",
            "--service-cluster",
            "atenet-egress",
            "--concurrency",
            str(envoy_cpu),
        ],
    }
    ext_proc_container: dict[str, Any] = {
        "name": "ext-proc",
        "resources": {
            "requests": {"cpu": str(ext_proc_cpu)},
            "limits": {"cpu": str(ext_proc_cpu)},
        },
    }
    patch: dict[str, Any] = {
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [envoy_container, ext_proc_container],
                },
            },
        },
    }

    if mode == "https-mitm":
        res = run(
            [
                "kubectl",
                "-n",
                "ate-system",
                "get",
                "deployment",
                "atenet-egress",
                "-o",
                'jsonpath={.spec.template.spec.containers[?(@.name=="envoy")].image}',
            ],
            capture_output=True,
            text=True,
        )
        envoy_image = res.stdout.strip()
        trust_tmpl = _repo_path("benchmarking/nighthawk-egress/manifests/gateway-trust-patch.yaml.tmpl").read_text()
        trust_patch = yaml.safe_load(trust_tmpl.replace("${ENVOY_IMAGE}", envoy_image))
        patch = _merge_patch(patch, trust_patch)

    pin_deployment("atenet-egress", "ate-system", patch)


def post_test(test: dict[str, Any]) -> None:
    """Teardown created resources outside substrate's own lifecycle:
    clean up resource-sampler RBAC and delete nighthawk namespace."""
    run_no_check(
        [
            "kubectl",
            "delete",
            "clusterrole,clusterrolebinding",
            "benchmark-runner-resource-sampler",
            "--ignore-not-found",
        ]
    )
    run_no_check(["kubectl", "delete", "namespace", "nighthawk", "--ignore-not-found"])


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    """Template substitutions specific to the nighthawk-egress runner Job."""
    c = config(test)
    envoy_cpu = c["envoyCpu"]
    ext_proc_cpu = c.get("extProcCpu", envoy_cpu)
    measuring_s = parse_duration_seconds(str(c["measuringPeriod"]))
    testing_s = parse_duration_seconds(str(c["testingStageDuration"]))
    deadline_s = parse_duration_seconds(str(c["convergenceDeadline"]))
    return {
        "MODE": c["mode"],
        "CONN_MODE": c["connMode"],
        "ENVOY_CPU": envoy_cpu,
        "EXT_PROC_CPU": ext_proc_cpu,
        "ACTORS": c["actors"],
        "ATESPACE": c["atespace"],
        "CLIENT_CONCURRENCY": c["clientConcurrency"],
        "CONNECTIONS": c["connections"],
        "MAX_PENDING": c["maxPendingRequests"],
        "INITIAL_TOTAL_RPS": c["initialTotalRps"],
        "EXP_FACTOR": c["exponentialFactor"],
        "MEASURING_PERIOD_S": measuring_s,
        "TESTING_STAGE_DURATION_S": testing_s,
        "CONVERGENCE_DEADLINE_S": deadline_s,
        "SUCCESS_RATE": c["successRateThreshold"],
        "SEND_RATE": c["sendRateThreshold"],
        "TAIL_LATENCY_SLO_MS": c["tailLatencySloMs"],
        "RUNNER_CPU": 2,
        "OUT_DIR": "/tmp/nighthawk-egress-out",
        "ATE_ATEAPI_CLIENT_AUTH": "cert",
    }
