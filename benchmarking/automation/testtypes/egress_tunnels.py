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

"""The `type: egress-tunnels` hooks (see the package docstring for the
lifecycle).

Measures how many egress tunnels the gateway carries and what they cost:
pre_test deploys the endpoint Services the actors call, the workloads deploy
adds the egress actor template to the shared pool, and the runner Job drives
benchmark-egress driver (cmd/benchmarking/egress) against them.
"""

import os
import re
from typing import Any

from util import build_and_push, parse_duration_seconds, run

TEST_TYPE = "egress-tunnels"

# What benchmarking/workloads/deploy.sh must create besides its defaults:
# the actor template the driver resumes actors from.
WORKLOAD_TEMPLATE = "egress"

# The gateway's mitm_internal circuit breakers as shipped in
# manifests/ate-install/atenet-egress.yaml: every open tunnel is one
# connection there, so a cell past this needs a patched gateway.
SHIPPED_TUNNEL_CAP = 16384

# Defaults for the tests.yaml `egress-tunnels:` block. Documented in
# benchmarking/egress-tunnels/README.md, "In CI".
DEFAULTS = {
    # A, B and C of the README: actors created, actors resumed and looping,
    # endpoints each loop calls. B x C is the tunnel count under test.
    "actors": 100,
    "parallel": 100,
    "endpoints": 100,
    "scheme": "http",
    "connMode": "keepalive",
    "requestInterval": "100ms",
    "requestTimeout": "5s",
    # Opens per second the gateway can handshake; the driver refuses a start
    # that asks for more. 0 leaves it unchecked.
    "opensBudget": 0,
    # Where the actors live; the template is the one WORKLOAD_TEMPLATE adds.
    "atespace": "benchmark-workloads",
    "template": WORKLOAD_TEMPLATE,
    # The run fails below this fraction of successful requests.
    "successRateThreshold": 0.999,
    # The runner pod's requests; the driver polls B actors in parallel.
    "runnerCpu": "2",
    "runnerMemory": "2Gi",
}

SCHEMES = ("http", "https")
CONN_MODES = ("keepalive", "new-conn")
MAX_ENDPOINTS = 1024


def config(test: dict[str, Any]) -> dict[str, Any]:
    """The test's `egress-tunnels:` block merged over DEFAULTS."""
    return {**DEFAULTS, **test.get(TEST_TYPE, {})}


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "egress-tunnels-runner-job.yaml.tmpl")


def validate(test: dict[str, Any]) -> None:
    """Raises ValueError on a malformed entry. duration is the loop time,
    so it is required; workerCount is required because B actors need
    somewhere to run."""
    name = test.get("name")
    if "duration" not in test:
        raise ValueError(f"{TEST_TYPE} test {name!r} missing 'duration'")
    parse_duration_seconds(str(test["duration"]))
    unknown = set(test.get(TEST_TYPE, {})) - set(DEFAULTS)
    if unknown:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} has unknown knob(s) {sorted(unknown)}; "
            f"allowed: {sorted(DEFAULTS)}"
        )
    c = config(test)
    for knob in ("actors", "parallel", "endpoints"):
        if not isinstance(c[knob], int) or c[knob] < 1:
            raise ValueError(f"{TEST_TYPE} test {name!r} needs {knob} (int >= 1), got {c[knob]!r}")
    if c["parallel"] > c["actors"]:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} resumes {c['parallel']} actors but creates only {c['actors']}"
        )
    if c["endpoints"] > MAX_ENDPOINTS:
        raise ValueError(f"{TEST_TYPE} test {name!r} endpoints {c['endpoints']} exceeds {MAX_ENDPOINTS}")
    tunnels = c["parallel"] * c["endpoints"]
    if tunnels > SHIPPED_TUNNEL_CAP:
        raise ValueError(
            f"{TEST_TYPE} test {name!r} opens {tunnels} tunnels (parallel x endpoints), past the "
            f"gateway's shipped cap of {SHIPPED_TUNNEL_CAP} per replica; larger cells need the "
            f"gateway sizing knobs, which this type does not have yet"
        )
    if c["scheme"] not in SCHEMES:
        raise ValueError(f"{TEST_TYPE} test {name!r} scheme must be one of {SCHEMES}, got {c['scheme']!r}")
    if c["connMode"] not in CONN_MODES:
        raise ValueError(f"{TEST_TYPE} test {name!r} connMode must be one of {CONN_MODES}, got {c['connMode']!r}")
    for knob in ("requestInterval", "requestTimeout"):
        if not re.fullmatch(r"\d+(ms|s|m)", str(c[knob])):
            raise ValueError(f"{TEST_TYPE} test {name!r} {knob} must be <int>ms|s|m, got {c[knob]!r}")
    if not isinstance(c["opensBudget"], int) or c["opensBudget"] < 0:
        raise ValueError(f"{TEST_TYPE} test {name!r} opensBudget must be an int >= 0, got {c['opensBudget']!r}")
    rate = c["successRateThreshold"]
    if not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
        raise ValueError(f"{TEST_TYPE} test {name!r} successRateThreshold must be in [0, 1], got {rate!r}")
    worker_count = test.get("workerCount")
    if not isinstance(worker_count, int) or worker_count < 1:
        raise ValueError(f"{TEST_TYPE} test {name!r} needs workerCount (int >= 1) for its {c['parallel']} actors")
    for knob in ("atespace", "template"):
        if not re.fullmatch(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?", str(c[knob])):
            raise ValueError(f"{TEST_TYPE} test {name!r} {knob} {c[knob]!r} must be a DNS label")


def build_image(commit: str) -> str:
    """Build & push the runner image: the driver binary plus runner.py."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/egress-tunnels-test:{commit}",
        "benchmarking/egress-tunnels/Dockerfile",
    )


def workload_templates(test: dict[str, Any]) -> list[str]:
    """The actor template the workloads deploy must add."""
    return [config(test)["template"]]


def pre_test(test: dict[str, Any]) -> None:
    """Deploy the endpoint Services and the target behind them, and for
    HTTPS the gateway trust patch. Runs after deploy_substrate, so the
    gateway the patch rolls is this test's."""
    c = config(test)
    cmd = ["benchmarking/egress-tunnels/deploy.sh", "--deploy", "--endpoints", str(c["endpoints"])]
    if c["scheme"] == "https":
        cmd.append("--https")
    run(cmd)


def post_test(test: dict[str, Any]) -> None:
    """Remove the endpoints and the trust patch before substrate goes."""
    run(["benchmarking/egress-tunnels/deploy.sh", "--delete"])


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    """Template substitutions specific to the egress-tunnels runner Job."""
    c = config(test)
    return {
        "ACTORS": c["actors"],
        "PARALLEL": c["parallel"],
        "ENDPOINTS": c["endpoints"],
        "DURATION": test["duration"],
        "SCHEME": c["scheme"],
        "CONN_MODE": c["connMode"],
        "REQUEST_INTERVAL": c["requestInterval"],
        "REQUEST_TIMEOUT": c["requestTimeout"],
        "OPENS_BUDGET": c["opensBudget"],
        "ATESPACE": c["atespace"],
        "TEMPLATE": c["template"],
        "SUCCESS_RATE": c["successRateThreshold"],
        "RUNNER_CPU": c["runnerCpu"],
        "RUNNER_MEMORY": c["runnerMemory"],
    }
