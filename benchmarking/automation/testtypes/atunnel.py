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

"""The `type: atunnel` hooks (see the package docstring for the lifecycle).

Runs benchmarking/atunnel, atunnel's isolation benchmark, as one Job pod: a
pki init container, the sink and the worker as sidecars, and runner.py as
the main container. The pod is the whole rig, so the type needs no Substrate
on the cluster. Cluster numbers are a baseline of their own, not a
continuation of benchmarking/atunnel/runs/ (see the bench's README).
"""

import math
import os
import re
from typing import Any

from util import build_and_push, parse_duration_seconds

TEST_TYPE = "atunnel"
NEEDS_SUBSTRATE = False

# Defaults for the tests.yaml `atunnel:` block: what shapes the pod. Load
# knobs such as --scenarios and --rungs go through `flags`, which runner.py
# forwards to `atunnel-bench load`.
DEFAULTS = {
    "tunnelCpus": 1,
    "tunnelMemory": "1g",
    "actors": 10,
    "bodyBytes": 16,
    # CPU request of the load generator and of the sink, each.
    "rigCpu": 4,
}

# parseBytes in cmd/atunnel-bench/worker.go: an integer with an optional
# k, m or g.
MEMORY = re.compile(r"(\d+)([kKmMgG]?)")
UNITS = {"": 1, "k": 1 << 10, "m": 1 << 20, "g": 1 << 30}


def config(test: dict[str, Any]) -> dict[str, Any]:
    """The test's `atunnel:` block merged over DEFAULTS."""
    return {**DEFAULTS, **(test.get("atunnel") or {})}


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "atunnel-runner-job.yaml.tmpl")


def memory_bytes(s: str) -> int:
    m = MEMORY.fullmatch(s)
    if not m:
        raise ValueError(f"memory {s!r} is not an integer with an optional k, m or g")
    return int(m.group(1)) * UNITS[m.group(2).lower()]


def _positive_int(name: str, knob: str, value: Any, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"atunnel test {name!r} has invalid atunnel.{knob} {value!r}: int >= {minimum}")


def validate(test: dict[str, Any]) -> None:
    """Raises ValueError on a malformed entry. `duration` is the job-wait
    budget and sets the Job's activeDeadlineSeconds."""
    name = test.get("name")
    if "duration" not in test:
        raise ValueError(f"atunnel test {name!r} missing 'duration'")
    try:
        parse_duration_seconds(str(test["duration"]))
    except ValueError as e:
        raise ValueError(f"atunnel test {name!r} duration: {e}") from None
    block = test.get("atunnel") or {}
    if not isinstance(block, dict):
        raise ValueError(f"atunnel test {name!r} atunnel block must be a mapping")
    unknown = set(block) - set(DEFAULTS)
    if unknown:
        raise ValueError(
            f"atunnel test {name!r} has unknown knob(s) {sorted(unknown)}; allowed: {sorted(DEFAULTS)}"
        )
    c = config(test)
    cpus = c["tunnelCpus"]
    if isinstance(cpus, bool) or not isinstance(cpus, (int, float)) or not math.isfinite(cpus) or not cpus > 0:
        raise ValueError(f"atunnel test {name!r} has invalid atunnel.tunnelCpus {cpus!r}: finite number > 0")
    if not isinstance(c["tunnelMemory"], str):
        raise ValueError(f"atunnel test {name!r} atunnel.tunnelMemory must be a string such as 1g")
    try:
        memory_bytes(c["tunnelMemory"])
    except ValueError as e:
        raise ValueError(f"atunnel test {name!r} atunnel.tunnelMemory: {e}") from None
    _positive_int(name, "actors", c["actors"], 1)
    _positive_int(name, "rigCpu", c["rigCpu"], 1)
    _positive_int(name, "bodyBytes", c["bodyBytes"], 0)


def build_image(commit: str) -> str:
    """Build & push the bench image with runner.py in it."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/atunnel-bench-test:{commit}",
        "benchmarking/atunnel/Dockerfile",
    )


def pre_test(test: dict[str, Any]) -> None:
    """Nothing to shape: the Job pod is the whole rig."""


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    """Template substitutions for the atunnel runner Job. The worker's
    figures are requests, not limits: the container also hosts the actors,
    and the tunnel's own cgroup carries the limits under test."""
    c = config(test)
    worker_mib = math.ceil((memory_bytes(c["tunnelMemory"]) + (1 << 30)) / (1 << 20))
    return {
        "TUNNEL_CPUS": c["tunnelCpus"],
        "TUNNEL_MEMORY": c["tunnelMemory"],
        "ACTORS": c["actors"],
        "BODY_BYTES": c["bodyBytes"],
        "RIG_CPU": c["rigCpu"],
        "WORKER_CPU": c["tunnelCpus"] + 1,
        "WORKER_MEMORY": f"{worker_mib}Mi",
        # Bounds a sidecar that never passes its startup probe.
        "ACTIVE_DEADLINE": parse_duration_seconds(test["duration"]) + 600,
    }
