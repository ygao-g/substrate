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

Runs the self-contained atunnel component benchmark in a single multi-container
Job pod (pki init container, sink and worker sidecars, and rig runner) without
deploying Substrate or workloads.
"""

import os
import re
from typing import Any

from util import build_and_push, parse_duration_seconds

TEST_TYPE = "atunnel"
NEEDS_SUBSTRATE = False

DEFAULTS = {
    "tunnelCpus": 1,
    "tunnelMemory": "1g",
    "actors": 10,
    "bodyBytes": 16,
    "rigCpu": 4,
}

_MEMORY_RE = re.compile(r"^(\d+)([kKmMgG]?)$")
_UNIT_BYTES = {
    "": 1,
    "k": 1 << 10,
    "K": 1 << 10,
    "m": 1 << 20,
    "M": 1 << 20,
    "g": 1 << 30,
    "G": 1 << 30,
}


def parse_bytes(s: str) -> int:
    m = _MEMORY_RE.fullmatch(s)
    if not m:
        raise ValueError(f"invalid memory size {s!r} (want e.g. '512m', '1g')")
    n = int(m.group(1))
    if n <= 0:
        raise ValueError(f"memory size must be > 0, got {s!r}")
    return n * _UNIT_BYTES[m.group(2)]


def config(test: dict[str, Any]) -> dict[str, Any]:
    """The test's `atunnel:` block merged over DEFAULTS."""
    return {**DEFAULTS, **(test.get("atunnel") or {})}


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "atunnel-runner-job.yaml.tmpl")


def validate(test: dict[str, Any]) -> None:
    name = test.get("name")
    if "duration" not in test:
        raise ValueError(f"atunnel test {name!r} missing 'duration'")
    parse_duration_seconds(str(test["duration"]))

    raw = test.get("atunnel") or {}
    if not isinstance(raw, dict):
        raise ValueError(f"atunnel test {name!r} atunnel block must be a mapping")
    allowed = set(DEFAULTS)
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(
            f"atunnel test {name!r} has unknown knob(s) "
            f"{sorted(unknown)}; allowed: {sorted(allowed)}"
        )

    cfg = config(test)
    tunnel_cpus = cfg["tunnelCpus"]
    if (
        isinstance(tunnel_cpus, bool)
        or not isinstance(tunnel_cpus, (int, float))
        or tunnel_cpus <= 0
    ):
        raise ValueError(
            f"atunnel test {name!r} has invalid atunnel.tunnelCpus {tunnel_cpus!r} (want > 0)"
        )

    tunnel_memory = cfg["tunnelMemory"]
    if not isinstance(tunnel_memory, str):
        raise ValueError(
            f"atunnel test {name!r} has invalid atunnel.tunnelMemory {tunnel_memory!r}"
        )
    try:
        parse_bytes(tunnel_memory)
    except ValueError as e:
        raise ValueError(
            f"atunnel test {name!r} has invalid atunnel.tunnelMemory: {e}"
        ) from e

    for field in ("actors", "rigCpu"):
        val = cfg[field]
        if isinstance(val, bool) or not isinstance(val, int) or val < 1:
            raise ValueError(
                f"atunnel test {name!r} has invalid atunnel.{field} {val!r} (want int >= 1)"
            )

    body_bytes = cfg["bodyBytes"]
    if isinstance(body_bytes, bool) or not isinstance(body_bytes, int) or body_bytes < 0:
        raise ValueError(
            f"atunnel test {name!r} has invalid atunnel.bodyBytes {body_bytes!r} (want int >= 0)"
        )


def build_image(commit: str) -> str:
    """Build & push the atunnel benchmark runner image."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/atunnel-bench-test:{commit}",
        "benchmarking/atunnel/Dockerfile",
    )


def pre_test(test: dict[str, Any]) -> None:
    """No cluster preparation needed for the self-contained atunnel pod."""


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    """Template substitutions specific to the atunnel runner Job."""
    cfg = config(test)
    return {
        "TUNNEL_CPUS": cfg["tunnelCpus"],
        "TUNNEL_MEMORY": cfg["tunnelMemory"],
        "ACTORS": cfg["actors"],
        "BODY_BYTES": cfg["bodyBytes"],
        "RIG_CPU": cfg["rigCpu"],
        "WORKER_CPU": cfg["tunnelCpus"] + 1,
        # Format as integer bytes because Kubernetes rejects lowercase 'g'/'m'/'k'.
        "WORKER_MEMORY": str(parse_bytes(cfg["tunnelMemory"]) + (1 << 30)),
        "ACTIVE_DEADLINE": parse_duration_seconds(str(test["duration"])) + 600,
    }
