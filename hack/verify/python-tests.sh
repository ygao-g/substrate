#!/usr/bin/env bash

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

# Runs the benchmarks' Python unit tests: the nighthawk-ingress module tests
# and the automation orchestrator tests. They import only pytest and PyYAML
# beyond the standard library, so the venv is scratch and installs just
# those. The descriptor-set round trip in test_spec.py skips outside the
# runner image.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

source hack/util/venv.sh

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
venv="${scratch}/venv"
ensure_venv "${venv}"
"${venv}/bin/pip" install --quiet pytest==9.1.1 PyYAML==6.0.3

# No cache directory and no bytecode: both would land in the tree.
PYTHONDONTWRITEBYTECODE=1 "${venv}/bin/python" -m pytest -p no:cacheprovider \
  benchmarking/nighthawk-ingress/tests \
  benchmarking/automation
