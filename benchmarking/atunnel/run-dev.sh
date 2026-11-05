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

# Quick dev run of the atunnel benchmark on a cluster, as the orchestrator's
# `type: atunnel` runs it: builds the image from the WORKING TREE (uncommitted
# changes included), submits the one-pod Job, waits, and prints results.txt
# and status.json. The cluster needs no Substrate; only a node the Job can
# land on (it tolerates ate.dev/benchmarking=true:NoSchedule).
#
#   benchmarking/atunnel/run-dev.sh --tunnel-cpus 1 --tunnel-memory 1g -- --scenarios IngressH1 --rungs 1000,2000
#
# Arguments after -- become the test's `flags`, which go to `atunnel-bench load`.
set -euo pipefail

TUNNEL_CPUS=1
TUNNEL_MEMORY=1g
ACTORS=10
RIG_CPU=4
DEST=""
VENV="${HOME}/.venvs/substrate-bench"
NAMESPACE="benchmarking"

usage() {
  cat <<USAGE
Usage: $0 [options] [-- load flags]
  --tunnel-cpus N     CPU limit for atunnel (default: ${TUNNEL_CPUS})
  --tunnel-memory M   memory limit for atunnel, e.g. 512m or 1g (default: ${TUNNEL_MEMORY})
  --actors N          actors atunnel hosts (default: ${ACTORS})
  --rig-cpu N         CPU request of the generator and of the sink, each (default: ${RIG_CPU})
  --dest gs://...     results root (default: gs://\$BUCKET_NAME/atunnel-results)
USAGE
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tunnel-cpus) TUNNEL_CPUS="$2"; shift 2 ;;
    --tunnel-memory) TUNNEL_MEMORY="$2"; shift 2 ;;
    --actors) ACTORS="$2"; shift 2 ;;
    --rig-cpu) RIG_CPU="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    --) shift; break ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done
LOAD_FLAGS=("$@")

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

source hack/util/venv.sh

# --- preflight (each failure prints its fix) ---------------------------------
[[ -f .ate-dev-env.sh ]] || {
  echo "ERROR: .ate-dev-env.sh not found at repo root (see GKE Quickstart in the repo README)" >&2
  exit 1
}
# shellcheck disable=SC1091
source .ate-dev-env.sh
for var in PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION KO_DOCKER_REPO BUCKET_NAME; do
  [[ -n "${!var:-}" ]] || { echo "ERROR: ${var} not set by .ate-dev-env.sh" >&2; exit 1; }
done
DEST="${DEST:-gs://${BUCKET_NAME}/atunnel-results}"

docker info >/dev/null 2>&1 || {
  echo "ERROR: docker daemon not running (the image is built locally)" >&2
  exit 1
}
kubectl cluster-info >/dev/null 2>&1 || {
  echo "ERROR: kubectl cannot reach ${CLUSTER_NAME}; run gcloud container clusters get-credentials" >&2
  exit 1
}

echo ">>> run config:"
echo "      cluster:        ${CLUSTER_NAME} (${CLUSTER_LOCATION})"
echo "      tunnel_cpus:    ${TUNNEL_CPUS}"
echo "      tunnel_memory:  ${TUNNEL_MEMORY}"
echo "      actors:         ${ACTORS}"
echo "      rig_cpu:        ${RIG_CPU}"
echo "      load flags:     ${LOAD_FLAGS[*]+"${LOAD_FLAGS[*]}"}"
echo "      dest:           ${DEST}"

# venv: importing orchestrator.py (validation and rendering) needs PyYAML.
ensure_venv "${VENV}"
if ! "${VENV}/bin/python" -c 'import yaml' 2>/dev/null; then
  "${VENV}/bin/pip" install --quiet pyyaml ||
    "${VENV}/bin/pip" install --quiet --index-url https://pypi.org/simple/ pyyaml
fi
PY="${VENV}/bin/python"

# --- image from the working tree ----------------------------------------------
SHA7="$(git rev-parse --short=7 HEAD)"
DIRTY=""
[[ -n "$(git status --porcelain)" ]] && DIRTY="-dirty"
TAG="quick-${SHA7}${DIRTY}-$(date +%H%M%S)"
IMAGE="${KO_DOCKER_REPO}/atunnel-bench-test:${TAG}"
echo ">>> building ${IMAGE} from the working tree"
# The clusters are amd64; the Dockerfile cross-compiles from an arm64 host.
docker build --platform linux/amd64 -f benchmarking/atunnel/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"

# --- render + submit the Job ---------------------------------------------------
NAME="atunnel_${TUNNEL_CPUS}cpu_${TUNNEL_MEMORY}"
JOB="runner-atunnel-quick-$(date +%H%M%S)"
export IMAGE JOB NAME DEST TAG TUNNEL_CPUS TUNNEL_MEMORY ACTORS RIG_CPU
# The [@]+ form keeps bash 3.2 (macOS) from treating an empty array as unset.
"${PY}" - ${LOAD_FLAGS[@]+"${LOAD_FLAGS[@]}"} <<'PYEOF' | kubectl apply -f -
import os
import sys

sys.path.insert(0, "benchmarking/automation")
import orchestrator
from testtypes import atunnel

flags = sys.argv[1:]
test = {
    "name": os.environ["NAME"],
    "type": "atunnel",
    "targetCluster": "dev",
    "duration": "30m",
    "atunnel": {
        "tunnelCpus": float(os.environ["TUNNEL_CPUS"]),
        "tunnelMemory": os.environ["TUNNEL_MEMORY"],
        "actors": int(os.environ["ACTORS"]),
        "rigCpu": int(os.environ["RIG_CPU"]),
    },
    "flags": flags,
}
if test["atunnel"]["tunnelCpus"].is_integer():
    test["atunnel"]["tunnelCpus"] = int(test["atunnel"]["tunnelCpus"])
orchestrator.validate_and_normalize_tests([test])
subs = {
    "JOB_NAME": os.environ["JOB"],
    "IMAGE": os.environ["IMAGE"],
    "TAG": os.environ["TAG"],
    "NAME": test["name"],
    "DEST": os.environ["DEST"],
    **atunnel.job_subs(test),
}
tmpl = atunnel.job_tmpl("benchmarking/automation/manifests")
print(orchestrator.render_template(tmpl, subs, flags))
PYEOF

# --- wait, streaming the runner's log ------------------------------------------
echo ">>> waiting for job/${JOB}"
LOGS_PID=""
trap '[[ -n "${LOGS_PID}" ]] && kill "${LOGS_PID}" 2>/dev/null || true' EXIT
( until kubectl logs -f "job/${JOB}" -c runner -n "${NAMESPACE}" 2>/dev/null; do sleep 5; done ) &
LOGS_PID=$!
STATUS="timeout"
for _ in $(seq 1 180); do  # 180 x 10s = 30 min
  S="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null)"
  F="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null)"
  [[ "${S}" == "1" ]] && { STATUS="complete"; break; }
  [[ "${F}" == "1" ]] && { STATUS="failed"; break; }
  sleep 10
done
kill "${LOGS_PID}" 2>/dev/null || true
LOGS_PID=""

# --- verdict --------------------------------------------------------------------
if [[ "${STATUS}" == "complete" ]]; then
  PREFIX="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/status.json" 2>/dev/null | tail -1)"
  if [[ -n "${PREFIX}" ]]; then
    PREFIX="${PREFIX%/status.json}"
    echo ">>> ${PREFIX}"
    gcloud storage cat "${PREFIX}/results.txt"
    gcloud storage cat "${PREFIX}/status.json"
  fi
  kubectl delete job "${JOB}" -n "${NAMESPACE}" >/dev/null
  echo ">>> ${NAME}: complete (tag ${TAG})"
else
  echo ">>> ${NAME}: ${STATUS}; job/${JOB} kept for debugging. The worker's log:" >&2
  kubectl logs "job/${JOB}" -c worker -n "${NAMESPACE}" --tail=50 >&2 || true
  echo "    kubectl logs job/${JOB} -n ${NAMESPACE} --all-containers=true --prefix=true" >&2
  exit 1
fi
