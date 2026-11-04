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

# Quick dev run of the self-contained atunnel benchmark on a cluster.
#
# Builds the runner image from the WORKING TREE (uncommitted changes included),
# submits the benchmark Job (pki init container, sink and worker sidecars, and
# rig runner), waits, and prints results.txt and status.json from GCS. Does not
# require Substrate or workloads to be deployed on the cluster.
#
#   ./benchmarking/atunnel/run-dev.sh
#   ./benchmarking/atunnel/run-dev.sh --tunnel-cpus 2 --tunnel-memory 2g -- --scenarios IngressH1
set -euo pipefail

TUNNEL_CPUS=1
TUNNEL_MEMORY="1g"
ACTORS=10
RIG_CPU=4
DEST=""
VENV="${HOME}/.venvs/substrate-bench"
NAMESPACE="benchmarking"
RIG_FLAGS=()

usage() {
  cat <<EOF
Usage: $0 [options] [-- rig-flags]
  --tunnel-cpus N       atunnel cgroup CPU limit (default: ${TUNNEL_CPUS})
  --tunnel-memory SIZE  atunnel cgroup memory limit, e.g. 512m, 1g (default: ${TUNNEL_MEMORY})
  --actors N            actor netns count on the worker (default: ${ACTORS})
  --rig-cpu N           sink and runner container CPU request (default: ${RIG_CPU})
  --dest URI            results root (default: gs://\$BUCKET_NAME/atunnel-results)
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tunnel-cpus) TUNNEL_CPUS="$2"; shift 2 ;;
    --tunnel-memory) TUNNEL_MEMORY="$2"; shift 2 ;;
    --actors) ACTORS="$2"; shift 2 ;;
    --rig-cpu) RIG_CPU="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    --) shift; RIG_FLAGS=("$@"); break ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

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
  echo "ERROR: docker daemon not running (the runner image is built locally)" >&2
  exit 1
}

kubectl cluster-info >/dev/null 2>&1 || {
  echo "ERROR: cannot reach cluster ${CLUSTER_NAME} via kubectl" >&2
  exit 1
}

echo ">>> run config:"
echo "      cluster:        ${CLUSTER_NAME} (${CLUSTER_LOCATION})"
echo "      tunnel_cpus:    ${TUNNEL_CPUS}"
echo "      tunnel_memory:  ${TUNNEL_MEMORY}"
echo "      actors:         ${ACTORS}"
echo "      rig_cpu:        ${RIG_CPU}"
echo "      dest:           ${DEST}"
if (( ${#RIG_FLAGS[@]} > 0 )); then
  echo "      rig_flags:      ${RIG_FLAGS[*]}"
fi

# venv: importing orchestrator.py (validation/rendering) needs PyYAML.
ensure_venv "${VENV}"
if ! "${VENV}/bin/python" -c 'import yaml' 2>/dev/null; then
  "${VENV}/bin/pip" install --quiet pyyaml ||
    "${VENV}/bin/pip" install --quiet --index-url https://pypi.org/simple/ pyyaml
fi
PY="${VENV}/bin/python"

# --- runner image from the working tree ---------------------------------------
SHA7="$(git rev-parse --short=7 HEAD)"
DIRTY=""
[[ -n "$(git status --porcelain)" ]] && DIRTY="-dirty"
TAG="quick-${SHA7}${DIRTY}-$(date +%H%M%S)"
IMAGE="${KO_DOCKER_REPO}/atunnel-bench-test:${TAG}"
echo ">>> building ${IMAGE} from the working tree"
docker build --platform linux/amd64 \
  -f benchmarking/atunnel/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"

# --- render + submit the Job ---------------------------------------------------
CPU_SLUG="${TUNNEL_CPUS//./p}"
NAME="atunnel_${CPU_SLUG}cpu_${TUNNEL_MEMORY}"
JOB="runner-atunnel-${CPU_SLUG}cpu-${TUNNEL_MEMORY}-quick-$(date +%H%M%S)"
export IMAGE JOB NAME DEST TAG TUNNEL_CPUS TUNNEL_MEMORY ACTORS RIG_CPU
"${PY}" - "${RIG_FLAGS[@]+"${RIG_FLAGS[@]}"}" <<'EOF' | kubectl apply -f -
import os
import sys

sys.path.insert(0, "benchmarking/automation")
import orchestrator
from testtypes import atunnel

cpus_raw = os.environ["TUNNEL_CPUS"]
tunnel_cpus = int(cpus_raw) if cpus_raw.isdigit() else float(cpus_raw)
flags = sys.argv[1:]

test = {
    "name": os.environ["NAME"],
    "type": "atunnel",
    "targetCluster": "dev",
    "duration": "15m",
    "atunnel": {
        "tunnelCpus": tunnel_cpus,
        "tunnelMemory": os.environ["TUNNEL_MEMORY"],
        "actors": int(os.environ["ACTORS"]),
        "rigCpu": int(os.environ["RIG_CPU"]),
    },
    "flags": flags,
}
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
EOF

# --- wait, streaming the runner container's log lines --------------------------
echo ">>> waiting for job/${JOB}"
( until kubectl logs -f "job/${JOB}" -n "${NAMESPACE}" -c runner 2>/dev/null; do sleep 5; done ) &
LOGS_PID=$!
STATUS="timeout"
for _ in $(seq 1 120); do  # 120 x 10s = 20 min
  S="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null)"
  F="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null)"
  [[ "${S}" == "1" ]] && { STATUS="complete"; break; }
  [[ "${F}" == "1" ]] && { STATUS="failed"; break; }
  sleep 10
done
kill "${LOGS_PID}" 2>/dev/null || true

# --- verdict --------------------------------------------------------------------
if [[ "${STATUS}" == "complete" ]]; then
  for artifact in results.txt status.json; do
    URI="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/${artifact}" 2>/dev/null | tail -1)"
    if [[ -n "${URI}" ]]; then
      echo ">>> ${URI}"
      gcloud storage cat "${URI}"
    fi
  done
  kubectl delete job "${JOB}" -n "${NAMESPACE}" >/dev/null
  echo ">>> ${NAME}: complete (tag ${TAG})"
else
  echo ">>> ${NAME}: ${STATUS} — worker log and job/${JOB} kept for debugging:" >&2
  kubectl logs "job/${JOB}" -n "${NAMESPACE}" -c worker >&2 || true
  exit 1
fi
