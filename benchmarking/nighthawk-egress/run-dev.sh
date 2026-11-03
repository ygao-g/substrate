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

# Quick dev run of the Nighthawk egress-capacity benchmark.
#
# Pins atenet-egress, applies gateway trust patch when running in https-mitm mode,
# deploys nighthawk-server, builds the runner image, submits the benchmark Job, and gathers output.

set -euo pipefail

MODE="http"
CONN_MODE="keepalive"
ENVOY_CPU=2
EXT_PROC_CPU=""
ACTORS=1
CONNECTIONS=""
TAIL_LATENCY_SLO_MS=25.0
INITIAL_TOTAL_RPS=1000
ATESPACE="benchmark-workloads"
NAMESPACE="benchmarking"
DEST=""
RUN_TAG=""
TEST_NAME=""
RUNNER_IMAGE=""
OUT_DIR="/tmp/nighthawk-egress-out"

usage() {
  cat <<EOF
Usage: $0 [options]
  --mode {http,https-mitm}  egress protocol mode (default: ${MODE})
  --conn-mode {keepalive,churn} connection mode (default: ${CONN_MODE})
  --envoy-cpu N             egress envoy CPU pin (default: ${ENVOY_CPU})
  --ext-proc-cpu N          egress ext-proc CPU pin (default: same as envoy-cpu)
  --actors N                number of traffic-driver instances (default: ${ACTORS})
  --connections N           connections per worker (default: 32 for keepalive, 250 for churn)
  --tail-latency-slo-ms N   SLO bound (default: ${TAIL_LATENCY_SLO_MS})
  --initial-total-rps N     initial total RPS for search (default: ${INITIAL_TOTAL_RPS})
  --atespace NAME           actor namespace (default: ${ATESPACE})
  --dest GCS_OR_LOCAL_PATH  results root (default: gs://\$BUCKET_NAME/nighthawk-egress-results)
  --run-tag TAG             tag for this run (commit sha or label)
  --test-name NAME          test name identifier
  --runner-image IMAGE      prebuilt runner image to use (skips docker build/push)
  --out-dir DIR             local output directory (default: ${OUT_DIR})
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --conn-mode) CONN_MODE="$2"; shift 2 ;;
    --envoy-cpu) ENVOY_CPU="$2"; shift 2 ;;
    --ext-proc-cpu) EXT_PROC_CPU="$2"; shift 2 ;;
    --actors) ACTORS="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --tail-latency-slo-ms) TAIL_LATENCY_SLO_MS="$2"; shift 2 ;;
    --initial-total-rps) INITIAL_TOTAL_RPS="$2"; shift 2 ;;
    --atespace) ATESPACE="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    --run-tag) RUN_TAG="$2"; shift 2 ;;
    --test-name) TEST_NAME="$2"; shift 2 ;;
    --runner-image) RUNNER_IMAGE="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

EXT_PROC_CPU="${EXT_PROC_CPU:-${ENVOY_CPU}}"
if [[ -z "${CONNECTIONS}" ]]; then
  if [[ "${CONN_MODE}" == "churn" ]]; then
    CONNECTIONS="250"
  else
    CONNECTIONS="32"
  fi
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

SCRIPT_DIR="benchmarking/nighthawk-egress"
MANIFEST_DIR="${SCRIPT_DIR}/manifests"

if [[ -f .ate-dev-env.sh ]]; then
  # shellcheck disable=SC1091
  source .ate-dev-env.sh
fi
for var in PROJECT_ID CLUSTER_NAME KO_DOCKER_REPO; do
  [[ -n "${!var:-}" ]] || { echo "ERROR: ${var} not set (export it or define in .ate-dev-env.sh)" >&2; exit 1; }
done
if [[ -z "${DEST}" ]] && [[ -n "${BUCKET_NAME:-}" ]]; then
  DEST="gs://${BUCKET_NAME}/nighthawk-egress-results"
fi

if [[ -z "${RUNNER_IMAGE}" ]]; then
  docker info >/dev/null 2>&1 || {
    echo "ERROR: docker daemon not running (the runner image is built locally)" >&2
    exit 1
  }
fi

kubectl get deployment atenet-egress -n ate-system >/dev/null 2>&1 || {
  echo "ERROR: substrate is not deployed on ${CLUSTER_NAME} (atenet-egress not found in ate-system)" >&2
  exit 1
}

if [[ -z "$(kubectl get workerpool -n benchmark-workloads -o name 2>/dev/null)" ]]; then
  echo "ERROR: no WorkerPool deployed in benchmark-workloads. Run:" >&2
  echo "  WORKLOAD_TEMPLATES=traffic-driver benchmarking/workloads/deploy.sh --deploy --worker-count ${ACTORS} --sandbox-class gvisor" >&2
  exit 1
fi

RUNNING_WORKERS="$(kubectl get pods -n benchmark-workloads -l ate.dev/worker-pool --no-headers 2>/dev/null | grep -c ' Running ' || true)"
if (( RUNNING_WORKERS < ACTORS )); then
  echo "ERROR: ${RUNNING_WORKERS} workers Running in benchmark-workloads, want at least ${ACTORS} for actor fleet. Run:" >&2
  echo "  WORKLOAD_TEMPLATES=traffic-driver benchmarking/workloads/deploy.sh --deploy --worker-count ${ACTORS} --sandbox-class gvisor" >&2
  exit 1
fi

echo ">>> run config:"
echo "      cluster:              ${CLUSTER_NAME}"
echo "      mode:                 ${MODE}"
echo "      conn_mode:            ${CONN_MODE}"
echo "      envoy_cpu:            ${ENVOY_CPU}"
echo "      ext_proc_cpu:         ${EXT_PROC_CPU}"
echo "      actors:               ${ACTORS}"
echo "      connections/worker:   ${CONNECTIONS}"
echo "      workers running:      ${RUNNING_WORKERS}"
echo "      tail_latency_slo_ms:  ${TAIL_LATENCY_SLO_MS}"
echo "      atespace:             ${ATESPACE}"
echo "      dest:                 ${DEST:-none}"

cleanup() {
  echo ">>> Cleaning up dev run resources"
  kubectl delete -f "${MANIFEST_DIR}/resource-sampler-rbac.yaml" --ignore-not-found >/dev/null 2>&1 || true
  if [[ "${MODE}" == "https-mitm" ]]; then
    kubectl patch deployment atenet-egress -n ate-system --type=strategic \
      --patch-file "${MANIFEST_DIR}/gateway-trust-unpatch.yaml" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo ">>> Ensuring namespace ${NAMESPACE} and ServiceAccount benchmark-runner"
kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    ate.dev/api-access: "true"
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: benchmark-runner
  namespace: ${NAMESPACE}
EOF

echo ">>> Deploying nighthawk namespace and server"
kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"
SERVER_REPLICAS="$(( ENVOY_CPU > 2 ? ENVOY_CPU : 2 ))"
sed "s/replicas: 1/replicas: ${SERVER_REPLICAS}/g" "${SCRIPT_DIR}/nighthawk-server.yaml" \
  | kubectl apply -f -
kubectl rollout status deployment/nighthawk-server -n nighthawk --timeout=180s

echo ">>> Applying resource-sampler RBAC"
kubectl apply -f "${MANIFEST_DIR}/resource-sampler-rbac.yaml"

echo ">>> Pinning atenet-egress (replicas=1, envoy_cpu=${ENVOY_CPU}, ext_proc_cpu=${EXT_PROC_CPU})"
kubectl scale deployment atenet-egress -n ate-system --replicas=1
kubectl patch deployment atenet-egress -n ate-system --type=strategic --patch "$(cat <<EOF
spec:
  template:
    spec:
      containers:
      - name: envoy
        resources:
          requests:
            cpu: "${ENVOY_CPU}"
            memory: 4Gi
          limits:
            cpu: "${ENVOY_CPU}"
            memory: 4Gi
        args:
        - -c
        - /etc/envoy/envoy.yaml
        - --service-node
        - atenet-egress
        - --service-cluster
        - atenet-egress
        - --concurrency
        - "${ENVOY_CPU}"
      - name: ext-proc
        resources:
          requests:
            cpu: "${EXT_PROC_CPU}"
          limits:
            cpu: "${EXT_PROC_CPU}"
EOF
)"
kubectl rollout status deployment/atenet-egress -n ate-system --timeout=180s

if [[ "${MODE}" == "https-mitm" ]]; then
  echo ">>> Applying servicedns upstream trust patch to atenet-egress"
  ENVOY_IMG="$(kubectl get deployment atenet-egress -n ate-system -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].image}')"
  sed "s|\${ENVOY_IMAGE}|${ENVOY_IMG}|g" "${MANIFEST_DIR}/gateway-trust-patch.yaml.tmpl" \
    | kubectl patch deployment atenet-egress -n ate-system --type=strategic --patch-file /dev/stdin
  kubectl rollout status deployment/atenet-egress -n ate-system --timeout=180s
fi

if [[ -n "${RUNNER_IMAGE}" ]]; then
  IMAGE="${RUNNER_IMAGE}"
  TAG="${RUN_TAG:-prebuilt}"
  echo ">>> Using prebuilt runner image: ${IMAGE}"
else
  # Runner image from the working tree
  SHA7="$(git rev-parse --short=7 HEAD)"
  DIRTY=""
  [[ -n "$(git status --porcelain)" ]] && DIRTY="-dirty"
  TAG="${RUN_TAG:-quick-${SHA7}${DIRTY}-$(date +%H%M%S)}"
  IMAGE="${KO_DOCKER_REPO}/nighthawk-egress-runner:${TAG}"
  NODE_ARCH="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}' 2>/dev/null || true)"
  echo ">>> building ${IMAGE} from working tree (platform: linux/${NODE_ARCH:-amd64})"
  docker build --platform "linux/${NODE_ARCH:-amd64}" -f benchmarking/nighthawk-egress/Dockerfile -t "${IMAGE}" .
  docker push "${IMAGE}"
fi

# Render and submit the Job
NAME="${TEST_NAME:-egress_cap_${MODE}_envoy_${ENVOY_CPU}cpu}"
JOB="runner-egress-${ENVOY_CPU}cpu-quick-$(date +%H%M%S)"
RUNNER_CPU="2"
ATE_ATEAPI_CLIENT_AUTH="${ATE_ATEAPI_CLIENT_AUTH:-cert}"

echo ">>> Submitting benchmark runner Job: ${JOB} in namespace ${NAMESPACE}"
sed -e "s|\${JOB_NAME}|${JOB}|g" \
    -e "s|\${NAME}|${NAME}|g" \
    -e "s|\${TAG}|${TAG}|g" \
    -e "s|\${IMAGE}|${IMAGE}|g" \
    -e "s|\${DEST}|${DEST}|g" \
    -e "s|\${MODE}|${MODE}|g" \
    -e "s|\${CONN_MODE}|${CONN_MODE}|g" \
    -e "s|\${ENVOY_CPU}|${ENVOY_CPU}|g" \
    -e "s|\${EXT_PROC_CPU}|${EXT_PROC_CPU}|g" \
    -e "s|\${ACTORS}|${ACTORS}|g" \
    -e "s|\${ATESPACE}|${ATESPACE}|g" \
    -e "s|\${CONNECTIONS}|${CONNECTIONS}|g" \
    -e "s|\${TAIL_LATENCY_SLO_MS}|${TAIL_LATENCY_SLO_MS}|g" \
    -e "s|\${INITIAL_TOTAL_RPS}|${INITIAL_TOTAL_RPS}|g" \
    -e "s|\${OUT_DIR}|/tmp/nighthawk-egress-out|g" \
    -e "s|\${RUNNER_CPU}|${RUNNER_CPU}|g" \
    -e "s|\${ATE_ATEAPI_CLIENT_AUTH}|${ATE_ATEAPI_CLIENT_AUTH}|g" \
    "${MANIFEST_DIR}/runner-job.yaml.tmpl" | kubectl apply -f -

# Wait, streaming runner progress lines
echo ">>> waiting for job/${JOB} (progress lines only; full logs land in logs.txt)"
( until kubectl logs -f "job/${JOB}" -n "${NAMESPACE}" 2>/dev/null \
    | grep --line-buffered -vE '^(\[(service|adaptive)\] |===ARTIFACT:)'; do sleep 5; done ) &
LOGS_PID=$!
STATUS="timeout"
for _ in $(seq 1 150); do
  S="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
  F="$(kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
  [[ "${S}" == "1" ]] && { STATUS="complete"; break; }
  [[ "${F}" == "1" ]] && { STATUS="failed"; break; }
  sleep 10
done
kill "${LOGS_PID}" 2>/dev/null || true

# Extract artifacts into OUT_DIR
echo ">>> Extracting artifacts into ${OUT_DIR}"
mkdir -p "${OUT_DIR}"
FULL_LOGS="$(kubectl logs "job/${JOB}" -n "${NAMESPACE}" 2>/dev/null || true)"
echo "${FULL_LOGS}" | grep -vE '^===ARTIFACT:' > "${OUT_DIR}/logs.txt" || true
echo "${FULL_LOGS}" | while IFS= read -r line; do
  if [[ "${line}" =~ ^===ARTIFACT:([^:]+):([a-zA-Z0-9+/=]+)=== ]]; then
    fname="${BASH_REMATCH[1]}"
    b64data="${BASH_REMATCH[2]}"
    echo "${b64data}" | (base64 -d 2>/dev/null || base64 -D) > "${OUT_DIR}/${fname}"
  fi
done

# Final verdict
if [[ "${STATUS}" == "complete" ]]; then
  if [[ -f "${OUT_DIR}/capacity.json" ]]; then
    echo ">>> Capacity summary from ${OUT_DIR}/capacity.json:"
    cat "${OUT_DIR}/capacity.json"
  elif [[ "${DEST}" =~ ^gs:// ]]; then
    CAPACITY="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/capacity.json" 2>/dev/null | tail -1 || true)"
    if [[ -n "${CAPACITY}" ]]; then
      echo ">>> ${CAPACITY}"
      gcloud storage cat "${CAPACITY}"
    fi
  fi
  kubectl delete job "${JOB}" -n "${NAMESPACE}" >/dev/null 2>&1 || true
  echo ">>> ${NAME}: complete (tag ${TAG})"
else
  echo ">>> ${NAME}: ${STATUS} - job/${JOB} kept for debugging:" >&2
  echo "      kubectl logs job/${JOB} -n ${NAMESPACE}" >&2
  exit 1
fi
