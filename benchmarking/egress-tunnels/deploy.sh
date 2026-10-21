#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Deploys the egress-tunnels benchmark's endpoints: the target server and one
# Service per endpoint in {TARGET_NAMESPACE}. With --https it also patches
# the egress gateway to trust the target's certificate. The actors run on the
# shared benchmark pool: WORKLOAD_TEMPLATES=egress benchmarking/workloads/deploy.sh
# creates the pool and the actor template. See benchmarking/egress-tunnels/README.md.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
# Relative paths in the arguments are the caller's, not the repository root's.
CALLER_DIR="${PWD}"
cd "${ROOT}"

if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

MANIFEST_DIR="benchmarking/egress-tunnels/manifests"
TARGET_NAMESPACE="egress-tests-targets"
# Keep in sync with maxEndpoints in cmd/benchmarking/egress/endpoints.go.
MAX_ENDPOINTS=1024
# The egress gateway and what --patch-gateway adds to it. Keep in sync with
# gateway.go and the gateway-trust-*.yaml manifests.
SYSTEM_NAMESPACE="ate-system"
GATEWAY_DEPLOYMENT="atenet-egress"
GATEWAY_CA_CONFIGMAP="egress-tests-target-ca"
TARGET_TLS_SECRET="egress-target-tls"

ENDPOINTS=100
# Service i forwards to target ports HTTP_PORT_BASE+i and TLS_PORT_BASE+i, so
# no two Services share a backend IP and port: on Dataplane V2 two Services'
# connections can otherwise arrive at one pod with the same 4-tuple. The
# target listens on the whole MAX_ENDPOINTS range whatever --endpoints is,
# so a Service left from an earlier, larger deploy still reaches an open
# port. The bases stay clear of the target's own :8080 and :8443 and of each
# other up to MAX_ENDPOINTS.
HTTP_PORT_BASE=10000
TLS_PORT_BASE=12000
TARGET_IMAGE=""
WAIT_TIMEOUT_SECS=300
HTTPS=false
KUBECONFIG_FILE=""
KUBE_CONTEXT=""
# --kubeconfig and --context, for every kubectl and ko call.
CLUSTER_ARGS=()

usage() {
  echo "Usage: $0 --deploy|--delete|--patch-gateway|--unpatch-gateway|--print-targets [options]"
  echo ""
  echo "Actions:"
  echo "  --deploy                Deploy the target server and the endpoint Services"
  echo "  --delete                Delete them again, and unpatch the gateway"
  echo "                          (run 'go run ./cmd/benchmarking/egress driver cleanup' first)"
  echo "  --patch-gateway         Make the egress gateway trust the target's certificate, for"
  echo "                          --scheme https runs. Rolls the gateway; test clusters only."
  echo "  --unpatch-gateway       Undo --patch-gateway"
  echo "  --print-targets         Print the target Deployment and the endpoint Services; change nothing"
  echo ""
  echo "Options:"
  echo "  --https                 With --deploy, also --patch-gateway"
  echo "  --endpoints C           Endpoint Services to create, egress-target-0 up to"
  echo "                          egress-target-<C-1> (default: ${ENDPOINTS}, at most ${MAX_ENDPOINTS})"
  echo "  --target-image IMAGE    With --deploy or --print-targets, the target image instead of"
  echo "                          its ko:// reference"
  echo "  --wait-timeout SECONDS  How long to wait for each rollout (default: ${WAIT_TIMEOUT_SECS})"
  echo "  --kubeconfig FILE       Kubeconfig for every kubectl and ko call"
  echo "                          (default: the usual loading rules)"
  echo "  --context CTX           Kubeconfig context for every call (default: the current context)"
}

# Scratch space for generated certificates, removed on exit.
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# Every kubectl call in this script goes through here, so none falls back to
# the current context when --context is given.
kubectl() {
  command kubectl ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"} "$@"
}

# ko apply passes what follows -- to kubectl apply.
ko_apply() {
  if ((${#CLUSTER_ARGS[@]})); then
    hack/run-tool.sh ko apply -f - -- "${CLUSTER_ARGS[@]}"
  else
    hack/run-tool.sh ko apply -f -
  fi
}

sha256_file() {
  if command -v sha256sum &>/dev/null; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

# render_services prints one Service per endpoint.
render_services() {
  local i
  for ((i = 0; i < ENDPOINTS; i++)); do
    echo "---"
    sed -e "s|\${INDEX}|${i}|g" -e "s|targetPort: http\$|targetPort: $((HTTP_PORT_BASE + i))|" \
      -e "s|targetPort: https\$|targetPort: $((TLS_PORT_BASE + i))|" "${MANIFEST_DIR}/target-service.yaml.tmpl"
  done
}

# render_targets prints the target Deployment, listening on every Service's
# port up to MAX_ENDPOINTS.
render_targets() {
  local image="ko://go.ate.dev/substrate/cmd/benchmarking/egress"
  awk -v http="${HTTP_PORT_BASE}-$((HTTP_PORT_BASE + MAX_ENDPOINTS - 1))" \
    -v tls="${TLS_PORT_BASE}-$((TLS_PORT_BASE + MAX_ENDPOINTS - 1))" '
    { print }
    $1 == "-" && $2 ~ /^--tls-key-file=/ {
      print "        - --listen-ports=" http
      print "        - --tls-listen-ports=" tls
    }' "${MANIFEST_DIR}/targets.yaml.tmpl" | sed -e "s|${image}|${TARGET_IMAGE:-${image}}|"
}

# ensure_target_tls_secret gives the target a certificate for every endpoint
# name, issued once by a CA whose key is then discarded.
ensure_target_tls_secret() {
  kubectl create namespace "${TARGET_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  if kubectl -n "${TARGET_NAMESPACE}" get secret "${TARGET_TLS_SECRET}" &>/dev/null; then
    return 0
  fi
  echo "Issuing the target's HTTPS certificate..."
  go run ./cmd/benchmarking/egress driver certs --out "${WORK_DIR}/certs"
  kubectl -n "${TARGET_NAMESPACE}" create secret generic "${TARGET_TLS_SECRET}" \
    --type=kubernetes.io/tls \
    --from-file=tls.crt="${WORK_DIR}/certs/tls.crt" \
    --from-file=tls.key="${WORK_DIR}/certs/tls.key" \
    --from-file=ca.crt="${WORK_DIR}/certs/ca.crt" >/dev/null
  # Pods running from an earlier Secret do not see the new one.
  if kubectl -n "${TARGET_NAMESPACE}" get deployment egress-target &>/dev/null; then
    kubectl -n "${TARGET_NAMESPACE}" rollout restart deployment/egress-target
  fi
}

# patch_gateway_trust makes the egress gateway trust the target's CA when it
# re-originates TLS, and waits for the gateway to roll.
patch_gateway_trust() {
  local envoy_image ca_file ca_sha256
  envoy_image=$(kubectl -n "${SYSTEM_NAMESPACE}" get deployment "${GATEWAY_DEPLOYMENT}" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].image}')
  if [[ -z "${envoy_image}" ]]; then
    echo "Error: ${SYSTEM_NAMESPACE}/${GATEWAY_DEPLOYMENT} has no envoy container; --https supports only the Envoy dataplane." >&2
    exit 1
  fi
  if ! kubectl -n "${TARGET_NAMESPACE}" get secret "${TARGET_TLS_SECRET}" &>/dev/null; then
    echo "Error: the target has no certificate yet; run $0 --deploy --https first." >&2
    exit 1
  fi

  ca_file="${WORK_DIR}/target-ca.crt"
  kubectl -n "${TARGET_NAMESPACE}" get secret "${TARGET_TLS_SECRET}" \
    -o go-template='{{index .data "ca.crt" | base64decode}}' >"${ca_file}"
  kubectl -n "${SYSTEM_NAMESPACE}" create configmap "${GATEWAY_CA_CONFIGMAP}" \
    --from-file=ca.crt="${ca_file}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  ca_sha256=$(sha256_file "${ca_file}")

  echo "Patching ${SYSTEM_NAMESPACE}/${GATEWAY_DEPLOYMENT} to trust the target's CA..."
  sed -e "s|\${ENVOY_IMAGE}|${envoy_image}|g" \
      -e "s|\${TARGET_CA_SHA256}|${ca_sha256}|g" \
      "${MANIFEST_DIR}/gateway-trust-patch.yaml.tmpl" >"${WORK_DIR}/gateway-trust-patch.yaml"
  kubectl -n "${SYSTEM_NAMESPACE}" patch deployment "${GATEWAY_DEPLOYMENT}" \
    --type strategic --patch-file "${WORK_DIR}/gateway-trust-patch.yaml"
  if ! kubectl -n "${SYSTEM_NAMESPACE}" rollout status deployment/"${GATEWAY_DEPLOYMENT}" \
    --timeout="${WAIT_TIMEOUT_SECS}s"; then
    echo "Error: the patched gateway did not become ready; the previous pod keeps serving." >&2
    echo "Undo the patch with $0 --unpatch-gateway." >&2
    exit 1
  fi
}

# unpatch_gateway_trust puts the gateway back on the public roots alone. The
# CA ConfigMap goes last, once no gateway pod mounts it.
unpatch_gateway_trust() {
  if kubectl -n "${SYSTEM_NAMESPACE}" get deployment "${GATEWAY_DEPLOYMENT}" \
    -o jsonpath='{.spec.template.spec.initContainers[*].name}' 2>/dev/null | grep -qw egress-tests-trust; then
    echo "Removing the egress-tests trust patch from ${SYSTEM_NAMESPACE}/${GATEWAY_DEPLOYMENT}..."
    kubectl -n "${SYSTEM_NAMESPACE}" patch deployment "${GATEWAY_DEPLOYMENT}" \
      --type strategic --patch-file "${MANIFEST_DIR}/gateway-trust-unpatch.yaml"
    kubectl -n "${SYSTEM_NAMESPACE}" rollout status deployment/"${GATEWAY_DEPLOYMENT}" \
      --timeout="${WAIT_TIMEOUT_SECS}s"
  fi
  kubectl -n "${SYSTEM_NAMESPACE}" delete configmap "${GATEWAY_CA_CONFIGMAP}" --ignore-not-found >/dev/null
}

deploy() {
  if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
    echo "Error: KO_DOCKER_REPO must be set (see .ate-dev-env.sh)." >&2
    exit 1
  fi
  ensure_target_tls_secret
  echo "Deploying ${ENDPOINTS} endpoints in ${TARGET_NAMESPACE}..."
  render_targets | ko_apply
  render_services | kubectl apply -f - >/dev/null
  kubectl rollout status deployment/egress-target \
    --namespace="${TARGET_NAMESPACE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  echo "Ready: endpoints egress-target-0 up to egress-target-$((ENDPOINTS - 1)).${TARGET_NAMESPACE}.svc.cluster.local"

  if [[ "${HTTPS}" == true ]]; then
    patch_gateway_trust
    echo "Ready for --scheme https."
  fi
}

delete() {
  echo "Deleting the egress-tunnels endpoints..."
  unpatch_gateway_trust
  kubectl delete namespace "${TARGET_NAMESPACE}" --ignore-not-found
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy)
      action="deploy"
      ;;
    --delete)
      action="delete"
      ;;
    --patch-gateway)
      action="patch-gateway"
      ;;
    --unpatch-gateway)
      action="unpatch-gateway"
      ;;
    --print-targets)
      action="print-targets"
      ;;
    --https)
      HTTPS=true
      ;;
    --target-image)
      shift
      TARGET_IMAGE="${1:?--target-image needs a value}"
      ;;
    --target-image=*)
      TARGET_IMAGE="${1#*=}"
      ;;
    --endpoints)
      shift
      ENDPOINTS="$1"
      ;;
    --endpoints=*)
      ENDPOINTS="${1#*=}"
      ;;
    --wait-timeout)
      shift
      WAIT_TIMEOUT_SECS="$1"
      ;;
    --wait-timeout=*)
      WAIT_TIMEOUT_SECS="${1#*=}"
      ;;
    --kubeconfig)
      shift
      KUBECONFIG_FILE="$1"
      ;;
    --kubeconfig=*)
      KUBECONFIG_FILE="${1#*=}"
      ;;
    --context)
      shift
      KUBE_CONTEXT="$1"
      ;;
    --context=*)
      KUBE_CONTEXT="${1#*=}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

for value in "${ENDPOINTS}" "${WAIT_TIMEOUT_SECS}"; do
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "Error: --endpoints and --wait-timeout take whole numbers, got '${value}'" >&2
    exit 1
  fi
done
if ((ENDPOINTS < 1 || ENDPOINTS > MAX_ENDPOINTS)); then
  echo "Error: --endpoints must be between 1 and ${MAX_ENDPOINTS}, got ${ENDPOINTS}" >&2
  exit 1
fi
if [[ -n "${KUBECONFIG_FILE}" ]]; then
  if [[ "${KUBECONFIG_FILE}" != /* ]]; then
    KUBECONFIG_FILE="${CALLER_DIR}/${KUBECONFIG_FILE}"
  fi
  CLUSTER_ARGS+=(--kubeconfig "${KUBECONFIG_FILE}")
fi
if [[ -n "${KUBE_CONTEXT}" ]]; then
  CLUSTER_ARGS+=(--context "${KUBE_CONTEXT}")
fi

case "${action}" in
  deploy) deploy ;;
  delete) delete ;;
  patch-gateway) patch_gateway_trust ;;
  unpatch-gateway) unpatch_gateway_trust ;;
  print-targets)
    render_targets
    render_services
    ;;
  *)
    usage
    exit 1
    ;;
esac
