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

# Deletes everything create-ipv6-only-cluster.sh made, in reverse order.
# Keeps going past a missing resource so a half-built stack still cleans up.

set -o nounset -o pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-east1}"
ZONE="${ZONE:-us-east1-b}"

PREFIX="${PREFIX:-ipv6ready}"
NETWORK="${NETWORK:-${PREFIX}-net}"
PSC_SUBNET="${PSC_SUBNET:-${PREFIX}-psc-subnet}"
NODE_SUBNET="${NODE_SUBNET:-${PREFIX}-nodes-subnet}"
ROUTER="${ROUTER:-${PREFIX}-router}"
NAT="${NAT:-${PREFIX}-nat64}"
DNS_POLICY="${DNS_POLICY:-${PREFIX}-dns64}"
CLUSTER="${CLUSTER:-${PREFIX}-cluster}"

gcloud() { command gcloud --project="${PROJECT}" --quiet "$@"; }

failures=0
step() {
  echo "== $1 =="
  shift
  "$@" || {
    echo "  (failed or already gone)"
    failures=$((failures + 1))
  }
}

step "cluster ${CLUSTER}" \
  gcloud beta container clusters delete "${CLUSTER}" --zone="${ZONE}"

# The policy must be unbound from the network before the network can go.
step "dns policy ${DNS_POLICY}" \
  gcloud dns policies update "${DNS_POLICY}" --networks=""
step "dns policy ${DNS_POLICY} (delete)" \
  gcloud dns policies delete "${DNS_POLICY}"

step "nat ${NAT}" \
  gcloud compute routers nats delete "${NAT}" --router="${ROUTER}" --region="${REGION}"
step "router ${ROUTER}" \
  gcloud compute routers delete "${ROUTER}" --region="${REGION}"

step "subnet ${NODE_SUBNET}" \
  gcloud compute networks subnets delete "${NODE_SUBNET}" --region="${REGION}"
step "subnet ${PSC_SUBNET}" \
  gcloud compute networks subnets delete "${PSC_SUBNET}" --region="${REGION}"

step "network ${NETWORK}" \
  gcloud compute networks delete "${NETWORK}"

echo
if [ "${failures}" -eq 0 ]; then
  echo "Teardown complete."
else
  echo "Teardown finished with ${failures} step(s) failed or already gone -- check the log above."
fi
