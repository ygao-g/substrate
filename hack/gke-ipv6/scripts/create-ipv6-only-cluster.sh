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

# Creates a single-stack IPv6-only GKE Standard cluster (go/anthropic-gke-ipv6-ug
# Steps 1-4) plus the NAT64/DNS64 egress path the guide leaves out.
#
# Billable. Tear down with teardown-ipv6-only-cluster.sh.

set -o errexit -o nounset -o pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
# us-central1 tops out below the version floor; us-west1 is the only alternative.
REGION="${REGION:-us-east1}"
ZONE="${ZONE:-us-east1-b}"
CLUSTER_VERSION="${CLUSTER_VERSION:-1.36.3-gke.1253000}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
NUM_NODES="${NUM_NODES:-1}"

PREFIX="${PREFIX:-ipv6ready}"
NETWORK="${NETWORK:-${PREFIX}-net}"
PSC_SUBNET="${PSC_SUBNET:-${PREFIX}-psc-subnet}"
NODE_SUBNET="${NODE_SUBNET:-${PREFIX}-nodes-subnet}"
ROUTER="${ROUTER:-${PREFIX}-router}"
NAT="${NAT:-${PREFIX}-nat64}"
DNS_POLICY="${DNS_POLICY:-${PREFIX}-dns64}"
CLUSTER="${CLUSTER:-${PREFIX}-cluster}"

# ULA (INTERNAL) nodes have no egress at all; GUA (EXTERNAL) gives them public
# IPv6, which is the only egress an IPv6-only GKE node can actually have -- see
# the NAT64 note below. The PSC subnet is always INTERNAL: that is what the
# control plane's private endpoint needs, and why the network still enables ULA.
NODE_ACCESS_TYPE="${NODE_ACCESS_TYPE:-EXTERNAL}"

gcloud() { command gcloud --project="${PROJECT}" "$@"; }

echo "project=${PROJECT} zone=${ZONE} cluster=${CLUSTER} nodes=${NUM_NODES}x${MACHINE_TYPE}"

echo "== Step 1: VPC =="
gcloud compute networks create "${NETWORK}" \
  --subnet-mode=CUSTOM --enable-ula-internal-ipv6

echo "== Step 2: PSC subnet =="
gcloud compute networks subnets create "${PSC_SUBNET}" \
  --network="${NETWORK}" --region="${REGION}" \
  --stack-type=IPV6_ONLY --ipv6-access-type=INTERNAL

echo "== Step 3: node subnet =="
gcloud compute networks subnets create "${NODE_SUBNET}" \
  --network="${NETWORK}" --region="${REGION}" \
  --stack-type=IPV6_ONLY --ipv6-access-type="${NODE_ACCESS_TYPE}"

# Off by default, and it is not an oversight: NAT64 does not work here. Cloud NAT
# translates only IPv4 for GKE nodes (cloud.google.com/nat/docs/public-nat), so
# 64:ff9b::/96 black-holes from both nodes and pods -- measured 2026-08-12, the
# gateway even publishes a mapping for the node's /96 that never carries traffic.
# DNS64 alone is worse than nothing on GKE: it turns an instant "no route" into a
# 20s timeout. Enable only if this VPC also gets plain Compute Engine VMs, which
# NAT64 does support.
if [ "${WITH_NAT64:-0}" = 1 ]; then
  echo "== Step 3.5: NAT64 + DNS64 (does NOT apply to GKE nodes) =="
  gcloud compute routers create "${ROUTER}" \
    --network="${NETWORK}" --region="${REGION}"
  gcloud compute routers nats create "${NAT}" \
    --router="${ROUTER}" --region="${REGION}" \
    --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges \
    --nat64-all-v6-subnet-ip-ranges
  gcloud dns policies create "${DNS_POLICY}" \
    --networks="${NETWORK}" --enable-dns64-all-queries \
    --description="DNS64 for IPv6-only GKE nodes"
fi

# No --enable-ip-alias: IPv6-only clusters do not use IP aliases, and gcloud's
# "--stack-type needs --enable-ip-alias" rule is an ipv4-ipv6-only rule.
# --release-channel=rapid is required and the guide omits it: CLUSTER_VERSION is
# RAPID-only in us-east1/us-west1, so --cluster-version alone hits REGULAR and fails.
echo "== Step 4: cluster (billable) =="
gcloud beta container clusters create "${CLUSTER}" \
  --zone="${ZONE}" \
  --cluster-version="${CLUSTER_VERSION}" \
  --release-channel=rapid \
  --stack-type=ipv6 \
  --network="${NETWORK}" \
  --subnetwork="${NODE_SUBNET}" \
  --private-endpoint-subnetwork="${PSC_SUBNET}" \
  --enable-dataplane-v2 \
  --cluster-dns=clouddns \
  --num-nodes="${NUM_NODES}" \
  --machine-type="${MACHINE_TYPE}"

echo
echo "Created. Tear down with:"
echo "  PREFIX=${PREFIX} ZONE=${ZONE} REGION=${REGION} $(dirname "$0")/teardown-ipv6-only-cluster.sh"
