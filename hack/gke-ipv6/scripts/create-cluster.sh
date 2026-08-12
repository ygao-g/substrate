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

set -e

CLUSTER_NAME="${1:-gke-ipv6-cluster}"
REGION="${2:-us-central1}"
NETWORK="${3:-gke-ipv6-vpc}"
SUBNET="${4:-gke-ipv6-vpc-node-subnet}"

echo "Creating GKE cluster ${CLUSTER_NAME} in ${REGION} (network: ${NETWORK}, subnet: ${SUBNET})..."

gcloud container clusters create "${CLUSTER_NAME}" \
  --region "${REGION}" \
  --network "${NETWORK}" \
  --subnetwork "${SUBNET}" \
  --enable-dataplane-v2 \
  --cluster-dns clouddns \
  --cluster-dns-scope cluster \
  --enable-ip-alias \
  --stack-type ipv4-ipv6 \
  --enable-kubernetes-alpha

echo "Cluster creation completed successfully!"
