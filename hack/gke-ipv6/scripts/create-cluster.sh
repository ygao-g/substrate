#!/usr/bin/env bash
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
  --stack-type IPV4_IPV6

echo "Cluster creation completed successfully!"
