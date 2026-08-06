#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/../manifests"

echo "Applying IPv6 GKE manifests from ${MANIFESTS_DIR}..."
kubectl apply -f "${MANIFESTS_DIR}/"

echo "Waiting for MutatingAdmissionPolicy to propagate..."
sleep 2

echo "Rolling out restarts for kube-system DaemonSets to inject GCE_METADATA_HOST..."
# Add any specific DaemonSets that must be restarted here.
# For example, gpu-maintenance-handler if it applies.
kubectl rollout restart ds -n kube-system

echo "Post-install configuration complete!"
