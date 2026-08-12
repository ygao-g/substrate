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
