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

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
reg_name="kind-registry"
reg_port="5001"

if [ "$#" -gt 0 ]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Creates the kind cluster '${KIND_CLUSTER_NAME}' and a local registry container on port ${reg_port}."
      echo
      echo "Configured through the environment:"
      echo "  KIND_CLUSTER_NAME  Name of the cluster to create (default: kind)."
      echo "  KIND_IP_FAMILY     Address families for pods and Services: ipv4, ipv6 or dual (default: ipv4)."
      exit 0
      ;;
  esac
fi

# Address families the cluster's pods and Services get: ipv4 (the default, and
# what every CI job but the dual-stack one runs), dual, or ipv6. "dual" makes
# IPv4 the primary family, so a Service with no ipFamilyPolicy still gets a v4
# ClusterIP and a pod's status.podIP is still v4 — i.e. it is additive and every
# IPv4 path keeps working. "ipv6" makes v6 primary and is the setting that
# actually exercises the single-family code paths (pod.Status.PodIP, PodIPs[0],
# the CoreDNS "IN A" answer).
KIND_IP_FAMILY="${KIND_IP_FAMILY:-ipv4}"
case "${KIND_IP_FAMILY}" in
  ipv4)
    # kind's own defaults for a single-stack IPv4 cluster; pinned here so all
    # three families are described in one place.
    pod_subnet="10.244.0.0/16"
    service_subnet="10.96.0.0/16"
    ;;
  ipv6)
    # ULA (fc00::/7) rather than GUA: nothing routes off the Docker host, and a
    # /56 pod subnet leaves each node the /64 kubeadm's
    # node-cidr-mask-size-ipv6 default hands out. kube-apiserver caps the
    # Service CIDR at 65536 addresses, so /112 is the largest it accepts.
    pod_subnet="fd00:10:244::/56"
    service_subnet="fd00:10:96::/112"
    ;;
  dual)
    # Order is significant: the first entry of each list is the cluster's
    # primary family.
    pod_subnet="10.244.0.0/16,fd00:10:244::/56"
    service_subnet="10.96.0.0/16,fd00:10:96::/112"
    ;;
  *)
    echo "error: KIND_IP_FAMILY must be one of ipv4, ipv6, dual (got '${KIND_IP_FAMILY}')" >&2
    exit 1
    ;;
esac

mkdir -p "${ROOT}/bin"

# 1. Create registry container unless it already exists
echo "Setting up local docker registry '${reg_name}' on port ${reg_port}..."
if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" = "true" ]; then
  if ! docker port "${reg_name}" | grep -q "${reg_port}"; then
    echo "Registry exists but is not mapped to port ${reg_port}. Recreating..."
    docker rm -f "${reg_name}"
  fi
fi

if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != "true" ]; then
  # Both loopback families are published, so `ko` pushing to localhost:5001 from
  # the host works whichever family its resolver picks. This already covers the
  # host side for an IPv6 or dual-stack cluster; the *node* side is separate and
  # goes over the "kind" Docker network below (step 4), where Docker's embedded
  # DNS answers "kind-registry" with whatever families that network has.
  docker run \
    -d --restart=always \
    --label created-by=agent-substrate \
    -p "127.0.0.1:${reg_port}:5000" \
    -p "[::1]:${reg_port}:5000" \
    --network bridge --name "${reg_name}" \
    registry:3
fi

# 2. Create kind configuration with containerdConfigPatches and feature gates.
#
# Probe for /dev/kvm where the kind nodes will actually run — inside the Docker
# provider VM on macOS (Lima/Colima/Docker Desktop), the host itself on Linux —
# and only wire up micro-VM (kata + cloud-hypervisor) support when present.
# Without KVM the cluster still works for gVisor.
echo "Probing for /dev/kvm in the Docker environment..."
HAS_KVM=0
if docker run --rm --device /dev/kvm busybox true >/dev/null 2>&1; then
  HAS_KVM=1
  echo "/dev/kvm found: micro-VM (kata + cloud-hypervisor) support will be enabled."
else
  echo "/dev/kvm not available: micro-VM support disabled (gVisor still works)."
fi

echo "Creating kind configuration for cluster '${KIND_CLUSTER_NAME}' (ipFamily=${KIND_IP_FAMILY})..."
cat <<EOF > "${ROOT}/bin/kind-config.yaml"
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  ipFamily: ${KIND_IP_FAMILY}
  podSubnet: "${pod_subnet}"
  serviceSubnet: "${service_subnet}"
nodes:
- role: control-plane
EOF
if [ "${HAS_KVM}" = "1" ]; then
  cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
  # Bind-mount /dev/kvm into the node so micro-VM (kata + cloud-hypervisor)
  # worker pods can use KVM.
  extraMounts:
  - hostPath: /dev/kvm
    containerPath: /dev/kvm
EOF
fi
cat <<EOF >> "${ROOT}/bin/kind-config.yaml"
# cmd/podcertcontroller depends on ClusterTrustBundle & PodCertificateRequest.
# They are not enabled by default as of Kubernetes v1.36
# https://github.com/kubernetes/kubernetes/blob/master/test/compatibility_lifecycle/reference/versioned_feature_list.yaml
featureGates:
  ClusterTrustBundle: true
  ClusterTrustBundleProjection: true
  PodCertificateRequest: true
runtimeConfig:
  "certificates.k8s.io/v1beta1": "true"
EOF

# An IPv6 or dual-stack cluster needs the "kind" Docker network to have IPv6.
# kind creates it that way, but a network left over from an older kind (or from
# a daemon that had IPv6 off) is reused as-is and the nodes then come up with no
# v6 address at all — which surfaces much later as pods stuck at ContainerCreating.
if [ "${KIND_IP_FAMILY}" != "ipv4" ]; then
  net_ipv6="$(docker network inspect kind --format '{{.EnableIPv6}}' 2>/dev/null || echo "absent")"
  if [ "${net_ipv6}" = "false" ]; then
    echo "error: the 'kind' Docker network exists without IPv6 enabled." >&2
    echo "       Detach everything from it and remove it, then re-run:" >&2
    echo "         docker network disconnect kind ${reg_name} && docker network rm kind" >&2
    echo "       Also confirm the daemon has IPv6 enabled (\"ip6tables\": true)." >&2
    exit 1
  fi
fi

echo "Deleting existing kind cluster '${KIND_CLUSTER_NAME}' if it exists..."
"${ROOT}"/hack/kind.sh delete cluster --name "${KIND_CLUSTER_NAME}" || true

echo "Creating kind cluster '${KIND_CLUSTER_NAME}'..."
"${ROOT}"/hack/kind.sh create cluster --name "${KIND_CLUSTER_NAME}" --config "${ROOT}/bin/kind-config.yaml"

# 2.5 Enable Proxy ARP on kind nodes for gVisor loopback pod-to-pod networking
echo "Enabling Proxy ARP on kind nodes..."
for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
  # Left on unconditionally: harmless on a v6-only cluster (the nodes still
  # carry IPv4 on the Docker bridge) and it keeps the IPv4 path byte-identical.
  docker exec "${node}" sysctl net.ipv4.conf.all.proxy_arp=1
  if [ "${KIND_IP_FAMILY}" != "ipv4" ]; then
    # proxy_ndp is the IPv6 counterpart, but only in name: proxy_arp answers for
    # every address the node has a route to, whereas proxy_ndp answers only for
    # addresses explicitly added with `ip -6 neigh add proxy <addr> dev <dev>`.
    # This flag is therefore necessary and not sufficient. It is also inert
    # today: the actor interior network is IPv4-only (169.254.17.0/30), so
    # nothing asks a node to proxy a v6 neighbour until the ateomnet dual-stack
    # work lands, which is when the per-address entries have to be added too.
    docker exec "${node}" sysctl net.ipv6.conf.all.forwarding=1
    docker exec "${node}" sysctl net.ipv6.conf.all.proxy_ndp=1
  fi
done

# 2.6 When KVM is available: make /dev/kvm usable inside the node and label
# nodes so micro-VM WorkerPools (nodeSelector ate.dev/sandboxClass=microvm) schedule.
if [ "${HAS_KVM}" = "1" ]; then
  echo "Preparing kind nodes for micro-VM (kata + cloud-hypervisor) runtime..."
  for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
    docker exec "${node}" chmod 666 /dev/kvm
    kubectl label node "${node}" ate.dev/sandboxClass=microvm --overwrite
  done
fi

# 3. Add the registry config to the nodes
echo "Adding registry config to kind nodes..."
REGISTRY_DIR="/etc/containerd/certs.d/localhost:${reg_port}"
for node in $("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}"); do
  docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
  cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
[host."http://${reg_name}:5000"]
EOF
done

# 4. Connect the registry to the cluster network if not already connected
echo "Connecting local registry to cluster network..."
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = "null" ]; then
  docker network connect "kind" "${reg_name}"
fi

# 5. Document the local registry in kube-public ConfigMap
echo "Documenting local registry in cluster..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF
