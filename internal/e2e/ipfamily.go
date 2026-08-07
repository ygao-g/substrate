// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// APINamespace and APIService locate the ate-api-server. The Service is
	// headless, so it has no ClusterIP and its dual-stack signal is
	// Spec.IPFamilies -- which governs the families of the per-pod records and
	// EndpointSlices -- rather than Spec.ClusterIPs.
	APINamespace = "ate-system"
	APIService   = "api"
	// APIAppLabel selects the ate-api-server pods.
	APIAppLabel = "app=ate-api-server"
	// APIGRPCPort is the control-plane gRPC port, the one F2 is about.
	APIGRPCPort = 443
)

// splitByFamily files IP strings into the first IPv4 and the first IPv6 address
// present, returning "" for a family that is absent. Unparseable entries and
// the "None" sentinel of a headless Service are skipped.
func splitByFamily(ips []string) (v4, v6 string) {
	for _, ip := range ips {
		if ip == "" || ip == corev1.ClusterIPNone {
			continue
		}
		// netip rather than net.IP: net.IP.To4 returns non-nil for a v4-mapped
		// v6 address and would misfile it as IPv4.
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			continue
		}
		switch {
		case addr.Is4() && v4 == "":
			v4 = ip
		case addr.Is6() && !addr.Is4In6() && v6 == "":
			v6 = ip
		}
	}
	return v4, v6
}

// ClusterIPsByFamily splits a Service's cluster IPs into its IPv4 and IPv6
// entries, returning "" for a family the Service does not have. A Service with
// no ipFamilyPolicy is SingleStack, so on a dual-stack cluster it still has
// exactly one ClusterIP and one of the two return values is empty — which is
// what makes this the right thing to gate a dual-stack assertion on.
//
// Spec.ClusterIPs is preferred over the singular Spec.ClusterIP, with a
// fallback for the latter because a Service object built by hand (or by a fake
// client) may only set the scalar.
func ClusterIPsByFamily(svc *corev1.Service) (v4, v6 string) {
	ips := svc.Spec.ClusterIPs
	if len(ips) == 0 && svc.Spec.ClusterIP != "" {
		ips = []string{svc.Spec.ClusterIP}
	}
	return splitByFamily(ips)
}

// PodIPsByFamily splits a Pod's addresses the same way, returning "" for a
// family the Pod does not have.
func PodIPsByFamily(pod *corev1.Pod) (v4, v6 string) {
	ips := make([]string, 0, len(pod.Status.PodIPs))
	for _, ip := range pod.Status.PodIPs {
		ips = append(ips, ip.IP)
	}
	if len(ips) == 0 && pod.Status.PodIP != "" {
		ips = []string{pod.Status.PodIP}
	}
	return splitByFamily(ips)
}

// ClusterIsDualStack reports whether the cluster allocates both address
// families, read from the nodes' pod CIDRs rather than from any Service.
//
// The distinction is the whole point. A Service with no ipFamilyPolicy is
// SingleStack and carries one ClusterIP even on a dual-stack cluster, so gating
// a dual-stack assertion on the families of the Service under test makes that
// assertion vacuous in exactly the case it exists to catch: the check skips,
// the suite is green, and the Service is IPv4-only. kube-controller-manager
// assigns one PodCIDR per configured family, so a node carrying two is the
// cluster-level signal, independent of every Service spec.
//
// Returns an error, not false, when no node reports a PodCIDR: a check that
// cannot tell which families the cluster has must fail loudly rather than skip.
// Clusters with external IPAM leave the field empty, but the kind path this
// suite targets always populates it.
func ClusterIsDualStack(ctx context.Context) (bool, error) {
	nodes, err := GetClients().K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("listing nodes: %w", err)
	}
	return nodesAreDualStack(nodes.Items)
}

// nodesAreDualStack holds the decision ClusterIsDualStack makes, separated from
// the API call so it can be tested without a cluster or a fake client.
func nodesAreDualStack(nodes []corev1.Node) (bool, error) {
	sawCIDR := false
	for i := range nodes {
		cidrs := nodes[i].Spec.PodCIDRs
		if len(cidrs) == 0 && nodes[i].Spec.PodCIDR != "" {
			cidrs = []string{nodes[i].Spec.PodCIDR}
		}
		if len(cidrs) == 0 {
			continue
		}
		sawCIDR = true

		var hasV4, hasV6 bool
		for _, cidr := range cidrs {
			prefix, err := netip.ParsePrefix(cidr)
			if err != nil {
				continue
			}
			if addr := prefix.Addr(); addr.Is4() {
				hasV4 = true
			} else if addr.Is6() && !addr.Is4In6() {
				hasV6 = true
			}
		}
		if hasV4 && hasV6 {
			return true, nil
		}
	}
	if !sawCIDR {
		return false, errors.New("no node reports spec.podCIDRs; cannot determine the cluster's address families")
	}
	return false, nil
}

// RouterClusterIPs returns the atenet-router Service's IPv4 and IPv6
// ClusterIPs. Either may be "".
func RouterClusterIPs(ctx context.Context) (v4, v6 string, err error) {
	svc, err := GetClients().K8s.CoreV1().Services(RouterNamespace).Get(ctx, RouterService, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("getting Service %s/%s: %w", RouterNamespace, RouterService, err)
	}
	v4, v6 = ClusterIPsByFamily(svc)
	return v4, v6, nil
}
