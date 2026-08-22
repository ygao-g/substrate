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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterIPFamilies reports which address families the cluster can assign,
// read from a node's podCIDRs. Both CI axes are single-family, so a test that
// only makes sense on one of them gates on this rather than assuming.
func ClusterIPFamilies(t *testing.T, ctx context.Context) map[corev1.IPFamily]bool {
	t.Helper()

	nodes, err := GetClients().K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("listing nodes to determine the cluster's address families: %v", err)
	}
	if len(nodes.Items) == 0 {
		t.Fatal("the cluster reports no nodes, so its address families cannot be determined")
	}

	families := map[corev1.IPFamily]bool{}
	for _, cidr := range nodes.Items[0].Spec.PodCIDRs {
		if strings.Contains(cidr, ":") {
			families[corev1.IPv6Protocol] = true
		} else {
			families[corev1.IPv4Protocol] = true
		}
	}
	if len(families) == 0 {
		t.Fatalf("node %q has no podCIDRs, so the cluster's address families cannot be determined", nodes.Items[0].Name)
	}
	return families
}
