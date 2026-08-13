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
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestClusterIPsByFamily(t *testing.T) {
	tests := []struct {
		name   string
		svc    corev1.Service
		wantV4 string
		wantV6 string
	}{
		{
			name:   "single-stack IPv4",
			svc:    corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{"10.96.0.10"}}},
			wantV4: "10.96.0.10",
		},
		{
			name:   "single-stack IPv6",
			svc:    corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{"fd00:10:96::a"}}},
			wantV6: "fd00:10:96::a",
		},
		{
			name:   "dual-stack, IPv4 primary",
			svc:    corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{"10.96.0.10", "fd00:10:96::a"}}},
			wantV4: "10.96.0.10",
			wantV6: "fd00:10:96::a",
		},
		{
			name:   "dual-stack, IPv6 primary",
			svc:    corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{"fd00:10:96::a", "10.96.0.10"}}},
			wantV4: "10.96.0.10",
			wantV6: "fd00:10:96::a",
		},
		{
			// The scalar field is what a hand-built or fake-client Service
			// tends to carry.
			name:   "falls back to the scalar ClusterIP",
			svc:    corev1.Service{Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.10"}},
			wantV4: "10.96.0.10",
		},
		{
			name: "headless Service has neither",
			svc:  corev1.Service{Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone}},
		},
		{
			name: "headless Service, None in the list form",
			svc:  corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{corev1.ClusterIPNone}}},
		},
		{
			// net.IP.To4 returns non-nil for this and would file it as IPv4.
			// netip.Addr.Is4In6 is why it does not.
			name:   "a v4-mapped v6 address is neither",
			svc:    corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{"::ffff:10.96.0.10"}}},
			wantV4: "",
			wantV6: "",
		},
		{
			name: "unparseable entries are skipped",
			svc:  corev1.Service{Spec: corev1.ServiceSpec{ClusterIPs: []string{"not-an-ip", ""}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v4, v6 := ClusterIPsByFamily(&tc.svc)
			if v4 != tc.wantV4 || v6 != tc.wantV6 {
				t.Errorf("ClusterIPsByFamily() = (%q, %q), want (%q, %q)", v4, v6, tc.wantV4, tc.wantV6)
			}
		})
	}
}

func TestPodIPsByFamily(t *testing.T) {
	tests := []struct {
		name   string
		pod    corev1.Pod
		wantV4 string
		wantV6 string
	}{
		{
			name:   "single-stack IPv4",
			pod:    corev1.Pod{Status: corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "10.244.1.7"}}}},
			wantV4: "10.244.1.7",
		},
		{
			name: "dual-stack",
			pod: corev1.Pod{Status: corev1.PodStatus{PodIPs: []corev1.PodIP{
				{IP: "10.244.1.7"}, {IP: "fd00:10:244::7"},
			}}},
			wantV4: "10.244.1.7",
			wantV6: "fd00:10:244::7",
		},
		{
			name: "IPv6-primary dual-stack",
			pod: corev1.Pod{Status: corev1.PodStatus{PodIPs: []corev1.PodIP{
				{IP: "fd00:10:244::7"}, {IP: "10.244.1.7"},
			}}},
			wantV4: "10.244.1.7",
			wantV6: "fd00:10:244::7",
		},
		{
			name:   "falls back to the scalar PodIP",
			pod:    corev1.Pod{Status: corev1.PodStatus{PodIP: "fd00:10:244::7"}},
			wantV6: "fd00:10:244::7",
		},
		{
			name: "a pod with no address yet",
			pod:  corev1.Pod{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v4, v6 := PodIPsByFamily(&tc.pod)
			if v4 != tc.wantV4 || v6 != tc.wantV6 {
				t.Errorf("PodIPsByFamily() = (%q, %q), want (%q, %q)", v4, v6, tc.wantV4, tc.wantV6)
			}
		})
	}
}

func TestNodesFamilies(t *testing.T) {
	node := func(cidrs ...string) corev1.Node {
		return corev1.Node{Spec: corev1.NodeSpec{PodCIDRs: cidrs}}
	}

	tests := []struct {
		name    string
		nodes   []corev1.Node
		wantV4  bool
		wantV6  bool
		wantErr bool
	}{
		{
			name:   "dual-stack node",
			nodes:  []corev1.Node{node("10.244.0.0/24", "fd00:10:244::/64")},
			wantV4: true,
			wantV6: true,
		},
		{
			name:   "IPv6-primary dual-stack node",
			nodes:  []corev1.Node{node("fd00:10:244::/64", "10.244.0.0/24")},
			wantV4: true,
			wantV6: true,
		},
		{
			name:   "single-stack IPv4",
			nodes:  []corev1.Node{node("10.244.0.0/24"), node("10.244.1.0/24")},
			wantV4: true,
		},
		{
			// The case the whole change is for: the family is reported, so the
			// IPv6 legs run instead of the test skipping itself away.
			name:   "single-stack IPv6",
			nodes:  []corev1.Node{node("fd00:10:244::/64")},
			wantV6: true,
		},
		{
			// One dual-stack node is enough: the cluster allocates both.
			name:   "a mix reports both families",
			nodes:  []corev1.Node{node("10.244.0.0/24"), node("10.244.1.0/24", "fd00:10:244:1::/64")},
			wantV4: true,
			wantV6: true,
		},
		{
			name:   "falls back to the scalar PodCIDR",
			nodes:  []corev1.Node{{Spec: corev1.NodeSpec{PodCIDR: "10.244.0.0/24"}}},
			wantV4: true,
		},
		{
			// The check must not silently answer "no families" when it cannot
			// tell -- that would turn every per-family assertion into a skip.
			name:    "no CIDRs anywhere is an error, not an empty set",
			nodes:   []corev1.Node{{}, {}},
			wantErr: true,
		},
		{
			name:    "no nodes at all is an error",
			nodes:   nil,
			wantErr: true,
		},
		{
			name:   "unparseable CIDRs are skipped, not fatal",
			nodes:  []corev1.Node{node("garbage", "fd00:10:244::/64")},
			wantV6: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotV4, gotV6, err := nodesFamilies(tc.nodes)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("nodesFamilies() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("nodesFamilies() error = %v, want nil", err)
			}
			if gotV4 != tc.wantV4 || gotV6 != tc.wantV6 {
				t.Errorf("nodesFamilies() = (v4=%v, v6=%v), want (v4=%v, v6=%v)",
					gotV4, gotV6, tc.wantV4, tc.wantV6)
			}
		})
	}
}
