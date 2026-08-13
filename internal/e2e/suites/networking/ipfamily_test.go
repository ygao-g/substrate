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

package networking

import (
	"context"
	"net"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
)

// TestDataPathServicesAreDualStack asserts that every Service on the actor
// request path actually got both address families from the API server.
//
// This is the check the rest of the IPv6 suite is missing, and the reason it
// cannot be folded into those tests: TestActorIngressPerFamily and
// TestActorDNSAAAA both gate themselves on the router Service having two
// ClusterIPs, so if the Service regresses to SingleStack they skip. A suite
// where the IPv6 tests skip reports green, and "everything skipped" is
// indistinguishable from "everything passed" in CI output. This test inverts
// that: it decides whether to run from the *cluster's* families, read off the
// nodes' pod CIDRs, and then fails when a Service has fewer than the cluster
// can give it.
//
// ipFamilyPolicy is what is really under test. Absent it, the API server
// defaults a Service to SingleStack and allocates an IPv4 ClusterIP only, which
// makes every downstream IPv6 change -- AAAA records, dual-stack Envoy
// listeners, a v6-capable control-plane bind -- inert. Nothing pins the
// manifests statically, so this is the only check that the policy is set at
// all, and it additionally catches the case where the manifest is right but the
// cluster refused the second family.
func TestDataPathServicesAreDualStack(t *testing.T) {
	ctx := context.Background()

	dual, err := e2e.ClusterIsDualStack(ctx)
	if err != nil {
		t.Fatalf("determining the cluster's address families: %v", err)
	}
	if !dual {
		t.Skip("single-stack cluster; no Service here can have two families")
	}

	tests := []struct {
		name      string
		namespace string
		service   string
		// headless Services have no ClusterIP to inspect. Spec.IPFamilies is
		// still meaningful for them: it selects the families of the
		// EndpointSlices, and so of the per-pod DNS records gRPC clients
		// balance across.
		headless bool
	}{
		{name: "atenet-router", namespace: e2e.RouterNamespace, service: e2e.RouterService},
		{name: "dns", namespace: e2e.DNSNamespace, service: e2e.DNSService},
		{name: "api", namespace: e2e.APINamespace, service: e2e.APIService, headless: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := e2e.GetClients().K8s.CoreV1().Services(tc.namespace).Get(ctx, tc.service, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("getting Service %s/%s: %v", tc.namespace, tc.service, err)
			}

			if policy := svc.Spec.IPFamilyPolicy; policy == nil || *policy == corev1.IPFamilyPolicySingleStack {
				got := "unset"
				if policy != nil {
					got = string(*policy)
				}
				t.Errorf("Service %s/%s has ipFamilyPolicy %s on a dual-stack cluster; want %s. "+
					"Every IPv6 change downstream of this Service is inert until it has both families",
					tc.namespace, tc.service, got, corev1.IPFamilyPolicyPreferDualStack)
			}

			if len(svc.Spec.IPFamilies) < 2 {
				t.Errorf("Service %s/%s has ipFamilies %v; want both IPv4 and IPv6 on a dual-stack cluster",
					tc.namespace, tc.service, svc.Spec.IPFamilies)
			}

			if tc.headless {
				return
			}
			v4, v6 := e2e.ClusterIPsByFamily(svc)
			if v4 == "" {
				t.Errorf("Service %s/%s has no IPv4 ClusterIP (clusterIPs=%v); IPv4 carries all traffic today, "+
					"so losing it is the expensive regression", tc.namespace, tc.service, svc.Spec.ClusterIPs)
			}
			if v6 == "" {
				t.Errorf("Service %s/%s has no IPv6 ClusterIP (clusterIPs=%v)",
					tc.namespace, tc.service, svc.Spec.ClusterIPs)
			}
		})
	}
}

// TestAteAPIServerAcceptsBothFamilies dials the control-plane gRPC port on each
// family of an ate-api-server pod's own address, from inside the cluster.
//
// It is the live counterpart to the ClusterIP assertions above: a Service can
// be dual-stack while the process behind it is not.
//
// ate-api-server's Deployment used to pass
// --grpc-listen-addr=0.0.0.0:443, and the IPv4 wildcard binds one family: the
// process listens on v4 and nothing answers on v6, however dual-stack the
// cluster and the Service are. Dropping the override lets the Go default
// ":443" bind every family the host has.
//
// Three things about the shape of this test are deliberate:
//
//   - It dials the *pod* address, not the Service. api is headless, so there is
//     no ClusterIP to dial, and the pod IP is the address the process actually
//     bound anyway.
//   - The client is in-cluster. No test process can choose its own address
//     family through a port-forward or through the apiserver proxy
//     subresources -- all three pick the family for it -- so a probe pod is the
//     only way to select which socket is being asked to answer.
//   - It is a TCP connect, not a gRPC call. The listener accepting the
//     connection is the whole claim; the port speaks mTLS and a handshake
//     failure past that point would say nothing about the bind. A probe against
//     the health port would say nothing either: 9090 is a separate listener.
func TestAteAPIServerAcceptsBothFamilies(t *testing.T) {
	ctx := context.Background()

	pods, err := e2e.GetClients().K8s.CoreV1().Pods(e2e.APINamespace).List(ctx, metav1.ListOptions{
		LabelSelector: e2e.APIAppLabel,
	})
	if err != nil {
		t.Fatalf("listing ate-api-server pods in %s: %v", e2e.APINamespace, err)
	}
	var target *corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodRunning {
			target = &pods.Items[i]
			break
		}
	}
	if target == nil {
		t.Fatalf("no running ate-api-server pod in %s", e2e.APINamespace)
	}

	podV4, podV6 := e2e.PodIPsByFamily(target)
	t.Logf("probing ate-api-server pod %s (v4=%q v6=%q)", target.Name, podV4, podV6)

	probeNS := e2e.CreateNamespace(t)
	probePod := startProbePod(t, ctx, probeNS.Name)

	for _, tc := range []struct{ family, addr string }{
		{"ipv4", podV4},
		{"ipv6", podV6},
	} {
		t.Run(tc.family, func(t *testing.T) {
			if tc.addr == "" {
				// A single-stack cluster gives the pod one address. The other
				// family is not a failure here -- there is nothing for the
				// process to have bound.
				t.Skipf("ate-api-server pod %s has no %s address", target.Name, tc.family)
			}
			// nc -z is a connect-and-close: exit 0 means the listener accepted.
			// Same probe image and same flags the networkpolicy suite uses.
			out, err := execInPod(probeNS.Name, probePod,
				"nc", "-z", "-w", "5", tc.addr, strconv.Itoa(e2e.APIGRPCPort))
			if err != nil {
				t.Fatalf("TCP connect to %s from %s/%s over %s failed: %v; output: %s. "+
					"A refused connection on %s only means the process did not bind that family -- "+
					"check the Deployment for a --grpc-listen-addr override pinning it to the IPv4 wildcard",
					net.JoinHostPort(tc.addr, strconv.Itoa(e2e.APIGRPCPort)),
					probeNS.Name, probePod, tc.family, err, out, tc.family)
			}
		})
	}
}
