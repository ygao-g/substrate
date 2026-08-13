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
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	routerAppLabel = "app=atenet-router"
	// envoyAdminPort is the admin listener in the router pod's envoy container.
	// It is not published by the Service, so the pod proxy subresource is the
	// only way at it from a test.
	envoyAdminPort = 9901

	// Listener names from cmd/atenet/internal/router/xds.go. They cannot be
	// imported: that package is under cmd/atenet/internal, so only cmd/atenet
	// may import it.
	ingressHTTPListener  = "ingress_http_listener"
	ingressHTTPSListener = "ingress_https_listener"

	// Same digest-pinned image the networkpolicy suite probes with. BusyBox's
	// wget handles bracketed IPv6 URLs and honors a user-supplied Host header
	// instead of adding its own, which is exactly what is needed here.
	probeImage = "busybox@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827681e7cf6c83d5e"
)

// envoyListeners is the subset of Envoy's admin /listeners?format=json response
// this test reads. additional_local_addresses is how a listener with
// Listener.additional_addresses reports its extra sockets.
type envoyListeners struct {
	ListenerStatuses []struct {
		Name         string `json:"name"`
		LocalAddress struct {
			SocketAddress envoySocketAddress `json:"socket_address"`
		} `json:"local_address"`
		AdditionalLocalAddresses []struct {
			SocketAddress envoySocketAddress `json:"socket_address"`
		} `json:"additional_local_addresses"`
	} `json:"listener_statuses"`
}

type envoySocketAddress struct {
	Address   string `json:"address"`
	PortValue int    `json:"port_value"`
	// Ipv4Compat is proto3-omitted from the admin JSON when false, so an absent
	// field decodes to the value this test wants. See the assertion below for
	// why false is load-bearing on the "::" socket.
	Ipv4Compat bool `json:"ipv4_compat"`
}

// TestRouterListenerAddresses asserts, from Envoy's own view of itself, that
// each ingress listener bound both an IPv4 and an IPv6 socket.
//
// This is the cheap half of the ingress coverage and the one that runs
// everywhere: the "::" socket binds on a single-stack IPv4 cluster too, it just
// carries no traffic there. It is also the only assertion in the suite that can
// fail when someone removes the IPv6 socket, because every other path a test has
// into the router — a port-forward, the pods/proxy subresource, the
// services/proxy subresource — is mediated by the API server and reaches the
// pod over whatever family the *kubelet or apiserver* chooses. None of them let
// the test select an address family, so none of them can select a listener
// socket.
func TestRouterListenerAddresses(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	pod := mustRouterPodName(t, ctx)

	raw, err := clients.K8s.CoreV1().RESTClient().Get().
		Namespace(e2e.RouterNamespace).
		Resource("pods").
		Name(pod+":"+strconv.Itoa(envoyAdminPort)).
		SubResource("proxy").
		Suffix("listeners").
		Param("format", "json").
		DoRaw(ctx)
	if err != nil {
		// The pods/proxy subresource reaches the pod on its primary-family
		// PodIP, so this hop used to be family-sensitive. It no longer is: the
		// admin listener binds "::" with ipv4_compat
		// (manifests/ate-install/atenet-router.yaml), which accepts connections
		// from either family. A failure here means the admin interface is not
		// answering — the container is not up, or the proxy path is blocked.
		t.Fatalf("reading Envoy admin /listeners from %s/%s: %v", e2e.RouterNamespace, pod, err)
	}

	var listeners envoyListeners
	if err := json.Unmarshal(raw, &listeners); err != nil {
		t.Fatalf("decoding /listeners response %q: %v", raw, err)
	}
	if len(listeners.ListenerStatuses) == 0 {
		t.Fatalf("Envoy reports no listeners at all; xDS has not converged. Body: %s", raw)
	}

	// One entry per listener name: every socket it is bound on.
	bound := map[string][]envoySocketAddress{}
	for _, ls := range listeners.ListenerStatuses {
		sockets := []envoySocketAddress{ls.LocalAddress.SocketAddress}
		for _, extra := range ls.AdditionalLocalAddresses {
			sockets = append(sockets, extra.SocketAddress)
		}
		bound[ls.Name] = sockets
	}

	for _, name := range []string{ingressHTTPListener, ingressHTTPSListener} {
		t.Run(name, func(t *testing.T) {
			sockets, ok := bound[name]
			if !ok {
				if name == ingressHTTPSListener {
					// The HTTPS listener only exists when --port-https is set.
					// It is set in the shipped manifest, but do not make this
					// test the thing that fails if that changes.
					t.Skipf("router has no %s; listeners present: %v", name, slices.Sorted(maps.Keys(bound)))
				}
				t.Fatalf("router has no %s; listeners present: %v", name, slices.Sorted(maps.Keys(bound)))
			}

			addrs := make([]string, 0, len(sockets))
			for _, s := range sockets {
				addrs = append(addrs, s.Address)
			}
			// The IPv4 socket is the one that carries all production traffic
			// today; losing it is the expensive regression, so assert it first.
			if !slices.Contains(addrs, "0.0.0.0") {
				t.Errorf("%s is bound on %v; want an IPv4 wildcard socket (0.0.0.0)", name, addrs)
			}
			if !slices.Contains(addrs, "::") {
				t.Errorf("%s is bound on %v; want an IPv6 wildcard socket (::) as well. "+
					"Envoy binds it on a single-stack cluster too, so this failing means the "+
					"listener lost its additional_addresses entry", name, addrs)
			}

			// ipv4_compat on the "::" socket would clear IPV6_V6ONLY and make it
			// accept IPv4 too -- colliding with the 0.0.0.0 socket already bound
			// to the same port. Envoy fails the whole listener when an additional
			// address cannot bind, so this does not degrade IPv6: it takes all
			// ingress down, both families. The xDS side is pinned in
			// cmd/atenet/internal/router/xds_test.go; this asserts it survived
			// the trip into Envoy.
			for _, s := range sockets {
				if s.Address == "::" && s.Ipv4Compat {
					t.Errorf("%s has ipv4_compat set on its :: socket (port %d); want false. "+
						"It collides with the 0.0.0.0 socket on the same port and Envoy rejects "+
						"the entire listener, dropping IPv4 ingress along with IPv6", name, s.PortValue)
				}
			}
		})
	}
}

// TestActorIngressPerFamily reaches an actor through the router over each of the
// router Service's ClusterIPs, from a pod inside the cluster.
//
// The client has to be in-cluster: e2e.RouterClient port-forwards to
// 127.0.0.1, which tunnels through the API server to the kubelet, so its own
// address family says nothing about which of the router's sockets served the
// request.
//
// Each family the cluster allocates is exercised; a family it does not have is
// skipped. Gating the whole test on the router being dual-stack instead meant an
// IPv6-only cluster -- the one place the IPv6 path is the only path -- never
// reached the router over IPv6 here at all.
func TestActorIngressPerFamily(t *testing.T) {
	ctx := context.Background()

	hasV4, hasV6, err := e2e.ClusterFamilies(ctx)
	if err != nil {
		t.Fatalf("determining the cluster's address families: %v", err)
	}

	routerV4, routerV6, err := e2e.RouterClusterIPs(ctx)
	if err != nil {
		t.Fatalf("reading atenet-router ClusterIPs: %v", err)
	}
	// A ClusterIP missing for a family the cluster does allocate is the
	// regression this test exists to catch, so fail rather than skip past it.
	if hasV4 && routerV4 == "" {
		t.Fatalf("cluster allocates IPv4 but atenet-router has no IPv4 ClusterIP")
	}
	if hasV6 && routerV6 == "" {
		t.Fatalf("cluster allocates IPv6 but atenet-router has no IPv6 ClusterIP")
	}

	actorName, _ := createAndResumeActor(t, ctx, "family", counterTemplate)
	dnsName := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}.DNSName()

	probeNS := e2e.CreateNamespace(t)
	probePod := startProbePod(t, ctx, probeNS.Name)

	// Both families, in one test: the point of dual-stack is that both work,
	// and a change that turns the IPv4 socket off is the costly failure.
	for _, tc := range []struct {
		family     string
		clusterIP  string
		clusterHas bool
	}{
		{"ipv4", routerV4, hasV4},
		{"ipv6", routerV6, hasV6},
	} {
		t.Run(tc.family, func(t *testing.T) {
			if !tc.clusterHas {
				t.Skipf("cluster allocates no %s pod CIDR", tc.family)
			}
			// The request must carry the actor's DNS name as the Host: it is
			// the only routing key the router's ext_proc has. Only the
			// *connection* goes to the literal.
			url := fmt.Sprintf("http://%s/readyz", net.JoinHostPort(tc.clusterIP, "80"))
			out, err := execInPod(probeNS.Name, probePod,
				"wget", "-q", "-T", "10", "-O", "-", "--header", "Host: "+dnsName, url)
			if err != nil {
				t.Fatalf("GET %s (Host: %s) from %s/%s over %s failed: %v; output: %s",
					url, dnsName, probeNS.Name, probePod, tc.family, err, out)
			}
			t.Logf("actor reached over %s via %s; body: %s", tc.family, tc.clusterIP, strings.TrimSpace(out))
		})
	}
}

func mustRouterPodName(t *testing.T, ctx context.Context) string {
	t.Helper()
	pods, err := e2e.GetClients().K8s.CoreV1().Pods(e2e.RouterNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: routerAppLabel,
	})
	if err != nil {
		t.Fatalf("listing atenet-router pods: %v", err)
	}
	for i := range pods.Items {
		if portforward.IsPodReady(&pods.Items[i]) {
			return pods.Items[i].Name
		}
	}
	t.Fatalf("no ready atenet-router pod in %s", e2e.RouterNamespace)
	return ""
}

func startProbePod(t *testing.T, ctx context.Context, namespace string) string {
	t.Helper()
	clients := e2e.GetClients()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ingress-probe", Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "probe",
				Image:   probeImage,
				Command: []string{"/bin/sleep", "3600"},
			}},
		},
	}
	if _, err := clients.K8s.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating probe pod %s/%s: %v", namespace, pod.Name, err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got, err := clients.K8s.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err == nil && got.Status.Phase == corev1.PodRunning {
			return pod.Name
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for probe pod %s/%s to run", namespace, pod.Name)
	return ""
}

// execInPod runs a command in a pod. It shells out to kubectl, matching what
// the networkpolicy suite already does, rather than pulling in client-go's
// remotecommand plumbing for two calls.
func execInPod(namespace, pod string, command ...string) (string, error) {
	args := []string{}
	if e2e.KubeConfig != "" {
		args = append(args, "--kubeconfig="+e2e.KubeConfig)
	}
	if e2e.KubeContext != "" {
		args = append(args, "--context="+e2e.KubeContext)
	}
	args = append(args, "exec", "-n", namespace, pod, "--")
	args = append(args, command...)
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	return string(out), err
}
