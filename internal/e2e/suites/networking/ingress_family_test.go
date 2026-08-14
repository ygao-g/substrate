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

// envoySocketAddress is a socket from /listeners. It deliberately has no
// ipv4_compat field: /listeners reports the *resolved* address a listener bound,
// and ipv4_compat is a config knob rather than a property of the bound address,
// so Envoy never emits it there. Decoding it here would silently yield false for
// every socket and make any assertion on it vacuous. Read it from
// envoyConfiguredSockets instead.
type envoySocketAddress struct {
	Address   string `json:"address"`
	PortValue int    `json:"port_value"`
}

// envoyConfigDump is the subset of Envoy's admin /config_dump this test reads.
// Ingress listeners arrive over xDS and land in dynamic_listeners; the egress
// gateway's listener is static config and lands in static_listeners.
type envoyConfigDump struct {
	Configs []struct {
		Type            string `json:"@type"`
		StaticListeners []struct {
			Listener envoyListenerConfig `json:"listener"`
		} `json:"static_listeners"`
		DynamicListeners []struct {
			ActiveState struct {
				Listener envoyListenerConfig `json:"listener"`
			} `json:"active_state"`
		} `json:"dynamic_listeners"`
	} `json:"configs"`
}

type envoyListenerConfig struct {
	Name    string `json:"name"`
	Address struct {
		SocketAddress envoyConfiguredSocket `json:"socket_address"`
	} `json:"address"`
	AdditionalAddresses []struct {
		Address struct {
			SocketAddress envoyConfiguredSocket `json:"socket_address"`
		} `json:"address"`
	} `json:"additional_addresses"`
}

type envoyConfiguredSocket struct {
	Address   string `json:"address"`
	PortValue int    `json:"port_value"`
	// Ipv4Compat is proto3-omitted from the JSON when false, so an absent field
	// decodes to false, which is the real configured value.
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
	pod := mustReadyPodName(t, ctx, e2e.RouterNamespace, routerAppLabel)
	bound := envoyBoundSockets(t, ctx, e2e.RouterNamespace, pod, envoyAdminPort)
	configured := envoyConfiguredSockets(t, ctx, e2e.RouterNamespace, pod, envoyAdminPort)

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
			//
			// This has to come from config_dump. /listeners reports resolved bound
			// addresses and never carries ipv4_compat, so asserting on it there
			// passes whatever the config says.
			for _, s := range configured[name] {
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
// Skipped unless the router Service is dual-stack. On a single-stack cluster
// there is exactly one ClusterIP and TestActorDirectAccess already covers it.
func TestActorIngressPerFamily(t *testing.T) {
	ctx := context.Background()

	routerV4, routerV6, err := e2e.RouterClusterIPs(ctx)
	if err != nil {
		t.Fatalf("reading atenet-router ClusterIPs: %v", err)
	}
	if routerV4 == "" || routerV6 == "" {
		t.Skipf("atenet-router is single-stack (v4=%q v6=%q); nothing to compare", routerV4, routerV6)
	}

	actorName, _ := createAndResumeActor(t, ctx, "family", counterTemplate)
	dnsName := resources.ActorDNSName(resources.ActorRef{Atespace: networkingAtespace, Name: actorName})

	probeNS := e2e.CreateNamespace(t)
	probePod := startProbePod(t, ctx, probeNS.Name)

	// Both families, in one test: the point of dual-stack is that both work,
	// and a change that turns the IPv4 socket off is the costly failure.
	for _, tc := range []struct{ family, clusterIP string }{
		{"ipv4", routerV4},
		{"ipv6", routerV6},
	} {
		t.Run(tc.family, func(t *testing.T) {
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

// envoyBoundSockets reads a gateway's Envoy admin /listeners and returns, per
// listener name, every socket it is actually bound on: the primary first, then
// any additional_local_addresses. Reading it back from Envoy rather than from
// the config is the point — it is the only place a listener that failed to bind
// shows up.
func envoyBoundSockets(t *testing.T, ctx context.Context, namespace, pod string, adminPort int) map[string][]envoySocketAddress {
	t.Helper()
	raw, err := e2e.GetClients().K8s.CoreV1().RESTClient().Get().
		Namespace(namespace).
		Resource("pods").
		Name(pod+":"+strconv.Itoa(adminPort)).
		SubResource("proxy").
		Suffix("listeners").
		Param("format", "json").
		DoRaw(ctx)
	if err != nil {
		// The pods/proxy subresource reaches the pod on its primary-family
		// PodIP, so this hop used to be family-sensitive. It no longer is: both
		// admin listeners bind "::" with ipv4_compat, which accepts connections
		// from either family. A failure here means the admin interface is not
		// answering — the container is not up, or the proxy path is blocked.
		t.Fatalf("reading Envoy admin /listeners from %s/%s: %v", namespace, pod, err)
	}

	var listeners envoyListeners
	if err := json.Unmarshal(raw, &listeners); err != nil {
		t.Fatalf("decoding /listeners response %q: %v", raw, err)
	}
	if len(listeners.ListenerStatuses) == 0 {
		t.Fatalf("Envoy in %s/%s reports no listeners at all. Body: %s", namespace, pod, raw)
	}

	bound := map[string][]envoySocketAddress{}
	for _, ls := range listeners.ListenerStatuses {
		sockets := []envoySocketAddress{ls.LocalAddress.SocketAddress}
		for _, extra := range ls.AdditionalLocalAddresses {
			sockets = append(sockets, extra.SocketAddress)
		}
		bound[ls.Name] = sockets
	}
	return bound
}

// envoyConfiguredSockets reads a gateway's Envoy admin /config_dump and returns,
// per listener name, the socket addresses as *configured*: the primary first,
// then any additional_addresses.
//
// It is the companion to envoyBoundSockets, not a replacement. /listeners
// answers "what did this listener actually bind", which is the only place a
// failed bind shows up; /config_dump answers "with what settings", which is the
// only place socket options like ipv4_compat appear at all. Assert each from the
// endpoint that reports it.
func envoyConfiguredSockets(t *testing.T, ctx context.Context, namespace, pod string, adminPort int) map[string][]envoyConfiguredSocket {
	t.Helper()
	raw, err := e2e.GetClients().K8s.CoreV1().RESTClient().Get().
		Namespace(namespace).
		Resource("pods").
		Name(pod + ":" + strconv.Itoa(adminPort)).
		SubResource("proxy").
		Suffix("config_dump").
		DoRaw(ctx)
	if err != nil {
		t.Fatalf("reading Envoy admin /config_dump from %s/%s: %v", namespace, pod, err)
	}

	var dump envoyConfigDump
	if err := json.Unmarshal(raw, &dump); err != nil {
		t.Fatalf("decoding /config_dump response from %s/%s: %v", namespace, pod, err)
	}

	flatten := func(l envoyListenerConfig) []envoyConfiguredSocket {
		sockets := []envoyConfiguredSocket{l.Address.SocketAddress}
		for _, extra := range l.AdditionalAddresses {
			sockets = append(sockets, extra.Address.SocketAddress)
		}
		return sockets
	}
	configured := map[string][]envoyConfiguredSocket{}
	for _, c := range dump.Configs {
		if !strings.Contains(c.Type, "ListenersConfigDump") {
			continue
		}
		for _, sl := range c.StaticListeners {
			configured[sl.Listener.Name] = flatten(sl.Listener)
		}
		for _, dl := range c.DynamicListeners {
			configured[dl.ActiveState.Listener.Name] = flatten(dl.ActiveState.Listener)
		}
	}
	if len(configured) == 0 {
		t.Fatalf("Envoy in %s/%s reports no listeners in /config_dump. Body: %s", namespace, pod, raw)
	}
	return configured
}

func mustReadyPodName(t *testing.T, ctx context.Context, namespace, label string) string {
	t.Helper()
	pods, err := e2e.GetClients().K8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: label,
	})
	if err != nil {
		t.Fatalf("listing %s pods in %s: %v", label, namespace, err)
	}
	for i := range pods.Items {
		if portforward.IsPodReady(&pods.Items[i]) {
			return pods.Items[i].Name
		}
	}
	t.Fatalf("no ready %s pod in %s", label, namespace)
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
