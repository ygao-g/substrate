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
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// egressListener is the name of the gateway's :443 listener in
// manifests/ate-install/atenet-egress.yaml. Unlike the router's ingress
// listeners it is static config, not xDS.
const egressListener = "egress"

// TestEgressListenerAddresses asserts that the egress gateway's :443 listener
// took the single-socket shape, from Envoy's own view of itself.
//
// The gateway is the mirror image of the router and needs its own test for it.
// Where an ingress listener pairs a 0.0.0.0 primary with a "::" additional
// address, this one is a lone socket, so it binds "::" with ipv4_compat *set* --
// the opposite polarity, for the opposite reason. Getting the two confused is
// the failure this pins: ipv4_compat here is what lets an IPv4 actor reach the
// gateway at all, and dropping it would strand every actor on a v4-primary
// cluster while the listener still looked healthy.
func TestEgressListenerAddresses(t *testing.T) {
	ctx := context.Background()
	pod := mustReadyPodName(t, ctx, e2e.EgressNamespace, e2e.EgressAppLabel)
	bound := envoyBoundSockets(t, ctx, e2e.EgressNamespace, pod, e2e.EgressAdminPort)
	configured := envoyConfiguredSockets(t, ctx, e2e.EgressNamespace, pod, e2e.EgressAdminPort)

	sockets, ok := bound[egressListener]
	if !ok {
		t.Fatalf("egress gateway has no %q listener bound; listeners present: %v",
			egressListener, slices.Sorted(maps.Keys(bound)))
	}
	if len(sockets) != 1 {
		t.Errorf("%s is bound on %d sockets, want 1: %v. It is static config with no "+
			"additional_addresses; a second socket means the manifest grew the ingress shape",
			egressListener, len(sockets), sockets)
	}

	s := sockets[0]
	if s.Address != "::" {
		t.Errorf("%s is bound on %q, want \"::\". A 0.0.0.0 bind leaves an IPv6-primary "+
			"actor with no path for its CONNECT", egressListener, s.Address)
	}

	// ipv4_compat has to come from config_dump: /listeners reports the resolved
	// bound address, which never carries it.
	var v6Socket *envoyConfiguredSocket
	for i, c := range configured[egressListener] {
		if c.Address == "::" {
			v6Socket = &configured[egressListener][i]
			break
		}
	}
	if v6Socket == nil {
		t.Fatalf("%s has no :: socket in /config_dump; configured sockets: %+v",
			egressListener, configured[egressListener])
	}
	if !v6Socket.Ipv4Compat {
		t.Errorf("%s has ipv4_compat unset on its :: socket (port %d); want true. Envoy "+
			"sets IPV6_V6ONLY without it, and unlike the ingress listeners there is no "+
			"0.0.0.0 peer socket here to catch IPv4 actors -- they would simply be refused",
			egressListener, v6Socket.PortValue)
	}
	t.Logf("%s bound on [%s]:%d, configured ipv4_compat=%v",
		egressListener, s.Address, s.PortValue, v6Socket.Ipv4Compat)
}

// TestEgressServesIPv4Client drives one real actor request through the gateway
// and then reads the listener's connection counter back out of Envoy.
//
// This is the assertion that only a dual-stack cluster can make. The egress
// listener requires a client certificate, so no probe pod can reach it -- the
// client has to be an actor. On a dual-stack cluster an actor pod is
// IPv4-primary, so a connection arriving at a socket bound "::" proves the
// v4-mapped path through ipv4_compat works end to end. On a single-stack
// cluster the same traffic proves nothing about families, so it is skipped
// rather than left to pass vacuously.
//
// It drives its own traffic rather than leaning on TestActorEgress: Go runs
// tests in source order, this file sorts before networking_test.go, and a
// counter that happens to be non-zero because some earlier test ran is not
// evidence.
func TestEgressServesIPv4Client(t *testing.T) {
	ctx := context.Background()

	dual, err := e2e.ClusterIsDualStack(ctx)
	if err != nil {
		t.Fatalf("determining the cluster's address families: %v", err)
	}
	if !dual {
		t.Skip("single-stack cluster; an actor's connection here says nothing about ipv4_compat")
	}

	pod := mustReadyPodName(t, ctx, e2e.EgressNamespace, e2e.EgressAppLabel)
	before := egressDownstreamConnections(t, ctx, pod)

	actorName, actor := createAndResumeActor(t, ctx, "egressfamily", egressTemplate)

	// The client here is atunnel in the actor's worker pod, and it has to be
	// IPv4-primary for this to mean what the test says it means. Assert that
	// rather than assume it: on a v6-primary dual-stack cluster the request
	// would take the plain IPv6 path and the ipv4_compat claim would go untested
	// while the test still passed.
	clientV4 := mustWorkerPodIPv4(t, ctx, actor)

	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	response, err := router.PostJSON(ctx, actorRef, "/", []byte(`{"url":"http://example.com/"}`))
	if err != nil {
		t.Fatalf("POST to egress Actor through ingress: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading egress response body (HTTP %d): %v", response.StatusCode, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Actor egress fetch returned HTTP %d, want 200; body: %s", response.StatusCode, body)
	}

	after := egressDownstreamConnections(t, ctx, pod)
	if after <= before {
		t.Errorf("egress listener downstream_cx_total went %d -> %d after an actor at %s fetched "+
			"successfully; want it to rise. The actor reached its destination without transiting "+
			"the gateway, so the :443 socket was never exercised", before, after, clientV4)
	}
	t.Logf("actor %s transited the gateway from IPv4 %s; downstream_cx_total %d -> %d",
		actorName, clientV4, before, after)
}

// egressDownstreamConnections totals the egress listener's downstream_cx_total
// across whatever Envoy named the stat. The listener stat prefix is derived
// from the bound address ("[__]_443" for [::]:443) and is not worth pinning
// separately from the address assertion above.
func egressDownstreamConnections(t *testing.T, ctx context.Context, pod string) int {
	t.Helper()
	raw, err := e2e.GetClients().K8s.CoreV1().RESTClient().Get().
		Namespace(e2e.EgressNamespace).
		Resource("pods").
		Name(pod+":"+strconv.Itoa(e2e.EgressAdminPort)).
		SubResource("proxy").
		Suffix("stats").
		Param("filter", `^listener\..*\.downstream_cx_total$`).
		DoRaw(ctx)
	if err != nil {
		t.Fatalf("reading Envoy admin /stats from %s/%s: %v", e2e.EgressNamespace, pod, err)
	}

	total := 0
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		_, value, ok := strings.Cut(line, ": ")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			t.Fatalf("parsing Envoy stat line %q: %v", line, err)
		}
		total += n
	}
	return total
}

// mustWorkerPodIPv4 returns the IPv4 address of the worker pod the Actor was
// assigned to, which is where its egress connection originates.
func mustWorkerPodIPv4(t *testing.T, ctx context.Context, actor *ateapipb.Actor) string {
	t.Helper()
	namespace := actor.GetWorkerAssignment().GetWorkerNamespace()
	name := actor.GetWorkerAssignment().GetWorkerPod()
	if namespace == "" || name == "" {
		t.Fatalf("resumed Actor has no worker pod assignment: %+v", actor)
	}
	pod, err := e2e.GetClients().K8s.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting worker pod %s/%s: %v", namespace, name, err)
	}
	v4, v6 := e2e.PodIPsByFamily(pod)
	if v4 == "" {
		t.Fatalf("worker pod %s/%s has no IPv4 address (v6=%q), so its connection to the gateway "+
			"cannot demonstrate ipv4_compat", namespace, name, v6)
	}
	return v4
}
