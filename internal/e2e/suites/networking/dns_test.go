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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The actor zone is served by a CoreDNS `template` block, which answers for any
// name matching <actor>.<atespace>.<suffix> whether or not that actor exists.
// These tests therefore need no actor fixture — they are asserting the zone's
// behavior, not an actor's.
func probeActorDNSName() string {
	return resources.ActorRef{Atespace: networkingAtespace, Name: "dns-probe"}.DNSName()
}

func mustDNSClient(t *testing.T, ctx context.Context) *e2e.DNSClient {
	t.Helper()
	dns, err := e2e.NewDNSClient(ctx)
	if err != nil {
		t.Fatalf("NewDNSClient: %v", err)
	}
	t.Cleanup(dns.Close)
	return dns
}

// TestActorDNSZone asserts that the actor zone answers an A query with the
// router's ClusterIP, and — the part no other test covers — that everything
// else it is asked returns a *benign* rcode rather than SERVFAIL.
//
// The rcode matters more than the missing record. NODATA is what every stub
// resolver expects for "this name has no address in this family"; SERVFAIL is a
// transport fault, and resolvers disagree about it. musl treats a SERVFAIL on
// either parallel query as a hard failure, so an Alpine-based client cannot
// resolve an actor name at all; glibc retries and pays the resolver timeout; and
// unlike NODATA it cannot be negatively cached, so every request pays again.
// Go's resolver masks it, which is why nothing in this repo has noticed.
//
// This test is family-agnostic and is expected to run, not skip, on a
// single-stack cluster.
func TestActorDNSZone(t *testing.T) {
	ctx := context.Background()
	dns := mustDNSClient(t, ctx)

	routerV4, _, err := e2e.RouterClusterIPs(ctx)
	if err != nil {
		t.Fatalf("reading atenet-router ClusterIPs: %v", err)
	}

	name := probeActorDNSName()

	t.Run("A answers with the router ClusterIP", func(t *testing.T) {
		if routerV4 == "" {
			// A v6-only cluster: there is no IPv4 ClusterIP to answer with, and
			// emitting an A record at all would be the bug.
			t.Skip("atenet-router has no IPv4 ClusterIP")
		}
		addrs, rcode, err := dns.Lookup(ctx, "ip4", name)
		if rcode != e2e.DNSAnswered {
			t.Fatalf("A %s: %v (%v); want the router ClusterIP %s", name, rcode, err, routerV4)
		}
		if !slices.Contains(addrs, routerV4) {
			t.Fatalf("A %s = %v; want it to contain the atenet-router ClusterIP %s", name, addrs, routerV4)
		}
	})

	t.Run("AAAA is not a server failure", func(t *testing.T) {
		// On a single-stack cluster the right answer is NODATA. This subtest
		// fails today, because the zone's only template is `template IN A`: a
		// qtype mismatch falls through to a plugin chain with nothing after it,
		// and plugin.NextOrFailure with a nil Next returns SERVFAIL.
		_, rcode, err := dns.Lookup(ctx, "ip6", name)
		if rcode == e2e.DNSFailed {
			t.Fatalf("AAAA %s: %v (%v); want NODATA. Every non-A qtype in this zone "+
				"SERVFAILs, which breaks musl-based clients on IPv4 clusters too", name, rcode, err)
		}
	})

	t.Run("a name outside the actor pattern is not a server failure", func(t *testing.T) {
		// A single-label name inside the zone: the zone matches, the qtype
		// matches, the actor regex does not. Without `fallthrough` on the
		// template the plugin returns SERVFAIL rather than NXDOMAIN. Also fails
		// today, and also on IPv4.
		bogus := "not-an-actor." + resources.ActorDNSSuffix
		_, rcode, err := dns.Lookup(ctx, "ip4", bogus)
		if rcode == e2e.DNSFailed {
			t.Fatalf("A %s: %v (%v); want NXDOMAIN", bogus, rcode, err)
		}
	})
}

// TestActorDNSAAAA asserts the zone publishes the router's IPv6 ClusterIP.
//
// Skipped unless the atenet-router Service actually has one, which is the
// steady state on every single-stack cluster: a Service with no
// ipFamilyPolicy is SingleStack and never gets a second ClusterIP, so there is
// nothing an AAAA could correctly point at.
//
// Deliberately separate from TestActorDNSZone: it is the only assertion here
// whose expected result changes when the cluster becomes dual-stack, so keeping
// it its own function lets a dual-stack CI job exclude it while the AAAA
// generator is still in flight.
func TestActorDNSAAAA(t *testing.T) {
	ctx := context.Background()

	_, routerV6, err := e2e.RouterClusterIPs(ctx)
	if err != nil {
		t.Fatalf("reading atenet-router ClusterIPs: %v", err)
	}
	if routerV6 == "" {
		t.Skip("atenet-router has no IPv6 ClusterIP; single-stack cluster, nothing to publish")
	}

	dns := mustDNSClient(t, ctx)
	name := probeActorDNSName()

	addrs, rcode, err := dns.Lookup(ctx, "ip6", name)
	if rcode != e2e.DNSAnswered {
		t.Fatalf("AAAA %s: %v (%v); want the router IPv6 ClusterIP %s", name, rcode, err, routerV6)
	}
	if !slices.Contains(addrs, routerV6) {
		t.Fatalf("AAAA %s = %v; want it to contain the atenet-router IPv6 ClusterIP %s", name, addrs, routerV6)
	}
}
