//go:build linux

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

package ateomnet

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/google/nftables"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// withTestNetNS runs fn with the calling thread inside a throwaway netns
// standing in for the worker pod's, and hands it a second throwaway netns
// standing in for an actor's interior one.
//
// Both are anonymous (netns.New, not NewNamed) so the test leaves nothing behind
// in /run/netns, and every link, sysctl, and nftables table SetupActorNetwork
// touches is scoped to a namespace that disappears with the test rather than to
// the machine running it.
func withTestNetNS(t *testing.T, fn func(interior netns.NsHandle)) {
	t.Helper()

	// Locked for the whole body: netns is a per-thread property, so an unlocked
	// goroutine could be rescheduled onto a thread still in the original
	// namespace midway through. It also means a t.Fatal inside fn tears the
	// thread down instead of returning it to the pool mis-configured.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	orig, err := netns.Get()
	if err != nil {
		t.Fatalf("getting current netns: %v", err)
	}
	defer orig.Close()
	// Registered before the namespaces below so it runs after they are closed.
	defer func() {
		if err := netns.Set(orig); err != nil {
			t.Errorf("restoring original netns: %v", err)
		}
	}()

	pod, err := netns.New() // netns.New switches the thread into the new namespace
	if err != nil {
		t.Fatalf("creating pod netns: %v", err)
	}
	defer pod.Close()
	interior, err := netns.New()
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	defer interior.Close()
	if err := netns.Set(pod); err != nil {
		t.Fatalf("entering pod netns: %v", err)
	}

	fn(interior)
}

// addPodEth0 plants a dummy link carrying cidrs in the current netns, standing
// in for the worker pod's own primary interface. withTestNetNS hands out a bare
// namespace, and the families on that interface are what SetupActorNetwork reads
// to decide the families the actor gets.
//
// The name has to be exactly "eth0": the probe is link-scoped, so under any
// other name it answers false and the test asserts the opposite of what it
// means to. It collides with ActorVethName by design -- that collision is the
// reason the probe cannot just scan the namespace.
func addPodEth0(t *testing.T, cidrs ...string) {
	t.Helper()

	link := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "eth0"}}
	if err := netlink.LinkAdd(link); err != nil {
		t.Fatalf("creating the stand-in pod eth0: %v", err)
	}
	if err := netlink.LinkSetUp(link); err != nil {
		t.Fatalf("bringing up the stand-in pod eth0: %v", err)
	}
	for _, cidr := range cidrs {
		addr := MustParseAddr(cidr)
		addr.Flags |= unix.IFA_F_NODAD // else an IPv6 address stays tentative
		if err := netlink.AddrAdd(link, addr); err != nil {
			t.Fatalf("assigning %s to the stand-in pod eth0: %v", cidr, err)
		}
	}
}

// requireNftables skips when the kernel in this environment cannot serve the
// nftables netlink API at all, which SetupActorNetwork needs and which is a
// property of the machine rather than of the code under test.
func requireNftables(t *testing.T) {
	t.Helper()
	c := &nftables.Conn{}
	if _, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4); err != nil {
		t.Skipf("nftables unavailable in this environment: %v", err)
	}
}

// actorNftTableExists reports whether the actor table is present in the family
// InstallActorNftablesRules creates it in. The family is load-bearing:
// ListTablesOfFamily puts it in the netlink dump header, so the kernel filters
// the dump and a query for the wrong family comes back empty rather than
// erroring.
func actorNftTableExists(t *testing.T) bool {
	t.Helper()
	c := &nftables.Conn{}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyINet)
	if err != nil {
		t.Fatalf("listing inet nftables tables: %v", err)
	}
	for _, table := range tables {
		if table.Name == ActorNftTableName {
			return true
		}
	}
	return false
}

// linkByName returns the link, or nil when it does not exist.
func linkByName(t *testing.T, name string) netlink.Link {
	t.Helper()
	link, err := netlink.LinkByName(name)
	if err == nil {
		return link
	}
	if _, ok := errors.AsType[netlink.LinkNotFoundError](err); ok {
		return nil
	}
	t.Fatalf("looking up link %q: %v", name, err)
	return nil
}

func hasAddr(t *testing.T, link netlink.Link, cidr string) bool {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("listing addresses of %q: %v", link.Attrs().Name, err)
	}
	want := MustParseAddr(cidr)
	for _, addr := range addrs {
		if addr.IPNet != nil && addr.IPNet.String() == want.IPNet.String() {
			return true
		}
	}
	return false
}

// assertIPv6AddrNoDAD requires cidr to be present on link and to carry
// IFA_F_NODAD.
//
// The flag is the whole point: the ateom container is unprivileged, so the
// accept_dad sysctl this replaced could not be written and setup failed outright
// on a real worker. It passes as root, where /proc/sys is writable either way,
// so nothing else here would catch a regression back to the sysctl.
func assertIPv6AddrNoDAD(t *testing.T, link netlink.Link, cidr string) {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V6)
	if err != nil {
		t.Fatalf("listing IPv6 addresses of %q: %v", link.Attrs().Name, err)
	}
	want := MustParseAddr(cidr)
	for _, addr := range addrs {
		if addr.IPNet == nil || addr.IPNet.String() != want.IPNet.String() {
			continue
		}
		if addr.Flags&unix.IFA_F_NODAD == 0 {
			t.Errorf("%s on %q has flags %#x, want IFA_F_NODAD (%#x) set", cidr, link.Attrs().Name, addr.Flags, unix.IFA_F_NODAD)
		}
		return
	}
	t.Errorf("%q does not carry %s, got %v", link.Attrs().Name, cidr, addrs)
}

// assertDefaultRoute requires link to carry -- or, when want is false, to not
// carry -- a default route via gw in the given family.
//
// The IPv6 case is the half that matters most: it is the ::/0 route, not the
// address, that lets Go's RFC 6724 sorting find a source for a AAAA and put it
// first.
func assertDefaultRoute(t *testing.T, link netlink.Link, family int, gw net.IP, want bool) {
	t.Helper()

	dst := "0.0.0.0/0"
	if family == netlink.FAMILY_V6 {
		dst = "::/0"
	}
	routes, err := netlink.RouteList(link, family)
	if err != nil {
		t.Fatalf("listing %s routes of %q: %v", dst, link.Attrs().Name, err)
	}
	var got bool
	for _, route := range routes {
		// A default route reports its destination either as nil or as an
		// explicit zero-length mask, depending on how the kernel rendered it.
		ones := 0
		if route.Dst != nil {
			ones, _ = route.Dst.Mask.Size()
		}
		if ones == 0 && route.Gw.Equal(gw) {
			got = true
		}
	}
	switch {
	case want && !got:
		t.Errorf("%q has no %s route via %s, got %v", link.Attrs().Name, dst, gw, routes)
	case !want && got:
		t.Errorf("%q has a %s route via %s, want none, got %v", link.Attrs().Name, dst, gw, routes)
	}
}

// TestSetupActorNetworkFinalState pins the namespace state gVisor and the
// micro-VM guest read after an activation: what links exist, where, with which
// addresses and routes. It deliberately asserts the end state rather than the
// sequence of netlink calls that produced it, so the setup path stays free to
// get there differently (as it did when the veth peer stopped being created in
// the pod netns and moved across).
func TestSetupActorNetworkFinalState(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		// A dual-stack pod: the actor gets IPv6 only because this does.
		addPodEth0(t, "10.244.0.7/24", "fd00:10:244::7/64")

		if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		// Worker pod side: the gateway end of the point-to-point link.
		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if !hasAddr(t, host, HostVethCIDR) {
			t.Errorf("host veth %q does not carry %s", HostVethName, HostVethCIDR)
		}
		if host.Attrs().Flags&1 == 0 { // net.FlagUp
			t.Errorf("host veth %q is not up", HostVethName)
		}
		assertIPv6AddrNoDAD(t, host, HostVethIPv6CIDR)

		// The actor interface must exist ONLY in the interior netns. A peer left
		// in the pod netns would mean the pair was built the old way -- and since
		// ActorVethName is "eth0", it would land on top of the pod's own
		// interface. What must still answer to that name here is the stand-in.
		switch stray := linkByName(t, ActorVethName); {
		case stray == nil:
			t.Errorf("the stand-in pod %q disappeared", ActorVethName)
		case stray.Type() != "dummy":
			t.Errorf("%q in the pod netns is a %s, want the stand-in dummy: the veth peer was left behind", ActorVethName, stray.Type())
		}

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			actor := linkByName(t, ActorVethName)
			if actor == nil {
				t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
			}
			if !hasAddr(t, actor, ActorVethCIDR) {
				t.Errorf("actor veth %q does not carry %s", ActorVethName, ActorVethCIDR)
			}
			if actor.Attrs().Flags&1 == 0 {
				t.Errorf("actor veth %q is not up", ActorVethName)
			}
			assertIPv6AddrNoDAD(t, actor, ActorVethIPv6CIDR)

			if lo := linkByName(t, "lo"); lo == nil {
				t.Error("interior netns has no loopback")
			} else if lo.Attrs().Flags&1 == 0 {
				t.Error("interior loopback is not up")
			}

			assertDefaultRoute(t, actor, netlink.FAMILY_V4, ActorVethGwIP, true)
			assertDefaultRoute(t, actor, netlink.FAMILY_V6, ActorVethIPv6GwIP, true)
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// disableIPv6 turns IPv6 off in the current netns the way an IPv4-only cluster
// does, so a link created afterwards rejects every IPv6 address.
func disableIPv6(t *testing.T) {
	t.Helper()
	for _, knob := range []string{"all", "default"} {
		path := "/proc/sys/net/ipv6/conf/" + knob + "/disable_ipv6"
		if err := os.WriteFile(path, []byte("1\n"), 0o644); err != nil {
			t.Skipf("cannot disable IPv6 in this netns (%s): %v", path, err)
		}
	}
}

// disableIPv6ForNewLinks turns IPv6 off for links created after it runs and
// leaves the ones that already exist -- and their addresses -- alone.
//
// disableIPv6 cannot stand in for it: writing "all" flushes the pod eth0's IPv6
// address too, so both halves of the gate go false at once and either one alone
// would produce the same result.
func disableIPv6ForNewLinks(t *testing.T) {
	t.Helper()
	const path = "/proc/sys/net/ipv6/conf/default/disable_ipv6"
	if err := os.WriteFile(path, []byte("1\n"), 0o644); err != nil {
		t.Skipf("cannot disable IPv6 for new links in this netns (%s): %v", path, err)
	}
}

// assertNoIPv6Addr requires link to carry no IPv6 address at all.
func assertNoIPv6Addr(t *testing.T, link netlink.Link) {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V6)
	if err != nil {
		t.Fatalf("listing IPv6 addresses of %q: %v", link.Attrs().Name, err)
	}
	if len(addrs) != 0 {
		t.Errorf("%q carries IPv6 addresses %v in an IPv4-only netns, want none", link.Attrs().Name, addrs)
	}
}

// assertNoGlobalIPv6Addr requires link to carry no IPv6 address beyond the
// fe80::/64 the kernel gives every up link wherever IPv6 is enabled at all.
// That link-local is not what strands an actor -- the routable address is --
// and only turning IPv6 off for the whole netns suppresses it, which is why
// assertNoIPv6Addr does not fit a pod that merely has no IPv6 of its own.
func assertNoGlobalIPv6Addr(t *testing.T, link netlink.Link) {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V6)
	if err != nil {
		t.Fatalf("listing IPv6 addresses of %q: %v", link.Attrs().Name, err)
	}
	for _, addr := range addrs {
		if addr.IP.IsGlobalUnicast() && !addr.IP.IsLinkLocalUnicast() {
			t.Errorf("%q carries global IPv6 address %s, want none", link.Attrs().Name, addr)
		}
	}
}

// TestSetupActorNetworkIPv4OnlyNetNS covers a worker pod whose netns has IPv6
// disabled outright, which is the default on an IPv4-only GKE cluster. Writing
// the "all" knob also flushes the addresses already assigned, so both halves of
// the gate are false here and the actor must come up IPv4-only. The IPv4 half
// must come up exactly as it does with IPv6 available -- an ungated attempt
// fails with EPERM on the path of every SetupActorNetwork call site, and the
// actor never leaves ResumeGoldenActor.
func TestSetupActorNetworkIPv4OnlyNetNS(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		disableIPv6(t)
		if err := NetNSDo(ctx, interior, func(context.Context) error {
			disableIPv6(t)
			return nil
		}); err != nil {
			t.Fatalf("disabling IPv6 in the interior netns: %v", err)
		}

		if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
			t.Fatalf("SetupActorNetwork on an IPv4-only netns: %v", err)
		}

		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if !hasAddr(t, host, HostVethCIDR) {
			t.Errorf("host veth %q does not carry %s", HostVethName, HostVethCIDR)
		}
		if host.Attrs().Flags&1 == 0 { // net.FlagUp
			t.Errorf("host veth %q is not up", HostVethName)
		}
		assertNoIPv6Addr(t, host)

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			actor := linkByName(t, ActorVethName)
			if actor == nil {
				t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
			}
			if !hasAddr(t, actor, ActorVethCIDR) {
				t.Errorf("actor veth %q does not carry %s", ActorVethName, ActorVethCIDR)
			}
			if actor.Attrs().Flags&1 == 0 {
				t.Errorf("actor veth %q is not up", ActorVethName)
			}
			assertNoIPv6Addr(t, actor)

			assertDefaultRoute(t, actor, netlink.FAMILY_V4, ActorVethGwIP, true)
			assertDefaultRoute(t, actor, netlink.FAMILY_V6, ActorVethIPv6GwIP, false)
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestSetupActorNetworkNoPodIPv6 covers the case that turned the IPv4 e2e job
// red: a worker pod on an IPv4-only cluster whose kernel has IPv6 compiled in.
// Nothing is disabled, so every capability probe says yes, but the pod's own
// eth0 carries no IPv6 and there is nowhere for an actor's IPv6 packet to go.
// Giving the actor an address and a ::/0 route anyway makes it prefer the AAAA
// of any dual-stack destination and the connection dies mid-response.
func TestSetupActorNetworkNoPodIPv6(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		addPodEth0(t, "10.244.0.7/24")

		if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
			t.Fatalf("SetupActorNetwork on a pod without IPv6: %v", err)
		}

		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if !hasAddr(t, host, HostVethCIDR) {
			t.Errorf("host veth %q does not carry %s", HostVethName, HostVethCIDR)
		}
		assertNoGlobalIPv6Addr(t, host)

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			actor := linkByName(t, ActorVethName)
			if actor == nil {
				t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
			}
			if !hasAddr(t, actor, ActorVethCIDR) {
				t.Errorf("actor veth %q does not carry %s", ActorVethName, ActorVethCIDR)
			}
			// The interior netns is created fresh, so its own sysctls always say
			// IPv6 is available whatever the pod's families are. Only a decision
			// carried across from the pod netns can get this right.
			assertNoGlobalIPv6Addr(t, actor)
			assertDefaultRoute(t, actor, netlink.FAMILY_V4, ActorVethGwIP, true)
			assertDefaultRoute(t, actor, netlink.FAMILY_V6, ActorVethIPv6GwIP, false)
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestSetupActorNetworkPerLinkIPv6Disabled covers the one case the capability
// half of the gate is there for: a dual-stack pod on a node that disables IPv6
// per link, so the veth created for the actor inherits disable_ipv6=1 while the
// pod's own eth0 keeps its address. Assigning IPv6 to that veth fails with
// EPERM, and because it happens on the path of every SetupActorNetwork call an
// ungated attempt takes actor startup from working to totally broken.
func TestSetupActorNetworkPerLinkIPv6Disabled(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		addPodEth0(t, "10.244.0.7/24", "fd00:10:244::7/64")
		disableIPv6ForNewLinks(t)

		if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
			t.Fatalf("SetupActorNetwork with IPv6 disabled per link: %v", err)
		}

		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if !hasAddr(t, host, HostVethCIDR) {
			t.Errorf("host veth %q does not carry %s", HostVethName, HostVethCIDR)
		}
		assertNoGlobalIPv6Addr(t, host)

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			actor := linkByName(t, ActorVethName)
			if actor == nil {
				t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
			}
			if !hasAddr(t, actor, ActorVethCIDR) {
				t.Errorf("actor veth %q does not carry %s", ActorVethName, ActorVethCIDR)
			}
			assertNoGlobalIPv6Addr(t, actor)
			assertDefaultRoute(t, actor, netlink.FAMILY_V6, ActorVethIPv6GwIP, false)
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestSetupActorNetworkPodIPv6Only is the other direction of the gate: a pod on
// an IPv6-only cluster must still get actor IPv6. A probe that reads the wrong
// link or the wrong scope fails closed, which every IPv4 test in this file would
// happily accept, so this and TestSetupActorNetworkFinalState are the two that
// notice.
//
// The actor still gets its IPv4 address and 0.0.0.0/0 route here, unconditionally
// and unusably -- the mirror image of the bug this gate fixes, left alone because
// nothing in the tree runs an actor on a v6-only cluster yet.
func TestSetupActorNetworkPodIPv6Only(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		addPodEth0(t, "fd00:10:244::7/64")

		if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
			t.Fatalf("SetupActorNetwork on an IPv6-only pod: %v", err)
		}

		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		assertIPv6AddrNoDAD(t, host, HostVethIPv6CIDR)

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			actor := linkByName(t, ActorVethName)
			if actor == nil {
				t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
			}
			assertIPv6AddrNoDAD(t, actor, ActorVethIPv6CIDR)
			assertDefaultRoute(t, actor, netlink.FAMILY_V6, ActorVethIPv6GwIP, true)
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestSetupActorNetworkIsRepeatable covers the activation cycle a reused worker
// runs: set up, tear down, set up again. The second setup has to succeed against
// whatever the first one left behind.
// TestRemoveActorNftablesRulesSweepsIPv4Family covers the upgrade case: a
// worker whose previous ateom created the actor table in the ip family. Table
// names are unique per family, so the inet-only cleanup this replaced could
// never see that table, and it would have kept redirecting alongside the inet
// one installed next to it.
func TestRemoveActorNftablesRulesSweepsIPv4Family(t *testing.T) {
	roottest.Require(t, "creating network namespaces and nftables rules")

	withTestNetNS(t, func(netns.NsHandle) {
		requireNftables(t)

		c := &nftables.Conn{}
		c.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: ActorNftTableName})
		if err := c.Flush(); err != nil {
			t.Fatalf("creating the stand-in ip actor table: %v", err)
		}

		if err := RemoveActorNftablesRules(); err != nil {
			t.Fatalf("RemoveActorNftablesRules: %v", err)
		}

		tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4)
		if err != nil {
			t.Fatalf("listing ip nftables tables: %v", err)
		}
		for _, table := range tables {
			if table.Name == ActorNftTableName {
				t.Fatal("the ip actor table survived cleanup")
			}
		}
	})
}

func TestSetupActorNetworkIsRepeatable(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		for i := range 3 {
			if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
				t.Fatalf("SetupActorNetwork (activation %d): %v", i, err)
			}
			if linkByName(t, HostVethName) == nil {
				t.Fatalf("host veth %q missing after activation %d", HostVethName, i)
			}
			if !actorNftTableExists(t) {
				t.Fatalf("nftables table %q missing after activation %d", ActorNftTableName, i)
			}
			if err := CleanupActorNetwork(ctx, interior); err != nil {
				t.Fatalf("CleanupActorNetwork (activation %d): %v", i, err)
			}
			// Install and teardown have to name the same family. When they do not,
			// teardown's dump comes back empty, its "missing tables are already
			// clean" path reports success, and the table survives -- so the next
			// activation stacks another copy of every chain and rule onto it and
			// the leak is invisible to every other assertion here.
			if actorNftTableExists(t) {
				t.Fatalf("nftables table %q survived cleanup after activation %d", ActorNftTableName, i)
			}
		}

		// Cleanup is idempotent: the extra call after the loop's last one must
		// still succeed, and both ends must be gone.
		if err := CleanupActorNetwork(ctx, interior); err != nil {
			t.Fatalf("CleanupActorNetwork on an already-clean network: %v", err)
		}
		if stray := linkByName(t, HostVethName); stray != nil {
			t.Errorf("host veth %q survived cleanup", HostVethName)
		}
		if actorNftTableExists(t) {
			t.Errorf("nftables table %q survived a repeated cleanup", ActorNftTableName)
		}
		if err := NetNSDo(ctx, interior, func(context.Context) error {
			if stray := linkByName(t, ActorVethName); stray != nil {
				t.Errorf("actor veth %q survived cleanup", ActorVethName)
			}
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestSetupActorNetworkHostVethHWAddr covers the micro-VM requirement: a CH
// snapshot freezes the guest's ARP entry for the gateway, so the worker-side
// veth MAC has to be exactly the one the caller asked for, on every pod.
func TestSetupActorNetworkHostVethHWAddr(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		want := MustParseMAC("02:a8:1e:00:00:01")
		if err := SetupActorNetwork(ctx, NetworkConfig{
			InteriorNetNS:      interior,
			HostVethHWAddr:     want,
			SweepInteriorLinks: true,
		}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if got := host.Attrs().HardwareAddr.String(); got != want.String() {
			t.Errorf("host veth MAC = %s, want %s", got, want)
		}
	})
}

// TestSetupActorNetworkSweepsInteriorLinks covers the other half of the micro-VM
// path: SweepInteriorLinks clears a previous activation's leftovers (kata's tap
// device) before the new pair is created, and must not take the loopback or the
// freshly created actor veth with it.
func TestSetupActorNetworkSweepsInteriorLinks(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		const leftover = "stale-tap0"
		if err := NetNSDo(ctx, interior, func(context.Context) error {
			return netlink.LinkAdd(&netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: leftover}})
		}); err != nil {
			t.Fatalf("planting a leftover interior link: %v", err)
		}

		if err := SetupActorNetwork(ctx, NetworkConfig{
			InteriorNetNS:      interior,
			SweepInteriorLinks: true,
		}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			if stray := linkByName(t, leftover); stray != nil {
				t.Errorf("leftover interior link %q was not swept", leftover)
			}
			if linkByName(t, ActorVethName) == nil {
				t.Errorf("actor veth %q missing after a sweeping setup", ActorVethName)
			}
			if linkByName(t, "lo") == nil {
				t.Error("sweep removed the interior loopback")
			}
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}
