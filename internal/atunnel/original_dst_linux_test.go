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

package atunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/roottest"
)

// originalDstFamily parameterizes the redirect test. The wiring is the same in
// every case; only the addresses, the nftables table family, the source-address
// matcher and the sockets on either end differ.
type originalDstFamily struct {
	name string
	// listenNetwork and listenIP describe the worker's listeners; an empty
	// listenIP binds the unspecified address. dialNetwork is what the actor
	// dials with, which is not always the same family the listener was opened
	// as.
	listenNetwork string
	listenIP      string
	dialNetwork   string
	nftFamily     nftables.TableFamily
	workerIP      string
	actorIP       string
	mask          net.IPMask
	addrFlags     int
	sourceEqual   func(string) []expr.Any
}

// Fixed addresses are safe because each test builds its own namespaces.
var originalDstFamilies = []originalDstFamily{
	{
		name:          "IPv4",
		listenNetwork: "tcp4",
		listenIP:      "198.18.0.1",
		dialNetwork:   "tcp4",
		nftFamily:     nftables.TableFamilyIPv4,
		workerIP:      "198.18.0.1",
		actorIP:       "198.18.0.2",
		mask:          net.CIDRMask(30, 32),
		sourceEqual:   ateomnet.IPSourceEqual,
	},
	{
		name:          "IPv6",
		listenNetwork: "tcp6",
		listenIP:      "fd00:198:18::1",
		dialNetwork:   "tcp6",
		nftFamily:     nftables.TableFamilyIPv6,
		workerIP:      "fd00:198:18::1",
		actorIP:       "fd00:198:18::2",
		mask:          net.CIDRMask(64, 128),
		// The veth is alone in a throwaway namespace, so nothing can collide
		// with it. Skipping DAD lets the listener bind straight away instead of
		// waiting out the tentative period.
		addrFlags:   unix.IFA_F_NODAD,
		sourceEqual: ipv6SourceEqual,
	},
	{
		// atunnel listens on an unspecified address, which Go opens as one
		// dual-stack AF_INET6 socket, so an IPv4 actor arrives there with a
		// v4-mapped local address and its original destination is still only
		// readable through the IPv4 socket option. This is the shape production
		// runs in and the one the family check exists for.
		name:          "DualStackV4Mapped",
		listenNetwork: "tcp",
		dialNetwork:   "tcp4",
		nftFamily:     nftables.TableFamilyIPv4,
		workerIP:      "198.18.0.1",
		actorIP:       "198.18.0.2",
		mask:          net.CIDRMask(30, 32),
		sourceEqual:   ateomnet.IPSourceEqual,
	},
}

// ipv6SourceEqual matches an IPv6 source address, mirroring what
// ateomnet.IPSourceEqual does for IPv4. It lives here because ateomnet has no
// IPv6 equivalent yet; fold it in once the actor's rules go dual-stack.
func ipv6SourceEqual(ip string) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.NFPROTO_IPV6}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 8, Len: 16},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: net.ParseIP(ip).To16()},
	}
}

// TestTCPOriginalDestinationRedirect models the production egress path: actor
// traffic enters the worker namespace over a veth, an nftables PREROUTING rule
// redirects it to a local listener, and that listener asks the kernel what the
// actor originally dialed. Redirecting a locally generated connection through
// OUTPUT would not exercise the same path.
//
// The worker side gets its own namespace rather than borrowing the host's. That
// keeps the test clear of any local firewall policy — a default-deny INPUT
// chain would otherwise drop the redirected SYN — and the veth and nftables
// table go away with the namespace instead of needing to be swept up.
func TestTCPOriginalDestinationRedirect(t *testing.T) {
	roottest.Require(t, "CAP_NET_ADMIN + CAP_SYS_ADMIN for network namespaces and an nftables REDIRECT rule")

	for _, family := range originalDstFamilies {
		t.Run(family.name, func(t *testing.T) {
			workerNS := newTestNetNS(t)
			actorNS := newTestNetNS(t)
			requireNftables(t, workerNS)
			setupTestVeth(t, family, workerNS, actorNS)

			// targetListener holds the port the actor means to reach, so the
			// assertion below cannot pass by the connection simply arriving
			// where it was aimed. redirectListener stands in for atunnel's own
			// egress listener and is where the redirect must land instead.
			redirectListener := listenInNetNS(t, workerNS, family)
			targetListener := listenInNetNS(t, workerNS, family)
			targetPort := targetListener.Addr().(*net.TCPAddr).Port
			redirectPort := redirectListener.Addr().(*net.TCPAddr).Port
			installOriginalDstRedirect(t, family, workerNS, targetPort, redirectPort)

			target := net.JoinHostPort(family.workerIP, strconv.Itoa(targetPort))
			clientDone := make(chan error, 1)
			go func() {
				// From the actor's side this is an ordinary connection to the
				// worker's address; PREROUTING rewrites it on the way in.
				clientDone <- ateomnet.NetNSDo(context.Background(), actorNS, func(context.Context) error {
					conn, err := net.DialTimeout(family.dialNetwork, target, 10*time.Second)
					if err != nil {
						return err
					}
					return conn.Close()
				})
			}()

			if err := redirectListener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatalf("setting the accept deadline: %v", err)
			}
			redirected, err := redirectListener.Accept()
			if err != nil {
				t.Fatalf("accepting the redirected connection: %v", err)
			}
			defer redirected.Close()

			// The accepted socket is addressed to redirectListener; the kernel's
			// conntrack record must still hold what the actor dialed.
			got, err := TCPOriginalDestination(redirected)
			if err != nil {
				t.Fatalf("TCPOriginalDestination: %v", err)
			}
			if got != target {
				t.Errorf("original destination = %q, want %q", got, target)
			}
			if err := <-clientDone; err != nil {
				t.Fatalf("dialing through the redirect: %v", err)
			}
		})
	}
}

// TestTCPOriginalDestinationPreservesErrno covers the failure path on an
// ordinary connection that no REDIRECT rule touched. Each family's lookup
// misses and reports ENOENT, and that error must reach the caller rather than
// the EOPNOTSUPP a single-family socket answers for the other family's option.
// A dual-stack socket answers both with ENOENT, so the errno alone does not
// say which lookup ran; the family named in the message does.
//
// The dual-stack case is the shape production actually sees. atunnel listens
// on an unspecified address, which Go opens as an AF_INET6 socket with
// IPV6_V6ONLY off, so every IPv4 actor connection arrives with a v4-mapped
// local address and must still take the IPv4 lookup.
//
// These run in a fresh namespace because conntrack tracks loopback in any
// namespace that has nftables rules — including the one Docker runs in — and a
// tracked connection returns its real destination instead of missing.
func TestTCPOriginalDestinationPreservesErrno(t *testing.T) {
	roottest.Require(t, "CAP_SYS_ADMIN for a network namespace with no conntrack hooks")

	tests := []struct {
		name          string
		listenNetwork string
		listenAddress string
		dialNetwork   string
		dialHost      string
		// wantFamily is the family the error has to name. Both lookups miss
		// with the same errno, so this is the only thing that distinguishes
		// the one that ran from the one that should have.
		wantFamily string
	}{
		{name: "IPv4", listenNetwork: "tcp4", listenAddress: "127.0.0.1:0", dialNetwork: "tcp4", dialHost: "127.0.0.1", wantFamily: "IPv4"},
		{name: "IPv6", listenNetwork: "tcp6", listenAddress: "[::1]:0", dialNetwork: "tcp6", dialHost: "::1", wantFamily: "IPv6"},
		{name: "DualStackV4Mapped", listenNetwork: "tcp", listenAddress: ":0", dialNetwork: "tcp4", dialHost: "127.0.0.1", wantFamily: "IPv4"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ns := newTestNetNS(t)
			// Recorded rather than asserted in place: t.Skipf unwinds the
			// goroutine, and NetNSDo has the thread switched into another
			// namespace at that point.
			var lookupErr error
			if err := ateomnet.NetNSDo(context.Background(), ns, func(context.Context) error {
				listener, err := net.Listen(test.listenNetwork, test.listenAddress)
				if err != nil {
					return err
				}
				defer listener.Close()
				_, port, err := net.SplitHostPort(listener.Addr().String())
				if err != nil {
					return err
				}
				client, err := net.DialTimeout(test.dialNetwork, net.JoinHostPort(test.dialHost, port), 10*time.Second)
				if err != nil {
					return err
				}
				defer client.Close()
				server, err := listener.Accept()
				if err != nil {
					return err
				}
				defer server.Close()

				_, lookupErr = TCPOriginalDestination(server)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if errors.Is(lookupErr, unix.ENOPROTOOPT) {
				// A kernel built without the conntrack socket option handler
				// cannot answer either family, so there is nothing to assert.
				t.Skipf("the kernel does not serve SO_ORIGINAL_DST: %v", lookupErr)
			}
			if !errors.Is(lookupErr, unix.ENOENT) {
				t.Errorf("want a lookup miss reported as ENOENT, got %v", lookupErr)
			}
			if want := "original " + test.wantFamily + " TCP destination"; !strings.Contains(lookupErr.Error(), want) {
				t.Errorf("want the error to report %q, got %v", want, lookupErr)
			}
		})
	}
}

// newTestNetNS returns a throwaway network namespace with its loopback up. It
// is anonymous rather than named: there is no /var/run/netns bind mount to
// collide with a concurrent run or to leak if the process is killed, and
// closing the handle takes every link and nftables table in it away.
func newTestNetNS(t *testing.T) netns.NsHandle {
	t.Helper()
	// A namespace is a property of the thread, and netns.New switches the
	// caller into the one it creates, so the thread has to be pinned until we
	// have switched back.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	current, err := netns.Get()
	if err != nil {
		t.Fatalf("getting the current netns: %v", err)
	}
	defer current.Close()
	// Registered before the namespace below so it runs after it is created.
	defer func() {
		if err := netns.Set(current); err != nil {
			t.Errorf("restoring the original netns: %v", err)
		}
	}()

	ns, err := netns.New()
	if err != nil {
		if errors.Is(err, unix.EPERM) {
			t.Skipf("needs CAP_SYS_ADMIN to create a network namespace: %v", err)
		}
		t.Fatalf("creating a test netns: %v", err)
	}
	t.Cleanup(func() { _ = ns.Close() })

	loopback, err := netlink.LinkByName("lo")
	if err != nil {
		t.Fatalf("looking up lo in the test netns: %v", err)
	}
	if err := netlink.LinkSetUp(loopback); err != nil {
		t.Fatalf("bringing lo up in the test netns: %v", err)
	}
	return ns
}

// requireNftables skips when the kernel in this environment cannot serve the
// nftables netlink API at all, which is a property of the machine rather than
// of the code under test.
func requireNftables(t *testing.T, ns netns.NsHandle) {
	t.Helper()
	c, err := nftables.New(nftables.WithNetNSFd(int(ns)))
	if err == nil {
		_, err = c.ListTablesOfFamily(nftables.TableFamilyIPv4)
	}
	if err != nil {
		t.Skipf("nftables unavailable in this environment: %v", err)
	}
}

// setupTestVeth joins the two namespaces with an addressed point-to-point veth.
func setupTestVeth(t *testing.T, family originalDstFamily, workerNS, actorNS netns.NsHandle) {
	t.Helper()
	const workerEnd, actorEnd = "atodw", "atoda"
	if err := ateomnet.NetNSDo(context.Background(), workerNS, func(context.Context) error {
		if err := netlink.LinkAdd(&netlink.Veth{
			LinkAttrs: netlink.LinkAttrs{Name: workerEnd},
			PeerName:  actorEnd,
		}); err != nil {
			return fmt.Errorf("creating the veth: %w", err)
		}
		peer, err := netlink.LinkByName(actorEnd)
		if err != nil {
			return err
		}
		if err := netlink.LinkSetNsFd(peer, int(actorNS)); err != nil {
			return fmt.Errorf("moving the actor end into its namespace: %w", err)
		}
		return configureVethEnd(family, workerEnd, family.workerIP)
	}); err != nil {
		if errors.Is(err, unix.EPERM) {
			t.Skipf("needs CAP_NET_ADMIN to create a veth: %v", err)
		}
		t.Fatalf("wiring the worker end of the veth: %v", err)
	}
	if err := ateomnet.NetNSDo(context.Background(), actorNS, func(context.Context) error {
		return configureVethEnd(family, actorEnd, family.actorIP)
	}); err != nil {
		t.Fatalf("wiring the actor end of the veth: %v", err)
	}
}

func configureVethEnd(family originalDstFamily, name, ip string) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return err
	}
	addr := &netlink.Addr{
		IPNet: &net.IPNet{IP: net.ParseIP(ip), Mask: family.mask},
		Flags: family.addrFlags,
	}
	if err := netlink.AddrAdd(link, addr); err != nil {
		return fmt.Errorf("adding %s to %s: %w", ip, name, err)
	}
	return netlink.LinkSetUp(link)
}

// listenInNetNS opens a listener inside ns. The socket stays bound to that
// namespace once created, so the caller can accept on it from wherever it
// happens to be running.
func listenInNetNS(t *testing.T, ns netns.NsHandle, family originalDstFamily) *net.TCPListener {
	t.Helper()
	var listener *net.TCPListener
	if err := ateomnet.NetNSDo(context.Background(), ns, func(context.Context) error {
		l, err := net.ListenTCP(family.listenNetwork, &net.TCPAddr{IP: net.ParseIP(family.listenIP)})
		if err != nil {
			return err
		}
		listener = l
		return nil
	}); err != nil {
		t.Fatalf("listening on %s %q: %v", family.listenNetwork, family.listenIP, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// installOriginalDstRedirect sends the actor's connections to targetPort on to
// redirectPort instead, the way a worker sends actor egress to atunnel.
func installOriginalDstRedirect(t *testing.T, family originalDstFamily, ns netns.NsHandle, targetPort, redirectPort int) {
	t.Helper()
	c, err := nftables.New(nftables.WithNetNSFd(int(ns)))
	if err != nil {
		t.Fatalf("opening nftables in the worker namespace: %v", err)
	}
	table := c.AddTable(&nftables.Table{Family: family.nftFamily, Name: "atunnel_original_dst_test"})
	chain := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	// Built from the same matchers as the production rule in
	// ateomnet.ActorEgressRedirectRule, with a destination-port match added so
	// the rule cannot fire again on the connection it just rewrote.
	exprs := append(family.sourceEqual(family.actorIP), ateomnet.TCPProtocol()...)
	exprs = append(exprs,
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(targetPort))},
		&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(redirectPort))},
		&expr.Redir{RegisterProtoMin: 1},
	)
	c.AddRule(&nftables.Rule{Table: table, Chain: chain, Exprs: exprs})
	if err := c.Flush(); err != nil {
		t.Fatalf("installing the %s redirect: %v", family.name, err)
	}
}
