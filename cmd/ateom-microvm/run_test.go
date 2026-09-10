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

package main

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateomnet"
)

// A symlink planted in the image at /etc or /etc/resolv.conf must not be followed
// out of the rootfs, or the image picks what ateom overwrites as root.
func TestWriteGuestResolvConfSymlinkEscape(t *testing.T) {
	for _, tc := range []struct {
		name string
		link string // rootfs-relative path planted as a symlink to the canary
		// A planted resolv.conf is just unlinked; only an escaping directory fails.
		wantErr bool
	}{
		{name: "etc dir", link: "etc", wantErr: true},
		{name: "resolv.conf", link: "etc/resolv.conf"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			canary := filepath.Join(dir, "canary")
			if err := os.WriteFile(canary, []byte("INITIAL_STATE"), 0o644); err != nil {
				t.Fatal(err)
			}
			rootfs := filepath.Join(dir, "rootfs")
			if err := os.MkdirAll(filepath.Join(rootfs, filepath.Dir(tc.link)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(canary, filepath.Join(rootfs, tc.link)); err != nil {
				t.Fatal(err)
			}

			if err := writeGuestResolvConf(rootfs); (err != nil) != tc.wantErr {
				t.Errorf("writeGuestResolvConf(%q) error = %v, wantErr %v", rootfs, err, tc.wantErr)
			}
			got, err := os.ReadFile(canary)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "INITIAL_STATE" {
				t.Errorf("canary = %q, want it untouched: the symlink was followed out of the rootfs", got)
			}
		})
	}
}

func TestWriteGuestResolvConf(t *testing.T) {
	want, err := os.ReadFile("/etc/resolv.conf")
	if err != nil || len(want) == 0 {
		t.Skipf("no host /etc/resolv.conf to copy: %v", err)
	}
	rootfs := t.TempDir()

	if err := writeGuestResolvConf(rootfs); err != nil {
		t.Fatalf("writeGuestResolvConf(%q) = %v", rootfs, err)
	}

	got, err := os.ReadFile(filepath.Join(rootfs, "etc", "resolv.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("guest resolv.conf = %q, want %q", got, want)
	}
	// A second boot of the same bundle must not fail on the file it just wrote.
	if err := writeGuestResolvConf(rootfs); err != nil {
		t.Errorf("writeGuestResolvConf(%q) second call = %v", rootfs, err)
	}
}

// A vsock socket that has gone missing means cloud-hypervisor stopped the VM
// (it unlinks the socket in the vsock device's shutdown), so the poll must give
// up at once instead of spending the whole timeout on a guest that is gone.
func TestDialAgentRetryGuestStopped(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "clh.sock")

	start := time.Now()
	_, err := dialAgentRetry(t.Context(), missing, 30*time.Second)
	elapsed := time.Since(start)

	if !errors.Is(err, errGuestStopped) {
		t.Errorf("dialAgentRetry(%q) error = %v, want it to wrap errGuestStopped", missing, err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("dialAgentRetry(%q) took %v; want it to give up immediately, not poll for the full timeout", missing, elapsed)
	}
}

// The socket exists but nothing answers yet: that is every dial until the guest
// reaches kata-containers.target, so it must keep polling to the timeout.
func TestDialAgentRetryNotListeningYet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clh.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to create test socket: %v", err)
	}
	// Leave the socket file behind, as cloud-hypervisor does while the VM runs:
	// connecting is then refused, which is what an unanswered CONNECT looks like.
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close test listener: %v", err)
	}

	const timeout = 700 * time.Millisecond
	start := time.Now()
	_, err = dialAgentRetry(t.Context(), path, timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("dialAgentRetry(%q) succeeded, want an error", path)
	}
	if errors.Is(err, errGuestStopped) {
		t.Errorf("dialAgentRetry(%q) error = %v, want a plain dial error (the guest is still running)", path, err)
	}
	if elapsed < timeout {
		t.Errorf("dialAgentRetry(%q) gave up after %v, want it to keep polling for at least %v", path, elapsed, timeout)
	}
}

// A canceled context ends the poll with the context's error, so a caller that
// gives up on the restore is not left waiting out the timeout.
func TestDialAgentRetryContextCanceled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clh.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to create test socket: %v", err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close test listener: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := dialAgentRetry(ctx, path, time.Minute); !errors.Is(err, context.Canceled) {
		t.Errorf("dialAgentRetry(%q) error = %v, want context.Canceled", path, err)
	}
}

// resolveGuestMemMiB must honor a declared limit (minus the VMM reserve), fall back
// to the kata default only when the limit is unset, and error — never silently boot
// bigger than declared — when the reserve leaves too little to boot a guest.
func TestResolveGuestMemMiB(t *testing.T) {
	const (
		mib      = 1024 * 1024
		reserve  = 256  // vmmMemReserveMiB
		fallback = 2048 // kata-config default
	)
	tests := []struct {
		name        string
		declaredMiB int64 // 0 => unset
		wantMiB     int
		wantErr     bool
	}{
		{name: "unset falls back to kata default", declaredMiB: 0, wantMiB: fallback},
		{name: "declared honored minus reserve", declaredMiB: 1536, wantMiB: 1536 - reserve},
		{name: "just above minimum", declaredMiB: reserve + minGuestMemMiB, wantMiB: minGuestMemMiB},
		{name: "reserve exactly swallows limit", declaredMiB: reserve, wantErr: true},
		{name: "limit below reserve", declaredMiB: 128, wantErr: true},
		{name: "boot-hang band (too little guest RAM)", declaredMiB: 320, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveGuestMemMiB(tc.declaredMiB*mib, reserve, fallback)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveGuestMemMiB(%dMiB) = %d, nil; want an error", tc.declaredMiB, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveGuestMemMiB(%dMiB) unexpected error: %v", tc.declaredMiB, err)
			}
			if got != tc.wantMiB {
				t.Errorf("resolveGuestMemMiB(%dMiB) = %d, want %d", tc.declaredMiB, got, tc.wantMiB)
			}
		})
	}
}

func TestInitParams(t *testing.T) {
	// The agent path must be the one the kata guest image actually ships, since the
	// kernel silently panics on an init= that does not exist.
	if got := initParams(true); got != "init=/usr/bin/kata-agent" {
		t.Errorf("initParams(true) = %q", got)
	}
	// Without the agent as PID 1, systemd needs kata's target — it powers the guest
	// off within seconds otherwise — and networkd must stay masked, the agent owns eth0.
	systemd := initParams(false)
	for _, want := range []string{
		"systemd.unit=kata-containers.target",
		"systemd.mask=systemd-networkd.service",
		"systemd.mask=systemd-networkd.socket",
	} {
		if !strings.Contains(systemd, want) {
			t.Errorf("initParams(false) = %q, missing %q", systemd, want)
		}
	}
	if strings.Contains(systemd, "init=") {
		t.Errorf("initParams(false) = %q, must not override init", systemd)
	}
}

// The guest must log over virtio-console, not the emulated UART: the UART traps to
// the VMM per byte, which costs ~800ms of cold boot on the kata guest's boot log.
// Debug mode adds the UART back (with earlycon) to capture the early messages hvc0
// is too late to see.
func TestBuildVMConfigConsole(t *testing.T) {
	const id = "actor-1"
	consoleLog := kata.ConsoleLogPath(id)

	cfg := buildVMConfig(id, "/vmlinux", "/rootfs.img", "", consoleLog, 256, 1, true, false)
	if cfg.Console == nil || cfg.Console.Mode != "File" || cfg.Console.File != consoleLog {
		t.Errorf("Console = %+v, want File %q", cfg.Console, consoleLog)
	}
	if cfg.Serial == nil || cfg.Serial.Mode != "Off" {
		t.Errorf("Serial = %+v, want mode Off", cfg.Serial)
	}
	if !strings.Contains(cfg.Payload.Cmdline, "console=hvc0") {
		t.Errorf("cmdline = %q, want console=hvc0", cfg.Payload.Cmdline)
	}
	if strings.Contains(cfg.Payload.Cmdline, "earlycon") {
		t.Errorf("cmdline = %q, must not pay for earlycon outside debug mode", cfg.Payload.Cmdline)
	}

	dbg := buildVMConfig(id, "/vmlinux", "/rootfs.img", "", consoleLog, 256, 1, true, true)
	if dbg.Serial == nil || dbg.Serial.Mode != "File" || dbg.Serial.File != kata.SerialLogPath(id) {
		t.Errorf("debug Serial = %+v, want File %q", dbg.Serial, kata.SerialLogPath(id))
	}
	if !strings.Contains(dbg.Payload.Cmdline, "earlycon=") {
		t.Errorf("debug cmdline = %q, want an earlycon", dbg.Payload.Cmdline)
	}
}

// The SIGTERM path signals these ids over ttrpc, and the agent rejects an id it
// does not know with InvalidContainerId — which aborts the whole graceful
// shutdown, so a stale id here silently costs the guest its clean exit.
func TestWorkloadIDs(t *testing.T) {
	ctrs := []actorContainer{{name: "counter"}, {name: "sidecar"}}
	got := workloadIDs(ctrs)
	if want := []string{"counter", "sidecar"}; !slices.Equal(got, want) {
		t.Errorf("workloadIDs() = %v, want %v", got, want)
	}
}

// The IPv4 configuration is what every micro-VM actor has always booted with;
// it has to survive the IPv6 addition byte for byte, and the IPv6 addition has
// to be purely additive on top of it.
func TestBuildGuestNetConfig(t *testing.T) {
	const mtu = 1500

	v4Addr := &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethIP, Mask: "30"}
	v6Addr := &agentpb.IPAddress{Family: agentpb.IPFamily_v6, Address: ateomnet.ActorVethIPv6IP, Mask: "126"}
	v4Routes := []*agentpb.Route{
		{Dest: ateomnet.ActorVethSubnet, Device: ateomnet.ActorVethName, Scope: uint32(unix.RT_SCOPE_LINK), Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: ateomnet.ActorVethGateway, Device: ateomnet.ActorVethName, Family: agentpb.IPFamily_v4},
	}
	v6Routes := []*agentpb.Route{
		{Dest: ateomnet.ActorVethIPv6Subnet, Device: ateomnet.ActorVethName, Family: agentpb.IPFamily_v6},
		{Dest: "::/0", Gateway: ateomnet.ActorVethIPv6Gateway, Device: ateomnet.ActorVethName, Family: agentpb.IPFamily_v6},
	}
	v4Neigh := &agentpb.ARPNeighbor{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethGateway},
		Device:      ateomnet.ActorVethName,
		Lladdr:      hostVethMAC,
		State:       0x80,
	}
	v6Neigh := &agentpb.ARPNeighbor{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v6, Address: ateomnet.ActorVethIPv6Gateway},
		Device:      ateomnet.ActorVethName,
		Lladdr:      hostVethMAC,
		State:       0x80,
	}
	iface := func(addrs ...*agentpb.IPAddress) *agentpb.Interface {
		return &agentpb.Interface{
			Device:      ateomnet.ActorVethName,
			Name:        ateomnet.ActorVethName,
			HwAddr:      actorGuestMAC,
			Mtu:         mtu,
			IPAddresses: addrs,
		}
	}

	for _, tc := range []struct {
		name     string
		actorNet ateomnet.ActorNetwork
		want     guestNetConfig
	}{{
		name:     "IPv4 only",
		actorNet: ateomnet.ActorNetwork{},
		want: guestNetConfig{
			iface:     iface(v4Addr),
			routes:    v4Routes,
			neighbors: []*agentpb.ARPNeighbor{v4Neigh},
		},
	}, {
		// Both families ride in one Interface message: the agent replaces the
		// address list with what it is handed, so a second UpdateInterface
		// carrying only the v6 address would drop 169.254.17.2 on the floor.
		// The routes stay grouped by family, connected before default, because
		// the agent installs them in order and a gatewayed route needs its
		// gateway on-link already.
		name:     "dual stack",
		actorNet: ateomnet.ActorNetwork{IPv6: true},
		want: guestNetConfig{
			iface:     iface(v4Addr, v6Addr),
			routes:    append(append([]*agentpb.Route{}, v4Routes...), v6Routes...),
			neighbors: []*agentpb.ARPNeighbor{v4Neigh, v6Neigh},
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := buildGuestNetConfig(mtu, tc.actorNet)
			if diff := cmp.Diff(tc.want.iface, got.iface, protocmp.Transform()); diff != "" {
				t.Errorf("interface diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.want.routes, got.routes, protocmp.Transform()); diff != "" {
				t.Errorf("routes diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.want.neighbors, got.neighbors, protocmp.Transform()); diff != "" {
				t.Errorf("neighbors diff (-want +got):\n%s", diff)
			}
		})
	}
}

// The properties above are pinned as literal data, which catches a change but
// not a mistake that was baked in from the start. These check the things that
// parse, look right, and fail only in the guest.
func TestBuildGuestNetConfigInvariants(t *testing.T) {
	v4 := buildGuestNetConfig(1500, ateomnet.ActorNetwork{})
	dual := buildGuestNetConfig(1500, ateomnet.ActorNetwork{IPv6: true})

	t.Run("IPv6 is additive", func(t *testing.T) {
		if diff := cmp.Diff(v4.iface.GetIPAddresses(), dual.iface.GetIPAddresses()[:1], protocmp.Transform()); diff != "" {
			t.Errorf("v4 address changed by enabling IPv6 (-v4only +dual):\n%s", diff)
		}
		if diff := cmp.Diff(v4.routes, dual.routes[:len(v4.routes)], protocmp.Transform()); diff != "" {
			t.Errorf("v4 routes changed by enabling IPv6 (-v4only +dual):\n%s", diff)
		}
		if diff := cmp.Diff(v4.neighbors, dual.neighbors[:len(v4.neighbors)], protocmp.Transform()); diff != "" {
			t.Errorf("v4 neighbors changed by enabling IPv6 (-v4only +dual):\n%s", diff)
		}
	})

	// A v4 gateway on a Family_v6 route parses, marshals and reads fine; it
	// black-holes the guest and nothing before the guest would say so.
	t.Run("v6 default uses the v6 gateway", func(t *testing.T) {
		var got *agentpb.Route
		for _, r := range dual.routes {
			if r.GetFamily() == agentpb.IPFamily_v6 && r.GetGateway() != "" {
				got = r
			}
		}
		if got == nil {
			t.Fatalf("no gatewayed IPv6 route in %v", dual.routes)
		}
		if got.GetGateway() == ateomnet.ActorVethGateway {
			t.Errorf("IPv6 default gateway = %q, which is the IPv4 gateway", got.GetGateway())
		}
		if got.GetGateway() != ateomnet.ActorVethIPv6Gateway {
			t.Errorf("IPv6 default gateway = %q, want %q", got.GetGateway(), ateomnet.ActorVethIPv6Gateway)
		}
		// The v4 default borrows the agent's empty-dest convention; spelling
		// the v6 one out keeps any agent version from guessing the family.
		if got.GetDest() != "::/0" {
			t.Errorf("IPv6 default dest = %q, want %q", got.GetDest(), "::/0")
		}
	})

	// The agent passes Scope straight into rtm_scope, and an IPv6 route has no
	// scope: RT_SCOPE_LINK copied across from the v4 line is rejected.
	t.Run("v6 routes carry no scope", func(t *testing.T) {
		for _, r := range dual.routes {
			if r.GetFamily() == agentpb.IPFamily_v6 && r.GetScope() != 0 {
				t.Errorf("IPv6 route %q Scope = %d, want 0", r.GetDest(), r.GetScope())
			}
		}
	})

	// The masks are decimal strings here and CIDRs in ateomnet; nothing but
	// this keeps the two spellings from drifting apart.
	t.Run("masks match the ateomnet prefix lengths", func(t *testing.T) {
		for _, tc := range []struct{ addr, cidr string }{
			{ateomnet.ActorVethIP, ateomnet.ActorVethCIDR},
			{ateomnet.ActorVethIPv6IP, ateomnet.ActorVethIPv6CIDR},
		} {
			prefix, err := netip.ParsePrefix(tc.cidr)
			if err != nil {
				t.Fatalf("ParsePrefix(%q) = %v", tc.cidr, err)
			}
			want := strconv.Itoa(prefix.Bits())
			var got string
			for _, a := range dual.iface.GetIPAddresses() {
				if a.GetAddress() == tc.addr {
					got = a.GetMask()
				}
			}
			if got != want {
				t.Errorf("%s mask = %q, want %q (from %s)", tc.addr, got, want, tc.cidr)
			}
		}
	})

	// Pinning the gateway to the guest's own MAC black-holes every restore,
	// and it is a one-character edit away.
	t.Run("neighbors pin the gateway to the host veth", func(t *testing.T) {
		families := map[agentpb.IPFamily]bool{}
		for _, n := range dual.neighbors {
			if n.GetLladdr() == actorGuestMAC {
				t.Errorf("neighbor %q Lladdr is the guest's own MAC", n.GetToIPAddress().GetAddress())
			}
			if n.GetLladdr() != hostVethMAC {
				t.Errorf("neighbor %q Lladdr = %q, want %q", n.GetToIPAddress().GetAddress(), n.GetLladdr(), hostVethMAC)
			}
			if n.GetState() != 0x80 {
				t.Errorf("neighbor %q State = %#x, want NUD_PERMANENT (%#x)", n.GetToIPAddress().GetAddress(), n.GetState(), 0x80)
			}
			families[n.GetToIPAddress().GetFamily()] = true
		}
		if !families[agentpb.IPFamily_v4] || !families[agentpb.IPFamily_v6] {
			t.Errorf("dual-stack neighbors cover %v, want both families", families)
		}
	})
}

// The restore path asks this whether the guest it just thawed matches the pod
// it landed on, so a wrong answer either strands the actor on one family or
// re-runs the whole guest setup on every resume.
func TestGuestHasIPv6(t *testing.T) {
	eth0 := func(addrs ...*agentpb.IPAddress) *agentpb.Interface {
		return &agentpb.Interface{Name: ateomnet.ActorVethName, IPAddresses: addrs}
	}
	v6 := func(a string) *agentpb.IPAddress {
		return &agentpb.IPAddress{Family: agentpb.IPFamily_v6, Address: a}
	}
	v4 := func(a string) *agentpb.IPAddress {
		return &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: a}
	}

	for _, tc := range []struct {
		name   string
		ifaces []*agentpb.Interface
		want   bool
	}{{
		name:   "no interfaces",
		ifaces: nil,
		want:   false,
	}, {
		name:   "IPv4 only",
		ifaces: []*agentpb.Interface{eth0(v4(ateomnet.ActorVethIP))},
		want:   false,
	}, {
		// Every link with IPv6 compiled in has one, including the guest's
		// before this change: counting it makes the answer always true and
		// the reconcile never runs.
		name:   "link-local alone is not IPv6",
		ifaces: []*agentpb.Interface{eth0(v4(ateomnet.ActorVethIP), v6("fe80::a8:1eff:fe00:2"))},
		want:   false,
	}, {
		name:   "dual stack",
		ifaces: []*agentpb.Interface{eth0(v4(ateomnet.ActorVethIP), v6(ateomnet.ActorVethIPv6IP))},
		want:   true,
	}, {
		// Only the actor veth is ours to reconcile.
		name: "IPv6 on another interface",
		ifaces: []*agentpb.Interface{
			eth0(v4(ateomnet.ActorVethIP)),
			{Name: "lo", IPAddresses: []*agentpb.IPAddress{v6("::1")}},
		},
		want: false,
	}, {
		// Classification is by parsing, so a mislabelled address still counts:
		// the guest's kernel has it either way.
		name:   "Family mis-set to v4",
		ifaces: []*agentpb.Interface{eth0(v4(ateomnet.ActorVethIPv6IP))},
		want:   true,
	}, {
		// The inverse: a v4-mapped form would satisfy a 16-byte family check.
		name:   "v4-mapped is not IPv6",
		ifaces: []*agentpb.Interface{eth0(v6("::ffff:169.254.17.2"))},
		want:   false,
	}, {
		name:   "loopback is not IPv6",
		ifaces: []*agentpb.Interface{eth0(v6("::1"))},
		want:   false,
	}, {
		name:   "unparseable address is skipped",
		ifaces: []*agentpb.Interface{eth0(v6("not-an-address"))},
		want:   false,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := guestHasIPv6(tc.ifaces); got != tc.want {
				t.Errorf("guestHasIPv6() = %v, want %v", got, tc.want)
			}
		})
	}
}
