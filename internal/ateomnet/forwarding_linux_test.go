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
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// Spelled out rather than shared with EnableForwarding: a test that reads the
// same constant as the code under test cannot catch the path changing.
const (
	v4ForwardPath = "/proc/sys/net/ipv4/ip_forward"
	v6ForwardPath = "/proc/sys/net/ipv6/conf/all/forwarding"
)

func readSysctl(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return strings.TrimSpace(string(b))
}

// TestEnableForwardingSetsBothFamilies asserts the effect the rest of the
// package only assumes: after EnableForwarding, both forwarding sysctls read 1
// in the calling namespace. The other tests here call SetupActorNetwork, which
// calls EnableForwarding, but none of them look at a sysctl — so a version that
// wrote nothing at all would leave them green while every IPv6 actor packet was
// silently dropped in ip6_forward().
//
// /proc/sys/net is per-netns for the calling thread, so the throwaway namespace
// withTestNetNS enters both isolates the write and makes the pre-state a known 0.
func TestEnableForwardingSetsBothFamilies(t *testing.T) {
	roottest.Require(t, "creating a network namespace and writing /proc/sys")

	withTestNetNS(t, func(netns.NsHandle) {
		if err := EnableForwarding(); err != nil {
			t.Fatalf("EnableForwarding: %v", err)
		}
		for _, path := range []string{v4ForwardPath, v6ForwardPath} {
			if got := readSysctl(t, path); got != "1" {
				t.Errorf("%s = %q, want 1", path, got)
			}
		}
	})
}

// TestEnableForwardingRemountsReadOnlyProcSys covers writeSysctlIfUnset's
// bind-remount branch, which is the one a real worker pod always takes: the
// runtime mounts /proc/sys read-only, so the plain write fails and the helper
// has to clear the ro flag, write, and put it back.
//
// The condition is manufactured rather than waited for, since the test binary's
// own /proc/sys is writable. Nothing here escapes the test: propagation is made
// private before the first remount, and the thread is deliberately left locked
// so the runtime destroys it — with its mount namespace — instead of returning
// it to the pool.
func TestEnableForwardingRemountsReadOnlyProcSys(t *testing.T) {
	roottest.Require(t, "remounting /proc/sys in a private mount namespace")

	runtime.LockOSThread() // no matching Unlock, on purpose (see above)

	if err := unix.Unshare(unix.CLONE_NEWNS | unix.CLONE_NEWNET); err != nil {
		t.Fatalf("unsharing mount and network namespaces: %v", err)
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		t.Fatalf("making mount propagation private: %v", err)
	}
	// Bind /proc/sys onto itself first: MS_REMOUNT|MS_RDONLY applies to a mount,
	// and /proc/sys is not one until it is.
	if err := unix.Mount("/proc/sys", "/proc/sys", "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind-mounting /proc/sys: %v", err)
	}
	if err := unix.Mount("", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
		t.Fatalf("remounting /proc/sys read-only: %v", err)
	}

	// Precondition: without it the test would pass through the fast path and
	// prove nothing about the branch it exists to cover.
	if err := os.WriteFile(v6ForwardPath, []byte("1\n"), 0o644); err == nil {
		t.Fatal("expected /proc/sys to be read-only, but a plain write succeeded")
	}

	if err := EnableForwarding(); err != nil {
		t.Fatalf("EnableForwarding with a read-only /proc/sys: %v", err)
	}
	if got := readSysctl(t, v6ForwardPath); got != "1" {
		t.Errorf("%s = %q, want 1", v6ForwardPath, got)
	}
	if err := os.WriteFile(v4ForwardPath, []byte("1\n"), 0o644); err == nil {
		t.Error("/proc/sys was left writable: the deferred read-only remount did not run")
	}
}
