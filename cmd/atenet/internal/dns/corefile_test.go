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

package dns

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
)

// actorMatchDirective is the match line shared by the IN A template and the
// NODATA template.
const actorMatchDirective = `match "^` + resources.ResourceNameRegexPattern + `\.` + resources.ResourceNameRegexPattern + `\.actors\.resources\.substrate\.ate\.dev\.$"`

// soaAuthorityDirective is the authority record that makes the negative answers
// cacheable.
const soaAuthorityDirective = `authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"`

func TestMakeCoreFile(t *testing.T) {
	tests := []struct {
		name     string
		routerV4 string
		routerV6 string
		expected []string
		// absent are substrings that must not appear. An address template for a
		// family the router has no address in is worse than no template at all:
		// the RR is a literal, so a v6 address in an `IN A` answer fails
		// dns.NewRR at query time and SERVFAILs the whole zone.
		absent []string
	}{
		{
			name:     "IPv4 only",
			routerV4: "10.240.0.10",
			expected: []string{
				"actors.resources.substrate.ate.dev:53 {",
				"log",
				"errors",
				"health :8080",
				"ready :8181",
				"reload",
				"template IN A actors.resources.substrate.ate.dev {",
				actorMatchDirective,
				`answer "{{ .Name }} 60 IN A 10.240.0.10"`,
				// NODATA for a real actor name on a qtype no address template
				// answered, plus the NXDOMAIN catch-all for everything else.
				"template ANY ANY actors.resources.substrate.ate.dev {",
				"rcode NOERROR",
				"rcode NXDOMAIN",
				soaAuthorityDirective,
			},
			// AAAA is left to the NODATA template, which is the right answer for
			// an actor that has no IPv6 address.
			absent: []string{"template IN AAAA"},
		},
		{
			name:     "different IPv4",
			routerV4: "192.168.1.1",
			expected: []string{
				"actors.resources.substrate.ate.dev:53 {",
				`answer "{{ .Name }} 60 IN A 192.168.1.1"`,
			},
		},
		{
			name:     "IPv6 only",
			routerV6: "fd00:10:96::8857",
			expected: []string{
				"template IN AAAA actors.resources.substrate.ate.dev {",
				actorMatchDirective,
				`answer "{{ .Name }} 60 IN AAAA fd00:10:96::8857"`,
				"rcode NOERROR",
				"rcode NXDOMAIN",
			},
			// The bug this whole change exists for: a v6-only cluster used to get
			// its sole ClusterIP published as an A record.
			absent: []string{"template IN A actors", "IN A fd00"},
		},
		{
			name:     "dual stack",
			routerV4: "10.240.0.10",
			routerV6: "fd00:10:96::8857",
			expected: []string{
				"template IN A actors.resources.substrate.ate.dev {",
				`answer "{{ .Name }} 60 IN A 10.240.0.10"`,
				"template IN AAAA actors.resources.substrate.ate.dev {",
				`answer "{{ .Name }} 60 IN AAAA fd00:10:96::8857"`,
			},
		},
		{
			name: "no addresses",
			// The controller does not call makeCoreFile in this state, but the
			// zone still has to be a loadable Corefile if it ever does: negative
			// answers only, never a template with an empty address in it.
			expected: []string{
				"actors.resources.substrate.ate.dev:53 {",
				"rcode NOERROR",
				"rcode NXDOMAIN",
			},
			absent: []string{"template IN A actors", "template IN AAAA", "answer "},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := makeCoreFile(tc.routerV4, tc.routerV6)
			for _, exp := range tc.expected {
				if !strings.Contains(got, exp) {
					t.Errorf("makeCoreFile(%q, %q) missing expected substring %q\nGot:\n%s", tc.routerV4, tc.routerV6, exp, got)
				}
			}
			for _, abs := range tc.absent {
				if strings.Contains(got, abs) {
					t.Errorf("makeCoreFile(%q, %q) contains unwanted substring %q\nGot:\n%s", tc.routerV4, tc.routerV6, abs, got)
				}
			}
		})
	}
}

// TestMakeCoreFileStable pins the property that keeps the reconcile loop quiet:
// the render depends only on its arguments. reconcileCoreDNSConfig rewrites the
// Corefile and signals CoreDNS whenever the render differs from what is on
// disk, so anything time-varying in the output -- the "Generated at" stamp, in
// particular -- would reload the DNS server on every tick.
func TestMakeCoreFileStable(t *testing.T) {
	first := makeCoreFile("10.240.0.10", "fd00:10:96::8857")
	second := makeCoreFile("10.240.0.10", "fd00:10:96::8857")
	if first != second {
		t.Errorf("makeCoreFile() is not stable across calls; the reconcile loop would rewrite and reload every tick\nFirst:\n%s\nSecond:\n%s", first, second)
	}
}

// TestMakeCoreFileNegativeAnswers pins down the properties of the two negative
// answer templates that keep musl libc clients working: the regex-matched
// NODATA template must come after the IN A template and before the NXDOMAIN
// catch-all, and the catch-all must be terminal, with no match line of its own.
func TestMakeCoreFileNegativeAnswers(t *testing.T) {
	tests := []struct {
		name     string
		routerV4 string
		routerV6 string
		// firstAddressTemplate is the address block that must come first; the
		// negative answers are ordered against it.
		firstAddressTemplate string
		// addressTemplates is how many address blocks the render should have,
		// which is also how many extra match directives and fallthroughs it
		// carries beyond the NODATA template's one of each.
		addressTemplates int
	}{
		{
			name:                 "IPv4 only",
			routerV4:             "10.240.0.10",
			firstAddressTemplate: "template IN A actors.resources.substrate.ate.dev {",
			addressTemplates:     1,
		},
		{
			name:                 "IPv6 only",
			routerV6:             "fd00:10:96::8857",
			firstAddressTemplate: "template IN AAAA actors.resources.substrate.ate.dev {",
			addressTemplates:     1,
		},
		{
			name:                 "dual stack",
			routerV4:             "10.240.0.10",
			routerV6:             "fd00:10:96::8857",
			firstAddressTemplate: "template IN A actors.resources.substrate.ate.dev {",
			addressTemplates:     2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := makeCoreFile(tc.routerV4, tc.routerV6)

			// Templates are evaluated in Corefile order, so these offsets are
			// load-bearing rather than cosmetic.
			ordered := []struct {
				name  string
				index int
			}{
				{"first address template", strings.Index(got, tc.firstAddressTemplate)},
				{"NODATA template", strings.Index(got, "rcode NOERROR")},
				{"NXDOMAIN catch-all", strings.Index(got, "rcode NXDOMAIN")},
			}
			for _, o := range ordered {
				if o.index < 0 {
					t.Fatalf("makeCoreFile(%q, %q) missing %s\nGot:\n%s", tc.routerV4, tc.routerV6, o.name, got)
				}
			}
			for i := 1; i < len(ordered); i++ {
				if ordered[i-1].index >= ordered[i].index {
					t.Errorf("makeCoreFile(%q, %q) emitted %s at offset %d, want it before %s at offset %d\nGot:\n%s",
						tc.routerV4, tc.routerV6, ordered[i-1].name, ordered[i-1].index, ordered[i].name, ordered[i].index, got)
				}
			}
			nxdomain := ordered[2].index

			// The NODATA template is scoped to real actor names; the catch-all
			// that follows it must not be, or nothing in the zone would ever get
			// NXDOMAIN.
			if lastMatch := strings.LastIndex(got, actorMatchDirective); lastMatch > nxdomain {
				t.Errorf("makeCoreFile(%q, %q) has a match directive at offset %d inside the NXDOMAIN catch-all at offset %d, want the catch-all to be unconditional\nGot:\n%s", tc.routerV4, tc.routerV6, lastMatch, nxdomain, got)
			}
			wantMatches := tc.addressTemplates + 1
			if matches := strings.Count(got, actorMatchDirective); matches != wantMatches {
				t.Errorf("makeCoreFile(%q, %q) has %d actor match directives, want %d (one per address template, plus the NODATA template)\nGot:\n%s", tc.routerV4, tc.routerV6, matches, wantMatches, got)
			}

			// Both negative answers need an SOA in the authority section to be
			// cacheable.
			if soas := strings.Count(got, soaAuthorityDirective); soas != 2 {
				t.Errorf("makeCoreFile(%q, %q) has %d SOA authority directives, want 2 (one per negative answer template)\nGot:\n%s", tc.routerV4, tc.routerV6, soas, got)
			}

			// Every block with a "match" needs a "fallthrough": on a regex miss
			// the plugin consults fall.Through(), and without it returns SERVFAIL
			// on the spot rather than evaluating the blocks that follow. So the
			// count tracks the match directives, and the catch-all must not have
			// one -- it is what terminates the chain.
			if fts := strings.Count(got, "fallthrough"); fts != wantMatches {
				t.Errorf("makeCoreFile(%q, %q) has %d fallthrough directives, want %d (one per template carrying a match)\nGot:\n%s", tc.routerV4, tc.routerV6, fts, wantMatches, got)
			}
			if lastFallthrough := strings.LastIndex(got, "fallthrough"); lastFallthrough > nxdomain {
				t.Errorf("makeCoreFile(%q, %q) has a fallthrough at offset %d inside the NXDOMAIN catch-all at offset %d, want the catch-all to be terminal\nGot:\n%s", tc.routerV4, tc.routerV6, lastFallthrough, nxdomain, got)
			}
		})
	}
}
