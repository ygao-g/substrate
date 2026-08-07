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
		routerIP string
		expected []string
	}{
		{
			name:     "standard local IP",
			routerIP: "10.240.0.10",
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
				// NODATA for a real actor name on a non-A qtype, plus the
				// NXDOMAIN catch-all for everything else in the zone.
				"template ANY ANY actors.resources.substrate.ate.dev {",
				"rcode NOERROR",
				"rcode NXDOMAIN",
				soaAuthorityDirective,
			},
		},
		{
			name:     "different IP",
			routerIP: "192.168.1.1",
			expected: []string{
				"actors.resources.substrate.ate.dev:53 {",
				`answer "{{ .Name }} 60 IN A 192.168.1.1"`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := makeCoreFile(tc.routerIP)
			for _, exp := range tc.expected {
				if !strings.Contains(got, exp) {
					t.Errorf("makeCoreFile(%q) missing expected substring %q\nGot:\n%s", tc.routerIP, exp, got)
				}
			}
		})
	}
}

// TestMakeCoreFileNegativeAnswers pins down the properties of the two negative
// answer templates that keep musl libc clients working: the regex-matched
// NODATA template must come after the IN A template and before the NXDOMAIN
// catch-all, and the catch-all must be terminal, with no match line of its own.
func TestMakeCoreFileNegativeAnswers(t *testing.T) {
	got := makeCoreFile("10.240.0.10")

	// Templates are evaluated in Corefile order, so these offsets are
	// load-bearing rather than cosmetic.
	ordered := []struct {
		name  string
		index int
	}{
		{"IN A template", strings.Index(got, "template IN A actors.resources.substrate.ate.dev {")},
		{"NODATA template", strings.Index(got, "rcode NOERROR")},
		{"NXDOMAIN catch-all", strings.Index(got, "rcode NXDOMAIN")},
	}
	for _, o := range ordered {
		if o.index < 0 {
			t.Fatalf("makeCoreFile() missing %s\nGot:\n%s", o.name, got)
		}
	}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].index >= ordered[i].index {
			t.Errorf("makeCoreFile() emitted %s at offset %d, want it before %s at offset %d\nGot:\n%s",
				ordered[i-1].name, ordered[i-1].index, ordered[i].name, ordered[i].index, got)
		}
	}
	nxdomain := ordered[2].index

	// The NODATA template is scoped to real actor names; the catch-all that
	// follows it must not be, or nothing in the zone would ever get NXDOMAIN.
	if lastMatch := strings.LastIndex(got, actorMatchDirective); lastMatch > nxdomain {
		t.Errorf("makeCoreFile() has a match directive at offset %d inside the NXDOMAIN catch-all at offset %d, want the catch-all to be unconditional\nGot:\n%s", lastMatch, nxdomain, got)
	}
	if matches := strings.Count(got, actorMatchDirective); matches != 2 {
		t.Errorf("makeCoreFile() has %d actor match directives, want 2 (the IN A and NODATA templates)\nGot:\n%s", matches, got)
	}

	// Both negative answers need an SOA in the authority section to be cacheable.
	if soas := strings.Count(got, soaAuthorityDirective); soas != 2 {
		t.Errorf("makeCoreFile() has %d SOA authority directives, want 2 (one per negative answer template)\nGot:\n%s", soas, got)
	}

	// Every block with a "match" needs a "fallthrough": on a regex miss the
	// plugin consults fall.Through(), and without it returns SERVFAIL on the
	// spot rather than evaluating the blocks that follow. Two match directives
	// therefore mean exactly two fallthroughs, and the catch-all must not have
	// one -- it is what terminates the chain.
	if fts := strings.Count(got, "fallthrough"); fts != 2 {
		t.Errorf("makeCoreFile() has %d fallthrough directives, want 2 (one per template carrying a match)\nGot:\n%s", fts, got)
	}
	if lastFallthrough := strings.LastIndex(got, "fallthrough"); lastFallthrough > nxdomain {
		t.Errorf("makeCoreFile() has a fallthrough at offset %d inside the NXDOMAIN catch-all at offset %d, want the catch-all to be terminal\nGot:\n%s", lastFallthrough, nxdomain, got)
	}
}
