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
	"fmt"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
)

// fallthroughDirective lets a template block that declares a "match" hand the
// query to the blocks after it when its regex rejects the name, instead of
// answering SERVFAIL on the spot. Bare, so it applies to every zone.
const fallthroughDirective = "  fallthrough"

// corefileTemplate is a Sprintf template for the CoreDNS configuration.
var corefileTemplate string

func init() {
	corefileTemplate = buildTemplate()
}

func buildTemplate() string {
	// Build up the corefileTemplate programmatically to make it easier to understand.
	var directives []string
	// Plugins to enable.
	directives = append(directives, "log")
	directives = append(directives, "errors")
	directives = append(directives, "health :8080")
	directives = append(directives, "ready :8181")
	directives = append(directives, "reload")

	// Construct match pattern for <ActorName>.<atespace>.<dnsDomain>. Both the
	// actor name and the atespace are DNS-1123 labels (same regex).
	directives = append(directives, fmt.Sprintf("template IN A %s {", resources.ActorDNSSuffix))
	// Escape the suffix's dots so they match literally; the final \. matches the FQDN's trailing dot.
	escapedSuffix := strings.ReplaceAll(resources.ActorDNSSuffix, ".", `\.`)
	actorMatch := fmt.Sprintf(`  match "^%s\.%s\.%s\.$"`, resources.ResourceNameRegexPattern, resources.ResourceNameRegexPattern, escapedSuffix)
	directives = append(directives, actorMatch)
	// Note the %s -- this will be filled with the router IP.
	directives = append(directives, `  answer "{{ .Name }} 60 IN A %s"`)
	directives = append(directives, fallthroughDirective)
	directives = append(directives, "}")

	// Without the two templates below, this zone answers everything else with
	// SERVFAIL: the template plugin is the end of the chain, so an unanswered
	// query reaches plugin.NextOrFailure with a nil Next. SERVFAIL is fatal for
	// musl libc (Alpine) clients, which map rcode 2 to EAI_AGAIN and abandon the
	// lookup without reading the paired A answer, and it cannot be negatively
	// cached, so every retry pays the resolver timeout again. The SOA in the
	// authority section is what makes the replies below cacheable.
	//
	// Two mechanics of the template plugin drive the shape of this, both from
	// its ServeDNS loop (`if !match { if !fthrough { return SERVFAIL }; continue }`):
	//
	//   - A class or qtype mismatch reports fthrough=true, so it moves on to the
	//     next template on its own. A *regex* miss reports fall.Through(), which
	//     is false unless the block declares "fallthrough" -- and returns
	//     SERVFAIL immediately, without evaluating any later template. So every
	//     block carrying a "match" needs "fallthrough", or the blocks after it
	//     are unreachable for any name its regex rejects.
	//   - Templates are evaluated in Corefile order, so the IN A block must stay
	//     first and the regex-matched NODATA block must precede the catch-all.
	soa := `  authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"`

	// NODATA for a real actor name queried with a qtype other than A (AAAA,
	// HTTPS, SRV, ...). NXDOMAIN here would break musl, which treats rcode 3 on
	// either half of its parallel A/AAAA pair as "no addresses at all".
	directives = append(directives, fmt.Sprintf("template ANY ANY %s {", resources.ActorDNSSuffix))
	directives = append(directives, actorMatch)
	directives = append(directives, "  rcode NOERROR")
	directives = append(directives, soa)
	directives = append(directives, fallthroughDirective)
	directives = append(directives, "}")

	// Terminal catch-all: any other name in the zone genuinely does not exist.
	// It carries no "match", which the plugin defaults to ".*", so it always
	// matches and no query can reach the nil Next behind it. It must not declare
	// "fallthrough" -- it is the block that stops the SERVFAIL.
	directives = append(directives, fmt.Sprintf("template ANY ANY %s {", resources.ActorDNSSuffix))
	directives = append(directives, "  rcode NXDOMAIN")
	directives = append(directives, soa)
	directives = append(directives, "}")

	// Generate the template.
	b := strings.Builder{}
	fmt.Fprintf(&b, "# Generated at %s\n", time.Now())
	fmt.Fprintf(&b, "%s:53 {\n  ", resources.ActorDNSSuffix)
	fmt.Fprint(&b, strings.Join(directives, "\n  "))
	fmt.Fprint(&b, "\n}\n")

	return b.String()
}

func makeCoreFile(routerIP string) string {
	return fmt.Sprintf(corefileTemplate, routerIP)
}
