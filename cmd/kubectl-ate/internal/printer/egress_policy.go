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

package printer

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
)

// ActorEgressPolicy pairs a policy with the actor it was fetched for. The
// policy itself carries no actor name and is always named "default".
type ActorEgressPolicy struct {
	Actor  *ateapipb.ObjectRef
	Policy *ateapipb.EgressPolicy
}

// egressPolicyEntry is one entry of the json/yaml egressPolicies list. Both
// fields hold already protojson-marshalled messages.
type egressPolicyEntry struct {
	Actor        json.RawMessage `json:"actor"`
	EgressPolicy json.RawMessage `json:"egressPolicy"`
}

// PrintEgressPoliciesTo prints several actors' egress policies. json and yaml
// wrap them in an egressPolicies list whose entries tag each policy with its
// actor, in the order given.
func PrintEgressPoliciesTo(out io.Writer, entries []ActorEgressPolicy, format string) error {
	switch format {
	case "json", "yaml":
		list := struct {
			EgressPolicies []json.RawMessage `json:"egressPolicies"`
		}{EgressPolicies: make([]json.RawMessage, 0, len(entries))}
		for _, entry := range entries {
			actor, err := protojson.Marshal(entry.Actor)
			if err != nil {
				return err
			}
			policy, err := protojson.Marshal(entry.Policy)
			if err != nil {
				return err
			}
			b, err := json.Marshal(egressPolicyEntry{Actor: actor, EgressPolicy: policy})
			if err != nil {
				return err
			}
			list.EgressPolicies = append(list.EgressPolicies, b)
		}
		b, err := json.Marshal(list)
		if err != nil {
			return fmt.Errorf("failed to marshal egress policies: %w", err)
		}
		return printJSON(out, b, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tACTOR\tRULES\tVERSION\tAGE")
		for _, entry := range entries {
			egressPolicyRow(w, entry.Actor.GetName(), entry.Policy)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

func egressPolicyRow(w io.Writer, actor string, policy *ateapipb.EgressPolicy) {
	fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
		policy.GetMetadata().GetAtespace(), actor, len(policy.GetRules()),
		policy.GetMetadata().GetVersion(), formatAge(policy.GetMetadata().GetCreateTime()))
}

// PrintEgressPolicyTo prints one actor's egress policy. json and yaml emit the
// bare EgressPolicy. The policy carries no actor name, so the caller passes it.
func PrintEgressPolicyTo(out io.Writer, actor string, policy *ateapipb.EgressPolicy, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, policy, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintEgressPoliciesTo(out, []ActorEgressPolicy{{Actor: &ateapipb.ObjectRef{Name: actor}, Policy: policy}}, format)
}
