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
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPrintEgressPolicyTo(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "team-a",
			Name:       "default",
			Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
			Version:    2,
			CreateTime: timestamppb.New(now.Add(-5 * time.Minute)),
		},
		Rules: []*ateapipb.EgressRule{
			{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}},
			{Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.64.0.0/16"}}},
		},
	}
	empty := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Version: 1, CreateTime: timestamppb.New(now.Add(-30 * time.Second))},
	}

	tests := []struct {
		name    string
		format  string
		actor   string
		policy  *ateapipb.EgressPolicy
		want    string
		wantErr bool
	}{
		{
			name:   "yaml bare document",
			format: "yaml",
			actor:  "c1",
			policy: policy,
			want: `metadata:
  atespace: team-a
  createTime: "2026-01-01T11:55:00Z"
  name: default
  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d
  version: "2"
rules:
- hostnames:
    patterns:
    - api.example.com
- cidrs:
    cidrs:
    - 10.64.0.0/16
`,
		},
		{
			name:   "json bare document",
			format: "json",
			actor:  "c1",
			policy: policy,
			want: `{
  "metadata": {
    "atespace": "team-a",
    "createTime": "2026-01-01T11:55:00Z",
    "name": "default",
    "uid": "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
    "version": "2"
  },
  "rules": [
    {
      "hostnames": {
        "patterns": [
          "api.example.com"
        ]
      }
    },
    {
      "cidrs": {
        "cidrs": [
          "10.64.0.0/16"
        ]
      }
    }
  ]
}
`,
		},
		{
			name:   "table one row",
			format: "table",
			actor:  "c1",
			policy: policy,
			want: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c1      2       2         5m
`,
		},
		{
			name:   "table no rules",
			format: "table",
			actor:  "c2",
			policy: empty,
			want: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c2      0       1         30s
`,
		},
		{name: "xml rejected", format: "xml", actor: "c1", policy: policy, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := PrintEgressPolicyTo(&buf, test.actor, test.policy, test.format)
			if (err != nil) != test.wantErr {
				t.Fatalf("PrintEgressPolicyTo(%q) error = %v, wantErr %t", test.format, err, test.wantErr)
			}
			if diff := cmp.Diff(test.want, buf.String()); diff != "" {
				t.Errorf("output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPrintEgressPoliciesTo(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	entries := []ActorEgressPolicy{
		{
			Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "c2"},
			Policy: &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace:   "team-a",
					Name:       "default",
					Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
					Version:    2,
					CreateTime: timestamppb.New(now.Add(-5 * time.Minute)),
				},
				Rules: []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}},
			},
		},
		{
			Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "c1"},
			Policy: &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Version: 1, CreateTime: timestamppb.New(now.Add(-30 * time.Second))},
			},
		},
	}

	tests := []struct {
		name    string
		format  string
		entries []ActorEgressPolicy
		want    string
		wantErr bool
	}{
		{
			name:    "table keeps the given order",
			format:  "table",
			entries: entries,
			want: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c2      1       2         5m
team-a     c1      0       1         30s
`,
		},
		{
			name:    "yaml wraps in egressPolicies",
			format:  "yaml",
			entries: entries,
			want: `egressPolicies:
- actor:
    atespace: team-a
    name: c2
  egressPolicy:
    metadata:
      atespace: team-a
      createTime: "2026-01-01T11:55:00Z"
      name: default
      uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d
      version: "2"
    rules:
    - hostnames:
        patterns:
        - api.example.com
- actor:
    atespace: team-a
    name: c1
  egressPolicy:
    metadata:
      atespace: team-a
      createTime: "2026-01-01T11:59:30Z"
      name: default
      version: "1"
`,
		},
		{
			name:    "json wraps in egressPolicies",
			format:  "json",
			entries: entries,
			want: `{
  "egressPolicies": [
    {
      "actor": {
        "atespace": "team-a",
        "name": "c2"
      },
      "egressPolicy": {
        "metadata": {
          "atespace": "team-a",
          "createTime": "2026-01-01T11:55:00Z",
          "name": "default",
          "uid": "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
          "version": "2"
        },
        "rules": [
          {
            "hostnames": {
              "patterns": [
                "api.example.com"
              ]
            }
          }
        ]
      }
    },
    {
      "actor": {
        "atespace": "team-a",
        "name": "c1"
      },
      "egressPolicy": {
        "metadata": {
          "atespace": "team-a",
          "createTime": "2026-01-01T11:59:30Z",
          "name": "default",
          "version": "1"
        }
      }
    }
  ]
}
`,
		},
		{
			name:    "one entry is still a list",
			format:  "yaml",
			entries: entries[:1],
			want: `egressPolicies:
- actor:
    atespace: team-a
    name: c2
  egressPolicy:
    metadata:
      atespace: team-a
      createTime: "2026-01-01T11:55:00Z"
      name: default
      uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d
      version: "2"
    rules:
    - hostnames:
        patterns:
        - api.example.com
`,
		},
		{name: "empty yaml", format: "yaml", want: "egressPolicies: []\n"},
		{
			name:   "empty json",
			format: "json",
			want: `{
  "egressPolicies": []
}
`,
		},
		{name: "empty table", format: "table", want: "ATESPACE   ACTOR   RULES   VERSION   AGE\n"},
		{name: "xml rejected", format: "xml", entries: entries, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := PrintEgressPoliciesTo(&buf, test.entries, test.format)
			if (err != nil) != test.wantErr {
				t.Fatalf("PrintEgressPoliciesTo(%q) error = %v, wantErr %t", test.format, err, test.wantErr)
			}
			if diff := cmp.Diff(test.want, buf.String()); diff != "" {
				t.Errorf("output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Each list entry must decode back into its actor and policy so that
// `jq '.egressPolicies[] | select(.actor.name==...) | .egressPolicy'` recovers
// exactly what a single-actor get prints.
func TestPrintEgressPoliciesTo_JSONRoundTrip(t *testing.T) {
	entries := []ActorEgressPolicy{
		{
			Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "c1"},
			Policy: &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace:   "team-a",
					Name:       "default",
					Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
					Version:    2,
					CreateTime: timestamppb.New(time.Date(2026, 1, 1, 11, 55, 0, 0, time.UTC)),
					UpdateTime: timestamppb.New(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
				},
				Rules: []*ateapipb.EgressRule{
					{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"*.example.com"}}},
					{All: &emptypb.Empty{}},
				},
			},
		},
		{
			Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "c2"},
			Policy: &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Version: 1},
				Rules:    []*ateapipb.EgressRule{{Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.64.0.0/16"}}}},
			},
		},
	}

	var buf bytes.Buffer
	if err := PrintEgressPoliciesTo(&buf, entries, "json"); err != nil {
		t.Fatalf("PrintEgressPoliciesTo(json) error = %v", err)
	}
	var doc struct {
		EgressPolicies []json.RawMessage `json:"egressPolicies"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", buf.String(), err)
	}
	if got, want := len(doc.EgressPolicies), len(entries); got != want {
		t.Fatalf("len(egressPolicies) = %d, want %d", got, want)
	}
	for i, raw := range doc.EgressPolicies {
		var entry struct {
			Actor        json.RawMessage `json:"actor"`
			EgressPolicy json.RawMessage `json:"egressPolicy"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			t.Fatalf("json.Unmarshal(entry %d = %q) error = %v", i, raw, err)
		}
		gotActor := &ateapipb.ObjectRef{}
		if err := protojson.Unmarshal(entry.Actor, gotActor); err != nil {
			t.Fatalf("protojson.Unmarshal(entry %d actor = %q) error = %v", i, entry.Actor, err)
		}
		if diff := cmp.Diff(entries[i].Actor, gotActor, protocmp.Transform()); diff != "" {
			t.Errorf("entry %d actor round trip mismatch (-want +got):\n%s", i, diff)
		}
		gotPolicy := &ateapipb.EgressPolicy{}
		if err := protojson.Unmarshal(entry.EgressPolicy, gotPolicy); err != nil {
			t.Fatalf("protojson.Unmarshal(entry %d egressPolicy = %q) error = %v", i, entry.EgressPolicy, err)
		}
		if diff := cmp.Diff(entries[i].Policy, gotPolicy, protocmp.Transform()); diff != "" {
			t.Errorf("entry %d policy round trip mismatch (-want +got):\n%s", i, diff)
		}
	}
}
