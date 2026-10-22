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

package contract

import "testing"

// named is a StartRequest with the three identity fields filled, so a case
// states only what it is about.
func named(r StartRequest) StartRequest {
	r.Actor, r.Cell, r.Run = "benchmark-workloads/egress-0", "cell-1", "run-1"
	return r
}

func TestStartRequestValidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		req     StartRequest
		wantErr bool
	}{
		{"ok", named(StartRequest{Endpoints: 10}), false},
		{"http", named(StartRequest{Endpoints: 10, Scheme: SchemeHTTP}), false},
		{"https", named(StartRequest{Endpoints: 10, Scheme: SchemeHTTPS}), false},
		{"bad scheme", named(StartRequest{Endpoints: 10, Scheme: "ftp"}), true},
		{"most endpoints", named(StartRequest{Endpoints: MaxEndpoints}), false},
		{"no endpoints", named(StartRequest{}), true},
		{"too many endpoints", named(StartRequest{Endpoints: MaxEndpoints + 1}), true},
		{"negative timeout", named(StartRequest{Endpoints: 1, RequestTimeoutMs: -1}), true},
		{"negative interval", named(StartRequest{Endpoints: 1, IntervalMs: -1}), true},
		{"offset", named(StartRequest{Endpoints: 3, IntervalMs: 100, OffsetMs: 50}), false},
		{"offset of one period", named(StartRequest{Endpoints: 3, IntervalMs: 100, OffsetMs: 300}), false},
		{"offset over one period", named(StartRequest{Endpoints: 3, IntervalMs: 100, OffsetMs: 301}), true},
		{"negative offset", named(StartRequest{Endpoints: 1, IntervalMs: 100, OffsetMs: -1}), true},
		{"offset without interval", named(StartRequest{Endpoints: 1, OffsetMs: 1}), true},
		{"no identity", StartRequest{Endpoints: 1}, true},
		{"no actor", StartRequest{Endpoints: 1, Cell: "cell-1", Run: "run-1"}, true},
		{"no cell", StartRequest{Endpoints: 1, Actor: "benchmark-workloads/egress-0", Run: "run-1"}, true},
		{"no run", StartRequest{Endpoints: 1, Actor: "benchmark-workloads/egress-0", Cell: "cell-1"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.req.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestURLScheme(t *testing.T) {
	t.Parallel()
	if got := (&StartRequest{}).URLScheme(); got != SchemeHTTP {
		t.Errorf("URLScheme() with no scheme = %q, want %q", got, SchemeHTTP)
	}
	if got := (&StartRequest{Scheme: SchemeHTTPS}).URLScheme(); got != SchemeHTTPS {
		t.Errorf("URLScheme() with https = %q, want %q", got, SchemeHTTPS)
	}
}
