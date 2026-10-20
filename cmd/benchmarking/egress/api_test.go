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

import "testing"

func TestStartRequestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		req     startRequest
		wantErr bool
	}{
		{"ok", startRequest{Endpoints: 10}, false},
		{"http", startRequest{Endpoints: 10, Scheme: schemeHTTP}, false},
		{"https", startRequest{Endpoints: 10, Scheme: schemeHTTPS}, false},
		{"bad scheme", startRequest{Endpoints: 10, Scheme: "ftp"}, true},
		{"most endpoints", startRequest{Endpoints: maxEndpoints}, false},
		{"no endpoints", startRequest{}, true},
		{"too many endpoints", startRequest{Endpoints: maxEndpoints + 1}, true},
		{"negative timeout", startRequest{Endpoints: 1, RequestTimeoutMs: -1}, true},
		{"negative interval", startRequest{Endpoints: 1, IntervalMs: -1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.req.validate(); (err != nil) != tc.wantErr {
				t.Errorf("validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestURLScheme(t *testing.T) {
	if got := (&startRequest{}).urlScheme(); got != schemeHTTP {
		t.Errorf("urlScheme() with no scheme = %q, want %q", got, schemeHTTP)
	}
	if got := (&startRequest{Scheme: schemeHTTPS}).urlScheme(); got != schemeHTTPS {
		t.Errorf("urlScheme() with https = %q, want %q", got, schemeHTTPS)
	}
}
