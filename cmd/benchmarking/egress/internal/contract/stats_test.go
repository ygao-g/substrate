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

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestStatsJSONRoundTrip pins the JSON a /stats or /stop answers with, as a
// driver decodes it: every counter, durations in nanoseconds, the errors by
// class and the per-endpoint rows, and nothing optional when there is nothing
// to report.
func TestStatsJSONRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		in      Stats
		present []string
		absent  []string
	}{
		{
			name: "every field",
			in: Stats{
				Scheme:                SchemeHTTPS,
				Elapsed:               61200 * time.Millisecond,
				Requests:              60,
				Successes:             55,
				NewConns:              4,
				Opened:                4,
				OpeningDone:           350 * time.Millisecond,
				RequestsAtOpeningDone: 4,
				Rounds:                15,
				LongestGap:            400 * time.Millisecond,
				Errors:                map[string]int64{"HTTP 503": 3, "timeout": 2},
				Endpoints:             []Endpoint{{Requests: 15, Errors: 1, LongestGap: 400 * time.Millisecond}, {Requests: 15}, {Requests: 15, Errors: 4}, {Requests: 15}},
			},
			present: []string{`"scheme":"https"`, `"elapsed":61200000000`, `"requests":60`, `"successes":55`, `"newConns":4`, `"opened":4`,
				`"openingDone":350000000`, `"requestsAtOpeningDone":4`, `"rounds":15`, `"longestGap":400000000`, `"errors":{"HTTP 503":3,"timeout":2}`, `"endpoints":[{"requests":15,"errors":1,"longestGap":400000000}`},
		},
		{
			name:    "counters only",
			in:      Stats{Scheme: SchemeHTTP, Requests: 3},
			present: []string{`"scheme":"http"`, `"requests":3`},
			absent:  []string{"endpoints", "errors"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range tc.present {
				if !strings.Contains(string(b), want) {
					t.Errorf("Stats JSON lacks %s: %s", want, b)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(string(b), absent) {
					t.Errorf("Stats JSON carries %q: %s", absent, b)
				}
			}
			var back Stats
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back, tc.in) {
				t.Errorf("round trip = %+v, want %+v", back, tc.in)
			}
		})
	}
}
