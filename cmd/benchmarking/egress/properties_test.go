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
	"bytes"
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"
)

// TestAggregationIsOrderIndependent adds the same scrapes in random orders
// and expects byte-identical families; a scrape added to itself doubles.
// CONTRACT G30.
func TestAggregationIsOrderIndependent(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 11))
	var scrapes []families
	for a := range 5 {
		var ds []time.Duration
		for range 200 + 50*a {
			ds = append(ds, time.Duration(50+r.IntN(2_000_000))*time.Microsecond)
		}
		scrapes = append(scrapes, fixtureScrape(t, 3, func(m *loopMetrics) {
			recordLatencies(ds...)(m)
			ctx := context.Background()
			m.requests.Add(ctx, int64(a), m.failure)
			m.requestErrors.Add(ctx, int64(a), m.class("timeout"))
			m.longestGap.Record(ctx, float64(a))
			m.rounds.Record(ctx, int64(a+1))
			m.endpointRequests.Add(ctx, int64(10*a), m.endpointResult[a%3][0])
		}))
	}
	encode := func(f families) []byte {
		// Encode in name order; the text encoder is deterministic per family.
		var buf bytes.Buffer
		enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
		names := make([]string, 0, len(f))
		for name := range f {
			names = append(names, name)
		}
		sortStrings(names)
		for _, name := range names {
			if err := enc.Encode(f[name]); err != nil {
				t.Fatal(err)
			}
		}
		return buf.Bytes()
	}
	var want []byte
	for trial := range 20 {
		order := r.Perm(len(scrapes))
		sum := families{}
		for _, i := range order {
			if err := sum.add(scrapes[i].clone(), "a"); err != nil {
				t.Fatal(err)
			}
		}
		got := encode(sum)
		if trial == 0 {
			want = got
			continue
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("order %v gives a different aggregate", order)
		}
	}
	one := scrapes[0]
	double := families{}
	for range 2 {
		if err := double.add(one.clone(), "a"); err != nil {
			t.Fatal(err)
		}
	}
	if double.counter(famRequests) != 2*one.counter(famRequests) || double.histogram(famLatency).GetSampleCount() != 2*one.histogram(famLatency).GetSampleCount() ||
		double.gauge(famLongestGap) != one.gauge(famLongestGap) || double.gauge(famRounds) != 2*one.gauge(famRounds) {
		t.Errorf("a scrape added to itself: requests %d vs %d, samples %d vs %d, longest gap %v vs %v, rounds %v vs %v",
			double.counter(famRequests), one.counter(famRequests), double.histogram(famLatency).GetSampleCount(), one.histogram(famLatency).GetSampleCount(),
			double.gauge(famLongestGap), one.gauge(famLongestGap), double.gauge(famRounds), one.gauge(famRounds))
	}
	// A clone is a true copy: adding to it leaves the original whole.
	c := one.clone()
	if err := c.add(one, "a"); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(scrapes[0][famLatency], one[famLatency]) || c.counter(famRequests) != 2*one.counter(famRequests) {
		t.Errorf("clone shares state with the original")
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
