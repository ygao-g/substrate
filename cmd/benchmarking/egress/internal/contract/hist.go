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
	"fmt"
	"strconv"
)

const (
	bucketsPerDecade = 20
	// maxBucketSeconds stops the bounds: the first bound at or past it is the
	// last. Slower samples count in the overflow bucket.
	maxBucketSeconds = 60
)

// mantissas are 10^(j/20) for j = 0..19, to four significant digits.
var mantissas = [bucketsPerDecade]string{
	"1", "1.122", "1.259", "1.413", "1.585", "1.778", "1.995", "2.239", "2.512", "2.818",
	"3.162", "3.548", "3.981", "4.467", "5.012", "5.623", "6.31", "7.079", "7.943", "8.913",
}

// Bounds are the upper bounds, in seconds, of every distribution: 20 per
// decade from 1 µs to 63.1 s. Each is parsed from a decimal string, so every
// build on every architecture holds the same 157 values, and a sum by bucket
// across the fleet is meaningful. Neighboring bounds differ by about 12%, the
// resolution of a quantile read from buckets.
var Bounds = func() []float64 {
	var bounds []float64
	for k := 0; ; k++ {
		b, err := strconv.ParseFloat(fmt.Sprintf("%se%d", mantissas[k%bucketsPerDecade], k/bucketsPerDecade-6), 64)
		if err != nil {
			panic(err)
		}
		bounds = append(bounds, b)
		if b >= maxBucketSeconds {
			return bounds
		}
	}
}()

// Distribution names one of the four latency histograms the actor exports,
// the instrument name on the wire.
type Distribution string

const (
	Opening Distribution = "opening" // end-to-end time of an endpoint's first request, when it succeeded
	Latency Distribution = "latency" // end-to-end time of later successful requests
	DNS     Distribution = "dns"     // lookup time, for requests that did a lookup
	TLS     Distribution = "tls"     // handshake time, for handshakes that completed
)

// DistributionNames lists the four distributions.
var DistributionNames = []Distribution{Opening, Latency, DNS, TLS}
