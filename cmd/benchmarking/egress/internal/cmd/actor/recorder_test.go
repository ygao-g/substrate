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

package actor

import (
	"context"
	"testing"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
)

func BenchmarkRecord(b *testing.B) {
	r, err := NewRecorder(context.Background(), contract.StartRequest{})
	if err != nil {
		b.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	ctx := context.Background()
	for i := 0; b.Loop(); i++ {
		r.Request(ctx, true)
		r.Record(ctx, contract.Latency, float64(i%1000)*1e-5)
	}
}
