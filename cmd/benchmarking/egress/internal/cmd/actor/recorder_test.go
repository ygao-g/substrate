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

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
)

// newTestRecorder is a Recorder with a manual reader on it.
func newTestRecorder(t *testing.T) (*Recorder, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	r, err := NewRecorder(context.Background(), contract.StartRequest{}, reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })
	return r, reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

// metricsByName indexes the one scope's metrics, failing on any other scope.
func metricsByName(t *testing.T, rm metricdata.ResourceMetrics) map[string]metricdata.Metrics {
	t.Helper()
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != meterName {
			t.Errorf("scope %q, want only %q", sm.Scope.Name, meterName)
		}
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// recordOneOfEach puts one measurement into every instrument.
func recordOneOfEach(ctx context.Context, r *Recorder) {
	r.Request(ctx, true)
	r.Request(ctx, false)
	r.RequestError(ctx, "timeout")
	r.ConnectionOpened(ctx)
	r.EndpointOpened(ctx)
	r.Gauges(ctx, 2, 1.5, 0.25)
	for i, name := range contract.DistributionNames {
		r.Record(ctx, name, float64(1+i)*1e-3)
	}
}

func TestIdleInstrumentsAreAbsent(t *testing.T) {
	t.Parallel()
	r, reader := newTestRecorder(t)
	if got := metricsByName(t, collect(t, reader)); len(got) != 0 {
		t.Errorf("fresh recorder collected %v, want nothing", got)
	}
	r.Record(context.Background(), contract.DNS, 0.002)
	got := metricsByName(t, collect(t, reader))
	if _, ok := got[string(contract.DNS)]; !ok || len(got) != 1 {
		t.Errorf("after one dns sample the reader holds %v, want dns alone", got)
	}
}

func TestRecordAfterShutdownDoesNotPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r, err := NewRecorder(ctx, contract.StartRequest{})
	if err != nil {
		t.Fatal(err)
	}
	recordOneOfEach(ctx, r)
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	recordOneOfEach(ctx, r)
	_ = r.ForceFlush(ctx)
}

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
