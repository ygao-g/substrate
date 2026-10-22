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
	"reflect"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
)

// identity is the actor, cell and run every test loop starts under.
var identity = contract.StartRequest{Actor: "benchmark-workloads/egress-0", Cell: "cell-1", Run: "run-1"}

// identified is req with the identity fields filled where a test left them
// empty, so a test states only what it is about.
func identified(req contract.StartRequest) contract.StartRequest {
	if req.Actor == "" {
		req.Actor = identity.Actor
	}
	if req.Cell == "" {
		req.Cell = identity.Cell
	}
	if req.Run == "" {
		req.Run = identity.Run
	}
	return req
}

// loopAttrs are the attributes every point of a loop started with identity
// carries.
var loopAttrs = attribute.NewSet(attribute.String("actor", identity.Actor), attribute.String("cell", identity.Cell), attribute.String("run", identity.Run))

// metricNames are the eleven instruments of the contract, as the SDK names
// them before the collector adds its suffixes.
var metricNames = []string{requestsName, requestErrorsName, connsOpenedName, endpointsOpenedName, roundsName, openingDoneName, longestGapName,
	string(contract.Opening), string(contract.Latency), string(contract.DNS), string(contract.TLS)}

// newTestRecorder is a Recorder for identity with a manual reader on it.
func newTestRecorder(t *testing.T) (*Recorder, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	r, err := NewRecorder(context.Background(), identity, reader)
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

// pointAttrs returns the attribute set of every data point of m.
func pointAttrs(t *testing.T, m metricdata.Metrics) []attribute.Set {
	t.Helper()
	var sets []attribute.Set
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Gauge[float64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	default:
		t.Errorf("%s has data of type %T", m.Name, m.Data)
	}
	return sets
}

// TestInstrumentsCarryTheLoopAttributes pins the wire contract of the
// recorder: the eleven names, the units that become _seconds, the shared
// bounds, and actor, cell and run on every point, with result and class
// beside them where the contract says so.
func TestInstrumentsCarryTheLoopAttributes(t *testing.T) {
	t.Parallel()
	r, reader := newTestRecorder(t)
	recordOneOfEach(context.Background(), r)
	byName := metricsByName(t, collect(t, reader))

	var names []string
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	if want := slices.Sorted(slices.Values(metricNames)); !slices.Equal(names, want) {
		t.Errorf("instruments = %v, want %v", names, want)
	}
	for name, m := range byName {
		wantUnit := ""
		if slices.Contains([]string{openingDoneName, longestGapName}, name) || slices.Contains(contract.DistributionNames, contract.Distribution(name)) {
			wantUnit = "s"
		}
		if m.Unit != wantUnit {
			t.Errorf("%s has unit %q, want %q", name, m.Unit, wantUnit)
		}
		sets := pointAttrs(t, m)
		if len(sets) == 0 {
			t.Errorf("%s has no data point", name)
		}
		for _, set := range sets {
			for _, kv := range loopAttrs.ToSlice() {
				if v, ok := set.Value(kv.Key); !ok || v != kv.Value {
					t.Errorf("%s point %v lacks %s=%s", name, set.Encoded(attribute.DefaultEncoder()), kv.Key, kv.Value.AsString())
				}
			}
			extra := set.Len() - loopAttrs.Len()
			switch name {
			case requestsName:
				if v, ok := set.Value("result"); extra != 1 || !ok || (v.AsString() != "success" && v.AsString() != "error") {
					t.Errorf("requests point has %v, want the loop attributes plus result", set.Encoded(attribute.DefaultEncoder()))
				}
			case requestErrorsName:
				if v, ok := set.Value("class"); extra != 1 || !ok || v.AsString() != "timeout" {
					t.Errorf("request_errors point has %v, want the loop attributes plus class", set.Encoded(attribute.DefaultEncoder()))
				}
			default:
				if extra != 0 {
					t.Errorf("%s point has %v, want the loop attributes alone", name, set.Encoded(attribute.DefaultEncoder()))
				}
			}
		}
	}
	if sum, ok := byName[requestsName].Data.(metricdata.Sum[int64]); !ok || len(sum.DataPoints) != 2 || !sum.IsMonotonic || sum.Temporality != metricdata.CumulativeTemporality {
		t.Errorf("requests = %+v, want a cumulative monotonic sum with a success and an error point", byName[requestsName].Data)
	}
	if g, ok := byName[roundsName].Data.(metricdata.Gauge[int64]); !ok || g.DataPoints[0].Value != 2 {
		t.Errorf("rounds = %+v, want a gauge at 2", byName[roundsName].Data)
	}
	if g, ok := byName[openingDoneName].Data.(metricdata.Gauge[float64]); !ok || g.DataPoints[0].Value != 1.5 {
		t.Errorf("opening_done = %+v, want a gauge at 1.5", byName[openingDoneName].Data)
	}
	for i, name := range contract.DistributionNames {
		h, ok := byName[string(name)].Data.(metricdata.Histogram[float64])
		if !ok || len(h.DataPoints) != 1 {
			t.Fatalf("%s = %+v, want a histogram with one point", name, byName[string(name)].Data)
		}
		p := h.DataPoints[0]
		if !reflect.DeepEqual(p.Bounds, contract.Bounds) || p.Count != 1 || p.Sum != float64(1+i)*1e-3 || len(p.BucketCounts) != len(contract.Bounds)+1 {
			t.Errorf("%s point: %d bounds, count %d, sum %v; want contract.Bounds, 1, %v", name, len(p.Bounds), p.Count, p.Sum, float64(1+i)*1e-3)
		}
	}
}

// TestResourceDefaultsTheServiceName pins the recorder's own service.name,
// the job the series are stored under when the environment sets none.
func TestResourceDefaultsTheServiceName(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	_, reader := newTestRecorder(t)
	if name, _ := collect(t, reader).Resource.Set().Value("service.name"); name.AsString() != serviceName {
		t.Errorf("service.name without the env = %q, want %q", name.AsString(), serviceName)
	}
}

func BenchmarkRecord(b *testing.B) {
	r, err := NewRecorder(context.Background(), identity)
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
