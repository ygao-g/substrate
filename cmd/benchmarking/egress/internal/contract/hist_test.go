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
	"strconv"
	"testing"
)

func TestBounds(t *testing.T) {
	t.Parallel()
	if n := len(Bounds); n != 157 {
		t.Fatalf("len(Bounds) = %d, want 157", n)
	}
	if Bounds[0] != 1e-6 || Bounds[60] != 0.001 || Bounds[156] != 63.1 {
		t.Errorf("Bounds[0,60,156] = %v %v %v, want 1e-06 0.001 63.1", Bounds[0], Bounds[60], Bounds[156])
	}
	for i, b := range Bounds {
		if i > 0 && b <= Bounds[i-1] {
			t.Errorf("Bounds[%d] = %v does not exceed Bounds[%d] = %v", i, b, i-1, Bounds[i-1])
		}
		if again, _ := strconv.ParseFloat(strconv.FormatFloat(b, 'g', 4, 64), 64); again != b {
			t.Errorf("Bounds[%d] = %v has more than four significant digits", i, b)
		}
	}
}
