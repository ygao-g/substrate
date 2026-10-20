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

import "fmt"

// Control routes the actor serves on port 80, the only inbound port of a
// sandbox; metricsRoute, the exposition, is defined with its families.
const (
	readyzRoute = "/readyz"
	startRoute  = "/start"
	stopRoute   = "/stop"
)

// noLoopMessage is the body of the 404 that /metrics and /stop answer while
// no loop runs. A driver tells a fresh actor from one built before the
// exposition by it: the old image's /metrics is a plain 404.
const noLoopMessage = "no loop is running"

// defaultRequestTimeoutMs applies when startRequest.RequestTimeoutMs is zero.
const defaultRequestTimeoutMs = 5000

// The schemes the loop can request endpoints over.
const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// startRequest is the JSON body of POST /start.
type startRequest struct {
	// Endpoints is C: the loop requests endpointURL(0) through
	// endpointURL(Endpoints-1), each on its own goroutine, until it is stopped.
	Endpoints int `json:"endpoints"`
	// NewConnPerRequest opens a new TCP connection for every request instead
	// of keeping one alive per endpoint.
	NewConnPerRequest bool `json:"newConnPerRequest"`
	// RequestTimeoutMs bounds one request; zero means defaultRequestTimeoutMs.
	RequestTimeoutMs int64 `json:"requestTimeoutMs"`
	// IntervalMs paces the actor: endpoint i's first request starts i
	// intervals after the loop, and each endpoint is then requested once per
	// period of C intervals, so the actor sends one request per interval
	// throughout. Zero sends every endpoint's requests back to back.
	IntervalMs int64 `json:"intervalMs"`
	// Scheme is schemeHTTP or schemeHTTPS; empty means schemeHTTP.
	Scheme string `json:"scheme,omitempty"`
}

// urlScheme is the scheme the loop requests endpoints over.
func (r *startRequest) urlScheme() string {
	if r.Scheme == "" {
		return schemeHTTP
	}
	return r.Scheme
}

// validate reports whether r can start a loop.
func (r *startRequest) validate() error {
	if r.Endpoints < 1 || r.Endpoints > maxEndpoints {
		return fmt.Errorf("endpoints must be between 1 and %d, got %d", maxEndpoints, r.Endpoints)
	}
	if r.RequestTimeoutMs < 0 {
		return fmt.Errorf("requestTimeoutMs cannot be negative: %d", r.RequestTimeoutMs)
	}
	if r.IntervalMs < 0 {
		return fmt.Errorf("intervalMs cannot be negative: %d", r.IntervalMs)
	}
	if r.Scheme != "" && r.Scheme != schemeHTTP && r.Scheme != schemeHTTPS {
		return fmt.Errorf("scheme must be %s or %s, got %q", schemeHTTP, schemeHTTPS, r.Scheme)
	}
	return nil
}
