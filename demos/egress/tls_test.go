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
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// tlsOrigin starts a TLS server answering /healthz and returns its URL and the
// PEM of the certificate it serves, which is self-signed and therefore its own
// trust anchor. Nothing in the system roots vouches for it, which is what makes
// it stand in for the in-cluster origin the e2e suite deploys.
func tlsOrigin(t *testing.T) (url, caPEM string) {
	t.Helper()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("from the private origin"))
	}))
	t.Cleanup(server.Close)

	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	return server.URL + "/healthz", string(encoded)
}

// fetchThrough posts input to the demo's fetch endpoint, backed by a client
// that really dials, and returns the recorder.
func fetchThrough(t *testing.T, input fetchRequest) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshaling the fetch request: %v", err)
	}
	recorder := httptest.NewRecorder()
	newHandler(&http.Client{Timeout: requestTimeout}).ServeHTTP(
		recorder, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload))))
	return recorder
}

// A request that names no TLS settings has to keep the client it was given,
// unchanged: every other test here reads the demo's behavior off an injected
// client, and a fetch path that quietly built its own would make those tests
// meaningless while still passing.
func TestClientForKeepsTheBaseClient(t *testing.T) {
	base := &http.Client{Timeout: requestTimeout}
	got, err := clientFor(base, fetchRequest{URL: "https://example.com/"})
	if err != nil {
		t.Fatalf("clientFor with no TLS settings: %v", err)
	}
	if got != base {
		t.Errorf("clientFor returned a new client %p, want the base client %p", got, base)
	}
}

// The supplied anchors have to be the ones that decide the handshake: trusted
// when handed over, and not otherwise. The second half is what makes this a
// test of the caller's CA rather than of the public PKI.
func TestFetchTrustsTheSuppliedCA(t *testing.T) {
	url, caPEM := tlsOrigin(t)

	recorder := fetchThrough(t, fetchRequest{URL: url, CAPEM: caPEM})
	if recorder.Code != http.StatusOK {
		t.Fatalf("fetch with the origin's CA = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
	}
	var got fetchResponse
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if got.Body != "from the private origin" {
		t.Errorf("response body = %q, want the origin's", got.Body)
	}

	recorder = fetchThrough(t, fetchRequest{URL: url})
	if recorder.Code != http.StatusBadGateway {
		t.Errorf("fetch on the system roots = %d, want %d; body = %s",
			recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

// serverName has to reach the handshake. The e2e caller dials a ClusterIP and
// supplies the certificate's name out of band, so a serverName that is ignored
// would make that fetch fail, and one that is never checked would make the
// whole verification vacuous.
func TestFetchChecksTheSuppliedServerName(t *testing.T) {
	url, caPEM := tlsOrigin(t)

	// httptest's certificate is issued for example.com as well as the loopback
	// address the URL carries, so this succeeds only if the name is honored.
	recorder := fetchThrough(t, fetchRequest{URL: url, CAPEM: caPEM, ServerName: "example.com"})
	if recorder.Code != http.StatusOK {
		t.Errorf("fetch with serverName=example.com = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
	}

	recorder = fetchThrough(t, fetchRequest{URL: url, CAPEM: caPEM, ServerName: "wrong.invalid"})
	if recorder.Code != http.StatusBadGateway {
		t.Errorf("fetch with a mismatched serverName = %d, want %d; body = %s",
			recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

// A caPEM that carries no certificate is the caller's mistake, so it has to be
// rejected before anything is dialed rather than silently falling back to the
// system roots -- which would turn a typo into a fetch that verified against
// anchors the caller never asked for.
func TestFetchRejectsAnUnusableCAPEM(t *testing.T) {
	dialed := false
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		dialed = true
		return nil, nil
	})}

	recorder := httptest.NewRecorder()
	newHandler(client).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader(`{"url":"https://example.com/","caPEM":"not a certificate"}`)))

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if dialed {
		t.Error("the demo dialed the origin despite the unusable caPEM")
	}
}
