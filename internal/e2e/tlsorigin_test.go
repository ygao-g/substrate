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

package e2e

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

// The minted set has to hold together end to end: the cert/key pair must be
// loadable by the server that will mount it, and the leaf must chain to the
// returned CA under exactly the minted name — that verification is the
// assertion the HTTPS egress test outsources to the TLS handshake.
func TestMintTLSOrigin(t *testing.T) {
	const dnsName = "egress-https-origin.invalid"
	certPEM, keyPEM, caPEM, err := mintTLSOrigin(dnsName)
	if err != nil {
		t.Fatalf("mintTLSOrigin: %v", err)
	}

	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		t.Errorf("the minted pair does not load as a TLS certificate: %v", err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatalf("the minted CA is not a usable PEM certificate:\n%s", caPEM)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatalf("the minted certificate is not PEM:\n%s", certPEM)
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing the minted leaf: %v", err)
	}

	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: dnsName}); err != nil {
		t.Errorf("the leaf does not verify against the returned CA for %s: %v", dnsName, err)
	}

	// The chain must verify on a clock running behind the minting host: the
	// micro-VM guest's clock lags by minutes, and an un-backdated root fails
	// there with "not yet valid" even though the leaf is backdated.
	skewed := time.Now().Add(-30 * time.Minute)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: dnsName, CurrentTime: skewed}); err != nil {
		t.Errorf("the chain does not verify on a clock %s behind the mint: %v", time.Since(skewed).Round(time.Minute), err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "other.invalid"}); err == nil {
		t.Error("the leaf verifies under a name it was not minted for")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: x509.NewCertPool(), DNSName: dnsName}); err == nil {
		t.Error("the leaf verifies with no trust anchors at all")
	}
}
