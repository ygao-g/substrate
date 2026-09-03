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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
)

// The e2e suite reads this origin's answers as evidence about the egress
// tunnel, so these tests pin them over loopback, with no gateway involved.

const testOriginDNSName = "https-origin.invalid"

// mintOriginFiles writes a throwaway CA's leaf for testOriginDNSName into the
// test's temp dir and returns the cert path, the key path and the CA in PEM.
func mintOriginFiles(t *testing.T) (certFile, keyFile string, caPEM []byte) {
	t.Helper()

	ca, err := localca.GenerateCA("testserver-https", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatalf("generating the origin CA: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating the origin key: %v", err)
	}
	chain, err := (&localca.ConcretePool{CAs: []*localca.CA{ca}}).CreateCertificate(&x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: testOriginDNSName},
		DNSNames:     []string{testOriginDNSName},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, key.Public())
	if err != nil {
		t.Fatalf("signing the origin leaf: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling the origin key: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")
	write := func(path, blockType string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
	}
	write(certFile, "CERTIFICATE", chain[0])
	write(keyFile, "PRIVATE KEY", keyDER)
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw})
	return certFile, keyFile, caPEM
}

// serveOriginLocal starts the origin on a real loopback listener and returns
// its address, so the handshake under test is the one the pod performs.
func serveOriginLocal(t *testing.T, certFile, keyFile string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := newHTTPSServer(listener.Addr().String())
	go func() {
		if err := server.ServeTLS(listener, certFile, keyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(func() { _ = server.Close() })
	return listener.Addr().String()
}

// getHealthz dials address with the given TLS configuration and returns the
// status of a GET /healthz.
func getHealthz(t *testing.T, address string, config *tls.Config) (int, error) {
	t.Helper()

	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: config},
		Timeout:   10 * time.Second,
	}
	response, err := client.Get("https://" + address + "/healthz")
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode, nil
}

// A caller trusting the minting CA reaches the origin; on the system roots it
// must not -- the ownership property the egress test relies on.
func TestHTTPSOriginServesTheMintedLeaf(t *testing.T) {
	certFile, keyFile, caPEM := mintOriginFiles(t)
	address := serveOriginLocal(t, certFile, keyFile)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatalf("the minted CA is not a usable PEM certificate:\n%s", caPEM)
	}

	status, err := getHealthz(t, address, &tls.Config{RootCAs: roots, ServerName: testOriginDNSName})
	if err != nil {
		t.Fatalf("GET /healthz with the minting CA trusted: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("GET /healthz = %d, want %d", status, http.StatusOK)
	}

	if _, err := getHealthz(t, address, &tls.Config{ServerName: testOriginDNSName}); err == nil {
		t.Error("GET /healthz on the system roots succeeded, want a certificate error")
	}
}

// The name arrives out of band (the caller dials an IP), so a wrong name has
// to fail, not just a wrong chain.
func TestHTTPSOriginRejectsAWrongServerName(t *testing.T) {
	certFile, keyFile, caPEM := mintOriginFiles(t)
	address := serveOriginLocal(t, certFile, keyFile)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatalf("the minted CA is not a usable PEM certificate:\n%s", caPEM)
	}

	if _, err := getHealthz(t, address, &tls.Config{RootCAs: roots, ServerName: "other.invalid"}); err == nil {
		t.Error("GET /healthz under the wrong server name succeeded, want a certificate error")
	}
}

// Missing credentials must fail flag validation, not start an origin no
// caller can handshake with.
func TestHTTPSCmdRequiresCredentials(t *testing.T) {
	for _, args := range [][]string{{}, {"--cert=/nonexistent"}, {"--key=/nonexistent"}} {
		cmd := newHTTPSCmd()
		cmd.SetArgs(args)
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		if err := cmd.Execute(); err == nil {
			t.Errorf("testserver https %v succeeded, want a required-flag error", args)
		}
	}
}
