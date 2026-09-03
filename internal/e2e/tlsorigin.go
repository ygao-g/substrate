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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// tlsOriginValidity bounds the throwaway credentials MintTLSOriginSecret
// mints. Generous against a long suite, worthless to steal: the CA never
// signs anything else and no one but the minting test ever trusts it.
const tlsOriginValidity = 24 * time.Hour

// MintTLSOriginSecret mints a throwaway CA and a leaf for dnsName, writes the
// leaf into a kubernetes.io/tls Secret called name in namespace, and returns
// the CA's PEM — which is the test's proof of possession: only a caller handed
// this PEM can complete a handshake with the origin serving the Secret, so a
// fetch that succeeds with it identifies the TLS peer as exactly that origin.
//
// The Secret must exist before the pod that mounts it, so callers create it in
// a namespace of their own rather than the one DeployServerPod would make.
// Like DeployServerPod it registers no cleanup: the Secret goes with the
// namespace.
func MintTLSOriginSecret(t *testing.T, ctx context.Context, namespace, name, dnsName string) string {
	t.Helper()

	certPEM, keyPEM, caPEM, err := mintTLSOrigin(dnsName)
	if err != nil {
		t.Fatalf("minting the TLS origin credentials for %s: %v", dnsName, err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	if _, err := GetClients().K8s.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating TLS origin secret %s/%s: %v", namespace, name, err)
	}
	return string(caPEM)
}

// mintTLSOrigin mints the credential set MintTLSOriginSecret publishes: a
// fresh single-use CA and an ECDSA P-256 ServerAuth leaf for dnsName signed by
// it. Split from the Secret write so the minting has a unit test that does not
// need a cluster.
//
// Both certificates are backdated an hour: the chain is verified wherever the
// fetch runs, and in the micro-VM lanes that is a guest whose clock lags the
// minting host by minutes, which makes anything minted at time.Now "not yet
// valid" there. That backdate is why this mints its own root instead of using
// localca.GenerateCA, which does not backdate.
func mintTLSOrigin(dnsName string) (certPEM, keyPEM, caPEM []byte, err error) {
	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(tlsOriginValidity)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating the origin CA key: %w", err)
	}
	rootTemplate := &x509.Certificate{
		Subject:               pkix.Name{CommonName: "e2e-tls-origin"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, caKey.Public(), caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating the origin CA: %w", err)
	}
	rootCert, err := x509.ParseCertificate(rootDER)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parsing the origin CA: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating the origin key: %w", err)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		Subject:     pkix.Name{CommonName: dnsName},
		DNSNames:    []string{dnsName},
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, rootCert, key.Public(), caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("signing the origin leaf: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshaling the origin key: %w", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	return certPEM, keyPEM, caPEM, nil
}
