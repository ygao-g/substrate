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
	"log"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

// newHTTPSCmd is the TLS counterpart of the http origin, for the egress test
// that needs a session the gateway cannot see inside. The certificate comes
// from a mounted Secret rather than being minted here, so the test that deploys
// this pod holds the CA it has to hand the Actor to trust it -- which is what
// lets that test use an origin it owns instead of a publicly-trusted one.
func newHTTPSCmd() *cobra.Command {
	var listenAddress, certFile, keyFile string
	cmd := &cobra.Command{
		Use:   "https",
		Short: "Serve a TLS origin answering /healthz.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			log.Printf("testserver https: listening on %s", listenAddress)
			return newHTTPSServer(listenAddress).ListenAndServeTLS(certFile, keyFile)
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":8443", "Address the TLS origin listens on.")
	cmd.Flags().StringVar(&certFile, "cert", "", "PEM certificate chain the origin serves.")
	cmd.Flags().StringVar(&keyFile, "key", "", "PEM private key for --cert.")
	_ = cmd.MarkFlagRequired("cert")
	_ = cmd.MarkFlagRequired("key")
	return cmd
}

// newHTTPSServer builds the origin the https subcommand serves. Split out so a
// test can drive it over its own loopback listener, and so the pod and the test
// serve the same handler.
func newHTTPSServer(listenAddress string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return &http.Server{
		Addr:              listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      2 * time.Minute,
	}
}
