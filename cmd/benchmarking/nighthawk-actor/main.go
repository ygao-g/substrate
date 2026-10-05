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

// nighthawk-actor is the load-generating actor of the Nighthawk egress
// benchmark. It serves a small HTTP control API on port 80, the one inbound
// port atunnel forwards into the sandbox, and on each POST /run spawns
// nighthawk_service and nighthawk_adaptive_load_client inside the actor, so
// the load leaves through the actor's egress path. See README.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
)

var (
	listenAddr     = pflag.String("listen", ":80", "Address the control API listens on.")
	workDir        = pflag.String("work-dir", "/tmp/nighthawk-actor", "Directory under which each run writes its spec and output.")
	serviceBin     = pflag.String("nighthawk-service", "nighthawk_service", "Path to the nighthawk_service binary.")
	adaptiveBin    = pflag.String("nighthawk-adaptive-client", "nighthawk_adaptive_load_client", "Path to the nighthawk_adaptive_load_client binary.")
	serviceAddress = pflag.String("service-address", "127.0.0.1:8443", "Loopback address nighthawk_service listens on for the adaptive client.")
)

func main() {
	pflag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(*workDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "create work dir %s: %v\n", *workDir, err)
		os.Exit(1)
	}

	ctl := newController(config{
		workDir:        *workDir,
		serviceBin:     *serviceBin,
		adaptiveBin:    *adaptiveBin,
		serviceAddress: *serviceAddress,
	}, execRunner{}, dialReady)
	srv := &http.Server{Addr: *listenAddr, Handler: ctl.handler()}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case err := <-errCh:
		fmt.Fprintf(os.Stderr, "serve %s: %v\n", *listenAddr, err)
		os.Exit(1)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctl.close()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
		os.Exit(1)
	}
}
