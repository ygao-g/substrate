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

// Command egress-tunnels drives the egress scale test: it creates A actors,
// resumes B of them, and has each resumed actor loop over HTTP requests to C
// in-cluster endpoints through the egress gateway. See README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/egresstunnels"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/portforward"
)

const usage = `Usage: go run ./cmd/benchmarking/egress-tunnels <command> [flags]

Commands:
  run       create the actors, run the egress loops, and print a report
  cleanup   delete the actors a run created
  certs     write a CA and the target's HTTPS certificate (deploy.sh uses this)

Run "go run ./cmd/benchmarking/egress-tunnels <command> -h" for the flags of a command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(ctx, os.Args[2:])
	case "cleanup":
		err = cleanupCmd(ctx, os.Args[2:])
	case "certs":
		err = certsCmd(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "egress-tunnels %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

// clusterFlags select the cluster and how to reach ateapi.
type clusterFlags struct {
	kubeconfig   string
	kubeContext  string
	apiEndpoint  string
	apiTokenFile string
	atespace     string
}

func (f *clusterFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "Path to the kubeconfig. Empty uses the default loading rules.")
	fs.StringVar(&f.kubeContext, "context", "", "Kubeconfig context. Empty uses the current context.")
	fs.StringVar(&f.apiEndpoint, "api-endpoint", "", "ateapi address. Empty port-forwards to the api Service.")
	fs.StringVar(&f.apiTokenFile, "api-token-file", "", "File holding the bearer token for ateapi, re-read on every call, such as a projected ServiceAccount token. Needs --api-endpoint. Empty mints an ate-client token.")
	fs.StringVar(&f.atespace, "atespace", "benchmark-workloads", "Atespace of the actor template and the actors.")
}

// connection is what both commands talk to.
type connection struct {
	api *ateclient.Client
	k8s kubernetes.Interface
}

// newATEClient is replaced in tests.
var newATEClient = ateclient.NewClient

func (f *clusterFlags) connect(ctx context.Context) (*connection, error) {
	if f.apiTokenFile != "" && f.apiEndpoint == "" {
		return nil, errors.New("--api-token-file needs --api-endpoint")
	}
	cfg, err := ateclient.LoadKubeConfig(f.kubeconfig, f.kubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kubernetes client: %w", err)
	}
	api, err := newATEClient(ctx, f.kubeconfig, f.kubeContext, f.apiEndpoint, f.apiTokenFile, false)
	if err != nil {
		return nil, fmt.Errorf("connecting to ateapi: %w", err)
	}
	return &connection{api: api, k8s: k8s}, nil
}

// runOptions is what the run command takes besides the runConfig.
type runOptions struct {
	cluster   clusterFlags
	routerURL string
	output    string
}

// parseRunFlags parses the run command's flags into a validated config.
func parseRunFlags(args []string) (runConfig, runOptions, error) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var opts runOptions
	opts.cluster.register(fs)
	cfg := runConfig{}
	fs.IntVar(&cfg.Actors, "actors", 1000, "A: actors to create. Reruns reuse the actors that exist.")
	fs.IntVar(&cfg.Parallel, "parallel", 1, "B: actors to resume and run the loop in, at the same time.")
	fs.IntVar(&cfg.Endpoints, "endpoints", 10, fmt.Sprintf("C: endpoints each loop holds a tunnel to, egress-target-0 up to egress-target-<C-1> (at most %d).", egresstunnels.MaxEndpoints))
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "How long the loops run once all of them have started.")
	fs.StringVar(&cfg.ConnMode, "conn-mode", connModeKeepAlive, "How the loops connect: keepalive (one connection per endpoint) or new-conn (one per request).")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 5*time.Second, "Timeout of one request.")
	fs.DurationVar(&cfg.RequestInterval, "request-interval", 100*time.Millisecond, "Each actor's pacing: one request per interval, so each endpoint is requested once per --endpoints intervals; 0 sends back to back at full speed.")
	fs.IntVar(&cfg.CreateConcurrency, "create-concurrency", 32, "Actors created at the same time.")
	fs.DurationVar(&cfg.ResumeTimeout, "resume-timeout", 5*time.Minute, "Per actor: how long to keep resuming (a full pool is retried) and waiting for it to answer.")
	fs.DurationVar(&cfg.ProgressInterval, "progress-interval", 30*time.Second, "How often to print progress; 0 turns it off.")
	fs.IntVar(&cfg.StartBatch, "start-batch", 0, "Start the loops in batches of this many actors, --start-gap apart, so one batch's opens (--start-batch / --request-interval per second) fit the gateway's handshake budget; 0 starts them all at once.")
	fs.DurationVar(&cfg.StartGap, "start-gap", 0, "Pause between batches of loop starts. With --start-batch and no --start-gap, one opening window: --endpoints x --request-interval.")
	fs.DurationVar(&cfg.StreamIdleTimeout, "stream-idle-timeout", 5*time.Minute, "The gateway's CONNECT stream idle timeout. A keep-alive run is refused when the per-endpoint period, --endpoints x --request-interval, is more than half of it; 0 skips the check.")
	fs.DurationVar(&cfg.Settle, "settle", 0, "If set, start the measured --duration only once every loop has opened all its endpoints with a success each and requested each of them twice, and no tunnel has been redialed for this long. Needs --progress-interval and must cover one period (--endpoints x --request-interval).")
	fs.DurationVar(&cfg.SettleTimeout, "settle-timeout", 30*time.Minute, "How long to wait for --settle before measuring anyway.")
	fs.StringVar(&cfg.Template, "template", "egress", "Actor template the actors are created from; WORKLOAD_TEMPLATES=egress benchmarking/workloads/deploy.sh creates it.")
	fs.StringVar(&cfg.Scheme, "scheme", egresstunnels.SchemeHTTP, "Scheme the loops request endpoints over: http, or https through the gateway's TLS interception (needs deploy.sh --https).")
	fs.StringVar(&opts.routerURL, "router-url", "", "atenet router base URL. Empty port-forwards to the atenet-router Service.")
	fs.StringVar(&opts.output, "output", "", "If set, write the report as JSON to this file.")
	if err := fs.Parse(args); err != nil {
		return cfg, opts, err
	}
	cfg.Atespace = opts.cluster.atespace
	gapSet := false
	fs.Visit(func(f *flag.Flag) { gapSet = gapSet || f.Name == "start-gap" })
	if cfg.StartBatch > 0 && !gapSet {
		cfg.StartGap = cfg.period()
	}
	return cfg, opts, cfg.validate()
}

func runCmd(ctx context.Context, args []string) error {
	cfg, opts, err := parseRunFlags(args)
	if err != nil {
		return err
	}
	cf, routerURL, output := opts.cluster, &opts.routerURL, &opts.output
	conn, err := cf.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.api.Close()

	if *routerURL == "" {
		restCfg, err := ateclient.LoadKubeConfig(cf.kubeconfig, cf.kubeContext)
		if err != nil {
			return fmt.Errorf("loading kubeconfig: %w", err)
		}
		port, stopForward, err := portforward.ServicePortForward(ctx, restCfg, conn.k8s,
			installdefaults.SystemNamespace, installdefaults.RouterServiceName, 80)
		if err != nil {
			return fmt.Errorf("port-forwarding to %s: %w", installdefaults.RouterServiceName, err)
		}
		defer stopForward()
		*routerURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	}

	r := newRunner(cfg, conn.api, conn.k8s, newRouterClient(*routerURL, cfg.Atespace, cfg.Parallel), os.Stdout)
	rep, runErr := r.run(ctx)
	if rep != nil {
		rep.print(os.Stdout)
		if *output != "" {
			if err := writeReport(*output, rep); err != nil {
				return errors.Join(runErr, err)
			}
			fmt.Fprintf(os.Stdout, "report written to %s\n", *output)
		}
	}
	return runErr
}

func writeReport(path string, rep *report) error {
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func cleanupCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	var cf clusterFlags
	cf.register(fs)
	actors := fs.Int("actors", 1000, "Delete actors egress-0 up to egress-<actors-1>. Use the largest --actors any run used.")
	concurrency := fs.Int("concurrency", 64, "Actors deleted at the same time.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *actors < 1 || *concurrency < 1 {
		return errors.New("--actors and --concurrency must be at least 1")
	}

	conn, err := cf.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.api.Close()

	res := cleanup(ctx, conn.api, defaultBackoff, cf.atespace, *actors, *concurrency, os.Stdout)
	printPhase(os.Stdout, "cleanup", res.phaseResult, fmt.Sprintf(" (%d did not exist)", res.NotFound))
	if res.Failed > 0 {
		return fmt.Errorf("%d actors were not deleted", res.Failed)
	}
	return ctx.Err()
}

func certsCmd(args []string) error {
	fs := flag.NewFlagSet("certs", flag.ExitOnError)
	out := fs.String("out", "", "Directory to write ca.crt, tls.crt and tls.key to. Required.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("--out is required")
	}
	bundle, err := egresstunnels.GenerateTargetCert(time.Now())
	if err != nil {
		return err
	}
	return bundle.Write(*out)
}
