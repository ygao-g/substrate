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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/portforward"
)

// driverCommand drives the egress scale test from outside the cluster: run
// creates A actors, resumes B of them, and has each resumed actor loop over
// HTTP requests to C in-cluster endpoints through the egress gateway; cleanup
// deletes the actors; certs writes the target's certificate for deploy.sh.
//
// Its files: run.go carries a run through its phases, report.go prints what
// happened, aggregate.go merges the actors' scrapes into the fleet's,
// router.go reaches the actors through the atenet router, policy.go builds
// the egress policy and checks the deploy, gateway.go checks the gateway's
// trust patch, retry.go retries control-plane calls, cleanup.go deletes the
// actors, and targetcert.go issues the certificate. The code map in
// benchmarking/egress-tunnels/README.md has the phases and their functions.
func driverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "driver",
		Short: "Drive the egress scale test: create and resume actors, run their loops, print a report",
	}
	cmd.AddCommand(runCommand(), cleanupCommand(), certsCommand())
	return cmd
}

// clusterFlags select the cluster and how to reach ateapi.
type clusterFlags struct {
	kubeconfig   string
	kubeContext  string
	apiEndpoint  string
	apiTokenFile string
	atespace     string
}

func (f *clusterFlags) register(fs *pflag.FlagSet) {
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

// bindRunFlags binds the run command's flags to cfg and opts.
func bindRunFlags(fs *pflag.FlagSet, cfg *runConfig, opts *runOptions) {
	opts.cluster.register(fs)
	fs.IntVar(&cfg.Actors, "actors", 1000, "A: actors to create. Reruns reuse the actors that exist.")
	fs.IntVar(&cfg.Parallel, "parallel", 1, "B: actors to resume and run the loop in, at the same time.")
	fs.IntVar(&cfg.Endpoints, "endpoints", 10, fmt.Sprintf("C: endpoints each loop holds a tunnel to, egress-target-0 up to egress-target-<C-1> (at most %d).", maxEndpoints))
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "How long the loops run once all of them have started.")
	fs.StringVar(&cfg.ConnMode, "conn-mode", connModeKeepAlive, "How the loops connect: keepalive (one connection per endpoint) or new-conn (one per request).")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 5*time.Second, "Timeout of one request.")
	fs.DurationVar(&cfg.RequestInterval, "request-interval", 100*time.Millisecond, "Each actor's pacing: one request per interval, so each endpoint is requested once per --endpoints intervals; 0 sends back to back at full speed.")
	fs.IntVar(&cfg.CreateConcurrency, "create-concurrency", 32, "Actors created at the same time.")
	fs.DurationVar(&cfg.ResumeTimeout, "resume-timeout", 5*time.Minute, "Per actor: how long to keep resuming (a full pool is retried) and waiting for it to answer.")
	fs.DurationVar(&cfg.ProgressInterval, "progress-interval", 30*time.Second, "How often to print progress; 0 turns it off.")
	fs.IntVar(&cfg.StartBatch, "start-batch", 0, "Start the loops in batches of this many actors, --start-gap apart, so one batch's opens (--start-batch / --request-interval per second) fit the gateway's handshake budget; 0 starts them all at once.")
	fs.DurationVar(&cfg.StartGap, "start-gap", 0, "Pause between batches of loop starts. With --start-batch and no --start-gap, one opening window: --endpoints x --request-interval.")
	fs.IntVar(&cfg.OpensBudget, "opens-budget", 0, "Opens per second the gateway can handshake. A run is refused when the actors started together, --start-batch or all of --parallel, ask for more per --request-interval, batches whose openings overlap counted together; 0 skips the check.")
	fs.DurationVar(&cfg.StreamIdleTimeout, "stream-idle-timeout", 5*time.Minute, "The gateway's CONNECT stream idle timeout. A keep-alive run is refused when the per-endpoint period, --endpoints x --request-interval, is more than half of it: a tunnel goes quiet for one period, or for one slow request bounded by --request-timeout, and the other half is that margin; 0 skips the check.")
	fs.DurationVar(&cfg.Settle, "settle", 0, "If set, start the measured --duration only once every loop has opened all its endpoints with a success each and requested each of them twice, and no tunnel has been redialed for this long. Needs --progress-interval and must cover one period (--endpoints x --request-interval).")
	fs.DurationVar(&cfg.SettleTimeout, "settle-timeout", 30*time.Minute, "How long to wait for --settle before measuring anyway.")
	fs.StringVar(&cfg.Template, "template", "egress", "Actor template the actors are created from; WORKLOAD_TEMPLATES=egress benchmarking/workloads/deploy.sh creates it.")
	fs.StringVar(&cfg.Scheme, "scheme", schemeHTTP, "Scheme the loops request endpoints over: http, or https through the gateway's TLS interception (needs deploy.sh --https).")
	fs.BoolVar(&cfg.Usage, "usage", false, "Sample what the egress path spends during the run: the gateway's and ateapi's process counters and Envoy's stats every second, cAdvisor every 5s. The report gains cpu, memory and gateway lines and a usage section.")
	fs.DurationVar(&cfg.PreIdle, "pre-idle", 0, "With --usage, sample this long before create, with none of the run's actors running; the report shows it as the baseline.")
	fs.StringVar(&opts.routerURL, "router-url", "", "atenet router base URL. Empty port-forwards to the atenet-router Service.")
	fs.StringVar(&opts.output, "output", "", "If set, write the report as JSON to this file.")
}

// completeRunConfig finishes cfg once fs is parsed: the atespace comes from the
// cluster flags, and --start-gap defaults to one period when batches are used.
func completeRunConfig(fs *pflag.FlagSet, cfg *runConfig, opts *runOptions) error {
	cfg.Atespace = opts.cluster.atespace
	if cfg.StartBatch > 0 && !fs.Changed("start-gap") {
		cfg.StartGap = cfg.period()
	}
	return cfg.validate()
}

// parseRunFlags parses the run command's flags into a validated config.
func parseRunFlags(args []string) (runConfig, runOptions, error) {
	var cfg runConfig
	var opts runOptions
	fs := pflag.NewFlagSet("run", pflag.ContinueOnError)
	bindRunFlags(fs, &cfg, &opts)
	if err := fs.Parse(args); err != nil {
		return cfg, opts, err
	}
	return cfg, opts, completeRunConfig(fs, &cfg, &opts)
}

func runCommand() *cobra.Command {
	var cfg runConfig
	var opts runOptions
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Create the actors, run the egress loops, and print a report",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := completeRunConfig(cmd.Flags(), &cfg, &opts); err != nil {
				return err
			}
			return runCmd(cmd.Context(), cfg, opts)
		},
	}
	bindRunFlags(cmd.Flags(), &cfg, &opts)
	return cmd
}

func runCmd(ctx context.Context, cfg runConfig, opts runOptions) error {
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
	if cfg.Usage {
		get, err := newSamplerGetter(cf.kubeconfig, cf.kubeContext)
		if err != nil {
			return err
		}
		r.usage = newUsageSampler(get, conn.k8s, cfg.Atespace)
	}
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

func cleanupCommand() *cobra.Command {
	var cf clusterFlags
	var actors, concurrency int
	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Delete the actors a run created",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cleanupCmd(cmd.Context(), cf, actors, concurrency)
		},
	}
	cf.register(cmd.Flags())
	cmd.Flags().IntVar(&actors, "actors", 1000, "Delete actors egress-0 up to egress-<actors-1>. Use the largest --actors any run used.")
	cmd.Flags().IntVar(&concurrency, "concurrency", 64, "Actors deleted at the same time.")
	return cmd
}

func cleanupCmd(ctx context.Context, cf clusterFlags, actors, concurrency int) error {
	if actors < 1 || concurrency < 1 {
		return errors.New("--actors and --concurrency must be at least 1")
	}

	conn, err := cf.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.api.Close()

	res := cleanup(ctx, conn.api, defaultBackoff, cf.atespace, actors, concurrency, os.Stdout)
	printPhase(os.Stdout, "cleanup", res.phaseResult, fmt.Sprintf(" (%d did not exist)", res.NotFound))
	if res.Failed > 0 {
		return fmt.Errorf("%d actors were not deleted", res.Failed)
	}
	return ctx.Err()
}

func certsCommand() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:   "certs",
		Short: "Write a CA and the target's HTTPS certificate (deploy.sh uses this)",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if out == "" {
				return errors.New("--out is required")
			}
			bundle, err := generateTargetCert(time.Now())
			if err != nil {
				return err
			}
			return bundle.write(out)
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "Directory to write ca.crt, tls.crt and tls.key to. Required.")
	return cmd
}
