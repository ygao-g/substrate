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

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

var (
	getEgressPolicyAtespaceFlag    string
	createEgressPolicyAtespaceFlag string
	createEgressPolicyFilenameFlag string
)

var getEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name ...>",
	Aliases: []string{"egress-policies"},
	Short:   "Get the egress policy of one or more actors",
	Long: "Get the egress policy of one or more actors. With several actors the table prints one row " +
		"each, and -o yaml/-o json print an egressPolicies list tagging each policy with its actor.",
	Args: cobra.MinimumNArgs(1),
	RunE: runGetEgressPolicy,
}

var createEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name> -f <manifest>",
	Aliases: []string{"egress-policies"},
	Short:   "Create an actor's egress policy from a manifest",
	Long: `Create the egress policy of an actor from a manifest file.

The manifest is a YAML or JSON EgressPolicy, as printed by
"kubectl ate get egress-policy <actor-name> -a <atespace> -o yaml".
Its metadata may be omitted.`,
	Args: cobra.ExactArgs(1),
	RunE: runCreateEgressPolicy,
}

// egressPolicyFromManifest parses a single protojson-shaped YAML or JSON
// document into an EgressPolicy. Parsing is strict: unknown fields are an
// error.
func egressPolicyFromManifest(data []byte) (*ateapipb.EgressPolicy, error) {
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, err
	}
	policy := &ateapipb.EgressPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, fmt.Errorf("invalid EgressPolicy: %w", err)
	}
	return policy, nil
}

// manifestToJSON converts a YAML or JSON manifest to JSON. YAML is a superset
// of JSON, so one reader parses both. The manifest must hold exactly one
// non-empty document; the empty documents a leading or trailing "---" leaves
// behind are skipped.
func manifestToJSON(data []byte) ([]byte, error) {
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var jsonData []byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		j, err := yaml.YAMLToJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		if string(j) == "null" {
			continue
		}
		if jsonData != nil {
			return nil, fmt.Errorf("manifest holds more than one document, expected one")
		}
		jsonData = j
	}
	if jsonData == nil {
		return nil, fmt.Errorf("manifest is empty")
	}
	return jsonData, nil
}

// policyName is the fixed name of an actor's egress policy.
const policyName = "default"

// overrideEgressPolicyMetadata sets metadata.atespace to atespace and
// metadata.name to policyName when the manifest omits them. A manifest that
// names another atespace or name is rejected, not retargeted.
func overrideEgressPolicyMetadata(policy *ateapipb.EgressPolicy, atespace string) error {
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	switch policy.Metadata.Atespace {
	case "":
		policy.Metadata.Atespace = atespace
	case atespace:
	default:
		return fmt.Errorf("manifest metadata.atespace %q does not match --atespace %q", policy.Metadata.Atespace, atespace)
	}
	switch policy.Metadata.Name {
	case "":
		policy.Metadata.Name = policyName
	case policyName:
	default:
		return fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, policyName)
	}
	return nil
}

// egressPolicyGetter abstracts GetActorEgressPolicy RPC calls.
type egressPolicyGetter interface {
	GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// getEgressPolicyRunner executes the get egress-policy command logic.
type getEgressPolicyRunner struct {
	getter    egressPolicyGetter
	actors    []*ateapipb.ObjectRef
	outputFmt string
	stdout    io.Writer
	stderr    io.Writer
}

func (r *getEgressPolicyRunner) Run(ctx context.Context) error {
	found := make([]printer.ActorEgressPolicy, 0, len(r.actors))
	var missing []string
	for _, actor := range r.actors {
		policy, err := r.getter.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
		if status.Code(err) == codes.NotFound {
			if actorIsMissing(err) {
				fmt.Fprintf(r.stderr, "actor %q in atespace %q not found\n", actor.GetName(), actor.GetAtespace())
				missing = append(missing, actor.GetAtespace()+"/"+actor.GetName())
				continue
			}
			// No policy is a valid state, not a failure: the gateway denies all egress.
			fmt.Fprintf(r.stderr, "actor %q in atespace %q has no egress policy; all egress is denied\n", actor.GetName(), actor.GetAtespace())
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to get egress policy for actor %q in atespace %q: %w", actor.GetName(), actor.GetAtespace(), err)
		}
		found = append(found, printer.ActorEgressPolicy{Actor: actor, Policy: policy})
	}
	// The output shape follows the command line, not what was found: one name
	// prints the bare document, several always print the list.
	if err := r.print(found); err != nil {
		return err
	}
	// The actors that do exist still print; the exit status reports the rest.
	if len(missing) > 0 {
		return fmt.Errorf("no such actor: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (r *getEgressPolicyRunner) print(found []printer.ActorEgressPolicy) error {
	if len(r.actors) == 1 {
		if len(found) == 0 {
			return nil
		}
		return printer.PrintEgressPolicyTo(r.stdout, found[0].Actor.GetName(), found[0].Policy, r.outputFmt)
	}
	return printer.PrintEgressPoliciesTo(r.stdout, found, r.outputFmt)
}

// actorIsMissing reports whether a NotFound blames the Actor rather than its
// policy. An ateapi too old to send the detail reports neither.
func actorIsMissing(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ResourceInfo); ok && info.GetResourceType() == "Actor" {
			return true
		}
	}
	return false
}

func runGetEgressPolicy(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	actors := make([]*ateapipb.ObjectRef, 0, len(args))
	for _, name := range args {
		actors = append(actors, &ateapipb.ObjectRef{Atespace: getEgressPolicyAtespaceFlag, Name: name})
	}
	runner := &getEgressPolicyRunner{
		getter:    apiClient,
		actors:    actors,
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
		stderr:    cmd.ErrOrStderr(),
	}
	return runner.Run(ctx)
}

// egressPolicyCreator abstracts CreateActorEgressPolicy RPC calls.
type egressPolicyCreator interface {
	CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// createEgressPolicyRunner executes the create egress-policy command logic.
type createEgressPolicyRunner struct {
	creator   egressPolicyCreator
	actor     *ateapipb.ObjectRef
	policy    *ateapipb.EgressPolicy
	outputFmt string
	stdout    io.Writer
}

func (r *createEgressPolicyRunner) Run(ctx context.Context) error {
	created, err := r.creator.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: r.actor, EgressPolicy: r.policy})
	if err != nil {
		return fmt.Errorf("failed to create egress policy for actor %q in atespace %q: %w", r.actor.GetName(), r.actor.GetAtespace(), err)
	}
	return printer.PrintEgressPolicyTo(r.stdout, r.actor.GetName(), created, r.outputFmt)
}

func runCreateEgressPolicy(cmd *cobra.Command, args []string) error {
	data, err := readFileOrStdin(cmd.InOrStdin(), createEgressPolicyFilenameFlag)
	if err != nil {
		return err
	}
	policy, err := egressPolicyFromManifest(data)
	if err != nil {
		return fmt.Errorf("failed to parse egress policy manifest %q: %w", createEgressPolicyFilenameFlag, err)
	}
	if err := overrideEgressPolicyMetadata(policy, createEgressPolicyAtespaceFlag); err != nil {
		return err
	}

	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &createEgressPolicyRunner{
		creator:   apiClient,
		actor:     &ateapipb.ObjectRef{Atespace: createEgressPolicyAtespaceFlag, Name: args[0]},
		policy:    policy,
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

func init() {
	getEgressPolicyCmd.Flags().StringVarP(&getEgressPolicyAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	_ = getEgressPolicyCmd.MarkFlagRequired("atespace")
	getCmd.AddCommand(getEgressPolicyCmd)

	createEgressPolicyCmd.Flags().StringVarP(&createEgressPolicyAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	createEgressPolicyCmd.Flags().StringVarP(&createEgressPolicyFilenameFlag, "filename", "f", "", "Manifest file holding one EgressPolicy; use - for stdin (required)")
	_ = createEgressPolicyCmd.MarkFlagRequired("atespace")
	_ = createEgressPolicyCmd.MarkFlagRequired("filename")
	createCmd.AddCommand(createEgressPolicyCmd)
}
