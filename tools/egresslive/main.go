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

// egresslive is a throwaway seeder for testing kubectl-ate's egress policy
// verbs against a live cluster. It gets, creates, or deletes one actor's
// EgressPolicy through the ate API and prints what the server returned, so the
// CLI's output can be checked against it. create writes a single hostnames
// rule.
//
// Usage:
//
//	ATE_CONTEXT=kind-kind go run ./tools/egresslive <atespace> <actor> get|create|delete
//
// KUBECONFIG and ATE_CONTEXT select the cluster; the client port-forwards to
// the API server and mints its own token, like kubectl-ate does.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: egresslive <atespace> <actor> get|create|delete")
		os.Exit(2)
	}
	atespace, name, verb := os.Args[1], os.Args[2], os.Args[3]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := ateclient.NewClient(ctx, os.Getenv("KUBECONFIG"), os.Getenv("ATE_CONTEXT"), "", "", false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}
	defer c.Close()
	actor := &ateapipb.ObjectRef{Atespace: atespace, Name: name}

	var p *ateapipb.EgressPolicy
	switch verb {
	case "get":
		p, err = c.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
	case "create":
		p, err = c.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "default"},
			Rules:    []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}},
		}})
	case "delete":
		p, err = c.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor})
	default:
		fmt.Fprintln(os.Stderr, "unknown verb", verb)
		os.Exit(2)
	}
	if err != nil {
		st, _ := status.FromError(err)
		fmt.Printf("%-8s -> %s: %s\n", verb, st.Code(), st.Message())
		os.Exit(1)
	}
	rules := protojson.MarshalOptions{}.Format(&ateapipb.EgressPolicy{Rules: p.GetRules()})
	fmt.Printf("%-8s -> ok uid=%s version=%d rules=%s\n", verb, p.GetMetadata().GetUid(), p.GetMetadata().GetVersion(), rules)
}
