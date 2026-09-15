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
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"
)

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
// of JSON, so one decoder parses both. The manifest must hold exactly one
// document, counted the way the YAML spec counts them: an empty document, such
// as the one a trailing "---" opens, is a document too, and is rejected.
func manifestToJSON(data []byte) ([]byte, error) {
	dec := yamlv3.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&yamlv3.Node{}); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("manifest is empty")
		}
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	// Decode again to check that the manifest holds no second document.
	if err := dec.Decode(&yamlv3.Node{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("manifest holds more than one document, expected one")
	}
	j, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(j) == "null" {
		return nil, errors.New("manifest is empty")
	}
	return j, nil
}

// overrideEgressPolicyMetadata defaults each of metadata.atespace and
// metadata.name the manifest leaves empty, then checks that both match the
// command line: the atespace flag and the fixed policy name.
func overrideEgressPolicyMetadata(policy *ateapipb.EgressPolicy, atespace string) error {
	const policyNameDefault = "default"
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	if policy.Metadata.Atespace == "" {
		policy.Metadata.Atespace = atespace
	}
	if policy.Metadata.Name == "" {
		policy.Metadata.Name = policyNameDefault
	}
	if policy.Metadata.Atespace != atespace {
		return fmt.Errorf("manifest metadata.atespace %q does not match --atespace %q", policy.Metadata.Atespace, atespace)
	}
	if policy.Metadata.Name != policyNameDefault {
		return fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, policyNameDefault)
	}
	return nil
}
