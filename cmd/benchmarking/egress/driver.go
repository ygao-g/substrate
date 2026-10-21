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
	"errors"
	"time"

	"github.com/spf13/cobra"
)

// driverCommand drives the egress scale test from outside the cluster: certs
// writes the target's certificate for deploy.sh. See
// benchmarking/egress-tunnels/README.md.
func driverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "driver",
		Short: "Drive the egress scale test: create and resume actors, run their loops, print a report",
	}
	cmd.AddCommand(certsCommand())
	return cmd
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
