// Copyright 2017 Istio Authors
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
	"strings"
	"time"

	"github.com/spf13/cobra"

	"istio.io/galley/cmd/shared"
	"istio.io/galley/pkg/server"
)

type serverArgs struct {
	storeURL              string
	grpcPort              uint16
	restPort              uint16
	watchProgressInterval time.Duration
	validators            []string
}

func serverCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	sa := &serverArgs{}
	serverCmd := cobra.Command{
		Use:   "server",
		Short: "Starts Galley as a server",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			runServer(sa, printf, fatalf)
		},
	}
	serverCmd.PersistentFlags().Uint16Var(&sa.grpcPort, "port", 9096, "Galley API port for gRPC")
	serverCmd.PersistentFlags().Uint16Var(&sa.restPort, "rest-port", 9097, "the port JSON/REST gateway to the Galley API")
	serverCmd.PersistentFlags().StringVar(&sa.storeURL, "store-url", "", "the URL for the backend storage")
	serverCmd.PersistentFlags().DurationVar(
		&sa.watchProgressInterval, "watch-progress-interval", 1*time.Second, "the interval which watcher server emits watch progress event")
	serverCmd.PersistentFlags().StringSliceVar(
		&sa.validators, "validators", nil, "the list of prefix=endpoint for validators.")
	return &serverCmd
}

func runServer(sa *serverArgs, printf, fatalf shared.FormatFn) {
	osb, err := server.CreateServer(sa.storeURL)
	if err != nil {
		fatalf("Failed to create server: %s", err.Error())
	}
	for _, vcfg := range sa.validators {
		eq := strings.Index(vcfg, "=")
		if eq < 0 {
			fatalf("equal sign is not found in the validator config %s", vcfg)
		}
		if err = osb.RegisterValidator(vcfg[:eq], vcfg[eq+1:]); err != nil {
			fatalf("failed to register the validator: %v", err)
		}
	}
	printf("Server started, listening on port %d", sa.grpcPort)
	printf("JSON/REST on port %d", sa.restPort)
	printf("CTL-C to break out of galley")
	if err = osb.Start(sa.grpcPort, sa.restPort, sa.watchProgressInterval); err != nil {
		fatalf("failed to start the server: %v", err)
	}
}
