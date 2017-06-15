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
	"time"

	"github.com/spf13/cobra"

	"istio.io/galley/cmd/shared"
	"istio.io/galley/pkg/server"
)

type serverArgs struct {
	storeURL    string
	port        uint16
	gatewayPort uint16
	interval    uint
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
	serverCmd.PersistentFlags().Uint16Var(&sa.port, "port", 9096, "Galley API port")
	serverCmd.PersistentFlags().Uint16Var(&sa.gatewayPort, "gateway-port", 9097, "the port for grpc-gateway to the API")
	serverCmd.PersistentFlags().StringVar(&sa.storeURL, "store-url", "", "the URL for the backend storage")
	serverCmd.PersistentFlags().UintVar(&sa.interval, "interval", 5000, "the interval to push updates in msecs")
	return &serverCmd
}

func runServer(sa *serverArgs, printf, fatalf shared.FormatFn) {
	osb, err := server.CreateServer(sa.storeURL)
	if err != nil {
		fatalf("Failed to create server: %s", err.Error())
	}
	printf("Server started, listening on port %d", sa.port)
	printf("CTL-C to break out of galley")
	if err = osb.Start(sa.port, sa.gatewayPort, time.Duration(sa.interval)*time.Millisecond); err != nil {
		fatalf("failed to start the server: %v", err)
	}
}
