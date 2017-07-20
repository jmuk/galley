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
	"flag"

	"github.com/spf13/cobra"

	"istio.io/galley/cmd/shared"
	"istio.io/galley/pkg/client"
	"istio.io/galley/pkg/client/config"
)

type commonOptions struct {
	// Option to override the default server address
	server string
}

var options commonOptions

// shared state between all istio subcommands
var global = struct {
	config *config.Config
	client *client.Client
}{}

// GetRootCmd generates the root command for istioctl
func GetRootCmd(args []string) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "istioctl",
		Short: "Istio configuration command line tool",
		Long: `
Istio configuration command line tool.

Create, list, modify, and delete configuration resources in the Istio
system.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if global.config, err = config.LoadFromDefault(); err != nil {
				// TODO manage all user configuration under `config`
				// subcommand instead of implicitly creating default
				// config here?
				if global.config, err = config.New(); err != nil {
					return err
				}
			}
			if options.server != "" {
				shared.Printf("Overriding default Galley server address with %q", options.server)
				global.config.Current().Server = options.server
			}

			switch cmd.Name() {
			case "create", "get", "replace", "delete":
				if global.client, err = client.NewFromConfig(global.config); err != nil {
					return err
				}
			}
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			// Persistent configuration changes
			if global.config == nil {
				return nil
			}
			return global.config.Save()
		},
	}
	rootCmd.SetArgs(args)
	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	// hack to make flag.Parsed return true such that glog is happy
	// about the flags having been parsed
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	/* #nosec */
	_ = fs.Parse([]string{})
	flag.CommandLine = fs

	rootCmd.AddCommand(createCmd(shared.Printf, shared.Fatalf))
	rootCmd.AddCommand(getCmd(shared.Printf, shared.Fatalf))
	rootCmd.AddCommand(replaceCmd(shared.Printf, shared.Fatalf))
	rootCmd.AddCommand(deleteCmd(shared.Printf, shared.Fatalf))
	rootCmd.AddCommand(completionCmd(shared.Printf, shared.Fatalf))
	rootCmd.AddCommand(shared.VersionCmd())

	rootCmd.Flags().StringVar(&options.server, "server", "",
		"Override the current galley server address")

	return rootCmd
}
