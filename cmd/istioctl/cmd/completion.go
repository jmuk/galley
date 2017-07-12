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
	"os"

	"github.com/spf13/cobra"

	"istio.io/galley/cmd/shared"
)

func completionCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	return &cobra.Command{
		Use:   "completion",
		Short: "Generate bash completion for Istio",
		Long: `
Output shell completion code for the bash shell. The shell output must
be evaluated to provide interactive completion of istioctl
commands.`,
		Example: `
# Add the following to .bash_profile.
source <(istioctl completion)

# Create a separate completion file and source that from .bash_profile
istioctl completion > ~/.istioctl-complete.bash
echo "source ~/.istioctl-complete.bash" >> ~/.bash_profile
`,
		Run: func(_ *cobra.Command, _ []string) {
			rootCmd := GetRootCmd(os.Args[1:])
			if err := rootCmd.GenBashCompletion(os.Stdout); err != nil {
				fatalf("could not generate bash completion: %v", err)
			}
		},
	}
}
