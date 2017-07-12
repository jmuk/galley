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
	"github.com/spf13/cobra"

	"istio.io/galley/cmd/shared"
)

func deleteCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	return &cobra.Command{
		Use:   "delete",
		Short: "Delete Istio configuration objects by path.",
		Long: `
Delete Istio configuration objects by path.

JSON and YAML formats are accepted. Only one type of the arguments may
be specified: filenames or resources and names.
`,
		Run: func(_ *cobra.Command, _ []string) {
			fatalf("delete not implemented yet")
		},
	}
}
