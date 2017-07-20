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

	"github.com/spf13/cobra"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/cmd/shared"
	"istio.io/galley/pkg/client"
	"istio.io/galley/pkg/client/file"
)

func deleteCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	var (
		filenames []string
		revision  int64
	)

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete Istio configuration objects by path.",
		Long: `
Delete Istio configuration objects by path.

JSON and YAML formats are accepted. Only one type of the arguments may
be specified: filenames or resources and names.
`,
		Run: func(c *cobra.Command, args []string) {
			var path string

			switch len(args) {
			case 0:
				if err := validateFilenames(filenames); err != nil {
					fatalf(err.Error())
				}
				filename := filenames[0]
				file, err := file.PartialDecodeFromFilename(filename, galleypb.ContentType_UNKNOWN)
				if err != nil {
					fatalf("cannot parse content from %q: %v", filename, err)
				}
				path = client.NameScopeToPath(file.Name, file.Scope)
			case 1:
				path = args[0]
				if !strings.HasSuffix(path, "*.cfg") {
					fatalf("cannot delete a directory")
				}
			case 2:
				fatalf("too many arguments: expected explicit path or -f flag")
			}

			if err := global.client.DeleteFile(path); err != nil {
				fatalf("cannot delete %q: %v", path, err)
			}
		},
	}

	cmd.Flags().StringArrayVarP(&filenames, "filename", "f", nil,
		"Filename to use to delete the resource")

	cmd.Flags().Int64Var(&revision, "revision", 0,
		"Revision to avoid blind writes (default '0' forces delete)")

	return cmd
}
