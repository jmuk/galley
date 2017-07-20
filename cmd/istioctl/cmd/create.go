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

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/cmd/shared"
	"istio.io/galley/pkg/client/file"
)

func createCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	var filenames []string

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create an Istio configuration object by filename.",
		Long: `
Create an Istio configuration object by filename.

JSON and YAML formats are accepted.
`,
		Run: func(c *cobra.Command, args []string) {
			if err := validateFilenames(filenames); err != nil {
				fatalf(err.Error())
			}
			filename := filenames[0]

			file, err := file.PartialDecodeFromFilename(filename, galleypb.ContentType_UNKNOWN)
			if err != nil {
				fatalf("cannot parse content from %q: %v", filename, err)
			}
			if _, err := global.client.CreateFile(file); err != nil {
				fatalf("cannot create file: %v", err)
			}
		},
	}

	cmd.Flags().StringArrayVarP(&filenames, "filename", "f", nil,
		"Filename to use to create the resource")

	return cmd
}
