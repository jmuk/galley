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
	"path/filepath"

	"github.com/spf13/cobra"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/cmd/shared"
	"istio.io/galley/pkg/client"
	"istio.io/galley/pkg/client/file"
)

func getCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	var (
		filenames       []string
		recurse         bool
		includeContents bool
	)

	cmd := &cobra.Command{
		Use:   "get",
		Short: "Display one or many Istio configuration objects by path.",
		Run: func(_ *cobra.Command, args []string) {
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
			case 2:
				fatalf("too many arguments: expected explicit path or -f flag")
			}

			var files []*file.File
			list := filepath.Ext(path) != ".cfg"
			if list {
				var err error
				if files, err = global.client.ListFiles(path, recurse, includeContents); err != nil {
					fatalf("cannot list files: %v", err)
				}
			} else {
				file, err := global.client.GetFile(path)
				if err != nil {
					fatalf("cannot get file: %v", err)
				}
				files = append(files, file)
			}
			for _, file := range files {
				printf("%v %v\n", file.Path, file.Revision)
				if !list || includeContents {
					printf("%v\n", string(file.Contents))
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&filenames, "filename", "f", nil,
		"Filename to use to get the resource")
	cmd.Flags().BoolVarP(&recurse, "recursive", "r", false,
		"List the hierarchy recursively")
	cmd.Flags().BoolVarP(&recurse, "include-contents", "i", false,
		"Include the contents along with the paths")

	return cmd
}
