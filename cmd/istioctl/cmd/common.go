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

import "fmt"

func validateFilenames(filenames []string) error {
	// TODO - refactor to accept input from stdin, http, multiple files?
	if len(filenames) == 0 {
		return fmt.Errorf("no filenames specified")
	}
	if len(filenames) > 1 {
		return fmt.Errorf("multiple input filenames not supported") // TODO?
	}
	return nil
}
