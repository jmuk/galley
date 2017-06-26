// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Revision 2.0 (the "License");
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

package server

import (
	"strings"

	"github.com/golang/protobuf/proto"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/store"
)

func getFile(s store.Store, path string) (*galleypb.File, error) {
	value, revision, err := s.Get(path + ":raw")
	if err != nil {
		return nil, err
	}
	file := &galleypb.File{}
	if err = proto.Unmarshal(value, file); err != nil {
		return nil, err
	}
	file.Revision = revision
	return file, nil
}

func readFiles(s store.Store, prefix string) ([]*galleypb.File, int64, error) {
	data, revision, err := s.List(prefix)
	if err != nil {
		return nil, 0, err
	}

	files := make([]*galleypb.File, 0, len(data))
	for path, value := range data {
		if !strings.HasSuffix(path, ":raw") {
			continue
		}
		file := &galleypb.File{}
		if err = proto.Unmarshal(value, file); err != nil {
			return nil, 0, err
		}
		files = append(files, file)
	}
	return files, revision, nil
}
