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

package server

import (
	"context"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/store"
)

func sendFileHeader(ctx context.Context, file *galleypb.File) error {
	return grpc.SendHeader(ctx, metadata.Pairs(
		"file-path", file.Path,
		"file-revision", strconv.FormatInt(file.Revision, 10),
	))
}

func getFile(ctx context.Context, s store.Store, path string) (*galleypb.File, error) {
	value, revision, err := s.Get(ctx, path+":raw")
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

func readFiles(ctx context.Context, s store.Store, prefix string) ([]*galleypb.File, int64, error) {
	data, revision, err := s.List(ctx, prefix)
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
