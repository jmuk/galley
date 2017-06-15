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

// Package server provides HTTP open service galley API server bindings.
package server

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/golang/protobuf/jsonpb"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	configpb "istio.io/api/config/v1"
	"istio.io/galley/pkg/store"
)

type configAPIServer struct {
	// TODO: allow multiple kvs.
	kvs store.KeyValueStore
}

var _ configpb.ServiceServer = &configAPIServer{}

// NewServiceServer creates a new configpb.ServiceServer instance with the
// specified storage.
func NewServiceServer(kvs store.KeyValueStore) (*configAPIServer, error) {
	return &configAPIServer{kvs}, nil
}

func (s *configAPIServer) GetObject(ctx context.Context, req *configpb.GetObjectRequest) (resp *configpb.Object, err error) {
	value, index, found := s.kvs.Get(buildPath(req.Meta))
	if !found {
		return nil, fmt.Errorf("object not found")
	}
	resp, err = buildObject(value, req.Meta, req.Incl)
	if err != nil {
		return nil, err
	}
	resp.Meta.Revision = int64(index)
	return resp, nil
}

func (s *configAPIServer) ListObjects(ctx context.Context, req *configpb.ListObjectsRequest) (resp *configpb.ObjectList, err error) {
	objs, revision, err := readKvsToObjects(s.kvs, buildPath(req.Meta), req.Incl)
	if err != nil {
		return nil, err
	}
	req.Meta.Revision = revision
	return &configpb.ObjectList{
		Meta:    req.Meta,
		Objects: objs,
	}, nil
}

func (s *configAPIServer) ListObjectTypes(ctx context.Context, req *configpb.ListObjectTypesRequest) (resp *configpb.ObjectTypeList, err error) {
	var prefix string
	if req.Meta == nil || (req.Meta.ApiGroup == "" && req.Meta.ApiGroupVersion == "") {
		prefix = "/"
	} else {
		prefix = fmt.Sprintf("/%s/%s/", req.Meta.ApiGroup, req.Meta.ApiGroupVersion)
	}
	keys, _, err := s.kvs.List(prefix, true)
	resp = &configpb.ObjectTypeList{
		Meta:        req.Meta,
		ObjectTypes: make([]*configpb.Meta, 0, len(keys)),
	}
	known := map[string]bool{}
	for _, k := range keys {
		m, err := pathToMeta(k)
		if err != nil {
			glog.Infof("can't parse key %s: %v", k, err)
			continue
		}
		m.Name = ""
		mKey := buildPath(m)
		if _, ok := known[mKey]; ok {
			continue
		}
		known[mKey] = true
		resp.ObjectTypes = append(resp.ObjectTypes, m)
	}
	return resp, nil
}

func (s *configAPIServer) CreateObject(ctx context.Context, req *configpb.CreateObjectRequest) (resp *configpb.Object, err error) {
	value, err := (&jsonpb.Marshaler{}).MarshalToString(req.SourceData)
	if err != nil {
		return nil, err
	}
	index, err := s.kvs.Set(buildPath(req.Meta), string(value))
	if err != nil {
		return nil, err
	}
	resp = &configpb.Object{Meta: req.Meta, SourceData: req.SourceData}
	resp.Meta.Revision = int64(index)
	return resp, nil
}

func (s *configAPIServer) UpdateObject(ctx context.Context, req *configpb.UpdateObjectRequest) (resp *configpb.Object, err error) {
	return s.CreateObject(ctx, &configpb.CreateObjectRequest{Meta: req.Meta, SourceData: req.SourceData})
}

func (s *configAPIServer) DeleteObject(ctx context.Context, req *configpb.DeleteObjectRequest) (resp *emptypb.Empty, err error) {
	err = s.kvs.Delete(buildPath(req.Meta))
	return &emptypb.Empty{}, err
}
