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

	galleypb "istio.io/api/galley/v1"
	"istio.io/galley/pkg/store"
)

// GalleyService is the implementation of galleypb.Galley service.
type GalleyService struct {
	// TODO: allow multiple kvs.
	kvs store.KeyValueStore
}

// NewGalleyService creates a new galleypb.GalleyService instance with the
// specified storage.
func NewGalleyService(kvs store.KeyValueStore) (*GalleyService, error) {
	return &GalleyService{kvs}, nil
}

// GetObject implements a galleypb.Galley method.
func (s *GalleyService) GetObject(ctx context.Context, req *galleypb.GetObjectRequest) (resp *galleypb.Object, err error) {
	value, index, found := s.kvs.Get(buildPath(req.Meta))
	if !found {
		return nil, fmt.Errorf("object not found")
	}
	resp, err = buildObject(value, req.Meta)
	if err != nil {
		return nil, err
	}
	resp.Meta.Revision = int64(index)
	return resp, nil
}

// ListObjects implements a galleypb.Galley method.
func (s *GalleyService) ListObjects(ctx context.Context, req *galleypb.ListObjectsRequest) (resp *galleypb.ObjectList, err error) {
	objs, revision, err := readKvsToObjects(s.kvs, buildPath(req.Meta))
	if err != nil {
		return nil, err
	}
	req.Meta.Revision = revision
	return &galleypb.ObjectList{
		Meta:    req.Meta,
		Objects: objs,
	}, nil
}

// ListObjectTypes implements a galleypb.Galley method.
func (s *GalleyService) ListObjectTypes(ctx context.Context, req *galleypb.ListObjectTypesRequest) (resp *galleypb.ObjectTypeList, err error) {
	var prefix string
	if req.Meta == nil || req.Meta.ApiGroup == "" {
		prefix = "/"
	} else {
		prefix = fmt.Sprintf("/%s/", req.Meta.ApiGroup)
	}
	keys, _, err := s.kvs.List(prefix, true)
	resp = &galleypb.ObjectTypeList{
		Meta:        req.Meta,
		ObjectTypes: make([]*galleypb.Meta, 0, len(keys)),
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

// CreateObject implements a galleypb.Galley method.
func (s *GalleyService) CreateObject(ctx context.Context, req *galleypb.ObjectRequest) (resp *galleypb.Object, err error) {
	value, err := (&jsonpb.Marshaler{}).MarshalToString(req.SourceData)
	if err != nil {
		return nil, err
	}
	index, err := s.kvs.Set(buildPath(req.Meta), string(value))
	if err != nil {
		return nil, err
	}
	resp = &galleypb.Object{Meta: req.Meta, SourceData: req.SourceData}
	resp.Meta.Revision = int64(index)
	return resp, nil
}

// UpdateObject implements a galleypb.Galley method.
func (s *GalleyService) UpdateObject(ctx context.Context, req *galleypb.ObjectRequest) (resp *galleypb.Object, err error) {
	return s.CreateObject(ctx, req)
}

// DeleteObject implements a galleypb.Galley method.
func (s *GalleyService) DeleteObject(ctx context.Context, req *galleypb.DeleteObjectRequest) (resp *emptypb.Empty, err error) {
	err = s.kvs.Delete(buildPath(req.Meta))
	return &emptypb.Empty{}, err
}
