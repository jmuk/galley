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
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/store"
)

// GalleyService is the implementation of galleypb.Galley service.
type GalleyService struct {
	s store.Store
	// TODO: contains validator info.
}

// NewGalleyService creates a new galleypb.GalleyService instance with the
// specified storage.
func NewGalleyService(s store.Store) (*GalleyService, error) {
	return &GalleyService{s}, nil
}

// GetFile implements galleypb.Galley interface.
func (s *GalleyService) GetFile(ctx context.Context, req *galleypb.GetFileRequest) (*galleypb.File, error) {
	f, err := getFile(ctx, s.s, req.Path)
	if err == store.ErrNotFound {
		return nil, status.New(codes.NotFound, err.Error()).Err()
	}
	if err = sendFileHeader(ctx, f); err != nil {
		return nil, err
	}
	return f, nil
}

// ListFiles implements galleypb.Galley interface.
func (s *GalleyService) ListFiles(ctx context.Context, req *galleypb.ListFilesRequest) (*galleypb.ListFilesResponse, error) {
	// TODO: support page tokens.
	entries, _, err := readFiles(ctx, s.s, req.Path)
	if err != nil {
		return nil, err
	}
	return &galleypb.ListFilesResponse{Entries: entries}, nil
}

func (s *GalleyService) createOrUpdate(ctx context.Context, file *galleypb.File, ctype galleypb.ContentType) (*galleypb.File, error) {
	bytes, err := proto.Marshal(file)
	if err != nil {
		return nil, status.New(codes.InvalidArgument, err.Error()).Err()
	}

	cfile, err := newConfigFile(file.Contents, ctype)
	if err != nil && glog.V(3) {
		glog.Infof("the file contents can't be parsed as ConfigFile message: %v", err)
	}
	if cfile != nil {
		// TODO: use File's metadata? see https://github.com/istio/galley/issues/55
		cfile.ServerMetadata = &galleypb.ServerMetadata{Path: file.Path}
		// TODO: validate the config file, invoke validation servers.
	}
	file.Revision, err = s.s.Set(ctx, rawPath(file.Path), bytes, -1 /* revision */)
	if err != nil {
		return nil, err
	}
	if cfile != nil {
		cfile.ServerMetadata.Revision = file.Revision + 1
		var cbytes []byte
		cbytes, err = proto.Marshal(cfile)
		if err != nil {
			return nil, status.New(codes.InvalidArgument, err.Error()).Err()
		}
		if _, err = s.s.Set(ctx, encodedPath(file.Path), cbytes, -1 /* revision */); err != nil {
			return nil, err
		}
	}
	if err = sendFileHeader(ctx, file); err != nil {
		return nil, err
	}
	return file, nil
}

// CreateFile implements galleypb.Galley interface.
func (s *GalleyService) CreateFile(ctx context.Context, req *galleypb.CreateFileRequest) (*galleypb.File, error) {
	if _, err := getFile(ctx, s.s, req.Path); err == nil {
		return nil, status.Newf(codes.InvalidArgument, "path %s already existed", req.Path).Err()
	}
	return s.createOrUpdate(ctx, &galleypb.File{Path: req.Path, Contents: req.Contents, Metadata: req.Metadata}, req.ContentType)
}

// UpdateFile implements galleypb.Galley interface.
func (s *GalleyService) UpdateFile(ctx context.Context, req *galleypb.UpdateFileRequest) (*galleypb.File, error) {
	if _, err := getFile(ctx, s.s, req.Path); err != nil {
		return nil, status.Newf(codes.NotFound, "can't update %s, not found", req.Path).Err()
	}
	return s.createOrUpdate(ctx, &galleypb.File{Path: req.Path, Contents: req.Contents, Metadata: req.Metadata}, req.ContentType)
}

// DeleteFile implements galleypb.Galley interface.
func (s *GalleyService) DeleteFile(ctx context.Context, req *galleypb.DeleteFileRequest) (*empty.Empty, error) {
	// TODO: validation.
	_, err := s.s.Delete(ctx, rawPath(req.Path))
	if err != nil {
		return nil, err
	}
	_, err = s.s.Delete(ctx, encodedPath(req.Path))
	if err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}
