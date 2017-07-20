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

// Package client manages client galley file CRUD operations. This is
// currently mostly a wrapper around the gRPC client stub, but allows
// for alternative transport schemes in the future, e.g. "net/http",
// "k8s.io/client-go/rest".
package client

import (
	"context"
	"strings"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/client/config"
	"istio.io/galley/pkg/client/file"
)

// https://github.com/golang/go/wiki/SliceTricks
func reverseStringSlice(a []string) {
	for i := len(a)/2 - 1; i >= 0; i-- {
		opp := len(a) - 1 - i
		a[i], a[opp] = a[opp], a[i]
	}
}

// NameScopeToPath returns the computed file path for Galley from a
// configuration's name and scope.
func NameScopeToPath(name, scope string) string {
	if scope == "" {
		return name
	}

	// convert from reverse dns to path-based directory
	segments := strings.Split(scope, ".")
	reverseStringSlice(segments)

	if name != "" {
		segments = append(segments, name)
	}
	return strings.Join(segments, "/")
}

// Client provides a client interface to the Galley configuration
// service.
type Client struct {
	galley  galleypb.GalleyClient
	conn    *grpc.ClientConn
	cfg     Config
	timeout time.Duration
}

// Config represents the interface expected by the Client to extract
// local user configuration.
type Config interface {
	Current() *config.Context
	UseContext(current string) error
}

// NewFromConfig creates a new instance of the client using the
// specified configuration.
func NewFromConfig(cfg Config) (*Client, error) {
	// TODO - auth, etc.?
	options := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
	}
	conn, err := grpc.Dial(cfg.Current().Server, options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		galley:  galleypb.NewGalleyClient(conn),
		cfg:     cfg,
		conn:    conn,
		timeout: time.Second,
	}, nil
}

// Close closes the client connection with the Galley service.
func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		glog.Warningf("Error closing client connection: %v", err)
	}
}

func (c *Client) reqCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.timeout)
}

// CreateFile creates a configuration file with the Galley service.
func (c *Client) CreateFile(in *file.File) (*file.File, error) {
	request := &galleypb.CreateFileRequest{
		Path:        NameScopeToPath(in.Name, in.Scope),
		Contents:    string(in.Contents),
		ContentType: in.ContentType,
	}
	ctx, cancel := c.reqCtx()
	defer cancel()
	response, err := c.galley.CreateFile(ctx, request)
	if err != nil {
		return nil, err
	}
	return &file.File{
		Path:     response.Path,
		Name:     in.Name,
		Scope:    in.Scope,
		Contents: []byte(response.Contents),
		Revision: response.Revision,
	}, nil
}

// UpdateFile updates a configuration file with the Galley service.
func (c *Client) UpdateFile(in *file.File) (*file.File, error) {
	request := &galleypb.UpdateFileRequest{
		Path:        NameScopeToPath(in.Name, in.Scope),
		Contents:    string(in.Contents),
		ContentType: in.ContentType,
	}
	ctx, cancel := c.reqCtx()
	defer cancel()
	response, err := c.galley.UpdateFile(ctx, request)
	if err != nil {
		return nil, err
	}
	return &file.File{
		Path:     response.Path,
		Name:     in.Name,
		Scope:    in.Scope,
		Contents: []byte(response.Contents),
		Revision: response.Revision,
	}, nil
}

// GetFile gets a configuration file from the Galley service.
func (c *Client) GetFile(path string) (*file.File, error) {
	request := &galleypb.GetFileRequest{
		Path: path,
	}
	ctx, cancel := c.reqCtx()
	defer cancel()
	response, err := c.galley.GetFile(ctx, request)
	if err != nil {
		return nil, err
	}

	parsed, err := file.PartialDecode([]byte(response.Contents), galleypb.ContentType_UNKNOWN)
	if err != nil {
		return nil, err
	}

	return &file.File{
		Path:     response.Path,
		Name:     parsed.Name,
		Scope:    parsed.Scope,
		Contents: []byte(response.Contents),
		Revision: response.Revision,
	}, nil
}

const maxPageSize = 0

// ListFiles lists configuration files existing in the Galley service based on a path.
func (c *Client) ListFiles(path string, recurse, includeContents bool) ([]*file.File, error) {
	var files []*file.File
	var token string

	for {
		request := &galleypb.ListFilesRequest{
			Path:            path,
			Recurse:         recurse,
			IncludeContents: includeContents,
			PageToken:       token,
			MaxPageSize:     maxPageSize,
		}
		ctx, cancel := c.reqCtx()
		defer cancel()
		response, err := c.galley.ListFiles(ctx, request)
		if err != nil {
			return nil, err
		}
		for _, entry := range response.Entries {
			parsed, err := file.PartialDecode([]byte(entry.Contents), galleypb.ContentType_UNKNOWN)
			if err != nil {
				return nil, err
			}
			files = append(files, &file.File{
				Path:     entry.Path,
				Name:     parsed.Name,
				Scope:    parsed.Scope,
				Contents: []byte(entry.Contents),
				Revision: entry.Revision,
			})
		}
		if response.NextPageToken == "" {
			break
		}
		token = response.NextPageToken
	}

	return files, nil
}

// DeleteFile deletes a configuration file from the Galley service.
func (c *Client) DeleteFile(path string) error {
	request := &galleypb.DeleteFileRequest{
		Path: path,
	}
	ctx, cancel := c.reqCtx()
	defer cancel()
	_, err := c.galley.DeleteFile(ctx, request)
	return err
}
