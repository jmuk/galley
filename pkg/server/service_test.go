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
	"reflect"
	"testing"

	"github.com/ghodss/yaml"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/store"
)

type galleyTestManager struct {
	*testManager
	client galleypb.GalleyClient
}

func (tm *galleyTestManager) registerGrpcServer(s store.Store, server *grpc.Server) error {
	svc, err := NewGalleyService(s)
	if err != nil {
		return err
	}

	galleypb.RegisterGalleyServer(server, svc)
	return nil
}

func (tm *galleyTestManager) createGrpcClient(conn *grpc.ClientConn) error {
	tm.client = galleypb.NewGalleyClient(conn)
	return nil
}

func newGalleyTestManager() *galleyTestManager {
	mgr := &galleyTestManager{}
	mgr.testManager = &testManager{grpcTestManager: mgr}
	return mgr
}

func TestCRUD(t *testing.T) {
	tm := newGalleyTestManager()
	err := tm.setup()
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}
	defer tm.close()

	p1 := "/dept1/svc1/service.cfg"
	p2 := "dept2/svc1/service.cfg"
	ctx := context.Background()
	file, err := tm.client.GetFile(ctx, &galleypb.GetFileRequest{Path: p1})
	if err == nil {
		t.Errorf("Got %+v unexpectedly", file)
	}

	resp, err := tm.client.ListFiles(ctx, &galleypb.ListFilesRequest{Path: "/dept1", IncludeContents: true})
	if err != nil {
		t.Errorf("Failed to list files: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("Unexpected response: %+v", resp)
	}

	_, err = tm.client.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     p1,
		Contents: testConfig,
	})
	if err != nil {
		t.Errorf("Falied to create the file %s: %+v", p1, err)
	}

	var header metadata.MD
	file, err = tm.client.GetFile(ctx, &galleypb.GetFileRequest{Path: p1}, grpc.Header(&header))
	if err != nil {
		t.Errorf("Failed to get the file: %v", err)
	}
	if file.Path != p1[1:] || file.Contents != testConfig {
		t.Errorf("Got %v, Want %v", file, &galleypb.File{Path: p1, Contents: testConfig})
	}
	path, ok := header["file-path"]
	if !ok {
		t.Errorf("file-path not found in header")
	}
	if !reflect.DeepEqual(path, []string{p1[1:]}) {
		t.Errorf("Got %+v, Want %+v", path, []string{p1[1:]})
	}
	rev, ok := header["file-revision"]
	if !ok {
		t.Errorf("file-revision not found in header")
	}
	if len(rev) != 1 {
		t.Errorf("Unexpected revision data: %+v", rev)
	}

	_, err = tm.client.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     p2,
		Contents: testConfig,
	})
	if err != nil {
		t.Errorf("Failed to create the file %s: %v", p2, err)
	}

	jsonData, err := yaml.YAMLToJSON([]byte(testConfig))
	if err != nil {
		t.Fatalf("Failed to convert the config data: %v", err)
	}
	_, err = tm.client.UpdateFile(ctx, &galleypb.UpdateFileRequest{
		Path:        p2,
		Contents:    string(jsonData),
		ContentType: galleypb.ContentType_JSON,
	})
	if err != nil {
		t.Errorf("Failed to update the file %s: %v", p2, err)
	}

	file, err = tm.client.GetFile(ctx, &galleypb.GetFileRequest{Path: p2})
	if err != nil {
		t.Errorf("Failed to get the file %s: %v", p2, err)
	}
	if file.Contents != string(jsonData) {
		t.Errorf("Got %s, Want %s", file.Contents, string(jsonData))
	}

	resp, err = tm.client.ListFiles(ctx, &galleypb.ListFilesRequest{Path: "/dept1", IncludeContents: true})
	if err != nil {
		t.Errorf("Failed to list files: %v", err)
	}
	if len(resp.Entries) != 1 || resp.Entries[0].Path != p1[1:] || resp.Entries[0].Contents != testConfig {
		t.Errorf("Unexpected list result: %+v", resp)
	}

	_, err = tm.client.DeleteFile(ctx, &galleypb.DeleteFileRequest{Path: p1})
	if err != nil {
		t.Errorf("Failed to delete the file %s: %v", p1, err)
	}
	file, err = tm.client.GetFile(ctx, &galleypb.GetFileRequest{Path: p1})
	if err == nil {
		t.Errorf("Unexpectedly get %s: %+v", p1, file)
	}
	_, err = tm.client.DeleteFile(ctx, &galleypb.DeleteFileRequest{Path: p2})
	if err != nil {
		t.Errorf("Failed to delete the file %s, %v", p2, err)
	}
	file, err = tm.client.GetFile(ctx, &galleypb.GetFileRequest{Path: p2})
	if err == nil {
		t.Errorf("Unexpectedly get %s: %+v", p2, file)
	}
}

func TestDeleteTwice(t *testing.T) {
	tm := &testManager{}
	err := tm.setup()
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}
	defer tm.close()

	ctx := context.Background()
	p1 := "foo/service.cfg"

	_, err = tm.client.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     p1,
		Contents: testConfig,
	})
	if err != nil {
		t.Errorf("Falied to create the file %s: %+v", p1, err)
	}

	_, err = tm.client.DeleteFile(ctx, &galleypb.DeleteFileRequest{Path: p1})
	if err != nil {
		t.Errorf("Failed to delete the file: %v", err)
	}

	_, err = tm.client.DeleteFile(ctx, &galleypb.DeleteFileRequest{Path: p1})
	if err == nil {
		t.Errorf("Unexpectedly succeeded to delete twice")
	}
	stat, ok := status.FromError(err)
	if !ok {
		t.Errorf("Returned error is not a gRPC error")
	}
	if stat.Code() != codes.NotFound {
		t.Errorf("Got %s, Want NotFound", stat.Code())
	}
}
