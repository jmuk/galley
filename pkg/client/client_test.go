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

package client

import (
	"fmt"
	"net"
	"reflect"
	"sort"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/client/config"
	"istio.io/galley/pkg/client/file"
	"istio.io/galley/pkg/server"
	"istio.io/galley/pkg/store"
	"istio.io/galley/pkg/store/memstore"
)

type testServer struct {
	store    store.Store
	server   *grpc.Server
	listener net.Listener
}

func newTestServer() (*testServer, error) {
	grpcOptions := []grpc.ServerOption{
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()),
	}
	s := &testServer{
		store:  memstore.New(),
		server: grpc.NewServer(grpcOptions...),
	}
	svc, err := server.NewGalleyService(s.store)
	if err != nil {
		return nil, err
	}
	galleypb.RegisterGalleyServer(s.server, svc)
	if s.listener, err = net.Listen("tcp", ":0"); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *testServer) start() {
	s.server.Serve(s.listener) // nolint: errcheck
}

func (s *testServer) stop() {
	s.server.Stop()
	s.store.Close() // nolint: errcheck
}

type clientTest struct {
	server *testServer
	client *Client
}

func (ct clientTest) Current() *config.Context {
	return &config.Context{
		Server: ct.server.listener.Addr().String(),
	}
}

// no-ops - not used yet
func (clientTest) UseContext(string) error { panic("not implemented") }

func newClientTest() (*clientTest, error) {
	var ct clientTest
	var err error
	if ct.server, err = newTestServer(); err != nil {
		return nil, fmt.Errorf("could not start test server: %v", err)
	}
	if ct.client, err = NewFromConfig(ct); err != nil {
		return nil, fmt.Errorf("could not create client: %v", err)
	}
	go ct.server.start()
	return &ct, nil
}

func (ct *clientTest) stop() {
	ct.server.stop()
}

var (
	testFile0 = []byte(`
{
  "scope": "a/b/c0",
  "name": "service.cfg",
  "config": [
    {
      "type": "route-rule",
      "spec": {
        "destination": "shipping.FQDN",
        "route": [
          {
            "tags": {
              "version": "v1"
            },
            "weight": 100
          }
        ]
      }
    }
  ]
}
`)

	testFile1 = []byte(`
{
  "scope": "a/b/c1",
  "name": "service.cfg",
  "config": [
    {
      "type": "route-rule",
      "spec": {
        "destination": "shipping.FQDN",
        "route": [
          {
            "tags": {
              "version": "v1"
            },
            "weight": 100
          }
        ]
      }
    }
  ]
}
`)
)

func TestClient(t *testing.T) {
	ct, err := newClientTest()
	if err != nil {
		t.Fatal(err)
	}
	defer ct.stop()

	jsonTestFile0, err := file.PartialDecode(testFile0, galleypb.ContentType_JSON)
	if err != nil {
		t.Fatalf("could not create test json file #0: %v", err)
	}
	wantFile0 := &file.File{
		Path:        NameScopeToPath(jsonTestFile0.Name, jsonTestFile0.Scope),
		Name:        jsonTestFile0.Name,
		Scope:       jsonTestFile0.Scope,
		ContentType: galleypb.ContentType_UNKNOWN,
		Contents:    jsonTestFile0.Contents,
	}

	jsonTestFile1, err := file.PartialDecode(testFile1, galleypb.ContentType_JSON)
	if err != nil {
		t.Fatalf("could not create test json file #1: %v", err)
	}
	wantFile1 := &file.File{
		Path:        NameScopeToPath(jsonTestFile1.Name, jsonTestFile1.Scope),
		Name:        jsonTestFile1.Name,
		Scope:       jsonTestFile1.Scope,
		ContentType: galleypb.ContentType_UNKNOWN,
		Contents:    jsonTestFile1.Contents,
	}

	checkStatus := func(gotErr error, wantCode codes.Code) error {
		if got, ok := status.FromError(gotErr); !ok {
			return fmt.Errorf("non-status error: type=%v", reflect.TypeOf(gotErr))
		} else if got.Code() != wantCode {
			return fmt.Errorf("wrong status: got %v want %v", got.Code(), wantCode)
		}
		return nil
	}

	// verify GET fails on non-existent path
	nonExistentPath := "/non/existent/path"
	_, err = ct.client.GetFile(nonExistentPath)
	if err = checkStatus(err, codes.NotFound); err != nil {
		t.Fatalf("GetFile(%v) %v", nonExistentPath, err)
	}

	// verify CREATE

	gotFile, err := ct.client.CreateFile(jsonTestFile0)
	if err = checkStatus(err, codes.OK); err != nil {
		t.Fatalf("CreateFile(%v): %v", wantFile0.Path, err)
	}
	wantFile0.Revision = 1
	if !reflect.DeepEqual(gotFile, wantFile0) {
		t.Fatalf("CreateFile(%v): \ngot %v \nwant %v", wantFile0.Path, gotFile, wantFile0)
	}
	// verify duplicate CREATE fails
	_, err = ct.client.CreateFile(jsonTestFile0)
	if err = checkStatus(err, codes.InvalidArgument); err != nil {
		t.Fatalf("CreateFile(%v): %v", wantFile0.Path, err)
	}

	// verify UPDATE with same content succeeds
	gotFile, err = ct.client.UpdateFile(jsonTestFile0)
	if err = checkStatus(err, codes.OK); err != nil {
		t.Fatalf("UpdateFile(%v): %v", wantFile0.Path, err)
	}
	wantFile0.Revision = 2
	if !reflect.DeepEqual(gotFile, wantFile0) {
		t.Fatalf("UpdateFile(%v): \ngot %v \nwant %v", wantFile0.Path, gotFile, wantFile0)
	}

	// verify GET succeeds on valid path
	gotFile, err = ct.client.GetFile(wantFile0.Path)
	if err = checkStatus(err, codes.OK); err != nil {
		t.Fatalf("GetFile(%v): %v", wantFile0.Path, err)
	}
	if !reflect.DeepEqual(gotFile, wantFile0) {
		t.Fatalf("CreateFile(%v): \ngot %v \nwant %v", wantFile0.Path, gotFile, wantFile0)
	}

	// verify LIST with multiple files
	_, err = ct.client.CreateFile(jsonTestFile1)
	if err = checkStatus(err, codes.OK); err != nil {
		t.Fatalf("CreateFile(%v): %v", NameScopeToPath(jsonTestFile1.Name, jsonTestFile1.Scope), err)
	}
	wantFile0.Revision = 3
	wantFile1.Revision = 3
	wantFiles := []*file.File{
		wantFile0,
		wantFile1,
	}
	listPath := "/a/b"
	gotFiles, err := ct.client.ListFiles(listPath, true, true)
	if err = checkStatus(err, codes.OK); err != nil {
		t.Fatalf("ListFiles(%v): %v", listPath, err)
	}
	// normalize ordering of list of files
	for _, files := range [][]*file.File{gotFiles, wantFiles} {
		sort.Slice(files, func(i, j int) bool {
			return NameScopeToPath(files[i].Name, files[i].Scope) <
				NameScopeToPath(files[j].Name, files[j].Scope)
		})
	}
	if !reflect.DeepEqual(gotFiles, wantFiles) {
		t.Fatalf("ListFiles(%v): \ngot %v \nwant %v", "a/b/", gotFiles, wantFiles)
	}

	/*
		TODO galley server returns OK when deleting non-existent
		path. Re-enable this step once server is fixed.

		// verify DELETE on non-existent path
		err = ct.client.DeleteFile(nonExistentPath)
		if err := checkStatus(err, codes.NotFound); err != nil {
			t.Fatalf("DeleteFile(%v) %v", nonExistentPath, err)
		}
	*/

	// verify DELETE on valid path
	err = ct.client.DeleteFile(wantFile1.Path)
	if err = checkStatus(err, codes.OK); err != nil {
		t.Fatalf("DeleteFile(%v) %v", wantFile1.Path, err)
	}
}

func TestNameScopeToPath(t *testing.T) {
	cases := []struct {
		name  string
		scope string
		want  string
	}{
		{
			name:  "service.cfg",
			scope: "",
			want:  "service.cfg",
		},
		{
			name:  "service.cfg",
			scope: "a",
			want:  "a/service.cfg",
		},
		{
			name:  "service.cfg",
			scope: "a.b.c",
			want:  "c/b/a/service.cfg",
		},
		{
			name:  "",
			scope: "a.b.c",
			want:  "c/b/a",
		},
	}

	for _, c := range cases {
		if got := NameScopeToPath(c.name, c.scope); got != c.want {
			t.Errorf("NameScopeToPath(%q, %q) failed: got %q want %q", c.name, c.scope, got, c.want)
		}
	}
}
