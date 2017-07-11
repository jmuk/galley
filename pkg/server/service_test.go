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
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/ghodss/yaml"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	galleypb "istio.io/galley/api/galley/v1"
	"istio.io/galley/pkg/store/memstore"
)

type testManager struct {
	client galleypb.GalleyClient
	s      *memstore.Store
	server *grpc.Server
}

func (tm *testManager) createGrpcServer() (string, error) {
	tm.s = memstore.New()
	svc, err := NewGalleyService(tm.s)
	if err != nil {
		return "", err
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", 0))
	if err != nil {
		return "", fmt.Errorf("unable to listen on socket: %v", err)
	}

	tm.server = grpc.NewServer()
	galleypb.RegisterGalleyServer(tm.server, svc)

	go func() {
		_ = tm.server.Serve(listener)
	}()
	return listener.Addr().String(), nil
}

func (tm *testManager) createGrpcClient(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	tm.client = galleypb.NewGalleyClient(conn)
	return nil
}

func (tm *testManager) setup() error {
	addr, err := tm.createGrpcServer()
	if err != nil {
		return err
	}
	return tm.createGrpcClient(addr)
}

func (tm *testManager) close() {
	if tm.server != nil {
		tm.server.GracefulStop()
	}
}

const testConfig = `
scope: shipping.FQDN
name: service.cfg
config:
  - type: constructor
    name: request_count
    spec:
      labels:
        dc: target.data_center
        service: target.service
      value: request.size
  - type: handler
    name: mystatsd
    spec:
      impl: istio.io/statsd
      params:
        host: statshost.FQDN
        port: 9080
  - type: rule
    spec:
      handler: $mystatsd
      instances:
      - $request_count
      selector: target.service == "shipping.FQDN"
  - type: constructor
    name: deny_source_ip
    spec:
      value: request.source_ip
  - type: rule
    spec:
      handler: $mesh.denyhandler
      instances:
      - $deny_source_ip
      selector: target.service == "shipping.FQDN" && source.labels["app"] != "billing"
## Proxy rules
  - type: route-rule
    spec:
      destination: billing.FQDN
      source: shipping.FQDN
      match:
        httpHeaders:
          cookie:
            regex: "^(.*?;)?(user=test-user)(;.*)?$"
      route:
      - tags:
          version: v1
        weight: 100
      httpFault:
        delay:
          percent: 5
          fixedDelay: 2s
  - type: route-rule
    spec:
      destination: shipping.FQDN
      match:
        httpHeaders:
          cookie:
            regex: "^(.*?;)?(user=test-user)(;.*)?$"
      route:
      - tags:
          version: v1
        weight: 90
      - tags:
          version: v2
        weight: 10
  - type: route-rule
    spec:
      destination: shipping.FQDN
      route:
      - tags:
          version: v1
        weight: 100
`

func TestCRUD(t *testing.T) {
	tm := &testManager{}
	err := tm.setup()
	defer tm.close()
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	p1 := "/dept1/svc1/service.cfg"
	p2 := "/dept2/svc1/service.cfg"
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
	if file.Path != p1 || file.Contents != testConfig {
		t.Errorf("Got %v, Want %v", file, &galleypb.File{Path: p1, Contents: testConfig})
	}
	path, ok := header["file-path"]
	if !ok {
		t.Errorf("file-path not found in header")
	}
	if !reflect.DeepEqual(path, []string{p1}) {
		t.Errorf("Got %+v, Want %+v", path, []string{p1})
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
	if len(resp.Entries) != 1 || resp.Entries[0].Path != p1 || resp.Entries[0].Contents != testConfig {
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
