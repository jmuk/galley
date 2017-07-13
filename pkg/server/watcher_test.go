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
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	galleypb "istio.io/galley/api/galley/v1"
	internalpb "istio.io/galley/pkg/server/internal"
	"istio.io/galley/pkg/store"
)

const watchIntervalForTest time.Duration = time.Millisecond
const waitDurationForTest = watchIntervalForTest * 3

const PUT = galleypb.ConfigFileChange_PUT
const DELETE = galleypb.ConfigFileChange_DELETE

type testWatcherManager struct {
	*testManager
	galley *galleyTestManager
	client galleypb.WatcherClient
}

func (tm *testWatcherManager) registerGrpcServer(s store.Store, server *grpc.Server) error {
	svc, err := NewWatcherServer(s, watchIntervalForTest)
	if err != nil {
		return err
	}
	galleypb.RegisterWatcherServer(server, svc)
	if tm.galley != nil {
		if err = tm.galley.registerGrpcServer(s, server); err != nil {
			return err
		}
	}
	return nil
}

func (tm *testWatcherManager) createGrpcClient(conn *grpc.ClientConn) error {
	tm.client = galleypb.NewWatcherClient(conn)
	if tm.galley != nil {
		return tm.galley.createGrpcClient(conn)
	}
	return nil
}

func (tm *testWatcherManager) setData(ctx context.Context, path, source string, withConfigFile bool) error {
	ifile := &internalpb.File{
		RawFile: &galleypb.File{Path: path, Contents: source},
	}
	if withConfigFile {
		var err error
		ifile.Encoded, err = newConfigFile(source, galleypb.ContentType_YAML)
		if err != nil {
			return err
		}
	}
	bytes, err := proto.Marshal(ifile)
	if err != nil {
		return err
	}
	_, err = tm.s.Set(ctx, path, bytes, -1)
	return err
}

func newWatcherManager(withGalley bool) *testWatcherManager {
	mgr := &testWatcherManager{}
	mgr.testManager = &testManager{grpcTestManager: mgr}
	if withGalley {
		mgr.galley = &galleyTestManager{testManager: mgr.testManager}
	}
	return mgr
}

type watchConsumer struct {
	s     galleypb.Watcher_WatchClient
	types map[string]bool

	done chan interface{}

	mu          sync.Mutex
	valid       bool
	initialized bool
	revision    int64
	initialData map[string]*galleypb.ConfigFile
	events      []*galleypb.ConfigFileChange
	recvCount   int
}

func (tm *testWatcherManager) newWatchConsumer(ctx context.Context, scope string, types []string) (*watchConsumer, error) {
	s, err := tm.client.Watch(ctx, &galleypb.WatchRequest{
		Scope:               scope,
		Types:               types,
		IncludeInitialState: true,
		StartRevision:       0,
	})
	if err != nil {
		return nil, err
	}
	ts := map[string]bool{}
	for _, t := range types {
		ts[t] = true
	}
	c := &watchConsumer{
		s:           s,
		types:       ts,
		done:        make(chan interface{}),
		revision:    -1,
		initialData: map[string]*galleypb.ConfigFile{},
	}
	go c.start()
	return c, nil
}

func (c *watchConsumer) Close(t *testing.T) {
	if err := c.s.CloseSend(); err != nil {
		t.Errorf("error on CloseSend: %v", err)
	}
	close(c.done)
}

func (c *watchConsumer) RecvCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recvCount
}

func (c *watchConsumer) Revision() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.revision
}

func (c *watchConsumer) NumEvents() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

func (c *watchConsumer) start() {
	for {
		resp, err := c.s.Recv()
		if err == io.EOF {
			close(c.done)
			return
		} else if err != nil {
			c.valid = false
			return
		}
		c.mu.Lock()
		c.recvCount++
		if created := resp.GetCreated(); created != nil {
			if c.valid {
				fmt.Printf("created event comes twice")
			}
			c.valid = !c.valid
			c.revision = created.CurrentRevision
		} else if initialState := resp.GetInitialState(); initialState != nil {
			if c.initialized {
				fmt.Printf("already initialized")
			} else {
				for _, ent := range initialState.Entries {
					c.initialData[ent.FileId] = ent.File
				}
				if initialState.Done {
					c.initialized = true
				}
			}
		} else if progress := resp.GetProgress(); progress != nil {
			c.revision = progress.CurrentRevision
		} else if events := resp.GetEvents(); events != nil {
			c.events = append(c.events, events.Events...)
		} else {
			fmt.Printf("unrecognized event %+v", resp)
			c.valid = false
		}
		c.mu.Unlock()
	}
}

func (c *watchConsumer) validateFile(t *testing.T, actual, expected *galleypb.ConfigFile) {
	if expected == nil {
		if actual != nil {
			t.Errorf("Got %+v, Want nil", actual)
		}
		return
	}
	if actual.Scope != expected.Scope || actual.Name != expected.Name {
		t.Errorf("Got %s/%s, Want %s/%s", actual.Scope, actual.Name, expected.Scope, expected.Name)
	}
	if !reflect.DeepEqual(actual.Metadata, expected.Metadata) {
		t.Errorf("Got %+v, Want %+v", actual.Metadata, expected.Metadata)
	}
	if expected.ServerMetadata != nil {
		if actual.ServerMetadata.Path != expected.ServerMetadata.Path {
			t.Errorf("Got %s, Want %s", actual.ServerMetadata.Path, expected.ServerMetadata.Path)
		}
	}

	w := &watcher{types: c.types}
	filtered := w.filterConfigFile(expected)
	if !reflect.DeepEqual(actual.Config, filtered.Config) {
		t.Errorf("Got %+v\nWant %+v", actual.Config, filtered.Config)
	}
}

func (c *watchConsumer) checkInitialData(t *testing.T, expected map[string]*galleypb.ConfigFile) {
	uncheckedFiles := map[string]bool{}
	for k := range c.initialData {
		uncheckedFiles[k] = true
	}
	for k, e := range expected {
		if a, ok := c.initialData[k]; ok {
			c.validateFile(t, a, e)
			delete(uncheckedFiles, k)
		} else {
			t.Errorf("initial data is missing %s", k)
		}
	}
	if len(uncheckedFiles) != 0 {
		names := make([]string, 0, len(uncheckedFiles))
		for k := range uncheckedFiles {
			names = append(names, k)
		}
		t.Errorf("unexpected files in the initialData: %+v", names)
	}
}

func (c *watchConsumer) checkEvents(t *testing.T, msg string, expected ...*galleypb.ConfigFileChange) {
	if len(c.events) < len(expected) {
		t.Errorf("%s: not receiving events sufficiently: got %d events, want %d events",
			msg, len(c.events), len(expected))
		return
	}
	toCheck := c.events[len(c.events)-len(expected):]
	for _, ev := range expected {
		found := -1
		for i, eav := range toCheck {
			if eav.Type == ev.Type && eav.FileId == ev.FileId {
				found = i
				if ev.Type == PUT {
					c.validateFile(t, eav.NewFile, ev.NewFile)
				}
			}
		}
		if found >= 0 {
			toCheck = append(toCheck[:found], toCheck[(found+1):]...)
		} else {
			t.Errorf("%s: event %+v not found", msg, ev)
		}
	}
}

func newTestChange(typ galleypb.ConfigFileChange_EventType, name string, file *galleypb.ConfigFile) *galleypb.ConfigFileChange {
	return &galleypb.ConfigFileChange{
		Type:    typ,
		FileId:  name,
		NewFile: file,
	}
}

func TestWatching(t *testing.T) {
	tm := newWatcherManager(false)
	err := tm.setup()
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}
	defer tm.close()

	k1 := "dept1/svc1/service.cfg"
	k2 := "dept1/svc2/service.cfg"
	k3 := "dept2/svc1/service.cfg"

	// setup the initial data
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configFile, err := newConfigFile(testConfig, galleypb.ContentType_YAML)
	if err != nil {
		t.Fatalf("failed to create the config file: %v", err)
	}

	if err = tm.setData(ctx, k1, testConfig, true); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	w1, err := tm.newWatchConsumer(ctx, "dept1", []string{"constructor", "handler"})
	if err != nil {
		t.Errorf("failed to watch: %v", err)
	}
	defer w1.Close(t)
	w2, err := tm.newWatchConsumer(ctx, "", []string{"unknown"})
	if err != nil {
		t.Errorf("failed to watch: %v", err)
	}
	defer w2.Close(t)
	time.Sleep(waitDurationForTest)
	if !w1.valid || !w1.initialized {
		t.Errorf("invalid status w1")
	}
	w1.checkInitialData(t, map[string]*galleypb.ConfigFile{
		k1: configFile,
	})
	if !w2.valid || !w2.initialized {
		t.Errorf("invalid status w2")
	}
	w2.checkInitialData(t, map[string]*galleypb.ConfigFile{})

	if err = tm.setData(ctx, k2, testConfig, true); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	if err = tm.setData(ctx, k3, testConfig, true); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	time.Sleep(waitDurationForTest)

	w1.checkEvents(t, "w1/put", newTestChange(PUT, k2, configFile))
	w2.checkEvents(t, "w2/put", newTestChange(PUT, k2, nil), newTestChange(PUT, k3, nil))

	if err = tm.setData(ctx, k2, testConfig, false); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	time.Sleep(waitDurationForTest)

	w1.checkEvents(t, "w1/put", newTestChange(PUT, k2, nil))

	r1 := w1.Revision()
	e1 := w1.NumEvents()

	if _, err = tm.s.Delete(ctx, k3); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	time.Sleep(waitDurationForTest)

	if rr1 := w1.Revision(); r1 >= rr1 {
		t.Errorf("revision is not updated properly, got %d, want >%d", rr1, r1)
	}
	if e1 != w1.NumEvents() {
		t.Errorf("new events arrived to w1 unexpectedly")
	}

	if _, err = tm.s.Delete(ctx, k2); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	time.Sleep(waitDurationForTest)
	w1.checkEvents(t, "w1/delete", newTestChange(DELETE, k2, nil))
	w2.checkEvents(t, "w2/delete", newTestChange(DELETE, k3, nil), newTestChange(DELETE, k2, nil))
}

func TestWithGalleyService(t *testing.T) {
	tm := newWatcherManager(true)
	err := tm.setup()
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}
	defer tm.close()

	configFile, err := newConfigFile(testConfig, galleypb.ContentType_YAML)
	if err != nil {
		t.Fatalf("failed to get the config file: %v", err)
	}

	k1 := "dept1/svc1/service.cfg"
	k2 := "dept2/svc1/service.cfg"

	configFile.ServerMetadata = &galleypb.ServerMetadata{Path: k1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := tm.newWatchConsumer(ctx, "dept1", []string{"rule"})
	if err != nil {
		t.Fatalf("failed to start watch: %v", err)
	}
	time.Sleep(waitDurationForTest)
	if !w.valid || !w.initialized {
		t.Fatalf("failed to initialize the watcher: %+v", w)
	}

	gc := tm.galley.client
	_, err = gc.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     k1,
		Contents: testConfig,
	})
	if err != nil {
		t.Fatalf("failed to create the file %s: %v", k1, err)
	}
	time.Sleep(waitDurationForTest)
	w.checkEvents(t, "create", newTestChange(PUT, k1, configFile))

	r1 := w.Revision()
	e1 := w.NumEvents()

	_, err = gc.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     k2,
		Contents: testConfig,
	})
	if err != nil {
		t.Errorf("failed to create the file %s: %v", k2, err)
	}
	time.Sleep(waitDurationForTest)
	if r2 := w.Revision(); r2 <= r1 {
		t.Errorf("Revision is not updated, got %d, want >%d", r2, r1)
	}
	if e2 := w.NumEvents(); e2 != e1 {
		t.Errorf("new events arrived unexpectedly")
	}

	if _, err = gc.DeleteFile(ctx, &galleypb.DeleteFileRequest{Path: k1}); err != nil {
		t.Errorf("failed to delete %s: %v", k1, err)
	}
	time.Sleep(waitDurationForTest)
	w.checkEvents(t, "delete", newTestChange(DELETE, k1, nil))
}

func TestFilterConfigFile(t *testing.T) {
	configFile, err := newConfigFile(testConfig, galleypb.ContentType_YAML)
	if err != nil {
		t.Fatalf("failed to get the config file: %v", err)
	}
	configFile.Metadata = &galleypb.Metadata{
		Labels: map[string]string{"foo": "bar"},
	}
	configFile.ServerMetadata = &galleypb.ServerMetadata{
		Path:     "/path/to/config.cfg",
		Revision: 42,
	}
	for _, cc := range []struct {
		configFile *galleypb.ConfigFile
		types      []string
		idx        []int
	}{
		{configFile, []string{"constructor", "handler"}, []int{0, 1, 3}},
		{configFile, []string{"rule", "route-rule"}, []int{2, 4, 5, 6, 7}},
		{configFile, []string{"foo"}, nil},
		{configFile, []string{}, nil},
		{nil, []string{"constructor", "handler"}, nil},
	} {
		t.Run(fmt.Sprintf("%+v", cc.types), func(tt *testing.T) {
			w := &watcher{types: map[string]bool{}}
			for _, t := range cc.types {
				w.types[t] = true
			}
			filtered := w.filterConfigFile(cc.configFile)
			if cc.idx == nil {
				if filtered != nil {
					tt.Fatalf("Got %+v, Want nil", filtered)
				}
				return
			}
			if filtered == nil {
				tt.Fatalf("Got nil unexpectedly")
			}
			objs := make([]*galleypb.ConfigObject, len(cc.idx))
			for i, idx := range cc.idx {
				objs[i] = configFile.Config[idx]
			}
			if !reflect.DeepEqual(filtered.Config, objs) {
				tt.Errorf("Filtered configs got %+v, want %+v", filtered.Config, objs)
			}
			if filtered.Scope != configFile.Scope || filtered.Name != configFile.Name {
				tt.Errorf("missing expected fields: got %s/%s, want %s/%s",
					filtered.Scope, filtered.Name, configFile.Scope, configFile.Name)
			}
			if !reflect.DeepEqual(filtered.Metadata, configFile.Metadata) {
				tt.Errorf("missing metadata: got %+v want %+v", filtered.Metadata, configFile.Metadata)
			}
			if !reflect.DeepEqual(filtered.ServerMetadata, configFile.ServerMetadata) {
				tt.Errorf("missing server metadata: got %+v want %+v", filtered.ServerMetadata, configFile.ServerMetadata)
			}
		})
	}
}
