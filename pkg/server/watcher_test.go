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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	galleypb "istio.io/galley/api/galley/v1"
	internalpb "istio.io/galley/pkg/server/internal"
	"istio.io/galley/pkg/store"
)

const watchIntervalForTest = time.Millisecond

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
	scope string
	types map[string]bool

	t *testing.T

	mu          sync.Mutex
	done        chan interface{}
	onEvent     func()
	created     bool
	initialized bool
	revision    int64
	initialData map[string]*galleypb.ConfigFile
	events      []*galleypb.ConfigFileChange
	recvCount   int
}

func (tm *testWatcherManager) newWatchConsumer(ctx context.Context, t *testing.T, scope string, types []string) (*watchConsumer, error) {
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
		scope:       scope,
		types:       ts,
		t:           t,
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

func (c *watchConsumer) setOnEvent(onEvent func() bool) {
	c.mu.Lock()
	c.done = make(chan interface{})
	c.onEvent = func() {
		if onEvent() {
			close(c.done)
			c.onEvent = nil
		}
	}
	c.onEvent()
	c.mu.Unlock()
}

func (c *watchConsumer) wait() {
	<-c.done
}

func (c *watchConsumer) start() {
	for {
		resp, err := c.s.Recv()
		if err == io.EOF || err == context.Canceled || err == context.DeadlineExceeded {
			return
		} else if st, ok := status.FromError(err); ok && st.Code() == codes.Canceled {
			// cancellation happens on the end of the tests.
			return
		} else if err != nil {
			c.t.Errorf("%s: unexpected error on Recv: %v", c.scope, err)
			return
		}
		c.mu.Lock()
		c.recvCount++
		switch r := resp.GetResponseUnion().(type) {
		case *galleypb.WatchResponse_Created:
			if c.created {
				c.t.Errorf("%s: created event comes twice", c.scope)
			}
			c.created = true
			c.revision = r.Created.CurrentRevision
		case *galleypb.WatchResponse_InitialState:
			if c.initialized {
				c.t.Errorf("%s: already initialized", c.scope)
			} else {
				for _, ent := range r.InitialState.Entries {
					c.initialData[ent.FileId] = ent.File
				}
				if r.InitialState.Done {
					c.initialized = true
				}
			}
		case *galleypb.WatchResponse_Progress:
			c.revision = r.Progress.CurrentRevision
		case *galleypb.WatchResponse_Events:
			c.events = append(c.events, r.Events.Events...)
		default:
			c.t.Errorf("%s: unrecognized event: %+v", c.scope, resp)
		}
		if c.onEvent != nil {
			c.onEvent()
		}
		c.mu.Unlock()
	}
}

func (c *watchConsumer) validateFile(got, want *galleypb.ConfigFile) error {
	if want == nil {
		if got != nil {
			return fmt.Errorf("got %+v, want nil", got)
		}
		return nil
	}
	if got.Scope != want.Scope || got.Name != want.Name {
		return fmt.Errorf("got %s/%s, want %s/%s", got.Scope, got.Name, want.Scope, want.Name)
	}
	if !reflect.DeepEqual(got.Metadata, want.Metadata) {
		return fmt.Errorf("metadata: got %+v, want %+v", got.Metadata, want.Metadata)
	}
	if want.ServerMetadata != nil {
		if got.ServerMetadata.Path != want.ServerMetadata.Path {
			return fmt.Errorf("server metadata: got %s, want %s", got.ServerMetadata.Path, want.ServerMetadata.Path)
		}
	}

	w := &watcher{types: c.types}
	filtered := w.filterConfigFile(want)
	if !reflect.DeepEqual(got.Config, filtered.Config) {
		return fmt.Errorf("config: got %+v\nwant %+v", got.Config, filtered.Config)
	}
	return nil
}

func (c *watchConsumer) checkInitialData(want map[string]*galleypb.ConfigFile) (errs []error) {
	uncheckedFiles := map[string]bool{}
	for k := range c.initialData {
		uncheckedFiles[k] = true
	}
	for k, w := range want {
		if g, ok := c.initialData[k]; ok {
			if err := c.validateFile(g, w); err != nil {
				errs = append(errs, fmt.Errorf("failed to validate file %s: %v", k, err))
			}
			delete(uncheckedFiles, k)
		} else {
			errs = append(errs, fmt.Errorf("initial data is missing %s", k))
		}
	}
	if len(uncheckedFiles) != 0 {
		names := make([]string, 0, len(uncheckedFiles))
		for k := range uncheckedFiles {
			names = append(names, k)
		}
		errs = append(errs, fmt.Errorf("unexpected files in the initialData: %+v", names))
	}
	return errs
}

func (c *watchConsumer) checkEventsAndReset(t *testing.T, msg string, expected ...*galleypb.ConfigFileChange) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
					if err := c.validateFile(eav.NewFile, ev.NewFile); err != nil {
						t.Errorf("invalid newfile data for PUT event %s: %v", ev.FileId, err)
					}
				}
			}
		}
		if found >= 0 {
			toCheck = append(toCheck[:found], toCheck[(found+1):]...)
		} else {
			t.Errorf("%s: event %+v not found", msg, ev)
		}
	}
	c.events = []*galleypb.ConfigFileChange{}
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

	w1, err := tm.newWatchConsumer(ctx, t, "dept1", []string{"constructor", "handler"})
	if err != nil {
		t.Errorf("failed to watch: %v", err)
	}
	defer w1.Close(t)
	w2, err := tm.newWatchConsumer(ctx, t, "", []string{"unknown"})
	if err != nil {
		t.Errorf("failed to watch: %v", err)
	}
	defer w2.Close(t)
	w1.setOnEvent(func() bool {
		return w1.created && w1.initialized
	})
	w2.setOnEvent(func() bool {
		return w2.created && w2.initialized
	})
	w1.wait()
	w2.wait()
	if errs := w1.checkInitialData(map[string]*galleypb.ConfigFile{k1: configFile}); len(errs) > 0 {
		t.Errorf("w1.checkInitialData failed: %+v", errs)
	}
	if errs := w2.checkInitialData(map[string]*galleypb.ConfigFile{}); len(errs) > 0 {
		t.Errorf("w2.checkInitialData failed: %+v", errs)
	}

	w1.setOnEvent(func() bool {
		return len(w1.events) >= 1
	})
	w2.setOnEvent(func() bool {
		return len(w2.events) >= 2
	})
	if err = tm.setData(ctx, k2, testConfig, true); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	if err = tm.setData(ctx, k3, testConfig, true); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	w1.wait()
	w2.wait()
	w1.checkEventsAndReset(t, "w1/put", newTestChange(PUT, k2, configFile))
	w2.checkEventsAndReset(t, "w2/put", newTestChange(PUT, k2, nil), newTestChange(PUT, k3, nil))

	w1.setOnEvent(func() bool {
		return len(w1.events) >= 1
	})
	if err = tm.setData(ctx, k2, testConfig, false); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	w1.wait()
	w1.checkEventsAndReset(t, "w1/put", newTestChange(PUT, k2, nil))

	r1 := w1.Revision()
	w1.setOnEvent(func() bool {
		return w1.revision > r1
	})
	if _, err = tm.s.Delete(ctx, k3); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	w1.wait()
	if w1.NumEvents() != 0 {
		t.Errorf("new events arrived to w1 unexpectedly")
	}

	w1.setOnEvent(func() bool {
		return len(w1.events) >= 1
	})
	w2.setOnEvent(func() bool {
		return len(w2.events) >= 2
	})
	if _, err = tm.s.Delete(ctx, k2); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	w1.wait()
	w2.wait()
	w1.checkEventsAndReset(t, "w1/delete", newTestChange(DELETE, k2, nil))
	w2.checkEventsAndReset(t, "w2/delete", newTestChange(DELETE, k3, nil), newTestChange(DELETE, k2, nil))
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
	w, err := tm.newWatchConsumer(ctx, t, "dept1", []string{"rule"})
	if err != nil {
		t.Fatalf("failed to start watch: %v", err)
	}
	w.setOnEvent(func() bool {
		return w.created && w.initialized
	})
	w.wait()

	w.setOnEvent(func() bool {
		return len(w.events) > 0
	})
	gc := tm.galley.client
	_, err = gc.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     k1,
		Contents: testConfig,
	})
	if err != nil {
		t.Fatalf("failed to create the file %s: %v", k1, err)
	}
	w.wait()
	w.checkEventsAndReset(t, "create", newTestChange(PUT, k1, configFile))

	r1 := w.Revision()
	w.setOnEvent(func() bool {
		return w.revision > r1
	})
	_, err = gc.CreateFile(ctx, &galleypb.CreateFileRequest{
		Path:     k2,
		Contents: testConfig,
	})
	if err != nil {
		t.Errorf("failed to create the file %s: %v", k2, err)
	}
	w.wait()
	if w.NumEvents() != 0 {
		t.Errorf("new events arrived unexpectedly")
	}

	w.setOnEvent(func() bool {
		return len(w.events) > 0
	})
	if _, err = gc.DeleteFile(ctx, &galleypb.DeleteFileRequest{Path: k1}); err != nil {
		t.Errorf("failed to delete %s: %v", k1, err)
	}
	w.wait()
	w.checkEventsAndReset(t, "delete", newTestChange(DELETE, k1, nil))
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
