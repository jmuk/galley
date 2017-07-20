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
	"io"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	galleypb "istio.io/galley/api/galley/v1"
	internalpb "istio.io/galley/pkg/server/internal"
	"istio.io/galley/pkg/store"
)

type watcher struct {
	ch     chan *galleypb.WatchResponse
	prefix string
	types  map[string]bool
}

// filterConfigFile checks the config objects and returns a new config file which
// only contains the config objects with watcher's types. If no such config objects
// are in the file, it returns nil instead.
func (w *watcher) filterConfigFile(file *galleypb.ConfigFile) *galleypb.ConfigFile {
	if file == nil {
		return nil
	}
	objs := make([]*galleypb.ConfigObject, 0, len(file.Config))
	for _, obj := range file.Config {
		if _, ok := w.types[obj.Type]; ok {
			objs = append(objs, obj)
		}
	}
	if len(objs) == 0 {
		return nil
	}
	return &galleypb.ConfigFile{
		Scope:          file.Scope,
		Name:           file.Name,
		Metadata:       file.Metadata,
		ServerMetadata: file.ServerMetadata,
		Config:         objs,
	}
}

// WatcherServer impelements the Watcher service.
type WatcherServer struct {
	s store.Store

	interval time.Duration

	mu              sync.Mutex
	cancelWatchChan context.CancelFunc
	nextWatcherID   int64
	watchers        map[int64]*watcher
	lastRevision    int64
}

var _ galleypb.WatcherServer = &WatcherServer{}

// NewWatcherServer creates a new galleypb.WatcherServer instance with
// the specified storage.
func NewWatcherServer(s store.Store, interval time.Duration) (*WatcherServer, error) {
	return &WatcherServer{s: s, interval: interval, watchers: map[int64]*watcher{}}, nil
}

func (s *WatcherServer) watchLoop(ctx context.Context, c <-chan store.Event) {
	t := time.NewTicker(s.interval)
	evs := []store.Event{}
	for {
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case ev := <-c:
			evs = append(evs, ev)
		case <-t.C:
			s.dispatchEvents(evs)
			evs = []store.Event{}
		}
	}
}

// filterEvents converts the store.Event instance to galleypb.Event and transforms the event
// slice to the mapping from watcher id to the events relevante to the watcher.
func (s *WatcherServer) filterEvents(evs []store.Event) map[int64]*galleypb.WatchEvents {
	filtered := map[int64]*galleypb.WatchEvents{}
	for _, ev := range evs {
		if ev.Revision > s.lastRevision {
			s.lastRevision = ev.Revision
		}
		var newFile *galleypb.ConfigFile
		var oldFile *galleypb.ConfigFile
		var err error
		for id, w := range s.watchers {
			if !strings.HasPrefix(ev.Key, w.prefix) {
				continue
			}
			target, ok := filtered[id]
			if !ok {
				target = &galleypb.WatchEvents{}
				filtered[id] = target
			}
			if newFile == nil && ev.Type == store.PUT {
				if newFile, err = getConfigFile(ev.Value); err != nil {
					glog.Warningf("failed to parse the value: %v", err)
					continue
				}
			}
			if oldFile == nil && ev.PreviousValue != nil {
				if oldFile, err = getConfigFile(ev.PreviousValue); err != nil {
					glog.Warningf("failed to parse the value: %v", err)
					continue
				}
			}
			pbev := &galleypb.ConfigFileChange{
				FileId:  ev.Key,
				OldFile: w.filterConfigFile(oldFile),
			}
			if ev.Type == store.PUT {
				pbev.Type = galleypb.ConfigFileChange_PUT
				pbev.NewFile = w.filterConfigFile(newFile)
			} else {
				// TODO: maybe removal should also check with the previous value?
				pbev.NewFile = &galleypb.ConfigFile{
					// TODO: create scope/name from the key.
					ServerMetadata: &galleypb.ServerMetadata{
						Path:     ev.Key,
						Revision: ev.Revision,
					},
				}
				pbev.Type = galleypb.ConfigFileChange_DELETE
			}
			if pbev != nil {
				target.Events = append(target.Events, pbev)
			}
		}
	}
	return filtered
}

func (s *WatcherServer) dispatchEvents(evs []store.Event) {
	s.mu.Lock()
	filtered := s.filterEvents(evs)
	for id, w := range s.watchers {
		if pbevs, ok := filtered[id]; ok && len(pbevs.Events) > 0 {
			w.ch <- &galleypb.WatchResponse{ResponseUnion: &galleypb.WatchResponse_Events{Events: pbevs}}
		} else {
			w.ch <- &galleypb.WatchResponse{
				ResponseUnion: &galleypb.WatchResponse_Progress{Progress: &galleypb.WatchProgress{CurrentRevision: s.lastRevision}},
			}
		}
	}
	s.mu.Unlock()
}

func (s *WatcherServer) initiateWatcher(ctx context.Context, req *galleypb.WatchRequest, stream galleypb.Watcher_WatchServer) (int64, *watcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := getPrefix(req.Scope)
	data, revision, err := s.s.List(ctx, prefix)
	if err != nil {
		return 0, nil, err
	}
	if revision > s.lastRevision {
		s.lastRevision = revision
	}

	// register watcher struct
	watcher := &watcher{
		ch:     make(chan *galleypb.WatchResponse),
		prefix: prefix,
		types:  map[string]bool{},
	}
	for _, t := range req.Types {
		watcher.types[t] = true
	}
	id := s.nextWatcherID
	s.watchers[id] = watcher
	s.nextWatcherID++
	if s.cancelWatchChan == nil {
		// do not use the input ctx, because the watch channel may outlive the requested watch stream.
		wctx, cancel := context.WithCancel(context.Background())
		wch, werr := s.s.Watch(wctx, "", req.StartRevision)
		if werr != nil {
			cancel()
			return -1, nil, werr
		}
		s.cancelWatchChan = cancel
		go s.watchLoop(wctx, wch)
	}

	err = stream.Send(&galleypb.WatchResponse{
		Status: status.New(codes.OK, "").Proto(),
		ResponseUnion: &galleypb.WatchResponse_Created{
			Created: &galleypb.WatchCreated{CurrentRevision: revision},
		},
	})
	if err != nil {
		return -1, nil, err
	}

	if req.IncludeInitialState {
		// TODO: paging when data is huge.
		state := &galleypb.InitialState{Entries: make([]*galleypb.ConfigFileEntry, 0, len(data)), Done: true}
		for k, v := range data {
			ifile := &internalpb.File{}
			if err = proto.Unmarshal(v, ifile); err != nil {
				glog.Errorf("failed to unmarshal a config file %s: %v", k, err)
				continue
			}
			filtered := watcher.filterConfigFile(ifile.Encoded)
			if filtered == nil {
				continue
			}
			state.Entries = append(state.Entries, &galleypb.ConfigFileEntry{
				FileId: k,
				File:   filtered,
			})
		}
		err = stream.Send(&galleypb.WatchResponse{
			Status: status.New(codes.OK, "").Proto(),
			ResponseUnion: &galleypb.WatchResponse_InitialState{
				InitialState: state,
			},
		})
		if err != nil {
			glog.Errorf("failed to send the initial state: %v", err)
		}
	}
	return id, watcher, nil
}

func (s *WatcherServer) removeWatcher(id int64) {
	s.mu.Lock()
	delete(s.watchers, id)
	if len(s.watchers) == 0 {
		s.cancelWatchChan()
		s.cancelWatchChan = nil
	}
	s.mu.Unlock()
}

// Watch implements a Watcher method.
func (s *WatcherServer) Watch(req *galleypb.WatchRequest, stream galleypb.Watcher_WatchServer) error {
	ctx := stream.Context()
	id, watcher, err := s.initiateWatcher(ctx, req, stream)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

outerLoop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break outerLoop
		case ev := <-watcher.ch:
			err = stream.Send(ev)
			if err != nil {
				break outerLoop
			}
		}
	}

	s.removeWatcher(id)

	if err == nil {
		return nil
	}
	code := codes.Internal
	switch err {
	case io.EOF:
		code = codes.OK
	case context.DeadlineExceeded:
		code = codes.DeadlineExceeded
	case context.Canceled:
		code = codes.Canceled
	}
	return status.Error(code, err.Error())
}
