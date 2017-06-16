// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Revision 2.0 (the "License");
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
	"sync"

	rpc "github.com/googleapis/googleapis/google/rpc"

	galleypb "istio.io/api/galley/v1"
	"istio.io/galley/pkg/store"
)

type watcher struct {
	stream galleypb.Watcher_WatchServer
	c      <-chan store.ChangeList
	cancel context.CancelFunc
}

type watcherServer struct {
	kvs store.KeyValueStore
	cw  store.ChangeWatcher

	lastNotifiedIndex int
	lastFetchedIndex  int
	nextWatcherID     int64
	watchers          map[int64]*watcher
	mu                sync.Mutex
}

var _ galleypb.WatcherServer = &watcherServer{}

// NewWatcherServer creates a new galleypb.WatcherServer instance with
// the specified storage.
func NewWatcherServer(kvs store.KeyValueStore) (*watcherServer, error) {
	s := &watcherServer{kvs: kvs, watchers: map[int64]*watcher{}}
	if cw, ok := kvs.(store.ChangeWatcher); ok {
		s.cw = cw
	} else {
		return nil, fmt.Errorf("config store %s is not a change watcher", kvs)
	}
	return s, nil
}

func (s *watcherServer) startWatch(req *galleypb.WatchCreateRequest, stream galleypb.Watcher_WatchServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs, revision, err := readKvsToObjects(s.kvs, buildPath(req.Subtree))
	if err != nil {
		stream.Send(&galleypb.WatchResponse{
			Status: &rpc.Status{
				Code:    int32(rpc.Code_INTERNAL),
				Message: err.Error(),
			},
		})
		return
	}

	id := s.nextWatcherID
	s.nextWatcherID++
	c, cancel := s.cw.Watch(buildPath(req.Subtree))
	s.watchers[id] = &watcher{
		c:      c,
		stream: stream,
		cancel: cancel,
	}
	go func() {
		for cl := range c {
			evs := &galleypb.WatchEvents{}
			for _, change := range cl.Changes {
				meta, err := pathToMeta(change.Key)
				ev := &galleypb.Event{}
				if change.Type == store.Update {
					ev.Kv, err = buildObject(change.Value, meta)
					if err != nil {
						continue
					}
					ev.Type = galleypb.Event_PUT
				} else {
					ev.Kv = &galleypb.Object{Meta: meta}
					ev.Type = galleypb.Event_DELETE
				}
				evs.Events = append(evs.Events, ev)
			}
			stream.Send(&galleypb.WatchResponse{
				WatchId:       id,
				Status:        &rpc.Status{Code: int32(rpc.Code_OK)},
				ResponseUnion: &galleypb.WatchResponse_Events{evs},
			})
		}
	}()
	stream.Send(&galleypb.WatchResponse{
		WatchId: id,
		Status:  &rpc.Status{Code: int32(rpc.Code_OK)},
		ResponseUnion: &galleypb.WatchResponse_Created{&galleypb.WatchCreated{
			InitialState:    objs,
			CurrentRevision: int64(revision),
		}},
	})
}

func (s *watcherServer) cancelWatch(req *galleypb.WatchCancelRequest, stream galleypb.Watcher_WatchServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.watchers[req.WatchId]
	resp := &galleypb.WatchResponse{
		WatchId:       req.WatchId,
		ResponseUnion: &galleypb.WatchResponse_Canceled{&galleypb.WatchCanceled{}},
	}
	if !ok {
		resp.Status = &rpc.Status{Code: int32(rpc.Code_NOT_FOUND)}
	} else {
		w.cancel()
		delete(s.watchers, req.WatchId)
		resp.Status = &rpc.Status{Code: int32(rpc.Code_OK)}
	}
	stream.Send(resp)
}

func (s *watcherServer) Watch(stream galleypb.Watcher_WatchServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}
		if createReq := req.GetCreateRequest(); createReq != nil {
			s.startWatch(createReq, stream)
		} else if cancelReq := req.GetCancelRequest(); cancelReq != nil {
			s.cancelWatch(cancelReq, stream)
		}
	}
	return nil
}
