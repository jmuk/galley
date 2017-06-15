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
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	rpc "github.com/googleapis/googleapis/google/rpc"

	configpb "istio.io/api/config/v1"
	"istio.io/galley/pkg/store"
)

type watcher struct {
	stream               configpb.Watcher_WatchServer
	keyPrefix            string
	sendPut              bool
	sendDelete           bool
	lastNotifiedRevision int64
}

type watcherServer struct {
	kvs store.KeyValueStore
	cr  store.ChangeLogReader

	lastNotifiedIndex int
	lastFetchedIndex  int
	nextWatcherID     int64
	watchers          map[int64]*watcher
	mu                sync.Mutex
}

var _ configpb.WatcherServer = &watcherServer{}

// NewWatcherServer creates a new configpb.WatcherServer instance with
// the specified storage.
func NewWatcherServer(kvs store.KeyValueStore) (*watcherServer, error) {
	s := &watcherServer{kvs: kvs, watchers: map[int64]*watcher{}}
	if cn, ok := kvs.(store.ChangeNotifier); ok {
		cn.RegisterListener(s)
	} else {
		return nil, fmt.Errorf("config store %s is not a change notifier", kvs)
	}
	if cr, ok := kvs.(store.ChangeLogReader); ok {
		s.cr = cr
	} else {
		return nil, fmt.Errorf("config store %s is not changelog readable", kvs)
	}
	return s, nil
}

func (s *watcherServer) start(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			s.check()
		}
	}()
}

func (s *watcherServer) check() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastFetchedIndex < s.lastNotifiedIndex {
		changes, err := s.cr.Read(s.lastFetchedIndex)
		if err != nil {
			glog.Warningf("failed to read changes: %v", err)
			return
		}
		for _, c := range changes {
			if c.Index < s.lastFetchedIndex {
				s.lastFetchedIndex = c.Index
			}
		}
		toSend := s.filterEvents(s.watchers, changes)
		for id, evs := range toSend {
			s.watchers[id].stream.Send(&configpb.WatchResponse{
				WatchId: id,
				Status: &rpc.Status{
					Code: int32(rpc.Code_OK),
				},
				ResponseUnion: &configpb.WatchResponse_Events{&configpb.WatchEvents{evs}},
			})
		}
	}
}

func (s *watcherServer) NotifyStoreChanged(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastNotifiedIndex = index
}

func (s *watcherServer) filterEvents(watchers map[int64]*watcher, changes []store.Change) map[int64][]*configpb.Event {
	toSend := map[int64][]*configpb.Event{}

	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Index > changes[j].Index
	})
	visited := map[string]bool{}
	filtered := make([]store.Change, 0, len(changes))
	for _, c := range changes {
		if visited[c.Key] {
			continue
		}
		visited[c.Key] = true
		filtered = append(filtered, c)
	}

	for _, c := range filtered {
		meta, err := pathToMeta(c.Key)
		if err != nil {
			glog.Warningf("%v", err)
			continue
		}
		ev := &configpb.Event{
			Kv: &configpb.Object{Meta: meta},
		}
		dataFetched := false
		if c.Type == store.Update {
			ev.Type = configpb.Event_UPDATE
		} else {
			ev.Type = configpb.Event_DELETE
		}
		for id, w := range watchers {
			if strings.HasPrefix(c.Key, w.keyPrefix) && w.lastNotifiedRevision < int64(c.Index) {
				if c.Type == store.Update && !dataFetched {
					dataFetched = true
					data, _, found := s.kvs.Get(c.Key)
					if found {
						glog.Warningf("failed to fetch data for key %s", c.Key)
					}
					ev.Kv, err = buildObject(data, ev.Kv.Meta, &configpb.ObjectFieldInclude{Data: true, SourceData: true})
					if err != nil {
						glog.Warningf("failed to builds an object for key %s: %v", c.Key, err)
					}
				}
				toSend[id] = append(toSend[id], ev)
				w.lastNotifiedRevision = int64(c.Index)
			}
		}
	}
	return toSend
}

func (s *watcherServer) startWatch(req *configpb.WatchCreateRequest, stream configpb.Watcher_WatchServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs, _, err := readKvsToObjects(s.kvs, buildPath(req.Subtree), &configpb.ObjectFieldInclude{true, true})
	if err != nil {
		stream.Send(&configpb.WatchResponse{
			Status: &rpc.Status{
				Code:    int32(rpc.Code_INTERNAL),
				Message: err.Error(),
			},
		})
		return
	}

	id := s.nextWatcherID
	s.nextWatcherID++
	s.watchers[id] = &watcher{
		keyPrefix:            buildPath(req.Subtree),
		stream:               stream,
		lastNotifiedRevision: req.StartRevision - 1,
	}
	stream.Send(&configpb.WatchResponse{
		WatchId:       id,
		Status:        &rpc.Status{Code: int32(rpc.Code_OK)},
		ResponseUnion: &configpb.WatchResponse_Created{&configpb.WatchCreated{objs}},
	})
}

func (s *watcherServer) cancelWatch(req *configpb.WatchCancelRequest, stream configpb.Watcher_WatchServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &configpb.WatchResponse{
		WatchId:       req.WatchId,
		ResponseUnion: &configpb.WatchResponse_Canceled{&configpb.WatchCanceled{}},
	}
	_, found := s.watchers[req.WatchId]
	if found {
		delete(s.watchers, req.WatchId)
		resp.Status = &rpc.Status{
			Code: int32(rpc.Code_OK),
		}
	} else {
		resp.Status = &rpc.Status{
			Code:    int32(rpc.Code_NOT_FOUND),
			Message: fmt.Sprintf("watcher id %d not found", req.WatchId),
		}
	}
	stream.Send(resp)
}

func (s *watcherServer) Watch(stream configpb.Watcher_WatchServer) error {
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
