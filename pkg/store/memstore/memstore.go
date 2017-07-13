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

// Package memstore offers in-memory storage which would be useful for testing.
package memstore

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"istio.io/galley/pkg/store"
)

type watcher struct {
	id     int
	prefix string
	ch     chan store.Event
}

// Store implements store.Store.
type Store struct {
	mu            sync.RWMutex
	data          map[string][]byte
	revision      int64
	watchers      map[int]*watcher
	nextWatcherID int
}

// String implements fmt.Stringer interface.
func (ms *Store) String() string {
	return fmt.Sprintf("%d: %+v", ms.revision, ms.data)
}

// Close implements io.Closer interface.
func (ms *Store) Close() error {
	return nil
}

// Get implements store.Reader interface.
func (ms *Store) Get(ctx context.Context, key string) ([]byte, int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	value, ok := ms.data[key]
	if !ok {
		return nil, ms.revision, store.ErrNotFound
	}
	return value, ms.revision, nil
}

// List implements store.Reader interface.
func (ms *Store) List(ctx context.Context, prefix string) (map[string][]byte, int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	results := map[string][]byte{}
	for k, v := range ms.data {
		if strings.HasPrefix(k, prefix) {
			results[k] = v
		}
	}
	return results, ms.revision, nil
}

func (ms *Store) dispatchWatchEvents(t store.EventType, key string, value, prevValue []byte, revision int64) {
	for _, w := range ms.watchers {
		if !strings.HasPrefix(key, w.prefix) {
			continue
		}
		w.ch <- store.Event{
			Type:          t,
			Key:           key,
			Value:         value,
			PreviousValue: prevValue,
			Revision:      revision,
		}
	}
}

// Set implements store.Writer interface.
func (ms *Store) Set(ctx context.Context, key string, value []byte, revision int64) (int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if revision >= 0 && ms.revision > revision {
		return ms.revision, &store.RevisionMismatchError{
			Key:              key,
			ExpectedRevision: revision,
			ActualRevision:   ms.revision,
		}
	}
	prevValue := ms.data[key]
	ms.data[key] = value
	ms.revision++
	ms.dispatchWatchEvents(store.PUT, key, value, prevValue, ms.revision)
	return ms.revision, nil
}

// Delete implements store.Writer interface.
func (ms *Store) Delete(ctx context.Context, key string) (int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	prevValue, ok := ms.data[key]
	if !ok {
		return ms.revision, store.ErrNotFound
	}
	delete(ms.data, key)
	ms.revision++
	ms.dispatchWatchEvents(store.DELETE, key, nil, prevValue, ms.revision)
	return ms.revision, nil
}

func (ms *Store) waitWatch(ctx context.Context, w *watcher) {
	<-ctx.Done()
	close(w.ch)

	ms.mu.Lock()
	delete(ms.watchers, w.id)
	ms.mu.Unlock()
}

// Watch implements store.Watcher interface.
func (ms *Store) Watch(ctx context.Context, key string, revision int64) (<-chan store.Event, error) {
	ms.mu.Lock()
	w := &watcher{ms.nextWatcherID, key, make(chan store.Event)}
	ms.nextWatcherID++
	ms.watchers[w.id] = w
	go ms.waitWatch(ctx, w)
	ms.mu.Unlock()
	return w.ch, nil
}

// New creates a new instance of Store.
func New() *Store {
	return &Store{
		data:     map[string][]byte{},
		revision: 0,
		watchers: map[int]*watcher{},
	}
}

// Register registers memory scheme as the store backend.
func Register(m map[string]store.Builder) {
	m["memory"] = func(u *url.URL) (store.Store, error) {
		return New(), nil
	}
}
