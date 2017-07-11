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
	"errors"
	"fmt"
	"strings"
	"sync"

	"istio.io/galley/pkg/store"
)

// Store implements store.Store.
type Store struct {
	mu       sync.RWMutex
	data     map[string][]byte
	revision int64
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
	if !strings.HasSuffix(prefix, "/") {
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
	ms.data[key] = value
	ms.revision++
	return ms.revision, nil
}

// Delete implements store.Writer interface.
func (ms *Store) Delete(ctx context.Context, key string) (int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.data, key)
	ms.revision++
	return ms.revision, nil
}

// Watch implements store.Watcher interface.
func (ms *Store) Watch(ctx context.Context, key string, revision int64) (<-chan store.Event, error) {
	// not implemented yet.
	return nil, errors.New("not implemented")
}

// New creates a new instance of Store.
func New() *Store {
	return &Store{
		data:     map[string][]byte{},
		revision: 0,
	}
}
