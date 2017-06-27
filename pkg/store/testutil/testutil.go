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

package testutil

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"istio.io/galley/pkg/store"
)

// TestManager manages the data to run test cases.
type TestManager struct {
	kv          store.KeyValue
	cleanupFunc func()
}

func (k *TestManager) cleanup() error {
	err := k.kv.Close()
	if k.cleanupFunc != nil {
		k.cleanupFunc()
	}
	return err
}

// NewTestManager creates a new StoreTestManager.
func NewTestManager(s store.KeyValue, cleanup func()) *TestManager {
	return &TestManager{s, cleanup}
}

// RunStoreTest runs the test cases for a KeyValueStore implementation.
func RunStoreTest(t *testing.T, newManagerFn func() (*TestManager, error)) {
	GOODKEYS := []string{
		"/scopes/global/adapters",
		"/scopes/global/descriptors",
		"/scopes/global/subjects/global/rules",
		"/scopes/global/subjects/svc1.ns.cluster.local/rules",
	}

	table := []struct {
		desc       string
		keys       []string
		listPrefix string
		listKeys   []string
	}{
		{"goodkeys", GOODKEYS, "/scopes/global/subjects",
			[]string{"/scopes/global/subjects/global/rules",
				"/scopes/global/subjects/svc1.ns.cluster.local/rules"},
		},
		{"goodkeys", GOODKEYS, "/scopes/", GOODKEYS},
	}

	for _, tt := range table {
		km, err := newManagerFn()
		if err != nil {
			t.Fatalf("failed to create a new manager: %v", err)
		}
		s := km.kv
		t.Run(tt.desc, func(t1 *testing.T) {
			var rv int64
			var found bool
			badkey := "a/b"
			_, rv, found = s.Get(badkey)
			if found {
				t.Errorf("Unexpectedly found %s", badkey)
			}
			var val []byte
			var err error
			// create keys
			for _, key := range tt.keys {
				kc := []byte(key)
				_, err = s.Set(key, kc, 0)
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", key, err)
				}
				val, _, found = s.Get(key)
				if !found || !bytes.Equal(kc, val) {
					t.Errorf("Got %s\nWant %s", val, kc)
				}
			}

			// check of optimistic concurrency
			_, err = s.Set(tt.keys[0], []byte("wrong_data"), rv)
			if err == nil {
				t.Errorf("Unexpected succeed of Set")
			}
			var d1 []byte
			d1, rv, _ = s.Get(tt.keys[0])
			rv, err = s.Set(tt.keys[0], []byte("new data"), rv)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			_, err = s.Set(tt.keys[0], d1, rv)
			if err != nil {
				t.Errorf("Unepxected error: %v", err)
			}

			d, _, err := s.List(tt.listPrefix)
			if err != nil {
				t.Error("Unexpected error", err)
			}
			k := make([]string, 0, len(d))
			for key := range d {
				k = append(k, key)
			}
			sort.Strings(k)
			if !reflect.DeepEqual(k, tt.listKeys) {
				t.Errorf("Got %s\nWant %s\n", k, tt.listKeys)
			}

			// Get the same list again, to make sure the cache of lists
			// are not broken.
			d, _, err = s.List(tt.listPrefix)
			if err != nil {
				t.Error("Unexpected error", err)
			}
			k = make([]string, 0, len(d))
			for key := range d {
				k = append(k, key)
			}
			sort.Strings(k)
			if !reflect.DeepEqual(k, tt.listKeys) {
				t.Errorf("Got %s\nWant %s\n", k, tt.listKeys)
			}

			err = s.Delete(k[1])
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			_, _, found = s.Get(k[1])
			if found {
				t.Errorf("Unexpectedly found %s", k[1])
			}
		})
		if err := km.cleanup(); err != nil {
			t.Errorf("failure on cleanup: %v", err)
		}
	}
}

func compareEvents(actual []store.Event, expected []store.Event) bool {
	// only compares the type, key, and the value. Not comparing the previous value and the revision
	// since it may differ based on the store implementation.
	if len(actual) != len(expected) {
		return false
	}
	for i, aev := range actual {
		eev := expected[i]
		if aev.Type != eev.Type || aev.Key != eev.Key || !bytes.Equal(aev.Value, eev.Value) {
			return false
		}
		if len(aev.PreviousValue) != 0 && !bytes.Equal(aev.PreviousValue, eev.PreviousValue) {
			return false
		}
	}
	return true
}

// RunWatcherTest runs the test cases for a Watcher implementation.
func RunWatcherTest(t *testing.T, newManagerFn func() (*TestManager, error)) {
	km, err := newManagerFn()
	if err != nil {
		t.Fatalf("failed to create a new manager: %v", err)
	}
	s := km.kv
	_, rv, err := s.List("")
	if err != nil {
		t.Fatalf("failed to get the revision: %v", err)
	}
	expected := []store.Event{
		{Type: store.PUT, Key: "/test/k1", Value: []byte("v1")},
		{Type: store.PUT, Key: "/test/k2", Value: []byte("v2")},
		{Type: store.PUT, Key: "/test/k1", Value: []byte("v11"), PreviousValue: []byte("v1")},
		{Type: store.DELETE, Key: "/test/k2", PreviousValue: []byte("v2")},
	}

	watchdone := make(chan interface{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	wch, err := s.Watch(ctx, "/test/", rv)
	if err != nil {
		t.Fatalf("can't watch: %v", err)
	}

	evs := []store.Event{}
	go func() {
		for ev := range wch {
			evs = append(evs, ev)
			if len(evs) == len(expected) {
				cancel()
			}
		}
		err = ctx.Err()
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected failure on watching: %v", err)
		}
		close(watchdone)
	}()

	rv, err = s.Set("/test/k1", []byte("v1"), rv)
	if err != nil {
		t.Errorf("failed to set: %v", err)
	}
	rv, err = s.Set("/test2/k1", []byte("v21"), rv)
	if err != nil {
		t.Errorf("failed to set: %v", err)
	}
	rv, err = s.Set("/test/k2", []byte("v2"), rv)
	if err != nil {
		t.Errorf("failed to set: %v", err)
	}
	_, err = s.Set("/test/k1", []byte("v11"), rv)
	if err != nil {
		t.Errorf("failed to set: %v", err)
	}
	err = s.Delete("/test/k2")
	if err != nil {
		t.Errorf("failed to set: %v", err)
	}
	<-watchdone
	if !compareEvents(evs, expected) {
		t.Errorf("Got: %+v\nWant %+v\n", evs, expected)
	}
	if err := km.cleanup(); err != nil {
		t.Errorf("failed on cleanup: %v", err)
	}
}
