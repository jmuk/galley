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

// Package shared contains types and functions that are used across the full
// set of galley commands.

package store

import (
	"fmt"
	"io"
	"time"

	"context"
)

type EventType int32

const (
	Event_PUT    EventType = 0
	Event_DELETE EventType = 1
)

// Event is a change that has occurred to the underlying store.
type Event struct {
	// Type of event ADD+UPDATE  or DELETE
	Type EventType
	// Key is the Affected Key.
	Key string
	// Value is the Value after the update.
	Value []byte
	// Previous Value is the value before the update.
	Previous_Value []byte
	// Repository revision at the last update.
	Revision int64
}

// KeyValueStore provides a generic interface to a key value store.
type KeyValueStore interface {
	KVReader

	KVWriter

	Watcher

	// Lock obtains a lock (or lease) a tree rooted at key and
	// returns a LockedKVWriter. The lock is valid for ttl duration.
	// All write operations fail after the lock expires.
	// Before validation requests are sent out, Galley acquires a
	// timebound lock (lease) on the part of the tree in question.
	Lock(key string, ttl time.Duration) (LockedKVWriter, error)

	fmt.Stringer

	io.Closer
}

type LockedKVWriter interface {
	KVWriter

	// Renew lease on the lock
	Renew(ttl time.Duration) error

	// Unlock the writer. All subsequest operations should
	// fail after unlocking.
	Unlock() error
}

// KVWriter defines write operations of KVStore.
type KVWriter interface {
	// Set a value.
	Set(key string, value []byte, revision int64) (out_revision int64, err error)

	// Delete a key.
	Delete(key string) error
}

// KVReader defines read operations of KVStore.
type KVReader interface {
	// Get value at a key, false if not found.
	Get(key string) (value []byte, revision int64, found bool)

	// List keys with the prefix.
	List(key string, recurse bool) (keys []string, revision int, err error)
}

type Watcher interface {
	// Watch a storage tree rooted at 'key'
	// Watch can be canceled by the caller by canceling the context.
	// Watch returns an Event channel that streams changes.
	// Server closes channel if there is an error.
	Watch(ctx context.Context, key string) (<-chan Event, error)
}
