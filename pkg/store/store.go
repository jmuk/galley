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

// Package store contains interfaces and implementations
// of key value storage layer.
package store

import (
	"context"
	"fmt"
	"io"
)

// Store provides a generic interface to a key value store.
type Store interface {
	// Get value at a key. Returns non-nil error if not found.
	Get(key string) (value []byte, revision int64, err error)

	// List keys with the key prefix. Reply includes values.
	List(keyPrefix string) (data map[string]string, revision int64, err error)

	// Set a value. revision is used for optimistic concurrency.
	// When revision is negative, it sets the value without checking.
	// When the error happens due to the failure of optimistic concurrency,
	// it should return RevisionMismatchError.
	Set(key string, value []byte, revision int64) (outRevision int64, err error)

	// Delete a key.
	Delete(key string) (outRevision int64, err error)

	Watcher
	fmt.Stringer
	io.Closer
}

// ErrNotFound is an error which can be used to express that the underlying
// storage works well but simply the value is missing.
var ErrNotFound error = fmt.Errorf("not found")

// RevisionMismatchError should be returned on Set method when the
// specified revision doesn't satisfy the expectation.
type RevisionMismatchError struct {
	Key              string
	ExpectedRevision int64
	ActualRevision   int64
}

// Error implements error interface.
func (err *RevisionMismatchError) Error() string {
	return fmt.Sprintf(
		"failed to set %s: revision %d is older than the actual revision %d",
		err.Key, err.ExpectedRevision, err.ActualRevision)
}

// Watcher defines a wachable store.
type Watcher interface {
	// Watch a storage tree rooted at 'key'
	// Watch can be canceled by the caller by canceling the context.
	// Watch returns an Event channel that streams changes.
	// Server closes channel if there is an error.
	// revision == 0 indicates watch starts with the current revision.
	// Attempting to start a watch on a revision that is not available (due to compaction)
	// results in an error.
	Watch(ctx context.Context, key string, revision int64) (<-chan Event, error)
}

// EventType is the type of a change event.
type EventType int32

const (
	// PUT event indicates an update operation. ADD/UPDATE
	PUT EventType = iota
	// DELETE event indicates deletion operation.
	DELETE
)

// Event is a change that has occurred to the underlying store.
type Event struct {
	Type          EventType // Type of event ADD+UPDATE  or DELETE
	Key           string    // Key is the Affected Key.
	Value         []byte    // Value is the Value after the update.
	PreviousValue []byte    // Previous Value is the value before the update.
	Revision      int64     // Repository revision at the last update.
}
