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

// Package store provides the interface to the backend storage
// for the config and the default fsstore implementation.
package store

import (
	"context"
	"fmt"
	"net/url"
)

// IndexNotSupported will be used as the returned index value when
// the KeyValueStore implementation does not support index.
const IndexNotSupported = -1

// Builder is the type to build a KeyValueStore.
type Builder func(u *url.URL) (KeyValueStore, error)

// RegisterFunc is the type to register a new builder for a scheme.
type RegisterFunc func(map[string]Builder)

// ChangeType denotes the type of a change
type ChangeType int

const (
	// Update - change was an update or a create to a key.
	Update ChangeType = iota
	// Delete - key was removed.
	Delete
)

// Change - A record of mutation to the underlying KeyValueStore.
type Change struct {
	// Key that was affected
	Key string `json:"key"`
	// Type how did the key change
	Type ChangeType `json:"change_type"`
	// The new value when Updated (empty when Deleted)
	Value string
	// The previous value before the change.
	PrevValue string
	// change log index number of the change
	Index int `json:"index"`
}

// ChangeList bundles multiple changes in a notification.
type ChangeList struct {
	// The list of changes.
	Changes []Change
	// The new storage revision after the change.
	Revision int
}

// KeyValueStore defines the key value store back end interface used by mixer
// and Mixer config API server.
//
// It should support back ends like redis, etcd and NFS
// All commands should return a change log index number which can be used
// to Read changes. If a KeyValueStore does not support it,
// it should return -1
type KeyValueStore interface {
	// Get value at a key, false if not found.
	Get(key string) (value string, index int, found bool)

	// Set a value.
	Set(key string, value string) (index int, err error)

	// List keys with the prefix.
	List(key string, recurse bool) (keys []string, index int, err error)

	// Delete a key.
	Delete(key string) error

	// Close the storage.
	Close()

	fmt.Stringer
}

// ChangeWatcher offers the interface for subscribing changes.
type ChangeWatcher interface {
	Watch(prefix string) (<-chan ChangeList, context.CancelFunc)
}

// Registry remembers the relationship between the URL scheme and the backend storage.
type Registry struct {
	builders map[string]Builder
}

// NewRegistry creates a new registry to build a store from the URL.
func NewRegistry(inventory ...RegisterFunc) *Registry {
	b := map[string]Builder{}
	for _, rf := range inventory {
		rf(b)
	}
	return &Registry{builders: b}
}

// URL types supported by the config store
const (
	// example fs:///tmp/testdata/configroot
	FSUrl = "fs"
)

// NewStore create a new store based on the config URL.
func (r *Registry) NewStore(configURL string) (KeyValueStore, error) {
	u, err := url.Parse(configURL)

	if err != nil {
		return nil, fmt.Errorf("invalid config URL %s %v", configURL, err)
	}

	if u.Scheme == FSUrl {
		return newFSStore(u.Path)
	}
	if builder, ok := r.builders[u.Scheme]; ok {
		return builder(u)
	}

	return nil, fmt.Errorf("unknown config URL %s %v", configURL, u)
}
