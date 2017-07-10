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

package etcd

import (
	"context"
	"net/url"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"

	"istio.io/galley/pkg/store"
)

// The size of the buffer for the channel which Watch returns.
// TODO: remove this constant and introduce some smarter approach
// to determine the buffer size nicely (or allow customization?)
const watchBufSize int = 10

// globalRevisionKey is the key to track the storage revision.
// This is used for Set() method to ensure that no other operations are
// made outside.
// TODO: remove this if we find the way to use the storage revision
// in the transaction conditions.
const globalRevisionKey string = "global_revision"

// Store implements store.Store for etcd.
type Store struct {
	client *clientv3.Client
	u      *url.URL
}

// String implements fmt.Stringer interface.
func (es *Store) String() string {
	return es.u.String()
}

// Close implements io.Closer interface.
func (es *Store) Close() error {
	return es.client.Close()
}

func normalizeKey(key string) string {
	if !strings.HasPrefix(key, "/") {
		return "/" + key
	}
	return key
}

// Get implements store.Reader interface.
func (es *Store) Get(key string) ([]byte, int64, error) {
	key = normalizeKey(key)
	resp, err := es.client.Get(es.client.Ctx(), key)
	if err != nil {
		return nil, 0, err
	}
	revision := resp.Header.Revision
	for _, kvs := range resp.Kvs {
		if string(kvs.Key) == key {
			return kvs.Value, revision, nil
		}
	}
	return nil, revision, store.ErrNotFound
}

// List implements store.Reader interface.
func (es *Store) List(prefix string) (map[string][]byte, int64, error) {
	prefix = normalizeKey(prefix)
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	resp, err := es.client.Get(es.client.Ctx(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}
	data := map[string][]byte{}
	for _, kvs := range resp.Kvs {
		data[string(kvs.Key)] = kvs.Value
	}
	return data, resp.Header.Revision, nil
}

// Set implements store.Writer interface.
func (es *Store) Set(key string, value []byte, revision int64) (int64, error) {
	var err error
	key = normalizeKey(key)
	var resp *clientv3.TxnResponse
	txn := es.client.Txn(es.client.Ctx())
	if revision < 0 {
		resp, err = txn.Then(
			clientv3.OpPut(key, string(value)),
			clientv3.OpPut(globalRevisionKey, ""),
		).Commit()
	} else {
		resp, err = es.client.Txn(es.client.Ctx()).If(
			clientv3.Compare(clientv3.ModRevision(globalRevisionKey), "<", revision+1)).Then(
			clientv3.OpPut(key, string(value)),
			clientv3.OpPut(globalRevisionKey, ""),
		).Commit()
	}
	if err != nil {
		return -1, err
	}
	outRevision := resp.Header.Revision
	if !resp.Succeeded {
		return outRevision, &store.RevisionMismatchError{
			Key:              key,
			ExpectedRevision: revision,
			ActualRevision:   outRevision,
		}
	}
	return outRevision, nil
}

// Delete implements store.Writer interface.
func (es *Store) Delete(key string) (int64, error) {
	key = normalizeKey(key)
	resp, err := es.client.Txn(es.client.Ctx()).Then(
		clientv3.OpPut(globalRevisionKey, ""),
		clientv3.OpDelete(key),
	).Commit()
	return resp.Header.Revision, err
}

// Watch implements store.Watcher interface.
func (es *Store) Watch(ctx context.Context, key string, revision int64) (<-chan store.Event, error) {
	key = normalizeKey(key)
	c := make(chan store.Event, watchBufSize)
	go func() {
		for resp := range es.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithRev(revision)) {
			for _, ev := range resp.Events {
				sev := store.Event{
					Revision: resp.Header.Revision,
					Key:      string(ev.Kv.Key),
				}
				if ev.Type == mvccpb.PUT {
					sev.Type = store.PUT
					sev.Value = ev.Kv.Value
					if ev.PrevKv != nil {
						sev.PreviousValue = ev.PrevKv.Value
					}
				} else {
					sev.Type = store.DELETE
				}
				c <- sev
			}
		}
		close(c)
	}()
	return c, nil
}

func newStore(u *url.URL) (store.Store, error) {
	origScheme := u.Scheme
	u.Scheme = "http"
	cfg := clientv3.Config{Endpoints: []string{u.String()}}
	u.Scheme = origScheme
	if u.User != nil {
		cfg.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			cfg.Password = password
		}
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Store{client, u}, nil
}

// Register registers etcd scheme as the store backend.
func Register(m map[string]store.Builder) {
	m["etcd"] = newStore
}
