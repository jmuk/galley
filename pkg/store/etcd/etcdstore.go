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
	"fmt"
	"net/url"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"

	"istio.io/galley/pkg/store"
)

// KeyValue implements store.KeyValue for etcd.
type KeyValue struct {
	client *clientv3.Client
	u      *url.URL
}

// String implements a fmt.Stringer method.
func (es *KeyValue) String() string {
	return es.u.String()
}

// Close implements an io.Closer method.
func (es *KeyValue) Close() error {
	return es.client.Close()
}

// Get implements a store.Reader method.
func (es *KeyValue) Get(key string) (value []byte, revision int64, found bool) {
	resp, err := es.client.Get(es.client.Ctx(), key)
	if err != nil {
		return value, 0, false
	}
	revision = resp.Header.Revision
	for _, kvs := range resp.Kvs {
		if string(kvs.Key) == key {
			return kvs.Value, revision, true
		}
	}
	return value, revision, false
}

// List implements a store.Reader method.
func (es *KeyValue) List(prefix string) (data map[string]string, revision int64, err error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	resp, err := es.client.Get(es.client.Ctx(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}
	data = map[string]string{}
	for _, kvs := range resp.Kvs {
		data[string(kvs.Key)] = string(kvs.Value)
	}
	return data, resp.Header.Revision, nil
}

// Set implements a store.Writer method.
func (es *KeyValue) Set(key string, value []byte, revision int64) (outRevision int64, err error) {
	if revision == 0 {
		var resp *clientv3.PutResponse
		resp, err = es.client.Put(es.client.Ctx(), key, string(value))
		if err != nil {
			return -1, err
		}
		outRevision = resp.Header.Revision
	} else {
		var resp *clientv3.TxnResponse
		resp, err = es.client.Txn(es.client.Ctx()).If(
			clientv3.Compare(clientv3.ModRevision(key), "<", revision+1)).Then(
			clientv3.OpPut(key, string(value))).Commit()
		if err != nil {
			return -1, err
		}
		outRevision = resp.Header.Revision
		if !resp.Succeeded {
			return outRevision, fmt.Errorf("failed to set: %s is newer than the expected revision %d", key, revision)
		}
	}
	return outRevision, nil
}

// Delete implements a store.Writer method.
func (es *KeyValue) Delete(key string) error {
	_, err := es.client.Delete(es.client.Ctx(), key)
	return err
}

// Watch implements a store.Watcher method.
func (es *KeyValue) Watch(ctx context.Context, key string, revision int64) (<-chan store.Event, error) {
	c := make(chan store.Event)
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

func newKeyValue(u *url.URL) (store.KeyValue, error) {
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
	return &KeyValue{client, u}, nil
}

// Register registers etcd scheme as the store backend.
func Register(m map[string]store.Builder) {
	m["etcd"] = newKeyValue
}
