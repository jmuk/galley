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
	"github.com/golang/glog"

	"istio.io/galley/pkg/store"
)

type etcdstore struct {
	client *clientv3.Client
	ctx    context.Context
	u      *url.URL
}

var _ store.KeyValueStore = &etcdstore{}
var _ store.ChangeWatcher = &etcdstore{}

func (es *etcdstore) String() string {
	return es.u.String()
}

func (es *etcdstore) Close() {
	err := es.client.Close()
	if err != nil {
		glog.Warningf("failed on closing connection: %v", err)
	}
}

func (es *etcdstore) Get(key string) (value string, index int, found bool) {
	resp, err := es.client.Get(es.ctx, key)
	if err != nil {
		return "", 0, false
	}
	index = int(resp.Header.Revision)
	for _, kvs := range resp.Kvs {
		if string(kvs.Key) == key {
			return string(kvs.Value), index, true
		}
	}
	return "", index, false
}

func (es *etcdstore) Set(key, value string) (index int, err error) {
	resp, err := es.client.Put(es.ctx, key, value)
	if err != nil {
		return 0, err
	}
	return int(resp.Header.Revision), nil
}

func (es *etcdstore) List(prefix string, recurse bool) (keys []string, index int, err error) {
	if strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	//endKey := prefix[:len(prefix)-1] + string('/'+1)
	resp, err := es.client.Get(es.ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, 0, err
	}
	for _, kvs := range resp.Kvs {
		keys = append(keys, string(kvs.Key))
	}
	return keys, int(resp.Header.Revision), nil
}

func (es *etcdstore) Delete(key string) error {
	_, err := es.client.Delete(es.ctx, key)
	return err
}

func (es *etcdstore) Watch(prefix string) (<-chan store.ChangeList, context.CancelFunc) {
	c := make(chan store.ChangeList)
	ctx, cancelFunc := context.WithCancel(es.ctx)
	go func() {
		for resp := range es.client.Watch(ctx, prefix, clientv3.WithPrefix()) {
			cl := store.ChangeList{
				Revision: int(resp.Header.Revision),
				Changes:  make([]store.Change, 0, len(resp.Events)),
			}
			for _, ev := range resp.Events {
				change := store.Change{
					Index: int(resp.Header.Revision),
					Key:   string(ev.Kv.Key),
				}
				if ev.Type == mvccpb.PUT {
					change.Type = store.Update
					change.Value = string(ev.Kv.Value)
					if ev.PrevKv != nil {
						change.PrevValue = string(ev.PrevKv.Value)
					}
				} else {
					change.Type = store.Delete
				}
				cl.Changes = append(cl.Changes, change)
			}
			c <- cl
		}
	}()
	return c, cancelFunc
}

func newEtcdstore(u *url.URL) (store.KeyValueStore, error) {
	u.Scheme = "http"
	cfg := clientv3.Config{Endpoints: []string{u.String()}}
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
	ctx := context.Background()
	return &etcdstore{client, ctx, u}, nil
}

// Register registers etcd scheme as the store backend.
func Register(m map[string]store.Builder) {
	m["etcd"] = newEtcdstore
}
