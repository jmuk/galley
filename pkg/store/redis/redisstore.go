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

// Package redis provides the Redis implementation of store interfaces.
package redis

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/redis"

	"istio.io/galley/pkg/store"
)

const (
	// The name of the config for redis to store the 'keyspace events' availability.
	keyspaceEventsConfigKey = "notify-keyspace-events"

	// The timeout for receiving the next keyspace event (i.e. the next change on the DB).
	subscriberTimeout = 200 * time.Millisecond
)

type subscriber struct {
	client *pubsub.SubClient

	lastIndex int
	changes   []store.Change
	mu        sync.Mutex
	running   bool
}

type redisStore struct {
	client *redis.Client

	// The URL connecting to the database.
	url *url.URL

	// listLength caches the number of keys returned for List() to
	// reduce the number of allocations for similar quries.
	listLength int
}

// doesConfigSupportsChangeNotifications returns true when the passed string contains the
// wanted value described in https://redis.io/topics/notifications#configuration
func doesConfigSupportsChangeNotifications(conf string) bool {
	// this use "keyevent" notifications.
	if !strings.Contains(conf, "E") {
		return false
	}
	return strings.Contains(conf, "A") || (strings.Contains(conf, "$") && strings.Contains(conf, "g"))
}

// getKeyspaceAvailability checks the keyspace events config and returns true when
// the configuration indicates that it can receive events for changes.
// See https://redis.io/topics/notifications for the details.
func (rs *redisStore) getKeyspaceAvailability() bool {
	resp := rs.client.Cmd("CONFIG", "GET", keyspaceEventsConfigKey)
	if resp.Err != nil {
		return false
	}
	confs, err := resp.Array()
	if err != nil {
		return false
	}
	conf, err := confs[1].Str()
	if err != nil {
		return false
	}
	return doesConfigSupportsChangeNotifications(conf)
}

// setupConnection sets up the connection to a redis server.
func setupConnection(host, password string, dbNum uint64, timeout time.Duration) (client *redis.Client, err error) {
	if timeout != time.Duration(0) {
		client, err = redis.DialTimeout("tcp", host, timeout)
	} else {
		client, err = redis.Dial("tcp", host)
	}
	if err != nil {
		return nil, fmt.Errorf("can't connect to the redis server %v: %v", host, err)
	}
	if len(password) > 0 {
		resp := client.Cmd("AUTH", password)
		if resp.Err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("failed to authenticate with password %s: %v", password, resp.Err)
		}
	}

	// Invoke PING to make sure the client can emit commands properly.
	if resp := client.Cmd("PING"); resp.Err != nil {
		_ = client.Close()
		return nil, resp.Err
	}

	if dbNum != 0 {
		// SELECT always returns okay, do not have to check the response.
		// See https://redis.io/commands/select
		client.Cmd("SELECT", dbNum)
	}

	return client, nil
}

// newStore creates a new redisStore instance for the given url.
func newStore(u *url.URL) (store.KeyValueStore, error) {
	var dbNum uint64
	if len(u.Path) > 1 {
		var err error
		if dbNum, err = strconv.ParseUint(u.Path[1:], 10, 0); err != nil {
			return nil, fmt.Errorf("failed to parse dbNum \"%s\", it should be an integer", u.Path[1:])
		}
	}

	var password string
	if u.User != nil {
		password, _ = u.User.Password()
	}

	client, err := setupConnection(u.Host, password, dbNum, 0)
	if err != nil {
		return nil, err
	}
	rs := &redisStore{
		client: client,
		url:    u,
	}
	if rs.getKeyspaceAvailability() {
		// TODO: redesign the subscription.
	}
	return rs, nil
}

func (rs *redisStore) String() string {
	return fmt.Sprintf("redisStore: %v", rs.url)
}

// index returns the current index increased by increase.
// If the store does not listen any changes, it will return store.IndexNotSupported.
// Note that the actual index value wouldn't change here, they should be updated
// when the actual change event arrives to the subscriber.
func (rs *redisStore) index(increase int) int {
	return store.IndexNotSupported
}

// Get implements a KeyValueStore method.
func (rs *redisStore) Get(key string) (value string, index int, found bool) {
	resp := rs.client.Cmd("GET", key)
	index = rs.index(0)
	if resp.Err != nil {

		return "", index, false
	}
	s, err := resp.Str()
	if err != nil {
		return "", index, false
	}
	return s, index, true
}

// Set implements a KeyValueStore method.
func (rs *redisStore) Set(key, value string) (index int, err error) {
	index = rs.index(1)
	resp := rs.client.Cmd("SET", key, value)
	if resp.Err != nil {
		return index, resp.Err
	}
	return index, nil
}

// List implements a KeyValueStore method.
func (rs *redisStore) List(key string, recurse bool) (keys []string, index int, err error) {
	index = rs.index(0)
	keys = make([]string, 0, rs.listLength)
	keyPattern := key
	if key[len(key)-1] != '/' {
		keyPattern += "/"
	}
	keyPattern += "*"
	cursor := 0
	for {
		resp := rs.client.Cmd("SCAN", cursor, "MATCH", keyPattern)
		if resp.Err != nil {
			err = resp.Err
			break
		}
		resps, rerr := resp.Array()
		if rerr != nil {
			err = rerr
			break
		}
		if nextCursor, cerr := resps[0].Int(); cerr != nil {
			err = cerr
			break
		} else {
			cursor = nextCursor
		}
		respKeys, aerr := resps[1].Array()
		if aerr != nil {
			err = aerr
			break
		}
		for i, rk := range respKeys {
			// TODO: check recurse flag for filitering keys.
			if key, err2 := rk.Str(); err2 != nil {
				glog.Warningf("illformed responses %d-th value for cursor %d isn't a string (%v)", i, cursor, rk)
				continue
			} else {
				keys = append(keys, key)
			}
		}
		if cursor == 0 {
			break
		}
	}
	if err == nil {
		rs.listLength = len(keys)
	}
	return keys, index, err
}

// Delete implements a KeyValueStore method.
func (rs *redisStore) Delete(key string) (err error) {
	return rs.client.Cmd("DEL", key).Err
}

// Close implements a KeyValueStore method.
func (rs *redisStore) Close() {
	if err := rs.client.Close(); err != nil {
		glog.Warningf("failed to close the connection: %v", err)
	}
}

// listen initiates the subscription of the changes, and starts a goroutine to listen updates.
func (sub *subscriber) listen(dbNum uint64) error {
	pattern := fmt.Sprintf("__keyevent@%d__:*", dbNum)
	resp := sub.client.PSubscribe(pattern)
	if resp.Err != nil {
		if err := sub.client.Client.Close(); err != nil {
			glog.Warningf("failed to close the subscriber client: %v", err)
		}
		return resp.Err
	}
	sub.running = true
	go func() {
		for {
			resp = sub.client.Receive()
			if !sub.running {
				break
			}
			if resp.Type == pubsub.Error {
				if resp.Timeout() {
					continue
				}
				glog.Warningf("unexpected error on subscription: %v", resp.Err)
				break
			}
			if resp.Type == pubsub.Message {
				// TBD
			}
		}
		sub.client.PUnsubscribe(pattern)
		_ = sub.client.Client.Close()
		sub.client = nil
	}()
	return nil
}

// Close finishes the subscription.
func (sub *subscriber) Close() {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.client == nil || !sub.running {
		return
	}
	sub.running = false
}

// Register registers this module as a config store.
// Do not use 'init()' for automatic registration; linker will drop
// the whole module because it looks unused.
func Register(m map[string]store.Builder) {
	m["redis"] = newStore
}
