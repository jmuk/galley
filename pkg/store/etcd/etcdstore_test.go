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
	"net/url"
	"os"
	"testing"

	"istio.io/galley/pkg/store/testutil"
)

var u *url.URL

func TestMain(m *testing.M) {
	// Currently it can only test with an actual etcd server.
	// The test main assumes the server URL is specified as
	// the ETCD_SERVER environment variable.
	// TODO: create the way to set up a (mock) etcd server.
	serverURL := os.Getenv("ETCD_SERVER")
	if serverURL == "" {
		os.Exit(0)
	}
	var err error
	u, err = url.Parse(serverURL)
	if err != nil {
		os.Exit(1)
	}
	u.Scheme = "etcd"
	os.Exit(m.Run())
}

func testManagerBuilder() (*testutil.TestManager, error) {
	es, err := newKeyValue(u)
	if err != nil {
		return nil, err
	}
	return testutil.NewTestManager(es, nil), nil
}

func TestEtcdStore(t *testing.T) {
	testutil.RunStoreTest(t, testManagerBuilder)
}

func TestEtcdOptimisticConcurrency(t *testing.T) {
	testutil.RunOptimisticConcurrency(t, testManagerBuilder)
}

func TestEtcdWatch(t *testing.T) {
	testutil.RunWatcherTest(t, testManagerBuilder)
}
