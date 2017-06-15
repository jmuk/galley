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

func TestEtcdWatch(t *testing.T) {
	testutil.RunWatcherTest(t, testManagerBuilder)
}
