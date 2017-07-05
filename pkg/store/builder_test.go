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

package store

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func testingRegister(m map[string]Builder) {
	m["test"] = func(u *url.URL) (Store, error) {
		if u.Host == "" {
			return nil, nil
		}
		return nil, fmt.Errorf("%s is not supported", u.Host)
	}
}

func TestNewStore(t *testing.T) {
	r := NewRegistrar([]RegisterFunc{testingRegister})
	for _, tt := range []struct {
		url string
		err error
	}{
		{"fs:///tmp", errors.New("unknown")},
		{"etcd:///tmp/testdata/configroot", errors.New("unknown")},
		{"/tmp/testdata/configroot", errors.New("unknown")},
		{"test:///test/url", nil},
		{"test://foo/bar", errors.New("foo is not supported")},
	} {
		t.Run(tt.url, func(t *testing.T) {
			_, err := r.NewStore(tt.url)
			if err == tt.err {
				return
			}

			if err != nil {
				if tt.err == nil || !strings.Contains(err.Error(), tt.err.Error()) {
					t.Errorf("got %s\nwant %s", err, tt.err)
				}
			}
		})
	}
}
