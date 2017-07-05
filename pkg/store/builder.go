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
	"fmt"
	"net/url"
)

// Builder is the type to build a new Store from a URL.
type Builder func(*url.URL) (Store, error)

// RegisterFunc is the function to register a builder to a URL scheme.
type RegisterFunc func(m map[string]Builder)

// Registrar holds the current registries of the builders.
type Registrar struct {
	builders map[string]Builder
}

// NewRegistrar creates a new registrar for the given register functions.
func NewRegistrar(funcs []RegisterFunc) *Registrar {
	r := &Registrar{map[string]Builder{}}
	for _, rf := range funcs {
		rf(r.builders)
	}
	return r
}

// NewStore creates a Store to the given URL string.
func (r *Registrar) NewStore(ustr string) (Store, error) {
	u, err := url.Parse(ustr)
	if err != nil {
		return nil, err
	}
	if builder, ok := r.builders[u.Scheme]; ok {
		return builder(u)
	}
	return nil, fmt.Errorf("unknown URL scheme %s", ustr)
}
