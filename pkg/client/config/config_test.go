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

package config

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

var (
	caseEmptyYAML   = ``
	caseEmptyConfig = Config{
		Contexts: map[string]*Context{},
	}

	caseCurrentContextOnlyYAML = `
current_context: default
`
	caseCurrentContextOnlyConfig = Config{
		CurrentContext: "default",
		Contexts: map[string]*Context{
			"default": {},
		},
	}

	caseSingleContextYAML = `
current_context: default
contexts:
  default:
    server: galley-address:80
`
	caseSingleContextConfig = Config{
		CurrentContext: "default",
		Contexts: map[string]*Context{
			"default": {Server: "galley-address:80"},
		},
	}

	caseSingleContextMissingCurrentYAML = `
contexts:
  default:
    server: galley-address:80
`
	caseSingleContextMissingCurrentConfig = Config{
		CurrentContext: "default",
		Contexts: map[string]*Context{
			"default": {Server: "galley-address:80"},
		},
	}

	caseMultipleContextsYAML = `
current_context: foobar
contexts:
  default:
    server: galley-address:80
  foobar:
    server: foobar-galley-address:80
`
	caseMultipleContextsConfig = Config{
		CurrentContext: "foobar",
		Contexts: map[string]*Context{
			"default": {Server: "galley-address:80"},
			"foobar":  {Server: "foobar-galley-address:80"},
		},
	}

	caseMultipleContextsCurrentMismatchYAML = `
current_context: baz
contexts:
  default:
    server: galley-address:80
  foobar:
    server: foobar-galley-address:80
`
	caseMultipleContextsCurrentMismatchConfig = Config{
		CurrentContext: "baz",
		Contexts: map[string]*Context{
			"baz":     {},
			"default": {Server: "galley-address:80"},
			"foobar":  {Server: "foobar-galley-address:80"},
		},
	}

	caseBadFormattingYAML = `
current_context: default
  default:
    server: galley-address:80
  foobar:
    server: foobar-galley-address:80
`
)

func TestConfigLoad(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantErr bool
		want    *Config
	}{
		{
			name: "empty",
			in:   caseEmptyYAML,
			want: &caseEmptyConfig,
		},
		{
			name: "current context only",
			in:   caseCurrentContextOnlyYAML,
			want: &caseCurrentContextOnlyConfig,
		},
		{
			name: "single context with missing current_context",
			in:   caseSingleContextMissingCurrentYAML,
			want: &caseSingleContextMissingCurrentConfig,
		},
		{
			name: "single context",
			in:   caseSingleContextYAML,
			want: &caseSingleContextConfig,
		},
		{
			name: "multiple contexts",
			in:   caseMultipleContextsYAML,
			want: &caseMultipleContextsConfig,
		},
		{
			name: "multiple context with mismatched current_context",
			in:   caseMultipleContextsCurrentMismatchYAML,
			want: &caseMultipleContextsCurrentMismatchConfig,
		},
		{
			name:    "bad formatting",
			in:      caseBadFormattingYAML,
			wantErr: true,
		},
	}

	for _, c := range cases {
		got, err := LoadFromString(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("%v: succeeded but should have failed", c.name)
			}
			continue
		} else {
			if err != nil {
				t.Errorf("%v: failed: %v", c.name, err)
				continue
			}
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v: incorrect config\n got %#v\nwant %#v ", c.name, got, c.want)
		}
	}
}

func TestConfigLoadFromFile(t *testing.T) {
	file, err := ioutil.TempFile("", "TestConfigLoadFromFile")
	if err != nil {
		t.Fatalf("Failed creating test input file: %v", err)
	}
	filename := file.Name()
	defer os.Remove(filename) // nolint: errcheck
	if _, err = file.WriteString(caseMultipleContextsYAML); err != nil {
		t.Fatalf("Failed writing test input file: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("Failed closing file: %v", err)
	}
	got, err := LoadFromFile(filename)
	if err != nil {
		t.Fatalf("Failed loading file: %v", err)
	}
	want := caseMultipleContextsConfig
	want.origin = filename
	if !reflect.DeepEqual(got, &want) {
		t.Errorf("Incorrect config\n got %#v\nwant %#v ", got, &want)
	}
}

func TestConfigUseContext(t *testing.T) {
	cfg, err := LoadFromString(caseMultipleContextsYAML)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	cases := []struct {
		name    string
		want    *Context
		use     string
		wantErr bool
	}{
		{
			name: "initial context",
			want: caseMultipleContextsConfig.Contexts["foobar"],
		},
		{
			name: "switch to known context",
			use:  "default",
			want: caseMultipleContextsConfig.Contexts["default"],
		},
		{
			name:    "switch to unknown context",
			use:     "baz",
			want:    caseMultipleContextsConfig.Contexts["default"],
			wantErr: true,
		},
	}

	for _, c := range cases {
		if c.use != "" {
			err := cfg.UseContext(c.use)
			if c.wantErr {
				if err == nil {
					t.Fatalf("%v: UseContext(%v) should have failed but didn't",
						c.name, c.use)
				}
			} else {
				if err != nil {
					t.Fatalf("%v: UseContext(%v) failed: %v",
						c.name, c.use, err)
				}
			}
		}
		if got := cfg.Current(); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%v: bad context, got %v want %v", c.name, got, c.want)
		}
	}
}
