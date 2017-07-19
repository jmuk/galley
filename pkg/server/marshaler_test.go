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

package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	galleypb "istio.io/galley/api/galley/v1"
)

func TestContentType(t *testing.T) {
	for _, ctype := range []string{"text/plain", "text/json", "application/octet-stream", "foobar"} {
		m := &RawMarshaler{
			jsonpb: nil,
			ctype:  ctype,
		}
		if m.ContentType() != ctype {
			t.Errorf("Got: %s, Expected: %s", m.ContentType(), ctype)
		}
	}
}

func newDecoder(input string) runtime.Decoder {
	m := &RawMarshaler{
		jsonpb: &runtime.JSONPb{},
		ctype:  "text/plain",
	}
	return m.NewDecoder(strings.NewReader(input))
}

func TestRawDecoderBytes(t *testing.T) {
	dec := newDecoder("foo bar")
	dst := []byte{}
	err := dec.Decode(&dst)
	if err != nil {
		t.Errorf("Got %v, Expected to succeed", err)
	} else if !bytes.Equal(dst, []byte("foo bar")) {
		t.Errorf("Got %s, Expected foobar", dst)
	}
}

func TestRawDecoderString(t *testing.T) {
	dec := newDecoder("foo bar")
	dst := ""
	err := dec.Decode(&dst)
	if err != nil {
		t.Errorf("Got %v, Expected to succeed", err)
	} else if dst != "foo bar" {
		t.Errorf("Got %s, Expected foobar", dst)
	}
}

func TestRawDecoderFail(t *testing.T) {
	dec := newDecoder(`{"contents": "foobar"}`)
	dst := &galleypb.File{}
	err := dec.Decode(&dst)
	if err == nil {
		t.Errorf("Succeeded, Expected to fail")
	}
}

func TestContentEncoder(t *testing.T) {
	m := &RawMarshaler{
		jsonpb: &runtime.JSONPb{},
		ctype:  "text/plain",
	}
	buf := bytes.NewBuffer(nil)
	enc := m.NewEncoder(buf)
	for _, cc := range []struct {
		msg      string
		input    interface{}
		expected string
	}{
		{
			"file",
			&galleypb.File{Contents: []byte("foo bar"), Revision: 1, Path: "foo/bar"},
			"foo bar",
		},
		{
			"other message",
			&galleypb.ListFilesResponse{
				Entries: []*galleypb.File{
					{Contents: []byte("foo bar"), Revision: 1, Path: "foo/bar"},
					{Contents: []byte("bazz"), Revision: 2, Path: "bazz"},
				},
				NextPageToken: "token",
			},
			`{"entries":[{"path":"foo/bar","contents":"Zm9vIGJhcg==","revision":"1"},{"path":"bazz","contents":"YmF6eg==","revision":"2"}],"nextPageToken":"token"}`,
		},
	} {
		buf.Reset()
		err := enc.Encode(cc.input)
		if err != nil {
			t.Errorf("Got %v, Expected to succeed", err)
		} else if buf.String() != cc.expected {
			t.Errorf("Got %s, Want %s", buf.String(), cc.expected)
		}
	}
}
