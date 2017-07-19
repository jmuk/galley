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
	"fmt"
	"io"
	"io/ioutil"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	galleypb "istio.io/galley/api/galley/v1"
)

// rawDecoder implements gw.Decoder which does nothing to decode.
// Instead it copies the input bytes into the bytes or string field.
type rawDecoder struct {
	r io.Reader
}

func (d *rawDecoder) Decode(v interface{}) error {
	var err error
	switch out := v.(type) {
	case *[]byte:
		*out, err = ioutil.ReadAll(d.r)
		return err
	case *string:
		var bs []byte
		bs, err = ioutil.ReadAll(d.r)
		if err != nil {
			return err
		}
		*out = string(bs)
		return nil
	}
	return fmt.Errorf("type mismatch for input %v", v)
}

// contentsEncoder implements gw.Encoder which prints out the contents
// field only. It falls back to the jsonpb decoder if the input type is not a File.
type contentsEncoder struct {
	w      io.Writer
	jsonpb *runtime.JSONPb
}

func (d *contentsEncoder) Encode(v interface{}) error {
	f, ok := v.(*galleypb.File)
	if !ok {
		return d.jsonpb.NewEncoder(d.w).Encode(v)
	}
	// TODO: maybe we should format the contents among yaml<->json
	// based on the file contents and the response content type.
	for written := 0; written < len(f.Contents); {
		n, err := d.w.Write(f.Contents[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

// RawMarshaler does not encode/decode. Instead it handles
// the raw request/response body to certain messages.
// With this, the grpc gateway will accept plain contents
// for CreateFile/UpdateFile, also GetFile returns the raw
// file contents only.
type RawMarshaler struct {
	jsonpb *runtime.JSONPb
	ctype  string
}

// ContentType implements gw.Marshaler interface
func (m *RawMarshaler) ContentType() string {
	return m.ctype
}

// Unmarshal implements gw.Marshaler interface
func (m *RawMarshaler) Unmarshal(data []byte, v interface{}) error {
	return m.NewDecoder(bytes.NewBuffer(data)).Decode(v)
}

// Marshal implements gw.Marshaler interface.
func (m *RawMarshaler) Marshal(v interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := m.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// NewDecoder implements gw.Marshaler interface
func (m *RawMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return &rawDecoder{r}
}

// NewEncoder implements gw.Marshaler interface
func (m *RawMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return &contentsEncoder{w, m.jsonpb}
}
