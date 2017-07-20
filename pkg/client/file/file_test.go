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

package file

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/struct"

	galleypb "istio.io/galley/api/galley/v1"
)

func TestPartialDecode(t *testing.T) {
	wantName := "com.example"
	wantScope := "service.cfg"
	wantContent := map[string]interface{}{
		"scope": wantScope,
		"name":  wantName,
		"config": []map[string]interface{}{
			{
				"type": "route-rule",
				"spec": "...",
			},
		},
	}

	wantContentJSON, err := json.Marshal(&wantContent)
	if err != nil {
		t.Fatalf("could not create JSON test content: %v", err)
	}
	wantContentYAML, err := yaml.Marshal(&wantContent)
	if err != nil {
		t.Fatalf("could not create YAML test content: %v", err)
	}
	var pb structpb.Struct
	if err = jsonpb.UnmarshalString(string(wantContentJSON), &pb); err != nil {
		t.Fatalf("could not create temp protobuf for ProtoText test content: %v", err)
	}
	wantContentProto, err := proto.Marshal(&pb)
	if err != nil {
		t.Fatalf("could not create ProtoText test content: %v", err)
	}

	cases := []struct {
		name      string
		content   []byte
		ctype     galleypb.ContentType
		wantCtype galleypb.ContentType

		wantErr bool
	}{
		{
			name:      "JSON as JSON",
			content:   wantContentJSON,
			ctype:     galleypb.ContentType_JSON,
			wantCtype: galleypb.ContentType_JSON,
		},
		{
			name:      "JSON as YAML",
			content:   wantContentJSON,
			ctype:     galleypb.ContentType_YAML,
			wantCtype: galleypb.ContentType_YAML,
		},
		{
			name:    "JSON as PROTO",
			content: wantContentJSON,
			ctype:   galleypb.ContentType_PROTO_TEXT,
			wantErr: true,
		},
		{
			name:      "JSON as UNKNOWN",
			content:   wantContentJSON,
			ctype:     galleypb.ContentType_UNKNOWN,
			wantCtype: galleypb.ContentType_JSON,
		},
		{
			name:      "YAML as JSON",
			content:   wantContentYAML,
			ctype:     galleypb.ContentType_JSON,
			wantCtype: galleypb.ContentType_JSON,
			wantErr:   true,
		},
		{
			name:      "YAML as YAML",
			content:   wantContentYAML,
			ctype:     galleypb.ContentType_YAML,
			wantCtype: galleypb.ContentType_YAML,
		},
		{
			name:    "YAML as PROTO",
			content: wantContentYAML,
			ctype:   galleypb.ContentType_PROTO_TEXT,
			wantErr: true,
		},
		{
			name:      "YAML as UNKNOWN",
			content:   wantContentYAML,
			ctype:     galleypb.ContentType_UNKNOWN,
			wantCtype: galleypb.ContentType_YAML,
		},
		{
			name:    "PROTO as JSON",
			content: wantContentProto,
			ctype:   galleypb.ContentType_JSON,
			wantErr: true,
		},
		{
			name:    "PROTO as YAML",
			content: wantContentProto,
			ctype:   galleypb.ContentType_YAML,
			wantErr: true,
		},
		{
			name:      "PROTO as PROTO",
			content:   wantContentProto,
			ctype:     galleypb.ContentType_PROTO_TEXT,
			wantCtype: galleypb.ContentType_PROTO_TEXT,
		},
		{
			name:      "PROTO as UNKNOWN",
			content:   wantContentProto,
			ctype:     galleypb.ContentType_UNKNOWN,
			wantCtype: galleypb.ContentType_PROTO_TEXT,
		},
	}

	for _, c := range cases {
		gotFile, gotErr := PartialDecode(c.content, c.ctype)
		expected := gotErr != nil
		if expected != c.wantErr {
			t.Errorf("%v: wrong error value: got %v want %v", c.name, expected, c.wantErr)
		}
		if gotErr != nil {
			continue
		}
		if gotFile.Name != wantName {
			t.Errorf("%v: wrong name  got %v want %v", c.name, gotFile.Name, wantName)
		}
		if gotFile.Scope != wantScope {
			t.Errorf("%v: wrong scope: got %v want %v", c.name, gotFile.Scope, wantScope)
		}
		if !reflect.DeepEqual(gotFile.Contents, c.content) {
			t.Errorf("%v: wrong contents: got %v want %v", c.name, gotFile.Contents, c.content)
		}
		if gotFile.ContentType != c.wantCtype {
			t.Errorf("%v: wrong content-type: got %v want %v", c.name, gotFile.ContentType, c.wantCtype)
		}
	}
}
