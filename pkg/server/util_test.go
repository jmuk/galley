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
	"fmt"
	"reflect"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/proto"

	galleypb "istio.io/galley/api/galley/v1"
)

func TestNewConfigFile(t *testing.T) {
	yamlConfig := []byte(testConfig)
	jsonConfig, err := yaml.YAMLToJSON(yamlConfig)
	if err != nil {
		t.Fatalf("failed to convert yaml data: %v", err)
	}
	configFile, _, err := newConfigFileForTest(testConfig)
	if err != nil {
		t.Fatalf("failed to convert get the config file: %v", err)
	}
	textConfig := []byte(proto.MarshalTextString(configFile))
	for _, cc := range []struct {
		msg    string
		source []byte
		ctype  galleypb.ContentType
		ok     bool
	}{
		{"yaml", yamlConfig, galleypb.ContentType_UNKNOWN, true},
		{"yaml", yamlConfig, galleypb.ContentType_YAML, true},
		{"yaml", yamlConfig, galleypb.ContentType_JSON, false},
		{"yaml", yamlConfig, galleypb.ContentType_PROTO_TEXT, false},
		{"json", jsonConfig, galleypb.ContentType_UNKNOWN, true},
		{"json", jsonConfig, galleypb.ContentType_JSON, true},
		{"json", jsonConfig, galleypb.ContentType_PROTO_TEXT, false},
		{"proto", textConfig, galleypb.ContentType_UNKNOWN, true},
		{"proto", textConfig, galleypb.ContentType_YAML, false},
		{"proto", textConfig, galleypb.ContentType_JSON, false},
		{"proto", textConfig, galleypb.ContentType_PROTO_TEXT, true},
	} {
		t.Run(fmt.Sprintf("%s/%s", cc.msg, cc.ctype), func(tt *testing.T) {
			result, err := newConfigFile(cc.source, cc.ctype)
			succeeded := err == nil
			if cc.ok != succeeded {
				tt.Errorf("got %v, want %v (error: %v)", succeeded, cc.ok, err)
			}
			if cc.ok && !reflect.DeepEqual(result, configFile) {
				tt.Errorf("got %+v, want %+v", result, configFile)
			}
		})
	}
}
