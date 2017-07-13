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

	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"

	galleypb "istio.io/galley/api/galley/v1"
)

// nolint: deadcode
const testConfig = `
scope: shipping.FQDN
name: service.cfg
config:
  - type: constructor
    name: request_count
    spec:
      labels:
        dc: target.data_center
        service: target.service
      value: request.size
  - type: handler
    name: mystatsd
    spec:
      impl: istio.io/statsd
      params:
        host: statshost.FQDN
        port: 9080
  - type: rule
    spec:
      handler: $mystatsd
      instances:
      - $request_count
      selector: target.service == "shipping.FQDN"
  - type: constructor
    name: deny_source_ip
    spec:
      value: request.source_ip
  - type: rule
    spec:
      handler: $mesh.denyhandler
      instances:
      - $deny_source_ip
      selector: target.service == "shipping.FQDN" && source.labels["app"] != "billing"
## Proxy rules
  - type: route-rule
    spec:
      destination: billing.FQDN
      source: shipping.FQDN
      match:
        httpHeaders:
          cookie:
            regex: "^(.*?;)?(user=test-user)(;.*)?$"
      route:
      - tags:
          version: v1
        weight: 100
      httpFault:
        delay:
          percent: 5
          fixedDelay: 2s
  - type: route-rule
    spec:
      destination: shipping.FQDN
      match:
        httpHeaders:
          cookie:
            regex: "^(.*?;)?(user=test-user)(;.*)?$"
      route:
      - tags:
          version: v1
        weight: 90
      - tags:
          version: v2
        weight: 10
  - type: route-rule
    spec:
      destination: shipping.FQDN
      route:
      - tags:
          version: v1
        weight: 100
`

// nolint: deadcode
func newConfigFileForTest(fileContent string) (*galleypb.ConfigFile, []byte, error) {
	configFile := &galleypb.ConfigFile{}
	jsonData, err := yaml.YAMLToJSON([]byte(fileContent))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert into json: %v", err)
	}
	if err = jsonpb.Unmarshal(bytes.NewBuffer(jsonData), configFile); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal: %v", err)
	}
	configBytes, err := proto.Marshal(configFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal: %v", err)
	}
	return configFile, configBytes, nil
}
