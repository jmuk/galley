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
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/struct"

	galleypb "istio.io/galley/api/galley/v1"
)

// File provides a container for common file request and response
// operations as described by istio.galley.v1.File services.
type File struct {
	Path        string
	Revision    int64
	Contents    []byte
	ContentType galleypb.ContentType

	// Name and Scope are extracted from the decoded contents
	Name  string
	Scope string
}

func (f File) String() string {
	return fmt.Sprintf("{Path: %v, Revision: %v ContentType: %v Contents: %v}",
		f.Path,
		f.Revision,
		f.ContentType,
		string(f.Contents),
	)
}

// structure to allow for partial decoding of file contents to
// discover name and scope.
type partialConfigFile struct {
	Name   string        `json:"name"`
	Scope  string        `json:"scope"`
	Config []interface{} `json:"config"`
}

func loadFileAsJSON(in []byte) (*partialConfigFile, error) {
	var pcf partialConfigFile
	if err := json.Unmarshal(in, &pcf); err != nil {
		return nil, err
	}
	return &pcf, nil
}

func loadFileAsYAML(in []byte) (*partialConfigFile, error) {
	var pcf partialConfigFile
	if err := yaml.Unmarshal(in, &pcf); err != nil {
		return nil, err
	}
	return &pcf, nil
}

func loadFileAsProto(in []byte) (*partialConfigFile, error) {
	var pb structpb.Struct
	if err := proto.Unmarshal(in, &pb); err != nil {
		return nil, err
	}
	return &partialConfigFile{
		Name:  pb.GetFields()["name"].GetStringValue(),
		Scope: pb.GetFields()["scope"].GetStringValue(),
	}, nil
}

// PartialDecode performs a partial decoding of the user specified
// configuration file. It returns a file container which includes the
// decoded name and scope along with the detected content-type.
func PartialDecode(contents []byte, ctype galleypb.ContentType) (*File, error) {
	var pcf *partialConfigFile
	var err error
	switch ctype {
	case galleypb.ContentType_JSON:
		if pcf, err = loadFileAsJSON(contents); err != nil {
			return nil, fmt.Errorf("cannot decode as json: %v", err)
		}
	case galleypb.ContentType_YAML:
		if pcf, err = loadFileAsYAML(contents); err != nil {
			return nil, fmt.Errorf("cannot decode as yaml: %v", err)
		}
	case galleypb.ContentType_PROTO_TEXT:
		if pcf, err = loadFileAsProto(contents); err != nil {
			return nil, fmt.Errorf("cannot decode as prototext: %v", err)
		}
	default:
		// Guess the content-type. The order is important here. YAML
		// is a superset of JSON so try JSON first.
		if pcf, err = loadFileAsJSON(contents); err == nil {
			ctype = galleypb.ContentType_JSON
			break
		}
		if pcf, err = loadFileAsYAML(contents); err == nil {
			ctype = galleypb.ContentType_YAML
			break
		}
		if pcf, err = loadFileAsProto(contents); err == nil {
			ctype = galleypb.ContentType_PROTO_TEXT
			break
		}
		return nil, errors.New("unknown content type: use --ctype or specify path explicitly with --path")
	}
	return &File{
		Name:        pcf.Name,
		Scope:       pcf.Scope,
		Contents:    contents,
		ContentType: ctype,
	}, nil
}

// PartialDecodeFromFilename is a helper function to partially decode a user
// specified configuration file based on its name.
func PartialDecodeFromFilename(filename string, ctype galleypb.ContentType) (*File, error) {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return PartialDecode(contents, ctype)
}
