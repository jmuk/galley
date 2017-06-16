// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Revision 2.0 (the "License");
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
	"strings"

	"github.com/golang/glog"
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"

	galleypb "istio.io/api/galley/v1"
	"istio.io/galley/pkg/store"
)

func buildPath(meta *galleypb.Meta) string {
	var paths = []string{meta.ApiGroup, meta.ObjectType, meta.ObjectTypeVersion, meta.ObjectGroup}
	if len(meta.Name) != 0 {
		paths = append(paths, meta.Name)
	}
	return "/" + strings.Join(paths, "/")
}

func pathToMeta(path string) (*galleypb.Meta, error) {
	if path[0] != '/' {
		return nil, fmt.Errorf("illformed path %s", path)
	}
	paths := strings.Split(path[1:], "/")
	if len(paths) < 5 {
		return nil, fmt.Errorf("insufficient path components: %s", path)
	}
	return &galleypb.Meta{
		ApiGroup:          paths[0],
		ObjectType:        paths[1],
		ObjectTypeVersion: paths[2],
		ObjectGroup:       paths[3],
		Name:              strings.Join(paths[4:], "/"),
	}, nil
}

func buildObject(data string, meta *galleypb.Meta) (obj *galleypb.Object, err error) {
	src := &structpb.Struct{}
	err = jsonpb.UnmarshalString(data, src)
	if err != nil {
		return nil, err
	}
	obj = &galleypb.Object{Meta: meta}
	obj.SourceData = src
	// TODO: obtain the binary data.
	return obj, nil
}

func readKvsToObjects(kvs store.KeyValueStore, prefix string) (objs []*galleypb.Object, revision int64, err error) {
	keys, index, err := kvs.List(prefix, true)
	if err != nil {
		return nil, 0, err
	}
	for _, k := range keys {
		m, err := pathToMeta(k)
		if err != nil {
			glog.Warningf("error on key: %v", err)
			continue
		}
		var obj *galleypb.Object
		value, gindex, found := kvs.Get(k)
		if !found {
			glog.Warningf("not found: %s", k)
			continue
		}
		obj, err = buildObject(value, m)
		if err != nil {
			glog.Warningf("error on fetching the content for %s: %v", k, err)
			continue
		}
		obj.Meta.Revision = int64(gindex)
		objs = append(objs, obj)
	}
	return objs, int64(index), err
}
