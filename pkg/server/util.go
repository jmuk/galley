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

	configpb "istio.io/api/config/v1"
	"istio.io/galley/pkg/store"
)

func buildPath(meta *configpb.Meta) string {
	var paths = []string{meta.ApiGroup, meta.ApiGroupVersion, meta.ObjectType, meta.ObjectGroup}
	if len(meta.Name) != 0 {
		paths = append(paths, meta.Name)
	}
	return "/" + strings.Join(paths, "/")
}

func pathToMeta(path string) (*configpb.Meta, error) {
	if path[0] != '/' {
		return nil, fmt.Errorf("illformed path %s", path)
	}
	paths := strings.Split(path[1:], "/")
	if len(paths) < 5 {
		return nil, fmt.Errorf("insufficient path components: %s", path)
	}
	return &configpb.Meta{
		ApiGroup:        paths[0],
		ApiGroupVersion: paths[1],
		ObjectType:      paths[2],
		ObjectGroup:     paths[3],
		Name:            strings.Join(paths[4:], "/"),
	}, nil
}

func buildObject(data string, meta *configpb.Meta, incl *configpb.ObjectFieldInclude) (obj *configpb.Object, err error) {
	src := &structpb.Struct{}
	err = jsonpb.UnmarshalString(data, src)
	if err != nil {
		return nil, err
	}
	obj = &configpb.Object{Meta: meta}
	if incl != nil && incl.SourceData {
		obj.SourceData = src
	}
	if incl != nil && incl.Data {
		glog.Infof("data is requested, but not supported yet")
	}
	return obj, nil
}

func readKvsToObjects(kvs store.KeyValueStore, prefix string, incl *configpb.ObjectFieldInclude) (objs []*configpb.Object, revision int64, err error) {
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
		var obj *configpb.Object
		if incl != nil && (incl.SourceData || incl.Data) {
			value, gindex, found := kvs.Get(k)
			if !found {
				glog.Warningf("not found: %s", k)
				continue
			}
			obj, err = buildObject(value, m, incl)
			if err != nil {
				glog.Warningf("error on fetching the content for %s: %v", k, err)
				continue
			}
			obj.Meta.Revision = int64(gindex)
		} else {
			obj = &configpb.Object{Meta: m}
			obj.Meta.Revision = int64(index)
		}
		objs = append(objs, obj)
	}
	return objs, int64(index), err
}
