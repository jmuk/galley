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

// Package config provides client configuration support for Galley's
// end-user tooling, i.e. istioctl. Common user configuration options
// can be stored in a known local file and can be managed
// independently for multiple installations of Istio. User
// configuration includes the address of Galley and user
// credentials/certificates to authenticate to Galley. It does not
// include Istio configuration objects.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	"github.com/ghodss/yaml"
)

// TODO: how do we manage k8s namespace for istio components,
// e.g. istio-system? This could be encoded in the `Server` address
// itself or maintained as a separate field.

// Context represents a specific instance of user context for an
// instance of Galley.
type Context struct {
	// Address of Galley.
	Server string `json:"server"`

	// TODO: Embed user/auth info in context or create separate 'user'
	// and 'cluster' structs which 'context' binds together a. la
	// kubectl?
}

// Config represents the in-memory version of the end users
// configuration. This is typically loaded from a file,
// e.g. ~/.istio/config.
type Config struct {
	// Contexts contains a map of all user-defined configuration
	// contexts keyed by the context name.
	Contexts map[string]*Context `json:"contexts"`

	// CurrentContext indicates the active context currently selected
	// by the user.
	CurrentContext string `json:"current_context"`

	// Used to save user updated settings. Not serialized.
	origin string
}

// Current returns the current user configuration context.
func (c *Config) Current() *Context {
	return c.Contexts[c.CurrentContext]
}

// UseContext switches the current context. Returns an error if the
// specified context is not found.
// TODO: should this add a new empty context instead of returning an
// error if context was not found?
func (c *Config) UseContext(current string) error {
	if _, ok := c.Contexts[current]; !ok {
		return fmt.Errorf("%v does not exist", current)
	}
	c.CurrentContext = current
	return nil
}

const (
	// PathEnvVar is an environment variable which allows the user to
	// override the default user configuration file.
	PathEnvVar = "ISTIOCONFIG"

	// PathHomeDir is the default home directory for Istio specific
	// configuration files relative to the user's $HOME directory.
	PathHomeDir = ".istio"

	// PathFileName is the default user configuration file relative to
	// PathHomeDir. The default user configuration filename is a
	// concatenation of $HOME, PathHomeDir, and PathFileName.
	PathFileName = "config"
)

// load loads a user configuration from an io.Reader into the
// in-memory representation.
func load(in io.Reader, origin string) (*Config, error) {
	b, err := ioutil.ReadAll(in)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Contexts == nil {
		c.Contexts = map[string]*Context{}
	}
	if c.CurrentContext == "" {
		if len(c.Contexts) == 1 {
			for name := range c.Contexts {
				c.CurrentContext = name
			}
		}
	} else if _, ok := c.Contexts[c.CurrentContext]; !ok {
		c.Contexts[c.CurrentContext] = &Context{}
	}
	c.origin = origin
	return &c, nil
}

// LoadFromFile loads a user configuration from a file.
func LoadFromFile(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return load(file, filename)
}

// LoadFromString loads a user configuration from the input string.
func LoadFromString(in string) (*Config, error) {
	return load(bytes.NewReader([]byte(in)), "")
}

// UserFilename returns the filename of the user configuration file.
func UserFilename() (string, error) {
	if name := os.Getenv(PathEnvVar); name != "" {
		return name, nil
	}
	cur, err := user.Current()
	if err != nil {
		return "", err
	}
	return filepath.Join(cur.HomeDir, PathHomeDir, PathFileName), nil
}

// LoadFromDefault loads the default user configuration.
func LoadFromDefault() (*Config, error) {
	filename, err := UserFilename()
	if err != nil {
		return nil, err
	}
	return LoadFromFile(filename)
}

const defaultCurrentContext = "default"

// New creates a new instance of a configuration object based on the
// default user filename. This can be subsequently updated and saved
// to create the default user configuration.
func New() (*Config, error) {
	filename, err := UserFilename()
	if err != nil {
		return nil, err
	}
	return &Config{
		CurrentContext: defaultCurrentContext,
		Contexts: map[string]*Context{
			defaultCurrentContext: {},
		},
		origin: filename,
	}, nil
}

// Save saves the in-memory user configuration to the originating
// file.
func (c *Config) Save() error {
	if c.origin == "" {
		return errors.New("no origin specified")
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	mode := os.FileMode(0644)
	info, err := os.Stat(c.origin)
	if os.IsExist(err) {
		mode = info.Mode()
	}
	return ioutil.WriteFile(c.origin, data, mode)
}
