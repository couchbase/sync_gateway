//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channels

import (
	"fmt"
	"strings"

	"github.com/couchbaselabs/walrus"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"

	"github.com/couchbaselabs/sync_gateway/base"
)

const funcWrapper = `
	function(newDoc, oldDoc, userCtx) {
		var v = %s;
		try {
			v(newDoc, oldDoc, userCtx);
		} catch(x) {
			if (x.forbidden)
				reject(403, x.forbidden);
			else if (x.unauthorized)
				reject(401, x.unauthorized);
			else
				throw(x);
		}
	}`

/** Result of running a channel-mapper function. */
type ChannelMapperOutput struct {
	Channels  base.Set
	Roles     AccessMap // roles granted to users via role() callback
	Access    AccessMap
	Rejection error
}

type ChannelMapper struct {
	output   *ChannelMapperOutput
	channels []string
	access   map[string][]string // channels granted to users via access() callback
	roles    map[string][]string // roles granted to users via role() callback
	js       *walrus.JSServer
	Src      string
}

// Maps user names (or role names prefixed with "role:") to arrays of channel or role names
type AccessMap map[string]base.Set

// Calls the function for each user whose access is different between the two AccessMaps
func ForChangedUsers(a, b AccessMap, fn func(user string)) {
	for name, access := range a {
		if !access.Equals(b[name]) {
			fn(name)
		}
	}
	for name, _ := range b {
		if _, existed := a[name]; !existed {
			fn(name)
		}
	}
}

func isOttoArray(value otto.Value) bool {
	return value.Class() == "Array" || value.Class() == "GoArray"
}

// Converts a JS array into a Go string array.
func ottoArrayToStrings(array otto.Value) []string {
	goValue, err := array.Export()
	if err != nil {
		return nil
	}
	if result, ok := goValue.([]string); ok {
		return result
	}
	goArray, ok := goValue.([]interface{})
	if !ok || len(goArray) == 0 {
		return nil
	}
	result := make([]string, 0, len(goArray))
	for _, item := range goArray {
		if str, ok := item.(string); ok {
			result = append(result, str)
		}
	}
	return result
}

func NewChannelMapper(funcSource string) (*ChannelMapper, error) {
	funcSource = fmt.Sprintf(funcWrapper, funcSource)
	mapper := &ChannelMapper{}
	var err error
	mapper.js, err = walrus.NewJSServer(funcSource)
	mapper.Src = funcSource
	if err != nil {
		return nil, err
	}

	// Implementation of the 'channel()' callback:
	mapper.js.DefineNativeFunction("channel", func(call otto.FunctionCall) otto.Value {
		for _, arg := range call.ArgumentList {
			if arg.IsString() {
				mapper.channels = append(mapper.channels, arg.String())
			} else if isOttoArray(arg) {
				array := ottoArrayToStrings(arg)
				if array != nil {
					mapper.channels = append(mapper.channels, array...)
				}
			}
		}
		return otto.UndefinedValue()
	})

	// Implementation of the 'access()' callback:
	mapper.js.DefineNativeFunction("access", func(call otto.FunctionCall) otto.Value {
		return mapper.addValueForUser(call.Argument(0), call.Argument(1), mapper.access)
	})

	// Implementation of the 'role()' callback:
	mapper.js.DefineNativeFunction("role", func(call otto.FunctionCall) otto.Value {
		return mapper.addValueForUser(call.Argument(0), call.Argument(1), mapper.roles)
	})

	// Implementation of the 'reject()' callback:
	mapper.js.DefineNativeFunction("reject", func(call otto.FunctionCall) otto.Value {
		if mapper.output.Rejection == nil {
			if status, err := call.Argument(0).ToInteger(); err == nil && status >= 400 {
				var message string
				if len(call.ArgumentList) > 1 {
					message = call.Argument(1).String()
				}
				mapper.output.Rejection = &base.HTTPError{int(status), message}
			}
		}
		return otto.UndefinedValue()
	})

	mapper.js.Before = func() {
		mapper.output = &ChannelMapperOutput{}
		mapper.channels = []string{}
		mapper.access = map[string][]string{}
		mapper.roles = map[string][]string{}
	}
	mapper.js.After = func(result otto.Value, err error) (interface{}, error) {
		output := mapper.output
		mapper.output = nil
		if err == nil {
			output.Channels, err = SetFromArray(mapper.channels, ExpandStar)
			if err == nil {
				output.Access, err = compileAccessMap(mapper.access, "")
				if err == nil {
					output.Roles, err = compileAccessMap(mapper.roles, "role:")
				}
			}
		}
		return output, err
	}
	return mapper, nil
}

func NewDefaultChannelMapper() (*ChannelMapper, error) {
	return NewChannelMapper(`function(doc){channel(doc.channels);}`)
}

// Common implementation of 'access()' and 'role()' callbacks
func (mapper *ChannelMapper) addValueForUser(user otto.Value, value otto.Value, mapping map[string][]string) otto.Value {
	usernameArray := []string{}
	if user.IsString() {
		usernameArray = []string{user.String()}
	} else if isOttoArray(user) {
		usernameArray = ottoArrayToStrings(user)
	}
	for _, name := range usernameArray {
		if value.IsString() {
			mapping[name] = append(mapping[name], value.String())
		} else if isOttoArray(value) {
			array := ottoArrayToStrings(value)
			if array != nil {
				mapping[name] = append(mapping[name], array...)
			}
		}
	}
	return otto.UndefinedValue()
}

func compileAccessMap(input map[string][]string, prefix string) (AccessMap, error) {
	access := make(AccessMap, len(input))
	for name, values := range input {
		// If a prefix is specified, strip it from all values or return error if missing:
		if prefix != "" {
			for i, value := range values {
				if strings.HasPrefix(value, prefix) {
					values[i] = value[len(prefix):]
				} else {
					return nil, fmt.Errorf("Value %q does not begin with %q", value, prefix)
				}
			}
		}
		var err error
		if access[name], err = SetFromArray(values, RemoveStar); err != nil {
			return nil, err
		}
	}
	return access, nil
}

// This is just for testing
func (mapper *ChannelMapper) callMapper(body map[string]interface{}, oldBodyJSON string, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	res, err := mapper.js.DirectCall(body, walrus.JSONString(oldBodyJSON), userCtx)
	return res.(*ChannelMapperOutput), err
}

func (mapper *ChannelMapper) MapToChannelsAndAccess(body map[string]interface{}, oldBodyJSON string, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	result1, err := mapper.js.Call(body, walrus.JSONString(oldBodyJSON), userCtx)
	if err != nil {
		return nil, err
	}
	output := result1.(*ChannelMapperOutput)
	return output, nil
}

func (mapper *ChannelMapper) SetFunction(fnSource string) (bool, error) {
	return mapper.js.SetFunction(fnSource)
}

func (mapper *ChannelMapper) Stop() {
	mapper.js.Stop()
}
