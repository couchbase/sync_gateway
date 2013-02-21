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
	"strconv"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/robertkrimen/otto"
)

type channelMapperOutput struct {
	channels []string
	access   AccessMap
}

type ChannelMapper struct {
	output *channelMapperOutput
	js     *base.JSServer
}

// Maps user names to arrays of channel names
type AccessMap map[string][]string

// Converts a JS array into a Go string array.
func ottoArrayToStrings(array *otto.Object) []string {
	lengthVal, err := array.Get("length")
	if err != nil {
		return nil
	}
	length, err := lengthVal.ToInteger()
	if err != nil || length <= 0 {
		return nil
	}

	result := make([]string, 0, length)
	for i := 0; i < int(length); i++ {
		item, err := array.Get(strconv.Itoa(i))
		if err == nil && item.IsString() {
			result = append(result, item.String())
		}
	}
	return result
}

func NewChannelMapper(funcSource string) (*ChannelMapper, error) {
	mapper := &ChannelMapper{}
	var err error
	mapper.js, err = base.NewJSServer(funcSource)
	if err != nil {
		return nil, err
	}

	// Implementation of the 'sync()' callback:
	mapper.js.DefineNativeFunction("sync", func(call otto.FunctionCall) otto.Value {
		for _, arg := range call.ArgumentList {
			if arg.IsString() {
				mapper.output.channels = append(mapper.output.channels, arg.String())
			} else if arg.Class() == "Array" {
				array := ottoArrayToStrings(arg.Object())
				if array != nil {
					mapper.output.channels = append(mapper.output.channels, array...)
				}
			}
		}
		return otto.UndefinedValue()
	})

	// Implementation of the 'access()' callback:
	mapper.js.DefineNativeFunction("access", func(call otto.FunctionCall) otto.Value {
		username := call.Argument(0)
		channels := call.Argument(1)
		usernameArray := []string{}
		if (username.IsString()) {
			usernameArray = []string{username.String()}
		} else if username.Class() == "Array" {
			usernameArray = ottoArrayToStrings(username.Object())
		}
		for _, name := range usernameArray {
			if channels.IsString() {
				mapper.output.access[name] = append(mapper.output.access[name], channels.String())
			} else if channels.Class() == "Array" {
				array := ottoArrayToStrings(channels.Object())
				if array != nil {
					mapper.output.access[name] = append(mapper.output.access[name], array...)
				}
			}
		}
		return otto.UndefinedValue()
	})

	mapper.js.Before = func() {
		mapper.output = &channelMapperOutput{
			channels: []string{},
			access:   map[string][]string{},
		}
	}
	mapper.js.After = func(result otto.Value, err error) (interface{}, error) {
		output := mapper.output
		mapper.output = nil
		return output, err
	}
	return mapper, nil
}

func NewDefaultChannelMapper() (*ChannelMapper, error) {
	return NewChannelMapper(`function(doc){sync(doc.channels);}`)
}

// This is just for testing
func (mapper *ChannelMapper) callMapper(input string) (*channelMapperOutput, error) {
	res, err := mapper.js.DirectCallFunction([]string{input})
	return res.(*channelMapperOutput), err
}

func (mapper *ChannelMapper) MapToChannelsAndAccess(input string) ([]string, AccessMap, error) {
	result1, err := mapper.js.CallFunction([]string{input})
	if err != nil {
		return nil, nil, err
	}
	output := result1.(*channelMapperOutput)
	return SimplifyChannels(output.channels, false), output.access, nil
}

func (mapper *ChannelMapper) SetFunction(fnSource string) (bool, error) {
	return mapper.js.SetFunction(fnSource)
}

func (mapper *ChannelMapper) Stop() {
	mapper.js.Stop()
}
