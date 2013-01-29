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

	"github.com/robertkrimen/otto"

	"github.com/couchbaselabs/sync_gateway/base"
)

type ChannelMapper struct {
	js       *base.JSServer
	channels []string
}

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
				mapper.channels = append(mapper.channels, arg.String())
			} else if arg.Class() == "Array" {
				array := ottoArrayToStrings(arg.Object())
				if array != nil {
					mapper.channels = append(mapper.channels, array...)
				}
			}
		}
		return otto.UndefinedValue()
	})

	mapper.js.Before = func() {
		mapper.channels = []string{}
	}
	mapper.js.After = func(result otto.Value, err error) (interface{}, error) {
		channels := mapper.channels
		mapper.channels = nil
		return channels, err
	}
	return mapper, nil
}

func NewDefaultChannelMapper() (*ChannelMapper, error) {
	return NewChannelMapper(`function(doc){sync(doc.channels);}`)
}

// This is just for testing
func (mapper *ChannelMapper) callMapper(input string) (interface{}, error) {
	return mapper.js.DirectCallFunction([]string{input})
}

func (mapper *ChannelMapper) MapToChannels(input string) ([]string, error) {
	result, err := mapper.js.CallFunction([]string{input})
	channels := result.([]string)
	if channels == nil {
		channels = SimplifyChannels(channels, false)
	}
	return channels, err
}

func (mapper *ChannelMapper) SetFunction(fnSource string) (bool, error) {
	return mapper.js.SetFunction(fnSource)
}

func (mapper *ChannelMapper) Stop() {
	mapper.js.Stop()
}
