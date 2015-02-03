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
	"github.com/couchbaselabs/walrus"
	_ "github.com/robertkrimen/otto/underscore"

	"github.com/couchbase/sync_gateway/base"
)

/** Result of running a channel-mapper function. */
type ChannelMapperOutput struct {
	Channels  base.Set
	Roles     AccessMap // roles granted to users via role() callback
	Access    AccessMap
	Rejection error
}

type ChannelMapper struct {
	*walrus.JSServer // "Superclass"
}

// Maps user names (or role names prefixed with "role:") to arrays of channel or role names
type AccessMap map[string]base.Set

// Number of SyncRunner tasks (and Otto contexts) to cache
const kTaskCacheSize = 4

func NewChannelMapper(fnSource string) *ChannelMapper {
	return &ChannelMapper{
		JSServer: walrus.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (walrus.JSServerTask, error) {
				return NewSyncRunner(fnSource)
			}),
	}
}

func NewDefaultChannelMapper() *ChannelMapper {
	return NewChannelMapper(`function(doc){channel(doc.channels);}`)
}

func (mapper *ChannelMapper) MapToChannelsAndAccess(body map[string]interface{}, oldBodyJSON string, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	result1, err := mapper.Call(body, walrus.JSONString(oldBodyJSON), userCtx)
	if err != nil {
		return nil, err
	}
	output := result1.(*ChannelMapperOutput)
	return output, nil
}

//////// UTILITY FUNCTIONS:

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

func (runner *SyncRunner) MapToChannelsAndAccess(body map[string]interface{}, oldBodyJSON string, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	result, err := runner.Call(body, walrus.JSONString(oldBodyJSON), userCtx)
	if err != nil {
		return nil, err
	}
	return result.(*ChannelMapperOutput), nil
}
