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
	"encoding/json"
	"strconv"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	_ "github.com/robertkrimen/otto/underscore"
)

/** Result of running a channel-mapper function. */
type ChannelMapperOutput struct {
	Channels  base.Set  // channels assigned to the document via channel() callback
	Roles     AccessMap // roles granted to users via role() callback
	Access    AccessMap // channels granted to users via access() callback
	Rejection error     // Error associated with failed validate (require callbacks, etc)
	Expiry    *uint32   // Expiry value specified by expiry() callback.  Standard CBS expiry format: seconds if less than 30 days, epoch time otherwise
}

type ChannelMapper struct {
	*sgbucket.JSServer // "Superclass"
}

// Maps user names (or role names prefixed with "role:") to arrays of channel or role names
type AccessMap map[string]base.Set

// Number of SyncRunner tasks (and Otto contexts) to cache
const kTaskCacheSize = 4

func NewChannelMapper(fnSource string) *ChannelMapper {
	return &ChannelMapper{
		JSServer: sgbucket.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
				return NewSyncRunner(fnSource)
			}),
	}
}

func NewDefaultChannelMapper() *ChannelMapper {
	return NewChannelMapper(`function(doc){channel(doc.channels);}`)
}

func (mapper *ChannelMapper) MapToChannelsAndAccess(body map[string]interface{}, oldBodyJSON string, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	numberFixBody := ConvertJSONNumbers(body)
	result1, err := mapper.Call(numberFixBody, sgbucket.JSONString(oldBodyJSON), userCtx)
	if err != nil {
		return nil, err
	}
	output := result1.(*ChannelMapperOutput)
	return output, nil
}

// Javscript max integer value (https://www.ecma-international.org/ecma-262/5.1/#sec-8.5)
const JavascriptMaxSafeInt = int64(1<<53 - 1)
const JavascriptMinSafeInt = -JavascriptMaxSafeInt

// ConvertJSONNumbers converts json.Number values to javascript number objects for use in the sync
// function.  Integers that would lose precision are left as json.Number, as are floats that can't be
// converted to float64.
func ConvertJSONNumbers(value interface{}) interface{} {
	switch value := value.(type) {
	case json.Number:
		if asInt, err := value.Int64(); err == nil {
			if asInt > JavascriptMaxSafeInt || asInt < JavascriptMinSafeInt {
				// Integer will lose precision when used in javascript - leave as json.Number
				return value
			}
			return asInt
		} else {
			numErr, _ := err.(*strconv.NumError)
			if numErr.Err == strconv.ErrRange {
				return value
			}
		}

		if asFloat, err := value.Float64(); err == nil {
			// Can't reliably detect loss of precision in float, due to number of variations in input float format
			return asFloat
		}
		return value
	case map[string]interface{}:
		for k, v := range value {
			value[k] = ConvertJSONNumbers(v)
		}
	case []interface{}:
		for i, v := range value {
			value[i] = ConvertJSONNumbers(v)
		}
	default:
	}
	return value
}

//////// UTILITY FUNCTIONS:

// Calls the function for each user whose access is different between the two AccessMaps
func ForChangedUsers(a, b AccessMap, fn func(user string)) {
	for name, access := range a {
		if !access.Equals(b[name]) {
			fn(name)
		}
	}
	for name := range b {
		if _, existed := a[name]; !existed {
			fn(name)
		}
	}
}

func (runner *SyncRunner) MapToChannelsAndAccess(body map[string]interface{}, oldBodyJSON string, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	result, err := runner.Call(body, sgbucket.JSONString(oldBodyJSON), userCtx)
	if err != nil {
		return nil, err
	}
	return result.(*ChannelMapperOutput), nil
}
