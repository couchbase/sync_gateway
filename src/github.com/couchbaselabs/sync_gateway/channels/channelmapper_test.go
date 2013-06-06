//  Copyright (c) 2012 Couchbase, Inc.
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
	"github.com/couchbaselabs/go.assert"
	"testing"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/robertkrimen/otto"
)

func parse(jsonStr string) map[string]interface{} {
	var parsed map[string]interface{}
	json.Unmarshal([]byte(jsonStr), &parsed)
	return parsed
}

var noUser = map[string]interface{}{"name": nil, "channels": []string{}}

func TestOttoValueToStringArray(t *testing.T) {
	// Test for https://github.com/robertkrimen/otto/issues/24
	value, _ := otto.New().ToValue([]string{"foo", "bar", "baz"})
	strings := ottoValueToStringArray(value)
	assert.DeepEquals(t, strings, []string{"foo", "bar", "baz"})
}

// verify that our version of Otto treats JSON parsed arrays like real arrays
func TestJavaScriptWorks(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(doc.x.concat(doc.y));}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"x":["abc"],"y":["xyz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, SetOf("abc", "xyz"))
}

// Just verify that the calls to the channel() fn show up in the output channel list.
func TestSyncFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel("foo", "bar"); channel("baz")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": []}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, SetOf("foo", "bar", "baz"))
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("foo", "bar"); access("foo", "baz")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{"foo": SetOf("bar", "baz")})
}

// Just verify that the calls to the channel() fn show up in the output channel list.
func TestSyncFunctionTakesArray(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(["foo", "bar","baz"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": []}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, SetOf("foo", "bar", "baz"))
}

// Calling channel() with an invalid channel name should return an error.
func TestSyncFunctionRejectsInvalidChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(["foo", "bad name","baz"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	_, err = mapper.callMapper(parse(`{"channels": []}`), `{}`, noUser)
	assert.True(t, err != nil)
}

// Calling access() with an invalid channel name should return an error.
func TestAccessFunctionRejectsInvalidChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("foo", "bad name");}`)
	assertNoError(t, err, "Couldn't create mapper")
	_, err = mapper.callMapper(parse(`{}`), `{}`, noUser)
	assert.True(t, err != nil)
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunctionTakesArrayOfUsers(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access(["foo","bar","baz"], "ginger")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{"bar": SetOf("ginger"), "baz": SetOf("ginger"), "foo": SetOf("ginger")})
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunctionTakesArrayOfChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", ["ginger", "earl_grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{"lee": SetOf("ginger", "earl_grey", "green")})
}

func TestAccessFunctionTakesArrayOfChannelsAndUsers(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access(["lee", "nancy"], ["ginger", "earl_grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access["lee"], SetOf("ginger", "earl_grey", "green"))
	assert.DeepEquals(t, res.Access["nancy"], SetOf("ginger", "earl_grey", "green"))
}

func TestAccessFunctionTakesEmptyArrayUser(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access([], ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{})
}

func TestAccessFunctionTakesEmptyArrayChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", [])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{})
}

func TestAccessFunctionTakesNullUser(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access(null, ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{})
}

func TestAccessFunctionTakesNullChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", null)}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{})
}

func TestAccessFunctionTakesNonChannelsInArray(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", ["ginger", null, 5])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{"lee": SetOf("ginger")})
}

func TestAccessFunctionTakesUndefinedUser(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {var x = {}; access(x.nothing, ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Access, AccessMap{})
}

// Just verify that the calls to the role() fn show up in the output. (It shares a common
// implementation with access(), so most of the above tests also apply to it.)
func TestRoleFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {role(["foo","bar","baz"], "role:froods")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Roles, AccessMap{"bar": SetOf("froods"), "baz": SetOf("froods"), "foo": SetOf("froods")})
}

// Now just make sure the input comes through intact
func TestInputParse(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(doc.channel);}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channel": "foo"}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, SetOf("foo"))
}

// A more realistic example
func TestDefaultChannelMapper(t *testing.T) {
	mapper, err := NewDefaultChannelMapper()
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, SetOf("foo", "bar", "baz"))

	res, err = mapper.callMapper(parse(`{"x": "y"}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, base.Set{})
}

// Empty/no-op channel mapper fn
func TestEmptyChannelMapper(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, base.Set{})
}

// channel mapper fn that uses _ underscore JS library
func TestChannelMapperUnderscoreLib(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(_.first(doc.channels));}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Channels, SetOf("foo"))
}

// Validation by calling reject()
func TestChannelMapperReject(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {reject(403, "bad");}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Rejection, &base.HTTPError{403, "bad"})
}

// Rejection by calling throw()
func TestChannelMapperThrow(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {throw({forbidden:"bad"});}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.Rejection, &base.HTTPError{403, "bad"})
}

// Test other runtime exception
func TestChannelMapperException(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {(nil)[5];}`)
	assertNoError(t, err, "Couldn't create mapper")
	_, err = mapper.callMapper(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assert.True(t, err != nil)
}

// Test the public API
func TestPublicChannelMapper(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	output, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, output.Channels, SetOf("foo", "bar", "baz"))
	mapper.Stop()
}

// Test changing the function
func TestSetFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {channel(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	output, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	changed, err := mapper.SetFunction(`function(doc) {channel("all");}`)
	assertTrue(t, changed, "SetFunction failed")
	assertNoError(t, err, "SetFunction failed")
	output, err = mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, noUser)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, output.Channels, SetOf("all"))
	mapper.Stop()
}

func TestChangedUsers(t *testing.T) {
	a := AccessMap{"alice": SetOf("x", "y"), "bita": SetOf("z"), "claire": SetOf("w")}
	b := AccessMap{"alice": SetOf("x", "z"), "bita": SetOf("z"), "diana": SetOf("w")}

	changes := map[string]bool{}
	ForChangedUsers(a, b, func(name string) {
		changes[name] = true
	})
	assert.DeepEquals(t, changes, map[string]bool{"alice": true, "claire": true, "diana": true})
}

//////// HELPERS:

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}
