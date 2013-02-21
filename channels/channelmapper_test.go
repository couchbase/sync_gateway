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
	"github.com/sdegutis/go.assert"
	"testing"
)

// Just verify that the calls to the sync() fn show up in the output channel list.
func TestSyncFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync("foo", "bar"); sync("baz")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{"channels": []}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.channels, []string{"foo", "bar", "baz"})
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("foo", "bar"); access("foo", "baz")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{"foo" : []string{"bar", "baz"}})
}

// Just verify that the calls to the sync() fn show up in the output channel list.
func TestSyncFunctionTakesArray(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(["foo", "bar","baz"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{"channels": []}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.channels, []string{"foo", "bar", "baz"})
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunctionTakesArrayOfUsers(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access(["foo","bar","baz"], "ginger")}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{"bar":[]string{"ginger"}, "baz":[]string{"ginger"}, "foo":[]string{"ginger"}})
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunctionTakesArrayOfChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{"lee":[]string{"ginger", "earl grey", "green"}})
}

func TestAccessFunctionTakesArrayOfChannelsAndUsers(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access(["lee", "nancy"], ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access["lee"], []string{"ginger", "earl grey", "green"})
	assert.DeepEquals(t, res.access["nancy"], []string{"ginger", "earl grey", "green"})
}

func TestAccessFunctionTakesEmptyArrayUser(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access([], ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{})
}

func TestAccessFunctionTakesEmptyArrayChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", [])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{})
}

func TestAccessFunctionTakesNullUser(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access(null, ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{})
}

func TestAccessFunctionTakesNullChannels(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", null)}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{})
}

func TestAccessFunctionTakesNonChannelsInArray(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {access("lee", ["ginger", null, 5])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{"lee":[]string{"ginger"}})
}

func TestAccessFunctionTakesUndefinedUser(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {var x = {}; access(x.nothing, ["ginger", "earl grey", "green"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.access, AccessMap{})
}

// Now just make sure the input comes through intact
func TestInputParse(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channel);}`)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{"channel": "foo"}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.channels, []string{"foo"})
}

// A more realistic example
func TestDefaultChannelMapper(t *testing.T) {
	mapper, err := NewDefaultChannelMapper()
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.channels, []string{"foo", "bar", "baz"})

	res, err = mapper.callMapper(`{"x": "y"}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.channels, []string{})
}

// Empty/no-op channel mapper fn
func TestEmptyChannelMapper(t *testing.T) {
	mapper, err := NewChannelMapper(``)
	assertNoError(t, err, "Couldn't create mapper")
	res, err := mapper.callMapper(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, res.channels, []string{})
}

// Test the public API
func TestPublicChannelMapper(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, _, err := mapper.MapToChannelsAndAccess(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"foo", "bar", "baz"})
	mapper.Stop()
}

// Test changing the function
func TestSetFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, _, err := mapper.MapToChannelsAndAccess(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	changed, err := mapper.SetFunction(`function(doc) {sync("all");}`)
	assertTrue(t, changed, "SetFunction failed")
	assertNoError(t, err, "SetFunction failed")
	channels, _, err = mapper.MapToChannelsAndAccess(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"all"})
	mapper.Stop()
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
