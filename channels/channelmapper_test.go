//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
	"github.com/robertkrimen/otto/underscore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

func parse(jsonStr string) map[string]interface{} {
	var parsed map[string]interface{}
	_ = base.JSONUnmarshal([]byte(jsonStr), &parsed)
	return parsed
}

func emptyMetaMap() map[string]interface{} {
	return map[string]interface{}{
		base.MetaMapXattrsKey: nil,
	}
}

var noUser = map[string]interface{}{"name": nil, "channels": []string{}}

func TestOttoValueToStringArray(t *testing.T) {
	// Test for https://github.com/robertkrimen/otto/issues/24
	value, _ := otto.New().ToValue([]string{"foo", "bar", "baz"})
	strings := ottoValueToStringArray(value)
	assert.Equal(t, []string{"foo", "bar", "baz"}, strings)

	// Test for https://issues.couchbase.com/browse/CBG-714
	value, _ = otto.New().ToValue([]interface{}{"a", []interface{}{"b", "g"}, "c", 4})
	strings = ottoValueToStringArray(value)
	assert.Equal(t, []string{"a", "c"}, strings)
}

// verify that our version of Otto treats JSON parsed arrays like real arrays
func TestJavaScriptWorks(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel(doc.x.concat(doc.y));}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"x":["abc"],"y":["xyz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "abc", "xyz"), res.Channels)
}

// Just verify that the calls to the channel() fn show up in the output channel list.
func TestSyncFunction(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel("foo", "bar"); channel("baz")}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": []}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "foo", "bar", "baz"), res.Channels)
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunction(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access("foo", "bar"); access("foo", "baz")}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{"foo": SetOf(t, "bar", "baz")}, res.Access)
}

// Just verify that the calls to the channel() fn show up in the output channel list.
func TestSyncFunctionTakesArray(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel(["foo", "bar ok","baz"])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": []}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "foo", "bar ok", "baz"), res.Channels)
}

// Calling channel() with an invalid channel name should return an error.
func TestSyncFunctionRejectsInvalidChannels(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel(["foo", "bad,name","baz"])}`)
	_, err := mapper.MapToChannelsAndAccess(parse(`{"channels": []}`), `{}`, emptyMetaMap(), noUser)
	assert.True(t, err != nil)
}

// Calling access() with an invalid channel name should return an error.
func TestAccessFunctionRejectsInvalidChannels(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access("foo", "bad,name");}`)
	_, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.True(t, err != nil)
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunctionTakesArrayOfUsers(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access(["foo","bar","baz"], "ginger")}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{"bar": SetOf(t, "ginger"), "baz": SetOf(t, "ginger"), "foo": SetOf(t, "ginger")}, res.Access)
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunctionTakesArrayOfChannels(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access("lee", ["ginger", "earl_grey", "green"])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{"lee": SetOf(t, "ginger", "earl_grey", "green")}, res.Access)
}

func TestAccessFunctionTakesArrayOfChannelsAndUsers(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access(["lee", "nancy"], ["ginger", "earl_grey", "green"])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "ginger", "earl_grey", "green"), res.Access["lee"])
	assert.Equal(t, SetOf(t, "ginger", "earl_grey", "green"), res.Access["nancy"])
}

func TestAccessFunctionTakesEmptyArrayUser(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access([], ["ginger", "earl grey", "green"])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{}, res.Access)
}

func TestAccessFunctionTakesEmptyArrayChannels(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access("lee", [])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{}, res.Access)
}

func TestAccessFunctionTakesNullUser(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access(null, ["ginger", "earl grey", "green"])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{}, res.Access)
}

func TestAccessFunctionTakesNullChannels(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access("lee", null)}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{}, res.Access)
}

func TestAccessFunctionTakesNonChannelsInArray(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {access("lee", ["ginger", null, 5])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{"lee": SetOf(t, "ginger")}, res.Access)
}

func TestAccessFunctionTakesUndefinedUser(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {var x = {}; access(x.nothing, ["ginger", "earl grey", "green"])}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{}, res.Access)
}

// Just verify that the calls to the role() fn show up in the output. (It shares a common
// implementation with access(), so most of the above tests also apply to it.)
func TestRoleFunction(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {role(["foo","bar","baz"], "role:froods")}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, AccessMap{"bar": SetOf(t, "froods"), "baz": SetOf(t, "froods"), "foo": SetOf(t, "froods")}, res.Roles)
}

// Now just make sure the input comes through intact
func TestInputParse(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel(doc.channel);}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channel": "foo"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "foo"), res.Channels)
}

// A more realistic example
func TestDefaultChannelMapper(t *testing.T) {
	mapper := NewDefaultChannelMapper()
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "foo", "bar", "baz"), res.Channels)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"x": "y"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.Set{}, res.Channels)
}

// Empty/no-op channel mapper fn
func TestEmptyChannelMapper(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.Set{}, res.Channels)
}

// channel mapper fn that uses _ underscore JS library
func TestChannelMapperUnderscoreLib(t *testing.T) {
	underscore.Enable() // It really slows down unit tests (by making otto.New take a lot longer)
	defer underscore.Disable()
	mapper := NewChannelMapper(`function(doc) {channel(_.first(doc.channels));}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "foo"), res.Channels)
}

// Validation by calling reject()
func TestChannelMapperReject(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {reject(403, "bad");}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, "bad"), res.Rejection)
}

// Rejection by calling throw()
func TestChannelMapperThrow(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {throw({forbidden:"bad"});}`)
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, "bad"), res.Rejection)
}

// Test other runtime exception
func TestChannelMapperException(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {(nil)[5];}`)
	_, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.True(t, err != nil)
}

// Test the public API
func TestPublicChannelMapper(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel(doc.channels);}`)
	output, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "foo", "bar", "baz"), output.Channels)
}

// Test the userCtx name parameter
func TestCheckUser(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc) {
			requireUser(doc.owner);
		}`)
	var sally = map[string]interface{}{"name": "sally", "channels": []string{}}
	res, err := mapper.MapToChannelsAndAccess(parse(`{"owner": "sally"}`), `{}`, emptyMetaMap(), sally)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)

	var linus = map[string]interface{}{"name": "linus", "channels": []string{}}
	res, err = mapper.MapToChannelsAndAccess(parse(`{"owner": "sally"}`), `{}`, emptyMetaMap(), linus)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorWrongUser), res.Rejection)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"owner": "sally"}`), `{}`, emptyMetaMap(), nil)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)
}

// Test the userCtx name parameter with a list
func TestCheckUserArray(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc) {
			requireUser(doc.owners);
		}`)
	var sally = map[string]interface{}{"name": "sally", "channels": []string{}}
	res, err := mapper.MapToChannelsAndAccess(parse(`{"owners": ["sally", "joe"]}`), `{}`, emptyMetaMap(), sally)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)

	var linus = map[string]interface{}{"name": "linus", "channels": []string{}}
	res, err = mapper.MapToChannelsAndAccess(parse(`{"owners": ["sally", "joe"]}`), `{}`, emptyMetaMap(), linus)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorWrongUser), res.Rejection)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"owners": ["sally"]}`), `{}`, emptyMetaMap(), nil)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)
}

// Test the userCtx role parameter
func TestCheckRole(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc) {
			requireRole(doc.role);
		}`)
	var sally = map[string]interface{}{"name": "sally", "roles": map[string]int{"girl": 1, "5yo": 1}}
	res, err := mapper.MapToChannelsAndAccess(parse(`{"role": "girl"}`), `{}`, emptyMetaMap(), sally)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)

	var linus = map[string]interface{}{"name": "linus", "roles": []string{"boy", "musician"}}
	res, err = mapper.MapToChannelsAndAccess(parse(`{"role": "girl"}`), `{}`, emptyMetaMap(), linus)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorMissingRole), res.Rejection)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"role": "girl"}`), `{}`, emptyMetaMap(), nil)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)
}

// Test the userCtx role parameter with a list
func TestCheckRoleArray(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc) {
			requireRole(doc.roles);
		}`)
	var sally = map[string]interface{}{"name": "sally", "roles": map[string]int{"girl": 1, "5yo": 1}}
	res, err := mapper.MapToChannelsAndAccess(parse(`{"roles": ["kid","girl"]}`), `{}`, emptyMetaMap(), sally)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)

	var linus = map[string]interface{}{"name": "linus", "roles": map[string]int{"boy": 1, "musician": 1}}
	res, err = mapper.MapToChannelsAndAccess(parse(`{"roles": ["girl"]}`), `{}`, emptyMetaMap(), linus)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorMissingRole), res.Rejection)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"roles": ["girl"]}`), `{}`, emptyMetaMap(), nil)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)
}

// Test the userCtx.channels parameter
func TestCheckAccess(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc) {
		requireAccess(doc.channel)
	}`)
	var sally = map[string]interface{}{"name": "sally", "roles": []string{"girl", "5yo"}, "channels": []string{"party", "school"}}
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channel": "party"}`), `{}`, emptyMetaMap(), sally)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)

	var linus = map[string]interface{}{"name": "linus", "roles": []string{"boy", "musician"}, "channels": []string{"party", "school"}}
	res, err = mapper.MapToChannelsAndAccess(parse(`{"channel": "work"}`), `{}`, emptyMetaMap(), linus)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorMissingChannelAccess), res.Rejection)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"channel": "magic"}`), `{}`, emptyMetaMap(), nil)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)
}

// Test the userCtx.channels parameter with a list
func TestCheckAccessArray(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc) {
		requireAccess(doc.channels)
	}`)
	var sally = map[string]interface{}{"name": "sally", "roles": []string{"girl", "5yo"}, "channels": []string{"party", "school"}}
	res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["swim","party"]}`), `{}`, emptyMetaMap(), sally)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)

	var linus = map[string]interface{}{"name": "linus", "roles": []string{"boy", "musician"}, "channels": []string{"party", "school"}}
	res, err = mapper.MapToChannelsAndAccess(parse(`{"channels": ["work"]}`), `{}`, emptyMetaMap(), linus)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorMissingChannelAccess), res.Rejection)

	res, err = mapper.MapToChannelsAndAccess(parse(`{"channels": ["magic"]}`), `{}`, emptyMetaMap(), nil)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, nil, res.Rejection)
}

// Test changing the function
func TestSetFunction(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {channel(doc.channels);}`)
	output, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	changed, err := mapper.SetFunction(`function(doc) {channel("all");}`)
	assert.True(t, changed, "SetFunction failed")
	assert.NoError(t, err, "SetFunction failed")
	output, err = mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, SetOf(t, "all"), output.Channels)
}

// Test that expiry function sets the expiry property
func TestExpiryFunction(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {expiry(doc.expiry);}`)
	res1, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":100}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error")
	assert.Equal(t, uint32(100), *res1.Expiry)

	res2, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"500"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error")
	assert.Equal(t, uint32(500), *res2.Expiry)

	res_stringDate, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"2105-01-01T00:00:00.000+00:00"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error")
	assert.Equal(t, uint32(4260211200), *res_stringDate.Expiry)

	// Validate invalid expiry values log warning and don't set expiry
	res3, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"abc"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry:abc")
	assert.True(t, res3.Expiry == nil)

	// Invalid: non-numeric
	res4, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":["100", "200"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry as array")
	assert.True(t, res4.Expiry == nil)

	// Invalid: negative value
	res5, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":-100}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry as negative value")
	assert.True(t, res5.Expiry == nil)

	// Invalid - larger than uint32
	res6, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":123456789012345}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry > unit32")
	assert.True(t, res6.Expiry == nil)

	// Invalid - non-unix date
	resInvalidDate, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"1805-01-01T00:00:00.000+00:00"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry:1805-01-01T00:00:00.000+00:00")
	assert.True(t, resInvalidDate.Expiry == nil)

	// No expiry specified
	res7, err := mapper.MapToChannelsAndAccess(parse(`{"value":5}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry not specified")
	assert.True(t, res7.Expiry == nil)
}

func TestExpiryFunctionConstantValue(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {expiry(100);}`)
	res1, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error")
	assert.Equal(t, uint32(100), *res1.Expiry)

	mapper = NewChannelMapper(`function(doc) {expiry("500");}`)
	res2, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error")
	assert.Equal(t, uint32(500), *res2.Expiry)

	mapper = NewChannelMapper(`function(doc) {expiry("2105-01-01T00:00:00.000+00:00");}`)
	res_stringDate, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error")
	assert.Equal(t, uint32(4260211200), *res_stringDate.Expiry)

	// Validate invalid expiry values log warning and don't set expiry
	mapper = NewChannelMapper(`function(doc) {expiry("abc");}`)
	res3, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry:abc")
	assert.True(t, res3.Expiry == nil)

	// Invalid: non-numeric
	mapper = NewChannelMapper(`function(doc) {expiry(["100", "200"]);}`)
	res4, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry as array")
	assert.True(t, res4.Expiry == nil)

	// Invalid: negative value
	mapper = NewChannelMapper(`function(doc) {expiry(-100);}`)
	res5, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry as negative value")
	assert.True(t, res5.Expiry == nil)

	// Invalid - larger than uint32
	mapper = NewChannelMapper(`function(doc) {expiry(123456789012345);}`)
	res6, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry as > unit32")
	assert.True(t, res6.Expiry == nil)

	// Invalid - non-unix date
	mapper = NewChannelMapper(`function(doc) {expiry("1805-01-01T00:00:00.000+00:00");}`)
	resInvalidDate, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry:1805-01-01T00:00:00.000+00:00")
	assert.True(t, resInvalidDate.Expiry == nil)

	// No expiry specified
	mapper = NewChannelMapper(`function(doc) {expiry();}`)
	res7, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess error for expiry not specified")
	assert.True(t, res7.Expiry == nil)
}

// Test that expiry function when invoked more than once by sync function
func TestExpiryFunctionMultipleInvocation(t *testing.T) {
	mapper := NewChannelMapper(`function(doc) {expiry(doc.expiry); expiry(doc.secondExpiry)}`)
	res1, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":100}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, uint32(100), *res1.Expiry)

	res2, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"500"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess failed")
	assert.Equal(t, uint32(500), *res2.Expiry)

	// Validate invalid expiry values log warning and don't set expiry
	res3, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"abc"}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry:abc")
	assert.True(t, res3.Expiry == nil)

	// Invalid: non-numeric
	res4, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":["100", "200"]}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array")
	assert.True(t, res4.Expiry == nil)

	// Invalid: negative value
	res5, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":-100}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array")
	assert.True(t, res5.Expiry == nil)

	// Invalid - larger than uint32
	res6, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":123456789012345}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array")
	assert.True(t, res6.Expiry == nil)

	// No expiry specified
	res7, err := mapper.MapToChannelsAndAccess(parse(`{"value":5}`), `{}`, emptyMetaMap(), noUser)
	assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array")
	assert.True(t, res7.Expiry == nil)
}

func TestMetaMap(t *testing.T) {
	mapper := NewChannelMapper(`function(doc, oldDoc, meta) {channel(meta.xattrs.myxattr.channels);}`)

	channels := []string{"chan1", "chan2"}

	metaMap := map[string]interface{}{
		base.MetaMapXattrsKey: map[string]interface{}{
			"myxattr": map[string]interface{}{
				"channels": channels,
			},
		},
	}

	res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, metaMap, noUser)
	require.NoError(t, err)
	assert.ElementsMatch(t, res.Channels.ToArray(), channels)
}

func TestNilMetaMap(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	mapper := NewChannelMapper(`function(doc, oldDoc, meta) {channel(meta.xattrs.myxattr.val);}`)

	metaMap := map[string]interface{}{
		base.MetaMapXattrsKey: map[string]interface{}{
			"": nil,
		},
	}

	_, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, metaMap, noUser)
	require.Error(t, err)
	assert.True(t, err.Error() == "TypeError: Cannot access member 'val' of undefined")
}

func TestChangedUsers(t *testing.T) {
	a := AccessMap{"alice": SetOf(t, "x", "y"), "bita": SetOf(t, "z"), "claire": SetOf(t, "w")}
	b := AccessMap{"alice": SetOf(t, "x", "z"), "bita": SetOf(t, "z"), "diana": SetOf(t, "w")}

	changes := map[string]bool{}
	ForChangedUsers(a, b, func(name string) {
		changes[name] = true
	})
	assert.Equal(t, map[string]bool{"alice": true, "claire": true, "diana": true}, changes)
}
