//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// Docs: https://pkg.go.dev/github.com/snej/v8go
)

func parse(jsonStr string) map[string]interface{} {
	var parsed map[string]interface{}
	_ = base.JSONUnmarshal([]byte(jsonStr), &parsed)
	return parsed
}

func emptyMetaMap() MetaMap {
	return MetaMap{}
}

var noUser = map[string]interface{}{"name": nil, "channels": []string{}}

// verify that our version of Otto treats JSON parsed arrays like real arrays
func TestJavaScriptWorks(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel(doc.x.concat(doc.y));}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"x":["abc"],"y":["xyz"]}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			assert.Equal(t, BaseSetOf(t, "abc", "xyz"), res.Channels)
		}
	})
}

// Verify the sync fn cannot access the internal JS variables used in the wrapper.
func TestHiddenVariables(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		for _, hidden := range []string{"userCtx", "shouldValidate", "result", "eval", "Function"} {
			mapper := NewChannelMapper(vm, `function(doc) {return `+hidden+`;}`, 0)
			_, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
			if assert.Error(t, err, "Was able to access %q", hidden) {
				assert.ErrorContains(t, err, "ReferenceError:")
				assert.ErrorContains(t, err, " is not defined")
			}
		}
	})
}

// Just verify that the calls to the channel() fn show up in the output channel list.
func TestSyncFunction(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel("foo", "bar"); channel("baz")}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": []}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, BaseSetOf(t, "foo", "bar", "baz"), res.Channels)
	})
}

// Just verify that the calls to the access() fn show up in the output channel list.
func TestAccessFunction(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access("foo", "bar"); access("foo", "baz")}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{"foo": BaseSetOf(t, "bar", "baz")}, res.Access)
	})
}

// Just verify that the calls to the channel() fn show up in the output channel list.
func TestSyncFunctionTakesArray(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel(["foo", "bar ok","baz"])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": []}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, BaseSetOf(t, "foo", "bar ok", "baz"), res.Channels)
	})
}

// Calling channel() with an invalid channel name should return an error.
func TestSyncFunctionRejectsInvalidChannels(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel(["foo", "bad,name","baz"])}`, 0)
		_, err := mapper.MapToChannelsAndAccess(parse(`{"channels": []}`), `{}`, emptyMetaMap(), noUser)
		assert.True(t, err != nil)
	})
}

// Calling access() with an invalid channel name should return an error.
func TestAccessFunctionRejectsInvalidChannels(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access("foo", "bad,name");}`, 0)
		_, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.True(t, err != nil)
	})
}

// Verify that the first parameter to access() may be an array of usernames.
func TestAccessFunctionTakesArrayOfUsers(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access(["foo","bar","baz"], "ginger")}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			assert.Equal(t, AccessMap{"bar": BaseSetOf(t, "ginger"), "baz": BaseSetOf(t, "ginger"), "foo": BaseSetOf(t, "ginger")}, res.Access)
		}
	})
}

// Verify that the second parameter to access() may be an array of channels.
func TestAccessFunctionTakesArrayOfChannels(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access("lee", ["ginger", "earl_grey", "green"])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{"lee": BaseSetOf(t, "ginger", "earl_grey", "green")}, res.Access)
	})
}

func TestAccessFunctionTakesArrayOfChannelsAndUsers(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access(["lee", "nancy"], ["ginger", "earl_grey", "green"])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, BaseSetOf(t, "ginger", "earl_grey", "green"), res.Access["lee"])
		assert.Equal(t, BaseSetOf(t, "ginger", "earl_grey", "green"), res.Access["nancy"])
	})
}

func TestAccessFunctionTakesEmptyArrayUser(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access([], ["ginger", "earl grey", "green"])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{}, res.Access)
	})
}

func TestAccessFunctionTakesEmptyArrayChannels(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access("lee", [])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{}, res.Access)
	})
}

func TestAccessFunctionTakesNullUser(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access(null, ["ginger", "earl grey", "green"])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{}, res.Access)
	})
}

func TestAccessFunctionTakesNullChannels(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access("lee", null)}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{}, res.Access)
	})
}

func TestAccessFunctionTakesNonChannelsInArray(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {access("lee", ["ginger", null, 5])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{"lee": BaseSetOf(t, "ginger")}, res.Access)
	})
}

func TestAccessFunctionTakesUndefinedUser(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {var x = {}; access(x.nothing, ["ginger", "earl grey", "green"])}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{}, res.Access)
	})
}

// Just verify that the calls to the role() fn show up in the output. (It shares a common
// implementation with access(), so most of the above tests also apply to it.)
func TestRoleFunction(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {role(["foo","bar","baz"], "role:froods")}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, AccessMap{"bar": BaseSetOf(t, "froods"), "baz": BaseSetOf(t, "froods"), "foo": BaseSetOf(t, "froods")}, res.Roles)
	})
}

// Now just make sure the input comes through intact
func TestInputParse(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel(doc.channel);}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channel": "foo"}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, BaseSetOf(t, "foo"), res.Channels)
	})
}

// A more realistic example
func TestDefaultChannelMapper(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		testCases := []struct {
			mapper *ChannelMapper
			name   string
		}{
			{
				mapper: NewChannelMapper(vm, DocChannelsSyncFunction, 0),
				name:   "explicit_function",
			},
			{
				mapper: NewChannelMapper(vm, GetDefaultSyncFunction(base.DefaultScope, base.DefaultCollection), 0),
				name:   "explicit_function",
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				res, err := test.mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
				assert.NoError(t, err, "MapToChannelsAndAccess failed")
				assert.Equal(t, BaseSetOf(t, "foo", "bar", "baz"), res.Channels)

				res, err = test.mapper.MapToChannelsAndAccess(parse(`{"x": "y"}`), `{}`, emptyMetaMap(), noUser)
				assert.NoError(t, err, "MapToChannelsAndAccess failed")
				assert.Equal(t, base.Set{}, res.Channels)
			})
		}
	})
}

// Empty/no-op channel mapper fn
func TestEmptyChannelMapper(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, base.Set{}, res.Channels)
	})
}

// channel mapper fn that uses _ underscore JS library
func TestChannelMapperUnderscoreLib(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel(_.first(doc.channels));}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, BaseSetOf(t, "foo"), res.Channels)
	})
}

// Validation by calling reject()
func TestChannelMapperReject(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {reject(403, "bad");}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
		if !assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			return
		}
		assert.Equal(t, base.HTTPErrorf(403, "bad"), res.Rejection)
	})
}

// Rejection by calling throw()
func TestChannelMapperThrow(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {throw({forbidden:"bad"});}`, 0)
		res, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, base.HTTPErrorf(403, "bad"), res.Rejection)
	})
}

// Test other runtime exception
func TestChannelMapperException(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {(nil)[5];}`, 0)
		_, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
		assert.True(t, err != nil)
	})
}

// Test the public API
func TestPublicChannelMapper(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {channel(doc.channels);}`, 0)
		output, err := mapper.MapToChannelsAndAccess(parse(`{"channels": ["foo", "bar", "baz"]}`), `{}`, emptyMetaMap(), noUser)
		assert.NoError(t, err, "MapToChannelsAndAccess failed")
		assert.Equal(t, BaseSetOf(t, "foo", "bar", "baz"), output.Channels)
	})
}

// Test the userCtx name parameter
func TestCheckUser(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc) {
			requireUser(doc.owner);
		}`, 0)
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
	})
}

// Test the userCtx name parameter with a list
func TestCheckUserArray(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc) {
			requireUser(doc.owners);
		}`, 0)
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
	})
}

// Test the userCtx role parameter
func TestCheckRole(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc) {
			requireRole(doc.role);
		}`, 0)
		var sally = map[string]interface{}{"name": "sally", "roles": map[string]int{"girl": 1, "5yo": 1}}
		res, err := mapper.MapToChannelsAndAccess(parse(`{"role": "girl"}`), `{}`, emptyMetaMap(), sally)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			assert.Equal(t, nil, res.Rejection)
		}

		var linus = map[string]interface{}{"name": "linus", "roles": []string{"boy", "musician"}}
		res, err = mapper.MapToChannelsAndAccess(parse(`{"role": "girl"}`), `{}`, emptyMetaMap(), linus)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			assert.Equal(t, base.HTTPErrorf(403, base.SyncFnErrorMissingRole), res.Rejection)
		}

		res, err = mapper.MapToChannelsAndAccess(parse(`{"role": "girl"}`), `{}`, emptyMetaMap(), nil)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			assert.Equal(t, nil, res.Rejection)
		}
	})
}

// Test the userCtx role parameter with a list
func TestCheckRoleArray(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc) {
			requireRole(doc.roles);
		}`, 0)
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
	})
}

// Test the userCtx.channels parameter
func TestCheckAccess(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc) {
		requireAccess(doc.channel)
	}`, 0)
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
	})
}

// Test the userCtx.channels parameter with a list
func TestCheckAccessArray(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc) {
		requireAccess(doc.channels)
	}`, 0)
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
	})
}

// Test that expiry function sets the expiry property
func TestExpiryFunction(t *testing.T) {
	var res *ChannelMapperOutput
	var err error

	checkExp := func(expected int) {
		if assert.NoError(t, err, "MapToChannelsAndAccess error") {
			if assert.NotNil(t, res.Expiry) {
				assert.Equal(t, uint32(expected), *res.Expiry)
			}
		}
	}
	checkNilExp := func() {
		if assert.NoError(t, err, "MapToChannelsAndAccess error") {
			assert.Nil(t, res.Expiry)
		}
	}

	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {expiry(doc.expiry);}`, 0)

		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":100}`), `{}`, emptyMetaMap(), noUser)
		checkExp(100)

		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":"500"}`), `{}`, emptyMetaMap(), noUser)
		checkExp(500)

		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":"2105-01-01T00:00:00.000+00:00"}`), `{}`, emptyMetaMap(), noUser)
		checkExp(4260211200)

		// Validate invalid expiry values log warning and don't set expiry
		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":"abc"}`), `{}`, emptyMetaMap(), noUser)
		checkNilExp()

		// Invalid: non-numeric
		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":["100", "200"]}`), `{}`, emptyMetaMap(), noUser)
		checkNilExp()

		// Invalid: negative value
		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":-100}`), `{}`, emptyMetaMap(), noUser)
		checkNilExp()

		// Invalid - larger than uint32
		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":123456789012345}`), `{}`, emptyMetaMap(), noUser)
		checkNilExp()

		// Invalid - non-unix date
		res, err = mapper.MapToChannelsAndAccess(parse(`{"expiry":"1805-01-01T00:00:00.000+00:00"}`), `{}`, emptyMetaMap(), noUser)
		checkNilExp()

		// No expiry specified
		res, err = mapper.MapToChannelsAndAccess(parse(`{"value":5}`), `{}`, emptyMetaMap(), noUser)
		checkNilExp()
	})
}

func TestExpiryFunctionConstantValue(t *testing.T) {
	cases := []struct {
		name     string
		expiry   string
		expected any
	}{
		{"Integer", `100`, uint32(100)},
		{"Numeric string", `"500"`, uint32(500)},
		{"IS08601", `"2105-01-01T00:00:00.000+00:00"`, uint32(4260211200)},
		{"Invalid string", `"abc"`, nil},
		{"Non-numeric", `["100", "200"]`, nil},
		{"Negative", `-100`, nil},
		{"Too Large", `123456789012345`, nil},
		{"Invalid date format", `"1805-01-01T00:00:00.000+00:00"`, nil},
		{"Nothing", ``, nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
				mapper := NewChannelMapper(vm, `function(doc) {expiry(`+c.expiry+`);}`, 0)
				res1, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, emptyMetaMap(), noUser)
				assert.NoError(t, err, "MapToChannelsAndAccess error")
				if c.expected != nil {
					if assert.NotNil(t, res1.Expiry) {
						assert.Equal(t, c.expected, *res1.Expiry)
					}
				} else {
					assert.Nil(t, res1.Expiry)
				}
			})
		})
	}
}

// Test that expiry function when invoked more than once by sync function
func TestExpiryFunctionMultipleInvocation(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc) {expiry(doc.expiry); expiry(doc.secondExpiry)}`, 0)
		res1, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":100}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			if assert.NotNil(t, res1.Expiry) {
				assert.Equal(t, uint32(100), *res1.Expiry)
			}
		}

		res2, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"500"}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess failed") {
			assert.Equal(t, uint32(500), *res2.Expiry)
		}

		// Validate invalid expiry values log warning and don't set expiry
		res3, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":"abc"}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry:abc") {
			assert.True(t, res3.Expiry == nil)
		}

		// Invalid: non-numeric
		res4, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":["100", "200"]}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array") {
			assert.True(t, res4.Expiry == nil)
		}

		// Invalid: negative value
		res5, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":-100}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array") {
			assert.True(t, res5.Expiry == nil)
		}

		// Invalid - larger than uint32
		res6, err := mapper.MapToChannelsAndAccess(parse(`{"expiry":123456789012345}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array") {
			assert.True(t, res6.Expiry == nil)
		}

		// No expiry specified
		res7, err := mapper.MapToChannelsAndAccess(parse(`{"value":5}`), `{}`, emptyMetaMap(), noUser)
		if assert.NoError(t, err, "MapToChannelsAndAccess filed for expiry as array") {
			assert.True(t, res7.Expiry == nil)
		}
	})
}

func TestMetaMap(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc, meta) {channel(meta.xattrs.myxattr.channels);}`, 0)

		channels := []string{"chan1", "chan2"}
		attr, _ := json.Marshal(map[string]interface{}{
			"channels": channels,
		})
		metaMap := MetaMap{Key: "myxattr", JSONValue: attr}

		res, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, metaMap, noUser)
		require.NoError(t, err)
		assert.ElementsMatch(t, res.Channels.ToArray(), channels)
	})
}

func TestNilMetaMap(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		mapper := NewChannelMapper(vm, `function(doc, oldDoc, meta) {channel(meta.xattrs.myxattr.val);}`, 0)

		metaMap := MetaMap{}

		_, err := mapper.MapToChannelsAndAccess(parse(`{}`), `{}`, metaMap, noUser)
		if assert.Error(t, err) {
			message := err.Error()
			assert.True(t, strings.Contains(message, "TypeError: Cannot read properties of null (reading 'myxattr')") || strings.Contains(message, "TypeError: Cannot access member 'myxattr' of null"),
				"Unexpected error message: %s", message)
		}
	})
}

func TestCollectionSyncFunction(t *testing.T) {
	js.TestWithVMs(t, func(t *testing.T, vm js.VM) {
		testCases := []struct {
			docBody string
			name    string
		}{
			{
				docBody: `{"channels": ["foo", "bar", "baz"]}`,
				name:    "legacyDocBody",
			},
			{
				docBody: `{"x": "y"}`,
				name:    "irrelevantData",
			},
			{
				docBody: `{"x": "y"}`,
				name:    "irrelevantData",
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				collectionName := "barcollection"
				mapper := NewChannelMapper(vm, GetDefaultSyncFunction("fooscope", collectionName), 0)
				res, err := mapper.MapToChannelsAndAccess(parse(test.docBody), `{}`, emptyMetaMap(), noUser)
				require.NoError(t, err)
				require.Equal(t, BaseSetOf(t, collectionName), res.Channels)
			})
		}
	})
}

func TestGetDefaultSyncFunction(t *testing.T) {
	testCases := []struct {
		scopeName      string
		collectionName string
		syncFn         string
	}{
		{
			scopeName:      base.DefaultScope,
			collectionName: base.DefaultCollection,
			syncFn:         DocChannelsSyncFunction,
		},
		{
			scopeName:      "fooscope",
			collectionName: "barcollection",
			syncFn:         `function(doc){channel("barcollection");}`,
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s.%s", test.scopeName, test.collectionName), func(t *testing.T) {
			require.Equal(t, test.syncFn, GetDefaultSyncFunction(test.scopeName, test.collectionName))
		})
	}
}
