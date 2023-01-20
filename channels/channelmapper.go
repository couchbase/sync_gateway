//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
	pkgerrors "github.com/pkg/errors"
	v8 "rogchap.com/v8go"
	// Docs: https://pkg.go.dev/rogchap.com/v8go
)

// Maps user names (or role names prefixed with "role:") to arrays of channel or role names
type AccessMap map[string]base.Set

// If the provided principal name (in access grant format) is a role, returns the role name without prefix
func AccessNameToPrincipalName(accessPrincipalName string) (principalName string, isRole bool) {
	if strings.HasPrefix(accessPrincipalName, RoleAccessPrefix) {
		return accessPrincipalName[len(RoleAccessPrefix):], true
	}
	return accessPrincipalName, false
}

// Document metadata passed to the sync function
type MetaMap struct {
	Key       string // xattr key; "" if none
	JSONValue []byte // raw JSON value of xattr key; nil if none
}

func (meta MetaMap) MarshalJSON() ([]byte, error) {
	var result string
	if meta.Key == "" {
		result = `{"xattrs":null}`
	} else {
		result = fmt.Sprintf(`{"xattrs":{%q:`, meta.Key)
		if meta.JSONValue != nil {
			result += string(meta.JSONValue) + "}}"
		} else {
			result += "null}}"
		}
	}
	return []byte(result), nil
}

// Prefix used to identify roles in access grants
const RoleAccessPrefix = "role:"

/** Result of running a channel-mapper function. */
type ChannelMapperOutput struct {
	Channels  base.Set  // channels assigned to the document via channel() callback
	Roles     AccessMap // roles granted to users via role() callback
	Access    AccessMap // channels granted to users via access() callback
	Rejection error     // Error associated with failed validate (require callbacks, etc)
	Expiry    *uint32   // Expiry value specified by expiry() callback.  Standard CBS expiry format: seconds if less than 30 days, epoch time otherwise
}

// The object that runs the sync function.
type ChannelMapper struct {
	service  *js.Service
	timeout  time.Duration
	fnSource string
}

const DefaultSyncFunction = `function(doc){channel(doc.channels);}`

const kChannelMapperServiceName = "channelMapper"

// The JavaScript code run by the SyncRunner; the sync fn is copied into it.
// See wrappedFuncSource().
//
//go:embed sync_fn_wrapper.js
var kSyncFnHostScript string

// Creates a ChannelMapper.
func NewChannelMapper(owner js.ServiceHost, fnSource string, timeout time.Duration) *ChannelMapper {
	service := js.NewService(owner, kChannelMapperServiceName, wrappedFuncSource(fnSource))
	return &ChannelMapper{
		service:  service,
		timeout:  timeout,
		fnSource: fnSource,
	}
}

// used by tests.
func newChannelMapperWithVMs(fnSource string, timeout time.Duration) *ChannelMapper {
	return NewChannelMapper(js.NewVM(), fnSource, timeout)
}

func (cm *ChannelMapper) closeVMs() {
	cm.service.Host().Close()
}

// Creates a ChannelMapper with the default sync function. (Used by tests)
func NewDefaultChannelMapper(vms *js.VMPool) *ChannelMapper {
	return NewChannelMapper(vms, DefaultSyncFunction, time.Duration(base.DefaultJavascriptTimeoutSecs)*time.Second)
}

func (mapper *ChannelMapper) Function() string {
	return mapper.fnSource
}

// This function is DEPRECATED. It's currently used in some tests that can't easily be changed.
// Its current implementation is a kludge, and it shouldn't be used in production.
func (mapper *ChannelMapper) SetFunction(fnSource string) error {
	mapper.fnSource = fnSource
	mapper.service = js.NewService(mapper.service.Host(), kChannelMapperServiceName, wrappedFuncSource(fnSource))
	return nil
}

func wrappedFuncSource(funcSource string) string {
	return fmt.Sprintf(
		kSyncFnHostScript,
		funcSource,
		base.SyncFnErrorAdminRequired,
		base.SyncFnErrorWrongUser,
		base.SyncFnErrorMissingRole,
		base.SyncFnErrorMissingChannelAccess,
	)
}

// Runs the sync function. Thread-safe.
func (mapper *ChannelMapper) MapToChannelsAndAccess(body map[string]any, oldBodyJSON string, metaMap MetaMap, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	docID, _ := body["_id"].(string)
	revID, _ := body["_rev"].(string)
	return mapper.MapToChannelsAndAccess2(docID, revID, string(bodyJSON), oldBodyJSON, metaMap, userCtx)
}

// Runs the sync function. Thread-safe.
func (mapper *ChannelMapper) MapToChannelsAndAccess2(docID string, revID string, bodyJSON string, oldBodyJSON string, metaMap MetaMap, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	result, err := mapper.service.WithRunner(func(runner *js.Runner) (any, error) {
		ctx := context.Background()
		if mapper.timeout > 0 {
			var cancelFn context.CancelFunc
			ctx, cancelFn = context.WithTimeout(ctx, mapper.timeout)
			defer cancelFn()
		}
		runner.SetContext(ctx)
		return callSyncFn(runner, docID, revID, bodyJSON, oldBodyJSON, metaMap, userCtx)
	})
	return result.(*ChannelMapperOutput), err
}

func callSyncFn(runner *js.Runner,
	docID string,
	revID string,
	bodyJSON string,
	oldBodyJSON string,
	metaMap MetaMap,
	userCtx map[string]interface{}) (*ChannelMapperOutput, error) {

	// Call the sync fn:
	args, err := runner.ConvertArgs(docID, revID, js.JSONString(bodyJSON), oldBodyJSON, metaMap.Key, string(metaMap.JSONValue), userCtx)
	if err != nil {
		return nil, err
	}
	jsResult, err := runner.RunAsObject(args...)
	if err != nil {
		return nil, err
	}

	// Convert the JavaScript jsResult object to a ChannelMapperOutput:
	output := &ChannelMapperOutput{}

	if rej, err := jsResult.Get("rejectionStatus"); err == nil {
		if status := rej.Integer(); status >= 400 {
			var message string
			if msg, err := jsResult.Get("rejectionMessage"); err == nil {
				message = msg.String()
			}
			output.Rejection = base.HTTPErrorf(int(status), message)
			return output, err
		}
	}

	if jsChannels, err := jsResult.Get("channels"); err != nil {
		return nil, err
	} else if channels, err := js.StringArrayToGo(jsChannels); err != nil {
		return nil, err
	} else if output.Channels, err = SetFromArray(channels, ExpandStar); err != nil {
		return nil, err
	}

	if output.Access, err = compileJSAccessMap(jsResult, "access", ""); err != nil {
		return nil, err
	}

	if output.Roles, err = compileJSAccessMap(jsResult, "roles", "role:"); err != nil {
		return nil, err
	}

	if jsExp, err := jsResult.Get("expiry"); err == nil {
		if expiry, reflectErr := reflectExpiry(jsExp); reflectErr == nil {
			output.Expiry = expiry
		} else {
			base.WarnfCtx(runner.ContextOrDefault(), "SyncRunner: Invalid value passed to expiry().  Value:%+v ", jsExp)
		}
	}
	return output, nil
}

//////// UTILITIES:

// Shortcut to get an object property as an object.
func v8GetObject(obj *v8.Object, key string) (*v8.Object, error) {
	if jsVal, err := obj.Get(key); err != nil || jsVal.IsUndefined() {
		return nil, err
	} else {
		return jsVal.AsObject()
	}
}

// Converts the "access" or "roles" property of the result into an AccessMap.
func compileJSAccessMap(jsResult *v8.Object, key string, prefix string) (AccessMap, error) {
	jsMap, err := v8GetObject(jsResult, key)
	if err != nil || jsMap == nil {
		return nil, err
	}
	jsKeys, err := v8GetObject(jsResult, key+"Keys")
	if err != nil {
		return nil, err
	}
	access := make(AccessMap)
	for i := uint32(0); jsKeys.HasIdx(i); i++ {
		userVal, err := jsKeys.GetIdx(i)
		if err != nil {
			return nil, err
		}
		user := userVal.String()
		jsItems, err := jsMap.Get(user)
		if err != nil {
			return nil, err
		}
		values, err := js.StringArrayToGo(jsItems)
		if err != nil {
			return nil, err
		} else if len(values) > 0 {
			if err = stripMandatoryPrefix(values, prefix); err != nil {
				return nil, err
			}
			if access[user], err = SetFromArray(values, RemoveStar); err != nil {
				return nil, err
			}
		}
	}
	return access, nil
}

// Strips a prefix from every item of an array.
func stripMandatoryPrefix(values []string, prefix string) error {
	if prefix != "" {
		for i, value := range values {
			if strings.HasPrefix(value, prefix) {
				values[i] = value[len(prefix):]
			} else {
				return base.RedactErrorf("Value %q does not begin with %q", base.UD(value), base.UD(prefix))
			}
		}
	}
	return nil
}

// Adapted from base.ReflectExpiry
func reflectExpiry(rawExpiry *v8.Value) (*uint32, error) {
	if rawExpiry.IsNumber() {
		return validateFloatAsUInt32Expiry(rawExpiry.Number())
	} else if rawExpiry.IsString() {
		expiry := rawExpiry.String()
		// First check if it's a numeric string
		expInt, err := strconv.ParseInt(expiry, 10, 32)
		if err == nil {
			return base.ValidateUint32Expiry(expInt)
		}
		// Check if it's an ISO-8601 date
		expRFC3339, err := time.Parse(time.RFC3339, expiry)
		if err == nil {
			return base.ValidateUint32Expiry(expRFC3339.Unix())
		} else {
			return nil, pkgerrors.Wrapf(err, "Unable to parse expiry %s as either numeric or date expiry", rawExpiry)
		}
	} else if rawExpiry.IsNullOrUndefined() {
		return nil, nil
	} else {
		return nil, fmt.Errorf("unrecognized expiry format")
	}
}

// Adapted from base.ValidateUint32Expiry
func validateFloatAsUInt32Expiry(expiry float64) (*uint32, error) {
	if expiry < 0 || expiry > math.MaxUint32 {
		return nil, fmt.Errorf("expiry value is not within valid range: %f", expiry)
	}
	uint32Expiry := uint32(expiry)
	return &uint32Expiry, nil
}
