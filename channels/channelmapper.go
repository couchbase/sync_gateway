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
	"strings"
	"time"

	"github.com/couchbase/sg-bucket/js"
	"github.com/couchbase/sync_gateway/base"
)

/** The data given to the channel-mapper function. */
type ChannelMapperInput struct {
	DocID   string
	RevID   string
	Body    string
	OldBody string
	Deleted bool
	Meta    MetaMap
	UserCtx map[string]interface{}
}

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

// DocChannelsSyncFunction is the default sync function used prior to collections.
const DocChannelsSyncFunction = `function(doc){channel(doc.channels);}`

// The JavaScript code run by the SyncRunner; the sync fn is copied into it.
// See wrappedFuncSource().
//
//go:embed sync_fn_wrapper_older.js
var kSyncFnHostScriptOlder string

//go:embed sync_fn_wrapper_modern.js
var kSyncFnHostScriptModern string

// NewChannelMapper creates a new channel mapper with a specific javascript function and a timeout. A zero value timeout will never timeout.
func NewChannelMapper(owner js.ServiceHost, fnSource string, timeout time.Duration) *ChannelMapper {
	service := js.NewService(owner, "channelMapper", wrappedFuncSource(fnSource, owner))
	return &ChannelMapper{
		service:  service,
		timeout:  timeout,
		fnSource: fnSource,
	}
}

// GetDefaultSyncFunction returns a sync function. The case of default collection will return a different sync function than one for collections.
func GetDefaultSyncFunction(scopeName, collectionName string) string {
	if base.IsDefaultCollection(scopeName, collectionName) {
		return DocChannelsSyncFunction
	}
	return `function(doc){channel("` + collectionName + `");}`

}

func (mapper *ChannelMapper) Function() string {
	return mapper.fnSource
}

// This function is DEPRECATED. It's currently used in some tests that can't easily be changed.
// Its current implementation is a kludge, and it shouldn't be used in production.
func (mapper *ChannelMapper) SetFunction(fnSource string) error {
	host := mapper.service.Host()
	mapper.fnSource = fnSource
	mapper.service = js.NewService(host, "channelMapper", wrappedFuncSource(fnSource, host))
	return nil
}

func wrappedFuncSource(funcSource string, host js.ServiceHost) string {
	// Use different wrapper scripts for modern vs older JS.
	script := kSyncFnHostScriptOlder
	if host.Engine().LanguageVersion() >= js.ES2015 {
		script = kSyncFnHostScriptModern
	}
	return fmt.Sprintf(
		script,
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
	input := ChannelMapperInput{
		DocID:   docID,
		RevID:   revID,
		Body:    string(bodyJSON),
		OldBody: oldBodyJSON,
		Meta:    metaMap,
		UserCtx: userCtx,
	}
	return mapper.MapToChannelsAndAccess2(input)
}

// Runs the sync function. Thread-safe.
func (mapper *ChannelMapper) MapToChannelsAndAccess2(input ChannelMapperInput) (*ChannelMapperOutput, error) {
	result, err := mapper.service.WithRunner(func(runner js.Runner) (any, error) {
		ctx := context.Background()
		if mapper.timeout > 0 {
			var cancelFn context.CancelFunc
			ctx, cancelFn = context.WithTimeout(ctx, mapper.timeout)
			defer cancelFn()
		}
		runner.SetContext(ctx)
		return callSyncFn(runner, input)
	})
	if err != nil {
		return nil, err
	}
	return result.(*ChannelMapperOutput), nil
}

// Parsed version of the JSON object returned by the sync-fn wrapper.
type syncFnResult = struct {
	RejectionStatus  int
	RejectionMessage string
	Channels         []string
	Access           map[string][]string
	Roles            map[string][]string
	Expiry           any
}

func callSyncFn(runner js.Runner, in ChannelMapperInput) (*ChannelMapperOutput, error) {
	// Call the sync fn:
	jsResult, err := runner.Run(in.DocID, in.RevID, in.Deleted, js.JSONString(in.Body), in.OldBody, in.Meta.Key, string(in.Meta.JSONValue), in.UserCtx)
	if err != nil {
		return nil, fmt.Errorf("unexpected error calling sync fn: %+w", err)
	}
	var result syncFnResult
	if err := json.Unmarshal([]byte(jsResult.(string)), &result); err != nil {
		return nil, fmt.Errorf("unparseable output from sync-fn wrapper: %w", err)
	}

	// Convert the JSON result to a ChannelMapperOutput:
	output := &ChannelMapperOutput{}
	if result.RejectionStatus >= 400 {
		output.Rejection = base.HTTPErrorf(result.RejectionStatus, result.RejectionMessage)
		return output, nil
	}
	if result.Channels != nil {
		if output.Channels, err = SetFromArray(result.Channels, ExpandStar); err != nil {
			return nil, err
		}
	}
	if output.Access, err = compileJSAccessMap(result.Access, ""); err != nil {
		return nil, err
	}
	if output.Roles, err = compileJSAccessMap(result.Roles, "role:"); err != nil {
		return nil, err
	}
	if result.Expiry != nil {
		if expiry, reflectErr := base.ReflectExpiry(result.Expiry); reflectErr == nil {
			output.Expiry = expiry
		} else {
			base.WarnfCtx(runner.ContextOrDefault(), "sync function set invalid expiry value `%+v` ", result.Expiry)
		}
	}
	return output, nil
}

//////// UTILITIES:

// Converts the "access" or "roles" property of the result into an AccessMap.
func compileJSAccessMap(result map[string][]string, prefix string) (AccessMap, error) {
	var err error
	access := make(AccessMap)
	for user, values := range result {
		if len(values) > 0 {
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
