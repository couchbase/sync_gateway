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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
	pkgerrors "github.com/pkg/errors"
	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// An object that runs a specific JS sync() function. Wraps a js.Runner.
type syncRunner struct {
	*js.Runner                      // "Superclass"
	output     *ChannelMapperOutput // Results being accumulated while the JS fn runs
	channels   []string             // channels granted via channel() callback
	access     map[string][]string  // channels granted to users via access() callback
	roles      map[string][]string  // roles granted to users via role() callback
	expiry     *uint32              // document expiry (in seconds) specified via expiry() callback
}

// Runs the sync function.
func (runner *syncRunner) call(body map[string]interface{},
	oldBodyJSON string,
	metaMap map[string]interface{},
	userCtx map[string]interface{}) (*ChannelMapperOutput, error) {

	// Clear the state:
	runner.output = &ChannelMapperOutput{}
	runner.channels = []string{}
	runner.access = map[string][]string{}
	runner.roles = map[string][]string{}
	runner.expiry = nil

	// Call the sync fn:
	if oldBodyJSON == "" {
		oldBodyJSON = "null"
	}
	args, err := runner.ConvertArgs(body, js.JSONString(oldBodyJSON), metaMap, userCtx)
	if err != nil {
		return nil, err
	}
	_, err = runner.Run(args...)

	// Extract the output:
	output := runner.output
	runner.output = nil
	if err == nil {
		output.Channels, err = SetFromArray(runner.channels, ExpandStar)
		if err == nil {
			output.Access, err = compileAccessMap(runner.access, "")
			if err == nil {
				output.Roles, err = compileAccessMap(runner.roles, RoleAccessPrefix)
			}
		}
		if runner.expiry != nil {
			output.Expiry = runner.expiry
		}
	}
	return output, err
}

//////// CALLBACKS:

func (runner *syncRunner) channelCallback(args []*v8.Value) (any, error) {
	for _, arg := range args {
		if strings := v8ValueToStringArray(arg, runner.ContextOrDefault()); strings != nil {
			runner.channels = append(runner.channels, strings...)
		}
	}
	return nil, nil
}

func (runner *syncRunner) accessCallback(args []*v8.Value) (any, error) {
	return runner.addValueForUser(args[0], args[1], runner.access), nil
}

func (runner *syncRunner) roleCallback(args []*v8.Value) (any, error) {
	return runner.addValueForUser(args[0], args[1], runner.roles), nil
}

func (runner *syncRunner) rejectCallback(args []*v8.Value) (any, error) {
	if runner.output.Rejection == nil {
		if status := args[0].Integer(); status >= 400 {
			var message string
			if len(args) > 1 {
				message = args[1].String()
			}
			runner.output.Rejection = base.HTTPErrorf(int(status), message)
		}
	}
	return nil, nil
}

func (runner *syncRunner) expiryCallback(args []*v8.Value) {
	if len(args) > 0 {
		expiry, reflectErr := reflectExpiry(args[0])
		if reflectErr != nil {
			base.WarnfCtx(runner.ContextOrDefault(), "SyncRunner: Invalid value passed to expiry().  Value:%+v ", args[0])
			return
		} else if expiry != nil {
			runner.expiry = expiry
		}
	}
}

// Common implementation of 'access()' and 'role()' callbacks
func (runner *syncRunner) addValueForUser(user *v8.Value, value *v8.Value, mapping map[string][]string) *v8.Value {
	valueStrings := v8ValueToStringArray(value, runner.ContextOrDefault())
	if len(valueStrings) > 0 {
		for _, name := range v8ValueToStringArray(user, runner.ContextOrDefault()) {
			mapping[name] = append(mapping[name], valueStrings...)
		}
	}
	return nil
}

//////// UTILITIES:

func compileAccessMap(input map[string][]string, prefix string) (AccessMap, error) {
	access := make(AccessMap, len(input))
	for name, values := range input {
		// If a prefix is specified, strip it from all values or return error if missing:
		if prefix != "" {
			for i, value := range values {
				if strings.HasPrefix(value, prefix) {
					values[i] = value[len(prefix):]
				} else {
					return nil, base.RedactErrorf("Value %q does not begin with %q", base.UD(value), base.UD(prefix))
				}
			}
		}
		var err error
		if access[name], err = SetFromArray(values, RemoveStar); err != nil {
			return nil, err
		}
	}
	return access, nil
}

// Converts a JS string or array into a Go string array.
func v8ValueToStringArray(value *v8.Value, ctx context.Context) []string {
	var result []string
	var nonStrings []any

	// Adapted from base.ValueToStringArray:
	if value.IsNullOrUndefined() {
		return result
	} else if value.IsString() {
		result = []string{value.String()}
	} else if value.IsArray() {
		array := value.Object()
		for i := uint32(0); array.HasIdx(i); i++ {
			if item, _ := array.GetIdx(i); item.IsString() {
				result = append(result, item.String())
			} else {
				nonStrings = append(nonStrings, item)
			}
		}
	} else {
		nonStrings = []any{value}
	}

	if nonStrings != nil {
		base.WarnfCtx(ctx, "Channel names must be string values only. Ignoring non-string channels: %s", base.UD(nonStrings))
	}
	return result
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
