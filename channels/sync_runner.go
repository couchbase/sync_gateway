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
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

// Prefix used to identify roles in access grants
const RoleAccessPrefix = "role:"

const funcWrapper = `
	function() {

		var realUserCtx, shouldValidate;
		var syncFn = %s;

		function makeArray(maybeArray) {
			if (Array.isArray(maybeArray)) {
				return maybeArray;
			} else {
				return [maybeArray];
			}
		}

		function inArray(string, array) {
			return array.indexOf(string) != -1;
		}

		function anyInArray(any, array) {
			for (var i = 0; i < any.length; ++i) {
				if (inArray(any[i], array))
					return true;
			}
			return false;
		}

		function anyKeysInArray(any, array) {
			for (var key in any) {
				if (inArray(key, array))
					return true;
			}
			return false;
		}

		function requireAdmin() {
			if (shouldValidate)
				throw({forbidden: "admin required"});
		}

		function requireUser(names) {
				if (!shouldValidate) return;
				names = makeArray(names);
				if (!inArray(realUserCtx.name, names))
					throw({forbidden: "wrong user"});
		}

		function requireRole(roles) {
				if (!shouldValidate) return;
				roles = makeArray(roles);
				if (!anyKeysInArray(realUserCtx.roles, roles))
					throw({forbidden: "missing role"});
		}

		function requireAccess(channels) {
				if (!shouldValidate) return;
				channels = makeArray(channels);
				if (!anyInArray(realUserCtx.channels, channels))
					throw({forbidden: "missing channel access"});
		}

		return function (newDoc, oldDoc, _realUserCtx) {
			realUserCtx = _realUserCtx;

			if (oldDoc) {
				oldDoc._id = newDoc._id;
			}

			// Proxy userCtx that allows queries but not direct access to user/roles:
			shouldValidate = (realUserCtx != null && realUserCtx.name != null);

			try {
				syncFn(newDoc, oldDoc);
			} catch(x) {
				if (x.forbidden)
				reject(403, x.forbidden);
				else if (x.unauthorized)
				reject(401, x.unauthorized);
				else
				throw(x);
			}
		}
	}()`

// An object that runs a specific JS sync() function. Not thread-safe!
type SyncRunner struct {
	sgbucket.JSRunner                      // "Superclass"
	output            *ChannelMapperOutput // Results being accumulated while the JS fn runs
	channels          []string
	access            map[string][]string // channels granted to users via access() callback
	roles             map[string][]string // roles granted to users via role() callback
	expiry            *uint32             // document expiry (in seconds) specified via expiry() callback
}

func NewSyncRunner(funcSource string) (*SyncRunner, error) {
	funcSource = fmt.Sprintf(funcWrapper, funcSource)
	runner := &SyncRunner{}
	err := runner.Init(funcSource)
	if err != nil {
		return nil, err
	}

	// Implementation of the 'channel()' callback:
	runner.DefineNativeFunction("channel", func(call otto.FunctionCall) otto.Value {
		for _, arg := range call.ArgumentList {
			if strings := ottoValueToStringArray(arg); strings != nil {
				runner.channels = append(runner.channels, strings...)
			}
		}
		return otto.UndefinedValue()
	})

	// Implementation of the 'access()' callback:
	runner.DefineNativeFunction("access", func(call otto.FunctionCall) otto.Value {
		return runner.addValueForUser(call.Argument(0), call.Argument(1), runner.access)
	})

	// Implementation of the 'role()' callback:
	runner.DefineNativeFunction("role", func(call otto.FunctionCall) otto.Value {
		return runner.addValueForUser(call.Argument(0), call.Argument(1), runner.roles)
	})

	// Implementation of the 'reject()' callback:
	runner.DefineNativeFunction("reject", func(call otto.FunctionCall) otto.Value {
		if runner.output.Rejection == nil {
			if status, err := call.Argument(0).ToInteger(); err == nil && status >= 400 {
				var message string
				if len(call.ArgumentList) > 1 {
					message = call.Argument(1).String()
				}
				runner.output.Rejection = base.HTTPErrorf(int(status), message)
			}
		}
		return otto.UndefinedValue()
	})

	// Implementation of the 'expiry()' callback:
	runner.DefineNativeFunction("expiry", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) > 0 {
			rawExpiry, exportErr := call.Argument(0).Export()
			if exportErr != nil {
				base.Warnf(base.KeyAll, "SyncRunner: Unable to export expiry parameter: %v Error: %s", call.Argument(0), exportErr)
				return otto.UndefinedValue()
			}

			// Called expiry with null/undefined value - ignore
			if rawExpiry == nil || call.Argument(0).IsUndefined() {
				return otto.UndefinedValue()
			}

			expiry, reflectErr := base.ReflectExpiry(rawExpiry)
			if reflectErr != nil {
				base.Warnf(base.KeyAll, "SyncRunner: Invalid value passed to expiry().  Value:%+v ", call.Argument(0))
				return otto.UndefinedValue()
			}

			runner.expiry = expiry
		}
		return otto.UndefinedValue()
	})

	runner.Before = func() {
		runner.output = &ChannelMapperOutput{}
		runner.channels = []string{}
		runner.access = map[string][]string{}
		runner.roles = map[string][]string{}
		runner.expiry = nil
	}
	runner.After = func(result otto.Value, err error) (interface{}, error) {
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
	return runner, nil
}

func (runner *SyncRunner) SetFunction(funcSource string) (bool, error) {
	funcSource = fmt.Sprintf(funcWrapper, funcSource)
	return runner.JSRunner.SetFunction(funcSource)
}

// Common implementation of 'access()' and 'role()' callbacks
func (runner *SyncRunner) addValueForUser(user otto.Value, value otto.Value, mapping map[string][]string) otto.Value {
	valueStrings := ottoValueToStringArray(value)
	if len(valueStrings) > 0 {
		for _, name := range ottoValueToStringArray(user) {
			mapping[name] = append(mapping[name], valueStrings...)
		}
	}
	return otto.UndefinedValue()
}

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

// If the provided principal name (in access grant format) is a role, returns the role name without prefix
func AccessNameToPrincipalName(accessPrincipalName string) (principalName string, isRole bool) {
	if strings.HasPrefix(accessPrincipalName, RoleAccessPrefix) {
		return accessPrincipalName[len(RoleAccessPrefix):], true
	}
	return accessPrincipalName, false
}

// Converts a JS string or array into a Go string array.
func ottoValueToStringArray(value otto.Value) []string {
	nativeValue, _ := value.Export()

	result := base.ValueToStringArray(nativeValue)

	if result == nil && !value.IsNull() && !value.IsUndefined() {
		base.Warnf(base.KeyAll, "SyncRunner: Non-string, non-array passed to JS callback: %s", base.UD(value))
	}

	return result
}
