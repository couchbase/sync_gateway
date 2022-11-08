/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// CONFIGURATION TYPES:

// Top level user-function config object: the map of names to queries.
type FunctionsConfig struct {
	Definitions      FunctionsDefs `json:"definitions"`                  // The function definitions
	MaxFunctionCount *int          `json:"max_function_count,omitempty"` // Maximum number of functions
	MaxCodeSize      *int          `json:"max_code_size,omitempty"`      // Maximum length (in bytes) of a function's code
	MaxRequestSize   *int          `json:"max_request_size,omitempty"`   // Maximum size of the JSON-encoded function arguments
}

type FunctionsDefs = map[string]*FunctionConfig

// Defines a JavaScript or N1QL function that a client can invoke by name.
// (Its name is the key in the FunctionsDefs.)
type FunctionConfig struct {
	Type     string   `json:"type"`
	Code     string   `json:"code"`               // Javascript function or N1QL 'SELECT'
	Args     []string `json:"args,omitempty"`     // Names of parameters/arguments
	Mutating bool     `json:"mutating,omitempty"` // Allowed to modify database?
	Allow    *Allow   `json:"allow,omitempty"`    // Permissions (admin-only if nil)
}

// Permissions for a function
type Allow struct {
	Channels []string `json:"channels,omitempty"` // Names of channel(s) that grant access
	Roles    []string `json:"roles,omitempty"`    // Names of role(s) that have access
	Users    base.Set `json:"users,omitempty"`    // Names of user(s) that have access
}

//////// INITIALIZATION:

// implements UserFunction. Used for both JS and N1QL.
type functionImpl struct {
	*FunctionConfig                    // Inherits from FunctionConfig
	name            string             // Name of function
	typeName        string             // "function" or "resolver"
	checkArgs       bool               // If true, args must be checked against Args
	allowByDefault  bool               // If true, a missing Allow means allow, not forbid
	compiled        *sgbucket.JSServer // Compiled form of the function; nil for N1QL
}

// Compiles the functions in a UserFunctionConfigMap, returning UserFunctions.
func CompileFunctions(config FunctionsConfig) (*db.UserFunctions, error) {
	if config.MaxFunctionCount != nil && len(config.Definitions) > *config.MaxFunctionCount {
		return nil, fmt.Errorf("too many functions declared (> %d)", *config.MaxFunctionCount)
	}
	fns := db.UserFunctions{
		MaxRequestSize: config.MaxRequestSize,
		Definitions:    map[string]db.UserFunction{},
	}
	var multiError *base.MultiError
	for name, fnConfig := range config.Definitions {
		if config.MaxCodeSize != nil && len(fnConfig.Code) > *config.MaxCodeSize {
			multiError = multiError.Append(fmt.Errorf("function code too large (> %d bytes)", *config.MaxCodeSize))
		} else if userFn, err := compileFunction(name, "function", fnConfig); err == nil {
			fns.Definitions[name] = userFn
		} else {
			multiError = multiError.Append(err)
		}
	}
	return &fns, multiError.ErrorOrNil()
}

// Validates a FunctionsConfig.
func ValidateFunctions(ctx context.Context, config FunctionsConfig) error {
	_, err := CompileFunctions(config)
	return err
}

// Creates a functionImpl from a UserFunctionConfig.
func compileFunction(name string, typeName string, fnConfig *FunctionConfig) (*functionImpl, error) {
	userFn := &functionImpl{
		FunctionConfig: fnConfig,
		name:           name,
		typeName:       typeName,
		checkArgs:      true,
	}
	var err error
	switch fnConfig.Type {
	case "javascript":
		userFn.compiled, err = newFunctionJSServer(name, typeName, fnConfig.Code)
	case "query":
		err = validateN1QLQuery(fnConfig.Code)
		if err != nil {
			err = fmt.Errorf("%s %q invalid query: %v", typeName, name, err)
		}
	default:
		err = fmt.Errorf("%s %q has unrecognized 'type' %q", typeName, name, fnConfig.Type)
	}

	if err == nil {
		if err = fnConfig.validateAllow(); err != nil {
			err = base.HTTPErrorf(http.StatusInternalServerError, "%s %q has an illegal 'allow' pattern: %v", typeName, name, err)

		}
	}
	return userFn, err
}

func (fn *functionImpl) Name() string {
	return fn.name
}

func (fn *functionImpl) isN1QL() bool {
	return fn.compiled == nil
}

func (fn *functionImpl) N1QLQueryName() (string, bool) {
	if fn.isN1QL() {
		return db.QueryTypeUserFunctionPrefix + fn.name, true
	} else {
		return "", false
	}
}

// Creates an Invocation of a UserFunction.
func (fn *functionImpl) Invoke(db *db.Database, args map[string]any, mutationAllowed bool, ctx context.Context) (db.UserFunctionInvocation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("missing context to UserFunction.Invoke")
	}

	if err := db.CheckTimeout(ctx); err != nil {
		return nil, err
	}

	if args == nil {
		args = map[string]any{}
	}
	if fn.checkArgs {
		if err := fn.checkArguments(args); err != nil {
			return nil, err
		}
	}

	// Check that the user is authorized:
	if fn.Allow != nil || !fn.allowByDefault {
		if err := fn.authorize(db.User(), args); err != nil {
			return nil, err
		}
	}

	if (!fn.Mutating || !mutationAllowed) && ctx.Value(readOnlyKey) == nil {
		// Add a value to the Context to indicate that mutations aren't allowed:
		var why string
		if !fn.Mutating {
			why = fmt.Sprintf("%s %q", fn.typeName, fn.name)
		} else {
			why = "a read-only API call"
		}
		ctx = context.WithValue(ctx, readOnlyKey, why)
	}

	if fn.isN1QL() {
		return &n1qlInvocation{
			functionImpl: fn,
			db:           db,
			args:         args,
			ctx:          ctx,
		}, nil
	} else {
		return &jsInvocation{
			functionImpl: fn,
			db:           db,
			ctx:          ctx,
			args:         args,
		}, nil
	}
}

//////// FUNCTION PARAMETERS/ARGUMENTS:

// Checks that `args` contains exactly the same keys as the list `parameterNames`.
// Adds the special "context" parameter containing user data.
func (fn *functionImpl) checkArguments(args map[string]any) error {
	// Make sure each specified parameter has a value in `args`:
	for _, paramName := range fn.Args {
		if _, found := args[paramName]; !found {
			return base.HTTPErrorf(http.StatusBadRequest, "%s %q parameter %q is missing", fn.typeName, fn.name, paramName)
		}
	}

	// Any extra parameters in `args` are illegal:
	if len(args) > len(fn.Args) {
		for _, paramName := range fn.Args {
			delete(args, paramName)
		}
		for badKey := range args {
			return base.HTTPErrorf(http.StatusBadRequest, "%s %q has no parameter %q", fn.typeName, fn.name, badKey)
		}
	}
	return nil
}

//////// AUTHORIZATION:

// Looks for bad '$' patterns in the config's Allow strings.
func (fn *FunctionConfig) validateAllow() error {
	if fn.Allow != nil {
		// Construct an args object with a value for each declared argument:
		fakeArgs := map[string]any{}
		for _, argName := range fn.Args {
			fakeArgs[argName] = "x"
		}
		// Subroutine that tests a pattern by trying to expand it with the args.
		// If the result is a 500 error, something's wrong with the pattern itself.
		checkPattern := func(pattern string) error {
			_, err := expandPattern(pattern, fakeArgs, nil)
			if err, ok := err.(*base.HTTPError); ok {
				if err != nil && err.Status >= http.StatusInternalServerError {
					return err
				}
			}
			return nil
		}
		// Test each role and channel string:
		for _, str := range fn.Allow.Roles {
			if err := checkPattern(str); err != nil {
				return err
			}
		}
		for _, str := range fn.Allow.Channels {
			if err := checkPattern(str); err != nil {
				return err
			}
		}
	}
	return nil
}

// Authorizes a User against the function config's Allow object:
// - The user's name must be contained in Users, OR
// - The user must have a role contained in Roles, OR
// - The user must have access to a channel contained in Channels.
// In Roles and Channels, patterns of the form `${param}` are expanded using `args` and `user`.
func (fn *functionImpl) authorize(user auth.User, args map[string]any) error {
	allow := fn.Allow
	if user == nil {
		return nil // User is admin
	} else if allow != nil { // No Allow object means admin-only
		if allow.Users.Contains(user.Name()) {
			return nil // User is explicitly allowed
		}
		userRoles := user.RoleNames()
		for _, rolePattern := range allow.Roles {
			if role, err := expandPattern(rolePattern, args, user); err != nil {
				return err
			} else if userRoles.Contains(role) {
				return nil // User has one of the allowed roles
			}
		}
		// Check if the user has access to one of the given channels.
		for _, channelPattern := range allow.Channels {
			if channelPattern == channels.AllChannelWildcard {
				return nil
			} else if channel, err := expandPattern(channelPattern, args, user); err != nil {
				return err
			} else if user.CanSeeChannel(channel) {
				return nil // User has access to one of the allowed channels
			}
		}
	}
	return user.UnauthError(fmt.Sprintf("you are not allowed to call %s %q", fn.typeName, fn.name))
}

// Expands patterns of the form `${param}` in `pattern`, looking up each such
// `param` in the `args` map and substituting its value.
// (`\$` is replaced with `$`.)
// It is an error if any `param` has no value, or if its value is not a string or integer.
func expandPattern(pattern string, args map[string]any, user auth.User) (string, error) {
	if strings.IndexByte(pattern, '$') < 0 {
		return pattern, nil
	}
	badConfig := false
	var err error
	channel := kChannelPropertyRegexp.ReplaceAllStringFunc(pattern, func(matched string) string {
		if badConfig {
			return ""
		} else if matched == "\\$" {
			return "$"
		} else if !strings.HasPrefix(matched, "${") || !strings.HasSuffix(matched, "}") {
			err = base.HTTPErrorf(http.StatusInternalServerError, "missing curly-brace in pattern %q", matched)
			badConfig = true
			return ""
		}
		arg := matched[2 : len(matched)-1]

		// Look up the argument:
		if strings.HasPrefix(arg, "args.") {
			var rval reflect.Value
			rval, err = evalKeyPath(args, arg[5:])
			if err != nil {
				if err := err.(*base.HTTPError); err != nil && err.Status >= http.StatusInternalServerError {
					badConfig = true
				}
				return ""
			}

			// Convert `rval` to a string:
			for rval.Kind() == reflect.Interface {
				rval = rval.Elem()
			}
			if rval.Kind() == reflect.String {
				return rval.String()
			} else if rval.CanInt() || rval.CanUint() || rval.CanFloat() {
				return fmt.Sprintf("%v", rval)
			} else {
				err = base.HTTPErrorf(http.StatusBadRequest, "argument %q must be a string or number, not %v", arg, rval.Kind())
				return ""
			}
		} else if arg == "context.user.name" {
			if user == nil {
				return ""
			} else {
				return user.Name()
			}
		} else {
			err = base.HTTPErrorf(http.StatusInternalServerError, "invalid variable expression %q", matched)
			badConfig = true
			return ""
		}
	})
	return channel, err
}

// Evaluates a "key path", like "points[3].x.y" on a JSON-based map.
func evalKeyPath(root map[string]any, keyPath string) (reflect.Value, error) {
	// Handle the first path component specially because we can access `root` without reflection:
	var value reflect.Value
	i := strings.IndexAny(keyPath, ".[")
	if i < 0 {
		i = len(keyPath)
	}
	key := keyPath[0:i]
	keyPath = keyPath[i:]
	firstVal := root[key]
	if firstVal == nil {
		return value, base.HTTPErrorf(http.StatusInternalServerError, "parameter %q is not declared in 'args'", key)
	}

	value = reflect.ValueOf(firstVal)
	if len(keyPath) == 0 {
		return value, nil
	}

	for len(keyPath) > 0 {
		ch := keyPath[0]
		keyPath = keyPath[1:]
		if ch == '.' {
			i = strings.IndexAny(keyPath, ".[")
			if i < 0 {
				i = len(keyPath)
			}
			key = keyPath[0:i]
			keyPath = keyPath[i:]

			if value.Kind() != reflect.Map {
				return value, base.HTTPErrorf(http.StatusBadRequest, "value is not a map")
			}
			value = value.MapIndex(reflect.ValueOf(key))
		} else if ch == '[' {
			i = strings.IndexByte(keyPath, ']')
			if i < 0 {
				return value, base.HTTPErrorf(http.StatusInternalServerError, "missing ']")
			}
			key = keyPath[0:i]
			keyPath = keyPath[i+1:]

			index, err := strconv.ParseUint(key, 10, 8)
			if err != nil {
				return value, err
			}
			if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
				return value, base.HTTPErrorf(http.StatusBadRequest, "value is a %v not an array", value.Type())
			} else if uint64(value.Len()) <= index {
				return value, base.HTTPErrorf(http.StatusBadRequest, "array index out of range")
			}
			value = value.Index(int(index))
		} else {
			return value, base.HTTPErrorf(http.StatusInternalServerError, "invalid character after a ']'")
		}
		for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
			value = value.Elem()
		}
	}
	return value, nil
}

// Regexp that matches a property pattern `${...}`, or a backslash-escaped `$`.
var kChannelPropertyRegexp = regexp.MustCompile(`(\\\$)|(\${?[^{}]*}?)`)
