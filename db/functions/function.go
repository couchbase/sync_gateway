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
	Type  string   `json:"type"`
	Code  string   `json:"code"`            // Javascript function or N1QL 'SELECT'
	Args  []string `json:"args,omitempty"`  // Names of parameters/arguments
	Allow *Allow   `json:"allow,omitempty"` // Permissions (admin-only if nil)
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
	*FunctionConfig
	name           string
	typeName       string
	checkArgs      bool
	allowByDefault bool
	compiled       *sgbucket.JSServer // Compiled form of the function; nil for N1QL
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

func (fn *functionImpl) CheckArgs(check bool) {
	fn.checkArgs = check
}

func (fn *functionImpl) AllowByDefault(allow bool) {
	fn.allowByDefault = allow
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

	if fn.isN1QL() {
		return &n1qlInvocation{
			functionImpl: fn,
			db:           db,
			args:         args,
			ctx:          ctx,
		}, nil
	} else {
		return &jsInvocation{
			functionImpl:    fn,
			db:              db,
			ctx:             ctx,
			args:            args,
			mutationAllowed: mutationAllowed,
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

// Authorizes a User against the function config's Allow object:
// - The user's name must be contained in Users, OR
// - The user must have a role contained in Roles, OR
// - The user must have access to a channel contained in Channels.
// In Roles and Channels, patterns of the form `$param` or `$(param)` are expanded using `args`.
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
			if role, err := allow.expandPattern(rolePattern, args, user); err != nil {
				return err
			} else if userRoles.Contains(role) {
				return nil // User has one of the allowed roles
			}
		}
		// Check if the user has access to one of the given channels.
		for _, channelPattern := range allow.Channels {
			if channelPattern == channels.AllChannelWildcard {
				return nil
			} else if channel, err := allow.expandPattern(channelPattern, args, user); err != nil {
				return err
			} else if user.CanSeeChannel(channel) {
				return nil // User has access to one of the allowed channels
			}
		}
	}
	return user.UnauthError(fmt.Sprintf("you are not allowed to call %s %q", fn.typeName, fn.name))
}

// Expands patterns of the form `$param` or `$(param)` in `pattern`, looking up each such
// `param` in the `args` map and substituting its value.
// (`$$` is replaced with `$`.)
// It is an error if any `param` has no value, or if its value is not a string or integer.
func (allow *Allow) expandPattern(pattern string, args map[string]any, user auth.User) (string, error) {
	if strings.IndexByte(pattern, '$') < 0 {
		return pattern, nil
	}
	var err error
	channel := kChannelPropertyRegexp.ReplaceAllStringFunc(pattern, func(arg string) string {
		arg = arg[1:]
		if arg == "$" {
			return arg
		}
		if arg[0] == '(' {
			arg = arg[1 : len(arg)-1]
		}
		// Look up the parameter:
		if strings.HasPrefix(arg, "user.") {
			// Treat "user." the same as "context.user.":
			arg = "context." + arg
		}
		if strings.HasPrefix(arg, "context.user.") {
			// Hacky special case for "context.user.":
			if user == nil {
				return ""
			}
			switch arg {
			case "context.user.name":
				return user.Name()
			case "context.user.email":
				return user.Email()
			}
		}
		if value, found := args[arg]; !found {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Invalid channel/role pattern %q in 'allow'", pattern)
			err = base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid; details in log")
			return ""
		} else if valueStr, ok := value.(string); ok {
			return valueStr
		} else if reflect.ValueOf(value).CanInt() || reflect.ValueOf(value).CanUint() || reflect.ValueOf(value).CanFloat() {
			return fmt.Sprintf("%v", value)
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Value of parameter '%s' must be a string or int", arg)
			return ""
		}
	})
	return channel, err
}

// Regexp that matches a property pattern -- either `$xxx` or `$(xxx)` where `xxx` is one or more
// alphanumeric characters or underscore. It also matches `$$` so it can be replaced with `$`.
var kChannelPropertyRegexp = regexp.MustCompile(`\$(\w+|\([^)]+\)|\$)`)
