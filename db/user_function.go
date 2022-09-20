/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// USER JS FUNCTION CONFIGURATION:

// Top level user-function config object: the map of names to queries.
type UserFunctionConfigMap = map[string]*UserFunctionConfig

// Defines a JavaScript function that a client can invoke by name.
// (The name is the key in the UserFunctionMap.)
type UserFunctionConfig struct {
	SourceCode string          `json:"javascript"`           // Javascript source
	Parameters []string        `json:"parameters,omitempty"` // Names of parameters
	Allow      *UserQueryAllow `json:"allow,omitempty"`      // Permissions (admin-only if nil)
}

type UserFunctions = map[string]UserFunction

type UserFunction struct {
	*UserFunctionConfig
	compiled *sgbucket.JSServer // Compiled form of the function
}

//////// INITIALIZATION:

// Compiles the JS functions in a UserFunctionMap, returning UserFunctions.
func compileUserFunctions(ctx context.Context, config UserFunctionConfigMap) (UserFunctions, error) {
	fns := UserFunctions{}
	var multiError *base.MultiError
	for name, fnConfig := range config {
		compiled, err := newUserFunctionJSServer(ctx, name, "user function", "args, context", fnConfig.SourceCode)
		if err == nil {
			fns[name] = UserFunction{
				UserFunctionConfig: fnConfig,
				compiled:           compiled,
			}
		} else {
			multiError = multiError.Append(err)
		}
	}
	return fns, multiError.ErrorOrNil()
}

func ValidateUserFunctions(ctx context.Context, config UserFunctionConfigMap) error {
	_, err := compileUserFunctions(ctx, config)
	return err
}

//////// RUNNING A USER FUNCTION:

// Invokes a JavaScript function by name.
func (db *Database) CallUserFunction(ctx context.Context, name string, args map[string]interface{}, mutationAllowed bool) (interface{}, error) {
	if err := db.CheckTimeout(ctx); err != nil {
		return nil, err
	}
	// Look up the function by name:
	fn, found := db.userFunctions[name]
	if !found {
		return nil, missingError(db.user, "function", name)
	}

	// Validate the query arguments:
	if args == nil {
		args = map[string]interface{}{}
	}
	if err := db.checkQueryArguments(args, fn.Parameters, "function", name); err != nil {
		return nil, err
	}

	// Check that the user is authorized:
	if err := fn.Allow.authorize(ctx, db.user, args, "function", name); err != nil {
		return nil, err
	}

	// Run the function:
	return fn.compiled.WithTask(func(task sgbucket.JSServerTask) (result interface{}, err error) {
		runner := task.(*userJSRunner)
		return runner.CallWithDB(ctx, db, mutationAllowed, args, newUserFunctionJSContext(db))
	})
}
