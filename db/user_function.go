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
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// USER JS FUNCTION CONFIGURATION:

// Top level user-function config object: the map of names to queries.
type UserFunctionMap = map[string]*UserFunctionConfig

// Defines a JavaScript function that a client can invoke by name.
// (The name is the key in the UserFunctionMap.)
type UserFunctionConfig struct {
	SourceCode string          `json:"javascript"`           // Javascript source
	Parameters []string        `json:"parameters,omitempty"` // Names of parameters
	Allow      *UserQueryAllow `json:"allow,omitempty"`      // Permissions (admin-only if nil)

	compiled *sgbucket.JSServer // Compiled form of the function (instantiated lazily)
}

//////// RUNNING A USER FUNCTION:

func (db *Database) CallUserFunction(name string, params map[string]interface{}, mutationAllowed bool) (interface{}, error) {
	// Look up the function by name:
	config, found := db.Options.UserFunctions[name]
	if !found {
		return nil, missingError(db.user, "function", name)
	}

	// Check that the user is authorized:
	if err := config.Allow.authorize(db.user, params, "function", name); err != nil {
		return nil, err
	}

	// Make sure each specified parameter has a value in `params`:
	if err := checkQueryArguments(params, config.Parameters, "function", name); err != nil {
		return nil, err
	}

	// Compile and run the function:
	compiled := config.compiled
	if compiled == nil {
		compiled = newUserFunctionJSServer(name, "user function", "args, context", config.SourceCode)
		// This is a race condition if two threads find the script uncompiled and both compile it.
		// However, the effect is simply that two identical UserFunction instances are created and
		// one of them (the first one stored to config.compiled) is thrown away; harmless.
		config.compiled = compiled
	}

	return compiled.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
		runner := task.(*javaScriptRunner)
		runner.currentDB = db
		runner.mutationAllowed = mutationAllowed
		return task.Call(params, newUserFunctionJSContext(db))
	})
}

func newUserFunctionJSContext(db *Database) map[string]interface{} {
	return map[string]interface{}{"user": makeUserCtx(db.user)}
}

func newUserFunctionJSServer(name string, what string, argList string, sourceCode string) *sgbucket.JSServer {
	js := fmt.Sprintf(kJavaScriptWrapper, argList, sourceCode)
	return sgbucket.NewJSServer(js, kUserFunctionCacheSize,
		func(fnSource string) (sgbucket.JSServerTask, error) {
			return newJavaScriptRunner(name, what, fnSource)
		})
}

// Number of and Otto contexts to cache per function
const kUserFunctionCacheSize = 2
