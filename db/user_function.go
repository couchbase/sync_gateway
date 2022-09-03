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
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// USER JS FUNCTION CONFIGURATION:

// Top level user-function config object: the map of names to queries.
type UserFunctionConfigMap = map[string]*UserFunctionConfig

// Defines a JavaScript or N1QL function that a client can invoke by name.
// (The name is the key in the UserFunctionMap.)
type UserFunctionConfig struct {
	Type       string          `json:"type"`
	Code       string          `json:"code"`                 // Javascript function or N1QL 'SELECT'
	Parameters []string        `json:"parameters,omitempty"` // Names of parameters
	Allow      *UserQueryAllow `json:"allow,omitempty"`      // Permissions (admin-only if nil)
}

type UserFunctions = map[string]*UserFunction

type UserFunction struct {
	*UserFunctionConfig
	compiled *sgbucket.JSServer // Compiled form of the function
}

//////// INITIALIZATION:

// Compiles the JS functions in a UserFunctionMap, returning UserFunctions.
func compileUserFunctions(config UserFunctionConfigMap) (UserFunctions, error) {
	fns := UserFunctions{}
	var multiError *base.MultiError
	for name, fnConfig := range config {
		var err error
		var compiled *sgbucket.JSServer
		switch fnConfig.Type {
		case "javascript":
			compiled, err = newUserFunctionJSServer(name, "user function", fnConfig.Code)
		case "query":
			compiled = nil
		default:
			err = fmt.Errorf("function %q has unrecognized 'type' %q", name, fnConfig.Type)
		}
		if err == nil {
			fns[name] = &UserFunction{
				UserFunctionConfig: fnConfig,
				compiled:           compiled,
			}
		} else {
			multiError = multiError.Append(err)
		}
	}
	return fns, multiError.ErrorOrNil()
}

func ValidateUserFunctions(config UserFunctionConfigMap) error {
	_, err := compileUserFunctions(config)
	return err
}

//////// RUNNING A USER FUNCTION:

type UserFunctionInvocation struct {
	*UserFunction
	name            string
	db              *Database
	args            map[string]interface{}
	mutationAllowed bool
}

// Invokes a user function by name.
func (db *Database) GetUserFunction(name string, args map[string]interface{}, mutationAllowed bool) (UserFunctionInvocation, error) {
	invocation := UserFunctionInvocation{
		name:            name,
		db:              db,
		args:            args,
		mutationAllowed: mutationAllowed,
	}
	if err := db.CheckTimeout(); err != nil {
		return invocation, err
	}
	// Look up the function by name:
	var found bool
	invocation.UserFunction, found = db.userFunctions[name]
	if !found {
		return invocation, missingError(db.user, "function", name)
	}

	// Validate the query arguments:
	if invocation.args == nil {
		invocation.args = map[string]interface{}{}
	}
	if err := db.checkQueryArguments(invocation.args, invocation.Parameters, "function", name); err != nil {
		return invocation, err
	}

	if invocation.compiled == nil {
		userArg := db.createUserArgument()
		if userArg != nil {
			invocation.args["user"] = userArg
		} else {
			invocation.args["user"] = map[string]interface{}{}
		}
	}

	// Check that the user is authorized:
	if err := invocation.Allow.authorize(db.user, invocation.args, "function", name); err != nil {
		return invocation, err
	}

	return invocation, nil
}

func (db *Database) CallUserFunction(name string, args map[string]interface{}, mutationAllowed bool) (interface{}, error) {
	invocation, err := db.GetUserFunction(name, args, mutationAllowed)
	if err != nil {
		return nil, err
	}
	return invocation.Run()
}

// Returns a query result iterator. If this function does not support iteration, returns nil;
// then call `run` instead.
func (fn *UserFunctionInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	if fn.compiled != nil {
		// JS:
		return nil, nil
	} else {
		// Return an iterator on the N1QL query results:
		iter, err := fn.db.N1QLQueryWithStats(fn.db.Ctx, QueryTypeUserPrefix+fn.name, fn.Code, fn.args,
			base.RequestPlus, false)
		if err != nil {
			// Return a friendlier error:
			var qe *gocb.QueryError
			if errors.As(err, &qe) {
				base.WarnfCtx(fn.db.Ctx, "Error running query %q: %v", fn.name, err)
				return nil, base.HTTPErrorf(http.StatusInternalServerError, "Query %q: %s", fn.name, qe.Errors[0].Message)
			} else {
				base.WarnfCtx(fn.db.Ctx, "Unknown error running query %q: %T %#v", fn.name, err, err)
				return nil, base.HTTPErrorf(http.StatusInternalServerError, "Unknown error running query %q (see logs)", fn.name)
			}
		}
		// Do a final timeout check, so the caller will know not to do any more work if time's up:
		return iter, fn.db.CheckTimeout()
	}
}

// Runs the function and returns the result.
// If this is a N1QL query, it will return all the result rows in an array.
func (fn *UserFunctionInvocation) Run() (interface{}, error) {
	if fn.compiled != nil {
		// Run the JavaScript function:
		return fn.compiled.WithTask(func(task sgbucket.JSServerTask) (result interface{}, err error) {
			runner := task.(*userJSRunner)
			return runner.CallWithDB(fn.db, fn.mutationAllowed, newUserFunctionJSContext(fn.db), fn.args)
		})
	} else {
		// Run the N1QL query. Result will be an array of rows.
		rows, err := fn.Iterate()
		if err != nil {
			return nil, err
		}
		defer func() {
			if rows != nil {
				rows.Close()
			}
		}()
		result := []interface{}{}
		var row interface{}
		for rows.Next(&row) {
			result = append(result, row)
		}
		err = rows.Close()
		rows = nil // prevent 'defer' from closing again
		return result, err
	}
}
