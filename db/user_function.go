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
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// USER JS FUNCTION CONFIGURATION:

// Top level user-function config object: the map of names to queries.
type UserFunctionConfigMap = map[string]*UserFunctionConfig

// Defines a JavaScript or N1QL function that a client can invoke by name.
// (The name is the key in the UserFunctionMap.)
type UserFunctionConfig struct {
	Type  string          `json:"type"`
	Code  string          `json:"code"`            // Javascript function or N1QL 'SELECT'
	Args  []string        `json:"args,omitempty"`  // Names of parameters/arguments
	Allow *UserQueryAllow `json:"allow,omitempty"` // Permissions (admin-only if nil)
}

type UserFunction = interface {
	Invoke(db *Database, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (UserFunctionInvocation, error)
	CheckArgs(bool)
	AllowByDefault(bool)
}

type UserFunctions = map[string]UserFunction

type UserFunctionInvocation interface {
	// Calls a user function, returning a query result iterator.
	// If this function does not support iteration, returns nil; then call `Run` instead.
	Iterate() (sgbucket.QueryResultIterator, error)

	// Calls a user function, returning the entire result.
	// (If this is a N1QL query it will return all the result rows in an array, which is less efficient than iterating them, so try calling `Iterate` first.)
	Run() (interface{}, error)

	// Calls a UserFunctionInvocation as a GraphQL resolver, given the parameters passed by the go-graphql API.
	Resolve(params graphql.ResolveParams) (interface{}, error)
}

//////// INITIALIZATION:

// Compiles the JS functions in a UserFunctionMap, returning UserFunctions.
func CompileUserFunctions(config UserFunctionConfigMap) (UserFunctions, error) {
	fns := UserFunctions{}
	var multiError *base.MultiError
	for name, fnConfig := range config {
		if userFn, err := compileUserFunction(name, "user function", fnConfig); err == nil {
			fns[name] = userFn
		} else {
			multiError = multiError.Append(err)
		}
	}
	return fns, multiError.ErrorOrNil()
}

func ValidateUserFunctions(config UserFunctionConfigMap) error {
	_, err := CompileUserFunctions(config)
	return err
}

// Returns all the query names in user functions and GraphQL resolvers.
func AllUserFunctionQueryNames(options DatabaseContextOptions) []string {
	var queryNames []string
	for name, fn := range options.UserFunctions {
		if fn.Type == "query" {
			queryNames = append(queryNames, QueryTypeUserFunctionPrefix+name)
		}
	}
	if options.GraphQL != nil {
		for typeName, resolvers := range options.GraphQL.Resolvers {
			for fieldName, resolver := range resolvers {
				if resolver.Type == "query" {
					queryNames = append(queryNames, QueryTypeUserFunctionPrefix+graphQLResolverName(typeName, fieldName))
				}
			}
		}
	}
	return queryNames
}

// Creates a UserFunction from a UserFunctionConfig.
func compileUserFunction(name string, typeName string, fnConfig *UserFunctionConfig) (UserFunction, error) {
	userFn := &userFunctionImpl{
		UserFunctionConfig: fnConfig,
		name:               name,
		typeName:           typeName,
		checkArgs:          true,
	}
	var err error
	switch fnConfig.Type {
	case "javascript":
		userFn.compiled, err = newUserFunctionJSServer(name, typeName, fnConfig.Code)
	case "query":
		err = nil
	default:
		err = fmt.Errorf("%s %q has unrecognized 'type' %q", typeName, name, fnConfig.Type)
	}
	return userFn, err
}

// implements UserFunction
type userFunctionImpl struct {
	*UserFunctionConfig
	name           string
	typeName       string
	checkArgs      bool
	allowByDefault bool
	compiled       *sgbucket.JSServer // Compiled form of the function
}

func (fn *userFunctionImpl) CheckArgs(check bool) {
	fn.checkArgs = check
}

func (fn *userFunctionImpl) AllowByDefault(allow bool) {
	fn.allowByDefault = allow
}

//////// RUNNING A USER FUNCTION:

// Looks up a UserFunction by name and returns an Invocation.
func (db *Database) GetUserFunction(name string, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (UserFunctionInvocation, error) {
	if fn, found := db.userFunctions[name]; found {
		return fn.Invoke(db, args, mutationAllowed, ctx)
	} else {
		return nil, missingError(db.user, "function", name)
	}
}

// Calls a user function by name, returning all the results at once.
func (db *Database) CallUserFunction(name string, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (interface{}, error) {
	invocation, err := db.GetUserFunction(name, args, mutationAllowed, ctx)
	if err != nil {
		return nil, err
	}
	return invocation.Run()
}

// implements UserFunctionInvocation
type userFunctionJSInvocation struct {
	*userFunctionImpl
	db              *Database
	ctx             context.Context
	args            map[string]interface{}
	mutationAllowed bool
}

// implements UserFunctionInvocation
type userFunctionN1QLInvocation struct {
	*userFunctionImpl
	db   *Database
	ctx  context.Context
	args map[string]interface{}
}

// Creates an Invocation of a UserFunction.
func (fn *userFunctionImpl) Invoke(db *Database, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (UserFunctionInvocation, error) {
	if ctx == nil {
		panic("missing context to UserFunction.Invoke")
	}

	if err := db.CheckTimeout(); err != nil {
		return nil, err
	}

	if args == nil {
		args = map[string]interface{}{}
	}
	if fn.checkArgs {
		// Validate the query arguments:
		if err := db.checkQueryArguments(args, fn.Args, fn.typeName, fn.name); err != nil {
			return nil, err
		}
	}

	// Check that the user is authorized:
	if fn.Allow != nil || !fn.allowByDefault {
		if err := fn.Allow.authorize(db.user, args, fn.typeName, fn.name); err != nil {
			return nil, err
		}
	}

	if fn.compiled == nil {
		// Create the N1QL arguments, which have the fn args under "args":
		var userArg interface{}
		if db.user != nil {
			userArg = db.createUserArgument()
		} else {
			userArg = map[string]interface{}{}
		}
		args = map[string]interface{}{"args": args, "user": userArg}

		return &userFunctionN1QLInvocation{
			userFunctionImpl: fn,
			db:               db,
			args:             args,
			ctx:              ctx,
		}, nil
	} else {
		return &userFunctionJSInvocation{
			userFunctionImpl: fn,
			db:               db,
			ctx:              ctx,
			args:             args,
			mutationAllowed:  mutationAllowed,
		}, nil
	}
}

func (fn *userFunctionJSInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	return nil, nil
}

func (fn *userFunctionJSInvocation) Run() (interface{}, error) {
	// Run the JavaScript function:
	return fn.compiled.WithTask(func(task sgbucket.JSServerTask) (result interface{}, err error) {
		runner := task.(*userJSRunner)
		return runner.CallWithDB(fn.db,
			fn.mutationAllowed,
			fn.ctx,
			newUserFunctionJSContext(fn.db),
			fn.args)
	})
}

func (fn *userFunctionN1QLInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	// N1QL:
	iter, err := fn.db.N1QLQueryWithStats(fn.ctx, QueryTypeUserFunctionPrefix+fn.name, fn.Code, fn.args,
		base.RequestPlus, false)

	if err != nil {
		// Return a friendlier error:
		var qe *gocb.QueryError
		if errors.As(err, &qe) {
			base.WarnfCtx(fn.ctx, "Error running query %q: %v", fn.name, err)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Query %q: %s", fn.name, qe.Errors[0].Message)
		} else {
			base.WarnfCtx(fn.ctx, "Unknown error running query %q: %T %#v", fn.name, err, err)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Unknown error running query %q (see logs)", fn.name)
		}
	}
	// Do a final timeout check, so the caller will know not to do any more work if time's up:
	return iter, fn.db.CheckTimeout()
}

func (fn *userFunctionN1QLInvocation) Run() (interface{}, error) {
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
