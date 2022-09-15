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
	"fmt"
	"net/http"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
)

/* This is the interface to the functions and GraphQL APIs implemented in the functions package. */

// Timeout for N1QL, JavaScript and GraphQL queries. (Applies to REST and BLIP requests.)
const UserQueryTimeout = 60 * time.Second

//////// USER FUNCTIONS

// A map from names to user functions.
type UserFunctions = map[string]UserFunction

// A JavaScript function or N1QL query that can be invoked by a client.
// (Created by functions.CompileUserFunction or functions.CompileUserFunctions)
type UserFunction = interface {
	// The function's name
	Name() string

	// Returns the name assigned to the N1QL query, if there is one.
	N1QLQueryName() (string, bool)

	// Creates an invocation of the function, which can then be run or iterated.
	Invoke(db *Database, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (UserFunctionInvocation, error)
}

// The context for running a user function; created by UserFunction.Invoke.
type UserFunctionInvocation interface {
	// Calls a user function, returning a query result iterator.
	// If this function does not support iteration, returns nil; then call `Run` instead.
	Iterate() (sgbucket.QueryResultIterator, error)

	// Calls a user function, returning the entire result.
	// (If this is a N1QL query it will return all the result rows in an array, which is less efficient than iterating them, so try calling `Iterate` first.)
	Run() (interface{}, error)
}

//////// GRAPHQL

// Represents a compiled GraphQL schema and its resolver functions.
// Created by functions.CompileGraphQL.
type GraphQL = interface {
	// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
	Query(db *Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*graphql.Result, error)

	// Returns the names of all N1QL queries used.
	N1QLQueryNames() []string
}

//////// DATABASE API FOR USER FUNCTIONS:

// Looks up a UserFunction by name and returns an Invocation.
func (db *Database) GetUserFunction(name string, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (UserFunctionInvocation, error) {
	if fn, found := db.Options.UserFunctions[name]; found {
		return fn.Invoke(db, args, mutationAllowed, ctx)
	} else {
		return nil, missingError(db.User(), "function", name)
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

// Top-level public method to run a GraphQL query on a db.Database.
func (db *Database) UserGraphQLQuery(query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*graphql.Result, error) {
	if graphql := db.Options.GraphQL; graphql != nil {
		return db.Options.GraphQL.Query(db, query, operationName, variables, mutationAllowed, ctx)
	} else {
		return nil, base.HTTPErrorf(http.StatusServiceUnavailable, "GraphQL is not configured")
	}
}

// Returns the appropriate HTTP error for when a function/query doesn't exist.
// For security reasons, we don't let a non-admin user know what function names exist;
// so instead of a 404 we return the same 401/403 error as if they didn't have access to it.
func missingError(user auth.User, what string, name string) error {
	if user == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no such %s %q", what, name)
	} else {
		return user.UnauthError(fmt.Sprintf("you are not allowed to call %s %q", what, name))
	}
}
