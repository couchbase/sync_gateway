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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
)

/* This is the interface to the functions and GraphQL APIs implemented in the functions package. */

// Timeout for N1QL, JavaScript and GraphQL queries. (Applies to REST and BLIP requests.)
const defaultUserFunctionTimeout = 60 * time.Second

// Abstract interface for user-functions & GraphQL configuration. (Implemented by functions.Config)
type IFunctionsAndGraphQLConfig interface {
	// Compiles the configuration into a live UserFunctions intance.
	Compile(*js.VMPool) (*UserFunctions, GraphQL, error)

	// Returns the names of all N1QL queries used.
	N1QLQueryNames() []string
}

//////// USER FUNCTIONS

// A map from names to user functions.
type UserFunctions struct {
	Definitions    map[string]UserFunction
	MaxRequestSize *int
}

// A JavaScript function or N1QL query that can be invoked by a client.
// (Created by functions.CompileUserFunction or functions.CompileUserFunctions)
type UserFunction interface {
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

	// Same as Run() but the result is encoded as JSON.
	RunAsJSON() ([]byte, error)
}

//////// GRAPHQL

// Represents a compiled GraphQL schema and its resolver functions.
// Created by functions.CompileGraphQL.
type GraphQL interface {
	// The configuration's maximum request size (in bytes); or nil if none.
	MaxRequestSize() *int

	// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
	// The result is an object with keys `data`, `errors`.
	// If `jsonEncoded` is true, it will be JSON-encoded and returned as a []byte.
	Query(db *Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*GraphQLResult, error)

	// Same as Query() but the result is JSON-encoded.
	QueryAsJSON(db *Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) ([]byte, error)

	// Returns the names of all N1QL queries used.
	N1QLQueryNames() []string
}

// Go representation of JSON result of a GraphQL query.
type GraphQLResult struct {
	Data       interface{}            `json:"data"`
	Errors     []*FormattedError      `json:"errors,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

// Go representation of an error in a GraphQL query result.
type FormattedError struct {
	Message    string                 `json:"message"`
	Locations  []SourceLocation       `json:"locations"`
	Path       []interface{}          `json:"path,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

func (err *FormattedError) Error() string {
	return err.Message
}

type SourceLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

//////// DATABASE API FOR USER FUNCTIONS:

// Looks up a UserFunction by name and returns an Invocation.
func (db *Database) GetUserFunction(name string, args map[string]interface{}, mutationAllowed bool, ctx context.Context) (UserFunctionInvocation, error) {
	if db.UserFunctions != nil {
		if fn, found := db.UserFunctions.Definitions[name]; found {
			return fn.Invoke(db, args, mutationAllowed, ctx)
		}
	}
	return nil, missingError(db.User(), "function", name)
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
func (db *Database) UserGraphQLQuery(query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*GraphQLResult, error) {
	if graphql := db.GraphQL; graphql == nil {
		return nil, base.HTTPErrorf(http.StatusServiceUnavailable, "GraphQL is not configured")
	} else {
		return graphql.Query(db, query, operationName, variables, mutationAllowed, ctx)
	}
}

//////// UTILITIES

// Returns an HTTP 413 error if `maxSize` is non-nil and less than `actualSize`.
func CheckRequestSize[T int | int64](actualSize T, maxSize *int) error {
	if maxSize != nil && int64(actualSize) > int64(*maxSize) {
		return base.HTTPErrorf(http.StatusRequestEntityTooLarge, "Arguments too large")
	} else {
		return nil
	}
}

// Utility that returns a rough estimate of the original size of the JSON a value was parsed from.
func EstimateSizeOfJSON(args any) int {
	switch arg := args.(type) {
	case nil:
		return 4
	case bool:
		return 4
	case int64:
		return 4
	case float64:
		return 8
	case json.Number:
		return len(arg)
	case string:
		return len(arg) + 2
	case []any:
		size := 2
		for _, item := range arg {
			size += EstimateSizeOfJSON(item) + 1
		}
		return size
	case map[string]any:
		size := 2
		for key, item := range arg {
			size += len(key) + EstimateSizeOfJSON(item) + 4
		}
		return size
	default:
		return 1
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
