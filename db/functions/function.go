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
	"encoding/json"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
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

// Configuration for GraphQL.
type GraphQLConfig struct {
	Schema           *string         `json:"schema,omitempty"`             // Schema in SDL syntax
	SchemaFile       *string         `json:"schemaFile,omitempty"`         // Path of schema file
	Resolvers        GraphQLTypesMap `json:"resolvers"`                    // Defines query/mutation code
	MaxSchemaSize    *int            `json:"max_schema_size,omitempty"`    // Maximum length (in bytes) of GraphQL schema
	MaxResolverCount *int            `json:"max_resolver_count,omitempty"` // Maximum number of GraphQL resolvers
	MaxCodeSize      *int            `json:"max_code_size,omitempty"`      // Maximum length (in bytes) of a function's code
	MaxRequestSize   *int            `json:"max_request_size,omitempty"`   // Maximum size of the encoded query & arguments
}

// Maps GraphQL type names (incl. "Query") to their resolvers.
type GraphQLTypesMap map[string]GraphQLResolverConfig

// Maps GraphQL field names to the resolvers that implement them.
type GraphQLResolverConfig map[string]FunctionConfig

//////// INITIALIZATION:

// Compiles the functions in a UserFunctionConfigMap, returning UserFunctions.
func CompileFunctions(fnConfig *FunctionsConfig, gqConfig *GraphQLConfig) (fns *db.UserFunctions, gq db.GraphQL, err error) {
	if fnConfig == nil && gqConfig == nil {
		return
	}

	env, err := newEnvironment(fnConfig, gqConfig)
	if err != nil {
		return
	}

	if fnConfig != nil {
		fns = &db.UserFunctions{
			MaxRequestSize: fnConfig.MaxRequestSize,
			Definitions:    map[string]db.UserFunction{},
		}
		for name, fnConfig := range fnConfig.Definitions {
			fns.Definitions[name] = &functionImpl{
				FunctionConfig: fnConfig,
				name:           name,
				env:            env,
			}
		}
	}
	if gqConfig != nil {
		gq = &graphQLImpl{
			config: gqConfig,
			env:    env,
		}
	}
	return
}

// Validates a FunctionsConfig.
func ValidateFunctions(ctx context.Context, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig) error {
	// To validate, we have to create an Evaluator, because that's when the JavaScript code
	// runs and reads the configuration.
	if fnConfig == nil && gqConfig == nil {
		return nil
	} else if env, err := newEnvironment(fnConfig, gqConfig); err != nil {
		return err
	} else {
		delegate := &databaseDelegate{
			ctx: ctx,
		}
		if eval, err := env.newEvaluator(delegate, nil); err != nil {
			return err
		} else {
			eval.close()
			return nil
		}
	}
}

//////// FUNCTIONIMPL

// implements UserFunction.
type functionImpl struct {
	*FunctionConfig              // Inherits from FunctionConfig
	name            string       // Name of function
	env             *environment // The V8 VM
}

func (fn *functionImpl) Name() string {
	return fn.name
}

func (fn *functionImpl) isN1QL() bool {
	return fn.Type == "query"
}

func (fn *functionImpl) N1QLQueryName() (string, bool) {
	if fn.isN1QL() {
		return db.QueryTypeUserFunctionPrefix + fn.name, true
	} else {
		return "", false
	}
}

// Creates an Invocation of a UserFunction.
func (fn *functionImpl) invoke(delegate evaluatorDelegate, user *userCredentials, args map[string]any, mutationAllowed bool) (db.UserFunctionInvocation, error) {
	eval, err := fn.env.newEvaluator(delegate, user)
	if err != nil {
		return nil, err
	}
	eval.setMutationAllowed(mutationAllowed)
	return &functionInvocation{
		functionImpl: fn,
		eval:         eval,
		args:         args,
	}, nil
}

// Creates an Invocation of a UserFunction.
func (fn *functionImpl) Invoke(dbc *db.Database, args map[string]any, mutationAllowed bool, ctx context.Context) (db.UserFunctionInvocation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("missing context to UserFunction.Invoke")
	}
	if err := dbc.CheckTimeout(ctx); err != nil {
		return nil, err
	}
	delegate := &databaseDelegate{
		db:  dbc,
		ctx: ctx,
	}
	if dbUser := dbc.User(); dbUser != nil {
		delegate.user = &userCredentials{
			Name:     dbUser.Name(),
			Roles:    dbUser.RoleNames().AllKeys(),
			Channels: dbUser.Channels().AllKeys(),
		}
	}
	return fn.invoke(delegate, delegate.user, args, mutationAllowed)
}

// Implements UserFunctionInvocation
type functionInvocation struct {
	*functionImpl
	eval *evaluator
	args map[string]any
}

func (inv *functionInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	return nil, nil
}

func (inv *functionInvocation) Run() (interface{}, error) {
	if resultJSON, err := inv.RunAsJSON(); err != nil {
		return nil, err
	} else if resultJSON == nil {
		return nil, nil
	} else {
		var result any
		err := json.Unmarshal([]byte(resultJSON), &result)
		return result, err
	}
}

func (inv *functionInvocation) RunAsJSON() ([]byte, error) {
	defer inv.eval.close()
	return inv.eval.callFunction(inv.name, inv.args)
}

//////// GRAPHQLIMPL

// Implementation of db.graphQLImpl interface.
type graphQLImpl struct {
	config *GraphQLConfig
	env    *environment // The V8 VM
}

func (gq *graphQLImpl) MaxRequestSize() *int {
	return gq.config.MaxRequestSize
}

func (gq *graphQLImpl) query(delegate evaluatorDelegate, user *userCredentials, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) ([]byte, error) {
	eval, err := gq.env.newEvaluator(delegate, user)
	if err != nil {
		return nil, err
	}
	defer eval.close()
	eval.setMutationAllowed(mutationAllowed)
	return eval.callGraphQL(query, operationName, variables)
}

func (gq *graphQLImpl) QueryAsJSON(dbc *db.Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("missing context to UserFunction.Invoke")
	}
	if err := dbc.CheckTimeout(ctx); err != nil {
		return nil, err
	}
	delegate := &databaseDelegate{
		db:  dbc,
		ctx: ctx,
	}
	if dbUser := dbc.User(); dbUser != nil {
		delegate.user = &userCredentials{
			Name:     dbUser.Name(),
			Roles:    dbUser.RoleNames().AllKeys(),
			Channels: dbUser.Channels().AllKeys(),
		}
	}
	return gq.query(delegate, delegate.user, query, operationName, variables, mutationAllowed, ctx)
}

func (gq *graphQLImpl) Query(dbc *db.Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*db.GraphQLResult, error) {
	resultJSON, err := gq.QueryAsJSON(dbc, query, operationName, variables, mutationAllowed, ctx)
	if err != nil {
		return nil, err
	} else if resultJSON == nil {
		return nil, nil
	} else {
		var result db.GraphQLResult
		err = json.Unmarshal(resultJSON, &result)
		return &result, err
	}
}

// Returns the names of all N1QL queries used.
func (gq *graphQLImpl) N1QLQueryNames() []string {
	queryNames := []string{}
	for typeName, resolvers := range gq.config.Resolvers {
		for fieldName, resolver := range resolvers {
			if resolver.Type == "query" {
				queryNames = append(queryNames, db.QueryTypeUserFunctionPrefix+graphQLResolverName(typeName, fieldName))
			}
		}
	}
	return queryNames
}

func graphQLResolverName(typeName string, fieldName string) string {
	return typeName + "." + fieldName
}
