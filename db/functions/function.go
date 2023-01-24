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
	"os"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/js"
)

//////// CONFIGURATION TYPES:

// Combines functions & GraphQL configuration. Implements db.IFunctionsAndGraphQLConfig.
type Config struct {
	Functions *FunctionsConfig
	GraphQL   *GraphQLConfig
}

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
	Subgraph         bool            `json:"subgraph,omitempty"`           // Enable Apollo Subgraph support
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

func (fnc *Config) Compile(vms *js.VMPool) (*db.UserFunctions, db.GraphQL, error) {
	return CompileFunctions(fnc.Functions, fnc.GraphQL, vms)
}

func (fnc *Config) N1QLQueryNames() []string {
	queryNames := []string{}
	if fnc.Functions != nil {
		for fnName, fn := range fnc.Functions.Definitions {
			if fn.Type == "query" {
				queryNames = append(queryNames, db.QueryTypeUserFunctionPrefix+fnName)
			}
		}
	}
	if fnc.GraphQL != nil {
		for typeName, resolvers := range fnc.GraphQL.Resolvers {
			for fieldName, resolver := range resolvers {
				if resolver.Type == "query" {
					queryNames = append(queryNames, db.QueryTypeUserFunctionPrefix+graphQLResolverName(typeName, fieldName))
				}
			}
		}
	}
	return queryNames
}

// Compiles the functions in a UserFunctionConfigMap, returning UserFunctions.
func CompileFunctions(fnConfig *FunctionsConfig, gqConfig *GraphQLConfig, vms *js.VMPool) (fns *db.UserFunctions, gq db.GraphQL, err error) {
	if fnConfig == nil && gqConfig == nil {
		return
	}
	if gqConfig, err = readSchema(gqConfig); err != nil {
		return
	}

	service := js.NewCustomService(vms, "functions", makeService(fnConfig, gqConfig))

	if fnConfig != nil {
		fns = &db.UserFunctions{
			MaxRequestSize: fnConfig.MaxRequestSize,
			Definitions:    map[string]db.UserFunction{},
		}
		for name, fnConfig := range fnConfig.Definitions {
			fns.Definitions[name] = &functionImpl{
				FunctionConfig: fnConfig,
				service:        service,
				name:           name,
			}
		}
	}
	if gqConfig != nil {
		gq = &graphQLImpl{
			config:  gqConfig,
			service: service,
		}
	}
	return
}

// Creates an evaluator using a new VM not belonging to a pool.
// Remember to close it when finished!
func newStandaloneEvaluator(ctx context.Context, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig, delegate evaluatorDelegate) (*evaluator, js.VM, error) {
	if fnConfig == nil && gqConfig == nil {
		return nil, nil, nil
	}
	vm := js.NewV8VM()
	service := js.NewCustomService(vm, "functions", makeService(fnConfig, gqConfig))
	if runner, err := service.GetRunner(); err != nil {
		vm.Close()
		return nil, nil, err
	} else {
		runner.SetContext(ctx)
		eval, err := newEvaluator(runner.(*js.V8Runner))
		if err != nil {
			vm.Close()
			return nil, nil, err
		}
		eval.setup(delegate, nil)
		return eval, vm, nil
	}
}

// Validates a FunctionsConfig & GraphQLConfig.
func ValidateFunctions(ctx context.Context, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig) error {
	gqConfig, err := readSchema(gqConfig)
	if err != nil {
		return err
	}
	eval, vm, err := newStandaloneEvaluator(ctx, fnConfig, gqConfig, &databaseDelegate{ctx: ctx})
	if err == nil && eval != nil {
		eval.close()
		vm.Close()
	}
	return err
}

//////// FUNCTIONIMPL

// implements UserFunction.
type functionImpl struct {
	*FunctionConfig // Inherits from FunctionConfig
	service         *js.Service
	name            string // Name of function
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
func (fn *functionImpl) Invoke(dbc *db.Database, args map[string]any, mutationAllowed bool, ctx context.Context) (db.UserFunctionInvocation, error) {
	inv := functionInvocation{
		functionImpl:    fn,
		args:            args,
		mutationAllowed: mutationAllowed,
	}
	err := inv.delegate.init(dbc, ctx)
	return &inv, err
}

// Implements UserFunctionInvocation
type functionInvocation struct {
	*functionImpl
	args            map[string]any
	mutationAllowed bool
	delegate        databaseDelegate
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
	delegate := &inv.delegate
	eval, err := makeEvaluator(inv.service, delegate.db, delegate, delegate.user, delegate.ctx)
	if err != nil {
		return nil, err
	}
	defer eval.close()
	eval.setMutationAllowed(inv.mutationAllowed)
	return eval.callFunction(inv.name, inv.args)
}

//////// GRAPHQLIMPL

// If the config's SchemaFile is set, reads the file and puts its contents into Schema.
// Returns a _copy_ of the config, to avoid mutating the original.
func readSchema(gq *GraphQLConfig) (*GraphQLConfig, error) {
	if gq == nil {
		return gq, nil
	} else if gq.SchemaFile != nil {
		// Read schema file:
		if gq.Schema != nil {
			return nil, fmt.Errorf("GraphQL: can't give both `schema` and `schemaFile`")
		} else if schema, err := os.ReadFile(*gq.SchemaFile); err != nil {
			return nil, fmt.Errorf("GraphQL: can't read schema file %s: %w", *gq.SchemaFile, err)
		} else {
			mutConfig := *gq
			mutConfig.Schema = base.StringPtr(string(schema))
			mutConfig.SchemaFile = nil
			return &mutConfig, nil
		}
	} else if gq.Schema != nil {
		return gq, nil
	} else {
		return nil, fmt.Errorf("GraphQL: either `schema` or `schemaFile` must be given")
	}
}

// Implementation of db.graphQLImpl interface.
type graphQLImpl struct {
	config  *GraphQLConfig
	service *js.Service
}

func (gq *graphQLImpl) MaxRequestSize() *int {
	return gq.config.MaxRequestSize
}

func (gq *graphQLImpl) QueryAsJSON(dbc *db.Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) ([]byte, error) {
	var delegate databaseDelegate
	if err := delegate.init(dbc, ctx); err != nil {
		return nil, err
	}
	eval, err := makeEvaluator(gq.service, dbc, &delegate, delegate.user, ctx)
	if err != nil {
		return nil, err
	}
	defer eval.close()

	eval.setMutationAllowed(mutationAllowed)
	return eval.callGraphQL(query, operationName, variables)
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
