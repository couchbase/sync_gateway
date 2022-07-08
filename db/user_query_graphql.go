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
	"os"

	gqltools "github.com/bhoriuchi/graphql-go-tools"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Configuration for GraphQL.
type GraphQLConfig struct {
	Schema     *string                          `json:"schema,omitempty"`     // Schema in SDL syntax
	SchemaFile *string                          `json:"schemaFile,omitempty"` // Path of schema file
	Resolvers  map[string]GraphQLResolverConfig `json:"resolvers"`            // Defines query/mutation code
}

// Maps GraphQL query/mutation names to the JS source code that implements them.
type GraphQLResolverConfig map[string]string

// GraphQL query handler for a Database.
type GraphQL struct {
	dbc    *DatabaseContext
	schema graphql.Schema // The compiled GraphQL schema object
}

type gqContextKey string

var dbKey = gqContextKey("db")                 // Context key to access the Database instance
var mutAllowedKey = gqContextKey("mutAllowed") // Context key to access `mutationAllowed`

// Top-level public method to run a GraphQL query on a Database.
func (db *Database) UserGraphQLQuery(query string, operationName string, variables map[string]interface{}, mutationAllowed bool) (*graphql.Result, error) {
	if db.graphQL == nil {
		return nil, base.HTTPErrorf(http.StatusServiceUnavailable, "GraphQL is not configured")
	}
	return db.graphQL.Query(db, query, operationName, variables, mutationAllowed)
}

// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (gq *GraphQL) Query(db *Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool) (*graphql.Result, error) {
	ctx := context.WithValue(db.Ctx, dbKey, db)
	ctx = context.WithValue(ctx, mutAllowedKey, mutationAllowed)
	result := graphql.Do(graphql.Params{
		Schema:         gq.schema,
		RequestString:  query,
		VariableValues: variables,
		OperationName:  operationName,
		Context:        ctx,
	})
	if len(result.Errors) > 0 {
		// ??? Is this worth logging?
		base.WarnfCtx(ctx, "GraphQL query produced errors: %#v", result.Errors)
	}
	return result, nil
}

// Creates a new GraphQL instance for this DatabaseContext, using the config from `dbc.Options`.
// Called once, when the database opens.
func NewGraphQL(dbc *DatabaseContext) (*GraphQL, error) {
	gq := &GraphQL{
		dbc: dbc,
	}
	opts := dbc.Options.GraphQL

	// Get the schema source, from either `schema` or `schemaFile`:
	var schemaSource string
	if opts.Schema != nil {
		if opts.SchemaFile != nil {
			return nil, fmt.Errorf("config error in `graphql`: Only one of `schema` and `schemaFile` may be used")
		}
		schemaSource = *opts.Schema
	} else {
		if opts.SchemaFile == nil {
			return nil, fmt.Errorf("config error in `graphql`: Either `schema` or `schemaFile` must be defined")
		}
		src, err := os.ReadFile(*opts.SchemaFile)
		if err != nil {
			return nil, fmt.Errorf("config error in `graphql`: Can't read schemaFile %s", *opts.SchemaFile)
		}
		schemaSource = string(src)
	}

	// Assemble the resolvers:
	resolvers := map[string]interface{}{}
	for name, resolver := range opts.Resolvers {
		fieldMap := map[string]*gqltools.FieldResolve{}
		for fieldName, jsCode := range resolver {
			if fn, err := gq.compileResolver(name, fieldName, jsCode); err == nil {
				fieldMap[fieldName] = &gqltools.FieldResolve{Resolve: fn}
			} else {
				return nil, err
			}
		}
		resolvers[name] = &gqltools.ObjectResolver{Fields: fieldMap}
	}

	// Now compile the schema and create the graphql.Schema object:
	var err error
	gq.schema, err = gqltools.MakeExecutableSchema(gqltools.ExecutableSchema{
		TypeDefs:  schemaSource,
		Resolvers: resolvers,
	})
	if err != nil {
		return nil, err
	}
	return gq, nil
}

//////// GRAPHQL RESOLVER:

// Creates a graphQLResolver for the given JavaScript code, and returns a graphql-go FieldResolveFn
// that invokes it.
func (gq *GraphQL) compileResolver(resolverName string, fieldName string, jsCode string) (graphql.FieldResolveFn, error) {
	name := resolverName + "." + fieldName
	resolver := &graphQLResolver{
		JSServer: sgbucket.NewJSServer(jsCode, kGQTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
				return NewUserFunctionRunner(name, "GraphQL resolver", kGraphQLResolverFuncWrapper, fnSource)
			}),
		Name: name,
	}
	return func(params graphql.ResolveParams) (interface{}, error) {
		db := params.Context.Value(dbKey).(*Database)
		mutationAllowed := params.Context.Value(mutAllowedKey).(bool)
		return resolver.Resolve(db, &params, mutationAllowed)
	}, nil
}

// An object that can run a JavaScript GraphQL resolve function, as found in the GraphQL config.
type graphQLResolver struct {
	*sgbucket.JSServer // "Superclass"
	Name               string
}

// Calls a GraphQLResolver. `params` is the parameter struct passed by the go-graphql API,
// and mutationAllowed is true iff the resolver is allowed to make changes to the database;
// the `save` callback checks this.
func (res *graphQLResolver) Resolve(db *Database, params *graphql.ResolveParams, mutationAllowed bool) (interface{}, error) {
	// Collect the 'subfields', the fields the query wants from the value being resolved:
	subfields := []string{}
	if len(params.Info.FieldASTs) > 0 {
		for _, sel := range params.Info.FieldASTs[0].SelectionSet.Selections {
			if subfield, ok := sel.(*ast.Field); ok {
				if subfield.Name.Kind == "Name" {
					subfields = append(subfields, subfield.Name.Value)
				}
			}
		}
	}
	//log.Printf("-- %q : subfields = %v", params.Info.FieldName, subfields)

	// The `info` parameter passed to the JS function.
	// The fields are a subset of graphql.ResolveInfo.
	type jsGQResolveInfo struct {
		FieldName      string                 `json:"fieldName"`
		RootValue      interface{}            `json:"rootValue"`
		VariableValues map[string]interface{} `json:"variableValues"`
		Subfields      []string               `json:"subfields"`
	}
	info := jsGQResolveInfo{
		FieldName:      params.Info.FieldName,
		RootValue:      params.Info.RootValue,
		VariableValues: params.Info.VariableValues,
		Subfields:      subfields,
	}

	return res.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
		runner := task.(*userFunctionRunner)
		runner.currentDB = db
		runner.mutationAllowed = mutationAllowed
		return task.Call(params.Source, params.Args, newUserFunctionJSContext(db), &info)
	})
}

// Number of ResolverRunner tasks (and Otto contexts) to cache for this resolver
const kGQTaskCacheSize = 2

// The outermost JavaScript code. Evaluating it returns a function, which is then called by the
// Runner every time it's invoked. (The reason the first few lines are ""-style strings is to make
// sure the resolver code ends up on line 1, which makes line numbers reported in syntax errors
// accurate.)
// `%s` is replaced with the resolver.
const kGraphQLResolverFuncWrapper = "function() {" +
	"	function resolveFn(parent, args, context, info) {" +
	"		%s;" + // <-- The actual JS code from the config file goes here
	`	}

		// This is the JS function invoked by the 'Call(...)' in GraphQLResolver.Resolve(), above
		return function (parent, args, context, info) {
			// The "context.app" object the resolver script calls:
			var _app = {
				get: _get,
				query: _query,
				save: _save
			};

			if (context)
				context.app = _app;
			else
				context = {app: _app};
			return resolveFn(parent, args, context, info);
		}
	}()`
