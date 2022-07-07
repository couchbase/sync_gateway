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
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
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

func (gq *GraphQL) compileResolver(resolverName string, fieldName string, jsCode string) (graphql.FieldResolveFn, error) {
	resolver := NewGraphQLResolver(resolverName+"."+fieldName, jsCode)
	return func(params graphql.ResolveParams) (interface{}, error) {
		db := params.Context.Value(dbKey).(*Database)
		mutationAllowed := params.Context.Value(mutAllowedKey).(bool)
		return resolver.Resolve(db, &params, mutationAllowed)
	}, nil
}
