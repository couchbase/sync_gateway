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
	if err := db.CheckTimeout(); err != nil {
		return nil, err
	}
	ctx := context.WithValue(db.Ctx, dbKey, db)
	ctx = context.WithValue(ctx, mutAllowedKey, mutationAllowed)
	result := graphql.Do(graphql.Params{
		Schema:         gq.schema,
		RequestString:  query,
		VariableValues: variables,
		OperationName:  operationName,
		Context:        ctx,
	})
	return result, nil
}

//////// GRAPHQL INITIALIZATION:

// Creates a new GraphQL instance for this DatabaseContext, using the config from `dbc.Options`.
// Called once, when the database opens.
func NewGraphQL(dbc *DatabaseContext) (*GraphQL, error) {
	if schema, err := dbc.Options.GraphQL.CompileSchema(); err != nil {
		return nil, err
	} else {
		gql := &GraphQL{
			dbc:    dbc,
			schema: schema,
		}
		return gql, nil
	}
}

// Validates a GraphQL configuration by parsing the schema.
func (config *GraphQLConfig) Validate() error {
	_, err := config.CompileSchema()
	return err
}

func (config *GraphQLConfig) CompileSchema() (graphql.Schema, error) {
	// Get the schema source, from either `schema` or `schemaFile`:
	schemaSource, err := config.getSchema()
	if err != nil {
		return graphql.Schema{}, err
	}

	var multiError *base.MultiError
	// Assemble the resolvers:
	resolvers := map[string]interface{}{}
	for name, resolver := range config.Resolvers {
		fieldMap := map[string]*gqltools.FieldResolve{}
		var typeNameResolver graphql.ResolveTypeFn
		for fieldName, jsCode := range resolver {
			if fieldName == "__typename" {
				// The "__typename" resolver returns the name of the concrete type of an
				// instance of an interface.
				typeNameResolver, err = config.compileTypeNameResolver(name, jsCode)
			} else {
				var fn graphql.FieldResolveFn
				fn, err = config.compileFieldResolver(name, fieldName, jsCode)
				fieldMap[fieldName] = &gqltools.FieldResolve{Resolve: fn}
			}
			if err != nil {
				multiError = multiError.Append(err)
			}
		}
		if typeNameResolver == nil {
			resolvers[name] = &gqltools.ObjectResolver{
				Fields: fieldMap,
			}
		} else {
			resolvers[name] = &gqltools.InterfaceResolver{
				Fields:      fieldMap,
				ResolveType: typeNameResolver,
			}
		}
	}
	if multiError != nil {
		return graphql.Schema{}, multiError.ErrorOrNil()
	}

	// Now compile the schema and create the graphql.Schema object:
	schema, err := gqltools.MakeExecutableSchema(gqltools.ExecutableSchema{
		TypeDefs:  schemaSource,
		Resolvers: resolvers,
		Debug:     true, // This enables logging of unresolved-type errors
	})

	if err == nil && len(schema.TypeMap()) == 0 {
		base.WarnfCtx(context.Background(), "GraphQL Schema object has no registered TypeMap -- this probably means the schema has unresolved types. See gqltools warnings above")
		err = fmt.Errorf("GraphQL Schema object has no registered TypeMap -- this probably means the schema has unresolved types")
	}
	return schema, err
}

// Reads the schema, either directly from the config or from an external file.
func (config *GraphQLConfig) getSchema() (string, error) {
	if config.Schema != nil {
		if config.SchemaFile != nil {
			return "", fmt.Errorf("GraphQL config: only one of `schema` and `schemaFile` may be used")
		}
		return *config.Schema, nil
	} else {
		if config.SchemaFile == nil {
			return "", fmt.Errorf("GraphQL config: either `schema` or `schemaFile` must be defined")
		}
		src, err := os.ReadFile(*config.SchemaFile)
		if err != nil {
			return "", fmt.Errorf("GraphQL config: cann't read file %s: %w", *config.SchemaFile, err)
		}
		return string(src), nil
	}
}

//////// FIELD RESOLVER:

// Creates a graphQLResolver for the given JavaScript code, and returns a graphql-go FieldResolveFn
// that invokes it.
func (config *GraphQLConfig) compileFieldResolver(typeName string, fieldName string, jsCode string) (graphql.FieldResolveFn, error) {
	name := typeName + "." + fieldName
	server, err := newUserFunctionJSServer(name, "GraphQL resolver", jsCode)
	if err != nil {
		return nil, err
	}
	resolver := &graphQLResolver{
		JSServer: server,
		Name:     name,
	}
	isMutation := typeName == "Mutation"
	return func(params graphql.ResolveParams) (interface{}, error) {
		db := params.Context.Value(dbKey).(*Database)
		mutationAllowed := params.Context.Value(mutAllowedKey).(bool)
		if isMutation && !mutationAllowed {
			return nil, base.HTTPErrorf(http.StatusForbidden, "a read-only request is not allowed to call a GraphQL mutation")
		}
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
	// Collect the 'resultFields', the fields the query wants from the value being resolved:
	resultFields := []string{}
	if len(params.Info.FieldASTs) > 0 {
		if set := params.Info.FieldASTs[0].SelectionSet; set != nil {
			for _, sel := range set.Selections {
				if subfield, ok := sel.(*ast.Field); ok {
					if subfield.Name.Kind == "Name" {
						resultFields = append(resultFields, subfield.Name.Value)
					}
				}
			}
		}
	}

	// The `info` parameter passed to the JS function; fields are a subset of graphql.ResolveInfo.
	// NOTE: We've removed these fields until we get feedback that they're needed by developers.
	//   `resultFields` is not provided (directly) by ResolveInfo; it contains the fields of the
	// resolver's result that will be used by the query (other fields will just be ignored.)
	// This enables some important optimizations.
	info := map[string]interface{}{
		// FieldName:      params.Info.FieldName,
		// RootValue:      params.Info.RootValue,
		// VariableValues: params.Info.VariableValues,
		"resultFields": resultFields,
	}

	return res.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
		runner := task.(*userJSRunner)
		return runner.CallWithDB(db, mutationAllowed,
			newUserFunctionJSContext(db),
			params.Args,
			params.Source,
			info)
	})
}

//////// TYPE-NAME RESOLVER:

func (config *GraphQLConfig) compileTypeNameResolver(interfaceName string, jsCode string) (graphql.ResolveTypeFn, error) {
	server, err := newUserFunctionJSServer(interfaceName, "GraphQL type-name resolver", jsCode)
	if err != nil {
		return nil, err
	}
	resolver := &graphQLResolver{
		JSServer: server,
		Name:     interfaceName + ".__typename",
	}
	return func(params graphql.ResolveTypeParams) *graphql.Object {
		db := params.Context.Value(dbKey).(*Database)
		return resolver.ResolveType(db, &params)
	}, nil
}

func (res *graphQLResolver) ResolveType(db *Database, params *graphql.ResolveTypeParams) (objType *graphql.Object) {
	info := map[string]interface{}{}
	result, err := res.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
		runner := task.(*userJSRunner)
		return runner.CallWithDB(db, false,
			newUserFunctionJSContext(db),
			params.Value,
			info)
	})
	if err != nil {
		base.WarnfCtx(params.Context, "GraphQL resolver %q failed with error %v", res.Name, err)
	} else if typeName, ok := result.(string); !ok {
		base.WarnfCtx(params.Context, "GraphQL resolver %q returned a non-string %v; return value must be a type-name string", res.Name, result)
	} else if typeVal := params.Info.Schema.Type(typeName); typeVal == nil {
		base.WarnfCtx(params.Context, "GraphQL resolver %q returned %q, which is not the name of a type", res.Name, typeName)
		base.WarnfCtx(params.Context, "TypeMap is %+v", params.Info.Schema.TypeMap())
	} else if objType, ok = typeVal.(*graphql.Object); !ok {
		base.WarnfCtx(params.Context, "GraphQL resolver %q returned %q which is not the name of an object type", res.Name, typeName)
	}
	return
}
