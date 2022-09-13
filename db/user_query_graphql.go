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
	"log"
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

// Maps GraphQL query/mutation names to the resolvers that implement them.
type GraphQLResolverConfig map[string]UserFunctionConfig

// GraphQL query handler for a Database.
type GraphQL struct {
	dbc    *DatabaseContext
	schema graphql.Schema // The compiled GraphQL schema object
}

type gqContextKey string

var dbKey = gqContextKey("db")                 // Context key to access the Database instance
var mutAllowedKey = gqContextKey("mutAllowed") // Context key to access `mutationAllowed`

// Top-level public method to run a GraphQL query on a Database.
func (db *Database) UserGraphQLQuery(query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*graphql.Result, error) {
	if db.graphQL == nil {
		return nil, base.HTTPErrorf(http.StatusServiceUnavailable, "GraphQL is not configured")
	}
	return db.graphQL.Query(db, query, operationName, variables, mutationAllowed, ctx)
}

// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (gq *GraphQL) Query(db *Database, query string, operationName string, variables map[string]interface{}, mutationAllowed bool, ctx context.Context) (*graphql.Result, error) {
	if err := db.CheckTimeout(); err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, dbKey, db)
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
		for fieldName, fnConfig := range resolver {
			if fnConfig.Args != nil {
				err = fmt.Errorf("'args' is not valid in a GraphQL resolver config")
			} else if fieldName == "__typename" {
				// The "__typename" resolver returns the name of the concrete type of an
				// instance of an interface.
				typeNameResolver, err = config.compileTypeNameResolver(name, fnConfig)
			} else {
				var fn graphql.FieldResolveFn
				fn, err = config.compileFieldResolver(name, fieldName, fnConfig)
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

func graphQLResolverName(typeName string, fieldName string) string {
	return typeName + ":" + fieldName
}

// Creates a graphQLResolver for the given JavaScript code, and returns a graphql-go FieldResolveFn
// that invokes it.
func (config *GraphQLConfig) compileFieldResolver(typeName string, fieldName string, fnConfig UserFunctionConfig) (graphql.FieldResolveFn, error) {
	name := graphQLResolverName(typeName, fieldName)
	isMutation := typeName == "Mutation"
	if isMutation && fnConfig.Type == "query" {
		return nil, fmt.Errorf("GraphQL mutations must be implemented in JavaScript")
	}

	userFn, err := compileUserFunction(name, "GraphQL resolver", &fnConfig)
	if err != nil {
		return nil, err
	}
	userFn.checkArgs = false
	userFn.allowByDefault = true

	return func(params graphql.ResolveParams) (interface{}, error) {
		db := params.Context.Value(dbKey).(*Database)
		if isMutation && !params.Context.Value(mutAllowedKey).(bool) {
			return nil, base.HTTPErrorf(http.StatusForbidden, "a read-only request is not allowed to call a GraphQL mutation")
		}
		invocation, err := userFn.Invoke(db, params.Args, isMutation, params.Context)
		if err != nil {
			return nil, err
		}
		return invocation.Resolve(params)
	}, nil
}

// Calls a UserFunctionInvocation as a GraphQL resolver, given the parameters passed by the go-graphql API.
func (fn *UserFunctionInvocation) Resolve(params graphql.ResolveParams) (interface{}, error) {
	// Collect the 'selectedFieldNames', the fields the query wants from the value being resolved:
	selectedFieldNames := []string{}
	if len(params.Info.FieldASTs) > 0 {
		for i, f := range params.Info.FieldASTs {
			log.Printf("### field %d is %#v", i, f) //TEMP
		}
		if set := params.Info.FieldASTs[0].SelectionSet; set != nil {
			for _, sel := range set.Selections {
				if subfield, ok := sel.(*ast.Field); ok {
					if subfield.Name.Kind == "Name" {
						selectedFieldNames = append(selectedFieldNames, subfield.Name.Value)
					}
				}
			}
		}
	}

	// The `info` parameter passed to the JS function; fields are a subset of graphql.ResolveInfo.
	// NOTE: We've removed these fields until we get feedback that they're needed by developers.
	//   `selectedFieldNames` is not provided (directly) by ResolveInfo; it contains the fields of the
	// resolver's result that will be used by the query (other fields will just be ignored.)
	// This enables some important optimizations.
	info := map[string]interface{}{
		// FieldName:      params.Info.FieldName,
		// RootValue:      params.Info.RootValue,
		// VariableValues: params.Info.VariableValues,
		"selectedFieldNames": selectedFieldNames,
	}

	if fn.compiled != nil {
		// JavaScript resolver:
		return fn.compiled.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
			runner := task.(*userJSRunner)
			return runner.CallWithDB(fn.db,
				fn.mutationAllowed,
				fn.ctx,
				newUserFunctionJSContext(fn.db),
				params.Args,
				params.Source,
				info)
		})

	} else {
		// N1QL resolver:
		fn.n1qlArgs["parent"] = params.Source
		fn.n1qlArgs["info"] = info
		// Run the query:
		result, err := fn.Run()
		if err != nil {
			return nil, err
		}
		if !isGraphQLListType(params.Info.ReturnType) {
			// GraphQL result type is not a list (array), but N1QL always returns an array.
			// So use the first row of the result as the value, if there is one.
			if rows, ok := result.([]interface{}); ok {
				if len(rows) > 0 {
					result = rows[0]
				} else {
					return nil, nil
				}
			}
			if isGraphQLScalarType(params.Info.ReturnType) {
				// GraphQL result type is a scalar, but a N1QL row is always an object.
				// Use the single field of the object, if any, as the result:
				row := result.(map[string]interface{})
				if len(row) != 1 {
					return nil, base.HTTPErrorf(http.StatusInternalServerError, "resolver %q returns scalar type %s, but its N1QL query returns %d columns, not 1", fn.name, params.Info.ReturnType, len(row))
				}
				for _, value := range row {
					result = value
				}
			}
		}
		return result, nil
	}
}

func isGraphQLListType(typ graphql.Output) bool {
	if nonnull, ok := typ.(*graphql.NonNull); ok {
		typ = nonnull.OfType
	}
	_, isList := typ.(*graphql.List)
	return isList
}

func isGraphQLScalarType(typ graphql.Output) bool {
	if nonnull, ok := typ.(*graphql.NonNull); ok {
		typ = nonnull.OfType
	}
	_, isScalar := typ.(*graphql.Scalar)
	return isScalar
}

//////// TYPE-NAME RESOLVER:

func (config *GraphQLConfig) compileTypeNameResolver(interfaceName string, fnConfig UserFunctionConfig) (graphql.ResolveTypeFn, error) {
	if fnConfig.Type != "javascript" {
		return nil, fmt.Errorf("a GraphQL '__typename__' resolver must be JavaScript")
	} else if fnConfig.Allow != nil {
		return nil, fmt.Errorf("'allow' is not valid in a GraphQL '__typename__' resolver")
	}

	server, err := newUserFunctionJSServer(interfaceName, "GraphQL type-name resolver", fnConfig.Code)
	if err != nil {
		return nil, err
	}
	name := interfaceName + ".__typename"

	return func(params graphql.ResolveTypeParams) *graphql.Object {
		db := params.Context.Value(dbKey).(*Database)
		info := map[string]interface{}{}
		result, err := server.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
			runner := task.(*userJSRunner)
			return runner.CallWithDB(db,
				false,
				params.Context,
				newUserFunctionJSContext(db),
				params.Value,
				info)
		})
		var objType *graphql.Object
		if err != nil {
			base.WarnfCtx(params.Context, "GraphQL resolver %q failed with error %v", name, err)
		} else if typeName, ok := result.(string); !ok {
			base.WarnfCtx(params.Context, "GraphQL resolver %q returned a non-string %v; return value must be a type-name string", name, result)
		} else if typeVal := params.Info.Schema.Type(typeName); typeVal == nil {
			base.WarnfCtx(params.Context, "GraphQL resolver %q returned %q, which is not the name of a type", name, typeName)
			base.WarnfCtx(params.Context, "TypeMap is %+v", params.Info.Schema.TypeMap())
		} else if objType, ok = typeVal.(*graphql.Object); !ok {
			base.WarnfCtx(params.Context, "GraphQL resolver %q returned %q which is not the name of an object type", name, typeName)
		}
		return objType
	}, nil
}
