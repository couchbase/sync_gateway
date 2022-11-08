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
	"fmt"
	"os"

	gqltools "github.com/bhoriuchi/graphql-go-tools"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

//////// CONFIGURATION TYPES:

// Configuration for GraphQL.
type GraphQLConfig struct {
	Schema           *string         `json:"schema,omitempty"`             // Schema in SDL syntax
	SchemaFile       *string         `json:"schemaFile,omitempty"`         // Path of schema file
	Resolvers        GraphQLTypesMap `json:"resolvers"`                    // Defines query/mutation code
	MaxSchemaSize    *int            `json:"max_schema_size,omitempty"`    // Maximum length (in bytes) of GraphQL schema; default is 0, for unlimited
	MaxResolverCount *int            `json:"max_resolver_count,omitempty"` // Maximum number of GraphQL resolvers; default is 0, for unlimited
	MaxCodeSize      *int            `json:"max_code_size,omitempty"`      // Maximum length (in bytes) of a function's code
	MaxRequestSize   *int            `json:"max_request_size,omitempty"`   // Maximum size of the encoded query & arguments
}

// Maps GraphQL type names (incl. "Query") to their resolvers.
type GraphQLTypesMap map[string]GraphQLResolverConfig

// Maps GraphQL field names to the resolvers that implement them.
type GraphQLResolverConfig map[string]FunctionConfig

//////// QUERYING

// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (gq *graphQLImpl) Query(database *db.Database, query string, operationName string, variables map[string]any, mutationAllowed bool, ctx context.Context) (*graphql.Result, error) {
	if err := db.CheckTimeout(ctx); err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, dbKey, database)
	if !mutationAllowed && ctx.Value(readOnlyKey) == nil {
		ctx = context.WithValue(ctx, readOnlyKey, "a read-only GraphQL API call")
	}
	result := graphql.Do(graphql.Params{
		Schema:         gq.schema,
		RequestString:  query,
		VariableValues: variables,
		OperationName:  operationName,
		Context:        ctx,
	})
	return result, nil
}

func (gq *graphQLImpl) MaxRequestSize() *int {
	return gq.config.MaxRequestSize
}

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

type gqContextKey string

var dbKey = gqContextKey("db")             // Context key to access the db.Database instance
var readOnlyKey = gqContextKey("readOnly") // Context key preventing mutation; val is fn name

//////// GRAPHQL INITIALIZATION:

// Implementation of db.graphQLImpl interface.
type graphQLImpl struct {
	config *GraphQLConfig
	schema graphql.Schema // The compiled GraphQL schema object
}

// Creates a new GraphQL instance from its configuration.
func CompileGraphQL(config *GraphQLConfig) (*graphQLImpl, error) {
	if schema, err := config.compileSchema(); err != nil {
		return nil, err
	} else {
		gql := &graphQLImpl{
			config: config,
			schema: schema,
		}
		return gql, nil
	}
}

// Validates a GraphQL configuration by parsing the schema.
func (config *GraphQLConfig) Validate(ctx context.Context) error {
	_, err := config.compileSchema()
	return err
}

func (config *GraphQLConfig) compileSchema() (schema graphql.Schema, err error) {
	// Get the schema source, from either `schema` or `schemaFile`:
	schemaSource, err := config.getSchema()
	if err != nil {
		return schema, err
	}
	if config.MaxSchemaSize != nil && len(schemaSource) > *config.MaxSchemaSize {
		return schema, fmt.Errorf("GraphQL schema too large (> %d bytes)", *config.MaxSchemaSize)
	}

	var multiError *base.MultiError
	resolverCount := 0
	// Assemble the resolvers:
	resolvers := map[string]any{}
ResolverLoop:
	for typeName, resolver := range config.Resolvers {
		fieldMap := map[string]*gqltools.FieldResolve{}
		var typeNameResolver graphql.ResolveTypeFn
		for fieldName, fnConfig := range resolver {
			if fnConfig.Args != nil {
				err = fmt.Errorf("'args' is not valid in a GraphQL resolver config")
			} else if config.MaxCodeSize != nil && len(fnConfig.Code) > *config.MaxCodeSize {
				err = fmt.Errorf("resolver %s code too large (> %d bytes)", fieldName, *config.MaxCodeSize)
				multiError = multiError.Append(err)
			} else if fieldName == "__typename" {
				// The "__typename" resolver returns the name of the concrete type of an
				// instance of an interface.
				typeNameResolver, err = config.compileTypeNameResolver(typeName, fnConfig)
				resolverCount += 1
			} else {
				var fn graphql.FieldResolveFn
				fn, err = config.compileFieldResolver(typeName, fieldName, fnConfig)
				fieldMap[fieldName] = &gqltools.FieldResolve{Resolve: fn}
				resolverCount += 1
			}
			if err != nil {
				multiError = multiError.Append(err)
			}
			if config.MaxResolverCount != nil && resolverCount > *config.MaxResolverCount {
				err = fmt.Errorf("too many GraphQL resolvers (> %d)", *config.MaxResolverCount)
				multiError = multiError.Append(err)
				break ResolverLoop // stop
			}
		}
		if typeNameResolver == nil {
			resolvers[typeName] = &gqltools.ObjectResolver{
				Fields: fieldMap,
			}
		} else {
			resolvers[typeName] = &gqltools.InterfaceResolver{
				Fields:      fieldMap,
				ResolveType: typeNameResolver,
			}
		}
	}
	if multiError != nil {
		return schema, multiError.ErrorOrNil()
	}

	// Now compile the schema and create the graphql.Schema object:
	schema, err = gqltools.MakeExecutableSchema(gqltools.ExecutableSchema{
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

// Subtype of db.UserFunctionInvocation that can be called as a GraphQL resolver.
// Implemented by userFunctionJSInvocation and userFunctionN1QLInvocation
type resolver = interface {
	db.UserFunctionInvocation

	// Calls a user function as a GraphQL resolver, given the parameters passed by the go-graphql API.
	Resolve(params graphql.ResolveParams) (any, error)
}

func graphQLResolverName(typeName string, fieldName string) string {
	return typeName + ":" + fieldName
}

// Creates a graphQLResolver for the given JavaScript code, and returns a graphql-go FieldResolveFn
// that invokes it.
func (config *GraphQLConfig) compileFieldResolver(typeName string, fieldName string, fnConfig FunctionConfig) (graphql.FieldResolveFn, error) {
	name := graphQLResolverName(typeName, fieldName)
	isMutation := typeName == "Mutation"
	if isMutation && fnConfig.Type == "query" {
		return nil, fmt.Errorf("GraphQL mutations must be implemented in JavaScript")
	}

	userFn, err := compileFunction(name, "GraphQL resolver", &fnConfig)
	if err != nil {
		return nil, err
	}
	userFn.checkArgs = false                                                // graphql-go does this
	userFn.allowByDefault = (typeName != "Query" && typeName != "Mutation") // not at top level
	userFn.Mutating = isMutation

	return func(params graphql.ResolveParams) (any, error) {
		ctx := params.Context
		db := ctx.Value(dbKey).(*db.Database)
		invocation, err := userFn.Invoke(db, params.Args, isMutation, ctx)
		if err != nil {
			return nil, err
		}
		return invocation.(resolver).Resolve(params)
	}, nil
}

func resolverInfo(params graphql.ResolveParams) map[string]any {
	// Collect the 'selectedFieldNames', the fields the query wants from the value being resolved:
	selectedFieldNames := []string{}
	if len(params.Info.FieldASTs) > 0 {
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

	// The `info` parameter passed to the resolver fn; fields are a subset of graphql.ResolveInfo.
	// NOTE: We've removed these fields until we get feedback that they're needed by developers.
	//   `selectedFieldNames` is not provided (directly) by ResolveInfo; it contains the fields of the
	// resolver's result that will be used by the query (other fields will just be ignored.)
	// This enables some important optimizations.
	return map[string]any{
		"selectedFieldNames": selectedFieldNames,
	}
}

//////// TYPE-NAME RESOLVER:

func (config *GraphQLConfig) compileTypeNameResolver(interfaceName string, fnConfig FunctionConfig) (graphql.ResolveTypeFn, error) {
	if fnConfig.Type != "javascript" {
		return nil, fmt.Errorf("a GraphQL '__typename__' resolver must be JavaScript")
	} else if fnConfig.Allow != nil {
		return nil, fmt.Errorf("'allow' is not valid in a GraphQL '__typename__' resolver")
	}

	fn, err := compileFunction(interfaceName, "GraphQL type-name resolver", &fnConfig)
	if err != nil {
		return nil, err
	}
	name := interfaceName + ".__typename"

	return func(params graphql.ResolveTypeParams) *graphql.Object {
		db_ := params.Context.Value(dbKey).(*db.Database)
		invocation, err := fn.Invoke(db_, nil, false, params.Context)
		if err != nil {
			return nil
		}
		result, err := invocation.(*jsInvocation).ResolveType(params)
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

//////// UTILITIES

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
