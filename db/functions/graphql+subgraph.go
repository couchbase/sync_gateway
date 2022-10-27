package functions

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	gqltools "github.com/bhoriuchi/graphql-go-tools"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Extra stuff for Apollo's Federation feature:
// https://www.apollographql.com/docs/federation/subgraph-spec

// Given the schema source code (SDL), adds subgraph support to it and returns it.
// Also adds necessary resolvers to the `resolvers` map.
func (config *GraphQLConfig) addSubgraphExtensions(schemaSource string, resolvers map[string]any) (string, error) {
	// Register scalar type parsers/serializers:
	resolvers["_Any"] = &gqltools.ScalarResolver{
		Serialize:    serializeAny,
		ParseValue:   parseAnyValue,
		ParseLiteral: parseAnyLiteral,
	}
	resolvers["FieldSet"] = &gqltools.ScalarResolver{
		Serialize:    graphql.String.Serialize,
		ParseValue:   graphql.String.ParseValue,
		ParseLiteral: graphql.String.ParseLiteral,
	}
	resolvers["link__Import"] = &gqltools.ScalarResolver{
		Serialize:    graphql.String.Serialize,
		ParseValue:   graphql.String.ParseValue,
		ParseLiteral: graphql.String.ParseLiteral,
	}

	// Find the names of Entity types declared in the schema:
	var multiError *base.MultiError
	entityNames, err := findEntityNames(schemaSource, resolvers)
	if err != nil {
		multiError = multiError.Append(err)
	} else if len(entityNames) == 0 {
		// This is not strictly an error, but almost certainly a mistake...
		multiError = multiError.Append(fmt.Errorf("subgraph schema does not define any Entity types (those with @key directives)"))
	}
	// Add a union named `_Entity` to the schema, that matches all the entity types:
	entitiesStr := fmt.Sprintf("union _Entity = %s\n", strings.Join(entityNames, " | "))
	schemaSource = entitiesStr + kSubgraphSchemaPreamble + kSubgraphQueryExtension + schemaSource

	// Define the field resolver for `Query._service`, which simply returns the schema SDL:
	if resolvers["Query"] == nil {
		resolvers["Query"] = &gqltools.ObjectResolver{
			Fields: gqltools.FieldResolveMap{},
		}
	}
	resolvers["Query"].(*gqltools.ObjectResolver).Fields["_service"] = &gqltools.FieldResolve{
		Resolve: func(p graphql.ResolveParams) (any, error) {
			return map[string]any{"sdl": schemaSource}, nil
		},
	}

	// Collect the reference resolvers:
	refResolvers := referenceResolvers{}
	for typeName, resolverConfig := range config.Resolvers {
		for fieldName, fnConfig := range resolverConfig {
			if fieldName == "__resolveReference" {
				if i := sort.SearchStrings(entityNames, typeName); i >= len(entityNames) || entityNames[i] != typeName {
					err = fmt.Errorf("%s: only Entity types may have a '__resolveReference' resolver", typeName)
				} else {
					refResolvers[typeName], err = config.compileReferenceResolver(typeName, fnConfig)
				}
				if err != nil {
					multiError = multiError.Append(err)
				}
			}
		}
	}
	for _, entity := range entityNames {
		if refResolvers[entity] == nil {
			multiError = multiError.Append(fmt.Errorf("%s: Entity type is missing a '__resolveReference' resolver", entity))
		}
	}

	// Define the field resolver for `Query._entities`:
	resolvers["Query"].(*gqltools.ObjectResolver).Fields["_entities"] = &gqltools.FieldResolve{
		Resolve: func(p graphql.ResolveParams) (any, error) {
			return resolveEntitiesQuery(p, refResolvers)
		},
	}

	// Now return the complete schema:
	return schemaSource, multiError.ErrorOrNil()
}

// Finds the subgraph schema's "Entities" -- object types that have the `@key` directive -- and returns a sorted array of their names.
func findEntityNames(schemaSource string, resolvers map[string]any) ([]string, error) {
	// Callback that will process object types that have the `@key` directive:
	var entityNames []string
	keyVisitor := gqltools.SchemaDirectiveVisitor{
		VisitObject: func(p gqltools.VisitObjectParams) error {
			// ignore directives with `resolvable:false`
			if resolvable, found := p.Args["resolvable"].(bool); resolvable || !found {
				entityNames = append(entityNames, p.Config.Name)
			}
			return nil
		},
	}

	// Parse the schema; this will call the VisitObject function for each type marked with @key:
	_, err := gqltools.MakeExecutableSchema(gqltools.ExecutableSchema{
		TypeDefs:  kSubgraphSchemaPreamble + schemaSource,
		Resolvers: resolvers,
		SchemaDirectives: gqltools.SchemaDirectiveVisitorMap{
			"key": &keyVisitor,
		},
		Debug: true, // This enables logging of unresolved-type errors
	})
	sort.Strings(entityNames)
	return entityNames, err
}

// Handles a `Query._entities` query according to the Federation spec.
func resolveEntitiesQuery(p graphql.ResolveParams, refResolvers referenceResolvers) (any, error) {
	reps := p.Args["representations"].([]any)
	if reps == nil {
		return nil, fmt.Errorf("'representations' argument is not an array")
	}
	// Map representations to results:
	results := []any{}
	for _, repJSON := range reps {
		// Parse representation from JSON:
		var rep map[string]any
		if rep = repJSON.(map[string]any); rep == nil {
			return nil, fmt.Errorf("non-object in 'representations' argument")
		}
		// Use the `__typename` key to look up the reference resolver:
		typename, ok := rep["__typename"].(string)
		if !ok {
			return nil, fmt.Errorf("missing '__typename' key in representation")
		}
		refResolver, ok := refResolvers[typename]
		if !ok {
			return nil, fmt.Errorf("missing __resolveReference resolver for type %q", typename)
		}
		// Call the resolver:
		result, err := refResolver(graphql.ResolveTypeParams{
			Value:   rep,
			Info:    p.Info,
			Context: p.Context,
		})
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

//////// REFERENCE RESOLVER:

type referenceResolvers = map[string]resolveReferenceFn

type resolveReferenceFn = func(p graphql.ResolveTypeParams) (any, error)

func (config *GraphQLConfig) compileReferenceResolver(interfaceName string, fnConfig FunctionConfig) (resolveReferenceFn, error) {
	if fnConfig.Type != "javascript" {
		return nil, fmt.Errorf("a GraphQL '__resolveReference' resolver must be JavaScript")
	} else if fnConfig.Allow != nil {
		return nil, fmt.Errorf("'allow' is not valid in a GraphQL '__resolveReference' resolver")
	}

	fn, err := compileFunction(interfaceName, "GraphQL reference resolver", &fnConfig)
	if err != nil {
		return nil, err
	}
	name := interfaceName + ".__resolveReference"

	return func(params graphql.ResolveTypeParams) (any, error) {
		db_ := params.Context.Value(dbKey).(*db.Database)
		invocation, err := fn.Invoke(db_, nil, false, params.Context)
		if err != nil {
			return nil, err
		}
		result, err := invocation.(*jsInvocation).ResolveType(params)
		if err != nil {
			base.WarnfCtx(params.Context, "GraphQL resolver %q failed with error %v", name, err)
		}
		return result, err
	}, nil
}

//////// THE "_Any" TYPE:

func serializeAny(value any) any {
	log.Printf("???? serializeAny: %T %#v", value, value) // TODO: When is this needed?
	if bytes, err := json.Marshal(value); err != nil {
		return nil
	} else {
		return string(bytes)
	}
}

func parseAnyValue(value any) any {
	return value // Value has already been parsed from JSON, so nothing to do
}

func parseAnyLiteral(valueAST ast.Value) any {
	log.Printf("???? parseAnyLiteral: %T %#v", valueAST, valueAST) // TODO: When is this needed?
	return nil
}

//////// SCHEMA ADDITIONS:

// Types and directives used in subgraph schemas. Pasted from spec.
const kSubgraphSchemaPreamble = `
	scalar _Any
	scalar FieldSet
	scalar link__Import

	enum link__Purpose {
		"SECURITY features provide metadata necessary to securely resolve fields."
		SECURITY
		"EXECUTION features provide metadata necessary for operation execution."
		EXECUTION
	}

	directive @external on FIELD_DEFINITION
	directive @requires(fields: FieldSet!) on FIELD_DEFINITION
	directive @provides(fields: FieldSet!) on FIELD_DEFINITION
	directive @key(fields: FieldSet!, resolvable: Boolean = true) on OBJECT | INTERFACE
	directive @link(url: String!, as: String, for: link__Purpose, import: [link__Import]) on SCHEMA
	directive @shareable on OBJECT | FIELD_DEFINITION
	directive @inaccessible on FIELD_DEFINITION | OBJECT | INTERFACE | UNION | ARGUMENT_DEFINITION | SCALAR | ENUM | ENUM_VALUE | INPUT_OBJECT | INPUT_FIELD_DEFINITION
	directive @tag(name: String!) on FIELD_DEFINITION | INTERFACE | OBJECT | UNION | ARGUMENT_DEFINITION | SCALAR | ENUM | ENUM_VALUE | INPUT_OBJECT | INPUT_FIELD_DEFINITION
	directive @override(from: String!) on FIELD_DEFINITION
	directive @composeDirective(name: String!) on SCHEMA
	`

// Declares top-level subgraph queries.
const kSubgraphQueryExtension = `
	type _Service {
		sdl: String!
	}
	extend type Query {
		_entities(representations: [_Any!]!): [_Entity]!
		_service: _Service!
	}
	`
