package functions

import (
	"context"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

//////// APOLLO FEDERATION (SUBGRAPHS)

func TestGraphQLSubgraph(t *testing.T) {
	// Subgraph schema definition:
	var kSchemaStr = `
	type Task @key(fields: "id") {
		id: ID!
		title: String!
	}
	type Person @key(fields: "id", resolvable: true) {
		id: ID!
		name: String!
	}
	type Wombat @key(fields: "id", resolvable: false) {  # (Not added to _Entities enum!)
		id: ID!
		name: String!
	}
	type Query {
		tasks: [Task!]!
	}`

	// GraphQL configuration:
	var config = GraphQLConfig{
		Schema:   &kSchemaStr,
		Subgraph: true,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"tasks": kDummyFieldResolver,
			},
			"Task": {
				"__resolveReference": {
					Type: "javascript",
					Code: `function(context, value) {
						return {type: "Task", id: value.id, title: "Task-"+value.id};
					}`,
				},
			},
			"Person": {
				"__resolveReference": {
					Type: "javascript",
					Code: `function(context, value) {
						return {type: "Person", id: value.id, name: "Bob "+value.id};
					}`,
				},
			},
			"_Entity": {
				"__typename": {
					Type: "javascript",
					Code: `function(context, value) {return value.type;}`,
				},
			},
		},
	}

	// Compile the schema:
	var gq db.GraphQL
	var err error
	t.Run("CompileSchema", func(t *testing.T) {
		_, gq, err = CompileFunctions(nil, &config)
		assert.NoError(t, err)
	})
	if err != nil {
		return
	}

	db, ctx := setupTestDBWithFunctions(t, nil, &kTestGraphQLConfigWithN1QL)
	defer db.Close(ctx)

	// Handle a `_service` query:
	t.Run("ServiceQuery", func(t *testing.T) {
		result, err := gq.Query(db, `query { _service { sdl } }`,
			"", nil, false, context.TODO())
		if !assertGraphQLNoErrors(t, result, err) {
			return
		}
		sdl, ok := result.Data.(map[string]any)["_service"].(map[string]any)["sdl"].(string)
		assert.True(t, ok, "No 'sdl' key in result: %#v", result)
		assert.True(t, strings.Contains(sdl, "union _Entity = Person | Task"), "Unexpected sdl: %s", sdl)
	})

	// Handle an `_entities` query:
	t.Run("EntitiesQuery", func(t *testing.T) {
		vars := map[string]any{
			"reprs": []any{
				map[string]any{"__typename": "Task", "id": "001"},
				map[string]any{"__typename": "Person", "id": "1138"},
			},
		}

		result, err := gq.Query(db, `
		query($reprs: [_Any!]!) {
			_entities(representations: $reprs) {
				...on Task { title }
				...on Person { name }
			}
		}`,
			"", vars, false, context.TODO())
		assertGraphQLResult(t, `{"_entities":[{"title":"Task-001"},{"name":"Bob 1138"}]}`, result, err)
	})
}

func TestGraphQLApolloCompatibilitySubgraph(t *testing.T) {
	// Subgraph schema definition; see https://github.com/apollographql/apollo-federation-subgraph-compatibility/blob/main/CONTRIBUTORS.md
	// (I had to comment out a few things to get it to compile with graphql-go)
	var kSchemaStr = `
	#extend schema
	#@link(
	#  url: "https://specs.apollo.dev/federation/v2.0",
	#  import: ["@extends", "@external", "@inaccessible", "@key", "@override", "@provides", "@requires", "@shareable", "@tag"]
	#)

  type Product @key(fields: "id") @key(fields: "sku package") @key(fields: "sku variation { id }") {
	id: ID!
	sku: String
	package: String
	variation: ProductVariation
	dimensions: ProductDimension
	createdBy: User @provides(fields: "totalProductsCreated")
	notes: String @tag(name: "internal")
	research: [ProductResearch!]!
  }

  type DeprecatedProduct @key(fields: "sku package") {
	sku: String!
	package: String!
	reason: String
	createdBy: User
  }

  type ProductVariation {
	id: ID!
  }

  type ProductResearch @key(fields: "study { caseNumber }") {
	study: CaseStudy!
	outcome: String
  }

  type CaseStudy {
	caseNumber: ID!
	description: String
  }

  type ProductDimension @shareable {
	size: String
	weight: Float
	unit: String @inaccessible
  }

  extend
   type Query {
	product(id: ID!): Product
	deprecatedProduct(sku: String!, package: String!): DeprecatedProduct @deprecated(reason: "Use product query instead")
  }

  extend
   type User @key(fields: "email") {
	averageProductsCreatedPerYear: Int @requires(fields: "totalProductsCreated yearsOfEmployment")
	email: ID! @external
	name: String @override(from: "users")
	totalProductsCreated: Int @external
	yearsOfEmployment: Int! @external
  }`

	var config = GraphQLConfig{
		Schema:   &kSchemaStr,
		Subgraph: true,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"product":           kDummyFieldResolver,
				"deprecatedProduct": kDummyFieldResolver,
			},
			"_Entity": {
				"__typename": {
					Type: "javascript",
					Code: `function(context, value) {return value.type;}`,
				},
			},
			"Product": {
				"__resolveReference": {
					Type: "javascript",
					Code: `function(context, value) {
						return {};
					}`,
				},
			},
			"DeprecatedProduct": {
				"__resolveReference": {
					Type: "javascript",
					Code: `function(context, value) {
						return {};
					}`,
				},
			},
			"ProductResearch": {
				"__resolveReference": {
					Type: "javascript",
					Code: `function(context, value) {
						return {};
					}`,
				},
			},
			"User": {
				"__resolveReference": {
					Type: "javascript",
					Code: `function(context, value) {
						return {};
					}`,
				},
			},
		},
	}

	// Compile the schema:
	//var gq db.GraphQL
	var err error
	t.Run("CompileSchema", func(t *testing.T) {
		_, _, err = CompileFunctions(nil, &config)
		assert.NoError(t, err)
	})
	if err != nil {
		return
	}
}
