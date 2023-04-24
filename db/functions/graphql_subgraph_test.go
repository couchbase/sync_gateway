//go:build cb_sg_v8

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
	"strings"
	"testing"

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

	// Validate the config:
	t.Run("ValidateSchema", func(t *testing.T) {
		assert.NoError(t, testValidateFunctions(nil, &config))
	})

	db, ctx := setupTestDBWithFunctions(t, nil, &config)
	defer db.Close(ctx)

	// Handle a `_service` query:
	t.Run("ServiceQuery", func(t *testing.T) {
		result, err := db.GraphQL.Query(db, `query { _service { sdl } }`,
			"", nil, false, ctx)
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

		result, err := db.GraphQL.Query(db, `
		query($reprs: [_Any!]!) {
			_entities(representations: $reprs) {
				...on Task { title }
				...on Person { name }
			}
		}`,
			"", vars, false, ctx)
		assertGraphQLResult(t, `{"_entities":[{"title":"Task-001"},{"name":"Bob 1138"}]}`, result, err)
	})
}

func TestGraphQLApolloCompatibilitySubgraph(t *testing.T) {
	// Subgraph schema definition; see https://github.com/apollographql/apollo-federation-subgraph-compatibility/blob/main/CONTRIBUTORS.md
	// (I had to comment out a few things to get it to compile with graphql-go)
	var kSchemaStr = `
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

#  extend
   type Query {
	product(id: ID!): Product
	deprecatedProduct(sku: String!, package: String!): DeprecatedProduct @deprecated(reason: "Use product query instead")
  }

#  extend
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

	assert.NoError(t, testValidateFunctions(nil, &config))
}
