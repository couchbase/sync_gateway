/*
Copyright 2020-Present Couchbase, Inc.

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
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The GraphQL schema:
var kTestGraphQLSchema = `
	type Task {
		id: ID!
		title: String!
		description: String
		done: Boolean
		tags: [String!]
		secretNotes: String		# Admin-only
	}
	type Query {
		square(n: Int!): Int!
		infinite: Int!
		task(id: ID!): Task
		tasks: [Task!]!
		toDo: [Task!]!
	}
	type Mutation {
		complete(id: ID!): Task
		addTag(id: ID!, tag: String!): Task
	}
`

// The GraphQL configuration:
var kTestGraphQLConfig = GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]GraphQLResolverConfig{
		"Query": {
			"square": {
				Type:  "javascript",
				Code:  `function(parent, args, context, info) {return args.n * args.n;}`,
				Allow: allowAll,
			},
			"infinite": {
				Type:  "javascript",
				Code:  `function(parent, args, context, info) {return context.user.function("infinite");}`,
				Allow: allowAll,
			},
			"task": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("getTask", {id: args.id});}`,
				Allow: allowAll,
			},
			"tasks": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("all");}`,
				Allow: allowAll,
			},
			"toDo": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						var result=new Array(); var all = context.user.function("all");
						for (var i = 0; i < all.length; i++)
							if (!all[i].done) result.push(all[i]);
						return result;}`,
				Allow: allowAll,
			},
		},
		"Mutation": {
			"complete": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
							if (Object.keys(parent).length != 0) throw "Unexpected parent";
							if (Object.keys(args).length != 1) throw "Unexpected args";
							if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
							if (!context.user) throw "Missing context.user";
							if (!context.admin) throw "Missing context.admin";
							context.requireMutating();
							var task = context.user.function("getTask", {id: args.id});
							if (!task) return undefined;
							task.done = true;
							return task;}`,
				Allow: allowAll,
			},
			"addTag": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
							context.requireMutating();
							var task = context.user.function("getTask", {id: args.id});
							if (!task) return undefined;
							if (!task.tags) task.tags = [];
							task.tags.push(args.tag);
							return task;}`,
				Allow: allowAll,
			},
		},
		"Task": {
			"secretNotes": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
								if (!parent.id) throw "Invalid parent";
								if (Object.keys(args).length != 0) throw "Unexpected args";
								if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
								if (!context.user) throw "Missing context.user";
								if (!context.admin) throw "Missing context.admin";
								return "TOP SECRET!";}`,
				Allow: &Allow{Users: base.Set{}}, // only admins
			},
		},
	},
}

// JS function helpers:
var kTestGraphQLUserFunctionsConfig = FunctionsConfig{
	Definitions: FunctionsDefs{
		"all": {
			Type: "javascript",
			Code: `function(context, args) {
						return [
						{id: "a", "title": "Applesauce", done:true, tags:["fruit","soft"]},
						{id: "b", "title": "Beer", description: "Bass ale please"},
						{id: "m", "title": "Mangoes"} ];}`,
			Allow: &Allow{Channels: []string{"*"}},
		},
		"getTask": {
			Type: "javascript",
			Code: `function(context, args) {
						var all = context.user.function("all");
						for (var i = 0; i < all.length; i++)
							if (all[i].id == args.id) return all[i];
						return undefined;}`,
			Args:  []string{"id"},
			Allow: &Allow{Channels: []string{"*"}},
		},
		"infinite": {
			Type: "javascript",
			Code: `function(context, args) {
				var result = context.user.graphql("query{ infinite }");
				if (result.errors) throw "GraphQL query failed:" + result.errors[0].message;
				return -1;}`,
			Allow: &Allow{Channels: []string{"*"}},
		},
	},
}

func assertGraphQLResult(t *testing.T, expected string, result *graphql.Result, err error) bool {
	if !assertGraphQLNoErrors(t, result, err) {
		return false
	}
	j, err := json.Marshal(result.Data)
	return assert.NoError(t, err) &&
		assert.Equal(t, expected, string(j))
}

func assertGraphQLNoErrors(t *testing.T, result *graphql.Result, err error) bool {
	return assert.NoError(t, err) &&
		assert.NotNil(t, result) &&
		assert.Zerof(t, len(result.Errors), "Unexpected GraphQL errors: %v", result.Errors)
}

// Per the spec, GraphQL errors are not indicated via `err`, rather through an `errors`
// property in the `result` object.
func assertGraphQLError(t *testing.T, expectedErrorText string, result *graphql.Result, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if len(result.Errors) == 0 {
		data, err := json.Marshal(result.Data)
		assert.NoError(t, err)
		assert.NotZero(t, len(result.Errors), "Expected GraphQL error but got none; data is %s", string(data))
		return
	}
	for _, err := range result.Errors {
		if strings.Contains(err.Error(), expectedErrorText) {
			return
		}
	}
	t.Logf("GraphQL error did not contain expected string %q: actually %#v", expectedErrorText, result.Errors)
	t.Fail()
}

// Unit test for GraphQL queries.
func TestUserGraphQL(t *testing.T) {
	//base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := setupTestDBWithFunctions(t, &kTestGraphQLUserFunctionsConfig, &kTestGraphQLConfig)
	defer db.Close(ctx)

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserGraphQLAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.SetUser(addUserAlice(t, db))
	assert.True(t, db.User().RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserGraphQLAsUser(t, ctx, db) })
}

func testUserGraphQLCommon(t *testing.T, ctx context.Context, db *db.Database) {
	// Successful query:
	result, err := db.UserGraphQLQuery(`query{ square(n: 12) }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"square":144}`, result, err)

	result, err = db.UserGraphQLQuery(`query($num:Int!){ square(n: $num) }`, "",
		map[string]any{"num": 12}, false, ctx)
	assertGraphQLResult(t, `{"square":144}`, result, err)

	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {id,title,done} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"task":{"done":true,"id":"a","title":"Applesauce"}}`, result, err)

	result, err = db.UserGraphQLQuery(`query{ tasks {title} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"tasks":[{"title":"Applesauce"},{"title":"Beer"},{"title":"Mangoes"}]}`, result, err)

	// ERRORS:

	// Nonexistent query:
	result, err = db.UserGraphQLQuery(`query{ bogus(id:"a") {id} }`, "", nil, false, ctx)
	assertGraphQLError(t, "Cannot query field \"bogus\" on type \"Query\"", result, err)

	// Invalid argument:
	result, err = db.UserGraphQLQuery(`query{ task(foo:69) {id,title,done} }`, "", nil, false, ctx)
	assertGraphQLError(t, "Unknown argument \"foo\"", result, err)

	// Mutation when no mutations allowed (mutationAllowed = false):
	result, err = db.UserGraphQLQuery(`mutation{ complete(id:"b") {done} }`, "", nil, false, ctx)
	assertGraphQLError(t, "403", result, err)

	// Infinite regress:
	result, err = db.UserGraphQLQuery(`query{ infinite }`, "", nil, false, ctx)
	assertGraphQLError(t, "508", result, err)
}

func testUserGraphQLAsAdmin(t *testing.T, ctx context.Context, db *db.Database) {
	testUserGraphQLCommon(t, ctx, db)

	// Admin tests updating "a":
	result, err := db.UserGraphQLQuery(`mutation{ addTag(id:"a", tag:"cold") {id,title,done,tags} }`, "", nil, true, ctx)
	assertGraphQLResult(t, `{"addTag":{"done":true,"id":"a","tags":["fruit","soft","cold"],"title":"Applesauce"}}`, result, err)

	// Admin-only field:
	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"task":{"secretNotes":"TOP SECRET!"}}`, result, err)
}

func testUserGraphQLAsUser(t *testing.T, ctx context.Context, db *db.Database) {
	testUserGraphQLCommon(t, ctx, db)

	// Regular user tests updating "m":
	result, err := db.UserGraphQLQuery(`mutation{ addTag(id:"m", tag:"ripe") {id,title,done,tags} }`, "", nil, true, ctx)
	assertGraphQLResult(t, `{"addTag":{"done":null,"id":"m","tags":["ripe"],"title":"Mangoes"}}`, result, err)

	// ERRORS:

	// Can't get admin-only field:
	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false, ctx)
	assertGraphQLError(t, "403", result, err)
}

//////// GRAPHQL N1QL RESOLVER TESTS

// The GraphQL configuration, using N1QL in some resolvers:
var kTestGraphQLConfigWithN1QL = GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]GraphQLResolverConfig{
		"Query": {
			"square": {
				Type:  "javascript",
				Code:  `function(parent, args, context, info) {return args.n * args.n;}`,
				Allow: allowAll,
			},
			"infinite": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						var result = context.user.graphql("query{ infinite }");
						if (result.errors) throw "GraphQL query failed: " + result.errors[0].message;
						return -1;}`,
				Allow: allowAll,
			},
			"task": {
				// This tests the ability of the resolver to return the 1st result row when the
				// GraphQL return type is not a List.
				Type:  "query",
				Code:  `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE meta().id = $args.id AND type = "task"`,
				Allow: allowAll,
			},
			"tasks": {
				Type:  "query",
				Code:  `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE type = "task" ORDER BY title`,
				Allow: allowAll,
			},
			"toDo": {
				Type:  "query",
				Code:  `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE type = "task" AND NOT done ORDER BY title`,
				Allow: allowAll,
			},
		},
		"Mutation": {
			"complete": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
					var task = context.user.defaultCollection.get(args.id);
					if (!task) return null;
					task.id = args.id;
					if (!task.done) {
					  task.done = true;
					  context.user.defaultCollection.save(task, args.id);
					}
					return task;}`,
				Allow: allowAll,
			},
			"addTag": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
							var task = context.user.defaultCollection.get(args.id);
							if (!task) return null;
							task.id = args.id;
							if (!task.tags) task.tags = [];
							task.tags.push(args.tag);
							context.user.defaultCollection.save(task, args.id);
							return task;}`,
				Allow: allowAll,
			},
		},
		"Task": {
			"secretNotes": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
								if (!parent.id) throw "Invalid parent";
								if (Object.keys(args).length != 0) throw "Unexpected args";
								if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
								if (!context.user) throw "Missing context.user";
								if (!context.admin) throw "Missing context.admin";
								return "TOP SECRET!";}`,
				Allow: &Allow{Users: base.Set{}}, // only admins
			},
			"description": {
				// This tests the magic $parent parameter,
				// and returning the single column of the single row when the result is a scalar.
				Type:  "query",
				Code:  `SELECT $parent.description`,
				Allow: allowAll,
			},
		},
	},
}

type Body = db.Body

// Unit test for GraphQL queries.
func TestUserGraphQLWithN1QL(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only (requires N1QL)")
	}

	db, ctx := setupTestDBWithFunctions(t, nil, &kTestGraphQLConfigWithN1QL)
	defer db.Close(ctx)

	_, _, _ = db.Put(ctx, "a", Body{"type": "task", "title": "Applesauce", "done": true, "tags": []string{"fruit", "soft"}, "channels": "wonderland"})
	_, _, _ = db.Put(ctx, "b", Body{"type": "task", "title": "Beer", "description": "Bass ale please", "channels": "wonderland"})
	_, _, _ = db.Put(ctx, "m", Body{"type": "task", "title": "Mangoes", "channels": "wonderland"})

	n1qlStore, ok := base.AsN1QLStore(db.Bucket)
	require.True(t, ok)

	createdPrimaryIdx := createPrimaryIndex(t, n1qlStore)

	if createdPrimaryIdx {
		defer func() {
			err := n1qlStore.DropIndex(base.PrimaryIndexName)
			require.NoError(t, err)
		}()

	}
	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserGraphQLAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.SetUser(addUserAlice(t, db))
	require.True(t, db.User().RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserGraphQLAsUser(t, ctx, db) })

	// Test the N1QL resolver that uses $parent:
	result, err := db.UserGraphQLQuery(`query{ task(id:"b") {description,title} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"task":{"description":"Bass ale please","title":"Beer"}}`, result, err)
}

//////// CONFIGURATION ERROR TESTS

// A placeholder GraphQL resolver function (should never be called!)
var kDummyFieldResolver = FunctionConfig{
	Type:  "javascript",
	Code:  `function(parent, args, context, info) {throw 'unimplemented';}`,
	Allow: allowAll,
}

var kDummyTypeResolver = FunctionConfig{
	Type: "javascript",
	Code: `function(context,value) {throw 'unimplemented';}`,
}

// All top-level fields (those of Query & Mutation) must have JS resolvers.
func TestGraphQLMissingFieldResolvers(t *testing.T) {
	var schema = `
	type Query {
		secrets: [String!]!
		scandals: [String!]!
	}
	type Mutation {
		crush(strength:Int!): Boolean
		mangle : String!
		destroy(utterly:Boolean): Boolean
	}`
	var config = GraphQLConfig{
		Schema: &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"secrets": kDummyFieldResolver,
			},
		},
	}
	_, err := CompileGraphQL(&config)
	assert.ErrorContains(t, err, "Query.scandals: Missing")
	assert.NotContains(t, err.Error(), "Query.secrets")
	assert.ErrorContains(t, err, "Mutation.crush: Missing")
	assert.ErrorContains(t, err, "Mutation.mangle: Missing")
	assert.ErrorContains(t, err, "Mutation.destroy: Missing")
}

// Every interface type in the schema must have a JS __typename resolver.
func TestGraphQLMissingTypeResolvers(t *testing.T) {
	var schema = `
		interface Node {
			id:             ID
		}
		interface Location {
			geo:            Geo
		}
		type Geo {
			lat:            Float!
			lon:            Float!
		}
		type Airport implements Node & Location {
			id:             ID!
			geo:            Geo
		}
		type Hotel implements Node & Location {
			id:             ID!
			geo:            Geo
		}
		type Query {
			locationsNear(geo: Geo!): [Location!]!
		}`
	var config = GraphQLConfig{
		Schema: &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"locationsNear": kDummyFieldResolver,
			},
			"Node": {
				"__typename": kDummyTypeResolver,
			},
		},
	}
	_, err := CompileGraphQL(&config)
	assert.ErrorContains(t, err, "Location: GraphQL interface must implement a '__typename' resolver")
	assert.NotContains(t, err.Error(), "Node:")
}

func TestGraphQLMaxSchemaSize(t *testing.T) {
	var schema = `
	type Task {
		id: ID!
		title: String!
		description: String
		done: Boolean
		tags: [String!]
		secretNotes: String		# Admin-only
	}`
	var config = GraphQLConfig{
		MaxSchemaSize: base.IntPtr(20),
		Schema:        &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": kDummyFieldResolver,
			},
		},
	}
	_, err := CompileGraphQL(&config)
	assert.ErrorContains(t, err, "GraphQL schema too large (> 20 bytes)")
}

func TestGraphQLMaxResolverCount(t *testing.T) {
	var schema = `
	type Task {
		id: ID!
		title: String!
		description: String
		done: Boolean
		tags: [String!]
		secretNotes: String		# Admin-only
	}`
	var config = GraphQLConfig{
		MaxResolverCount: base.IntPtr(1),
		Schema:           &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": kDummyFieldResolver,
			},
			"Mutation": {
				"complete": kDummyFieldResolver,
			},
		},
	}
	_, err := CompileGraphQL(&config)
	assert.ErrorContains(t, err, "too many GraphQL resolvers (> 1)")
}

//////// APOLLO FEDERATION (SUBGRAPHS)

func TestGraphQLSubgraph(t *testing.T) {
	// Subgraph schema definition:
	var kSchemaStr = `
	scalar FieldSet
	directive @key(fields: FieldSet!, resolvable: Boolean = true)  on OBJECT | INTERFACE

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
		gq, err = CompileGraphQL(&config)
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
		assert.True(t, strings.HasPrefix(sdl, "union _Entity = Person | Task\n"), "Unexpected sdl: %s", sdl)
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

  #extend
   type Query {
	product(id: ID!): Product
	deprecatedProduct(sku: String!, package: String!): DeprecatedProduct @deprecated(reason: "Use product query instead")
  }

  #extend
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
		_, err = CompileGraphQL(&config)
		assert.NoError(t, err)
	})
	if err != nil {
		return
	}
}
