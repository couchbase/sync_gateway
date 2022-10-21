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
				Type: "javascript",
				Code: `async function(parent, args, context, info) {
							return await context.user.function("infinite");}`,
				Allow: allowAll,
			},
			"task": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (parent !== undefined) throw "Unexpectedly-defined parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (!info.selectedFieldNames) throw "No info.selectedFieldNames";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("getTask", {id: args.id});}`,
				Allow: allowAll,
			},
			"tasks": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (parent !== undefined) throw "Unexpectedly-defined parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (!info.selectedFieldNames) throw "No info.selectedFieldNames";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("all");}`,
				Allow: allowAll,
			},
			"toDo": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (parent !== undefined) throw "Unexpectedly-defined parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (!info.selectedFieldNames) throw "No info.selectedFieldNames";
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
							if (parent !== undefined) throw "Unexpectedly-defined parent";
							if (Object.keys(args).length != 1) throw "Unexpected args";
							if (!info.selectedFieldNames) throw "No info.selectedFieldNames";
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
			Code: `async function(context, args) {
				var result = await context.user.graphql("query{ infinite }");
				if (result.errors) throw "GraphQL query failed:" + result.errors[0].message;
				return -1;}`,
			Allow: &Allow{Channels: []string{"*"}},
		},
	},
}

func assertGraphQLResult(t *testing.T, expected string, result *db.GraphQLResult, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if !assert.Zerof(t, len(result.Errors), "Unexpected GraphQL errors: %v", result.Errors) {
		for _, err := range result.Errors {
			t.Logf("\t%v", err)
		}
		return
	}
	j, err := json.Marshal(result.Data)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(j))
}

// Per the spec, GraphQL errors are not indicated via `err`, rather through an `errors`
// property in the `result` object.
func assertGraphQLError(t *testing.T, expectedErrorText string, result *db.GraphQLResult, err error) {
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
	assert.FailNowf(t, "GraphQL error did not contain expected string", "Expected to find %q: actually %#v", expectedErrorText, result.Errors)
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
				"square": {
					Type: "javascript",
					Code: `function(parent, args, context, info) {return args.n * args.n;}`,
				},
			},
		},
	}
	_, _, err := CompileFunctions(nil, &config)
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
				"square": {
					Type: "javascript",
					Code: `function(parent, args, context, info) {return args.n * args.n;}`,
				},
			},
			"Mutation": {
				"complete": {
					Type: "javascript",
					Code: `function(parent, args, context, info) { }`,
				},
			},
		},
	}
	_, _, err := CompileFunctions(nil, &config)
	assert.ErrorContains(t, err, "too many GraphQL resolvers (> 1)")
}
