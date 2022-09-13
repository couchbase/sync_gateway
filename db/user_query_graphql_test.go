/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
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
				Type: "javascript",
				Code: `function(context,args) {return args.n * args.n;}`,
			},
			"infinite": {
				Type: "javascript",
				Code: `function(context,args) {return context.user.function("infinite");}`,
			},
			"task": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (Object.keys(info) != "resultFields") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("getTask", {id: args.id});}`,
			},
			"tasks": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (Object.keys(info) != "resultFields") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("all");}`,
			},
			"toDo": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (Object.keys(info) != "resultFields") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						var result=new Array(); var all = context.user.function("all");
						for (var i = 0; i < all.length; i++)
							if (!all[i].done) result.push(all[i]);
						return result;}`,
			},
		},
		"Mutation": {
			"complete": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
							if (Object.keys(parent).length != 0) throw "Unexpected parent";
							if (Object.keys(args).length != 1) throw "Unexpected args";
							if (Object.keys(info) != "resultFields") throw "Unexpected info";
							if (!context.user) throw "Missing context.user";
							if (!context.admin) throw "Missing context.admin";
							var task = context.user.function("getTask", {id: args.id});
							if (!task) return undefined;
							task.done = true;
							return task;}`,
			},
			"addTag": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
							var task = context.user.function("getTask", {id: args.id});
							if (!task) return undefined;
							if (!task.tags) task.tags = [];
							task.tags.push(args.tag);
							return task;}`,
			},
		},
		"Task": {
			"secretNotes": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
								if (!parent.id) throw "Invalid parent";
								if (Object.keys(args).length != 0) throw "Unexpected args";
								if (Object.keys(info) != "resultFields") throw "Unexpected info";
								if (!context.user) throw "Missing context.user";
								if (!context.admin) throw "Missing context.admin";
								return "TOP SECRET!";}`,
				Allow: &UserQueryAllow{Users: base.Set{}}, // only admins
			},
		},
	},
}

// JS function helpers:
var kTestGraphQLUserFunctionsConfig = UserFunctionConfigMap{
	"all": {
		Type: "javascript",
		Code: `function(context, args) {
						return [
						{id: "a", "title": "Applesauce", done:true, tags:["fruit","soft"]},
						{id: "b", "title": "Beer", description: "Bass ale please"},
						{id: "m", "title": "Mangoes"} ];}`,
		Allow: &UserQueryAllow{Channels: []string{"*"}},
	},
	"getTask": {
		Type: "javascript",
		Code: `function(context, args, parent, info) {
						var all = context.user.function("all");
						for (var i = 0; i < all.length; i++)
							if (all[i].id == args.id) return all[i];
						return undefined;}`,
		Args:  []string{"id"},
		Allow: &UserQueryAllow{Channels: []string{"*"}},
	},
	"infinite": {
		Type: "javascript",
		Code: `function(context, args, parent, info) {
				var result = context.user.graphql("query{ infinite }");
				if (result.errors) throw "GraphQL query failed:" + result.errors[0].message;
				return -1;}`,
		Allow: &UserQueryAllow{Channels: []string{"*"}},
	},
}

func assertGraphQLResult(t *testing.T, expected string, result *graphql.Result, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if !assert.Zerof(t, len(result.Errors), "Unexpected GraphQL errors: %v", result.Errors) {
		for _, err := range result.Errors {
			t.Logf("\t%v", err)
			t.Logf("\t\t%T %#v", err.OriginalError(), err.OriginalError())
		}
		return
	}
	j, err := json.Marshal(result.Data)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(j))
}

// Per the spec, GraphQL errors are not indicated via `err`, rather through an `errors`
// property in the `result` object.
func assertGraphQLError(t *testing.T, expectedErrorText string, result *graphql.Result, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if !assert.NotZero(t, len(result.Errors)) {
		data, err := json.Marshal(result.Data)
		assert.NoError(t, err)
		t.Logf("Expected GraphQL error but got none; data is %s", string(data))
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
	cacheOptions := DefaultCacheOptions()
	db := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: kTestGraphQLUserFunctionsConfig,
	})
	db.Ctx = context.TODO()
	defer db.Close()

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserGraphQLAsAdmin(t, db) })

	// Now create a user and make it current:
	db.user = addUserAlice(t, db)
	assert.True(t, db.user.RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserGraphQLAsUser(t, db) })
}

func testUserGraphQLCommon(t *testing.T, db *Database) {
	// Successful query:
	result, err := db.UserGraphQLQuery(`query{ square(n: 12) }`, "", nil, false, db.Ctx)
	assertGraphQLResult(t, `{"square":144}`, result, err)

	result, err = db.UserGraphQLQuery(`query($num:Int!){ square(n: $num) }`, "",
		map[string]interface{}{"num": 12}, false, db.Ctx)
	assertGraphQLResult(t, `{"square":144}`, result, err)

	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {id,title,done} }`, "", nil, false, db.Ctx)
	assertGraphQLResult(t, `{"task":{"done":true,"id":"a","title":"Applesauce"}}`, result, err)

	result, err = db.UserGraphQLQuery(`query{ tasks {title} }`, "", nil, false, db.Ctx)
	assertGraphQLResult(t, `{"tasks":[{"title":"Applesauce"},{"title":"Beer"},{"title":"Mangoes"}]}`, result, err)

	// ERRORS:

	// Nonexistent query:
	result, err = db.UserGraphQLQuery(`query{ bogus(id:"a") {id} }`, "", nil, false, db.Ctx)
	assertGraphQLError(t, "Cannot query field \"bogus\" on type \"Query\"", result, err)

	// Invalid argument:
	result, err = db.UserGraphQLQuery(`query{ task(foo:69) {id,title,done} }`, "", nil, false, db.Ctx)
	assertGraphQLError(t, "Unknown argument \"foo\"", result, err)

	// Mutation when no mutations allowed:
	result, err = db.UserGraphQLQuery(`mutation{ complete(id:"a") {done} }`, "", nil, false, db.Ctx)
	assertGraphQLError(t, "403", result, err)

	// Infinite regress:
	result, err = db.UserGraphQLQuery(`query{ infinite }`, "", nil, false, db.Ctx)
	assertGraphQLError(t, "508", result, err)
}

func testUserGraphQLAsAdmin(t *testing.T, db *Database) {
	testUserGraphQLCommon(t, db)

	// Admin tests updating "a":
	result, err := db.UserGraphQLQuery(`mutation{ addTag(id:"a", tag:"cold") {id,title,done,tags} }`, "", nil, true, db.Ctx)
	assertGraphQLResult(t, `{"addTag":{"done":true,"id":"a","tags":["fruit","soft","cold"],"title":"Applesauce"}}`, result, err)

	// Admin-only field:
	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false, db.Ctx)
	assertGraphQLResult(t, `{"task":{"secretNotes":"TOP SECRET!"}}`, result, err)
}

func testUserGraphQLAsUser(t *testing.T, db *Database) {
	testUserGraphQLCommon(t, db)

	// Regular user tests updating "m":
	result, err := db.UserGraphQLQuery(`mutation{ addTag(id:"m", tag:"ripe") {id,title,done,tags} }`, "", nil, true, db.Ctx)
	assertGraphQLResult(t, `{"addTag":{"done":null,"id":"m","tags":["ripe"],"title":"Mangoes"}}`, result, err)

	// ERRORS:

	// Can't get admin-only field:
	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false, db.Ctx)
	assertGraphQLError(t, "403", result, err)
}

//////// GRAPHQL N1QL RESOLVER TESTS

// The GraphQL configuration, using N1QL in some resolvers:
var kTestGraphQLConfigWithN1QL = GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]GraphQLResolverConfig{
		"Query": {
			"square": {
				Type: "javascript",
				Code: `function(context,args) {return args.n * args.n;}`,
			},
			"infinite": {
				Type: "javascript",
				Code: `function(context,args) {
						var result = context.user.graphql("query{ infinite }");
						if (result.errors) throw "GraphQL query failed: " + result.errors[0].message;
						return -1;}`,
			},
			"task": {
				// This tests the ability of the resolver to return the 1st result row when the
				// GraphQL return type is not a List.
				Type: "query",
				Code: `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE meta().id = $id AND type = "task"`,
			},
			"tasks": {
				Type: "query",
				Code: `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE type = "task" ORDER BY title`,
			},
			"toDo": {
				Type: "query",
				Code: `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE type = "task" AND NOT done ORDER BY title`,
			},
		},
		"Mutation": {
			"complete": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
					var task = context.user.defaultCollection.get(args.id);
					if (!task) return null;
					task.id = args.id;
					if (!task.done) {
					  task.done = true;
					  context.user.defaultCollection.save(args.id, task);
					}
					return task;}`,
			},
			"addTag": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
							var task = context.user.defaultCollection.get(args.id);
							if (!task) return null;
							task.id = args.id;
							if (!task.tags) task.tags = [];
							task.tags.push(args.tag);
							context.user.defaultCollection.save(args.id, task);
							return task;}`,
			},
		},
		"Task": {
			"secretNotes": {
				Type: "javascript",
				Code: `function(context, args, parent, info) {
								if (!parent.id) throw "Invalid parent";
								if (Object.keys(args).length != 0) throw "Unexpected args";
								if (Object.keys(info) != "resultFields") throw "Unexpected info";
								if (!context.user) throw "Missing context.user";
								if (!context.admin) throw "Missing context.admin";
								return "TOP SECRET!";}`,
				Allow: &UserQueryAllow{Users: base.Set{}}, // only admins
			},
			"description": {
				// This tests the magic $parent parameter,
				// and returning the single column of the single row when the result is a scalar.
				Type: "query",
				Code: `SELECT $parent.description`,
			},
		},
	},
}

// Unit test for GraphQL queries.
func TestUserGraphQLWithN1QL(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only (requires N1QL)")
	}

	cacheOptions := DefaultCacheOptions()
	db := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions: &cacheOptions,
		GraphQL:      &kTestGraphQLConfigWithN1QL,
	})
	db.Ctx = context.TODO()
	defer db.Close()

	db.Put("a", Body{"type": "task", "title": "Applesauce", "done": true, "tags": []string{"fruit", "soft"}, "channels": "wonderland"})
	db.Put("b", Body{"type": "task", "title": "Beer", "description": "Bass ale please", "channels": "wonderland"})
	db.Put("m", Body{"type": "task", "title": "Mangoes", "channels": "wonderland"})

	// Without this, N1QL can't do any queries on documents:
	rows, _ := db.N1QLQueryWithStats(db.Ctx, "", "CREATE PRIMARY INDEX ON $_keyspace", nil,
		base.RequestPlus, false)
	rows.Close()

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserGraphQLAsAdmin(t, db) })

	// Now create a user and make it current:
	db.user = addUserAlice(t, db)
	assert.True(t, db.user.RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserGraphQLAsUser(t, db) })

	// Test the N1QL resolver that uses $parent:
	result, err := db.UserGraphQLQuery(`query{ task(id:"b") {description,title} }`, "", nil, false, db.Ctx)
	assertGraphQLResult(t, `{"task":{"description":"Bass ale please","title":"Beer"}}`, result, err)
}
