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
		secretNotes: String		# Admin-only
	}
	type Query {
		task(id: ID!): Task
		tasks: [Task!]!
		toDo: [Task!]!
	}
	type Mutation {
		complete(id: ID!): Task
	}
`

// The GraphQL configuration:
var kTestGraphQLConfig = GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]GraphQLResolverConfig{
		"Query": {
			"task": `if (Object.keys(parent).length != 0) throw "Unexpected parent";
					 if (Object.keys(args).length != 1) throw "Unexpected args";
					 if (Object.keys(info) != "resultFields") throw "Unexpected info";
					 if (!context.user) throw "Missing context.user";
					 if (!context.app) throw "Missing context.app";
					 var all = context.app.func("all");
					 for (var i = 0; i < all.length; i++)
					  	if (all[i].id == args.id) return all[i];
					 return undefined;`,
			"tasks": `if (Object.keys(parent).length != 0) throw "Unexpected parent";
		  			  if (Object.keys(args).length != 0) throw "Unexpected args";
					  if (Object.keys(info) != "resultFields") throw "Unexpected info";
					  if (!context.user) throw "Missing context.user";
					  if (!context.app) throw "Missing context.app";
					  return context.app.func("all");`,
			"toDo": `if (Object.keys(parent).length != 0) throw "Unexpected parent";
					 if (Object.keys(args).length != 1) throw "Unexpected args";
					 if (Object.keys(info) != "resultFields") throw "Unexpected info";
					 if (!context.user) throw "Missing context.user";
					 if (!context.app) throw "Missing context.app";
					 var result=new Array(); var all = context.app.func("all");
					 for (var i = 0; i < all.length; i++)
						if (!all[i].done) result.push(all[i]);
					 return result;`,
		},
		"Mutation": {
			"complete": `if (Object.keys(parent).length != 0) throw "Unexpected parent";
						 if (Object.keys(args).length != 1) throw "Unexpected args";
						 if (Object.keys(info) != "resultFields") throw "Unexpected info";
						 if (!context.user) throw "Missing context.user";
						 if (!context.app) throw "Missing context.app";
						 context.app.save(args.id, {id:args.id,done:true}); return args.id;`,
		},
		"Task": {
			"secretNotes": `if (!parent.id) throw "Invalid parent";
							if (Object.keys(args).length != 0) throw "Unexpected args";
							if (Object.keys(info) != "resultFields") throw "Unexpected info";
							if (!context.user) throw "Missing context.user";
							if (!context.app) throw "Missing context.app";
							context.user.requireAdmin();
							return "TOP SECRET!";`,
		},
	},
}

// JS function helpers:
var kTestGraphQLUserFunctionsConfig = UserFunctionMap{
	"all": &UserFunctionConfig{
		SourceCode: `return [
			{id: "a", "title": "Applesauce", done:true},
			{id: "b", "title": "Beer", description: "Bass ale please"},
			{id: "m", "title": "Mangoes"} ];`,
		Allow: &UserQueryAllow{Channels: []string{"*"}},
	},
}

func assertGraphQLResult(t *testing.T, expected string, result *graphql.Result, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if errs := result.Errors; len(errs) > 0 {
		t.Logf("Unexpected GraphQL errors: %v", errs)
		for _, err := range errs {
			t.Logf("\t%v", err)
			t.Logf("\t\t%T %#v", err.OriginalError(), err.OriginalError())
		}
		t.Fail()
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
	if len(result.Errors) == 0 {
		data, err := json.Marshal(result.Data)
		assert.NoError(t, err)
		t.Logf("Expected GraphQL error but got none; data is %s", string(data))
		t.Fail()
		return
	}
	for _, err := range result.Errors {
		if strings.Contains(err.Error(), expectedErrorText) {
			return
		}
	}
	t.Logf("GraphQL error was not the one expected: %#v", result.Errors)
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
	result, err := db.UserGraphQLQuery(`query{ task(id:"a") {id,title,done} }`, "", nil, false)
	assertGraphQLResult(t, `{"task":{"done":true,"id":"a","title":"Applesauce"}}`, result, err)

	// ERRORS:

	// Nonexistent query:
	result, err = db.UserGraphQLQuery(`query{ bogus(id:"a") {id} }`, "", nil, false)
	assertGraphQLError(t, "Cannot query field \"bogus\" on type \"Query\"", result, err)

	// Invalid argument:
	result, err = db.UserGraphQLQuery(`query{ task(foo:69) {id,title,done} }`, "", nil, false)
	assertGraphQLError(t, "Unknown argument \"foo\"", result, err)

	// Mutation when no mutations allowed:
	result, err = db.UserGraphQLQuery(`mutation{ complete(id:"a") {done} }`, "", nil, false)
	assertGraphQLError(t, "403", result, err)
}

func testUserGraphQLAsAdmin(t *testing.T, db *Database) {
	testUserGraphQLCommon(t, db)

	// Admin-only field:
	result, err := db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false)
	assertGraphQLResult(t, `{"task":{"secretNotes":"TOP SECRET!"}}`, result, err)
}

func testUserGraphQLAsUser(t *testing.T, db *Database) {
	testUserGraphQLCommon(t, db)

	// ERRORS:

	// Can't get admin-only field:
	result, err := db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false)
	assertGraphQLError(t, "403", result, err)
}
