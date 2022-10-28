//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package functionsapitest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const kDummyGraphQLSchema = `
	type Query {
		square(n: Int!) : Int!
	}`

var allowAll = &functions.Allow{Channels: []string{"*"}}

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
var kTestGraphQLConfig = functions.GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]functions.GraphQLResolverConfig{
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
				Allow: &functions.Allow{Users: base.Set{}}, // only admins
			},
		},
	},
}

// JS function helpers:
var kTestGraphQLUserFunctionsConfig = functions.FunctionsConfig{
	Definitions: functions.FunctionsDefs{
		"all": {
			Type: "javascript",
			Code: `function(context, args) {
							return [
							{id: "a", "title": "Applesauce", done:true, tags:["fruit","soft"]},
							{id: "b", "title": "Beer", description: "Bass ale please"},
							{id: "m", "title": "Mangoes"} ];}`,
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
		"getTask": {
			Type: "javascript",
			Code: `function(context, args) {
							var all = context.user.function("all");
							for (var i = 0; i < all.length; i++)
								if (all[i].id == args.id) return all[i];
							return undefined;}`,
			Args:  []string{"id"},
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
		"infinite": {
			Type: "javascript",
			Code: `function(context, args) {
					var result = context.user.graphql("query{ infinite }");
					if (result.errors) throw "GraphQL query failed:" + result.errors[0].message;
					return -1;}`,
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
	},
}

func TestFunctionsConfigGraphQLGetEmpty(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestFunctionsConfigGraphQLGet(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(kDummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		var body functions.GraphQLConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, base.StringPtr(kDummyGraphQLSchema), body.Schema)
	})
}

func TestFunctionsConfigGraphQLPut(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(kDummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/graphql", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/graphql", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("ReplaceBogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "obviously not a valid schema ^_^"
		}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("Replace", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ sum(n:3) }"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"sum":6}}`, string(response.BodyBytes()))
	})
	t.Run("Delete", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/graphql", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.GraphQL)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ sum(n:3) }"}`)
		assert.Equal(t, 503, response.Result().StatusCode)
	})
}

// Test for GraphQL mutations Admin-only
func TestGraphQLMutationsAdminOnly(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsAdmin - Add Tag", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "mutation($id:ID!, $tag: String!){ addTag(id:$id, tag:$tag) {id,title,done,tags} }" , "variables": {"id": "a", "tag":"cool"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"addTag":{"done":true,"id":"a","tags":["fruit","soft","cool"],"title":"Applesauce"}}}`, string(response.BodyBytes()))
	})

	t.Run("AsAdmin - Complete", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query":"mutation($id: ID!){ complete(id:$id) {done} }", "variables" : {"id":"b"}}`)

		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"complete":{"done":true}}}`, string(response.BodyBytes()))
	})
}

// Test for GraphQL mutations Custom User

func TestGraphQLMutationsCustomUser(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	// Flow: Create a user --> Send request --> Verify response --> Delete the created user
	t.Run("AsUser - Add Tag", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"jinesh", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "mutation($id:ID!, $tag: String!){ addTag(id:$id, tag:$tag) {id,title,done,tags} }" , "variables": {"id": "a", "tag":"cool"}}`, nil, "jinesh", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"addTag":{"done":true,"id":"a","tags":["fruit","soft","cool"],"title":"Applesauce"}}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/jinesh", "")
		assert.Equal(t, 200, response.Result().StatusCode)

	})

	t.Run("AsUserUser - Complete", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"jinesh", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query":"mutation($id: ID!){ complete(id:$id) {done} }", "variables" : {"id":"b"}}`, nil, "jinesh", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"complete":{"done":true}}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/jinesh", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

}
