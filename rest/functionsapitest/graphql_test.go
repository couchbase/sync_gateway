//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package functionsapitest

import (
	"encoding/json"
	"github.com/graphql-go/graphql"
	"net/http"
	"os"
	"strings"
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
            secretNotes: String     # Admin-only
        }
        type Query {
            square(n: Int!): Int!
            infinite: Int!
            task(id: ID!): Task
            tasks: [Task!]!
			tasksClone: [Task!]!
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
			"tasksClone": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("allClone");}`,
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
                            {id: "b", "title": "Beer", done:false,description: "Bass ale please"},
                            {id: "m", "title": "Mangoes",done:false} ];}`,
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
		"allClone": {
			Type: "javascript",
			Code: `function(context, args) {
						return [
						{id: "a", "title": "Applesauce", done:true, tags:["fruit","soft"]},
						{id: "b", "title": "Beer", description: "Bass ale please"},
						{id: "m", "title": "Mangoes"} ];}`,
			Allow: &functions.Allow{Channels: []string{"wonderland"}},
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
			Schema:    base.StringPtr(kTestGraphQLSchema),
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
		assert.Equal(t, base.StringPtr(kTestGraphQLSchema), body.Schema)
	})
}

func TestFunctionsConfigGraphQLPut(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(kTestGraphQLSchema),
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

// Test for GraphQL Query: Admin-only
func TestGraphQLQueryAdminOnly(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsAdmin - square", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ square(n:2) }"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"square":4}}`, string(response.BodyBytes()))
	})

	t.Run("AsAdmin - Infinite", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ infinite }"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "GraphQL query failed")
	})

	t.Run("AsAdmin - task", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query($id:ID!){ task(id:$id) { id , title } }" , "variables": {"id": "a"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"task":{"id":"a","title":"Applesauce"}}}`, string(response.BodyBytes()))
	})

	t.Run("AsAdmin - tasks", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{tasks{title}}"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasks":[{"title":"Applesauce"},{"title":"Beer"},{"title":"Mangoes"}]}}`, string(response.BodyBytes()))

	})
	t.Run("AsAdmin - toDo", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{toDo{title}}"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"toDo":[{"title":"Beer"},{"title":"Mangoes"}]}}`, string(response.BodyBytes()))
	})

	t.Run("AsAdmin - secretNotes", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query($id:ID!){ task(id:$id) { secretNotes } }" , "variables": {"id": "a"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"task":{"secretNotes":"TOP SECRET!"}}}`, string(response.BodyBytes()))
	})
}

func testErrorMessage(t *testing.T, response *rest.TestResponse, expectedErrorText string) {
	graphQLResponse := graphql.Result{}
	err := json.Unmarshal(response.BodyBytes(), &graphQLResponse)
	assert.NoError(t, err)
	assert.NotZero(t, len(graphQLResponse.Errors), "Expected GraphQL error but got none; data is %s", graphQLResponse.Data)
	for _, err = range graphQLResponse.Errors {
		if strings.Contains(err.Error(), expectedErrorText) {
			return
		}
	}
	t.Logf("GraphQL error did not contain expected string %q: actually %#v", expectedErrorText, graphQLResponse.Errors)
	t.Fail()
}

// Test for GraphQL Query: Custom User
func TestGraphQLQueryCustomUser(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsUser - square", func(t *testing.T) {
		//user Created
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		//request sent by the custom user
		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query{ square(n:2) }"}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"square":4}}`, string(response.BodyBytes()))

		//custom user deleted by the Admin
		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("AsUser - infinite", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query{ infinite }"}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("AsUser - task", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query($id:ID!){ task(id:$id) { id , title } }" , "variables": {"id": "a"}}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"task":{"id":"a","title":"Applesauce"}}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("AsUser - tasks", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query{tasks{title}}"}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasks":[{"title":"Applesauce"},{"title":"Beer"},{"title":"Mangoes"}]}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("AsUser - toDo", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query{toDo{title}}"}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"toDo":[{"title":"Beer"},{"title":"Mangoes"}]}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	//secretNotes(Admin-only field)-> custom user cannot access it
	t.Run("AsUser - secretNotes", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query($id:ID!){ task(id:$id) { secretNotes } }" , "variables": {"id": "a"}}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "403 you are not allowed to call GraphQL resolver")

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	//Check If User is not able to call Resolver within which Function  is not accessible to user
	t.Run("User should receive error if the Function in Resolver does not belong to user channel", func(t *testing.T) {
		graphQLRequestBodyWithInvalidResolver := `{"query": "query{ taskClone { id } }"}`
		response := rt.SendAdminRequest("POST", "/db/_graphql", graphQLRequestBodyWithInvalidResolver)
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		testErrorMessage(t, response, "Cannot query field \"taskClone\"")

		//Create a User With Specific Channel
		userResponse := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"dummy","email":"dummy@couchbase.com", "password":"letmein", "admin_channels":["!"]}`)
		assert.Equal(t, http.StatusCreated, userResponse.Result().StatusCode)

		graphQLRequestBody := `{"query": "query{ tasksClone { id } }"}`
		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", graphQLRequestBody, nil, "dummy", "letmein")
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		testErrorMessage(t, response, "403")

		//Update The Dummy User
		userResponse = rt.SendAdminRequest("PUT", "/db/_user/dummy", `{"name":"dummy","email":"dummy@couchbase.com", "password":"letmein", "admin_channels":["!","wonderland"]}`)
		assert.Equal(t, http.StatusOK, userResponse.Result().StatusCode)

		// Now the function allCLone will be accessible
		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", graphQLRequestBody, nil, "dummy", "letmein")
		assert.Equal(t, http.StatusOK, userResponse.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasksClone":[{"id":"a"},{"id":"b"},{"id":"m"}]}}`, response.Body.String())
	})
}

// Test for GraphQL Valid Configuration Schema
func TestValidGraphQLConfigurationValues(t *testing.T) {
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

	//If max_schema_size >= given schema size then Valid
	//here max_schema_size allowed is 34 bytes and given schema size is also 34 bytes
	//hence it is a valid config
	t.Run("Check max_schema_size allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			},
			"max_schema_size" : 34
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	//If max_resolver_count >= given number of resolvers then Valid
	//here max_resolver_count allowed is 2 and total resolvers are also 2, hence it is a valid config
	t.Run("Check max_resolver_count allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int! \n square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					},
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_resolver_count" : 2
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	//If max_request_size >= length of JSON-encoded arguments passed to a function then Valid
	//here max_request_size allowed is 114 and size of arguments is also 114, hence it is a valid config
	t.Run("Check max_request_size allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_request_size" : 114
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query($numberToBeSquared:Int!){ square(n:$numberToBeSquared) }", "variables": {"numberToBeSquared": 4}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"square":16}}`, string(response.BodyBytes()))
	})

	//only one out of the schema or schemaFile is allowed
	//here only SchemaFile is provided, hence it is a valid config
	t.Run("Provide only schema or schema file", func(t *testing.T) {
		validSchema := "type Query {sum(n: Int!) : Int!}"
		err := os.WriteFile("schema.graphql", []byte(validSchema), 0666)
		assert.NoError(t, err)
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schemaFile": "schema.graphql",
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
		err = os.Remove("schema.graphql")
		assert.NoError(t, err)
	})
}

// This function checks for failure when invalid GraphQL configuration
// values are provided.
func TestInvalidGraphQLConfigurationValues(t *testing.T) {
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

	// The max_schema_size allowed here is 20 bytes and the given schema is 32 bytes.
	// Hence, this will be a bad request
	t.Run("Check max_schema_size allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			},
			"max_schema_size" : 20
		}`)

		var responseMap map[string]interface{}
		json.Unmarshal([]byte(string(response.BodyBytes())), &responseMap)

		assert.Equal(t, 400, response.Result().StatusCode)
		assert.Contains(t, responseMap["reason"], "GraphQL schema too large")
		assert.Contains(t, responseMap["error"], "Bad Request")
	})

	// The max_resolver_count allowed here is 1 and we have provided
	// 2 resolvers (sum and square). Hence, this will be a bad request
	t.Run("Check max_resolver_count allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int! \n square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					},
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_resolver_count" : 1
		}`)

		var responseMap map[string]interface{}
		json.Unmarshal([]byte(string(response.BodyBytes())), &responseMap)

		assert.Equal(t, 400, response.Result().StatusCode)
		assert.Contains(t, responseMap["reason"], "too many GraphQL resolvers")
		assert.Contains(t, responseMap["error"], "Bad Request")

	})

	// The maximum length in bytes of the JSON-encoded arguments passed to
	// a function at runtime (max_request_size) allowed here is 5 but we
	// have supplied larger arguments in the POST request.
	t.Run("Check max_request_size allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_request_size" : 5
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query($numberToBeSquared:Int!){ square(n:$numberToBeSquared) }", "variables": {"numberToBeSquared": 4}}`)

		var responseMap map[string]interface{}
		json.Unmarshal([]byte(string(response.BodyBytes())), &responseMap)

		assert.Equal(t, 413, response.Result().StatusCode)
		assert.Contains(t, responseMap["reason"], "Arguments too large")
		assert.Contains(t, responseMap["error"], "Request Entity Too Large")
	})

	// Only one out of schema and schemaFile is allowed to be present
	// but we have provided both of them. Hence, this will be a bad request.
	t.Run("Provide both schema and schema file", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"schemaFile": "someInvalidPath/someInvalidFileName",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			}
		}`)

		var responseMap map[string]interface{}
		json.Unmarshal([]byte(string(response.BodyBytes())), &responseMap)

		assert.Equal(t, 400, response.Result().StatusCode)
		assert.Contains(t, responseMap["reason"], "GraphQL config: only one of `schema` and `schemaFile` may be used")
		assert.Contains(t, responseMap["error"], "Bad Request")
	})

	// Flow: Create a file with a bogus schema --> Send the request --> Capture the error --> Delete the created file
	t.Run("Provide invalid schema file", func(t *testing.T) {
		bogusSchema := "obviously not a valid schema ^_^"
		err := os.WriteFile("schema.graphql", []byte(bogusSchema), 0666)
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schemaFile": "schema.graphql",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			}
		}`)
		var responseMap map[string]interface{}
		json.Unmarshal([]byte(string(response.BodyBytes())), &responseMap)

		assert.Equal(t, 400, response.Result().StatusCode)
		assert.Contains(t, responseMap["reason"], "Syntax Error GraphQL")
		assert.Contains(t, responseMap["reason"], "Unexpected Name")
		assert.Contains(t, responseMap["error"], "Bad Request")

		err = os.Remove("schema.graphql")
		assert.NoError(t, err)
	})
}
