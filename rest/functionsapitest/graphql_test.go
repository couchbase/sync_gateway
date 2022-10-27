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
	"fmt"
	"github.com/graphql-go/graphql"
	"net/http"
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
						if (Object.keys(args).length != 1) throw "Unexpected args";
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
		"allClone": {
			Type: "javascript",
			Code: `function(context, args) {
						return [
						{id: "a", "title": "Applesauce", done:true, tags:["fruit","soft"]},
						{id: "b", "title": "Beer", description: "Bass ale please"},
						{id: "m", "title": "Mangoes"} ];}`,
			Allow: &functions.Allow{Channels: []string{"wonderland"}},
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

func TestFunctionsConfigGraphQLAuth(t *testing.T) {
	// Put the schemas and resolvers
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
		GraphQL:       &kTestGraphQLConfig,
	})
	defer rt.Close()

	//Create a User
	response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ritik","email":"ritik.raj@couchbase.com", "password":"letmein", "admin_channels":["*"]}`)
	assert.Equal(t, http.StatusCreated, response.Result().StatusCode)

	username := "ritik"
	password := "letmein"

	sendUserRequestWithHeaderWrapper := func(method string, resource string, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}

	//Check if User is able to access those functions
	t.Run("User can run Functions", func(t *testing.T) {
		graphQLRequestBody := `{"query": "query($number:Int!){ square(n:$number) }",
				  "variables": {"number": 13}}`

		response := sendUserRequestWithHeaderWrapper("POST", "/db/_graphql", graphQLRequestBody)
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"square":169}}`, response.Body.String())

	})

	//Check if User can call Functions called within resolvers
	t.Run("User can call Functions present inside resolvers", func(t *testing.T) {
		graphQLRequestBody := `{"query": "query{ tasks { id } }"}`
		response := sendUserRequestWithHeaderWrapper("POST", "/db/_graphql", graphQLRequestBody)
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasks":[{"id":"a"},{"id":"b"},{"id":"m"}]}}`, response.Body.String())

		graphQLRequestBody = `{"query": "query($id:ID!){ task(id:$id) { id , title } }" , "variables": {"id": "a"}}`
		response = sendUserRequestWithHeaderWrapper("POST", "/db/_graphql", graphQLRequestBody)
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"task":{"id":"a","title":"Applesauce"}}}`, response.Body.String())
	})

	//Check If User is not able to call Resolver within which Function  is adminOnly
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
		testErrorMessage(t, response, "not allowed to call function")

		//Update The Dummy User
		userResponse = rt.SendAdminRequest("PUT", "/db/_user/dummy", `{"name":"dummy","email":"dummy@couchbase.com", "password":"letmein", "admin_channels":["!","wonderland"]}`)
		assert.Equal(t, http.StatusOK, userResponse.Result().StatusCode)

		// Now the function allCLone will be accessible
		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", graphQLRequestBody, nil, "dummy", "letmein")
		assert.Equal(t, http.StatusOK, userResponse.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasksClone":[{"id":"a"},{"id":"b"},{"id":"m"}]}}`, response.Body.String())
	})

	//Only Admin
	t.Run("Accessing A Admin Only Field", func(t *testing.T) {
		graphqlRequestBody := `{"query": "query($id:ID!){ task(id:$id){ secretNotes } }" , "variables": {"id": "a"}}`
		response := sendUserRequestWithHeaderWrapper("POST", "/db/_graphql", graphqlRequestBody)
		testErrorMessage(t, response, "403")
		testErrorMessage(t, response, "not allowed to call GraphQL resolver")

		// Admin should be able to access that Field
		response = rt.SendAdminRequest("POST", "/db/_graphql", graphqlRequestBody)
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"task":{"secretNotes":"TOP SECRET!"}}}`, response.Body.String())
	})

	t.Run("Mutability should not be allowed for GET method", func(t *testing.T) {
		tagToBeAdded := "trialTag"

		queryParam := `mutation($id:ID!,$tag:String!){ addTag(id:$id,tag:$tag) { id , title , description , done , tags } }`
		variableParam := fmt.Sprintf(`{"id": "a" , "tag": "%s"}`, tagToBeAdded)
		getRequestUrl := fmt.Sprintf("/db/_graphql?query=%s&variables=%s", queryParam, variableParam)
		response := sendUserRequestWithHeaderWrapper("GET", getRequestUrl, "")
		testErrorMessage(t, response, "403 \"requireMutating\"")

		// Should be able to update when using POST
		requestBody := fmt.Sprintf(`{"query": "%s" , "variables": %s}`, queryParam, variableParam)
		response = sendUserRequestWithHeaderWrapper("POST", "/db/_graphql", requestBody)

		var responseMap *graphql.Result
		err := json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)
		assert.Zero(t, responseMap.Errors)

		updatedTagList := responseMap.Data.(map[string]interface{})["addTag"].(map[string]interface{})["tags"].([]interface{})
		assert.NotZero(t, updatedTagList)
		assert.Equal(t, true, checkElementIsPresent(updatedTagList, tagToBeAdded))
	})
}

func checkElementIsPresent(list []interface{}, element string) bool {
	for _, item := range list {
		if item == element {
			return true
		}
	}
	return false
}
