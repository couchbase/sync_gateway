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
	"strings"
	"testing"

	"github.com/graphql-go/graphql"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

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
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query{ square(n:2) }"}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"square":4}}`, string(response.BodyBytes()))

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

	//Check If User is not able to call Resolver within which Function is not accessible to user
	t.Run("AsUser - valid channel check", func(t *testing.T) {
		graphQLRequestBodyWithInvalidResolver := `{"query": "query{ taskClone { id } }"}`
		response := rt.SendAdminRequest("POST", "/db/_graphql", graphQLRequestBodyWithInvalidResolver)
		assert.Equal(t, 200, response.Result().StatusCode)
		testErrorMessage(t, response, "Cannot query field \"taskClone\"")

		//Create a User With Specific Channel
		userResponse := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"dummy","email":"dummy@couchbase.com", "password":"letmein", "admin_channels":["!"]}`)
		assert.Equal(t, 201, userResponse.Result().StatusCode)

		graphQLRequestBody := `{"query": "query{ tasksClone { id } }"}`
		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", graphQLRequestBody, nil, "dummy", "letmein")
		assert.Equal(t, 200, response.Result().StatusCode)
		testErrorMessage(t, response, "403")

		//Update The Dummy User
		userResponse = rt.SendAdminRequest("PUT", "/db/_user/dummy", `{"name":"dummy","email":"dummy@couchbase.com", "password":"letmein", "admin_channels":["!","wonderland"]}`)
		assert.Equal(t, 200, userResponse.Result().StatusCode)

		// Now the function allClone will be accessible
		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", graphQLRequestBody, nil, "dummy", "letmein")
		assert.Equal(t, 200, userResponse.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasksClone":[{"id":"a"},{"id":"b"},{"id":"m"}]}}`, response.Body.String())
	})
}

// Test for GraphQL mutations via Guest user
func TestGraphQLMutationsGuest(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true, EnableUserQueries: true})
	if rt == nil {
		return
	}
	defer rt.Close()
	rt.DatabaseConfig = &rest.DatabaseConfig{
		DbConfig: rest.DbConfig{
			GraphQL:       &kTestGraphQLConfig,
			UserFunctions: &kTestGraphQLUserFunctionsConfig,
		},
	}

	t.Run("AsGuest - Add Tag", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "mutation($id:ID!, $tag: String!){ addTag(id:$id, tag:$tag) {id,title,done,tags} }" , "variables": {"id": "a", "tag":"cool"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"addTag":{"done":true,"id":"a","tags":["fruit","soft","cool"],"title":"Applesauce"}}}`, string(response.BodyBytes()))
	})

	t.Run("AsGuest - Complete", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query":"mutation($id: ID!){ complete(id:$id) {done} }", "variables" : {"id":"b"}}`)

		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"complete":{"done":true}}}`, string(response.BodyBytes()))
	})
}

// Test for GraphQL queries via Guest user
func TestGraphQLQueriesGuest(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true, EnableUserQueries: true})
	if rt == nil {
		return
	}
	defer rt.Close()
	rt.DatabaseConfig = &rest.DatabaseConfig{
		DbConfig: rest.DbConfig{
			GraphQL:       &kTestGraphQLConfig,
			UserFunctions: &kTestGraphQLUserFunctionsConfig,
		},
	}

	t.Run("AsGuest - square", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query{ square(n:2) }"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"square":4}}`, string(response.BodyBytes()))
	})

	t.Run("AsGuest - Infinite", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query{ infinite }"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "GraphQL query failed")
	})

	t.Run("AsGuest - task", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query($id:ID!){ task(id:$id) { id , title } }" , "variables": {"id": "a"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"task":{"id":"a","title":"Applesauce"}}}`, string(response.BodyBytes()))
	})
	t.Run("AsGuest - tasks", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query{tasks{title}}"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"tasks":[{"title":"Applesauce"},{"title":"Beer"},{"title":"Mangoes"}]}}`, string(response.BodyBytes()))

	})
	t.Run("AsGuest - toDo", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query{toDo{title}}"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"toDo":[{"title":"Beer"},{"title":"Mangoes"}]}}`, string(response.BodyBytes()))
	})

	t.Run("AsGuest - secretNotes", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query($id:ID!){ task(id:$id) { secretNotes } }" , "variables": {"id": "a"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "401")
	})
}
