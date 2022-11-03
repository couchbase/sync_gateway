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
	"strings"
	"testing"

	"github.com/graphql-go/graphql"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

// Test for GraphQL Query Admin-Only
func TestGraphQLQueryAdminOnly(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsAdmin - getUser", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query($id:ID!){ getUser(id:$id) { id , name } }" , "variables": {"id": 1}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getUser":{"id":"1","name":"user1"}}}`, string(response.BodyBytes()))

		queryParam := `query($id:ID!){ getUser(id:$id) { id , name } }`
		variableParam := `{"id": 1}`
		getRequestUrl := fmt.Sprintf("/db/_graphql?query=%s&variables=%s", queryParam, variableParam)
		response = rt.SendAdminRequest("GET", getRequestUrl, "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getUser":{"id":"1","name":"user1"}}}`, string(response.BodyBytes()))
	})

	t.Run("AsAdmin - getAllUsers", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{getAllUsers{name}}"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getAllUsers":[{"name":"user1"},{"name":"user2"},{"name":"user3"}]}}`, string(response.BodyBytes()))

	})
}

// Test for GraphQL Query Custom User
func TestGraphQLQueryCustomUser(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsUser - getUser", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query($id:ID!){ getUser(id:$id) { id , name } }" , "variables": {"id": 3}}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getUser":{"id":"3","name":"user3"}}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("AsUser - getAllUsers", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"janhavi", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "query{getAllUsers{name}}"}`, nil, "janhavi", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getAllUsers":[{"name":"user1"},{"name":"user2"},{"name":"user3"}]}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/janhavi", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

}

// Test for GraphQL Query Guest User
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

	t.Run("AsGuest - getUser", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query($id:ID!){ getUser(id:$id) { id , name } }" , "variables": {"id": 1}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getUser":{"id":"1","name":"user1"}}}`, string(response.BodyBytes()))
	})
	t.Run("AsGuest - getAllUsers", func(t *testing.T) {
		response := rt.SendRequest("POST", "/db/_graphql", `{"query": "query{getAllUsers{name}}"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"getAllUsers":[{"name":"user1"},{"name":"user2"},{"name":"user3"}]}}`, string(response.BodyBytes()))

	})
}

// Test for GraphQL Mutations Admin
func TestGraphQLMutationsAdminOnly(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsAdmin - updateName", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query":"mutation($id: ID!, $name:String!){ updateName(id:$id,name:$name) {id,name} }", "variables" : {"id":1,"name":"newUser"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"updateName":{"id":"1","name":"newUser"}}}`, string(response.BodyBytes()))
	})

	t.Run("AsAdmin - addEmail", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "mutation($id:ID!, $email: String!){ addEmail(id:$id, email:$email) {id,name,Emails} }" , "variables": {"id": 2, "email":"pqr@gmail.com"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"addEmail":{"Emails":["xyz@gmail.com","def@gmail.com","pqr@gmail.com"],"id":"2","name":"user2"}}}`, string(response.BodyBytes()))
	})
}

// Test for GraphQL mutations Custom User: New Schema
func TestGraphQLMutationsCustomUser(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &kTestGraphQLConfig,
		UserFunctions: &kTestGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsUser - updateName", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"jinesh", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query":"mutation($id: ID!, $name:String!){ updateName(id:$id,name:$name) {id,name} }", "variables" : {"id":1,"name":"newUser"}}`, nil, "jinesh", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"updateName":{"id":"1","name":"newUser"}}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("DELETE", "/db/_user/jinesh", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("AsUser - addEmail", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"jinesh", "password":"password"}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("POST", "/db/_graphql", `{"query": "mutation($id:ID!, $email: String!){ addEmail(id:$id, email:$email) {id,name,Emails} }" , "variables": {"id": 2, "email":"pqr@gmail.com"}}`, nil, "jinesh", "password")
		assert.Equal(t, 200, response.Result().StatusCode)
		testErrorMessage(t, response, "403 you are not allowed to call GraphQL resolver")

		response = rt.SendAdminRequest("DELETE", "/db/_user/jinesh", "")
		assert.Equal(t, 200, response.Result().StatusCode)
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

	t.Run("AsGuest - updateName", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query":"mutation($id: ID!, $name:String!){ updateName(id:$id,name:$name) {id,name} }", "variables" : {"id":1,"name":"newUser"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"updateName":{"id":"1","name":"newUser"}}}`, string(response.BodyBytes()))
	})

	t.Run("AsGuest - addEmail", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "mutation($id:ID!, $email: String!){ addEmail(id:$id, email:$email) {id,name,Emails} }" , "variables": {"id": 2, "email":"pqr@gmail.com"}}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"addEmail":{"Emails":["xyz@gmail.com","def@gmail.com","pqr@gmail.com"],"id":"2","name":"user2"}}}`, string(response.BodyBytes()))
	})
}

func TestContextDeadline(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema: base.StringPtr(`type Query{checkContextDeadline(Timeout: Int!): Int}`),
			Resolvers: map[string]functions.GraphQLResolverConfig{
				"Query": {
					"checkContextDeadline": {
						Type: "javascript",
						Code: `function(parent, args, context, info) {
									var start = new Date().getTime();
									while (new Date().getTime() < start + args.Timeout);
									return 0;
								}`,
						Allow: allowAll,
					},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()
	t.Run("AsAdmin - exceedContextDeadline", func(t *testing.T) {
		requestQuery := fmt.Sprintf(`{"query": "query{ checkContextDeadline(Timeout:%d) }"}`, db.UserFunctionTimeout.Milliseconds()*2)
		response := rt.SendAdminRequest("POST", "/db/_graphql", requestQuery)

		assert.Equal(t, 200, response.Result().StatusCode)
		testErrorMessage(t, response, "context deadline exceeded")
	})
	t.Run("AsAdmin - doNotExceedContextDeadline", func(t *testing.T) {
		requestQuery := fmt.Sprintf(`{"query": "query{ checkContextDeadline(Timeout:%d) }"}`, db.UserFunctionTimeout.Milliseconds()/2)
		response := rt.SendAdminRequest("POST", "/db/_graphql", requestQuery)

		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"checkContextDeadline":0}}`, string(response.BodyBytes()))
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
