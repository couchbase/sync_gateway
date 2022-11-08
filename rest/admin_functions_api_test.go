//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//////// ALL USER QUERY APIS:

// When feature flag is not enabled, all API calls return 404:
func TestFunctionsConfigGetWithoutFeatureFlag(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{EnableUserQueries: false})
	defer rt.Close()

	t.Run("Functions, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All Functions", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Single Function", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})

	t.Run("GraphQL, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("GraphQL", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

// Test use of "Etag" and "If-Match" headers to safely update function/query/graphql config.
func TestFunctionsConfigMVCC(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"xxx": {
					Type: "javascript",
					Code: "function(){return 42;}",
				},
				"xxxN1QL": {
					Type: "query",
					Code: "SELECT 42",
				},
				"yyy": {
					Type: "query",
					Code: "SELECT 999",
				},
			},
		},
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(kDummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	runTest := func(t *testing.T, uri string, newValue string) {
		// Get initial etag:
		response := rt.SendAdminRequest("GET", uri, "")
		assert.Equal(t, 200, response.Result().StatusCode)
		etag := response.HeaderMap.Get("Etag")
		assert.Regexp(t, `"[^"]+"`, etag)

		// Update config, just to change its etag:
		response = rt.SendAdminRequest("PUT", uri, newValue)
		assert.Equal(t, 200, response.Result().StatusCode)
		newEtag := response.HeaderMap.Get("Etag")
		assert.Regexp(t, `"[^"]+"`, newEtag)
		assert.NotEqual(t, etag, newEtag)

		// A GET should also return the new etag:
		response = rt.SendAdminRequest("GET", uri, "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, newEtag, response.HeaderMap.Get("Etag"))

		// Try to update using If-Match with the old etag:
		headers := map[string]string{"If-Match": etag}
		response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
		assert.Equal(t, 412, response.Result().StatusCode)

		// Now update successfully using the current etag:
		headers["If-Match"] = newEtag
		response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
		assert.Equal(t, 200, response.Result().StatusCode)
		newestEtag := response.HeaderMap.Get("Etag")
		assert.Regexp(t, `"[^"]+"`, newestEtag)
		assert.NotEqual(t, etag, newestEtag)
		assert.NotEqual(t, newEtag, newestEtag)

		// Try to delete using If-Match with the previous etag:
		response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
		assert.Equal(t, 412, response.Result().StatusCode)

		// Now delete successfully using the current etag:
		headers["If-Match"] = newestEtag
		response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
		assert.Equal(t, 200, response.Result().StatusCode)
	}

	t.Run("Function", func(t *testing.T) {
		runTest(t, "/db/_config/functions/xxx", `{
			"type": "javascript",
			"code": "function(){return 69;}"
		}`)
	})

	t.Run("Query", func(t *testing.T) {
		runTest(t, "/db/_config/functions/xxxN1QL", `{
			"type": "query",
			"code": "select 69"
		}`)
	})

	t.Run("Functions", func(t *testing.T) {
		runTest(t, "/db/_config/functions", `{
			"definitions": {
				"zzz": {"type": "javascript", "code": "function(){return 69;}"} } }`)
	})

	t.Run("GraphQL", func(t *testing.T) {
		runTest(t, "/db/_config/graphql", `{
			"schema": "type Query {square(n: Int!) : Int!}", "resolvers":{}
		}`)
	})
}

//////// JAVASCRIPT FUNCTIONS:

// When there's no existing config, API calls return 404:
func TestFunctionsConfigGetMissing(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestFunctionsConfigGet(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"square": {
					Type:  "javascript",
					Code:  "function(context,args){return args.numero * args.numero;}",
					Args:  []string{"numero"},
					Allow: &functions.Allow{Channels: []string{"wonderland"}},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		var body functions.FunctionsConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.NotNil(t, body.Definitions["square"])
	})
	t.Run("Single", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/square", "")
		var body functions.FunctionConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, "function(context,args){return args.numero * args.numero;}", body.Code)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/bogus", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestFunctionsConfigPut(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"square": {
					Type:  "javascript",
					Code:  "function(context,args){return args.numero * args.numero;}",
					Args:  []string{"numero"},
					Allow: &functions.Allow{Channels: []string{"wonderland"}},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/functions", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/functions", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions", `{
			"definitions": {
				"sum": {"type": "javascript",
						"code": "function(context,args){return args.numero + args.numero;}",
						"args": ["numero"],
						"allow": {"channels": ["*"]}} } }`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.Nil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, "26", string(response.BodyBytes()))

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("DeleteAll", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions)

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestFunctionsConfigPutOne(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"square": {
					Type:  "javascript",
					Code:  "function(context,args){return args.numero * args.numero;}",
					Args:  []string{"numero"},
					Allow: &functions.Allow{Channels: []string{"wonderland"}},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/functions/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/function/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Bogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `[]`)
		assert.Equal(t, 400, response.Result().StatusCode)
		response = rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{"ruby": "foo"}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("Add", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/sum", `{
			"type": "javascript",
			"code": "function(context,args){return args.numero + args.numero;}",
			"args": ["numero"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		assert.Equal(t, "26", string(response.BodyBytes()))
	})
	t.Run("ReplaceOne", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{
			"type": "javascript",
			"code": "function(context,args){return -args.n * args.n;}",
			"args": ["n"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		assert.Equal(t, "-169", string(response.BodyBytes()))
	})
	t.Run("DeleteOne", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions/square", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])
		assert.Equal(t, 1, len(rt.GetDatabase().Options.UserFunctions.Definitions))

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

//////// GRAPHQL

const kDummyGraphQLSchema = `
	type Query {
		square(n: Int!) : Int!
	}`

func TestFunctionsConfigGraphQLGetEmpty(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{})
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
	rt := newRestTesterForUserQueries(t, DbConfig{
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
	rt := newRestTesterForUserQueries(t, DbConfig{
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

//////// UTILITIES:

// Creates a new RestTester using persistent config, and a database "db".
// Only the user-query-related fields are copied from `queryConfig`; the rest are ignored.
func newRestTesterForUserQueries(t *testing.T, queryConfig DbConfig) *RestTester {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires persistent configs")
		return nil
	}

	rt := NewRestTester(t, &RestTesterConfig{
		groupID:           base.StringPtr(t.Name()), // Avoids race conditions between tests
		EnableUserQueries: true,
	})

	_ = rt.Bucket() // initializes the bucket as a side effect
	dbConfig := dbConfigForTestBucket(rt.TestBucket)
	dbConfig.UserFunctions = queryConfig.UserFunctions
	dbConfig.GraphQL = queryConfig.GraphQL

	resp, err := rt.CreateDatabase("db", dbConfig)
	if !assert.NoError(t, err) || !AssertStatus(t, resp, 201) {
		rt.Close()
		t.FailNow()
		return nil // (never reached)
	}
	return rt
}

func AssertStatus(t testing.TB, response *TestResponse, expectedStatus int) bool {
	return assert.Equalf(t, expectedStatus, response.Code,
		"Response status %d %q (expected %d %q)\nfor %s <%s> : %s",
		response.Code, http.StatusText(response.Code),
		expectedStatus, http.StatusText(expectedStatus),
		response.Req.Method, response.Req.URL, response.Body)
}
