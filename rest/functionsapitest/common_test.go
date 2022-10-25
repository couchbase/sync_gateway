//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package functionsapitest

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

//////// ALL ADMIN USER QUERY APIS:

// When feature flag is not enabled, all API calls return 404:
func TestFunctionsConfigGetWithoutFeatureFlag(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{EnableUserQueries: false})
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
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
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

//////// Concurrency Tests

var kGraphQLTestConfig = &rest.DatabaseConfig{DbConfig: rest.DbConfig{
	UserFunctions: &functions.FunctionsConfig{
		Definitions: functions.FunctionsDefs{
			"square": {
				Type:  "javascript",
				Code:  "function(context,args) {return args.n * args.n;}",
				Args:  []string{"n"},
				Allow: &functions.Allow{Channels: []string{"*"}},
			},
			"squareN1QL": {
				Type:  "query",
				Code:  "SELECT $args.n * $args.n AS square",
				Args:  []string{"n"},
				Allow: &functions.Allow{Channels: []string{"*"}},
			},
		},
	},
	GraphQL: &functions.GraphQLConfig{
		Schema: base.StringPtr(`type Query { square(n: Int!): Int! }`),
		Resolvers: map[string]functions.GraphQLResolverConfig{
			"Query": {
				"square": functions.FunctionConfig{
					Type:  "javascript",
					Code:  `function(context,args) {return args.n * args.n;}`,
					Allow: &functions.Allow{Channels: []string{"*"}},
				},
			},
		},
	},
}}

// Asserts that running testFunc in 100 concurrent goroutines is no more than 10% slower
// than running it 100 times in succession. A low bar indeed, but can detect some serious
// bottlenecks, or of course deadlocks.
func testConcurrently(t *testing.T, rt *rest.RestTester, testFunc func() bool) bool {
	const numTasks = 100

	// Run testFunc once to prime the pump:
	if !testFunc() {
		return false
	}

	// 100 sequential calls:
	startTime := time.Now()
	for i := 0; i < numTasks; i++ {
		if !testFunc() {
			return false
		}
	}
	sequentialDuration := time.Since(startTime)

	// Now run 100 concurrent calls:
	var wg sync.WaitGroup
	startTime = time.Now()
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testFunc()
		}()
	}
	wg.Wait()
	concurrentDuration := time.Since(startTime)

	log.Printf("---- %d sequential took %v, concurrent took %v ... speedup is %f",
		numTasks, sequentialDuration, concurrentDuration,
		float64(sequentialDuration)/float64(concurrentDuration))
	return assert.LessOrEqual(t, concurrentDuration, 1.1*numTasks*sequentialDuration)
}

func TestFunctions(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true, EnableUserQueries: true})
	defer rt.Close()
	rt.DatabaseConfig = kGraphQLTestConfig

	t.Run("GraphQL with variables", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("POST", "/db/_graphql",
				`{"query": "query($number:Int!){ square(n:$number) }",
				  "variables": {"number": 13}}`)
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "{\"data\":{\"square\":169}}", string(response.BodyBytes()))
		})
	})
}

func TestFunctionsConcurrently(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true, EnableUserQueries: true})
	defer rt.Close()
	rt.DatabaseConfig = kGraphQLTestConfig

	t.Run("Function", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("GET", "/db/_function/square?n=13", "")
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "169", string(response.BodyBytes()))
		})
	})

	t.Run("GraphQL", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("POST", "/db/_graphql", `{"query":"query{ square(n:13) }"}`)
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "{\"data\":{\"square\":169}}", string(response.BodyBytes()))
		})
	})

	t.Run("Query", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() {
			t.Skip("Skipping query subtest")
		} else {
			testConcurrently(t, rt, func() bool {
				response := rt.SendRequest("GET", "/db/_function/squareN1QL?n=13", "")
				return assert.Equal(t, 200, response.Result().StatusCode) &&
					assert.Equal(t, "[{\"square\":169}\n]\n", string(response.BodyBytes()))
			})
		}
	})
}
