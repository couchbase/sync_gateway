// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/stretchr/testify/assert"
)

//////// Concurrency Tests

var kGraphQLTestConfig = &DatabaseConfig{DbConfig: DbConfig{
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
	JavaScriptEngine: base.StringPtr("V8"),
}}

func runSequentially(rt *RestTester, testFunc func() bool, numTasks int) time.Duration {
	startTime := time.Now()
	for i := 0; i < numTasks; i++ {
		testFunc()
	}
	return time.Since(startTime)
}

func runConcurrently(rt *RestTester, testFunc func() bool, numTasks int, numThreads int) time.Duration {
	tasksPerThread := numTasks / numThreads
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < tasksPerThread; i++ {
				testFunc()
			}
		}()
	}
	wg.Wait()
	return time.Since(startTime)
}

// Asserts that running testFunc in 100 concurrent goroutines is no more than 10% slower
// than running it 100 times in succession. A low bar indeed, but can detect some serious
// bottlenecks, or of course deadlocks.
func testConcurrently(t *testing.T, rt *RestTester, testFunc func() bool) bool {
	const numTasks = 10000
	const numThreads = 4

	assert.GreaterOrEqual(t, runtime.GOMAXPROCS(0), 2, "Not enough OS threads available")

	// prime the pump:
	runSequentially(rt, testFunc, 5)

	base.WarnfCtx(context.TODO(), "---- Starting sequential tasks ----")
	sequentialDuration := runSequentially(rt, testFunc, numTasks)
	base.WarnfCtx(context.TODO(), "---- Starting concurrent tasks ----")
	concurrentDuration := runConcurrently(rt, testFunc, numTasks, numThreads)
	base.WarnfCtx(context.TODO(), "---- End ----")

	log.Printf("---- %d sequential took %v, concurrent took %v ... speedup is %f",
		numTasks, sequentialDuration, concurrentDuration,
		float64(sequentialDuration)/float64(concurrentDuration))
	return assert.LessOrEqual(t, float64(concurrentDuration), 1.1*float64(sequentialDuration))
}

func TestFunctionsConcurrently(t *testing.T) {
	//TODO: Switch back to NewRestTester()
	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{GuestEnabled: true, EnableUserQueries: true, DatabaseConfig: kGraphQLTestConfig})
	defer rt.Close()

	t.Run("Function", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("GET", "/{{.db}}/_function/square?n=13", "")
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "169", string(response.BodyBytes()))
		})
	})

	t.Run("GraphQL", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("POST", "/{{.db}}/_graphql", `{"query":"query{ square(n:13) }"}`)
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "{\"data\":{\"square\":169}}", string(response.BodyBytes()))
		})
	})

	t.Run("GraphQL with variables", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("POST", "/{{.db}}/_graphql",
				`{"query": "query($number:Int!){ square(n:$number) }",
				  "variables": {"number": 13}}`)
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "{\"data\":{\"square\":169}}", string(response.BodyBytes()))
		})
	})

	t.Run("Query", func(t *testing.T) {
		if base.UnitTestUrlIsWalrus() {
			t.Skip("Skipping query subtest")
		} else {
			testConcurrently(t, rt, func() bool {
				response := rt.SendRequest("GET", "/{{.db}}/_function/squareN1QL?n=13", "")
				return assert.Equal(t, 200, response.Result().StatusCode) &&
					assert.Equal(t, "[{\"square\":169}]", string(response.BodyBytes()))
			})
		}
	})
}
