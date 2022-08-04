/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

// Asserts that running testFunc in 100 concurrent goroutines is no more than 10% slower
// than running it 100 times in succession. A low bar indeed, but can detect some serious
// bottlenecks, or of course deadlocks.
func testConcurrently(t *testing.T, rt *RestTester, testFunc func() bool) bool {
	// Run testFunc once to prime the pump:
	if !testFunc() {
		return false
	}

	// Run once to get a baseline time:
	startTime := time.Now()
	if !testFunc() {
		return false
	}
	singleDuration := time.Since(startTime)

	// Now run 100 concurrent calls:
	const numTasks = 100
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
	multiDuration := time.Since(startTime)

	log.Printf("---- One call took %v, %d calls took %v ... speedup is %f",
		singleDuration, numTasks, multiDuration,
		float64(singleDuration)/(float64(multiDuration)/numTasks))
	return assert.LessOrEqual(t, multiDuration, 1.1*numTasks*singleDuration)
}

func TestConcurrentQueries(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true, EnableUserQueries: true})
	defer rt.Close()
	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.n * args.n;",
				Parameters: []string{"n"},
				Allow:      &db.UserQueryAllow{Channels: []string{"*"}},
			},
		},
		UserQueries: db.UserQueryMap{
			"square": &db.UserQueryConfig{
				Statement:  "SELECT $n * $n AS square",
				Parameters: []string{"n"},
				Allow:      &db.UserQueryAllow{Channels: []string{"*"}},
			},
		},
		GraphQL: &db.GraphQLConfig{
			Schema: base.StringPtr(`type Query { square(n: Int!): Int! }`),
			Resolvers: map[string]db.GraphQLResolverConfig{
				"Query": {
					"square": `return args.n * args.n;`,
				},
			},
		},
	}}

	t.Run("Function", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("GET", "/db/_function/square?n=13", "")
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "169", string(response.BodyBytes()))
		})
	})

	t.Run("Query", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("GET", "/db/_query/square?n=13", "")
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "[{\"square\":169}\n]\n", string(response.BodyBytes()))
		})
	})

	t.Run("GraphQL", func(t *testing.T) {
		testConcurrently(t, rt, func() bool {
			response := rt.SendRequest("POST", "/db/_graphql", `{"query":"query{ square(n:13) }"}`)
			return assert.Equal(t, 200, response.Result().StatusCode) &&
				assert.Equal(t, "{\"data\":{\"square\":169}}", string(response.BodyBytes()))
		})
	})
}
