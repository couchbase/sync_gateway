//go:build cb_sg_v8
// +build cb_sg_v8

/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"context"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

const kConcurrentTestNumTasks = 100000
const kConcurrentTestTimeout = 60 * time.Second

func TestGraphQLConcurrently(t *testing.T) {
	const kConcurrentTestNumTasks = 100000
	maxProcs := runtime.GOMAXPROCS(0)
	log.Printf("FYI, GOMAXPROCS = %d", maxProcs)

	fnConfig := &FunctionsConfig{
		Definitions: FunctionsDefs{
			"square": {
				Type:  "javascript",
				Code:  "function(context,args) {return args.n * args.n;}",
				Args:  []string{"n"},
				Allow: &Allow{Channels: []string{"*"}},
			},
		},
	}
	gqConfig := &GraphQLConfig{
		Schema: base.StringPtr(`type Query { square(n: Int!): Int! }`),
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": FunctionConfig{
					Type:  "javascript",
					Code:  `function(context,args) {return args.n * args.n;}`,
					Allow: &Allow{Channels: []string{"*"}},
				},
			},
		},
	}

	db, ctx := setupTestDBWithFunctionsAndLogging(t, fnConfig, gqConfig, false)
	defer db.Close(ctx)

	runConcurrently(ctx, kConcurrentTestNumTasks, maxProcs, func(ctx context.Context) bool {
		_, err := db.UserGraphQLQuery(`{"query":"query{ square(n:13) }"}`, "", nil, false, ctx)
		return assert.Nil(t, err)
	})
}

func runSequentially(ctx context.Context, numTasks int, testFunc func(context.Context) bool) time.Duration {
	ctx, cancel := context.WithTimeout(ctx, kConcurrentTestTimeout)
	defer cancel()
	startTime := time.Now()
	for i := 0; i < numTasks; i++ {
		testFunc(ctx)
	}
	return time.Since(startTime)
}

func runConcurrently(ctx context.Context, numTasks int, numThreads int, testFunc func(context.Context) bool) time.Duration {
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			myCtx, cancel := context.WithTimeout(ctx, kConcurrentTestTimeout)
			defer cancel()
			for j := 0; j < numTasks/numThreads; j++ {
				if !testFunc(myCtx) {
					break
				}
			}
		}()
	}
	wg.Wait()
	return time.Since(startTime)
}
