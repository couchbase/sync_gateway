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

package js

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

//////// VALIDATION TESTS

func TestValidateJavascriptFunction(t *testing.T) {
	TestWithVMs(t, func(t *testing.T, vm VM) {
		assert.NoError(t, ValidateJavascriptFunction(vm, `function(doc) {return doc.x;}`, 1, 1))
		if vm.Engine().LanguageVersion() >= ES2015 { // Otto does not support new-style function syntax
			assert.NoError(t, ValidateJavascriptFunction(vm, `(doc,foo) => {return doc.x;}`, 2, 2))
		}
		err := ValidateJavascriptFunction(vm, `function() {return doc.x;}`, 1, 2)
		assert.ErrorContains(t, err, "function must have at least 1 parameters")
		err = ValidateJavascriptFunction(vm, `function(doc, foo, bar) {return doc.x;}`, 1, 2)
		assert.ErrorContains(t, err, "function must have no more than 2 parameters")
		err = ValidateJavascriptFunction(vm, `function(doc) {return doc.x;`, 1, 1)
		assert.ErrorContains(t, err, "SyntaxError")
		err = ValidateJavascriptFunction(vm, `"not a function"`, 1, 1)
		assert.ErrorContains(t, err, "code is not a function, but a string")
		err = ValidateJavascriptFunction(vm, `17 + 34`, 1, 1)
		assert.ErrorContains(t, err, "code is not a function, but a number")
	})
}

//////// CONCURRENCY TESTS

const kVMPoolTestScript = `function(n) {return n * n;}`
const kVMPoolTestTimeout = 60 * time.Second
const kVMPoolTestNumTasks = 65536

func TestPoolsSequentially(t *testing.T) {
	ctx := base.TestCtx(t)
	if !assertPriorTimeoutAtLeast(t, ctx, kVMPoolTestTimeout) {
		return
	}
	TestWithVMPools(t, 4, func(t *testing.T, pool *VMPool) {
		service := NewService(pool, "testy", kVMPoolTestScript)
		runSequentially(ctx, kVMPoolTestNumTasks, func(ctx context.Context) bool {
			result, err := service.Run(ctx, 13)
			return assert.NoError(t, err) && assert.EqualValues(t, result, 169)
		})
	})
}

func TestPoolsConcurrently(t *testing.T) {
	maxProcs := runtime.GOMAXPROCS(0)
	log.Printf("FYI, GOMAXPROCS = %d", maxProcs)
	if !assert.GreaterOrEqual(t, maxProcs, 2, "Not enough OS threads available") {
		return
	}

	ctx := base.TestCtx(t)
	if !assertPriorTimeoutAtLeast(t, ctx, kVMPoolTestTimeout) {
		return
	}

	TestWithVMPools(t, maxProcs, func(t *testing.T, pool *VMPool) {
		numTasks := kVMPoolTestNumTasks
		service := NewService(pool, "testy", kVMPoolTestScript)
		t.Run("Function", func(t *testing.T) {
			testConcurrently(t, ctx, numTasks, maxProcs, func(ctx context.Context) bool {
				result, err := service.Run(ctx, 13)
				return assert.NoError(t, err) && assert.EqualValues(t, result, 169)
			})
		})
	})
}

//////// CONCURRENCY BENCHMARKS

func BenchmarkVMPoolIntsSequentially(b *testing.B) {
	ctx := base.TestCtx(b)
	if !assertPriorTimeoutAtLeast(b, ctx, kVMPoolTestTimeout) {
		return
	}
	pool := NewVMPool(V8, 32)
	service := NewService(pool, "testy", kVMPoolTestScript)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, 13)
		return assert.NoError(b, err) && assert.EqualValues(b, result, 169)
	}
	b.ResetTimer()
	runSequentially(ctx, b.N, testFunc)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolIntsConcurrently(b *testing.B) {
	const kNumThreads = 8
	ctx := base.TestCtx(b)
	pool := NewVMPool(V8, 32)
	service := NewService(pool, "testy", kVMPoolTestScript)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, 13)
		return assert.NoError(b, err) && assert.EqualValues(b, result, 169)
	}
	b.ResetTimer()
	runConcurrently(ctx, b.N, kNumThreads, testFunc)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolStringsSequentially(b *testing.B) {
	fmt.Printf("-------- N = %d -------\n", b.N)
	ctx := base.TestCtx(b)
	pool := NewVMPool(V8, 32)
	service := NewService(pool, "testy", kVMPoolTestScript)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, "This is a test of the js package")
		return assert.NoError(b, err) && assert.EqualValues(b, result, "This is a test of the js packageThis is a test of the js package")
	}
	b.ResetTimer()
	runSequentially(ctx, b.N, testFunc)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolStringsConcurrently(b *testing.B) {
	const kNumThreads = 8
	ctx := base.TestCtx(b)
	pool := NewVMPool(V8, 32)
	service := NewService(pool, "testy", kVMPoolTestScript)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, "This is a test of the js package")
		return assert.NoError(b, err) && assert.EqualValues(b, result, "This is a test of the js packageThis is a test of the js package")
	}
	b.ResetTimer()
	runConcurrently(ctx, b.N, kNumThreads, testFunc)
	b.StopTimer()
	pool.Close()
}

//////// SUPPORT FUNCTIONS

func runSequentially(ctx context.Context, numTasks int, testFunc func(context.Context) bool) time.Duration {
	ctx, cancel := context.WithTimeout(ctx, kVMPoolTestTimeout)
	defer cancel()
	startTime := time.Now()
	for i := 0; i < numTasks; i++ {
		if !testFunc(ctx) {
			break
		}
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
			myCtx, cancel := context.WithTimeout(ctx, kVMPoolTestTimeout)
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

// Asserts that running testFunc in 100 concurrent goroutines is no more than 10% slower
// than running it 100 times in succession. A low bar indeed, but can detect some serious
// bottlenecks, or of course deadlocks.
func testConcurrently(t *testing.T, ctx context.Context, numTasks int, numThreads int, testFunc func(context.Context) bool) bool {
	// prime the pump:
	runSequentially(ctx, 1, testFunc)

	base.WarnfCtx(context.TODO(), "---- Starting %d sequential tasks ----", numTasks)
	sequentialDuration := runSequentially(ctx, numTasks, testFunc)
	base.WarnfCtx(context.TODO(), "---- Starting %d concurrent tasks on %d goroutines ----", numTasks, numThreads)
	concurrentDuration := runConcurrently(ctx, numTasks, numThreads, testFunc)
	base.WarnfCtx(context.TODO(), "---- End ----")

	log.Printf("---- %d sequential took %v, concurrent (%d threads) took %v ... speedup is %f",
		numTasks, sequentialDuration, numThreads, concurrentDuration,
		float64(sequentialDuration)/float64(concurrentDuration))
	if numThreads < 4 {
		// In CI there tend to be few threads available, and the machine is usually heavily
		// loaded, which makes the timing unpredictable.
		return true
	}
	return assert.LessOrEqual(t, float64(concurrentDuration), 1.1*float64(sequentialDuration))
}

func assertPriorTimeoutAtLeast(t testing.TB, ctx context.Context, min time.Duration) bool {
	deadline, exists := ctx.Deadline()
	return !exists || assert.GreaterOrEqual(t, time.Until(deadline), min)
}
