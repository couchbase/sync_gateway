package js

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestValidateJavascriptFunction(t *testing.T) {
	vm := NewVM()
	defer vm.Close()

	assert.NoError(t, vm.ValidateJavascriptFunction(`function(doc) {return doc.x;}`, 1, 1))
	assert.NoError(t, vm.ValidateJavascriptFunction(`(doc,foo) => {return doc.x;}`, 2, 2))

	err := vm.ValidateJavascriptFunction(`function() {return doc.x;}`, 1, 2)
	assert.ErrorContains(t, err, "function must have at least 1 parameters")
	err = vm.ValidateJavascriptFunction(`function(doc, foo, bar) {return doc.x;}`, 1, 2)
	assert.ErrorContains(t, err, "function must have no more than 2 parameters")
	err = vm.ValidateJavascriptFunction(`function(doc) {return doc.x;`, 1, 1)
	assert.ErrorContains(t, err, "SyntaxError")
	err = vm.ValidateJavascriptFunction(`"not a function"`, 1, 1)
	assert.ErrorContains(t, err, "code is not a function, but a string")
	err = vm.ValidateJavascriptFunction(`17 + 34`, 1, 1)
	assert.ErrorContains(t, err, "code is not a function, but a number")
}

const kVMPoolTestTimeout = 90 * time.Second

func runSequentially(ctx context.Context, testFunc func(context.Context) bool, numTasks int) time.Duration {
	ctx, cancel := context.WithTimeout(ctx, kVMPoolTestTimeout)
	defer cancel()
	startTime := time.Now()
	for i := 0; i < numTasks; i++ {
		testFunc(ctx)
	}
	return time.Since(startTime)
}

func runConcurrently(ctx context.Context, testFunc func(context.Context) bool, numTasks int, numThreads int) time.Duration {
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			myCtx, cancel := context.WithTimeout(ctx, kVMPoolTestTimeout)
			defer cancel()
			for j := 0; j < numTasks/numThreads; j++ {
				testFunc(myCtx)
			}
		}()
	}
	wg.Wait()
	return time.Since(startTime)
}

// Asserts that running testFunc in 100 concurrent goroutines is no more than 10% slower
// than running it 100 times in succession. A low bar indeed, but can detect some serious
// bottlenecks, or of course deadlocks.
func testConcurrently(t *testing.T, ctx context.Context, testFunc func(context.Context) bool) bool {
	const kNumTasks = 65536 * 10
	const kNumThreads = 8

	// prime the pump:
	runSequentially(ctx, testFunc, 1)

	base.WarnfCtx(context.TODO(), "---- Starting sequential tasks ----")
	sequentialDuration := runSequentially(ctx, testFunc, kNumTasks)
	base.WarnfCtx(context.TODO(), "---- Starting concurrent tasks ----")
	concurrentDuration := runConcurrently(ctx, testFunc, kNumTasks, kNumThreads)
	base.WarnfCtx(context.TODO(), "---- End ----")

	log.Printf("---- %d sequential took %v, concurrent (%d threads) took %v ... speedup is %f",
		kNumTasks, sequentialDuration, kNumThreads, concurrentDuration,
		float64(sequentialDuration)/float64(concurrentDuration))
	return assert.LessOrEqual(t, float64(concurrentDuration), 1.1*float64(sequentialDuration))
}

func TestPoolsConcurrently(t *testing.T) {
	log.Printf("FYI, GOMAXPROCS = %d", runtime.GOMAXPROCS(0))
	const kJSCode = `function(n) {return n * n;}`

	ctx := base.TestCtx(t)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)

	t.Run("Function", func(t *testing.T) {
		testConcurrently(t, ctx, func(ctx context.Context) bool {
			result, err := service.Run(ctx, 13)
			return assert.NoError(t, err) && assert.EqualValues(t, result, 169)
		})
	})
}

func BenchmarkVMPoolIntsSequentially(b *testing.B) {
	const kJSCode = `function(n) {return n * n;}`
	ctx := base.TestCtx(b)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, 13)
		return assert.NoError(b, err) && assert.EqualValues(b, result, 169)
	}
	b.ResetTimer()
	runSequentially(ctx, testFunc, b.N)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolIntsConcurrently(b *testing.B) {
	const kNumThreads = 8
	const kJSCode = `function(n) {return n * n;}`
	ctx := base.TestCtx(b)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, 13)
		return assert.NoError(b, err) && assert.EqualValues(b, result, 169)
	}
	b.ResetTimer()
	runConcurrently(ctx, testFunc, b.N, kNumThreads)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolStringsSequentially(b *testing.B) {
	fmt.Printf("-------- N = %d -------\n", b.N)
	const kJSCode = `function(str) {return str + str;}`
	ctx := base.TestCtx(b)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, "This is a test of the js package")
		return assert.NoError(b, err) && assert.EqualValues(b, result, "This is a test of the js packageThis is a test of the js package")
	}
	b.ResetTimer()
	runSequentially(ctx, testFunc, b.N)
	b.StopTimer()
	pool.Close()
}

func BenchmarkVMPoolStringsConcurrently(b *testing.B) {
	const kNumThreads = 8
	const kJSCode = `function(str) {return str + str;}`
	ctx := base.TestCtx(b)
	pool := NewVMPool(32)
	service := NewService(pool, "testy", kJSCode)
	testFunc := func(ctx context.Context) bool {
		result, err := service.Run(ctx, "This is a test of the js package")
		return assert.NoError(b, err) && assert.EqualValues(b, result, "This is a test of the js packageThis is a test of the js package")
	}
	b.ResetTimer()
	runConcurrently(ctx, testFunc, b.N, kNumThreads)
	b.StopTimer()
	pool.Close()
}
