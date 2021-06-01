/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestSequenceAllocator(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	sgw := base.NewSyncGatewayStats()
	testStats := sgw.NewDBStats("", false, false, false).Database()

	// Create a sequence allocator without using constructor, to test without a releaseSequenceMonitor
	//   - allows manually triggered release
	a := &sequenceAllocator{
		bucket:            bucket,
		dbStats:           testStats,
		sequenceBatchSize: idleBatchSize,
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
	}

	// Set high incr frequency to force batch size increase
	oldFrequency := MaxSequenceIncrFrequency
	defer func() { MaxSequenceIncrFrequency = oldFrequency }()
	MaxSequenceIncrFrequency = 60 * time.Second

	initSequence, err := a.lastSequence()
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	// Initial allocation should use batch size of 1
	nextSequence, err := a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	assertNewAllocatorStats(t, testStats, 1, 1, 1, 0)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 0)

	// Subsequent allocation shouldn't trigger allocation
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 3, 0)

	// Subsequent allocation should increase batch to 4, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), nextSequence)
	assert.Equal(t, 4, int(a.sequenceBatchSize))
	assertNewAllocatorStats(t, testStats, 3, 7, 4, 0)

	// Release unused sequences.  Should reduce batch size to 1 (based on 3 unused)
	a.releaseUnusedSequences()
	assertNewAllocatorStats(t, testStats, 3, 7, 4, 3)
	assert.Equal(t, 1, int(a.sequenceBatchSize))

	// Subsequent allocation should increase batch to 2, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), nextSequence)
	assertNewAllocatorStats(t, testStats, 4, 9, 5, 3)
	assert.Equal(t, 2, int(a.sequenceBatchSize))

}

func TestReleaseSequencesOnStop(t *testing.T) {

	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	sgw := base.NewSyncGatewayStats()
	testStats := sgw.NewDBStats("", false, false, false).Database()

	oldFrequency := MaxSequenceIncrFrequency
	defer func() { MaxSequenceIncrFrequency = oldFrequency }()
	MaxSequenceIncrFrequency = 1000 * time.Millisecond

	a, err := newSequenceAllocator(bucket, testStats)
	// Reduce sequence wait for Stop testing
	a.releaseSequenceWait = 10 * time.Millisecond
	assert.NoError(t, err, "error creating allocator")

	// Initial allocation should use batch size of 1
	nextSequence, err := a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	assertNewAllocatorStats(t, testStats, 1, 1, 1, 0)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 0)

	// Stop the allocator
	a.Stop()

	releasedCount := 0
	// Ensure unused sequence is released on Stop
	for i := 0; i < 20; i++ {
		releasedCount = int(testStats.SequenceReleasedCount.Value())
		if releasedCount == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, 1, releasedCount, "Expected 1 released sequence")
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 1)

}

// Reproduces deadlock from CBG-663.  Required adding a sleep inside the <-time.After case in
// releaseSequenceMonitor to reliably queue up a large number of sequence allocation requests
// between <-time.After fires and releaseUnusedSequences is called (where previously reserveNotify would block)
func TestSequenceAllocatorDeadlock(t *testing.T) {

	t.Skip("Requires additional sleep in production code to reliably hit race")

	var a *sequenceAllocator
	var err error

	var wg sync.WaitGroup
	callbackCount := 0
	incrCallback := func() {
		callbackCount++
		if callbackCount == 2 {
			// queue up a number of sequence requests
			// Wait for 500ms for releaseSequenceMonitor time.After to trigger
			time.Sleep(100 * time.Millisecond)

			for i := 0; i < 500; i++ {
				wg.Add(1)
				go func(a *sequenceAllocator) {
					_, err := a.nextSequence()
					assert.NoError(t, err)
					wg.Done()
				}(a)
			}
		}
	}

	bucket := base.NewLeakyBucket(base.GetTestBucket(t), base.LeakyBucketConfig{IncrCallback: incrCallback})
	defer bucket.Close()

	sgw := base.NewSyncGatewayStats()
	testStats := sgw.NewDBStats("", false, false, false).Database()

	oldFrequency := MaxSequenceIncrFrequency
	defer func() { MaxSequenceIncrFrequency = oldFrequency }()
	MaxSequenceIncrFrequency = 1000 * time.Millisecond

	a, err = newSequenceAllocator(bucket, testStats)
	// Reduce sequence wait for Stop testing
	a.releaseSequenceWait = 10 * time.Millisecond
	assert.NoError(t, err, "error creating allocator")

	nextSequence, err := a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)

	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)

	wg.Wait()

	a.Stop()
}

func TestReleaseSequenceWait(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	sgw := base.NewSyncGatewayStats()
	testStats := sgw.NewDBStats("", false, false, false).Database()

	a, err := newSequenceAllocator(bucket, testStats)
	assert.NoError(t, err)

	startTime := time.Now().Add(-1 * time.Second)
	amountWaited := a.waitForReleasedSequences(startTime)
	// Time will be a little less than a.releaseSequenceWait - 1*time.Second - validate
	// there's a non-zero wait that's less than releaseSequenceWait
	assert.True(t, amountWaited > 0)
	assert.True(t, amountWaited < a.releaseSequenceWait)

	// Validate no wait for a time in the past longer than releaseSequenceWait
	noWaitTime := time.Now().Add(-5 * time.Second)
	amountWaited = a.waitForReleasedSequences(noWaitTime)
	assert.Equal(t, time.Duration(0), amountWaited)
}

func assertNewAllocatorStats(t *testing.T, stats *base.DatabaseStats, incr, reserved, assigned, released int64) {
	assert.Equal(t, incr, stats.SequenceIncrCount.Value())
	assert.Equal(t, reserved, stats.SequenceReservedCount.Value())
	assert.Equal(t, assigned, stats.SequenceAssignedCount.Value())
	assert.Equal(t, released, stats.SequenceReleasedCount.Value())
}
