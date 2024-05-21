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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequenceAllocator(t *testing.T) {

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()

	// Create a sequence allocator without using constructor, to test without a releaseSequenceMonitor
	//   - allows manually triggered release
	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           testStats,
		sequenceBatchSize: idleBatchSize,
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	// Set high incr frequency to force batch size increase
	oldFrequency := MaxSequenceIncrFrequency
	defer func() { MaxSequenceIncrFrequency = oldFrequency }()
	MaxSequenceIncrFrequency = 60 * time.Second

	initSequence, err := a.lastSequence(ctx)
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	// Initial allocation should use batch size of 1
	nextSequence, err := a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	assertNewAllocatorStats(t, testStats, 1, 1, 1, 0)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 0)

	// Subsequent allocation shouldn't trigger allocation
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 3, 0)

	// Subsequent allocation should increase batch to 4, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), nextSequence)
	assert.Equal(t, 4, int(a.sequenceBatchSize))
	assertNewAllocatorStats(t, testStats, 3, 7, 4, 0)

	// Release unused sequences.  Should reduce batch size to 1 (based on 3 unused)
	a.releaseUnusedSequences(ctx)
	assertNewAllocatorStats(t, testStats, 3, 7, 4, 3)
	assert.Equal(t, 1, int(a.sequenceBatchSize))

	// Subsequent allocation should increase batch to 2, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), nextSequence)
	assertNewAllocatorStats(t, testStats, 4, 9, 5, 3)
	assert.Equal(t, 2, int(a.sequenceBatchSize))

}

func TestReleaseSequencesOnStop(t *testing.T) {

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()

	oldFrequency := MaxSequenceIncrFrequency
	defer func() { MaxSequenceIncrFrequency = oldFrequency }()
	MaxSequenceIncrFrequency = 1000 * time.Millisecond
	a, err := newSequenceAllocator(ctx, bucket.GetSingleDataStore(), testStats, base.DefaultMetadataKeys)
	// Reduce sequence wait for Stop testing
	a.releaseSequenceWait = 10 * time.Millisecond
	assert.NoError(t, err, "error creating allocator")

	// Initial allocation should use batch size of 1
	nextSequence, err := a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	assertNewAllocatorStats(t, testStats, 1, 1, 1, 0)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 0)

	// Stop the allocator
	a.Stop(ctx)

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
	ctx := base.TestCtx(t)
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
					_, err := a.nextSequence(ctx)
					assert.NoError(t, err)
					wg.Done()
				}(a)
			}
		}
	}

	bucket := base.NewLeakyBucket(base.GetTestBucket(t), base.LeakyBucketConfig{IncrCallback: incrCallback})
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()

	oldFrequency := MaxSequenceIncrFrequency
	defer func() { MaxSequenceIncrFrequency = oldFrequency }()
	MaxSequenceIncrFrequency = 1000 * time.Millisecond

	a, err = newSequenceAllocator(ctx, bucket.DefaultDataStore(), testStats, base.DefaultMetadataKeys)
	// Reduce sequence wait for Stop testing
	a.releaseSequenceWait = 10 * time.Millisecond
	assert.NoError(t, err, "error creating allocator")

	nextSequence, err := a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)

	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)

	wg.Wait()

	a.Stop(ctx)
}

func TestReleaseSequenceWait(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()
	a, err := newSequenceAllocator(ctx, bucket.GetSingleDataStore(), testStats, base.DefaultMetadataKeys)
	require.NoError(t, err)
	defer a.Stop(ctx)

	startTime := time.Now().Add(-1 * time.Second)
	amountWaited := a.waitForReleasedSequences(ctx, startTime)
	// Time will be a little less than a.releaseSequenceWait - 1*time.Second - validate
	// there's a non-zero wait that's less than releaseSequenceWait
	assert.True(t, amountWaited > 0)
	assert.True(t, amountWaited < a.releaseSequenceWait)

	// Validate no wait for a time in the past longer than releaseSequenceWait
	noWaitTime := time.Now().Add(-5 * time.Second)
	amountWaited = a.waitForReleasedSequences(ctx, noWaitTime)
	assert.Equal(t, time.Duration(0), amountWaited)
}

func assertNewAllocatorStats(t *testing.T, stats *base.DatabaseStats, incr, reserved, assigned, released int64) {
	assert.Equal(t, incr, stats.SequenceIncrCount.Value())
	assert.Equal(t, reserved, stats.SequenceReservedCount.Value())
	assert.Equal(t, assigned, stats.SequenceAssignedCount.Value())
	assert.Equal(t, released, stats.SequenceReleasedCount.Value())
}

func TestNextSequenceGreaterThanSingleNode(t *testing.T) {

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()

	// Create a sequence allocator without using constructor, to test without a releaseSequenceMonitor
	// Set sequenceBatchSize=10 to test variations of batching
	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           testStats,
		sequenceBatchSize: 10,                      // set initial batch size to 10 to support all test cases
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	initSequence, err := a.lastSequence(ctx)
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	// nextSequenceGreaterThan(0) should perform initial batch allocation of size 10,  and not release any sequences
	nextSequence, releasedSequenceCount, err := a.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	require.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 1, 0) // incr, reserved, assigned, released counts

	// Calling the same again should use from the existing batch
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assert.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 2, 0)

	// Test case where greaterThan == s.Last + 1
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), nextSequence)
	require.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 3, 0)

	// When requested nextSequenceGreaterThan is > s.Last + 1, we should release previously allocated sequences but
	// don't require a new incr
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 5)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), nextSequence)
	require.Equal(t, 2, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 4, 2)

	// Test when requested nextSequenceGreaterThan == s.Max; should release previously allocated sequences and allocate a new batch
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), nextSequence)
	assert.Equal(t, 4, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 2, 20, 5, 6)

	// Test when requested nextSequenceGreaterThan = s.Max + 1; should release previously allocated sequences AND max+1
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 21)
	assert.NoError(t, err)
	assert.Equal(t, uint64(22), nextSequence)
	assert.Equal(t, 10, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 3, 31, 6, 16)

	// Test when requested nextSequenceGreaterThan > s.Max + batch size; should release 9 previously allocated sequences (23-31)
	// and 19 in the gap to the requested sequence (32-50)
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 50)
	assert.NoError(t, err)
	assert.Equal(t, uint64(51), nextSequence)
	assert.Equal(t, 28, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 4, 60, 7, 44)

}

func TestNextSequenceGreaterThanMultiNode(t *testing.T) {

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	// Create two sequence allocators without using constructor, to test without a releaseSequenceMonitor
	// Set sequenceBatchSize=10 to test variations of batching
	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	statsA, err := stats.NewDBStats("A", false, false, false, nil, nil)
	require.NoError(t, err)
	statsB, err := stats.NewDBStats("B", false, false, false, nil, nil)
	require.NoError(t, err)
	dbStatsA := statsA.DatabaseStats
	dbStatsB := statsB.DatabaseStats

	require.NoError(t, err)
	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsA,
		sequenceBatchSize: 10,                      // set initial batch size to 10 to support all test cases
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	b := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsB,
		sequenceBatchSize: 10,                      // set initial batch size to 10 to support all test cases
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	initSequence, err := a.lastSequence(ctx)
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	initSequence, err = b.lastSequence(ctx)
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	// nextSequenceGreaterThan(0) on A should perform initial batch allocation of size 10 (allocs 1-10),  and not release any sequences
	nextSequence, releasedSequenceCount, err := a.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	assert.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsA, 1, 10, 1, 0) // incr, reserved, assigned, released counts

	// nextSequenceGreaterThan(0) on B should perform initial batch allocation of size 10 (allocs 11-20),  and not release any sequences
	nextSequence, releasedSequenceCount, err = b.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), nextSequence)
	assert.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsB, 1, 10, 1, 0)

	// calling nextSequenceGreaterThan(15) on B will assign from the existing batch, and release 12-15
	nextSequence, releasedSequenceCount, err = b.nextSequenceGreaterThan(ctx, 15)
	assert.NoError(t, err)
	assert.Equal(t, uint64(16), nextSequence)
	assert.Equal(t, 4, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsB, 1, 10, 2, 4)

	// calling nextSequenceGreaterThan(15) on A will increment _sync:seq by 5 on it's previously allocated sequence (10).
	// Since node B has already updated _sync:seq to 20, will result in:
	//   node A releasing sequences 2-10 from it's existing buffer
	//   node A allocating and releasing sequences 21-24
	//   node A adding sequences 25-35 to its buffer, and assigning 25 to the current request
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 15)
	assert.NoError(t, err)
	assert.Equal(t, uint64(26), nextSequence)
	assert.Equal(t, 14, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsA, 2, 25, 2, 14)

}

// TestSingleNodeSyncSeqRollback:
//   - Test rollback of _sync:seq doc in bucket for a single node
//   - Use nextSequence to allocate sequences and trigger the rollback handling code
//   - Between each sequence allocation alter the _sync:seq doc to some value in the bucket + alter a.last to mock
//     allocator allocating sequences to end of current batch
//   - Asserts that handling using nextSequence function is correct
func TestSingleNodeSyncSeqRollback(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()
	ds := bucket.GetSingleDataStore()

	a, err := newSequenceAllocator(ctx, ds, testStats, base.DefaultMetadataKeys)
	require.NoError(t, err)
	a.sequenceBatchSize = 10

	nxtSeq, err := a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nxtSeq)

	// alter _sync:seq in bucket to 5
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 10

	// triggers correction value increase to 10 (sequence batch value) thus nextSeq is higher than you would expect
	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(16), nxtSeq)
	assert.Equal(t, uint64(25), a.max)

	// alter _sync:seq in bucket to end seq in prev batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 10)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 25

	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(26), nxtSeq)
	assert.Equal(t, uint64(35), a.max)

	// alter _sync:seq in bucket to start seq in batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 26)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 35

	// triggers correction value increase to 10 (sequence batch value) thus nextSeq is higher than you would expect
	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(37), nxtSeq)
	assert.Equal(t, uint64(46), a.max)

	// rollback _sync:seq in bucket to start seq to seq outside of batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 46

	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(47), nxtSeq)
	assert.Equal(t, uint64(56), a.max)
}

// TestSingleNodeNextSeqGreaterThanRollbackHandling:
//   - Test rollback of _sync:seq doc in bucket for a single node
//   - Use nextSequenceGreaterThan to allocate sequences and trigger the rollback handling code
//   - Between each sequence allocation alter the _sync:seq doc to some value in the bucket + alter a.last to mock
//     allocator allocating sequences to end of current batch
//   - Asserts that handling using nextSequenceGreaterThan function is correct
func TestSingleNodeNextSeqGreaterThanRollbackHandling(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	sgw, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := sgw.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Database()
	ds := bucket.GetSingleDataStore()

	a, err := newSequenceAllocator(ctx, ds, testStats, base.DefaultMetadataKeys)
	require.NoError(t, err)
	a.sequenceBatchSize = 10

	// allocate something
	nxtSeq, err := a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nxtSeq)

	// alter _sync:seq in bucket to 5
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 10

	// triggers correction value increase to 10 (sequence batch value) thus nextSeq is higher than you would expect
	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 15)
	require.NoError(t, err)
	assert.Equal(t, uint64(21), nxtSeq)
	assert.Equal(t, uint64(21), a.last)
	assert.Equal(t, uint64(30), a.max)

	// alter _sync:seq in bucket to end seq in prev batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 10)
	require.NoError(t, err)

	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 30)
	require.NoError(t, err)
	assert.Equal(t, uint64(31), nxtSeq)
	assert.Equal(t, uint64(31), a.last)
	assert.Equal(t, uint64(40), a.max)

	// alter _sync:seq in bucket to end seq in prev batch start seq
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 21)
	require.NoError(t, err)

	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 40)
	require.NoError(t, err)
	assert.Equal(t, uint64(41), nxtSeq)
	assert.Equal(t, uint64(41), a.last)
	assert.Equal(t, uint64(50), a.max)

	// alter _sync:seq in bucket to prev batch value
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 50)
	require.NoError(t, err)
	assert.Equal(t, uint64(51), nxtSeq)
	assert.Equal(t, uint64(51), a.last)
	assert.Equal(t, uint64(60), a.max)
}

// TestSyncSeqRollbackMultiNode:
//   - Test rollback of _sync:seq doc in bucket for a multi node cluster node
//   - Use nextSequenceGreaterThan to allocate sequences and trigger the rollback handling code
//   - Alter _sync:seq in the bucket to rollback the value
//   - Alter last seq on each allocator to mock allocation of some sequences
//   - Use two go routines to test two nodes racing to update the rollback back _sync:seq document
//   - Asser that the resulting batches on each node do not overlap
func TestSyncSeqRollbackMultiNode(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	statsA, err := stats.NewDBStats("A", false, false, false, nil, nil)
	require.NoError(t, err)
	statsB, err := stats.NewDBStats("B", false, false, false, nil, nil)
	require.NoError(t, err)
	dbStatsA := statsA.DatabaseStats
	dbStatsB := statsB.DatabaseStats
	ds := bucket.GetSingleDataStore()

	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsA,
		sequenceBatchSize: 10,                      // set initial batch size to 10 to support all test cases
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	b := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsB,
		sequenceBatchSize: 10,                      // set initial batch size to 10 to support all test cases
		reserveNotify:     make(chan struct{}, 50), // Buffered to allow multiple allocations without releaseSequenceMonitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	initSequence, err := a.lastSequence(ctx)
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	initSequence, err = b.lastSequence(ctx)
	assert.Equal(t, uint64(0), initSequence)
	assert.NoError(t, err, "error retrieving last sequence")

	// perform batch allocation on sequence allocator a
	nextSequence, _, err := a.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)

	// perform batch allocation on sequence allocator b
	nextSequence, _, err = b.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), nextSequence)

	// alter _sync:seq in bucket to prev value
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 2)
	require.NoError(t, err)

	// set a.last on this allocator to 5 (mock some sequences being allocated)
	a.last = 5

	// set b.last on this allocator to 15 (mock some sequences being allocated)
	b.last = 15

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		nextSequence, _, err = b.nextSequenceGreaterThan(ctx, 20)
		assert.NoError(t, err)
		wg.Done()
	}()

	go func() {
		nextSequence, _, err = a.nextSequenceGreaterThan(ctx, 10)
		assert.NoError(t, err)
		wg.Done()
	}()
	wg.Wait()

	// above goroutines will race each other for access to _sync:seq
	// to assert that _sync:seq is corrected appropriately we need to determine which sequence allocator got there first
	// assert that each sequence batch doesn't overlap
	if a.last < b.last {
		assert.Greater(t, b.last, a.max)
	} else {
		assert.Greater(t, a.last, b.max)
	}

	// grab sync seq value from bucket
	syncSeqVal, err := a.getSequence()
	require.NoError(t, err)
	// get max batch value
	maxBatchSeq := math.Max(float64(a.max), float64(b.max))
	// assert equal to _sync:seq Value
	assert.Equal(t, syncSeqVal, uint64(maxBatchSeq))
}
