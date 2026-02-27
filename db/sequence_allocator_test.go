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
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
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
	assertNewAllocatorStats(t, testStats, 1, 1, 1, 0, nextSequence, 1)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 0, nextSequence, 3)

	// Subsequent allocation shouldn't trigger allocation
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 3, 0, nextSequence, 3)

	// Subsequent allocation should increase batch to 4, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), nextSequence)
	assert.Equal(t, 4, int(a.sequenceBatchSize))
	assertNewAllocatorStats(t, testStats, 3, 7, 4, 0, nextSequence, 7)

	// Release unused sequences.  Should reduce batch size to 1 (based on 3 unused)
	a.releaseUnusedSequences(ctx)
	assertNewAllocatorStats(t, testStats, 3, 7, 4, 3, nextSequence, 7)
	assert.Equal(t, 1, int(a.sequenceBatchSize))

	// Subsequent allocation should increase batch to 2, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), nextSequence)
	assertNewAllocatorStats(t, testStats, 4, 9, 5, 3, nextSequence, 9)
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
	assertNewAllocatorStats(t, testStats, 1, 1, 1, 0, nextSequence, 1)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 0, nextSequence, 3)

	// Stop the allocator
	a.Stop(ctx)

	releasedCount := 0
	// Ensure unused sequence is released on Stop
	for range 20 {
		releasedCount = int(testStats.SequenceReleasedCount.Value())
		if releasedCount == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, 1, releasedCount, "Expected 1 released sequence")
	assertNewAllocatorStats(t, testStats, 2, 3, 2, 1, nextSequence, 3)

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

			for range 500 {
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
	base.LongRunningTest(t)

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

func assertNewAllocatorStats(t *testing.T, stats *base.DatabaseStats, incrCount, reservedCount, assignedCount, releasedCount int64, lastAssignedValue, lastReservedValue uint64) {
	assert.Equal(t, incrCount, stats.SequenceIncrCount.Value())
	assert.Equal(t, uint64(reservedCount), stats.SequenceReservedCount.Value())
	assert.Equal(t, uint64(assignedCount), stats.SequenceAssignedCount.Value())
	assert.Equal(t, uint64(releasedCount), stats.SequenceReleasedCount.Value())
	assert.Equal(t, lastAssignedValue, stats.LastSequenceAssignedValue.Value())
	assert.Equal(t, lastReservedValue, stats.LastSequenceReservedValue.Value())
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
	assertNewAllocatorStats(t, testStats, 1, 10, 1, 0, nextSequence, 10) // incr, reserved, assigned, released counts

	// Calling the same again should use from the existing batch
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assert.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 2, 0, nextSequence, 10)

	// Test case where greaterThan == s.Last + 1
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), nextSequence)
	require.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 3, 0, nextSequence, 10)

	// When requested nextSequenceGreaterThan is > s.Last + 1, we should release previously allocated sequences but
	// don't require a new incr
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 5)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), nextSequence)
	require.Equal(t, 2, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 1, 10, 4, 2, nextSequence, 10)

	// Test when requested nextSequenceGreaterThan == s.Max; should release previously allocated sequences and allocate a new batch
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), nextSequence)
	assert.Equal(t, 4, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 2, 20, 5, 6, nextSequence, 20)

	// Test when requested nextSequenceGreaterThan = s.Max + 1; should release previously allocated sequences AND max+1
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 21)
	assert.NoError(t, err)
	assert.Equal(t, uint64(22), nextSequence)
	assert.Equal(t, 10, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 3, 31, 6, 16, nextSequence, 31)

	// Test when requested nextSequenceGreaterThan > s.Max + batch size; should release 9 previously allocated sequences (23-31)
	// and 19 in the gap to the requested sequence (32-50)
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 50)
	assert.NoError(t, err)
	assert.Equal(t, uint64(51), nextSequence)
	assert.Equal(t, 28, int(releasedSequenceCount))
	assertNewAllocatorStats(t, testStats, 4, 60, 7, 44, nextSequence, 60)

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
	assertNewAllocatorStats(t, dbStatsA, 1, 10, 1, 0, nextSequence, 10) // incr, reserved, assigned, released counts

	// nextSequenceGreaterThan(0) on B should perform initial batch allocation of size 10 (allocs 11-20),  and not release any sequences
	nextSequence, releasedSequenceCount, err = b.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), nextSequence)
	assert.Equal(t, 0, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsB, 1, 10, 1, 0, nextSequence, 20)

	// calling nextSequenceGreaterThan(15) on B will assign from the existing batch, and release 12-15
	nextSequence, releasedSequenceCount, err = b.nextSequenceGreaterThan(ctx, 15)
	assert.NoError(t, err)
	assert.Equal(t, uint64(16), nextSequence)
	assert.Equal(t, 4, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsB, 1, 10, 2, 4, nextSequence, 20)

	// calling nextSequenceGreaterThan(15) on A will increment _sync:seq by 5 on it's previously allocated sequence (10).
	// Since node B has already updated _sync:seq to 20, calling nextSequenceGreaterThan(15) on A will result in:
	//   node A releasing sequences 2-10 from it's existing buffer
	//   node A adding sequences 21-30 to its buffer, and assigning 21 to the current request
	nextSequence, releasedSequenceCount, err = a.nextSequenceGreaterThan(ctx, 15)
	assert.NoError(t, err)
	assert.Equal(t, uint64(21), nextSequence)
	assert.Equal(t, 9, int(releasedSequenceCount))
	assertNewAllocatorStats(t, dbStatsA, 2, 20, 2, 9, nextSequence, 30)

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

	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           testStats,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50), // no sequence release monitor
		metaKeys:          base.DefaultMetadataKeys,
	}

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
	assert.Equal(t, uint64(521), nxtSeq)
	assert.Equal(t, uint64(530), a.max)

	// alter _sync:seq in bucket to end seq in prev batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 10)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 530

	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1041), nxtSeq)
	assert.Equal(t, uint64(1050), a.max)

	// alter _sync:seq in bucket to start seq in batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 521)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 1050

	// triggers correction value increase to 10 (sequence batch value) thus nextSeq is higher than you would expect
	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1561), nxtSeq)
	assert.Equal(t, uint64(1570), a.max)

	// rollback _sync:seq in bucket to start seq to seq outside of batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 1570

	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(2081), nxtSeq)
	assert.Equal(t, uint64(2090), a.max)
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

	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           testStats,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50), // no sequence release monitor
		metaKeys:          base.DefaultMetadataKeys,
	}

	// allocate something
	nxtSeq, err := a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nxtSeq)

	// alter _sync:seq in bucket to 5
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	// alter s.last to mock sequences being allocated
	a.last = 10

	// nextSequenceGreaterThan fetches current _sync:seq, detects rollback and adjusts by
	// delta from last allocated (5) + syncSeqCorrectionValue (500)
	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 15)
	require.NoError(t, err)

	assert.Equal(t, 521, int(nxtSeq))
	assert.Equal(t, 521, int(a.last))
	assert.Equal(t, 530, int(a.max))

	// alter _sync:seq in bucket to end seq in prev batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 10)
	require.NoError(t, err)

	// nextSequenceGreaterThan fetches current _sync:seq, detects rollback and adjusts by
	// delta from last allocated (530-10=520) + syncSeqCorrectionValue (500) + batch size (10)
	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 535)
	require.NoError(t, err)
	assert.Equal(t, uint64(1041), nxtSeq)
	assert.Equal(t, uint64(1041), a.last)
	assert.Equal(t, uint64(1050), a.max)

	// alter _sync:seq in bucket to start seq in prev batch
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 1041)
	require.NoError(t, err)

	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 1055)
	require.NoError(t, err)
	assert.Equal(t, 1561, int(nxtSeq))
	assert.Equal(t, 1561, int(a.last))
	assert.Equal(t, 1570, int(a.max))

	// alter _sync:seq in bucket to prev batch value
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	nxtSeq, _, err = a.nextSequenceGreaterThan(ctx, 1575)
	require.NoError(t, err)
	assert.Equal(t, uint64(2081), nxtSeq)
	assert.Equal(t, uint64(2081), a.last)
	assert.Equal(t, uint64(2090), a.max)
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
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}

	b := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsB,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}

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
		_, _, err := b.nextSequenceGreaterThan(ctx, 20)
		assert.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, _, err := a.nextSequenceGreaterThan(ctx, 10)
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

// TestFiveNodeRollbackMiddleNodesDetects:
//   - Mock 5 nodes each with their allocation of sequence batches
//   - Rollback the _sync:seq document to 5 then mock node c getting to end of sequence batch
//   - Trigger new allocation of batch on that node, and assert the _sync:seq document is corrected
//   - Trigger new batch allocation on node a and assert that batch is unique
func TestFiveNodeRollbackMiddleNodesDetects(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	statsA, err := stats.NewDBStats("A", false, false, false, nil, nil)
	require.NoError(t, err)
	statsB, err := stats.NewDBStats("B", false, false, false, nil, nil)
	require.NoError(t, err)
	statsC, err := stats.NewDBStats("C", false, false, false, nil, nil)
	require.NoError(t, err)
	statsD, err := stats.NewDBStats("D", false, false, false, nil, nil)
	require.NoError(t, err)
	statsE, err := stats.NewDBStats("E", false, false, false, nil, nil)
	require.NoError(t, err)
	dbStatsA := statsA.DatabaseStats
	dbStatsB := statsB.DatabaseStats
	dbStatsC := statsC.DatabaseStats
	dbStatsD := statsD.DatabaseStats
	dbStatsE := statsE.DatabaseStats
	ds := bucket.GetSingleDataStore()

	a := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsA,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}
	b := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsB,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}
	c := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsC,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}
	d := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsD,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}
	e := &sequenceAllocator{
		datastore:         bucket.GetSingleDataStore(),
		dbStats:           dbStatsE,
		sequenceBatchSize: 10,
		reserveNotify:     make(chan struct{}, 50),
		metaKeys:          base.DefaultMetadataKeys,
	}

	nextSequence, _, err := a.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), nextSequence)
	nextSequence, _, err = b.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), nextSequence)
	nextSequence, _, err = c.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(21), nextSequence)
	nextSequence, _, err = d.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(31), nextSequence)
	nextSequence, _, err = e.nextSequenceGreaterThan(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(41), nextSequence)

	// alter _sync:seq in bucket to rolled back value
	err = ds.Set(a.metaKeys.SyncSeqKey(), 0, nil, 5)
	require.NoError(t, err)

	// mock node c getting to end of batch
	c.last = 30

	// trigger new batch allocation for node c, should correct the rollback by the minimum the node expect
	// sync:seq to be + 500
	nxtSeq, err := c.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(541), nxtSeq)
	assert.Equal(t, uint64(541), c.last)
	assert.Equal(t, uint64(550), c.max)

	// mock a getting to end of batch and trigger new batch allocation, assert it continues from corrected value
	a.last = 10
	nxtSeq, err = a.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(551), nxtSeq)
	assert.Equal(t, uint64(551), a.last)
	assert.Equal(t, uint64(560), a.max)
}

// TestVariableRateAllocators simulates the following scenario:
//   - import nodes have high sequence allocation rate
//   - client-facing nodes have low sequence allocation rate
//   - documents are imported, then the same documents are immediately updated by clients
//     (including sequence validation triggering nextSequenceGreaterThan)
//
// Ensures we don't release more sequences than would be expected based on allocator batch size
func TestVariableRateAllocators(t *testing.T) {
	base.LongRunningTest(t)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	var expectedAllocations uint64

	dataStore := bucket.GetSingleDataStore()
	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	importStats, err := stats.NewDBStats("import", false, false, false, nil, nil)
	require.NoError(t, err)

	importFeedAllocator, err := newSequenceAllocator(ctx, dataStore, importStats.DatabaseStats, base.DefaultMetadataKeys)
	require.NoError(t, err)

	// All test allocators are stopped when allocatorCtx is closed
	allocatorCtx, cancelFunc := context.WithCancel(ctx)

	// Start import node allocator, performing 10000 allocations/second.
	var allocatorWg sync.WaitGroup
	allocatorWg.Add(1)
	go func() {
		count := runAllocator(allocatorCtx, importFeedAllocator, 100*time.Microsecond) // 10000 writes/second
		atomic.AddUint64(&expectedAllocations, count)
		allocatorWg.Done()
	}()

	// Start multiple client node allocators, performing 100 allocations/second
	clientAllocators := make([]*sequenceAllocator, 0)
	clientAllocatorCount := 10
	for i := 0; i <= clientAllocatorCount; i++ {
		clientStats, err := stats.NewDBStats(fmt.Sprintf("client%d", i), false, false, false, nil, nil)
		require.NoError(t, err)
		clientAllocator, err := newSequenceAllocator(ctx, dataStore, clientStats.DatabaseStats, base.DefaultMetadataKeys)
		require.NoError(t, err)
		clientAllocators = append(clientAllocators, clientAllocator)
		allocatorWg.Add(1)
		go func() {
			count := runAllocator(allocatorCtx, clientAllocator, 10*time.Millisecond) // 100 writes/second
			atomic.AddUint64(&expectedAllocations, count)
			allocatorWg.Done()
		}()
	}

	// Wait for allocators to get up to maximum batch size
	time.Sleep(500 * time.Millisecond)
	documentCount := 10
	var updateWg sync.WaitGroup
	updateWg.Add(documentCount)
	for range documentCount {
		go func() {
			_ = multiNodeUpdate(t, ctx, importFeedAllocator, clientAllocators, 5, 10*time.Millisecond)
			updateWg.Done()
			atomic.AddUint64(&expectedAllocations, 6)
		}()
	}

	updateWg.Wait()

	// Stop background allocation goroutines, wait for them to close
	cancelFunc()
	allocatorWg.Wait()

	log.Printf("expectedSequence (num allocations):%v", atomic.LoadUint64(&expectedAllocations))

	importFeedAllocator.Stop(ctx)
	numAssigned := importFeedAllocator.dbStats.SequenceAssignedCount.Value()
	numReleased := importFeedAllocator.dbStats.SequenceReleasedCount.Value()
	for _, allocator := range clientAllocators {
		allocator.Stop(ctx)
		numAssigned += allocator.dbStats.SequenceAssignedCount.Value()
		clientSequencesReleased := allocator.dbStats.SequenceReleasedCount.Value()
		numReleased += clientSequencesReleased

	}

	log.Printf("Total sequences released + assigned: %v", numReleased+numAssigned)
	actualSequence, err := importFeedAllocator.getSequence()
	log.Printf("actual sequence (getSequence): %v", actualSequence)
	require.NoError(t, err)
}

// multiNodeUpdate obtains an initial sequence from an import allocator (import node), then performs repeated updates to the doc using random pool of iterators (random SG node).
// Performs sequenceGreaterThan, then ensures that allocator doesn't release more than the sequence batch size
func multiNodeUpdate(t *testing.T, ctx context.Context, importAllocator *sequenceAllocator, clientAllocators []*sequenceAllocator, updateCount int, interval time.Duration) (releasedCount uint64) {
	currentSequence, _ := importAllocator.nextSequence(ctx)

	for range updateCount {
		allocatorIndex := rand.Intn(len(clientAllocators))
		clientAllocator := clientAllocators[allocatorIndex]
		nextSequence, err := clientAllocator.nextSequence(ctx)
		require.NoError(t, err, "nextSequence error: %v", err)
		if nextSequence < currentSequence {
			prevNext := nextSequence
			var numReleased uint64
			nextSequence, numReleased, err = clientAllocator.nextSequenceGreaterThan(ctx, currentSequence)
			require.NoError(t, err, "nextSequenceGreaterThan error: %v", err)
			log.Printf("allocator %d released %d sequences because next < current (%d < %d)", numReleased, allocatorIndex, prevNext, currentSequence)
			// At most clientAllocator should only need to release the current batch
			assert.LessOrEqual(t, numReleased, getClientSequenceBatchSize(clientAllocator))
			releasedCount += numReleased
		}
		currentSequence = nextSequence
		time.Sleep(interval)
	}

	return releasedCount
}

func runAllocator(ctx context.Context, a *sequenceAllocator, frequency time.Duration) (allocationCount uint64) {

	allocationCount = 0
	ticker := time.NewTicker(frequency)
	for {
		select {
		case <-ticker.C:
			_, _ = a.nextSequence(ctx)
			allocationCount++
		case <-ctx.Done():
			ticker.Stop()
			log.Printf("allocator count: %v", allocationCount)
			return allocationCount
		}
	}
}

func getClientSequenceBatchSize(allocator *sequenceAllocator) uint64 {
	allocator.mutex.Lock()
	defer allocator.mutex.Unlock()
	return allocator.sequenceBatchSize
}
