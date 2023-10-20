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

func Test_findSequenceRanges(t *testing.T) {
	tests := []struct {
		name      string
		sequences []uint64
		expected  []seqRange
		error     string
	}{
		{
			name:      "empty",
			sequences: []uint64{},
			expected:  []seqRange{},
		},
		{
			name:      "single",
			sequences: []uint64{1},
			expected:  []seqRange{{1, 1}},
		},
		{
			name:      "single gap",
			sequences: []uint64{1, 3},
			expected:  []seqRange{{1, 1}, {3, 3}},
		},
		{
			name:      "single range",
			sequences: []uint64{1, 2, 3},
			expected:  []seqRange{{1, 3}},
		},
		{
			name:      "multiple ranges",
			sequences: []uint64{1, 2, 3, 5, 6, 7, 9, 10, 11},
			expected:  []seqRange{{1, 3}, {5, 7}, {9, 11}},
		},
		{
			name:      "multiple ranges and singles",
			sequences: []uint64{1, 2, 3, 5, 7, 9, 10, 11, 13},
			expected:  []seqRange{{1, 3}, {5, 5}, {7, 7}, {9, 11}, {13, 13}},
		},
		{
			name:      "out of order",
			sequences: []uint64{1, 2, 5, 4, 3},
			error:     "input not ordered",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := findSequenceRanges(tt.sequences)
			if tt.error == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.error)
			}
			assert.Equalf(t, tt.expected, actual, "findSequenceRanges(%v)", tt.sequences)
		})
	}
}
