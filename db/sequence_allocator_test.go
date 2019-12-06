package db

import (
	"expvar"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func DisableTestSequenceAllocator(t *testing.T) {

	testBucket := testBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket
	testStats := new(expvar.Map).Init()

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
	assertAllocatorStats(t, testStats, 1, 1, 1, 0)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertAllocatorStats(t, testStats, 2, 3, 2, 0)

	// Subsequent allocation shouldn't trigger allocation
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), nextSequence)
	assertAllocatorStats(t, testStats, 2, 3, 3, 0)

	// Subsequent allocation should increase batch to 4, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), nextSequence)
	assert.Equal(t, 4, int(a.sequenceBatchSize))
	assertAllocatorStats(t, testStats, 3, 7, 4, 0)

	// Release unused sequences.  Should reduce batch size to 1 (based on 3 unused)
	a.releaseUnusedSequences()
	assertAllocatorStats(t, testStats, 3, 7, 4, 3)
	assert.Equal(t, 1, int(a.sequenceBatchSize))

	// Subsequent allocation should increase batch to 2, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), nextSequence)
	assertAllocatorStats(t, testStats, 4, 9, 5, 3)
	assert.Equal(t, 2, int(a.sequenceBatchSize))

}

func DisableTestReleaseSequencesOnStop(t *testing.T) {

	testBucket := testBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket
	testStats := new(expvar.Map).Init()

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
	assertAllocatorStats(t, testStats, 1, 1, 1, 0)

	// Subsequent allocation should increase batch size to 2, allocate 1
	nextSequence, err = a.nextSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), nextSequence)
	assertAllocatorStats(t, testStats, 2, 3, 2, 0)

	// Stop the allocator
	a.Stop()

	releasedCount := 0
	// Ensure unused sequence is released on Stop
	for i := 0; i < 20; i++ {
		releasedCount = getAllocatorStat(testStats, base.StatKeySequenceReleasedCount)
		if releasedCount == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, 1, releasedCount, "Expected 1 released sequence")
	assertAllocatorStats(t, testStats, 2, 3, 2, 1)

}

func assertAllocatorStats(t *testing.T, stats *expvar.Map, incr, reserved, assigned, released int) {
	assertAllocatorStat(t, stats, base.StatKeySequenceIncrCount, incr)
	assertAllocatorStat(t, stats, base.StatKeySequenceReservedCount, reserved)
	assertAllocatorStat(t, stats, base.StatKeySequenceAssignedCount, assigned)
	assertAllocatorStat(t, stats, base.StatKeySequenceReleasedCount, released)
}

func getAllocatorStat(stats *expvar.Map, key string) (value int) {
	varValue := stats.Get(key)
	if varValue == nil {
		return 0
	}
	value, _ = strconv.Atoi(varValue.String())
	return value
}

func assertAllocatorStat(t *testing.T, stats *expvar.Map, key string, expectedValue int) {
	value := getAllocatorStat(stats, key)
	assert.Equal(t, expectedValue, value, "Incorrect value for %s", key)
}
