//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"container/heap"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestAddPendingLogs and its helpers stay in package db: the test holds changeCache.lock
// across a direct push onto the pendingLogs heap, _pushRangeToPending and _addPendingLogs.
// The underscore-prefixed methods require the caller to hold that lock, and a held lock
// cannot be expressed across a package boundary - a test accessor that took the lock itself
// would no longer exercise the buffering path this test pins down.

// TestAddPendingLogs:
//   - Test age-based eviction of sequences and ranges from pending logs.
//   - Adds to pending logs directly via heap.Push with backdated TimeReceived,
//     triggers eviction with call to _addPendingLogs
//   - tests duplicate handling when popping pending entries
//   - reproduces CBG-4215
func TestAddPendingLogs(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testCases := []struct {
		incoming             []sequenceRange // test simulates adding these to pending in the order found in the slice
		channelName          string          // non-range values (endSeq=0) will be assigned to this channel for verification of proper caching
		expectedCached       []uint64        // expected cached sequences
		expectedSkipped      []sequenceRange // expected skipped sequence ranges
		expectedNextSequence uint64          // expected cache.nextSequence
	}{
		{
			// single range
			incoming:             []sequenceRange{{2, 4}},
			expectedNextSequence: 5,
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// multiple non-overlapping ranges arriving out of sequence order
			incoming:             []sequenceRange{{2, 4}, {9, 10}, {6, 8}, {14, 20}, {11, 13}, {5, 5}},
			expectedNextSequence: 21,
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// non overlapping ranges, arrive in sequence order
			incoming:             []sequenceRange{{2, 4}, {5, 8}},
			expectedNextSequence: 9,
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// non overlapping ranges, arrive out of sequence order
			incoming:             []sequenceRange{{5, 8}, {2, 4}},
			expectedNextSequence: 9,
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// range overlaps single, range arrives first
			incoming:             []sequenceRange{{2, 4}, {3, 0}},
			channelName:          "A",
			expectedNextSequence: 4,
			expectedCached:       []uint64{3},
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// single arrives then range overlaps single both sides
			incoming:             []sequenceRange{{3, 3}, {2, 4}},
			expectedNextSequence: 4,
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// range arrives then another that completely covers range in the pending list
			incoming:             []sequenceRange{{4, 8}, {2, 10}},
			expectedNextSequence: 9,
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// overlapping ranges, low range arrives first
			incoming:             []sequenceRange{{4, 8}, {6, 10}},
			expectedNextSequence: 11,
			expectedSkipped:      []sequenceRange{{1, 3}},
		},
		{
			// completely overlapping ranges, larger range arrives first
			incoming:             []sequenceRange{{4, 8}, {6, 8}},
			expectedNextSequence: 9,
			expectedSkipped:      []sequenceRange{{1, 3}},
		},
		{
			// range arrives then partly overlapping range arrives
			incoming:             []sequenceRange{{4, 8}, {6, 10}},
			expectedNextSequence: 11,
			expectedSkipped:      []sequenceRange{{1, 3}},
		},
		{
			// range arrives, partly overlapping range arrives the single overlapping range
			incoming:             []sequenceRange{{4, 8}, {8, 10}, {9, 0}},
			channelName:          "B",
			expectedNextSequence: 10,
			expectedCached:       []uint64{9},
			expectedSkipped:      []sequenceRange{{1, 3}},
		},
		{
			// single range arrives, left side overlapping range arrives
			incoming:             []sequenceRange{{4, 0}, {2, 4}},
			expectedNextSequence: 5,
			expectedCached:       []uint64{4},
			expectedSkipped:      []sequenceRange{{1, 1}},
		},
		{
			// single sequence arrives, right side overlapping range arrives
			incoming:             []sequenceRange{{4, 0}, {4, 5}},
			channelName:          "C",
			expectedNextSequence: 6,
			expectedSkipped:      []sequenceRange{{1, 3}},
		},
		{
			// range arrives, lower end overlapping range arrives
			incoming:             []sequenceRange{{6, 10}, {4, 8}},
			expectedNextSequence: 11,
			expectedSkipped:      []sequenceRange{{1, 3}},
		},
	}

	for index, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", index), func(t *testing.T) {
			ctx := base.TestCtx(t)
			testChannelID := channels.NewID(testCase.channelName, GetSingleDatabaseCollection(t, dbContext).GetCollectionID())
			testChangeCache := &changeCache{}
			if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
				CachePendingSeqMaxWait: 1 * time.Minute,
				CacheSkippedSeqMaxWait: 20 * time.Minute,
				CachePendingSeqMaxNum:  10,
			}, dbContext.MetadataKeys); err != nil {
				log.Printf("Init failed for testChangeCache: %v", err)
				t.Fail()
			}

			if err := testChangeCache.Start(0); err != nil {
				log.Printf("Start error for testChangeCache: %v", err)
				t.Fail()
			}
			defer testChangeCache.Stop(ctx)
			require.NoError(t, err)

			// If we expect cached entries, perform a get to warm the cache for the channel
			if len(testCase.expectedCached) > 0 {
				cachedEntries, err := testChangeCache.getChannelCache().GetCachedChanges(ctx, testChannelID)
				require.NoError(t, err)
				require.Equal(t, 0, len(cachedEntries))
			}

			// process overlapping unused sequence ranges that should end up going to pending without duplicates
			// acquire cache lock to push to pending logs
			testChangeCache.lock.Lock()
			backdatedTimeReceived := channels.NewFeedTimestamp(base.Ptr(time.Now().Add(-1 * time.Hour)))
			for i, incomingRange := range testCase.incoming {
				if incomingRange.end == 0 {
					// treat as a document pushed to pending
					logEntry := MakeLogEntry(incomingRange.start, fmt.Sprintf("doc%d", i), "1-abc", []string{testChannelID.Name}, testChannelID.CollectionID)
					logEntry.TimeReceived = backdatedTimeReceived
					heap.Push(&testChangeCache.pendingLogs, logEntry)
				} else {
					testChangeCache._pushRangeToPending(incomingRange.start, incomingRange.end, backdatedTimeReceived)
				}
			}
			// Call _addPendingLogs to trigger eviction from pendingLogs based on age
			_ = testChangeCache._addPendingLogs(ctx)
			assert.Equal(t, int(testCase.expectedNextSequence), int(testChangeCache.nextSequence), "Cache nextSequence doesn't match expected")
			testChangeCache.lock.Unlock()
			if len(testCase.expectedCached) > 0 {
				cachedEntries, err := testChangeCache.getChannelCache().GetCachedChanges(ctx, testChannelID)
				require.NoError(t, err)
				require.Equal(t, len(testCase.expectedCached), len(cachedEntries))
			}
			if len(testCase.expectedSkipped) > 0 {
				require.Equal(t, len(testCase.expectedSkipped), testChangeCache.skippedSeqs.list.GetLength(), "Number of skipped sequence entries doesn't match expected")
				i := 0
				for c := testChangeCache.skippedSeqs.list.Front(); c != nil; c = c.Next() {
					expectedEntry := testCase.expectedSkipped[i]
					assert.Equal(t, expectedEntry.start, c.Key().Start, "skipped entry start mismatch")
					assert.Equal(t, expectedEntry.end, c.Key().End, "skipped entry end mismatch")
					i++
				}
			}

		})
	}

}

type sequenceRange struct {
	start uint64
	end   uint64
}
