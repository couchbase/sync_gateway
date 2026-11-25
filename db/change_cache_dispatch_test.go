//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db/dispatchtracker"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// dispatchTestCache is a started change cache on its own database, for the dispatch tests in this file.
type dispatchTestCache struct {
	ctx          context.Context
	cache        *changeCache
	collectionID uint32
}

// startDispatchTestCache starts a change cache whose next expected sequence is nextSequence. setUp, if not nil, runs
// between Init and Start, where the dispatch limits and options can still be changed.
func startDispatchTestCache(t *testing.T, nextSequence uint64, options *CacheOptions, setUp func(*changeCache)) *dispatchTestCache {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		dbContext.Close(ctx)
	})

	cache := &changeCache{}
	require.NoError(t, cache.Init(ctx, dbContext, dbContext.channelCache, options, dbContext.MetadataKeys))
	if setUp != nil {
		setUp(cache)
	}
	require.NoError(t, cache.Start(nextSequence-1))
	t.Cleanup(func() {
		cache.Stop(ctx)
	})
	return &dispatchTestCache{ctx: ctx, cache: cache, collectionID: GetSingleDatabaseCollection(t, dbContext).GetCollectionID()}
}

// docEntry returns a document entry at seq in channel A.
func (tc *dispatchTestCache) docEntry(seq uint64) *LogEntry {
	return MakeLogEntry(seq, fmt.Sprintf("doc%d", seq), "1-a", []string{"A"}, tc.collectionID)
}

// TestDispatchReleasedAdvancesSequences covers which sequence a lock hold's dispatch moves the high cache sequence and
// publish cursor to when the released entries have no channels to insert into. Getting the high sequence wrong
// stalls changes feeds; getting the cursor wrong gives a channel cache created afterwards a looser validFrom than the
// released sequences justify. It stays in package db to reach the unused-sequence and principal entry points.
func TestDispatchReleasedAdvancesSequences(t *testing.T) {
	// requireSequences checks the high cache sequence, the publish cursor, and the validFrom of a channel cache
	// created after the release.
	requireSequences := func(t *testing.T, tc *dispatchTestCache, wantHigh uint64) {
		assert.Equal(t, wantHigh, tc.cache.channelCache.GetHighCacheSequence(), "high cache sequence")
		impl, ok := tc.cache.channelCache.(*channelCacheImpl)
		require.True(t, ok)
		assert.Equal(t, wantHigh, impl.publishCursor.Load(), "publish cursor")

		single, err := tc.cache.channelCache.getSingleChannelCache(tc.ctx, channels.NewID("A", tc.collectionID))
		require.NoError(t, err)
		singleImpl, ok := single.(*singleChannelCacheImpl)
		require.True(t, ok)
		validFrom, _ := singleImpl.GetCachedChanges(ChangesOptions{})
		assert.Equal(t, wantHigh+1, validFrom, "validFrom of a cache created after the release")
	}

	t.Run("unused range advances to its end sequence", func(t *testing.T) {
		tc := startDispatchTestCache(t, 5, nil, nil)
		tc.cache.releaseUnusedSequenceRange(tc.ctx, 5, 9, channels.NewFeedTimestampFromNow())
		requireSequences(t, tc, 9)
	})

	t.Run("unused sequence advances to itself", func(t *testing.T) {
		tc := startDispatchTestCache(t, 5, nil, nil)
		tc.cache.releaseUnusedSequence(tc.ctx, 5, channels.NewFeedTimestampFromNow())
		requireSequences(t, tc, 5)
	})

	t.Run("principal advances to itself", func(t *testing.T) {
		tc := startDispatchTestCache(t, 7, nil, nil)
		tc.cache.processEntry(tc.ctx, &LogEntry{
			Sequence:     7,
			DocID:        "_user/alice",
			IsPrincipal:  true,
			TimeReceived: channels.NewFeedTimestampFromNow(),
		})
		requireSequences(t, tc, 7)
	})

	// A range that fills the gap in front of a pending document releases both in one hold. Both sequences have to
	// reach the highest entry released, not stop at the end of the range that triggered the release.
	t.Run("release through pending advances to the highest entry", func(t *testing.T) {
		tc := startDispatchTestCache(t, 5, nil, nil)
		tc.cache.processEntry(tc.ctx, MakeLogEntry(8, "doc8", "1-a", []string{"B"}, tc.collectionID))
		require.Equal(t, uint64(4), tc.cache.channelCache.GetHighCacheSequence(), "doc 8 must wait in pending for 5-7")

		tc.cache.releaseUnusedSequenceRange(tc.ctx, 5, 7, channels.NewFeedTimestampFromNow())
		requireSequences(t, tc, 8)
	})
}

// TestDispatchRunsAndBudget covers how a lock hold's release is cut into runs and limited to its budget of one run for
// each free tracker slot. A cascade the budget cuts short must be resumed by whatever hold comes next, even one that
// releases nothing of its own, or the rest of the cascade waits for the pending time-out although it has arrived.
func TestDispatchRunsAndBudget(t *testing.T) {
	// requireState checks, after a hold, the next expected sequence, the number of entries left in pending, and the
	// number of runs dispatched so far. Every run retires within its hold, so the watermark has to have reached
	// everything released.
	requireState := func(t *testing.T, tc *dispatchTestCache, wantNext uint64, wantPending int, wantRuns uint64, msg string) {
		t.Helper()
		c := tc.cache
		c.lock.RLock()
		defer c.lock.RUnlock()
		assert.Equal(t, wantNext, c.nextSequence, "next sequence: %s", msg)
		assert.Len(t, c.pendingLogs, wantPending, "pending: %s", msg)
		assert.Equal(t, wantRuns+1, c.nextTicket, "next ticket: %s", msg)
		assert.Equal(t, c.nextTicket, c.tracker.Head(), "every run must have retired: %s", msg)
		assert.Equal(t, wantNext-1, c.channelCache.GetHighCacheSequence(), "watermark: %s", msg)
	}
	limits := func(runSize int, capacity uint64) func(*changeCache) {
		return func(c *changeCache) {
			c.SetDispatchLimitsForTest(t, runSize, capacity)
		}
	}

	t.Run("a release is cut into runs of at most the run size", func(t *testing.T) {
		tc := startDispatchTestCache(t, 1, nil, limits(3, 16))
		for seq := uint64(2); seq <= 7; seq++ {
			tc.cache.processEntry(tc.ctx, tc.docEntry(seq))
		}
		requireState(t, tc, 1, 6, 0, "2-7 wait for 1")

		tc.cache.processEntry(tc.ctx, tc.docEntry(1))
		requireState(t, tc, 8, 0, 3, "1-7 are released as runs of 3, 3 and 1")
	})

	t.Run("a cascade over the budget is resumed by later holds", func(t *testing.T) {
		// Two runs of two, so a hold may release four entries.
		tc := startDispatchTestCache(t, 1, nil, limits(2, 2))
		for seq := uint64(2); seq <= 10; seq++ {
			tc.cache.processEntry(tc.ctx, tc.docEntry(seq))
		}
		requireState(t, tc, 1, 9, 0, "2-10 wait for 1")

		tc.cache.processEntry(tc.ctx, tc.docEntry(1))
		requireState(t, tc, 5, 6, 2, "the hold releasing 1 stops after 4 entries")
		assert.Equal(t, uint64(1), tc.cache.numPartialDrains)

		// A duplicate releases nothing itself, but its hold resumes the drain.
		tc.cache.processEntry(tc.ctx, tc.docEntry(1))
		requireState(t, tc, 9, 2, 4, "the duplicate's hold releases 5-8")
		assert.Equal(t, uint64(2), tc.cache.numPartialDrains)

		// So does an unused range that was never pending, and this time the budget is enough.
		tc.cache.releaseUnusedSequenceRange(tc.ctx, 1, 2, channels.NewFeedTimestampFromNow())
		requireState(t, tc, 11, 0, 5, "the range's hold releases 9 and 10")
		assert.Equal(t, uint64(2), tc.cache.numPartialDrains, "a hold that empties pending did not run out of budget")
	})

	t.Run("a cascade over the budget is resumed by later arrivals", func(t *testing.T) {
		// The same cascade as above, resumed by ordinary new documents. Each one lands in pending behind a gap,
		// which doesn't drain pending itself, so only the resume releases what is left of the cascade.
		tc := startDispatchTestCache(t, 1, nil, limits(2, 2))
		for seq := uint64(2); seq <= 10; seq++ {
			tc.cache.processEntry(tc.ctx, tc.docEntry(seq))
		}
		tc.cache.processEntry(tc.ctx, tc.docEntry(1))
		requireState(t, tc, 5, 6, 2, "the hold releasing 1 stops after 4 entries")
		assert.Equal(t, uint64(1), tc.cache.numPartialDrains)

		// 11 hasn't arrived, so 12 waits in pending, and its hold releases 5-8.
		tc.cache.processEntry(tc.ctx, tc.docEntry(12))
		requireState(t, tc, 9, 3, 4, "12's hold releases 5-8, leaving 9, 10 and 12")
		assert.Equal(t, uint64(2), tc.cache.numPartialDrains)

		// 13 waits too. Its hold releases 9 and 10, and the drain stops at the gap, not the budget.
		tc.cache.processEntry(tc.ctx, tc.docEntry(13))
		requireState(t, tc, 11, 2, 5, "13's hold releases 9 and 10, then waits for 11")
		assert.Equal(t, uint64(2), tc.cache.numPartialDrains, "a drain stopped by a gap did not run out of budget")

		tc.cache.processEntry(tc.ctx, tc.docEntry(11))
		requireState(t, tc, 14, 0, 7, "11 releases itself, 12 and 13 as two runs")
		assert.Equal(t, uint64(2), tc.cache.numPartialDrains)
	})

	t.Run("the background release is limited to the budget", func(t *testing.T) {
		tc := startDispatchTestCache(t, 1, nil, func(c *changeCache) {
			c.SetDispatchLimitsForTest(t, 1, 2)
			// Init has already started the background task on the default interval, so it won't run during the
			// test. With no max wait, each call below releases, and skips whatever is missing.
			c.options.CachePendingSeqMaxWait = 0
		})
		for seq := uint64(2); seq <= 6; seq++ {
			tc.cache.processEntry(tc.ctx, tc.docEntry(seq))
		}
		// 1 never arrives. Each call skips it if needed and releases two entries.
		require.NoError(t, tc.cache.InsertPendingEntries(tc.ctx))
		requireState(t, tc, 4, 3, 2, "the first background hold skips 1 and releases 2 and 3")
		require.NoError(t, tc.cache.InsertPendingEntries(tc.ctx))
		requireState(t, tc, 6, 1, 4, "the second releases 4 and 5")
		require.NoError(t, tc.cache.InsertPendingEntries(tc.ctx))
		requireState(t, tc, 7, 0, 5, "the third releases 6")
		assert.Equal(t, uint64(0), tc.cache.GetMaxStableCachedForTest(t, tc.ctx), "1 is skipped, so nothing is stable")
	})
}

// TestCutRuns covers how a lock hold's released entries are cut into runs: how many runs, which entries each holds,
// its ticket, and where each lets the watermark move. Every released entry must be in exactly one run, in release
// order.
func TestCutRuns(t *testing.T) {
	const firstTicket = 7

	// wantRun is a run's expected entries, by sequence, and releasedThrough.
	type wantRun struct {
		seqs            []uint64
		releasedThrough uint64
	}
	for _, test := range []struct {
		name     string
		runSize  int
		released []*LogEntry
		want     []wantRun
	}{
		{
			name:    "nothing released cuts no runs",
			runSize: 3,
		},
		{
			name:     "fewer than a run",
			runSize:  3,
			released: docEntries(10, 11),
			want:     []wantRun{{seqs: []uint64{10, 11}, releasedThrough: 11}},
		},
		{
			name:     "exactly one run",
			runSize:  3,
			released: docEntries(10, 12),
			want:     []wantRun{{seqs: []uint64{10, 11, 12}, releasedThrough: 12}},
		},
		{
			name:     "one past a run",
			runSize:  3,
			released: docEntries(10, 13),
			want: []wantRun{
				{seqs: []uint64{10, 11, 12}, releasedThrough: 12},
				{seqs: []uint64{13}, releasedThrough: 13},
			},
		},
		{
			name:     "exactly two runs",
			runSize:  3,
			released: docEntries(10, 15),
			want: []wantRun{
				{seqs: []uint64{10, 11, 12}, releasedThrough: 12},
				{seqs: []uint64{13, 14, 15}, releasedThrough: 15},
			},
		},
		{
			name:     "two runs and a partial run",
			runSize:  3,
			released: docEntries(10, 16),
			want: []wantRun{
				{seqs: []uint64{10, 11, 12}, releasedThrough: 12},
				{seqs: []uint64{13, 14, 15}, releasedThrough: 15},
				{seqs: []uint64{16}, releasedThrough: 16},
			},
		},
		{
			name:     "a run size of one",
			runSize:  1,
			released: docEntries(10, 12),
			want: []wantRun{
				{seqs: []uint64{10}, releasedThrough: 10},
				{seqs: []uint64{11}, releasedThrough: 11},
				{seqs: []uint64{12}, releasedThrough: 12},
			},
		},
		{
			name:     "an inner run ending in an unused range moves the watermark to the range's end",
			runSize:  2,
			released: []*LogEntry{{Sequence: 10}, {Sequence: 11, EndSequence: 14}, {Sequence: 15}},
			want: []wantRun{
				{seqs: []uint64{10, 11}, releasedThrough: 14},
				{seqs: []uint64{15}, releasedThrough: 15},
			},
		},
		{
			name:     "an unused range released from skipped ends an inner run at a run size of one",
			runSize:  1,
			released: []*LogEntry{{Sequence: 3, EndSequence: 6, UnusedSequence: true}, {Sequence: 20}, {Sequence: 21}},
			want: []wantRun{
				{seqs: []uint64{3}, releasedThrough: 6},
				{seqs: []uint64{20}, releasedThrough: 20},
				{seqs: []uint64{21}, releasedThrough: 21},
			},
		},
		{
			name:     "a late sequence ends an inner run at a run size of one",
			runSize:  1,
			released: []*LogEntry{{Sequence: 3, Skipped: true}, {Sequence: 20}, {Sequence: 21}},
			want: []wantRun{
				{seqs: []uint64{3}, releasedThrough: 3},
				{seqs: []uint64{20}, releasedThrough: 20},
				{seqs: []uint64{21}, releasedThrough: 21},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			nextSequence := uint64(10)
			if len(test.released) > 0 {
				last := test.released[len(test.released)-1]
				nextSequence = max(last.Sequence, last.EndSequence) + 1
			}
			for _, entry := range test.released {
				nextSequence = max(nextSequence, entry.Sequence+1)
			}
			c := &changeCache{
				runSize:      test.runSize,
				nextTicket:   firstTicket,
				nextSequence: nextSequence,
				released:     test.released,
			}
			var runs []dispatchRun
			c._cutRuns(func(run dispatchRun) {
				runs = append(runs, run)
			})

			require.Len(t, runs, len(test.want), "number of runs")
			for i, run := range runs {
				seqs := make([]uint64, 0, len(run.entries))
				for _, entry := range run.entries {
					seqs = append(seqs, entry.Sequence)
				}
				assert.Equal(t, test.want[i].seqs, seqs, "run %d entries", i)
				assert.Equal(t, test.want[i].releasedThrough, run.releasedThrough, "run %d releasedThrough", i)
				assert.Equal(t, uint64(firstTicket+i), run.ticket, "run %d ticket", i)
			}
			assert.Equal(t, uint64(firstTicket+len(runs)), c.nextTicket, "one ticket per run")
		})
	}
}

// TestLateSequenceRemovedByItsRun covers late sequences through ordering, cutting and dispatch: a late arrival must
// leave skippedSeqs from the run that holds it, even when its hold also resumes a cut-short cascade over several runs. Removing it from any later run would, once runs finish out of
// order, let the stable sequence pass it before it is cached.
func TestLateSequenceRemovedByItsRun(t *testing.T) {
	// 1 and 2 are skipped, then arrive late, each in its own hold. Before they do, a cascade from 3 is cut short by
	// the budget, leaving numWaiting releasable entries for the late arrivals' holds to resume.
	lateSeqs := []uint64{1, 2}
	for _, test := range []struct {
		name       string
		runSize    int
		capacity   uint64
		numWaiting int
		wantRuns   []uint64 // runs dispatched by each late arrival's hold
	}{
		{name: "nothing to resume", runSize: 2, capacity: 2, numWaiting: 0, wantRuns: []uint64{1, 1}},
		{name: "resumed entries fit in the late sequence's run", runSize: 3, capacity: 1, numWaiting: 5, wantRuns: []uint64{1, 1}},
		{name: "resumed entries spill into later runs", runSize: 2, capacity: 2, numWaiting: 5, wantRuns: []uint64{2, 2}},
		{name: "a run size of one", runSize: 1, capacity: 4, numWaiting: 6, wantRuns: []uint64{4, 4}},
		{name: "only the first late arrival resumes", runSize: 2, capacity: 4, numWaiting: 3, wantRuns: []uint64{2, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// removedBy is the ticket of the run dispatching when each sequence left skippedSeqs. Dispatch is
			// synchronous, so that is the tracker's head: the run is retired only after its removals.
			removedBy := map[uint64]uint64{}
			tc := startDispatchTestCache(t, 1, nil, func(c *changeCache) {
				c.SetDispatchLimitsForTest(t, test.runSize, test.capacity)
				c.SetRemoveSkippedHookForTest(t, func(seq uint64) {
					_, dup := removedBy[seq]
					assert.False(t, dup, "%d left skippedSeqs twice", seq)
					removedBy[seq] = c.tracker.Head()
				})
			})
			c := tc.cache
			budget := test.runSize * int(test.capacity)
			// The cache reads its options under its lock, from the background task as well as these calls.
			setMaxPending := func(maxNum int) {
				c.lock.Lock()
				defer c.lock.Unlock()
				c.options.CachePendingSeqMaxNum = maxNum
			}

			// With no room in pending, 3 skips 1 and 2 at once.
			setMaxPending(0)
			c.processEntry(tc.ctx, tc.docEntry(3))
			require.True(t, c.WasSkipped(1) && c.WasSkipped(2))

			// 4 waits for 5 behind it, then arrives and releases a budget's worth, leaving numWaiting.
			setMaxPending(1000)
			lastSeq := uint64(4 + budget + test.numWaiting - 1)
			for seq := uint64(5); seq <= lastSeq; seq++ {
				c.processEntry(tc.ctx, tc.docEntry(seq))
			}
			c.processEntry(tc.ctx, tc.docEntry(4))
			numPending, _, _ := c.DispatchStateForTest(t)
			require.Equal(t, test.numWaiting, numPending, "left by the cascade")

			for i, seq := range lateSeqs {
				_, _, firstTicket := c.DispatchStateForTest(t)
				c.processEntry(tc.ctx, tc.docEntry(seq))
				_, _, nextTicket := c.DispatchStateForTest(t)

				assert.Equal(t, test.wantRuns[i], nextTicket-firstTicket, "runs dispatched by %d's hold", seq)
				assert.Equal(t, firstTicket, removedBy[seq], "%d must leave skippedSeqs from its hold's first run, which holds it", seq)
				assert.False(t, c.WasSkipped(seq))
			}
			numPending, _, _ = c.DispatchStateForTest(t)
			assert.Equal(t, max(0, test.numWaiting-2*(budget-1)), numPending, "each late arrival's hold resumes up to its budget")
		})
	}
}

// docEntries returns entries for from through to.
func docEntries(from, to uint64) []*LogEntry {
	entries := make([]*LogEntry, 0, to-from+1)
	for seq := from; seq <= to; seq++ {
		entries = append(entries, &LogEntry{Sequence: seq})
	}
	return entries
}

// TestGetMaxStableCachedFollowsWatermark covers the stable sequence reported to feeds: it must never pass the
// watermark, since an entry above the watermark may not be cached yet, nor the oldest skipped sequence, which may
// still arrive.
func TestGetMaxStableCachedFollowsWatermark(t *testing.T) {
	tc := startDispatchTestCache(t, 1, nil, nil)
	c := tc.cache
	// Ordering has moved past 1-10, but nothing has been cached, so the watermark is still at 0. Only the dispatch
	// split can leave the two apart; here the test sets them directly.
	c.orderAndDispatch(tc.ctx, func() { c.nextSequence = 11 })
	assert.Equal(t, uint64(0), c._getMaxStableCached(tc.ctx), "the watermark bounds the stable sequence")

	c.channelCache.updateHighCacheSequence(8)
	assert.Equal(t, uint64(8), c._getMaxStableCached(tc.ctx), "the stable sequence follows the watermark")

	c.PushSkipped(tc.ctx, 5, 5)
	assert.Equal(t, uint64(4), c._getMaxStableCached(tc.ctx), "a skipped sequence below the watermark bounds the stable sequence")

	require.NoError(t, c.RemoveSkipped(5))
	assert.Equal(t, uint64(8), c._getMaxStableCached(tc.ctx))
}

// TestChangeCacheTrackerCapacity covers the size of the completion tracker. The database config always sets it, but
// options built without the config leave it zero, and the cache must then size it from max pending as the config does.
func TestChangeCacheTrackerCapacity(t *testing.T) {
	sizedFor100K, err := dispatchtracker.CapacityFor(100_000)
	require.NoError(t, err)
	require.Greater(t, sizedFor100K, uint64(dispatchtracker.MinCapacity), "the case below must size above the minimum")

	for _, test := range []struct {
		name            string
		maxRunsInFlight uint64
		maxNumPending   int
		want            uint64
		wantErr         bool
	}{
		{name: "configured", maxRunsInFlight: 2048, maxNumPending: 10, want: 2048},
		{name: "unset, sized from max pending", maxNumPending: 100_000, want: sizedFor100K},
		{name: "unset, no max pending", want: dispatchtracker.MinCapacity},
		{name: "invalid", maxRunsInFlight: 1000, maxNumPending: 10, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			bucket := base.GetTestBucket(t)
			dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
				Scopes: GetScopesOptions(t, bucket, 1),
			})
			require.NoError(t, err)
			defer dbContext.Close(ctx)

			options := DefaultCacheOptions()
			options.MaxRunsInFlight = test.maxRunsInFlight
			options.CachePendingSeqMaxNum = test.maxNumPending
			cache := &changeCache{}
			err = cache.Init(ctx, dbContext, dbContext.channelCache, &options, dbContext.MetadataKeys)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, cache.Start(0))
			defer cache.Stop(ctx)
			assert.Equal(t, test.want, cache.tracker.Capacity())
			assert.Equal(t, dispatchtracker.RunSize, cache.runSize)
		})
	}
}

// TestPutNotifyBuffer covers the rules for returning a run's notify buffer to the pool: a pooled buffer must not keep
// channel names alive, and one grown past maxPooledNotifyBufferIDs is dropped rather than pinned in the pool.
func TestPutNotifyBuffer(t *testing.T) {
	t.Run("cleared before it is pooled", func(t *testing.T) {
		buf := make([]channels.ID, 2, 8)
		buf[0], buf[1] = channels.NewID("A", 1), channels.NewID("B", 1)
		putNotifyBuffer(&buf)
		assert.Empty(t, buf)
		assert.Equal(t, []channels.ID{{}, {}}, buf[:2], "the IDs must be zeroed, not only truncated")
	})

	t.Run("dropped when oversized", func(t *testing.T) {
		buf := make([]channels.ID, 1, maxPooledNotifyBufferIDs+1)
		buf[0] = channels.NewID("A", 1)
		putNotifyBuffer(&buf)
		// The buffer is left as it was, so the pool never got it: a pooled buffer is always cleared first.
		assert.Equal(t, []channels.ID{channels.NewID("A", 1)}, buf)
	})
}
