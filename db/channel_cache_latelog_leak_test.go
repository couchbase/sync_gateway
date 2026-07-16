/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/require"
)

// startABCChangesFeed opens a real continuous _changes feed on channel ABC, exactly as a live client
// connection would via changes.go. The feed is canceled at test cleanup so its goroutine can always
// unblock and exit, even if its output is never read again.
func startABCChangesFeed(ctx context.Context, t *testing.T, collectionWithUser *DatabaseCollectionWithUser) <-chan *ChangeEntry {
	feedCtx, cancel := context.WithCancelCause(ctx)
	t.Cleanup(func() { cancel(errors.New("test teardown")) })
	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ChangesCtx: feedCtx,
		Continuous: true,
		Wait:       true,
	}
	feed, err := collectionWithUser.MultiChangesFeed(feedCtx, base.SetOf("ABC"), options)
	require.NoError(t, err, "feed initialization error")
	return feed
}

// TestNumEntriesInLateFeedLeaksWaitingForConsumptionOfChanges reproduces a leak in the channel
// cache's late-sequence bookkeeping (lateLogs) through real production code paths only - no direct
// calls into the channel cache. It:
//
//  1. Starts two real continuous _changes feeds on the same channel, both healthy.
//  2. Writes a skip/late-arrival cycle so both feeds register a late-sequence listener
//     (newLateSequenceFeed/getLateFeed in changes.go), then stops draining feed 2 - simulating a
//     client that's still connected but has stopped consuming its changes (e.g. a bad connection).
//     feed 2's MultiChangesFeed goroutine ends up permanently waiting for consumption of changes -
//     blocked sending to its own output channel - and can never loop back to release its
//     late-sequence bookmark.
//  3. Repeats the skip/late-arrival cycle many times, draining feed 1 normally throughout, and
//     asserts DbStats.Cache().NumEntriesInLateFeed never decreases between cycles - proving feed 2's
//     stuck bookmark permanently blocks the cache's purge, not just that growth outpaces cleanup.
//  4. Forces the change cache's other two housekeeping routines to run (CleanSkippedSequenceQueue,
//     InsertPendingEntries), interleaved with plain non-skipped writes, and asserts the count still
//     never drops - showing neither routine reclaims the leaked entry either.
//
// NumEntriesInLateFeed is asserted rather than the cache's internal lateLogs slice, to show the leak
// is observable through the same stat an operator would be looking at in production.
func TestNumEntriesInLateFeedLeaksWaitingForConsumptionOfChanges(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection := collectionWithUser.DatabaseCollection

	// drainUntilWait reads events until MultiChangesFeed sends its nil "caught up, waiting for the
	// next change" marker.
	drainUntilWait := func(feed <-chan *ChangeEntry) {
		for {
			select {
			case event := <-feed:
				if event == nil {
					return
				}
			case <-time.After(10 * time.Second):
				t.Fatal("expected event didn't arrive over feed")
				return
			}
		}
	}

	numEntriesInLateFeed := func() int64 { return db.DbStats.Cache().NumEntriesInLateFeed.Value() }

	feed1 := startABCChangesFeed(ctx, t, collectionWithUser) // stays healthy: drained after every write for the whole test
	feed2 := startABCChangesFeed(ctx, t, collectionWithUser) // drained through the first cycle only, then abandoned

	// writeSeq persists a sequence and waits for feed1 (the healthy feed) to catch up. feed2 is only
	// drained explicitly below, and only while it's still meant to be healthy.
	writeSeq := func(seq uint64) {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		drainUntilWait(feed1)
	}

	drainUntilWait(feed1)
	drainUntilWait(feed2)
	// Not necessarily 0: subscribing itself activates one or more channel caches (e.g. the "*" star
	// channel alongside "ABC"), and each new singleChannelCacheImpl starts with one sentinel entry in
	// lateLogs (initializeLateLogs) before any document is ever written.
	baseline := numEntriesInLateFeed()

	// Step 1: an in-order write - no skip involved yet, both feeds still healthy.
	writeSeq(1)
	drainUntilWait(feed2)

	// Step 2: write 3, skipping 2. Both feeds pick it up and, in doing so, register a late-sequence
	// listener on ABC (newLateSequenceFeed), since a gap is now open below their high sequence.
	writeSeq(3)
	drainUntilWait(feed2) // last time feed2 is ever drained - it's abandoned after this

	// Step 3: sequence 2 resolves late. This is the real production trigger for AddLateSequence:
	// changeCache.processEntry's out-of-order branch marks it Skipped and fans it out to every
	// channel the doc belongs to. feed2 is not drained again for the rest of the test.
	writeSeq(2)

	afterFirstCycle := numEntriesInLateFeed()
	require.Greater(t, afterFirstCycle, baseline,
		"the late-arriving sequence should have added at least one entry above baseline")

	// Step 4: repeat the skip/late-arrival cycle. feed1 keeps draining normally and releasing its own
	// bookmark; feed2 is never drained again. MultiChangesFeed's output channel is buffered (50 -
	// changes.go:739), so feed2's goroutine keeps looping and genuinely releasing its bookmark into
	// that unread buffer for a while - it only actually ends up waiting for consumption of changes
	// once the buffer fills, so this runs enough cycles to comfortably exceed it.
	//
	// Each cycle's late-resolution write triggers AddLateSequence, which always attempts a purge
	// (_purgeLateLogEntries, channel_cache_single.go:812) - so every cycle is a fresh chance to
	// reclaim old entries. Asserting the count never drops between cycles - not just that it's higher
	// at the end - is what proves those purge attempts are genuinely failing throughout, because
	// feed2's stuck listener permanently blocks them, rather than growth merely outpacing cleanup.
	const numCycles = 40
	seq := uint64(3)
	prevCount := afterFirstCycle
	for range numCycles {
		seq += 2
		writeSeq(seq)     // skips seq-1
		writeSeq(seq - 1) // resolves seq-1 late -> AddLateSequence -> purge attempt

		count := numEntriesInLateFeed()
		require.GreaterOrEqual(t, count, prevCount,
			"NumEntriesInLateFeed must never drop between cycles - a drop would mean this cycle's "+
				"purge attempt reclaimed feed2's stuck entry, which should never happen while feed2 "+
				"stays unreleased")
		prevCount = count
	}

	require.Greater(t, prevCount, afterFirstCycle,
		"NumEntriesInLateFeed should keep climbing across cycles while feed2 stays stuck waiting for "+
			"consumption of changes, even though feed1 is fully healthy and actively advancing on the "+
			"same channel throughout")

	t.Logf("num_entries_in_late_feed after first cycle: %d, after %d more cycles with feed2 waiting for consumption of changes: %d",
		afterFirstCycle, numCycles, prevCount)

	// Step 5: confirm the cache's other two housekeeping routines don't reclaim the leak either.
	// CleanSkippedSequenceQueue and the CachePendingSeqMaxWait-gated pending-log flush
	// (InsertPendingEntries) are the changeCache's two periodic background tasks
	// (change_cache.go:181-191), but they operate on entirely separate structures - skippedSeqs and
	// pendingLogs, respectively - not the channel cache's lateLogs. They're called directly here
	// rather than waiting on their real timers, since shortWaitCache sets CacheSkippedSeqMaxWait to a
	// full 2 minutes.
	countBeforeHousekeeping := prevCount
	seq++
	writeSeq(seq) // plain in-order write: nothing skipped, no AddLateSequence

	require.NoError(t, db.changeCache.InsertPendingEntries(ctx))
	require.NoError(t, db.changeCache.CleanSkippedSequenceQueue(ctx))

	for range 5 { // more plain sequential writes after the housekeeping routines have run
		seq++
		writeSeq(seq)
	}

	require.GreaterOrEqual(t, numEntriesInLateFeed(), countBeforeHousekeeping,
		"NumEntriesInLateFeed must not drop after plain writes plus a forced run of "+
			"CleanSkippedSequenceQueue and the pending-log flush - neither touches lateLogs, so "+
			"feed2's stuck entry survives them too")
}

// TestNumEntriesInLateFeedLeaksWaitingForSlowConsumptionOfChanges shows the same leak reproduces
// even when feed 2 is never abandoned and keeps reading - it's just drained far less often than
// feed 1, so there's a persistent (not momentary) gap between production and consumption.
//
// A version of this test where both feeds run as free-running background goroutines - feed 2
// throttled with a fixed per-read delay, writes fired continuously with no pacing - turned out to be
// unreliable and not worth keeping: MultiChangesFeed's outer loop only processes whatever accumulated
// since its last wake-up (currentCachedSequence is snapshotted fresh every time it wakes from
// changeWaiter.Wait(), changes.go:1204), so a consumer that's merely slower but still reads on every
// wake-up self-corrects at any paced write rate - it took an extreme, unpaced, ~200,000-write burst to
// show any effect at all, and even that was a transient bump, not a clean reproduction. Explicitly
// controlling how often feed 2 is drained relative to real cache-processing ticks - by pacing writes
// off of feed 1's drain, exactly like TestNumEntriesInLateFeedLeaksWaitingForConsumptionOfChanges -
// reproduces the same leak cheaply and deterministically instead.
func TestNumEntriesInLateFeedLeaksWaitingForSlowConsumptionOfChanges(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection := collectionWithUser.DatabaseCollection

	drainUntilWait := func(feed <-chan *ChangeEntry) {
		for {
			select {
			case event := <-feed:
				if event == nil {
					return
				}
			case <-time.After(10 * time.Second):
				t.Fatal("expected event didn't arrive over feed")
				return
			}
		}
	}

	numEntriesInLateFeed := func() int64 { return db.DbStats.Cache().NumEntriesInLateFeed.Value() }

	feed1 := startABCChangesFeed(ctx, t, collectionWithUser) // healthy: drained after every write
	feed2 := startABCChangesFeed(ctx, t, collectionWithUser) // never abandoned, but drained far less often than feed1

	drainUntilWait(feed1)
	drainUntilWait(feed2)
	baseline := numEntriesInLateFeed()

	WriteDirect(t, collection, []string{"ABC"}, 1)
	drainUntilWait(feed1)
	drainUntilWait(feed2)

	WriteDirect(t, collection, []string{"ABC"}, 3) // skip 2
	drainUntilWait(feed1)
	drainUntilWait(feed2)

	WriteDirect(t, collection, []string{"ABC"}, 2) // resolve 2 late
	drainUntilWait(feed1)
	drainUntilWait(feed2)

	// feed2 now lags: it's only drained once every feed2DrainEvery cycles, so it's persistently
	// slower than the writer (which produces a skip/late-arrival pair every cycle) but is never
	// abandoned - it always eventually reads again.
	const numCycles = 60
	const feed2DrainEvery = 10
	seq := uint64(3)
	var count int64
	for i := range numCycles {
		seq += 2
		WriteDirect(t, collection, []string{"ABC"}, seq) // skips seq-1
		drainUntilWait(feed1)
		WriteDirect(t, collection, []string{"ABC"}, seq-1) // resolves seq-1 late
		drainUntilWait(feed1)

		if (i+1)%feed2DrainEvery == 0 {
			drainUntilWait(feed2)
		}
		count = numEntriesInLateFeed()
	}

	require.Greater(t, count, baseline,
		"NumEntriesInLateFeed should climb well past baseline: feed2 never stopped reading, but its "+
			"read rate can't keep up with the write rate, so its goroutine ends up just as stuck "+
			"waiting for consumption of changes as a fully abandoned consumer")

	t.Logf("num_entries_in_late_feed baseline: %d, after %d cycles with feed2 drained only every %d cycles: %d",
		baseline, numCycles, feed2DrainEvery, count)
}
