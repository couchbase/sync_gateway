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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// drainUntilWait reads events from a continuous _changes feed until MultiChangesFeed sends its nil
// "caught up, waiting" marker, returning the sequences delivered in this drain. Callers that only need
// the barrier behaviour can ignore the return value.
func drainUntilWait(t *testing.T, feed <-chan *ChangeEntry) (drained []uint64) {
	t.Helper()
	for {
		select {
		case event := <-feed:
			if event == nil {
				return drained
			}
			drained = append(drained, event.Seq.Seq)
		case <-time.After(10 * time.Second):
			t.Fatal("expected event didn't arrive over feed")
			return drained
		}
	}
}

// startChangesFeed opens a real continuous _changes feed exactly as a live client connection would via
// changes.go: it watches the all-channels wildcard and inherits the channel access of the user attached to
// collectionWithUser (the REST _changes handler does the same - see changes_api.go), rather than applying an
// explicit channel filter. collectionWithUser must therefore have a user with the appropriate access set.
// The feed is canceled at test cleanup so its goroutine can always unblock and exit, even if abandoned.
func startChangesFeed(ctx context.Context, t *testing.T, collectionWithUser *DatabaseCollectionWithUser) <-chan *ChangeEntry {
	feedCtx, cancel := context.WithCancelCause(ctx)
	t.Cleanup(func() { cancel(errors.New("test teardown")) })
	// Give each feed its own DatabaseCollectionWithUser, exactly as production does - every changes feed / BLIP
	// handler copies it via copyDatabaseCollectionWithUser. MultiChangesFeed's goroutine calls ReloadUser, which
	// reassigns the user field and is explicitly documented as unsafe to call from concurrent goroutines, so two
	// feeds sharing a single collectionWithUser would race on that field under -race.
	feedCollection := &DatabaseCollectionWithUser{
		DatabaseCollection: collectionWithUser.DatabaseCollection,
		user:               collectionWithUser.user,
	}
	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ChangesCtx: feedCtx,
		Continuous: true,
		Wait:       true,
	}
	feed, err := feedCollection.MultiChangesFeed(feedCtx, base.SetOf(channels.AllChannelWildcard), options)
	require.NoError(t, err, "feed initialization error")
	return feed
}

// fastFeedBroadcast shrinks the continuous-feed broadcast intervals so the 500ms skipped-sequence slow mode
// (SkippedSequenceBroadcastChangesTime) doesn't dominate these tests' runtime.
func fastFeedBroadcast(cacheOptions CacheOptions) CacheOptions {
	cacheOptions.BroadcastChangesInterval = 5 * time.Millisecond
	cacheOptions.SkippedSequenceBroadcastInterval = 5 * time.Millisecond
	return cacheOptions
}

// shortWaitCacheWithLateLogMax returns the standard short-wait test cache options with a small
// lateLogs cap, so the length-based force-prune (channel_cache_single.go _purgeLateLogEntries) fires
// within a short test rather than only after 500 late entries accumulate.
func shortWaitCacheWithLateLogMax(lateLogMax int) CacheOptions {
	cacheOptions := fastFeedBroadcast(shortWaitCache())
	cacheOptions.LateLogMaxLength = lateLogMax
	return cacheOptions
}

// TestLateLogsBoundedWhenConsumerStops is the fix-side counterpart to the CBG-5610 reproduction. It
// drives a continuous _changes feed that registers a late-sequence listener and
// then stops consuming (an abandoned/hung client) - but asserts that NumEntriesInLateFeed stays bounded
// by LateLogMaxLength instead of growing forever. Without the length cap this same scenario grows the
// count unbounded with the cycle count (the behaviour the reproduction test used to assert); with the
// cap, _purgeLateLogEntries force-drops the stalled feed's pinned lastSequence and the queue plateaus.
func TestLateLogsBoundedWhenConsumerStops(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	const lateLogMax = 5
	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCacheWithLateLogMax(lateLogMax))
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // feeds inherit this user's channel access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection

	numEntriesInLateFeed := func() int64 { return db.DbStats.Cache().NumEntriesInLateFeed.Value() }
	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }

	feed1 := startChangesFeed(ctx, t, collectionWithUser) // stays healthy: drained after every write
	feed2 := startChangesFeed(ctx, t, collectionWithUser) // drained through the first cycle only, then abandoned

	// Only channel ABC accumulates late entries in this test, so assert against ABC's own lateLogs rather
	// than the global gauge (whose floor is the variable number of live channel caches under a wildcard feed).
	cc, ok := collectionWithUser.changeCache().getChannelCache().(*channelCacheImpl)
	require.True(t, ok)
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)

	writeSeq := func(seq uint64) {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		drainUntilWait(t, feed1)
	}

	drainUntilWait(t, feed1)
	drainUntilWait(t, feed2)

	// A skip/late cycle so both feeds register a late-sequence listener, then abandon feed2.
	writeSeq(1)
	drainUntilWait(t, feed2)
	writeSeq(3) // skips 2
	drainUntilWait(t, feed2)
	writeSeq(2) // resolves 2 late -> AddLateSequence; feed2 is never drained again

	// Repeat the skip/late cycle many times with feed2 abandoned. Each late-resolution write triggers
	// AddLateSequence -> _purgeLateLogEntries, which must keep lateLogs at or below the cap by
	// force-dropping feed2's stuck lastSequence - even though feed2's goroutine never advances it.
	const numCycles = 40
	seq := uint64(3)
	for range numCycles {
		seq += 2
		writeSeq(seq)     // skips seq-1
		writeSeq(seq - 1) // resolves seq-1 late -> AddLateSequence -> force-prune

		require.LessOrEqualf(t, abcCache.lateLogCount(), int64(lateLogMax),
			"ABC's lateLogs must stay bounded by LateLogMaxLength (%d) even though feed2 is abandoned - "+
				"the length cap should force-prune its stuck lastSequence", lateLogMax)
	}

	// The abandoned feed never returns to read, so it never observes that its lastSequence was pruned -
	// no rollback is ever counted. The memory is reclaimed by the force-prune alone. (A returning feed
	// would increment this - see TestLateLogsForcedRollbackResetsSlowFeed.)
	require.Equal(t, int64(0), forcedRollbacks(),
		"an abandoned feed's lateLogs are reclaimed by the force-prune without any rollback being counted")

	t.Logf("ABC late entries after %d cycles with feed2 abandoned: %d (cap %d); total num_entries_in_late_feed: %d",
		numCycles, abcCache.lateLogCount(), lateLogMax, numEntriesInLateFeed())
}

// TestLateLogsForcedRollbackResetsSlowFeed shows the safety-net side of the cap: when a slow (but not
// abandoned) feed falls far enough behind that its lastSequence is force-pruned from lateLogs, its next
// read fails the GetLateSequencesSince lookup and it is reset to its low sequence - incrementing
// LateFeedForcedRollbacks - rather than silently missing the pruned sequences. The feed then recovers
// and delivers up to the latest sequence.
func TestLateLogsForcedRollbackResetsSlowFeed(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	const lateLogMax = 5
	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCacheWithLateLogMax(lateLogMax))
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // feeds inherit this user's channel access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection

	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }

	feed1 := startChangesFeed(ctx, t, collectionWithUser) // healthy: drained after every write
	feed2 := startChangesFeed(ctx, t, collectionWithUser) // slow: drained only occasionally

	// feed2 lags only on channel ABC, so assert against ABC's own lateLogs rather than the global gauge
	// (whose floor is the variable number of live channel caches under a wildcard feed).
	cc, ok := collectionWithUser.changeCache().getChannelCache().(*channelCacheImpl)
	require.True(t, ok)
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)

	// feed2Seen records every sequence feed2 delivers across its entire lifetime, so we can prove at the
	// end that the forced rollback lost no data - i.e. feed2 received every sequence that was written,
	// not just that it eventually reached the latest one.
	feed2Seen := make(map[uint64]bool)
	drainFeed2 := func() {
		for _, seq := range drainUntilWait(t, feed2) {
			feed2Seen[seq] = true
		}
	}

	drainUntilWait(t, feed1)
	drainFeed2()

	writeSeq := func(seq uint64) {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		drainUntilWait(t, feed1)
	}

	// Register both late-sequence listeners via a skip/late cycle.
	writeSeq(1)
	drainFeed2()
	writeSeq(3) // skips 2
	drainFeed2()
	writeSeq(2) // resolves 2 late

	// feed2 is drained only every feed2DrainEvery cycles, so it falls far enough behind that its
	// lastSequence is force-pruned by the cap. When it is next drained it must hit the rollback path.
	const numCycles = 40
	const feed2DrainEvery = 20
	seq := uint64(3)
	for i := range numCycles {
		seq += 2
		writeSeq(seq)     // skips seq-1
		writeSeq(seq - 1) // resolves seq-1 late

		require.LessOrEqualf(t, abcCache.lateLogCount(), int64(lateLogMax),
			"ABC's lateLogs must stay bounded by LateLogMaxLength (%d) while feed2 lags", lateLogMax)

		if (i+1)%feed2DrainEvery == 0 {
			drainFeed2() // feed2 returns; if its lastSequence was pruned this forces a rollback
		}
	}

	require.Greaterf(t, forcedRollbacks(), int64(0),
		"feed2 fell far enough behind that its lastSequence was pruned from lateLogs; returning to read "+
			"must have forced at least one rollback (LateFeedForcedRollbacks)")

	// Recovery: after a final in-order write, feed2 must be able to drain up to the latest sequence,
	// proving the rollback reset it rather than leaving it stuck or silently missing sequences.
	// Use the next contiguous sequence so that 1..finalSeq is a complete, gap-free set of written
	// sequences - the no-data-loss assertion below relies on every value in that range having been written.
	seq++
	finalSeq := seq
	WriteDirect(t, collection, []string{"ABC"}, finalSeq)
	drainUntilWait(t, feed1)

	var maxSeqSeen uint64
	recoveryDeadline := time.After(30 * time.Second)
	for maxSeqSeen < finalSeq {
		select {
		case event := <-feed2:
			if event != nil {
				feed2Seen[event.Seq.Seq] = true
				if event.Seq.Seq > maxSeqSeen {
					maxSeqSeen = event.Seq.Seq
				}
			}
		case <-recoveryDeadline:
			t.Fatalf("feed2 did not recover to seq %d after rollback (reached %d, seen %d distinct)",
				finalSeq, maxSeqSeen, len(feed2Seen))
		}
	}

	// No data loss: despite feed2's lastSequence being force-pruned from lateLogs mid-stream, every
	// sequence that was written (1..finalSeq, contiguous) must have been delivered to feed2 at some point.
	// The rollback re-reads the channel cache from feed2's last safe low sequence, so the pruned late
	// arrivals are recovered rather than skipped. A regression that dropped them would leave a gap here.
	var missing []uint64
	for s := uint64(1); s <= finalSeq; s++ {
		if !feed2Seen[s] {
			missing = append(missing, s)
		}
	}
	require.Emptyf(t, missing,
		"forced lateLogs rollback lost data: feed2 never received sequence(s) %v (received %d of %d)",
		missing, len(feed2Seen), finalSeq)

	t.Logf("forced_rollbacks: %d, feed2 recovered to seq %d having received all %d sequences with no gaps",
		forcedRollbacks(), maxSeqSeen, finalSeq)
}

// TestLateLogsAgedPruneReclaimsStalledFeed covers the age-based reclaim path (pruneLateLogAge, run by
// the cleanAgedLateLogs background task). Unlike the length cap, this fires on a timer rather than on add,
// so it is the only mechanism that reclaims a stalled feed's lateLogs once the channel goes quiet and no
// further late sequences arrive. Here the length cap is set high so it never fires, a stalled feed's
// pinned entries accumulate, and then running the age sweep after they exceed LateLogAge reclaims them -
// even though a listener still references them and no new sequences are being added.
func TestLateLogsAgedPruneReclaimsStalledFeed(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	cacheOptions := fastFeedBroadcast(shortWaitCache())
	cacheOptions.LateLogMaxLength = 100000 // large: isolate the age path so the length cap never fires
	// Large LateLogAge so the CleanAgedLateLogs background task (which runs on this interval) doesn't fire
	// during the test and reclaim the entries before we can assert they accumulated. The manual sweep below
	// lowers ABC's own threshold to force the age-based reclaim deterministically.
	cacheOptions.LateLogAge = time.Hour
	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // feeds inherit this user's channel access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection

	feed1 := startChangesFeed(ctx, t, collectionWithUser) // healthy: drained after every write
	feed2 := startChangesFeed(ctx, t, collectionWithUser) // abandoned after registering its listener

	writeSeq := func(seq uint64) {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		drainUntilWait(t, feed1)
	}

	drainUntilWait(t, feed1)
	drainUntilWait(t, feed2)

	writeSeq(1)
	drainUntilWait(t, feed2)
	writeSeq(3) // skips 2
	drainUntilWait(t, feed2)
	writeSeq(2) // resolves 2 late; feed2 is never drained again

	// Run enough skip/late cycles that feed2's output buffer fills, it stops advancing its lastSequence,
	// and its pinned entries accumulate in lateLogs (the leak - the length cap is intentionally too high
	// to fire here).
	const numCycles = 30
	seq := uint64(3)
	for range numCycles {
		seq += 2
		writeSeq(seq)     // skips seq-1
		writeSeq(seq - 1) // resolves seq-1 late
	}

	cc, ok := collectionWithUser.changeCache().getChannelCache().(*channelCacheImpl)
	require.True(t, ok)
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)

	beforeSweep := abcCache.lateLogCount()
	require.Greater(t, beforeSweep, int64(2),
		"stalled feed2 should have accumulated several pinned late entries in ABC that the (high) length cap leaves in place")

	// Channel goes quiet: no more late sequences arrive, so _purgeLateLogEntries never runs again. Drop ABC's
	// age threshold and let its accumulated entries exceed it, then run the age sweep (as the cleanAgedLateLogs
	// background task does on its timer). It must reclaim feed2's pinned ABC entries down to the tail even
	// though feed2 still references them. Check ABC's own queue rather than the global gauge, whose floor is
	// the variable number of live channel caches.
	abcCache.options.LateLogAge = time.Millisecond
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NoError(t, cc.cleanAgedLateLogs(ctx))
		require.Equal(t, int64(1), abcCache.lateLogCount(),
			"age sweep must reclaim stalled feed2's pinned ABC late entries down to the tail once older than LateLogAge")
	}, 10*time.Second, 10*time.Millisecond)

	t.Logf("ABC late entries before age sweep: %d, after: %d", beforeSweep, abcCache.lateLogCount())
}

// TestLateLogsStatReleasedOnChannelEviction verifies that when channel caches are evicted by compaction the
// NumEntriesInLateFeed gauge is decremented by the evicted channels' lateLogs entries (including their
// sentinels). Those entries are dropped wholesale on eviction without going through the per-entry purge
// paths, so without an explicit decrement the gauge would leak upward as channels churn. Several channels
// are seeded with extra late sequences (behind a registered listener, so they aren't immediately purged) to
// prove the full per-channel count is released, not merely one entry per evicted channel. The invariant
// checked throughout is that NumEntriesInLateFeed always equals the real total held by the channels that
// are still present in the cache.
func TestLateLogsStatReleasedOnChannelEviction(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20 // high watermark 16, low watermark 12

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	activeChannels := channels.NewActiveChannels(&base.SgwIntStat{})
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(ctx, "testDb", options, testQueryHandlerFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	numEntriesInLateFeed := func() int64 { return testStats.NumEntriesInLateFeed.Value() }

	// sumLateLogs returns the true number of lateLogs entries currently held across all channel caches still
	// present in the collection - the value NumEntriesInLateFeed must always equal.
	sumLateLogs := func() int64 {
		var total int64
		cache.channelCaches.Range(func(value any) bool {
			if scc := AsSingleChannelCache(ctx, value); scc != nil {
				total += scc.lateLogCount()
			}
			return true
		})
		return total
	}

	// Add 16 channels (the high watermark) so compaction isn't triggered yet. Each new channel contributes
	// its sentinel entry; every 4th channel is additionally seeded with 3 late sequences (behind a
	// registered listener that pins the sentinel so _purgeLateLogEntries doesn't immediately drop them).
	for i := 1; i <= 16; i++ {
		scc, _ := cache.addChannelCache(ctx, channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID))
		if i%4 == 0 {
			scc.RegisterLateSequenceClient() // pin the sentinel so the seeded late entries survive the purge
			for seq := uint64(1); seq <= 3; seq++ {
				scc.AddLateSequence(&LogEntry{Sequence: seq})
			}
		}
	}
	require.Equal(t, 16, cache.channelCaches.Length())

	// Invariant before compaction: the gauge equals the real total (16 sentinels + 4*3 seeded late seqs).
	require.Equal(t, sumLateLogs(), numEntriesInLateFeed(),
		"NumEntriesInLateFeed should equal the real lateLogs total before compaction")
	beforeCompaction := numEntriesInLateFeed()
	require.Greater(t, beforeCompaction, int64(16),
		"seeded late sequences should push the gauge above one-entry-per-channel")

	// Add another channel to exceed the high watermark and trigger compaction down to the low watermark.
	cache.addChannelCache(ctx, channels.NewID("chan_17", base.DefaultCollectionID))
	require.True(t, waitForCompaction(cache), "compaction didn't complete in expected time")
	require.Equal(t, 12, cache.channelCaches.Length(), "compaction should evict down to the low watermark")

	// The decisive assertion: after eviction the gauge must have been decremented to exactly the lateLogs
	// still held by the surviving channels - none of the evicted channels' entries leaked.
	require.Equal(t, sumLateLogs(), numEntriesInLateFeed(),
		"NumEntriesInLateFeed must equal the surviving channels' lateLogs total after eviction (no leak)")
	require.Less(t, numEntriesInLateFeed(), beforeCompaction,
		"eviction must have decremented NumEntriesInLateFeed")

	t.Logf("num_entries_in_late_feed before compaction: %d, after: %d (%d channels remain)",
		beforeCompaction, numEntriesInLateFeed(), cache.channelCaches.Length())
}

// TestLateLogsConcurrentReleaseAndPrune exercises ReleaseLateSequenceClient concurrently with the operations
// that reassign the lateLogs slice - AddLateSequence (which force-prunes on every add), plus
// RegisterLateSequenceClient and GetLateSequencesSince. It is primarily a race-detector target.
func TestLateLogsConcurrentReleaseAndPrune(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()

	cache := newSingleChannelCache(collection, channels.NewID("raceChan", collection.GetCollectionID()), 0, testStats)
	cache.options.LateLogMaxLength = 5 // force _purgeLateLogEntries to reassign the slice on nearly every add

	const releaserGoroutines = 4
	const iterations = 2000
	var wg sync.WaitGroup

	// Writer: append late sequences. Each add force-prunes past the small cap, reassigning the slice.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			cache.AddLateSequence(&LogEntry{Sequence: uint64(i + 1)})
		}
	}()

	// Registrars/releasers: repeatedly register a late-sequence listener then release it, iterating
	// lateLogs (unlocked in the buggy version) while the writer mutates the slice.
	for range releaserGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				seq := cache.RegisterLateSequenceClient()
				cache.ReleaseLateSequenceClient(seq)
			}
		}()
	}

	// Reader: concurrently walk the late feed as a real feed does - registering a listener first (which pins
	// the entry it parks on) and re-registering on the rollback error, rather than resetting to a bare since=0.
	// This keeps every GetLateSequencesSince removeListener balanced against a prior addListener, matching the
	// production changes-feed lifecycle (newLateSequenceFeed -> getLateFeed -> newLateSequenceFeed on rollback).
	wg.Add(1)
	go func() {
		defer wg.Done()
		since := cache.RegisterLateSequenceClient()
		for range iterations {
			if _, last, feedErr := cache.GetLateSequencesSince(since); feedErr == nil {
				since = last
			} else {
				since = cache.RegisterLateSequenceClient()
			}
		}
	}()

	wg.Wait()

	require.GreaterOrEqual(t, testStats.NumEntriesInLateFeed.Value(), int64(0),
		"NumEntriesInLateFeed must never go negative under concurrent churn")
	require.LessOrEqualf(t, testStats.NumEntriesInLateFeed.Value(), int64(cache.options.LateLogMaxLength)+1,
		"lateLogs must stay bounded by the length cap (%d) under concurrent churn", cache.options.LateLogMaxLength)
}

// TestLateLogOptionsPropagation verifies that the LateLogMaxLength / LateLogAge cache options are propagated
// to each per-channel cache by newChannelCacheWithOptions, and that non-positive values fall back to the
// package defaults rather than disabling the caps.
func TestLateLogOptionsPropagation(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	cacheStats := dbstats.Cache()

	// Configured (positive) values are applied to the per-channel cache.
	options := DefaultCacheOptions().ChannelCacheOptions
	options.LateLogMaxLength = 7
	options.LateLogAge = 42 * time.Second
	sc := newChannelCacheWithOptions(ctx, collection, channels.NewID("configured", collection.GetCollectionID()), 0, options, cacheStats)
	require.Equal(t, 7, sc.options.LateLogMaxLength)
	require.Equal(t, 42*time.Second, sc.options.LateLogAge)

	// Non-positive values fall back to the defaults - the caps are never disabled by a zero value.
	options.LateLogMaxLength = 0
	options.LateLogAge = 0
	scDefault := newChannelCacheWithOptions(ctx, collection, channels.NewID("defaulted", collection.GetCollectionID()), 0, options, cacheStats)
	require.Equal(t, DefaultLateLogMaxLength, scDefault.options.LateLogMaxLength)
	require.Equal(t, DefaultLateLogAge, scDefault.options.LateLogAge)
}

// TestLateLogsAgedForcedRollbackResetsSlowFeed is the age-based counterpart to
// TestLateLogsForcedRollbackResetsSlowFeed. Here the length cap is set high so it never fires; instead the
// age sweep (pruneLateLogAge, run by the cleanAgedLateLogs background task) drops a slow feed's parked
// late-sequence position once it is older than LateLogAge. When that feed returns to read, its
// GetLateSequencesSince lookup fails and it is rolled back to its low sequence and recovers - it must
// increment LateFeedForcedRollbacks and still deliver every written sequence with no data loss.
func TestLateLogsAgedForcedRollbackResetsSlowFeed(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	cacheOptions := fastFeedBroadcast(shortWaitCache())
	cacheOptions.LateLogMaxLength = 100000 // large: isolate the age path so the length cap never fires
	cacheOptions.LateLogAge = time.Millisecond
	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // feeds inherit this user's channel access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection

	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }

	cc, ok := collectionWithUser.changeCache().getChannelCache().(*channelCacheImpl)
	require.True(t, ok)

	feed1 := startChangesFeed(ctx, t, collectionWithUser) // healthy: drained after every write
	feed2Seen := make(map[uint64]bool)
	feed2 := startChangesFeed(ctx, t, collectionWithUser) // slow: parked while its position ages out

	drainUntilWait(t, feed1)
	for _, seq := range drainUntilWait(t, feed2) {
		feed2Seen[seq] = true
	}

	writeSeq := func(seq uint64) {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		drainUntilWait(t, feed1)
	}

	// Register both late-sequence listeners via a skip/late cycle, then leave feed2 to lag.
	writeSeq(1)
	for _, seq := range drainUntilWait(t, feed2) {
		feed2Seen[seq] = true
	}
	writeSeq(3) // skips 2
	for _, seq := range drainUntilWait(t, feed2) {
		feed2Seen[seq] = true
	}
	writeSeq(2) // resolves 2 late; feed2 is not drained again until recovery

	// Run enough skip/late cycles that feed2's output buffer fills and it stops advancing its late-sequence
	// position, leaving that position frozen well behind the tail. The length cap is intentionally too high
	// to fire, so nothing is pruned yet.
	const numCycles = 40
	seq := uint64(3)
	for range numCycles {
		seq += 2
		writeSeq(seq)     // skips seq-1
		writeSeq(seq - 1) // resolves seq-1 late
	}

	// Age sweep: with LateLogAge=1ms every accumulated entry is now old, so the sweep collapses ABC's
	// lateLogs to its tail even though feed2 still references an interior entry - exactly what the
	// cleanAgedLateLogs background task does on its timer. Check ABC's own queue rather than the global gauge,
	// whose floor is the (variable) number of live channel caches. Poll until ABC has collapsed.
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, cc.cleanAgedLateLogs(ctx))
		assert.Equal(c, int64(1), abcCache.lateLogCount(),
			"age sweep must collapse ABC's lateLogs to just its tail, aging out feed2's parked position")
	}, 10*time.Second, 10*time.Millisecond)

	// Recovery / no data loss: after a final in-order write, reading feed2 forces it to notice its position
	// was aged out (rollback), reset to its low sequence, and catch back up to the latest sequence.
	seq++
	finalSeq := seq
	WriteDirect(t, collection, []string{"ABC"}, finalSeq)
	drainUntilWait(t, feed1)

	var maxSeqSeen uint64
	recoveryDeadline := time.After(30 * time.Second)
	for maxSeqSeen < finalSeq {
		select {
		case event := <-feed2:
			if event != nil {
				feed2Seen[event.Seq.Seq] = true
				if event.Seq.Seq > maxSeqSeen {
					maxSeqSeen = event.Seq.Seq
				}
			}
		case <-recoveryDeadline:
			t.Fatalf("feed2 did not recover to seq %d after aged rollback (reached %d, seen %d distinct)",
				finalSeq, maxSeqSeen, len(feed2Seen))
		}
	}

	require.Greaterf(t, forcedRollbacks(), int64(0),
		"feed2's late-sequence position was aged out of lateLogs; returning to read must have forced a rollback")

	// No data loss: every written sequence 1..finalSeq must have reached feed2 despite the age-driven prune.
	var missing []uint64
	for s := uint64(1); s <= finalSeq; s++ {
		if !feed2Seen[s] {
			missing = append(missing, s)
		}
	}
	require.Emptyf(t, missing,
		"aged lateLogs rollback lost data: feed2 never received sequence(s) %v (received %d of %d)",
		missing, len(feed2Seen), finalSeq)

	t.Logf("forced_rollbacks (age path): %d, feed2 recovered to seq %d with no gaps", forcedRollbacks(), maxSeqSeen)
}

// TestLateLogsHealthyFeedsNoRollback is the no-regression guard for the normal path: with the shipped
// default caps (LateLogMaxLength=500, LateLogAge=5m) and consumers that keep up, a steady skip/late load
// must never force a rollback, must never lose data, and must keep lateLogs collapsed by the ordinary
// zero-listener purge (NumEntriesInLateFeed stays near its baseline rather than growing with the load).
func TestLateLogsHealthyFeedsNoRollback(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	// shortWaitCache carries the shipped default late-log caps - deliberately not overridden here.
	db, ctx := setupTestDBWithCacheOptions(t, fastFeedBroadcast(shortWaitCache()))
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // feeds inherit this user's channel access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection

	numEntriesInLateFeed := func() int64 { return db.DbStats.Cache().NumEntriesInLateFeed.Value() }
	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }

	feed1 := startChangesFeed(ctx, t, collectionWithUser) // both feeds stay healthy: drained after every write
	feed2 := startChangesFeed(ctx, t, collectionWithUser)
	feed1Seen := make(map[uint64]bool)
	feed2Seen := make(map[uint64]bool)
	drainBoth := func() {
		for _, s := range drainUntilWait(t, feed1) {
			feed1Seen[s] = true
		}
		for _, s := range drainUntilWait(t, feed2) {
			feed2Seen[s] = true
		}
	}
	drainBoth()

	writeSeq := func(seq uint64) {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		drainBoth()
	}

	// A skip/late cycle so both feeds register a late-sequence listener and reach steady state.
	writeSeq(1)
	writeSeq(3) // skips 2
	writeSeq(2) // resolves 2 late

	// Baseline once both feeds are caught up: the count the healthy path should hover around (sentinels plus
	// the single entry both feeds are parked on), independent of how many cycles follow.
	baseline := numEntriesInLateFeed()

	const numCycles = 30
	seq := uint64(3)
	for range numCycles {
		seq += 2
		writeSeq(seq)     // skips seq-1
		writeSeq(seq - 1) // resolves seq-1 late

		require.LessOrEqualf(t, numEntriesInLateFeed(), baseline+3,
			"healthy feeds should keep lateLogs collapsed via the zero-listener purge - it must not grow with "+
				"the load (baseline %d)", baseline)
		require.Equalf(t, int64(0), forcedRollbacks(),
			"healthy feeds under the default cap must never be forced to roll back")
	}

	finalSeq := seq

	// No data loss: both healthy feeds received every written sequence.
	for _, feed := range []struct {
		name string
		seen map[uint64]bool
	}{{"feed1", feed1Seen}, {"feed2", feed2Seen}} {
		var missing []uint64
		for s := uint64(1); s <= finalSeq; s++ {
			if !feed.seen[s] {
				missing = append(missing, s)
			}
		}
		require.Emptyf(t, missing, "%s missed sequence(s) %v under healthy skip/late load", feed.name, missing)
	}

	require.Equal(t, int64(0), forcedRollbacks(), "no rollbacks should have occurred for healthy feeds")

	t.Logf("healthy feeds: forced_rollbacks=%d, num_entries_in_late_feed=%d (baseline %d), delivered all %d sequences",
		forcedRollbacks(), numEntriesInLateFeed(), baseline, finalSeq)
}

// TestLateLogsPurgeEdgeCases covers the boundary behaviour of the lateLogs caps directly at the
// singleChannelCache level: the exact length bound, the degenerate cap of one, sentinel accounting,
// releasing an already-pruned sequence, and the age prune's minimum-one-entry / no-negative-stat guarantees.
func TestLateLogsPurgeEdgeCases(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	// newCache builds a fresh single-channel cache with its own stats so each sub-case is isolated.
	newCache := func(t *testing.T, lateLogMax int) (*singleChannelCacheImpl, *base.CacheStats) {
		stats, err := base.NewSyncGatewayStats()
		require.NoError(t, err)
		dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
		require.NoError(t, err)
		cacheStats := dbstats.Cache()
		sc := newSingleChannelCache(collection, channels.NewID("edge", collection.GetCollectionID()), 0, cacheStats)
		sc.options.LateLogMaxLength = lateLogMax
		return sc, cacheStats
	}

	t.Run("length cap bounds the queue and force-drops a pinned position", func(t *testing.T) {
		sc, cacheStats := newCache(t, 5)
		since := sc.RegisterLateSequenceClient() // pins the sentinel (seq 0) at the front of the queue
		for i := uint64(1); i <= 20; i++ {
			sc.AddLateSequence(&LogEntry{Sequence: i})
			require.LessOrEqualf(t, sc.lateLogCount(), int64(5),
				"force-prune must never let lateLogs exceed the cap, even with a listener pinning the front (i=%d)", i)
		}
		require.Equal(t, sc.lateLogCount(), cacheStats.NumEntriesInLateFeed.Value(), "stat must match the actual queue length")
		// Once the queue hit the cap the pinned sentinel was force-dropped, so the caller must be rolled back
		// rather than served from a position that no longer exists.
		_, _, err := sc.GetLateSequencesSince(since)
		require.Error(t, err, "the length cap must force-drop even a still-referenced position, forcing a rollback")
	})

	t.Run("cap of one keeps only the newest entry and forces rollback", func(t *testing.T) {
		sc, _ := newCache(t, 1)
		since := sc.RegisterLateSequenceClient() // registers on the sentinel (seq 0)
		for i := uint64(1); i <= 5; i++ {
			sc.AddLateSequence(&LogEntry{Sequence: i})
		}
		require.Equal(t, int64(1), sc.lateLogCount(), "cap of one collapses lateLogs to just the newest entry")
		_, _, err := sc.GetLateSequencesSince(since)
		require.Error(t, err, "the caller's pruned position must force a rollback rather than silently skipping")
	})

	t.Run("fresh cache reports one sentinel entry", func(t *testing.T) {
		sc, cacheStats := newCache(t, 5)
		require.Equal(t, int64(1), sc.lateLogCount(), "a fresh channel cache holds exactly its sentinel entry")
		require.Equal(t, int64(1), cacheStats.NumEntriesInLateFeed.Value())
	})

	t.Run("release of a pruned sequence returns false without corrupting counts", func(t *testing.T) {
		sc, cacheStats := newCache(t, 5)
		since := sc.RegisterLateSequenceClient() // on the sentinel (seq 0)
		for i := uint64(1); i <= 20; i++ {
			sc.AddLateSequence(&LogEntry{Sequence: i}) // force-prunes the sentinel out from under the listener
		}
		require.False(t, sc.ReleaseLateSequenceClient(since), "releasing an already-pruned sequence must report not-found")
		require.Equal(t, sc.lateLogCount(), cacheStats.NumEntriesInLateFeed.Value(), "stat stays consistent with the queue")
	})

	t.Run("age prune keeps at least one entry and never drives the stat negative", func(t *testing.T) {
		sc, cacheStats := newCache(t, 100000) // length cap out of the way, isolate the age path
		sc.options.LateLogAge = time.Millisecond

		// No-op on a minimal queue: pruning a sentinel-only cache leaves the sentinel and the stat at one.
		sc.pruneLateLogAge(ctx)
		require.Equal(t, int64(1), sc.lateLogCount())
		require.Equal(t, int64(1), cacheStats.NumEntriesInLateFeed.Value())

		sc.RegisterLateSequenceClient()
		for i := uint64(1); i <= 10; i++ {
			sc.AddLateSequence(&LogEntry{Sequence: i})
		}
		time.Sleep(2 * time.Millisecond) // let the entries exceed LateLogAge
		sc.pruneLateLogAge(ctx)
		require.Equal(t, int64(1), sc.lateLogCount(), "age prune collapses to the tail but always keeps one entry")
		require.Equal(t, cacheStats.NumEntriesInLateFeed.Value(), int64(1), "stat must never go negative")
		require.Equal(t, sc.lateLogCount(), cacheStats.NumEntriesInLateFeed.Value())
	})
}

// TestLateLogsAgedPrunePreservesParkedSentinel
//
// The sentinel entry created by initializeLateLogs should set its `arrived` field, so it doesn't hold the zero
// time.Time. pruneLateLogAge drops leading entries whose time.Since(arrived) > LateLogAge; for a zero time
// that comparison is ~always true, so the sentinel is force-pruned whenever any other late entry sits behind
// it.
func TestLateLogsAgedPrunePreservesParkedSentinel(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	cacheStats := dbstats.Cache()

	sc := newSingleChannelCache(collection, channels.NewID("sentinel", collection.GetCollectionID()), 0, cacheStats)
	// Large age and length caps so neither the length force-prune nor a legitimate age-out of the (fresh)
	// non-sentinel entry can fire - the only thing that could drop the sentinel here is the zero-arrived bug.
	sc.options.LateLogAge = 5 * time.Minute
	sc.options.LateLogMaxLength = 100000

	// A continuous feed that connected before any late sequence arrived parks on the sentinel (Sequence 0).
	since := sc.RegisterLateSequenceClient()
	require.Equal(t, uint64(0), since, "a feed connecting before any late sequence parks on the seq-0 sentinel")

	// One late sequence resolves, arriving just now (well within LateLogAge). Queue is now [sentinel, seq7];
	// the sentinel is pinned by the parked feed's listener so the zero-listener purge leaves it in place.
	sc.AddLateSequence(&LogEntry{Sequence: 7})
	require.Equal(t, int64(2), sc.lateLogCount(), "queue should hold the pinned sentinel plus the one fresh late entry")

	// The age sweep runs (as cleanAgedLateLogs would on its timer). The only non-sentinel entry is fresh, so
	// nothing has legitimately aged past the 5-minute LateLogAge; the sentinel must be retained.
	sc.pruneLateLogAge(ctx)

	// Behavioural assertion: the parked feed reads from its since=0 position and must be served seq 7, not
	// rolled back. With the bug the sentinel was pruned, so this lookup fails and LateFeedForcedRollbacks fires.
	entries, last, err := sc.GetLateSequencesSince(since)
	require.NoError(t, err, "feed parked on the sentinel must still be served after the age sweep - the never-arrived "+
		"sentinel must not be treated as infinitely old and force-pruned")
	require.Equal(t, uint64(7), last)
	require.Equal(t, 1, len(entries))
	require.Equal(t, uint64(7), entries[0].Sequence)
	require.Equal(t, int64(0), cacheStats.LateFeedForcedRollbacks.Value(),
		"a healthy feed parked on the sentinel must not be force-rolled-back by the age sweep")
}

// TestLateLogsStatLeakOnConcurrentAddChannelCache is a for ensuring NumEntriesInLateFeed doesn't leak when
// two callers race to first-create the same channel cache. getChannelCache checks channelCaches.Get and, on a
// miss, calls addChannelCache; two goroutines that both miss the Get both call addChannelCache for the same
// channel. addChannelCache builds the singleChannelCache (whose initializeLateLogs already did
// NumEntriesInLateFeed.Add(1) for its sentinel) BEFORE GetOrInsert, so the loser of the insert has its
// freshly-built cache discarded by GetOrInsert without ever decrementing that +1 - permanently over-reporting
// the gauge by one per lost race, since the discarded cache is never in channelCaches for compaction to evict.
func TestLateLogsStatLeakOnConcurrentAddChannelCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	activeChannels := channels.NewActiveChannels(&base.SgwIntStat{})
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(ctx, "testDb", options, testQueryHandlerFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	numEntriesInLateFeed := func() int64 { return testStats.NumEntriesInLateFeed.Value() }
	sumLateLogs := func() int64 {
		var total int64
		cache.channelCaches.Range(func(value any) bool {
			if scc := AsSingleChannelCache(ctx, value); scc != nil {
				total += scc.lateLogCount()
			}
			return true
		})
		return total
	}

	ch := channels.NewID("contended", base.DefaultCollectionID)

	// First caller wins the insert.
	first, ok := cache.addChannelCache(ctx, ch)
	require.True(t, ok)

	// Second caller (the other goroutine that also missed the Get check) builds a fresh cache - incrementing
	// the shared gauge for its sentinel - then GetOrInsert returns the existing cache and discards the new one.
	second, ok := cache.addChannelCache(ctx, ch)
	require.True(t, ok)
	require.True(t, first == second, "both callers must resolve to the single inserted cache")
	require.Equal(t, 1, cache.channelCaches.Length(), "only one cache for the contended channel is ever inserted")

	require.Equal(t, sumLateLogs(), numEntriesInLateFeed(),
		"NumEntriesInLateFeed must equal the real lateLogs total held by inserted caches; the discarded loser "+
			"cache's sentinel increment must not leak into the gauge")
}
