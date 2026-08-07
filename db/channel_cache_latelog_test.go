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
// within a short test rather than only after 500 late entries accumulate. Late logs uses the same maximum as chanel cache
// configuration.
func shortWaitCacheWithLateLogMax(lateLogMax int) CacheOptions {
	cacheOptions := fastFeedBroadcast(shortWaitCache())
	cacheOptions.ChannelCacheMaxLength = lateLogMax
	return cacheOptions
}

// TestLateLogsBoundedWhenConsumerStops is the fix-side counterpart to the CBG-5610 reproduction. It
// drives a continuous _changes feed that registers a late-sequence listener and
// then stops consuming (an abandoned/hung client) - but asserts that NumEntriesInLateFeed stays bounded
// by ChannelCacheMaxLength instead of growing forever. Without the length cap this same scenario grows the
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
			"ABC's lateLogs must stay bounded by ChannelCacheMaxLength (%d) even though feed2 is abandoned - "+
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
			"ABC's lateLogs must stay bounded by ChannelCacheMaxLength (%d) while feed2 lags", lateLogMax)

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
	cacheOptions.ChannelCacheMaxLength = 100000 // large: isolate the age path so the length cap never fires
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
// NumEntriesInLateFeed gauge is decremented by the evicted channels' counted (non-sentinel) lateLogs entries.
// Those entries are dropped wholesale on eviction without going through the per-entry purge paths, so without
// an explicit decrement the gauge would leak upward as channels churn. Every channel is seeded with late
// sequences (behind a registered listener, so they aren't immediately purged) to prove the full per-channel
// count is released. The invariant checked throughout is that NumEntriesInLateFeed always equals the real
// (non-sentinel) total held by the channels that are still present in the cache - and, since the seq-0 sentinels
// aren't counted, the gauge is exactly the seeded late entries rather than carrying a per-channel floor.
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

	// sumLateLogs returns the true number of counted (non-sentinel) lateLogs entries currently held across all
	// channel caches still present in the collection - the value NumEntriesInLateFeed must always equal.
	sumLateLogs := func() int64 {
		var total int64
		cache.channelCaches.Range(func(value any) bool {
			if scc := AsSingleChannelCache(ctx, value); scc != nil {
				total += scc.countedLateLogCount()
			}
			return true
		})
		return total
	}

	// Add 16 channels (the high watermark) so compaction isn't triggered yet. Each channel's seq-0 sentinel is
	// not counted; each is seeded with 3 late sequences (behind a registered listener that pins the sentinel so
	// _purgeLateLogEntries doesn't immediately drop them) so that every channel contributes counted entries and
	// any eviction is guaranteed to decrement the gauge.
	const seededPerChannel = 3
	for i := 1; i <= 16; i++ {
		scc, _ := cache.addChannelCache(ctx, channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID))
		scc.RegisterLateSequenceClient() // pin the sentinel so the seeded late entries survive the purge
		for seq := uint64(1); seq <= seededPerChannel; seq++ {
			scc.AddLateSequence(&LogEntry{Sequence: seq})
		}
	}
	require.Equal(t, 16, cache.channelCaches.Length())

	// The gauge counts only the seeded late entries (16 channels x 3), with no per-channel floor - the sentinels
	// are excluded. Under the old sentinel-counting behaviour this would have been 16 + 16*3.
	require.Equal(t, int64(16*seededPerChannel), numEntriesInLateFeed(),
		"gauge should count only the seeded late entries, not the per-channel sentinels")
	require.Equal(t, sumLateLogs(), numEntriesInLateFeed(),
		"NumEntriesInLateFeed should equal the real non-sentinel lateLogs total before compaction")
	beforeCompaction := numEntriesInLateFeed()

	// Add another channel to exceed the high watermark and trigger compaction down to the low watermark.
	cache.addChannelCache(ctx, channels.NewID("chan_17", base.DefaultCollectionID))
	require.True(t, waitForCompaction(cache), "compaction didn't complete in expected time")
	require.Equal(t, 12, cache.channelCaches.Length(), "compaction should evict down to the low watermark")

	// The decisive assertion: after eviction the gauge must have been decremented to exactly the non-sentinel
	// lateLogs still held by the surviving channels - none of the evicted channels' entries leaked.
	require.Equal(t, sumLateLogs(), numEntriesInLateFeed(),
		"NumEntriesInLateFeed must equal the surviving channels' non-sentinel lateLogs total after eviction (no leak)")
	require.Less(t, numEntriesInLateFeed(), beforeCompaction,
		"eviction must have decremented NumEntriesInLateFeed by the evicted channels' late entries")

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
	cache.options.ChannelCacheMaxLength = 5 // force _purgeLateLogEntries to reassign the slice on nearly every add

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
	require.LessOrEqualf(t, testStats.NumEntriesInLateFeed.Value(), int64(cache.options.ChannelCacheMaxLength),
		"lateLogs must stay bounded by the length cap (%d) under concurrent churn - the sentinel isn't counted", cache.options.ChannelCacheMaxLength)
}

// TestLateLogOptionsPropagation verifies that the LateLogAge cache option is propagated
// to each per-channel cache by newChannelCacheWithOptions, and that non-positive values fall back to the
// package defaults rather than disabling the caps. Also verifies that late logs max length is set to chanel cache max length.
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
	options.ChannelCacheMaxLength = 7
	options.LateLogAge = 42 * time.Second
	sc := newChannelCacheWithOptions(ctx, collection, channels.NewID("configured", collection.GetCollectionID()), 0, options, cacheStats)
	require.Equal(t, 7, sc.options.ChannelCacheMaxLength)
	require.Equal(t, 42*time.Second, sc.options.LateLogAge)

	// Non-positive values fall back to the defaults - the caps are never disabled by a zero value.
	options.ChannelCacheMaxLength = 0
	options.LateLogAge = 0
	scDefault := newChannelCacheWithOptions(ctx, collection, channels.NewID("defaulted", collection.GetCollectionID()), 0, options, cacheStats)
	require.Equal(t, DefaultChannelCacheMaxLength, scDefault.options.ChannelCacheMaxLength)
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
	cacheOptions.ChannelCacheMaxLength = 100000 // large: isolate the age path so the length cap never fires
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
// default caps (ChannelCacheMaxLength=500, LateLogAge=5m) and consumers that keep up, a steady skip/late load
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
		require.NoError(t, db.WaitForSequenceNotSkipped(ctx, seq))
		drainBoth()
	}

	// A skip/late cycle so both feeds register a late-sequence listener and reach steady state.
	writeSeq(1)
	writeSeq(3) // skips 2
	writeSeq(2) // resolves 2 late

	// Baseline once both feeds are caught up: the count the healthy path should hover around (the single entry
	// both feeds are parked on; sentinels aren't counted), independent of how many cycles follow.
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
		sc.options.ChannelCacheMaxLength = lateLogMax
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
		require.Equal(t, sc.countedLateLogCount(), cacheStats.NumEntriesInLateFeed.Value(), "stat must match the counted (non-sentinel) queue length")
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

	t.Run("fresh cache reports one sentinel entry but a zero gauge", func(t *testing.T) {
		sc, cacheStats := newCache(t, 5)
		require.Equal(t, int64(1), sc.lateLogCount(), "a fresh channel cache holds exactly its sentinel entry")
		require.Equal(t, int64(0), cacheStats.NumEntriesInLateFeed.Value(), "the sentinel is not counted, so a fresh cache contributes nothing to the gauge")
	})

	t.Run("release of a pruned sequence returns false without corrupting counts", func(t *testing.T) {
		sc, cacheStats := newCache(t, 5)
		since := sc.RegisterLateSequenceClient() // on the sentinel (seq 0)
		for i := uint64(1); i <= 20; i++ {
			sc.AddLateSequence(&LogEntry{Sequence: i}) // force-prunes the sentinel out from under the listener
		}
		require.False(t, sc.ReleaseLateSequenceClient(since), "releasing an already-pruned sequence must report not-found")
		require.Equal(t, sc.countedLateLogCount(), cacheStats.NumEntriesInLateFeed.Value(), "stat stays consistent with the counted (non-sentinel) queue")
	})

	t.Run("age prune keeps at least one entry and never drives the stat negative", func(t *testing.T) {
		sc, cacheStats := newCache(t, 100000) // length cap out of the way, isolate the age path
		sc.options.LateLogAge = time.Millisecond

		// No-op on a minimal queue: pruning a sentinel-only cache leaves the (uncounted) sentinel in place, so the
		// queue length is one but the gauge stays at zero.
		sc.pruneLateLogAge(ctx)
		require.Equal(t, int64(1), sc.lateLogCount())
		require.Equal(t, int64(0), cacheStats.NumEntriesInLateFeed.Value())

		sc.RegisterLateSequenceClient()
		for i := uint64(1); i <= 10; i++ {
			sc.AddLateSequence(&LogEntry{Sequence: i})
		}
		time.Sleep(2 * time.Millisecond) // let the entries exceed LateLogAge
		sc.pruneLateLogAge(ctx)
		require.Equal(t, int64(1), sc.lateLogCount(), "age prune collapses to the tail but always keeps one entry")
		// The surviving tail entry is now the always-retained placeholder (the seq-0 sentinel was pruned off the
		// front), so it is not counted: the gauge collapses to zero, never negative, and matches the counted queue.
		require.Equal(t, int64(0), cacheStats.NumEntriesInLateFeed.Value(), "stat collapses to zero (only the retained placeholder remains) and never goes negative")
		require.Equal(t, sc.countedLateLogCount(), cacheStats.NumEntriesInLateFeed.Value())
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
	sc.options.ChannelCacheMaxLength = 100000

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

// TestLateLogsStatLeakOnConcurrentAddChannelCache is a guard ensuring NumEntriesInLateFeed doesn't leak when
// two callers race to first-create the same channel cache. getChannelCache checks channelCaches.Get and, on a
// miss, calls addChannelCache; two goroutines that both miss the Get both call addChannelCache for the same
// channel, and the loser of the GetOrInsert has its freshly-built cache discarded. Because the seq-0 sentinel
// is not counted (initializeLateLogs doesn't touch the gauge) and the discarded cache is never in channelCaches
// (so no late arrival can be added to it), the loser contributes nothing to the gauge - and this test guards
// that the gauge stays at the real non-sentinel total (zero here) rather than over-reporting per lost race.
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
				total += scc.countedLateLogCount()
			}
			return true
		})
		return total
	}

	ch := channels.NewID("contended", base.DefaultCollectionID)

	// First caller wins the insert.
	first, ok := cache.addChannelCache(ctx, ch)
	require.True(t, ok)

	// Second caller (the other goroutine that also missed the Get check) builds a fresh cache, then GetOrInsert
	// returns the existing cache and discards the new one.
	second, ok := cache.addChannelCache(ctx, ch)
	require.True(t, ok)
	require.True(t, first == second, "both callers must resolve to the single inserted cache")
	require.Equal(t, 1, cache.channelCaches.Length(), "only one cache for the contended channel is ever inserted")

	require.Equal(t, sumLateLogs(), numEntriesInLateFeed(),
		"NumEntriesInLateFeed must equal the real non-sentinel lateLogs total held by inserted caches; the "+
			"discarded loser cache must not leak into the gauge")
	require.Equal(t, int64(0), numEntriesInLateFeed(),
		"both caches hold only their uncounted sentinels, so the gauge must be zero after the race")
}

// TestLateLogsSpikeNotPrunedUntilNewLateSequence reproduces the tricky-to-time scenario where a large spike of
// previously-skipped sequences all resolve (arrive late) near-simultaneously while a continuous _changes feed
// is parked, floods a channel's lateLogs, is then served to the feed in a catch-up, and verifies the decisive
// property: those served late entries are NOT reclaimed until the next skipped sequence arrives to trigger the
// purge. The length force-prune (_purgeLateLogEntries) runs only on AddLateSequence, and the age sweep only on
// its timer - so with both caps set arbitrarily large, a channel that goes quiet after a spike holds the whole
// spike indefinitely, and only the next late arrival collapses it.
func TestLateLogsSpikeNotPrunedUntilNewLateSequence(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	// The number of previously-skipped sequences that all resolve (arrive late) while the feed is parked.
	const spikeSize = 2000

	cacheOptions := fastFeedBroadcast(shortWaitCache())
	cacheOptions.ChannelCacheMaxLength = 10 * spikeSize // large: the length force-prune must never fire during the spike
	cacheOptions.LateLogAge = time.Hour                 // large: the age sweep must never reclaim entries during the test
	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // the feed inherits this user's ABC access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection
	cCache := collection.changeCache()

	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }

	// Prime: start a real continuous feed and let it catch up. On its first iteration it creates ABC's channel
	// cache (marking it active, so late arrivals are recorded there) and registers a late-sequence listener on
	// ABC's seq-0 sentinel, then parks waiting.
	feed := startChangesFeed(ctx, t, collectionWithUser)
	seen := make(map[uint64]bool) // every sequence the feed delivers across its lifetime
	collectSeen := func(seqs []uint64) {
		for _, s := range seqs {
			seen[s] = true
		}
	}
	collectSeen(drainUntilWait(t, feed)) // initial caught-up marker

	// One in-order write so the feed is streaming normally and the cache's nextSequence advances to 2.
	WriteDirect(t, collection, []string{"ABC"}, 1)
	collectSeen(drainUntilWait(t, feed))

	cc, ok := cCache.getChannelCache().(*channelCacheImpl)
	require.True(t, ok)
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)
	require.Equal(t, int64(1), abcCache.lateLogCount(), "ABC starts with just its sentinel entry")

	// Hold an extra listener on ABC's sentinel for the duration of the spike. While this front listener is held,
	// the zero-listener purge can never advance past the head of the queue, so every late arrival accumulates
	// regardless of how much the real feed reads - making the otherwise timing-sensitive accumulation
	// deterministic. Released before the final assertions.
	pinnedSentinel := abcCache.RegisterLateSequenceClient()
	require.Equal(t, uint64(0), pinnedSentinel, "the extra listener parks on the seq-0 sentinel before any late sequence arrives")

	// Create the skip: a single write far ahead (seq spikeSize+2) leaves 2..spikeSize+1 with no arrivals. Once
	// that lone pending entry ages out (shortWaitCache flushes pending after 5ms) the whole gap is pushed to the
	// skipped list in one range, so 2..spikeSize+1 are now previously-skipped sequences.
	gapWriteSeq := uint64(spikeSize + 2)
	firstSkipped := uint64(2)
	lastSkipped := uint64(spikeSize + 1)
	WriteDirect(t, collection, []string{"ABC"}, gapWriteSeq)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, cCache.WasSkipped(firstSkipped) && cCache.WasSkipped(lastSkipped),
			"the gap %d..%d should have been pushed to the skipped list", firstSkipped, lastSkipped)
	}, 10*time.Second, 5*time.Millisecond)

	// The spike: every skipped sequence resolves (arrives late) in a tight loop. The feed is deliberately not
	// drained here, so it stays parked (its output buffer fills and it blocks) while the arrivals flood into
	// ABC's lateLogs. Each arrival calls AddLateSequence -> _purgeLateLogEntries, but the pinned sentinel and the
	// large caps mean nothing is ever dropped.
	for seq := firstSkipped; seq <= lastSkipped; seq++ {
		WriteDirect(t, collection, []string{"ABC"}, seq)
	}

	// All spikeSize late arrivals land in ABC's lateLogs (plus the sentinel), and none are pruned.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(spikeSize+1), abcCache.lateLogCount(),
			"all %d late arrivals plus the sentinel should be held in ABC's lateLogs", spikeSize)
	}, 60*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(0), forcedRollbacks(),
		"nothing should have been pruned out from under the parked feed while the spike accumulated")
	t.Logf("spike loaded: ABC lateLogs=%d (sentinel + %d late arrivals)", abcCache.lateLogCount(), spikeSize)

	// Wake the feed and let it drain the entire spike from lateLogs. Read across broadcast cycles until the feed
	// has delivered every late sequence in the spike.
	drainDeadline := time.After(30 * time.Second)
	for {
		allSeen := true
		for s := firstSkipped; s <= lastSkipped; s++ {
			if !seen[s] {
				allSeen = false
				break
			}
		}
		if allSeen {
			break
		}
		select {
		case <-drainDeadline:
			t.Fatalf("feed did not deliver the full late-sequence spike from lateLogs")
		default:
		}
		collectSeen(drainUntilWait(t, feed))
	}

	// Serving the spike moved the feed's listener to the newest late entry but pruned nothing - reading late
	// sequences never triggers a purge. The full spike is still resident in lateLogs.
	require.Equal(t, int64(spikeSize+1), abcCache.lateLogCount(),
		"serving the spike to the feed must not prune lateLogs - only a new late sequence triggers the purge")

	// Release the extra sentinel listener. Now every entry the feed has already passed carries a zero listener
	// count and is fully eligible for the zero-listener purge - yet still nothing reclaims it, because no new late
	// sequence has arrived to run _purgeLateLogEntries (and the age sweep and length cap are configured never to
	// fire). This is the decisive assertion: absent another skipped sequence, the served spike is not pruned.
	require.True(t, abcCache.ReleaseLateSequenceClient(pinnedSentinel), "the extra sentinel listener should still be present to release")
	require.Never(t, func() bool {
		return abcCache.lateLogCount() < int64(spikeSize+1)
	}, 1*time.Second, 50*time.Millisecond,
		"with no new skipped sequence arriving, the served spike must remain in lateLogs (no length cap, no age sweep, no purge trigger)")

	// Now a single new skipped sequence resolves. This one AddLateSequence call runs _purgeLateLogEntries, which
	// finally reclaims the whole now-unreferenced spike, collapsing lateLogs down to the feed's parked entry plus
	// the new arrival.
	gapWriteSeq2 := uint64(spikeSize + 4)
	newlySkipped := uint64(spikeSize + 3)
	WriteDirect(t, collection, []string{"ABC"}, gapWriteSeq2)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, cCache.WasSkipped(newlySkipped), "seq %d should have been pushed to the skipped list", newlySkipped)
	}, 10*time.Second, 5*time.Millisecond)
	WriteDirect(t, collection, []string{"ABC"}, newlySkipped)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.LessOrEqual(c, abcCache.lateLogCount(), int64(3),
			"the next skipped sequence's AddLateSequence must purge the whole served spike from lateLogs")
	}, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(0), forcedRollbacks(),
		"the feed's parked entry is retained by the purge, so no forced rollback should ever occur")

	t.Logf("after one new late sequence: ABC lateLogs collapsed to %d, forced_rollbacks=%d",
		abcCache.lateLogCount(), forcedRollbacks())
}

// TestLateLogsSpikeForcePrunedBoundsLateLogsAndForcesRollback
// The scenario: a continuous feed registers its late-sequence listener on a channel's seq-0 sentinel and then
// stalls without reading (a hung/abandoned client). A spike of thousands of skipped sequences then all resolve
// (arrive late) at once while it is stalled. With the default caps:
//
//   - lateLogs stay bounded by ChannelCacheMaxLength instead of growing with the spike (the leak this branch fixes:
//     before compaction the stalled feed pinned the front and the queue grew unbounded with every arrival);
//   - the length force-prune drops even the stalled feed's referenced sentinel, so when the feed next reads that
//     position its getLateFeed call fails and the changes loop rolls it back (LateFeedForcedRollbacks) - the
//     safety net that resets a feed whose position was compacted away rather than silently skipping sequences;
//   - the feed recovers and catches up to the latest sequence rather than staying stuck.
//
// Timing is the usual hazard - a continuous feed woken by the broadcast ticker would ordinarily read the late
// arrivals as they trickle in and advance its listener off the sentinel, so the sentinel would never be the
// referenced position that gets force-pruned. We remove that race by first filling the feed's fixed 50-entry
// output buffer (changes.go) with in-order writes we never drain: the feed goroutine blocks on the output send
// with its late-sequence listener still parked on the sentinel (getLateFeed returns early without advancing
// lastSequence while no late sequences exist), and stays blocked. We detect that state precisely with
// len(feed) == cap(feed) - no timing guess - then run the spike while the feed is provably stalled, so the
// length cap is the only thing that can drop the referenced sentinel. Draining afterwards makes the feed's next
// getLateFeed hit the pruned sentinel and roll back inside the real loop.
func TestLateLogsSpikeForcePrunedBoundsLateLogsAndForcesRollback(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	// The number of previously-skipped sequences that all resolve (arrive late) while the feed is stalled. Must
	// exceed the length cap so the force-prune fires.
	const spikeSize = 2000
	require.Greater(t, spikeSize, DefaultChannelCacheMaxLength,
		"the spike must exceed DefaultChannelCacheMaxLength for the length force-prune to fire")

	// In-order writes used only to fill the feed's output buffer so the goroutine blocks. Must exceed the feed's
	// hard-coded output buffer capacity (50 in changes.go); the exact block point is confirmed via len==cap below.
	const bufferFillers = 60

	// Shipped default late-log caps (DefaultChannelCacheMaxLength=500, LateLogAge=5m) - deliberately not overridden. The
	// length cap is the mechanism under test; the 5-minute age is far longer than this test so the age sweep
	// never fires.
	db, ctx := setupTestDBWithCacheOptions(t, fastFeedBroadcast(shortWaitCache()))
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user // the feed inherits this user's ABC access via the wildcard, as a real client would
	collection := collectionWithUser.DatabaseCollection
	cCache := collection.changeCache()

	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }

	// Prime: start a real continuous feed and let it catch up. On its first iteration it creates ABC's channel
	// cache (marking it active, so late arrivals are recorded there) and registers a late-sequence listener on
	// ABC's seq-0 sentinel, then parks waiting.
	feed := startChangesFeed(ctx, t, collectionWithUser)
	seen := make(map[uint64]bool) // every sequence the feed delivers across its lifetime
	collectSeen := func(seqs []uint64) {
		for _, s := range seqs {
			seen[s] = true
		}
	}
	collectSeen(drainUntilWait(t, feed)) // initial caught-up marker

	// One in-order write so the feed is streaming normally and the cache's nextSequence advances to 2.
	WriteDirect(t, collection, []string{"ABC"}, 1)
	collectSeen(drainUntilWait(t, feed))

	cc, ok := cCache.getChannelCache().(*channelCacheImpl)
	require.True(t, ok)
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)
	require.Equal(t, int64(1), abcCache.lateLogCount(), "ABC starts with just its sentinel entry")

	// Stall the feed with its late-sequence listener still on the sentinel. Writing more in-order sequences than
	// the feed's output buffer holds, and never draining them, blocks the feed goroutine on its output send.
	// These writes are contiguous so nothing is skipped, so getLateFeed keeps returning early without advancing
	// the feed's late position - it stays parked on the seq-0 sentinel. We wait until the buffer is provably full
	// (len==cap) and every filler is cached (nextSequence advanced) before starting the spike.
	for seq := uint64(2); seq <= bufferFillers+1; seq++ {
		WriteDirect(t, collection, []string{"ABC"}, seq)
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, cap(feed), len(feed), "the feed's output buffer must fill so the goroutine blocks with its late listener still on the sentinel")
		assert.GreaterOrEqual(c, cCache.getNextSequence(), uint64(bufferFillers+2), "all in-order fillers must be cached before the spike")
	}, 30*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(0), forcedRollbacks(), "no rollback should have occurred yet - the stalled feed has not read a late sequence")

	// Create the skip: a single write far ahead leaves the whole spike range with no arrivals. Once that lone
	// pending entry ages out (shortWaitCache flushes pending after 5ms) the gap is pushed to the skipped list in
	// one range, so those sequences are now previously-skipped.
	firstSkipped := uint64(bufferFillers + 2)
	lastSkipped := uint64(bufferFillers + 1 + spikeSize)
	gapWriteSeq := lastSkipped + 1
	WriteDirect(t, collection, []string{"ABC"}, gapWriteSeq)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, cCache.WasSkipped(firstSkipped) && cCache.WasSkipped(lastSkipped),
			"the gap %d..%d should have been pushed to the skipped list", firstSkipped, lastSkipped)
	}, 10*time.Second, 5*time.Millisecond)

	// The spike: every skipped sequence resolves (arrives late) in a tight loop while the feed is provably
	// stalled (blocked on its full output buffer). Each arrival calls AddLateSequence -> _purgeLateLogEntries;
	// the length force-prune keeps the queue bounded and force-drops the sentinel that the stalled feed still
	// references, since only the length cap can reclaim a referenced entry.
	for seq := firstSkipped; seq <= lastSkipped; seq++ {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		require.NoError(t, db.WaitForSequenceNotSkipped(ctx, seq))
		// force a listener on each so compaction won't evict pass max length
		abcCache.lateLogLock.Lock()
		abcCache.lateLogs[len(abcCache.lateLogs)-1].addListener()
		abcCache.lateLogLock.Unlock()
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, uint64(0), cCache.getOldestSkippedSequence(ctx),
			"all %d skipped sequences should have resolved (arrived late)", spikeSize)
	}, 60*time.Second, 20*time.Millisecond)

	// FIX 1 - bounded: despite thousands of late arrivals behind a stalled feed, lateLogs never grew past the
	// length cap. Without compaction (the pre-fix behaviour, TestLateLogsSpikeNotPrunedUntilNewLateSequence) the
	// stalled feed would have pinned the spike in place and the queue would hold spikeSize+1 entries.
	require.Equalf(t, abcCache.lateLogCount(), int64(DefaultChannelCacheMaxLength),
		"the length force-prune must bound ABC's lateLogs at DefaultChannelCacheMaxLength (%d) even under a %d-sequence spike behind a stalled feed",
		DefaultChannelCacheMaxLength, spikeSize)
	t.Logf("spike resolved with feed stalled: ABC lateLogs bounded at %d (cap %d, spike %d), forced_rollbacks=%d",
		abcCache.lateLogCount(), DefaultChannelCacheMaxLength, spikeSize, forcedRollbacks())

	// FIX 2 - the forced rollback happens inside the real changes loop: the stalled feed's referenced sentinel
	// was force-compacted away by the length cap. Nothing has rolled back yet (the feed hasn't read a late
	// sequence). Now write a fresh in-order sequence and drain the feed: it unblocks, and its next getLateFeed
	// reads the pruned sentinel, fails, and the changes loop rolls it back (incrementing LateFeedForcedRollbacks)
	// then recovers it - eventually catching up to the latest sequence. Draining to finalSeq proves it isn't
	// left stuck.
	require.Equal(t, int64(0), forcedRollbacks(), "the stalled feed must not have rolled back before it is drained")
	finalSeq := gapWriteSeq + 1
	WriteDirect(t, collection, []string{"ABC"}, finalSeq)
	drainDeadline := time.After(60 * time.Second)
	for !seen[finalSeq] {
		select {
		case <-drainDeadline:
			t.Fatalf("feed did not recover to the latest sequence %d after the spike (rollback recovery)", finalSeq)
		default:
		}
		collectSeen(drainUntilWait(t, feed))
	}
	require.Greater(t, forcedRollbacks(), int64(0),
		"the feed's sentinel was force-compacted away by the length cap; its getLateFeed must have rolled back inside the changes loop")

	// FIX 3 - lateLogs remain bounded after recovery: the spike left no residue on the channel cache.
	require.Equal(t, abcCache.lateLogCount(), int64(DefaultChannelCacheMaxLength),
		"ABC's lateLogs must remain bounded by the length cap after the feed recovers")

	t.Logf("feed recovered to seq %d; ABC lateLogs=%d, forced_rollbacks=%d",
		finalSeq, abcCache.lateLogCount(), forcedRollbacks())
}

// TestEvictAllLateWhenFirstIteOnlyItemWithListener:
// Much like TestLateLogsSpikeForcePrunedBoundsLateLogsAndForcesRollback but the test does not force listeners on each
// entry to stop compaction from evicting past the length cap.
func TestEvictAllLateWhenFirstIteOnlyItemWithListener(t *testing.T) {
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	// The number of previously-skipped sequences that all resolve (arrive late) while the feed is stalled. Must
	// exceed the length cap so the force-prune fires.
	const spikeSize = 2000
	require.Greater(t, spikeSize, DefaultChannelCacheMaxLength,
		"the spike must exceed DefaultChannelCacheMaxLength for the length force-prune to fire")

	// In-order writes used only to fill the feed's output buffer so the goroutine blocks. Must exceed the feed's
	// hard-coded output buffer capacity (50 in changes.go); the exact block point is confirmed via len==cap below.
	const bufferFillers = 60

	// Shipped default late-log caps (DefaultChannelCacheMaxLength=500, LateLogAge=5m) - deliberately not overridden. The
	// length cap is the mechanism under test; the 5-minute age is far longer than this test so the age sweep
	// never fires.
	db, ctx := setupTestDBWithCacheOptions(t, fastFeedBroadcast(shortWaitCache()))
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user = user
	collection := collectionWithUser.DatabaseCollection
	cCache := collection.changeCache()

	forcedRollbacks := func() int64 { return db.DbStats.Cache().LateFeedForcedRollbacks.Value() }
	feed := startChangesFeed(ctx, t, collectionWithUser)
	seen := make(map[uint64]bool) // every sequence the feed delivers across its lifetime
	collectSeen := func(seqs []uint64) {
		for _, s := range seqs {
			seen[s] = true
		}
	}
	collectSeen(drainUntilWait(t, feed))

	// One in-order write so the feed is streaming normally and the cache's nextSequence advances to 2.
	WriteDirect(t, collection, []string{"ABC"}, 1)
	collectSeen(drainUntilWait(t, feed))

	cc, ok := cCache.getChannelCache().(*channelCacheImpl)
	require.True(t, ok)
	abcCacheIface, err := cc.getSingleChannelCache(ctx, channels.NewID("ABC", collection.GetCollectionID()))
	require.NoError(t, err)
	abcCache := abcCacheIface.(*singleChannelCacheImpl)
	require.Equal(t, int64(1), abcCache.lateLogCount(), "ABC starts with just its sentinel entry")

	// Stall the feed with its late-sequence listener still on the sentinel. Writing more in-order sequences than
	// the feed's output buffer holds, and never draining them, blocks the feed goroutine on its output send.
	for seq := uint64(2); seq <= bufferFillers+1; seq++ {
		WriteDirect(t, collection, []string{"ABC"}, seq)
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, cap(feed), len(feed), "the feed's output buffer must fill so the goroutine blocks with its late listener still on the sentinel")
		assert.GreaterOrEqual(c, cCache.getNextSequence(), uint64(bufferFillers+2), "all in-order fillers must be cached before the spike")
	}, 30*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(0), forcedRollbacks(), "no rollback should have occurred yet - the stalled feed has not read a late sequence")

	// Create the skip: a single write far ahead leaves the whole spike range with no arrivals.
	firstSkipped := uint64(bufferFillers + 2)
	lastSkipped := uint64(bufferFillers + 1 + spikeSize)
	gapWriteSeq := lastSkipped + 1
	WriteDirect(t, collection, []string{"ABC"}, gapWriteSeq)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, cCache.WasSkipped(firstSkipped) && cCache.WasSkipped(lastSkipped),
			"the gap %d..%d should have been pushed to the skipped list", firstSkipped, lastSkipped)
	}, 10*time.Second, 5*time.Millisecond)

	// The spike: every skipped sequence resolves (arrives late) in a tight loop while the feed is provably
	// stalled (blocked on its full output buffer). Each arrival calls AddLateSequence -> _purgeLateLogEntries;
	// the length force-prune keeps the queue bounded and force-drops the sentinel that the stalled feed
	// still references. Given all other items in th elate logs list do not have any listeners the compaction process
	// will also clean all those items leaving only the sentinel item.
	for seq := firstSkipped; seq <= lastSkipped; seq++ {
		WriteDirect(t, collection, []string{"ABC"}, seq)
		require.NoError(t, db.WaitForSequenceNotSkipped(ctx, seq))
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, uint64(0), cCache.getOldestSkippedSequence(ctx),
			"all %d skipped sequences should have resolved (arrived late)", spikeSize)
	}, 60*time.Second, 20*time.Millisecond)

	// We should only have the sentinel entry left. Once we went above the max length for late logs and removed the
	// earliest entry, this entry was the ony entry with a listener so it was safe to remove all other entries until the last entry
	require.Equal(t, abcCache.lateLogCount(), 1,
		"the length force-prune should event down to one item")
}
