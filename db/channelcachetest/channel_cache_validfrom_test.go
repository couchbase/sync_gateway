//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channelcachetest

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// maxFeedIterations bounds the simulated feed so a regression that stops it making progress fails the test
// rather than hanging it.
const maxFeedIterations = 1000

// feedBatchLimit makes the simulated feed read in small batches, so one feed exercises several GetChanges
// calls (and so several cache/query boundary crossings) rather than a single call that returns everything.
const feedBatchLimit = 7

// validFromHarness models "every write ever made" against a channel cache whose query handler is shared
// with the test. A write has two independent halves: publishing it to the bucket (seeding the query
// handler, which is what a query fallback can see) and dispatching it to the channel cache (AddToCache,
// which is what the cache feed does). write does both back to back; seedOnly and dispatch allow later tests
// to separate them, so a test can place other actions (cache creation, eviction) between the two halves.
type validFromHarness struct {
	t     *testing.T
	ctx   context.Context
	cache *db.ChannelCacheImplForTest
	qh    *db.QueryHandlerForTest
	stats *base.CacheStats

	// model records every sequence written, per channel name, so the feed's output can be checked against
	// the full history rather than against what the cache happens to hold.
	model map[string]map[uint64]struct{}
	// highestWritten is the highest sequence given to write or seedOnly, across all channels.
	highestWritten uint64
	// pending holds the sequences seeded but not yet dispatched, keyed by sequence. Goroutines started by a
	// test dispatch concurrently with the test goroutine seeding, so pendingLock guards it.
	pending     map[uint64]pendingWrite
	pendingLock sync.Mutex
}

// pendingWrite is what dispatch needs to build a fresh entry for a sequence that was only seeded.
type pendingWrite struct {
	docID    string
	channels []string
	skipped  bool
}

func newValidFromHarness(t *testing.T, mutateOptions func(*db.ChannelCacheOptions)) *validFromHarness {
	options := db.DefaultCacheOptions().ChannelCacheOptions
	if mutateOptions != nil {
		mutateOptions(&options)
	}

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	qh := &db.QueryHandlerForTest{}
	ctx := base.TestCtx(t)
	cache, err := db.NewChannelCacheForTest(t, ctx, "testDb", options, qh.AsFactory,
		channels.NewActiveChannels(&base.SgwIntStat{}), dbstats.Cache())
	require.NoError(t, err)
	t.Cleanup(func() { cache.Stop(ctx) })

	return &validFromHarness{
		t:       t,
		ctx:     ctx,
		cache:   cache,
		qh:      qh,
		stats:   dbstats.Cache(),
		model:   make(map[string]map[uint64]struct{}),
		pending: make(map[uint64]pendingWrite),
	}
}

func (h *validFromHarness) channelID(name string) channels.ID {
	return channels.NewID(name, base.DefaultCollectionID)
}

// entry builds a fresh log entry. Entries are never shared between the query handler and AddToCache, because
// AddToCache clears the entry's Channels.
func (h *validFromHarness) entry(seq uint64, docID string, channelNames []string, skipped bool) *db.LogEntry {
	entry := db.MakeLogEntry(seq, docID, "1-abc", channelNames, base.DefaultCollectionID)
	entry.Skipped = skipped
	return entry
}

// seedOnly publishes a write to the bucket model and the query handler without dispatching it to the cache.
// Pair it with dispatch to model the gap between a write becoming visible to queries and reaching the cache.
func (h *validFromHarness) seedOnly(seq uint64, channelNames ...string) {
	h.seedOnlyWithFlags(seq, false, channelNames...)
}

func (h *validFromHarness) seedOnlyWithFlags(seq uint64, skipped bool, channelNames ...string) {
	docID := fmt.Sprintf("doc_%d", seq)
	h.qh.SeedEntries(db.LogEntries{h.entry(seq, docID, channelNames, false)})
	for _, name := range channelNames {
		if h.model[name] == nil {
			h.model[name] = make(map[uint64]struct{})
		}
		h.model[name][seq] = struct{}{}
	}
	h.addPending(seq, pendingWrite{docID: docID, channels: channelNames, skipped: skipped})
	h.highestWritten = max(h.highestWritten, seq)
}

func (h *validFromHarness) addPending(seq uint64, pending pendingWrite) {
	h.pendingLock.Lock()
	defer h.pendingLock.Unlock()
	_, exists := h.pending[seq]
	require.False(h.t, exists, "sequence %d already seeded", seq)
	h.pending[seq] = pending
}

func (h *validFromHarness) takePending(seq uint64) (pendingWrite, bool) {
	h.pendingLock.Lock()
	defer h.pendingLock.Unlock()
	pending, ok := h.pending[seq]
	delete(h.pending, seq)
	return pending, ok
}

// tryDispatch sends a previously seeded sequence to the channel cache as a fresh entry. It reports a failure as
// an error rather than failing the test, so it is safe to call from a goroutine other than the test's.
func (h *validFromHarness) tryDispatch(seq uint64) error {
	pending, ok := h.takePending(seq)
	if !ok {
		return fmt.Errorf("sequence %d was not seeded, or is already dispatched", seq)
	}
	h.cache.AddToCache(h.ctx, h.entry(seq, pending.docID, pending.channels, pending.skipped))
	return nil
}

// dispatch sends a previously seeded sequence to the channel cache as a fresh entry.
func (h *validFromHarness) dispatch(seq uint64) {
	require.NoError(h.t, h.tryDispatch(seq))
}

// publish seeds a sequence and advances the publish cursor to it without dispatching, modelling a sequence that
// has been released but whose dispatch to the channel caches has not happened yet. Send it later with dispatch.
func (h *validFromHarness) publish(seq uint64, channelNames ...string) {
	h.seedOnly(seq, channelNames...)
	h.cache.AdvancePublishCursorForTest(h.t, seq)
}

// write is publish-and-dispatch: the common case of a write that is both queryable and cached.
func (h *validFromHarness) write(seq uint64, channelNames ...string) {
	h.seedOnly(seq, channelNames...)
	h.dispatch(seq)
}

// writeRange writes every sequence in [from, to], putting those where inChannel returns true in channelName
// and the rest in a channel nobody reads, so the high sequence still advances.
func (h *validFromHarness) writeRange(from, to uint64, channelName string, inChannel func(seq uint64) bool) {
	for seq := from; seq <= to; seq++ {
		if inChannel(seq) {
			h.write(seq, channelName)
		} else {
			h.write(seq, "other")
		}
	}
}

// evict removes a channel's cache, as compaction would, without touching the query handler.
func (h *validFromHarness) evict(channelName string) {
	caches := h.cache.ChannelCachesForTest(h.t)
	_, found := caches.Get(h.channelID(channelName))
	require.True(h.t, found, "channel %q has no cache to evict", channelName)
	caches.Remove(h.channelID(channelName))
}

// activeCache returns the channel's cache without creating it, or nil.
func (h *validFromHarness) activeCache(channelName string) *db.SingleChannelCacheImplForTest {
	value, found := h.cache.ChannelCachesForTest(h.t).Get(h.channelID(channelName))
	if !found {
		return nil
	}
	return db.AsSingleChannelCache(h.ctx, value)
}

// singleCache returns the channel's cache, creating it as a feed would, and requires it not to be a bypass cache.
func (h *validFromHarness) singleCache(channelName string) *db.SingleChannelCacheImplForTest {
	cache, err := db.GetSingleChannelCacheForTest(h.t, h.ctx, h.cache, h.channelID(channelName))
	require.NoError(h.t, err)
	impl, ok := db.AsSingleChannelCacheImplForTest(h.t, cache)
	require.True(h.t, ok, "channel %q was given a bypass cache", channelName)
	return impl
}

// cachedSequences reads the cache's log and the validFrom it is valid from in one lock acquisition.
func cachedSequences(cache *db.SingleChannelCacheImplForTest) (validFrom uint64, sequences []uint64) {
	validFrom, entries := cache.GetCachedChanges(db.ChangesOptions{})
	for _, entry := range entries {
		sequences = append(sequences, entry.Sequence)
	}
	return validFrom, sequences
}

// feedFrom simulates a changes feed on one channel: repeated GetChanges calls, each resuming from the last
// sequence received, until the feed has seen the highest sequence the model holds for the channel. Returns
// every sequence received, in the order received.
func (h *validFromHarness) feedFrom(channelName string, since uint64) []uint64 {
	target := since
	for seq := range h.model[channelName] {
		target = max(target, seq)
	}

	var received []uint64
	for i := 0; since < target && i < maxFeedIterations; i++ {
		options := db.ChangesOptions{
			Since:      db.SequenceID{Seq: since},
			Limit:      feedBatchLimit,
			ChangesCtx: h.ctx,
		}
		changes, err := h.cache.GetChanges(h.ctx, h.channelID(channelName), options)
		require.NoError(h.t, err)
		require.NotEmpty(h.t, changes, "feed on %q made no progress from since=%d, target=%d", channelName, since, target)
		for _, change := range changes {
			received = append(received, change.Sequence)
		}
		since = changes[len(changes)-1].Sequence
	}
	require.GreaterOrEqual(h.t, since, target, "feed on %q did not catch up within %d iterations", channelName, maxFeedIterations)
	return received
}

// expectedAbove returns the model's sequences for a channel that are strictly above since, ascending.
func (h *validFromHarness) expectedAbove(channelName string, since uint64) []uint64 {
	var expected []uint64
	for seq := range h.model[channelName] {
		if seq > since {
			expected = append(expected, seq)
		}
	}
	slices.Sort(expected)
	return expected
}

// requireExactlyOnce checks that received is exactly the model's sequences above since, so both a missing
// sequence and a duplicated one are reported.
func (h *validFromHarness) requireExactlyOnce(channelName string, since uint64, received []uint64) {
	counts := make(map[uint64]int)
	for _, seq := range received {
		counts[seq]++
	}
	var missing, duplicated, unexpected []uint64
	for _, seq := range h.expectedAbove(channelName, since) {
		switch counts[seq] {
		case 0:
			missing = append(missing, seq)
		case 1:
		default:
			duplicated = append(duplicated, seq)
		}
		delete(counts, seq)
	}
	for seq := range counts {
		unexpected = append(unexpected, seq)
	}
	slices.Sort(unexpected)
	require.True(h.t, len(missing) == 0 && len(duplicated) == 0 && len(unexpected) == 0,
		"channel %q since=%d: missing=%v duplicated=%v unexpected=%v", channelName, since, missing, duplicated, unexpected)
}

// requireNothingBelowValidFrom checks that no entry in the cache's log has a sequence below the cache's validFrom.
func requireNothingBelowValidFrom(t testing.TB, cache *db.SingleChannelCacheImplForTest) {
	validFrom, sequences := cachedSequences(cache)
	for _, seq := range sequences {
		assert.GreaterOrEqual(t, seq, validFrom, "cache holds sequence %d below its validFrom %d", seq, validFrom)
	}
}

// requireInvariants runs both invariant checks for a feed from since on the channel, against the cache
// currently held for it.
func (h *validFromHarness) requireInvariants(channelName string, since uint64) {
	h.requireExactlyOnce(channelName, since, h.feedFrom(channelName, since))
	if cache := h.activeCache(channelName); cache != nil {
		requireNothingBelowValidFrom(h.t, cache)
	}
}

func inEveryThird(seq uint64) bool {
	return seq%3 == 0
}

func TestChannelCacheValidFromNoRace(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)
	h := newValidFromHarness(t, nil)

	h.writeRange(1, 164, "A", inEveryThird)

	cache := h.singleCache("A")
	validFrom, cached := cachedSequences(cache)
	assert.Equal(t, uint64(165), validFrom)
	assert.Empty(t, cached, "nothing dispatched after creation yet")
	h.requirePrunedOnInsert(0)

	for seq := uint64(165); seq <= 175; seq++ {
		h.write(seq, "A")
	}
	_, cached = cachedSequences(cache)
	assert.Equal(t, []uint64{165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175}, cached)
	requireNothingBelowValidFrom(t, cache)

	// Below validFrom comes from the query, above it from the cache.
	for _, since := range []uint64{0, 150, 164, 165, 170, 175} {
		h.requireInvariants("A", since)
	}
}

func TestChannelCacheValidFromEvictedAndRecreated(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)
	h := newValidFromHarness(t, nil)

	h.writeRange(1, 20, "A", inEveryThird)
	first := h.singleCache("A")
	h.writeRange(21, 30, "A", func(seq uint64) bool {
		return seq%2 == 0
	})
	requireNothingBelowValidFrom(t, first)

	h.evict("A")
	// Writes while the channel has no cache reach only the query path.
	h.writeRange(31, 45, "A", func(seq uint64) bool {
		return seq%2 == 1
	})
	require.Nil(t, h.activeCache("A"))

	recreated := h.singleCache("A")
	require.NotSame(t, first, recreated)
	validFrom, _ := cachedSequences(recreated)
	assert.Equal(t, uint64(46), validFrom, "recreated cache is valid from the high sequence at recreation, plus one")

	h.writeRange(46, 60, "A", inEveryThird)
	for _, since := range []uint64{0, 10, 30, 44, 45, 50, 60} {
		h.requireInvariants("A", since)
	}
}

func TestChannelCacheValidFromBypass(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)
	h := newValidFromHarness(t, func(options *db.ChannelCacheOptions) {
		options.MaxNumChannels = 2
		options.CompactHighWatermarkPercent = 100
		options.CompactLowWatermarkPercent = 50
	})

	h.write(1, "A", "B", "C")
	h.singleCache("A")
	h.singleCache("B")

	// With the cache at capacity, C gets a bypass cache that serves everything from the query handler.
	bypass, err := db.GetSingleChannelCacheForTest(t, h.ctx, h.cache, h.channelID("C"))
	require.NoError(t, err)
	_, isSingle := db.AsSingleChannelCacheImplForTest(t, bypass)
	require.False(t, isSingle, "expected a bypass cache for C")

	for seq := uint64(2); seq <= 30; seq++ {
		h.write(seq, "A", "B", "C")
	}
	for _, since := range []uint64{0, 1, 15, 29} {
		h.requireInvariants("C", since)
	}
	assert.Nil(t, h.activeCache("C"), "bypass must not create a cache")
	assert.Greater(t, h.stats.ChannelCacheBypassCount.Value(), int64(0))
}

// A skipped (late) sequence below the cache's validFrom is never cached, so it can only reach a feed
// through the query.
func TestChannelCacheValidFromLateSequenceBelowValidFrom(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)
	h := newValidFromHarness(t, nil)

	// Every sequence is published in order, because the query handler serves entries in seeding order
	// and a real query serves them in sequence order. Sequence 100 is the one dispatch is late for.
	for seq := uint64(90); seq <= 110; seq++ {
		switch {
		case seq == 100:
			h.seedOnlyWithFlags(seq, true, "A")
		case inEveryThird(seq):
			h.seedOnly(seq, "A")
		default:
			h.seedOnly(seq, "other")
		}
	}
	for seq := uint64(90); seq <= 110; seq++ {
		if seq != 100 {
			h.dispatch(seq)
		}
	}
	cache := h.singleCache("A")
	validFrom, _ := cachedSequences(cache)
	require.Equal(t, uint64(111), validFrom)

	h.dispatch(100)
	_, cached := cachedSequences(cache)
	assert.NotContains(t, cached, uint64(100), "late sequence below validFrom must not enter the cache log")
	requireNothingBelowValidFrom(t, cache)

	for _, since := range []uint64{0, 89, 99, 100, 105} {
		h.requireInvariants("A", since)
	}
}

// fenceJoinTimeout bounds every wait on a goroutine or on the cursor, so a regression fails the test rather than
// hanging it.
const fenceJoinTimeout = 10 * time.Second

// fenceBlockedWindow is how long a goroutine that should be blocked on the cache lock is observed for. A goroutine
// that is correctly blocked is never done, so the window can only expose the broken case, never false-fail.
const fenceBlockedWindow = 100 * time.Millisecond

// requireBlocked asserts done is not signalled within the window: the goroutine that would signal it is stuck.
func requireBlocked[T any](t testing.TB, done <-chan T, window time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
		require.Fail(t, "goroutine was not blocked", msg)
	case <-time.After(window):
	}
}

// requireFinished is the counterpart to requireBlocked: it waits for the goroutine behind done to finish, and
// returns the value it sent.
func requireFinished[T any](t testing.TB, done <-chan T, msg string) (res T) {
	t.Helper()
	select {
	case v := <-done:
		return v
	case <-time.After(fenceJoinTimeout):
		require.FailNow(t, "timed out waiting for goroutine to finish", msg)
	}
	return // not reachable
}

// waitForCursor waits for the publish cursor to reach seq. AddToCache advances the cursor as its first action, so
// this tells a hook the dispatching goroutine has published before the hook goes on.
func waitForCursor(t testing.TB, cache *db.ChannelCacheImplForTest, seq uint64) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, cache.PublishCursorForTest(t), seq)
	}, fenceJoinTimeout, time.Millisecond,
		"publish cursor did not reach %d: AddToCache must advance the cursor before it looks up (and can block on) a channel cache", seq)
}

// dispatchAsync dispatches a seeded sequence on its own goroutine, delivering the result on the returned channel.
func (h *validFromHarness) dispatchAsync(seq uint64) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- h.tryDispatch(seq)
	}()
	return done
}

// dispatchBlockedOnLock starts an async dispatch, waits for it to have published, and requires it to be blocked
// (on the cache lock the calling hook holds).
func (h *validFromHarness) dispatchBlockedOnLock(seq uint64) <-chan error {
	done := h.dispatchAsync(seq)
	waitForCursor(h.t, h.cache, seq)
	requireBlocked(h.t, done, fenceBlockedWindow, fmt.Sprintf("dispatch of %d should wait for the new cache's lock", seq))
	return done
}

func (h *validFromHarness) requireDispatched(done <-chan error, seq uint64) {
	require.NoError(h.t, requireFinished(h.t, done, fmt.Sprintf("dispatch of %d", seq)))
}

// hooks installs the fence hooks on the channel cache under test.
func (h *validFromHarness) hooks(hooks db.ValidFromHooksForTest) {
	h.cache.SetValidFromHooksForTest(h.t, hooks)
}

// requireFinal runs both invariants on channel "A" for each since. requireInvariants skips the below-validFrom
// check when the channel has no cache, so the cache's presence is required first rather than letting that check
// pass vacuously.
func (h *validFromHarness) requireFinal(sinces ...uint64) {
	require.NotNil(h.t, h.activeCache("A"))
	for _, since := range sinces {
		h.requireInvariants("A", since)
	}
}

// requirePrunedOnInsert checks the pruned-on-insert counter. Call it straight after the cache is created and the
// dispatches are joined, before later writes change it.
func (h *validFromHarness) requirePrunedOnInsert(pruned int64) {
	h.t.Helper()
	assert.Equal(h.t, pruned, h.stats.ChannelCachePrunedOnInsert.Value(), "chan_cache_pruned_on_insert")
}

// fenceSinces fall below the cursor at BeforeInsert (164), between it and the cursor read after the insert (170), and above it.
var fenceSinces = []uint64{0, 150, 164, 166, 168, 170, 171, 180}

// requireValidFrom checks the cache's validFrom and its log; a nil log means the cache must be empty.
func requireValidFrom(t testing.TB, cache *db.SingleChannelCacheImplForTest, expected uint64, cachedExpected []uint64) {
	t.Helper()
	validFrom, cached := cachedSequences(cache)
	assert.Equal(t, expected, validFrom, "validFrom")
	if cachedExpected == nil {
		assert.Empty(t, cached, "cache log")
	} else {
		assert.Equal(t, cachedExpected, cached, "cache log")
	}
}

// TestChannelCacheValidFromFence drives addChannelCache's publish-cursor fence through each interleaving that
// matters, using the hooks to place dispatch at an exact point. Each case ends with both invariants: a feed from
// any since sees every sequence in the channel exactly once, and the cache holds nothing below its validFrom.
func TestChannelCacheValidFromFence(t *testing.T) {
	newHarness := func(t *testing.T, mutate func(*db.ChannelCacheOptions)) *validFromHarness {
		base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)
		return newValidFromHarness(t, mutate)
	}
	// writeMiddle writes 165..170, with 167 in channel A and the rest in a channel nobody reads.
	writeMiddle := func(h *validFromHarness) {
		for seq := uint64(165); seq <= 170; seq++ {
			if seq == 167 {
				h.write(seq, "A")
			} else {
				h.write(seq, "other")
			}
		}
	}

	t.Run("middle_range_dispatched_before_insert", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		once := false
		h.hooks(db.ValidFromHooksForTest{BeforeInsert: func(channels.ID) {
			if once {
				return
			}
			once = true
			writeMiddle(h)
		}})
		cache := h.singleCache("A")
		require.True(t, once)
		requireValidFrom(t, cache, 171, nil)
		h.requirePrunedOnInsert(0)
		h.requireExactlyOnce("A", 150, h.feedFrom("A", 150))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("middle_range_dispatched_during_window", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		var done <-chan error
		h.hooks(db.ValidFromHooksForTest{AfterInsert: func(channels.ID) {
			if done != nil {
				return
			}
			h.seedOnly(168, "A")
			done = h.dispatchBlockedOnLock(168)
		}})
		cache := h.singleCache("A")
		require.NotNil(t, done)
		h.requireDispatched(done, 168)
		requireValidFrom(t, cache, 169, nil)
		h.requirePrunedOnInsert(1)
		h.requireExactlyOnce("A", 150, h.feedFrom("A", 150))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("published_before_cursor_read_dispatched_after_fence", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		fired := 0
		h.hooks(db.ValidFromHooksForTest{
			AfterInsert: func(channels.ID) {
				h.publish(168, "other")
				h.publish(169, "A")
				h.publish(170, "other")
			},
			AfterFence: func(channels.ID) {
				fired++
				h.dispatch(168)
				h.dispatch(169)
				h.dispatch(170)
			},
		})
		cache := h.singleCache("A")
		require.Equal(t, 1, fired)
		requireValidFrom(t, cache, 171, nil)
		h.requirePrunedOnInsert(1)
		h.requireExactlyOnce("A", 150, h.feedFrom("A", 150))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("top_range_dispatched_inside_window", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		var done <-chan error
		once := false
		h.hooks(db.ValidFromHooksForTest{
			BeforeInsert: func(channels.ID) {
				if once {
					return
				}
				once = true
				writeMiddle(h)
			},
			AfterCursorRead: func(channels.ID) {
				if done != nil {
					return
				}
				h.seedOnly(172, "A")
				done = h.dispatchBlockedOnLock(172)
			},
		})
		cache := h.singleCache("A")
		require.NotNil(t, done)
		h.requireDispatched(done, 172)
		// 172 is above the cursor read, so the cache must take it.
		requireValidFrom(t, cache, 171, []uint64{172})
		h.requirePrunedOnInsert(0)
		h.requireExactlyOnce("A", 150, h.feedFrom("A", 150))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("bottom_range_in_flight", func(t *testing.T) {
		h := newHarness(t, nil)
		// 160 is published, so the cursor already covers it, but its dispatch is still in flight.
		h.writeRange(1, 159, "A", inEveryThird)
		h.publish(160, "A")
		h.writeRange(161, 164, "A", inEveryThird)
		require.Equal(t, uint64(164), h.cache.PublishCursorForTest(t))
		var done <-chan error
		h.hooks(db.ValidFromHooksForTest{AfterInsert: func(channels.ID) {
			if done == nil {
				done = h.dispatchBlockedOnLock(160)
			}
		}})
		cache := h.singleCache("A")
		require.NotNil(t, done)
		h.requireDispatched(done, 160)
		requireValidFrom(t, cache, 165, nil)
		h.requirePrunedOnInsert(1)
		h.requireExactlyOnce("A", 150, h.feedFrom("A", 150))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("late_sequence_below_cursor", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 149, "A", inEveryThird)
		h.seedOnlyWithFlags(150, true, "A")
		h.writeRange(151, 164, "A", inEveryThird)
		var done <-chan error
		h.hooks(db.ValidFromHooksForTest{AfterInsert: func(channels.ID) {
			if done == nil {
				done = h.dispatchBlockedOnLock(150)
			}
		}})
		cache := h.singleCache("A")
		require.NotNil(t, done)
		h.requireDispatched(done, 150)
		requireValidFrom(t, cache, 165, nil)
		h.requirePrunedOnInsert(1)
		var late []uint64
		for _, entry := range cache.LateLogsForTest(t) {
			late = append(late, entry.LogEntryForTest(t).Sequence)
		}
		assert.Contains(t, late, uint64(150), "late log %v", late)
		h.requireExactlyOnce("A", 140, h.feedFrom("A", 140))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("pruning_after_fence", func(t *testing.T) {
		h := newHarness(t, func(options *db.ChannelCacheOptions) {
			options.ChannelCacheMaxLength = 5
			options.ChannelCacheMinLength = 1
		})
		h.writeRange(1, 164, "A", inEveryThird)
		once := false
		h.hooks(db.ValidFromHooksForTest{BeforeInsert: func(channels.ID) {
			if once {
				return
			}
			once = true
			writeMiddle(h)
		}})
		cache := h.singleCache("A")
		requireValidFrom(t, cache, 171, nil)
		h.requirePrunedOnInsert(0)
		for seq := uint64(171); seq <= 180; seq++ {
			h.write(seq, "A")
		}
		requireValidFrom(t, cache, 176, []uint64{176, 177, 178, 179, 180})
		for _, since := range []uint64{150, 170, 175} {
			h.requireExactlyOnce("A", since, h.feedFrom("A", since))
		}
		h.requireFinal(fenceSinces...)
	})

	t.Run("two_creators_race", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		var (
			nested                       bool
			winner                       *db.SingleChannelCacheImplForTest
			inserts, cursorReads, fences int
			cursorBeforeInsert           uint64
			cursorAfterInsert            uint64
		)
		h.hooks(db.ValidFromHooksForTest{
			BeforeInsert: func(channels.ID) {
				if nested {
					return
				}
				nested = true
				cursorBeforeInsert = h.cache.PublishCursorForTest(t)
				// These miss the absent cache, and fall between the cursor at the loser's BeforeInsert and the winner's cursor read.
				h.write(165, "other")
				h.write(166, "A")
				h.write(167, "other")
				var ok bool
				winner, ok = h.cache.AddChannelCacheForTest(t, h.ctx, h.channelID("A"))
				require.True(t, ok)
			},
			AfterInsert: func(channels.ID) {
				inserts++
				h.publish(168, "other")
				h.publish(169, "A")
			},
			AfterCursorRead: func(channels.ID) {
				cursorReads++
				cursorAfterInsert = h.cache.PublishCursorForTest(t)
			},
			AfterFence: func(channels.ID) {
				fences++
				h.dispatch(168)
				h.dispatch(169)
			},
		})
		got, ok := h.cache.AddChannelCacheForTest(t, h.ctx, h.channelID("A"))
		require.True(t, ok)
		require.Equal(t, uint64(164), cursorBeforeInsert)
		assert.Same(t, winner, got, "both creators must end up with the cache that won the insert")
		assert.Same(t, winner, h.activeCache("A"))
		assert.Equal(t, 1, inserts, "AfterInsert")
		assert.Equal(t, 1, cursorReads, "AfterCursorRead")
		assert.Equal(t, 1, fences, "AfterFence")
		// The winner read the cursor at 169 after the insert, and 169 was dispatched only after the fence.
		assert.Equal(t, uint64(169), cursorAfterInsert)
		requireValidFrom(t, got, 170, nil)
		h.requirePrunedOnInsert(1)
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("dispatcher_blocked_across_fence", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		var done <-chan error
		h.hooks(db.ValidFromHooksForTest{
			AfterInsert: func(channels.ID) {
				if done == nil {
					h.publish(168, "A")
				}
			},
			AfterCursorRead: func(channels.ID) {
				if done == nil {
					done = h.dispatchBlockedOnLock(168)
				}
			},
		})
		cache := h.singleCache("A")
		require.NotNil(t, done)
		h.requireDispatched(done, 168)
		// The cursor read saw 168, so 168 is below validFrom and must never become visible in the cache.
		requireValidFrom(t, cache, 169, nil)
		h.requirePrunedOnInsert(1)
		h.requireExactlyOnce("A", 150, h.feedFrom("A", 150))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})

	t.Run("evicted_and_recreated_mid_dispatch", func(t *testing.T) {
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		first := h.singleCache("A")
		h.writeRange(165, 168, "A", func(seq uint64) bool {
			return seq%2 == 0
		})
		h.evict("A")
		// Written while the channel has no cache, so they reach only the query path.
		h.writeRange(169, 170, "A", func(uint64) bool {
			return true
		})
		once := false
		h.hooks(db.ValidFromHooksForTest{BeforeInsert: func(channels.ID) {
			if once {
				return
			}
			once = true
			h.writeRange(171, 172, "A", func(uint64) bool {
				return true
			})
		}})
		recreated := h.singleCache("A")
		require.NotSame(t, first, recreated)
		requireValidFrom(t, recreated, 173, nil)
		h.requirePrunedOnInsert(0)
		h.write(173, "A")
		h.requireFinal(0, 150, 164, 168, 170, 172, 173)
	})

	t.Run("reader_during_window", func(t *testing.T) {
		// A reader that finds the cache between its insert and the validFrom assignment must not act on an unassigned
		// validFrom. Were it to use the placeholder, the reader would query too little and take the rest from the
		// cache; 167 was dispatched before the insert, so it is in neither, and it would be lost for that reader for
		// good. The cache lock held across the window makes the reader wait for the assigned validFrom (169), so 167
		// and 168 both come from the query.
		type readResult struct {
			sequences []uint64
			err       error
		}
		h := newHarness(t, nil)
		h.writeRange(1, 164, "A", inEveryThird)
		var (
			started     bool
			dispatched  chan struct{} // closed once the dispatch returns; dispatchErr is set before that
			dispatchErr error
			readDone    chan struct{} // closed once the reader returns; read is set before that
			read        readResult
		)
		h.hooks(db.ValidFromHooksForTest{
			BeforeInsert: func(channels.ID) {
				if !started {
					h.write(167, "A")
				}
			},
			AfterInsert: func(channels.ID) {
				if started {
					return
				}
				started = true
				h.seedOnly(168, "A")
				dispatched = make(chan struct{})
				go func() {
					defer close(dispatched)
					dispatchErr = h.tryDispatch(168)
				}()
				waitForCursor(t, h.cache, 168)
				readDone = make(chan struct{})
				go func() {
					defer close(readDone)
					changes, err := h.cache.GetChanges(h.ctx, h.channelID("A"), db.ChangesOptions{
						Since:      db.SequenceID{Seq: 150},
						ChangesCtx: h.ctx,
					})
					result := readResult{err: err}
					for _, change := range changes {
						result.sequences = append(result.sequences, change.Sequence)
					}
					read = result
				}()
				// Correctly blocked, neither goroutine finishes; this only gives a broken implementation time to
				// let them through before the fence is finished.
				select {
				case <-dispatched:
				case <-time.After(fenceBlockedWindow):
				}
				select {
				case <-readDone:
				case <-time.After(fenceBlockedWindow):
				}
			},
		})
		cache := h.singleCache("A")
		require.True(t, started)
		requireFinished(t, dispatched, "dispatch of 168")
		require.NoError(t, dispatchErr)
		requireFinished(t, readDone, "reader")
		require.NoError(t, read.err)
		h.requireExactlyOnce("A", 150, read.sequences)
		assert.Equal(t, []uint64{153, 156, 159, 162, 167, 168}, read.sequences)
		// The reader's query was cached by prependChanges, so the cache now reaches back below 169; it must still
		// agree with the model.
		validFrom, _ := cachedSequences(cache)
		assert.LessOrEqual(t, validFrom, uint64(169))
		h.write(173, "A")
		h.requireFinal(fenceSinces...)
	})
}

// TestChannelCacheValidFromAfterInit checks that a cache created before any dispatch is valid from the sequence
// the channel cache was initialised at, plus one. The cache holds nothing from before Init, so a lower validFrom
// would let a feed from an earlier since be served from the empty cache instead of falling back to a query.
func TestChannelCacheValidFromAfterInit(t *testing.T) {
	h := newValidFromHarness(t, nil)
	h.cache.Init(100)

	cache := h.singleCache("A")
	requireValidFrom(t, cache, 101, nil)
}

const (
	fenceBenchHotChannels  = 8  // channels with a pre-created cache that entries are dispatched to
	fenceBenchColdChannels = 50 // pool of channels that never have a cache, so lookups of absent caches happen
	fenceBenchColdPerEntry = 3  // cold channels each entry is also in
)

// BenchmarkChannelCacheFenceContention measures how long AddToCache callers stall while hot channels' caches are
// evicted and recreated at a fixed, paced rate (as compaction followed by a new request would). Each entry is in one
// hot channel plus a few cold ones. It reports per-AddToCache latency percentiles and per-creation latency. It uses
// only test APIs that also exist before the publish cursor was introduced, so the same file runs against older code.
func BenchmarkChannelCacheFenceContention(b *testing.B) {
	for _, dispatchers := range []int{1, 8} {
		for _, rate := range []int{0, 100, 1000} {
			b.Run(fmt.Sprintf("dispatchers=%d/recreateRate=%d", dispatchers, rate), func(b *testing.B) {
				benchChannelCacheFence(b, dispatchers, rate)
			})
		}
	}
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return float64(sorted[idx].Nanoseconds())
}

func benchChannelCacheFence(b *testing.B, numDispatchers, recreateRate int) {
	b.ReportAllocs()
	// Each cache creation logs; keep that out of the benchmark output.
	base.SetUpBenchmarkLogging(b, base.LevelError, base.KeyNone)

	options := db.DefaultCacheOptions().ChannelCacheOptions
	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)
	qh := &db.QueryHandlerForTest{}
	ctx := base.TestCtx(b)
	cache, err := db.NewChannelCacheForTest(b, ctx, "testDb", options, qh.AsFactory,
		channels.NewActiveChannels(&base.SgwIntStat{}), dbstats.Cache())
	require.NoError(b, err)
	defer cache.Stop(ctx)

	hotIDs := make([]channels.ID, fenceBenchHotChannels)
	hotNames := make([]string, fenceBenchHotChannels)
	for i := range hotIDs {
		hotNames[i] = fmt.Sprintf("hot_%d", i)
		hotIDs[i] = channels.NewID(hotNames[i], base.DefaultCollectionID)
		_, err = db.GetSingleChannelCacheForTest(b, ctx, cache, hotIDs[i])
		require.NoError(b, err)
	}

	// Entries are built up front: AddToCache clears Channels, so they can't be reused.
	n := uint64(b.N)
	entries := make([]*db.LogEntry, n)
	for i := range entries {
		names := make([]string, 0, 1+fenceBenchColdPerEntry)
		names = append(names, hotNames[i%fenceBenchHotChannels])
		for j := range fenceBenchColdPerEntry {
			names = append(names, fmt.Sprintf("cold_%d", (i*fenceBenchColdPerEntry+j)%fenceBenchColdChannels))
		}
		entries[i] = db.MakeLogEntry(0, fmt.Sprintf("doc_%d", i), "1-abc", names, base.DefaultCollectionID)
	}

	var nextIndex, nextSeq atomic.Uint64
	next := func() uint64 {
		return nextIndex.Add(1) - 1
	}

	// Per-dispatcher latency slices, pre-allocated so the hot loop touches no shared state.
	latencies := make([][]time.Duration, numDispatchers)
	for d := range latencies {
		latencies[d] = make([]time.Duration, 0, int(n)/numDispatchers+4096)
	}

	recCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var recreateDone sync.WaitGroup
	var creationLat []time.Duration // owned by the recreator until recreateDone.Wait()
	if recreateRate > 0 {
		creationLat = make([]time.Duration, 0, recreateRate*30)
		recreateDone.Add(1)
		go func() {
			defer recreateDone.Done()
			caches := cache.ChannelCachesForTest(b)
			ticker := time.NewTicker(time.Second / time.Duration(recreateRate))
			defer ticker.Stop()
			round := 0
			for recCtx.Err() == nil {
				select {
				case <-recCtx.Done():
				case <-ticker.C:
					id := hotIDs[round%fenceBenchHotChannels]
					round++
					caches.Remove(id)
					start := time.Now()
					_, getErr := db.GetSingleChannelCacheForTest(b, ctx, cache, id)
					if getErr == nil {
						creationLat = append(creationLat, time.Since(start))
					}
				}
			}
		}()
	}

	var dispatchers sync.WaitGroup
	b.ResetTimer()
	for d := range numDispatchers {
		dispatchers.Add(1)
		go func() {
			defer dispatchers.Done()
			lat := latencies[d]
			for i := next(); i < n; i = next() {
				entry := entries[i]
				entry.Sequence = nextSeq.Add(1)
				start := time.Now()
				cache.AddToCache(ctx, entry)
				lat = append(lat, time.Since(start))
			}
			latencies[d] = lat
		}()
	}
	dispatchers.Wait()
	b.StopTimer()
	elapsed := b.Elapsed()
	cancel(nil)
	recreateDone.Wait()

	all := make([]time.Duration, 0, n)
	for _, l := range latencies {
		all = append(all, l...)
	}
	slices.Sort(all)
	b.ReportMetric(percentile(all, 0.50), "p50-ns")
	b.ReportMetric(percentile(all, 0.99), "p99-ns")
	b.ReportMetric(percentile(all, 0.999), "p99.9-ns")
	b.ReportMetric(percentile(all, 1.0), "max-ns")

	if recreateRate > 0 {
		var sum, maxLat time.Duration
		for _, l := range creationLat {
			sum += l
			maxLat = max(maxLat, l)
		}
		if len(creationLat) > 0 {
			b.ReportMetric(float64(sum.Nanoseconds())/float64(len(creationLat)), "create-mean-ns")
		}
		b.ReportMetric(float64(maxLat.Nanoseconds()), "create-max-ns")
		b.ReportMetric(float64(len(creationLat))/elapsed.Seconds(), "recreations/sec")
	}
}
