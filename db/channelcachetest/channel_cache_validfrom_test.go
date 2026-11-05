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
	"testing"

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
	// pending holds the sequences seeded but not yet dispatched, keyed by sequence.
	pending map[uint64]pendingWrite
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
	_, exists := h.pending[seq]
	require.False(h.t, exists, "sequence %d already seeded", seq)
	docID := fmt.Sprintf("doc_%d", seq)
	h.qh.SeedEntries(db.LogEntries{h.entry(seq, docID, channelNames, false)})
	for _, name := range channelNames {
		if h.model[name] == nil {
			h.model[name] = make(map[uint64]struct{})
		}
		h.model[name][seq] = struct{}{}
	}
	h.pending[seq] = pendingWrite{docID: docID, channels: channelNames, skipped: skipped}
	h.highestWritten = max(h.highestWritten, seq)
}

// dispatch sends a previously seeded sequence to the channel cache as a fresh entry.
func (h *validFromHarness) dispatch(seq uint64) {
	pending, ok := h.pending[seq]
	require.True(h.t, ok, "sequence %d was not seeded, or is already dispatched", seq)
	delete(h.pending, seq)
	h.cache.AddToCache(h.ctx, h.entry(seq, pending.docID, pending.channels, pending.skipped))
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
