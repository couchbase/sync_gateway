//  Copyright 2015-Present Couchbase, Inc.
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
	"log"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestChannelCacheMaxSize(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	cache := db.changeCache.getChannelCache()

	collectionID := GetSingleDatabaseCollection(t, db.DatabaseContext).GetCollectionID()

	// Make channels active
	_, err := cache.GetChanges(ctx, channels.NewID("TestA", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)
	_, err = cache.GetChanges(ctx, channels.NewID("TestB", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)
	_, err = cache.GetChanges(ctx, channels.NewID("TestC", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)
	_, err = cache.GetChanges(ctx, channels.NewID("TestD", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)

	// Add some entries to caches, leaving some empty caches
	cache.AddToCache(ctx, logEntry(1, "doc1", "1-a", []string{"TestB", "TestC", "TestD"}, collectionID))
	cache.AddToCache(ctx, logEntry(2, "doc2", "1-a", []string{"TestB", "TestC", "TestD"}, collectionID))
	cache.AddToCache(ctx, logEntry(3, "doc3", "1-a", []string{"TestB", "TestC", "TestD"}, collectionID))
	cache.AddToCache(ctx, logEntry(4, "doc4", "1-a", []string{"TestC"}, collectionID))

	db.UpdateCalculatedStats(ctx)

	maxEntries := db.DbStats.Cache().ChannelCacheMaxEntries.Value()
	assert.Equal(t, 4, int(maxEntries))
}

// TestChannelCacheCurrentVersion:
//   - Makes channel channels active for channels used in test by requesting changes on each channel
//   - Add 4 docs to the channel cache with CV defined in the log entry
//   - Get changes for each channel in question and assert that the CV is populated in each entry expected
func TestChannelCacheCurrentVersion(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	cache := db.changeCache.getChannelCache()

	collectionID := GetSingleDatabaseCollection(t, db.DatabaseContext).GetCollectionID()

	// Make channels active
	_, err := cache.GetChanges(ctx, channels.NewID("chanA", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)
	_, err = cache.GetChanges(ctx, channels.NewID("chanB", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)
	_, err = cache.GetChanges(ctx, channels.NewID("chanC", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)
	_, err = cache.GetChanges(ctx, channels.NewID("chanD", collectionID), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err)

	cache.AddToCache(ctx, testLogEntryWithCV(1, "doc1", "1-a", []string{"chanB", "chanC", "chanD"}, collectionID, "test1", 123))
	cache.AddToCache(ctx, testLogEntryWithCV(2, "doc2", "1-a", []string{"chanB", "chanC", "chanD"}, collectionID, "test2", 1234))
	cache.AddToCache(ctx, testLogEntryWithCV(3, "doc3", "1-a", []string{"chanC", "chanD"}, collectionID, "test3", 12345))
	cache.AddToCache(ctx, testLogEntryWithCV(4, "doc4", "1-a", []string{"chanC"}, collectionID, "test4", 123456))

	// assert on channel cache entries for 'chanC'
	entriesChanC, err := cache.GetChanges(ctx, channels.NewID("chanC", collectionID), getChangesOptionsWithZeroSeq(t))
	assert.NoError(t, err)
	require.Len(t, entriesChanC, 4)
	assert.True(t, verifyChannelSequences(entriesChanC, []uint64{1, 2, 3, 4}))
	assert.True(t, verifyChannelDocIDs(entriesChanC, []string{"doc1", "doc2", "doc3", "doc4"}))
	assert.True(t, verifyCVEntries(entriesChanC, []cvValues{{source: "test1", version: 123}, {source: "test2", version: 1234}, {source: "test3", version: 12345}, {source: "test4", version: 123456}}))

	// assert on channel cache entries for 'chanD'
	entriesChanD, err := cache.GetChanges(ctx, channels.NewID("chanD", collectionID), getChangesOptionsWithZeroSeq(t))
	assert.NoError(t, err)
	require.Len(t, entriesChanD, 3)
	assert.True(t, verifyChannelSequences(entriesChanD, []uint64{1, 2, 3}))
	assert.True(t, verifyChannelDocIDs(entriesChanD, []string{"doc1", "doc2", "doc3"}))
	assert.True(t, verifyCVEntries(entriesChanD, []cvValues{{source: "test1", version: 123}, {source: "test2", version: 1234}, {source: "test3", version: 12345}}))

	// assert on channel cache entries for 'chanB'
	entriesChanB, err := cache.GetChanges(ctx, channels.NewID("chanB", collectionID), getChangesOptionsWithZeroSeq(t))
	assert.NoError(t, err)
	require.Len(t, entriesChanB, 2)
	assert.True(t, verifyChannelSequences(entriesChanB, []uint64{1, 2}))
	assert.True(t, verifyChannelDocIDs(entriesChanB, []string{"doc1", "doc2"}))
	assert.True(t, verifyCVEntries(entriesChanB, []cvValues{{source: "test1", version: 123}, {source: "test2", version: 1234}}))
}

func getCacheUtilization(stats *base.CacheStats) (active, tombstones, removals int) {
	active = int(stats.ChannelCacheRevsActive.Value())
	tombstones = int(stats.ChannelCacheRevsTombstone.Value())
	removals = int(stats.ChannelCacheRevsRemoval.Value())

	return active, tombstones, removals
}

// Test Cases
// - simple compact
// - validate compaction stops at LWM
// - validate compaction continues
// - multiple calls to start
// - compact with concurrent additions to cache
// - getChanges when cache full

func TestChannelCacheSimpleCompact(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	// Define cache with max channels 20, hwm will be 16, low water mark will be 12
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(base.TestCtx(t), "testDb", options, testQueryHandlerFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	require.NoError(t, err)

	// Add 16 channels to the cache.  Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 16; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		cache.addChannelCache(ctx, channels.NewID(channelName, base.DefaultCollectionID))
	}
	// Validate cache size
	assert.Equal(t, 16, cache.channelCaches.Length())

	// Add another channel to cache
	cache.addChannelCache(ctx, channels.NewID("chan_17", base.DefaultCollectionID))

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 12, cache.channelCaches.Length())

}

func TestChannelCacheCompactInactiveChannels(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	// Define cache with max channels 20, watermarks 50/90
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 50

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)

	ctx := base.TestCtx(t)
	cache, err := newChannelCache(base.TestCtx(t), "testDb", options, testQueryHandlerFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	// Add 16 channels to the cache.  Mark odd channels as active, even channels as inactive.
	// Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 18; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		cache.addChannelCache(ctx, channel)
		if i%2 == 1 {
			log.Printf("Marking channel %q as active", channel)
			activeChannels.IncrChannel(channel)
		}
	}
	// Validate cache size
	assert.Equal(t, 18, cache.channelCaches.Length())

	log.Printf("adding 19th element to cache...")
	// Add another channel to cache, should trigger compaction
	cache.addChannelCache(ctx, channels.NewID("chan_19", base.DefaultCollectionID))

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 10, cache.channelCaches.Length())

	// Validate active channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		_, isCached := cache.channelCaches.Get(channel)
		if i%2 == 1 {
			assert.True(t, isCached, fmt.Sprintf("Channel %q was active, should be retained in cache", channel))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Channel %q was inactive, should be evicted from cache", channel))
		}
	}

}

// TestChannelCacheCompactNRU tests compaction where a subset of the channels are marked as recently used
// between compact triggers.  In the second compact, NRU channels should have eviction priority.
func TestChannelCacheCompactNRU(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	// Define cache with max channels 20, watermarks 50/90
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 70

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(base.TestCtx(t), "testDb", options, testQueryHandlerFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	// Add 18 channels to the cache.  Mark channels 1-10 as active
	// Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 18; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		cache.addChannelCache(ctx, channel)
		if i <= 10 {
			log.Printf("Marking channel %q as active", channel)
			activeChannels.IncrChannel(channel)
		}
	}
	// Validate cache size
	assert.Equal(t, 18, cache.channelCaches.Length())

	// Add another channel to cache, should trigger compaction
	cache.addChannelCache(ctx, channels.NewID("chan_19", base.DefaultCollectionID))
	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Expect channels 1-10, 11-15 to be evicted, and all to be marked as NRU during compaction
	assert.Equal(t, 14, cache.channelCaches.Length())

	// Validate recently used channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		_, isCached := cache.channelCaches.Get(channel)
		if i <= 10 || i > 15 {
			assert.True(t, isCached, fmt.Sprintf("Expected %q to be cached", channel))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Expected %q to not be cached", channel))
		}
	}

	// Mark channels 1-5 as recently used
	for i := 1; i <= 5; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		cacheElement, isCached := cache.channelCaches.Get(channel)
		assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached during recently used update", channel))
		AsSingleChannelCache(ctx, cacheElement).recentlyUsed.Set(true)
	}

	// Add new channels to trigger compaction.  At start of compaction, expect:
	//    Channels 1-5: inactive, recently used (manually updated)
	//    Channels 6-14: inactive, not recently used
	//    Channels 15-19: inactive, recently used (first compact since creation)
	for i := 1; i <= 19; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		if i <= 10 {
			log.Printf("Marking channel %q as inactive", channel)
			activeChannels.DecrChannel(ctx, channel)
		} else {
			cache.addChannelCache(ctx, channel)
		}
	}

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	//   1-5 are inactive, recently used
	//   6-14 are inactive, not recently used
	//   15-19 were recently used (added)
	//   Need to compact 5 channels to reach LRU
	// Expect channels 1-5, 11-19 to be retained in cache
	assert.Equal(t, 14, cache.channelCaches.Length())
	// Validate recently used channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", i), base.DefaultCollectionID)
		_, isCached := cache.channelCaches.Get(channel)
		if i <= 5 || i >= 11 {
			assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached", channel))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Expected %s to not be cached", channel))
		}
	}
}

// TestChannelCacheHighLoadCache validates behaviour under high query load when the total number of channels is lower than
// or equal to the CompactHighWatermark
func TestChannelCacheHighLoadCacheHit(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyCache)

	// Define cache with max channels 20, watermarks 50/90
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 100
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 70

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	queryHandler := &testQueryHandler{}
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(ctx, "testDb", options, queryHandler.asFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	channelCount := 90
	// define channel set
	channelNames := make([]string, 0)
	for i := 1; i <= channelCount; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		channelNames = append(channelNames, channelName)
	}

	// Seed the query handler with a single doc that's in all the channels
	queryEntry := testLogEntryForChannels(1, channelNames)
	queryHandler.seedEntries(LogEntries{queryEntry})

	// Send entry to the cache.  Don't reuse queryEntry here, as AddToCache strips out the channels property
	logEntry := testLogEntryForChannels(1, channelNames)
	cache.AddToCache(ctx, logEntry)

	workerCount := 25
	getChangesCount := 400
	// Start [workerCount] goroutines, each issuing [getChangesCount] changes queries against a random channel

	var workerWg sync.WaitGroup
	for range workerCount {
		workerWg.Add(1)
		go func() {
			changesSuccessCount := 0
			for range getChangesCount {
				channelNumber := rand.Intn(channelCount) + 1
				channel := channels.NewID(fmt.Sprintf("chan_%d", channelNumber), base.DefaultCollectionID)
				options := getChangesOptionsWithCtxOnly(t)
				changes, err := cache.GetChanges(base.TestCtx(t), channel, options)
				if len(changes) == 1 {
					changesSuccessCount++
				}
				assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %s", channel))
				assert.True(t, len(changes) == 1, "Expected one change per channel")
			}
			assert.Equal(t, changesSuccessCount, getChangesCount)
			workerWg.Done()
		}()

	}
	workerWg.Wait()

	log.Printf("Query count: %d, Changes count:%d", queryHandler.queryCount, workerCount*getChangesCount)

	// Expect only a single query per channel (cache initialization)
	assert.Equal(t, queryHandler.queryCount, channelCount)
}

// TestChannelCacheHighLoadCache validates behaviour under high query load when the total number of channels is much higher than
// CompactHighWatermark.  Validates that all changes requests return the expected response, even for queries issued while compaction is
// active.
func TestChannelCacheHighLoadCacheMiss(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelWarn, base.KeyCache)

	// Define cache with max channels 100, watermarks 90/70
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 100
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 70

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	queryHandler := &testQueryHandler{}
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(ctx, "testDb", options, queryHandler.asFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	channelCount := 200
	// define channel set
	channelNames := make([]string, 0)
	for i := 1; i <= channelCount; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		channelNames = append(channelNames, channelName)
	}

	// Seed the query handler with a single doc that's in all the channels
	queryEntry := testLogEntryForChannels(1, channelNames)
	queryHandler.seedEntries(LogEntries{queryEntry})

	// Send entry to the cache.  Don't reuse queryEntry here, as AddToCache strips out the channels property
	logEntry := testLogEntryForChannels(1, channelNames)
	cache.AddToCache(ctx, logEntry)

	workerCount := 25
	getChangesCount := 400
	// Start [workerCount] goroutines, each issuing [getChangesCount] changes queries against a random channel

	var workerWg sync.WaitGroup
	for range workerCount {
		workerWg.Add(1)
		go func() {
			changesSuccessCount := 0
			for range getChangesCount {
				channelNumber := rand.Intn(channelCount) + 1
				channel := channels.NewID(fmt.Sprintf("chan_%d", channelNumber), base.DefaultCollectionID)
				options := getChangesOptionsWithCtxOnly(t)
				changes, err := cache.GetChanges(base.TestCtx(t), channel, options)
				if len(changes) == 1 {
					changesSuccessCount++
				}
				assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %q", channel))
				assert.True(t, len(changes) == 1, "Expected one change per channel")
			}
			assert.Equal(t, changesSuccessCount, getChangesCount)
			workerWg.Done()
		}()

	}
	workerWg.Wait()

	log.Printf("Query count: %d, Changes count:%d", queryHandler.queryCount, workerCount*getChangesCount)
}

// TestChannelCacheBypass validates that the bypass 'cache' is used when the cache max_num_channels is reached.
// To force this scenario, HWM is set to 100%, which effectively disables compaction.
func TestChannelCacheBypass(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelWarn, base.KeyCache)

	// Define cache with max channels 20, watermarks 50/100
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20
	options.CompactHighWatermarkPercent = 100
	options.CompactLowWatermarkPercent = 50

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()
	queryHandler := &testQueryHandler{}
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	ctx := base.TestCtx(t)
	cache, err := newChannelCache(ctx, "testDb", options, queryHandler.asFactory, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")
	defer cache.Stop(ctx)

	channelCount := 100
	// define channel set
	channelNames := make([]string, 0)
	for i := 1; i <= channelCount; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		channelNames = append(channelNames, channelName)
	}

	// Seed the query handler with a single doc that's in all the channels
	queryEntry := testLogEntryForChannels(1, channelNames)
	queryHandler.seedEntries(LogEntries{queryEntry})

	// Send entry to the cache.  Don't reuse queryEntry here, as AddToCache strips out the channels property
	logEntry := testLogEntryForChannels(1, channelNames)
	cache.AddToCache(ctx, logEntry)

	// Issue queries for all channels.  First 20 should end up in the cache, remaining 80 should trigger bypass
	for c := 1; c <= channelCount; c++ {
		channel := channels.NewID(fmt.Sprintf("chan_%d", c), base.DefaultCollectionID)
		options := getChangesOptionsWithCtxOnly(t)
		changes, err := cache.GetChanges(base.TestCtx(t), channel, options)
		assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %q", channel))
		assert.True(t, len(changes) == 1, "Expected one change per channel")
	}

	// check bypass count stat
	bypassCountStat := testStats.ChannelCacheBypassCount
	require.NotNil(t, bypassCountStat)
	assert.Equal(t, 80, int(bypassCountStat.Value()))
}

func waitForCompaction(cache *channelCacheImpl) (compactionComplete bool) {
	for i := 0; i <= 10; i++ {
		if cache.compactRunning.IsTrue() {
			time.Sleep(100 * time.Millisecond)
		} else {
			return true
		}
	}
	return false
}

// Used for singleChannelCache testing with non-shared testQueryHandler
func testQueryHandlerFactory(collectionID uint32) (ChannelQueryHandler, error) {
	return &testQueryHandler{}, nil
}

type testQueryHandler struct {
	entries    LogEntries
	queryCount int
	lock       sync.RWMutex
}

// Used to initialize channel cache with a shared, single TestQueryHandler
func (qh *testQueryHandler) asFactory(collectionID uint32) (ChannelQueryHandler, error) {
	return qh, nil
}

func (qh *testQueryHandler) getChangesInChannelFromQuery(ctx context.Context, channel string, startSeq, endSeq uint64, limit int, activeOnly bool) (LogEntries, error) {
	queryEntries := make(LogEntries, 0)
	qh.lock.RLock()
	for _, entry := range qh.entries {
		_, ok := entry.Channels[channel]
		if ok {
			if activeOnly && !entry.IsActive() {
				continue
			}
			queryEntries = append(queryEntries, entry)
			if limit > 0 && len(queryEntries) >= limit {
				break
			}
		}
	}
	qh.lock.RUnlock()

	qh.lock.Lock()
	qh.queryCount++
	qh.lock.Unlock()
	return queryEntries, nil
}

func (qh *testQueryHandler) seedEntries(seededEntries LogEntries) {
	qh.lock.Lock()
	qh.entries = append(qh.entries, seededEntries...)
	qh.lock.Unlock()
}

func TestChannelCacheBackgroundTaskWithIllegalTimeInterval(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelWarn, base.KeyCache)
	options := DefaultCacheOptions().ChannelCacheOptions

	// Specify illegal time interval for background task. Time interval should be > 0
	options.ChannelCacheAge = 0
	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	testStats := dbstats.Cache()

	queryHandler := &testQueryHandler{}
	activeChannelStat := &base.SgwIntStat{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)

	cache, err := newChannelCache(base.TestCtx(t), "testDb", options, queryHandler.asFactory, activeChannels, testStats)
	assert.Error(t, err, "Background task error whilst creating channel cache")
	assert.Nil(t, cache)

	backgroundTaskError, ok := err.(*BackgroundTaskError)
	require.True(t, ok)
	assert.Equal(t, "CleanAgedItems", backgroundTaskError.TaskName)
	assert.Equal(t, options.ChannelCacheAge, backgroundTaskError.Interval)
}

// - The channel cache is validFrom sequence n, with one active mutation resident in the cache
// - There are channel removals (only) in the bucket with m < sequence < n
// - Client issues a GetChanges request with since=m
func TestChannelCacheActiveOnlyAndLimit(t *testing.T) {
	ctx, db, collection := setupDBWithChannelCacheSize(t, 2)

	const (
		activeChannel   = "active"
		inactiveChannel = "inactive"
		doc1            = "doc1"
		doc2            = "doc2"
		doc3            = "doc3"
	)

	// doc1 rev1: channel active
	// doc1 rev2: channel inactive
	// doc2 rev1: channel active
	// doc2 rev2: channel inactive
	// doc3 rev1: channel active
	revID, _, err := collection.Put(ctx, doc1, Body{"channels": activeChannel})
	require.NoError(t, err)
	_, _, err = collection.Put(ctx, doc1, Body{"channels": inactiveChannel, "_rev": revID})
	require.NoError(t, err)
	revID, _, err = collection.Put(ctx, doc2, Body{"channels": activeChannel})
	require.NoError(t, err)
	_, _, err = collection.Put(ctx, doc2, Body{"channels": inactiveChannel, "_rev": revID})
	require.NoError(t, err)

	_, _, err = collection.Put(ctx, doc3, Body{"channels": activeChannel})
	require.NoError(t, err)

	db.WaitForPendingChanges(t)

	// prime channel cache, doc2 and doc3 should be in cache
	changesOptions := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: false,
		ChangesCtx: base.TestCtx(t),
	}
	require.Len(t, getChanges(t, collection, base.SetOf(activeChannel), changesOptions), 3)

	// whether limit or no limit, should only be 1 active entry
	for _, limit := range []int{0, 1} {
		t.Run("limit="+fmt.Sprint(limit), func(t *testing.T) {
			changesOptions = ChangesOptions{
				Since:      SequenceID{Seq: 0},
				ActiveOnly: true,
				ChangesCtx: base.TestCtx(t),
				Limit:      limit,
			}
			require.Len(t, getChanges(t, collection, base.SetOf(activeChannel), changesOptions), 1)
		})
	}
}

func TestChannelCacheActiveOnlyScenarios(t *testing.T) {
	const activeChannel = "active"

	t.Run("query returns an active rev, cache is all removals", func(t *testing.T) {
		ctx, db, collection := setupDBWithChannelCacheSize(t, 2)

		// doc1: active (seq 1) - will be in backing store, not cache
		_, _, _ = collection.Put(ctx, "doc1", Body{"channels": activeChannel})

		// doc2: active (seq 2) -> inactive (seq 3) - seq 3 in cache
		revID2, _, _ := collection.Put(ctx, "doc2", Body{"channels": activeChannel})
		_, _, _ = collection.Put(ctx, "doc2", Body{"channels": "other", "_rev": revID2})

		// doc3: active (seq 4) -> inactive (seq 5) - seq 5 in cache
		revID3, _, _ := collection.Put(ctx, "doc3", Body{"channels": activeChannel})
		_, _, _ = collection.Put(ctx, "doc3", Body{"channels": "other", "_rev": revID3})

		db.WaitForPendingChanges(t)

		// With limit 1 (before)
		changesOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ActiveOnly: true, Limit: 1, ChangesCtx: base.TestCtx(t)}
		changes := getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 1)
		assert.Equal(t, "doc1", changes[0].ID)

		// No limit
		changesOptions.Limit = 0
		changes = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 1)
		assert.Equal(t, "doc1", changes[0].ID)

		// With limit 1 (after)
		changesOptions.Limit = 1
		changes = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 1)
		assert.Equal(t, "doc1", changes[0].ID)
	})

	t.Run("query returns an active rev, cache also has an active rev", func(t *testing.T) {
		ctx, db, collection := setupDBWithChannelCacheSize(t, 2)

		// doc1: active (seq 1) - backing store
		_, _, _ = collection.Put(ctx, "doc1", Body{"channels": activeChannel})

		// doc2: active (seq 2) -> inactive (seq 3) - cache
		revID2, _, _ := collection.Put(ctx, "doc2", Body{"channels": activeChannel})
		_, _, _ = collection.Put(ctx, "doc2", Body{"channels": "other", "_rev": revID2})

		// doc3: active (seq 4) - cache
		_, _, _ = collection.Put(ctx, "doc3", Body{"channels": activeChannel})

		db.WaitForPendingChanges(t)

		// With limit 1 (before)
		changesOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ActiveOnly: true, Limit: 1, ChangesCtx: base.TestCtx(t)}
		changes := getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 1)
		assert.Equal(t, "doc1", changes[0].ID)

		// No limit: should get doc1 and doc3
		changesOptions.Limit = 0
		changes = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 2)
		assert.Equal(t, "doc1", changes[0].ID)
		assert.Equal(t, "doc3", changes[1].ID)

		// With limit 1 (after)
		changesOptions.Limit = 1
		changes = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 1)
		assert.Equal(t, "doc1", changes[0].ID)
	})

	t.Run("query has no active revs, cache has no active revs", func(t *testing.T) {
		ctx, db, collection := setupDBWithChannelCacheSize(t, 2)

		// doc1: active (seq 1) -> inactive (seq 2)
		revID1, _, _ := collection.Put(ctx, "doc1", Body{"channels": activeChannel})
		_, _, _ = collection.Put(ctx, "doc1", Body{"channels": "other", "_rev": revID1})

		// doc2: active (seq 3) -> inactive (seq 4)
		revID2, _, _ := collection.Put(ctx, "doc2", Body{"channels": activeChannel})
		_, _, _ = collection.Put(ctx, "doc2", Body{"channels": "other", "_rev": revID2})

		db.WaitForPendingChanges(t)

		// With limit 1 (before)
		changesOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ActiveOnly: true, Limit: 1, ChangesCtx: base.TestCtx(t)}
		changes := getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 0)

		// No limit: should get nothing
		changesOptions.Limit = 0
		changes = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 0)

		// With limit 1 (after)
		changesOptions.Limit = 1
		changes = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 0)
	})
	t.Run("cache populated, query requires pagination", func(t *testing.T) {
		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = 5
		cacheOptions.ChannelQueryLimit = 5
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)
		// seed activeChannel in the cache prior to writing docs
		changesOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t)}
		_ = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		// Write 20 docs to the channel.  5 should be cached, 15 require query
		for i := range 20 {
			docID := fmt.Sprintf("doc%d", i+1)
			_, _, _ = collection.Put(ctx, docID, Body{"channels": activeChannel})
		}
		db.WaitForPendingChanges(t)
		changesOptions = ChangesOptions{Since: SequenceID{Seq: 0}, ActiveOnly: true, Limit: 0, ChangesCtx: base.TestCtx(t)}
		changes := getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 20)
		for i, change := range changes {
			assert.Equal(t, fmt.Sprintf("doc%d", i+1), change.ID, "change at index %d", i)
		}
	})
	t.Run("cache populated, query requires pagination with limit", func(t *testing.T) {
		// With ChannelQueryLimit=5 and Limit=10, each GetChanges call returns 5 entries from the
		// query range, which equals the requested limit, so there's no room to also append the
		// cache (docs 16-20) - if there were, the first call would skip docs 6-15.
		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = 5
		cacheOptions.ChannelQueryLimit = 5
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)
		changesOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t)}
		_ = getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		for i := 1; i <= 20; i++ {
			_, _, _ = collection.Put(ctx, fmt.Sprintf("doc%d", i), Body{"channels": activeChannel})
		}
		db.WaitForPendingChanges(t)
		// Limit=10 spans two query batches (5+5) before reaching cache. The pagination loop must
		// not prematurely append cache after the first batch hits the active limit.
		changesOptions = ChangesOptions{Since: SequenceID{Seq: 0}, ActiveOnly: true, Limit: 10, ChangesCtx: base.TestCtx(t)}
		changes := getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
		require.Len(t, changes, 10)
		for i, change := range changes {
			assert.Equal(t, fmt.Sprintf("doc%d", i+1), change.ID, "change at index %d", i)
		}
	})
}

func setupDBWithChannelCacheSize(t *testing.T, maxLength int) (context.Context, *Database, *DatabaseCollectionWithUser) {
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = maxLength
	return setupDBWithChannelCacheSettings(t, cacheOptions)
}

func setupDBWithChannelCacheSettings(t *testing.T, cacheOptions CacheOptions) (context.Context, *Database, *DatabaseCollectionWithUser) {
	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	t.Cleanup(func() { db.Close(ctx) })
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	_, err := collection.UpdateSyncFun(ctx, channels.DocChannelsSyncFunction)
	require.NoError(t, err)
	return ctx, db, collection
}

// FuzzChannelCacheActiveOnly verifies that GetChanges with ActiveOnly=true returns every active entry
// exactly once, in sequence order, regardless of how results are split between backing-store query
// batches and the in-memory cache.
func FuzzChannelCacheActiveOnly(f *testing.F) {
	// --- SEED CORPUS FOR EXPLICIT BOUNDARY CASES ---

	// 1. request limit (R) aligns exactly with query pagination limit (Q) (R == Q, or R == k * Q)
	f.Add(uint8(15), uint8(5), uint8(5), uint8(5))  // R == Q (both 5)
	f.Add(uint8(20), uint8(5), uint8(5), uint8(10)) // R == 2 * Q (request 10, page size 5)
	f.Add(uint8(10), uint8(2), uint8(2), uint8(4))  // R == 2 * Q with tiny limits (request 4, page size 2, cache size 2)

	// 2. single query, no cache (cache validFrom is high, so all historical entries must be fetched from DB)
	// (cacheMaxLength=1 means cache is effectively empty for other sequences; query limit is large enough to fetch all in one page)
	f.Add(uint8(20), uint8(1), uint8(15), uint8(5))

	// 3. Query gap before boundary, cache at boundary (query limit restricts DB page size, cacheMaxLength=1 keeps cache minimal)
	f.Add(uint8(15), uint8(1), uint8(2), uint8(10)) // request limit 10, database page size 2, no cache

	// 4. single query + partial cache (cacheMaxLength is large enough to hold some but not all docs, single DB query)
	f.Add(uint8(10), uint8(5), uint8(10), uint8(8))

	// 5. multiple queries + partial cache (cache holds part, DB pagination requested to get the rest over multiple pages)
	f.Add(uint8(12), uint8(4), uint8(3), uint8(10))

	// 6. single query + full cache (cacheMaxLength is large enough to hold all doc sequences, query limit is large)
	f.Add(uint8(10), uint8(10), uint8(15), uint8(10))

	// 7. multiple queries + full cache (cacheMaxLength is large, but query limit is small)
	f.Add(uint8(15), uint8(10), uint8(3), uint8(12))

	// 8. Other general permutations
	f.Add(uint8(5), uint8(5), uint8(5), uint8(0))  // all docs fit in cache, no request limit
	f.Add(uint8(1), uint8(5), uint8(5), uint8(1))  // single doc, limit=1
	f.Add(uint8(0), uint8(5), uint8(5), uint8(5))  // no docs
	f.Add(uint8(15), uint8(5), uint8(5), uint8(5)) // request limit < cache boundary

	f.Fuzz(func(t *testing.T, numDocs, cacheMaxLength, queryLimit, requestLimit uint8) {
		if cacheMaxLength == 0 {
			cacheMaxLength = 1
		}
		if queryLimit == 0 {
			queryLimit = 1
		}
		const maxDocs = 30
		n := int(numDocs)
		if n > maxDocs {
			n = maxDocs
		}

		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = int(cacheMaxLength)
		cacheOptions.ChannelQueryLimit = int(queryLimit)
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

		// Seed the channel in the cache before writing docs so subsequent writes land
		// in the cache from sequence 1.
		_ = getChanges(t, collection, base.SetOf("active"), ChangesOptions{
			Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t),
		})

		type docState struct {
			id     string
			revID  string
			seq    uint64
			active bool
		}

		docsMap := make(map[string]*docState)
		var docIDs []string

		// Deterministic random generator based on inputs to ensure reproducibility
		rng := rand.New(rand.NewSource(int64(numDocs) + int64(cacheMaxLength)*100 + int64(queryLimit)*10000 + int64(requestLimit)*1000000))

		nextDocID := 1
		for i := 1; i <= n; i++ {
			// Choose action:
			// 0: Create new active doc
			// 1: Create new other (inactive) doc
			// 2: Update existing to active
			// 3: Update existing to other (inactive)
			action := rng.Intn(4)
			if len(docIDs) == 0 {
				action = rng.Intn(2) // Only create actions are possible initially
			}

			var docID string
			var body Body
			var active bool

			switch action {
			case 0:
				docID = fmt.Sprintf("doc_act_%d", nextDocID)
				nextDocID++
				body = Body{"channels": "active"}
				active = true
			case 1:
				docID = fmt.Sprintf("doc_oth_%d", nextDocID)
				nextDocID++
				body = Body{"channels": "other"}
				active = false
			case 2:
				// Update existing to active
				idx := rng.Intn(len(docIDs))
				docID = docIDs[idx]
				state := docsMap[docID]
				body = Body{"channels": "active", "_rev": state.revID}
				active = true
			case 3:
				// Update existing to other
				idx := rng.Intn(len(docIDs))
				docID = docIDs[idx]
				state := docsMap[docID]
				body = Body{"channels": "other", "_rev": state.revID}
				active = false
			}

			newRevID, doc, err := collection.Put(ctx, docID, body)
			require.NoError(t, err)

			if state, exists := docsMap[docID]; exists {
				state.revID = newRevID
				state.seq = doc.Sequence
				state.active = active
			} else {
				docsMap[docID] = &docState{
					id:     docID,
					revID:  newRevID,
					seq:    doc.Sequence,
					active: active,
				}
				docIDs = append(docIDs, docID)
				sort.Strings(docIDs) // maintain sorted order for deterministic selection
			}
		}

		db.WaitForPendingChanges(t)

		changes := getChanges(t, collection, base.SetOf("active"), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      int(requestLimit),
			ChangesCtx: base.TestCtx(t),
		})

		// Collect the expected active docs at their final sequence numbers
		var activeDocs []*docState
		for _, state := range docsMap {
			if state.active {
				activeDocs = append(activeDocs, state)
			}
		}
		sort.Slice(activeDocs, func(i, j int) bool {
			return activeDocs[i].seq < activeDocs[j].seq
		})

		// Correct count: all active docs, or exactly requestLimit if that is smaller
		limit := int(requestLimit)
		expectedCount := len(activeDocs)
		if limit > 0 && limit < len(activeDocs) {
			expectedCount = limit
		}

		// No duplicates in the returned changes
		seen := make(map[string]bool, len(changes))
		for _, c := range changes {
			require.False(t, seen[c.ID], "duplicate entry %s", c.ID)
			seen[c.ID] = true
		}
		require.Len(t, changes, expectedCount)

		// Entries must be the first expectedCount active docs in final sequence order
		for i, c := range changes {
			require.Equal(t, activeDocs[i].id, c.ID, "wrong doc ID at index %d", i)
			require.Equal(t, activeDocs[i].seq, c.Seq.Seq, "wrong sequence at index %d", i)
		}
	})
}

// TestChannelCacheActiveOnlyLimitWithCrossChannelGap reproduces a scenario where the cache's
// validFrom sequence doesn't correspond to any entry in the queried channel, because the
// intervening sequence(s) belong to a *different* channel. The query's last returned entry can
// then be well below cacheValidFrom (e.g. N), while the cache's first entry starts above it
// (e.g. N+2), even though the query fully scanned through to the cache boundary with no gap.
func TestChannelCacheActiveOnlyLimitWithCrossChannelGap(t *testing.T) {
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = 2
	ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

	const activeChannel = "active"
	const otherChannel = "other"

	// Prime the cache *before* any writes so subsequent docs are appended live (and pruned
	// live via _pruneCacheLength), which is the path where validFrom can land on a sequence
	// that isn't an entry in this channel at all (see _pruneCacheLength).
	primingOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t)}
	_ = getChanges(t, collection, base.SetOf(activeChannel), primingOptions)

	// doc1: active (seq1) -> removed from channel (seq2)
	revID, _, err := collection.Put(ctx, "doc1", Body{"channels": activeChannel})
	require.NoError(t, err)
	_, _, err = collection.Put(ctx, "doc1", Body{"channels": otherChannel, "_rev": revID})
	require.NoError(t, err)

	// doc2: active (seq3) -> removed from channel (seq4)
	revID, _, err = collection.Put(ctx, "doc2", Body{"channels": activeChannel})
	require.NoError(t, err)
	_, _, err = collection.Put(ctx, "doc2", Body{"channels": otherChannel, "_rev": revID})
	require.NoError(t, err)

	// docGap: seq5, entirely in a different channel - creates a sequence gap for activeChannel.
	_, _, err = collection.Put(ctx, "docGap", Body{"channels": otherChannel})
	require.NoError(t, err)

	// doc3, doc4: active (seq6, seq7) - stay active. With ChannelCacheMaxLength=2 these live
	// appends prune doc1/doc2's entries out of the cache, pushing validFrom to 5 (docGap's
	// sequence), which isn't an entry in activeChannel at all.
	_, _, err = collection.Put(ctx, "doc3", Body{"channels": activeChannel})
	require.NoError(t, err)
	_, _, err = collection.Put(ctx, "doc4", Body{"channels": activeChannel})
	require.NoError(t, err)

	db.WaitForPendingChanges(t)

	changesOptions := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      1,
		ChangesCtx: base.TestCtx(t),
	}
	changes := getChanges(t, collection, base.SetOf(activeChannel), changesOptions)
	require.Len(t, changes, 1)
	assert.Equal(t, "doc3", changes[0].ID)
}

func TestChannelCacheActiveOnlyBoundariesAndGaps(t *testing.T) {
	const activeChannel = "active"
	const otherChannel = "other"

	t.Run("Query and cache both exactly at boundary (No Gap)", func(t *testing.T) {
		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = 2
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

		// Prime cache
		_ = getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t),
		})

		_, _, err := collection.Put(ctx, "doc1", Body{"channels": activeChannel})
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc2", Body{"channels": activeChannel})
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc3", Body{"channels": activeChannel})
		require.NoError(t, err)

		db.WaitForPendingChanges(t)

		// Since doc1 is pruned (cache length 2), validFrom is 2 (doc2's sequence is 2).
		// Querying with Limit=10 should return all 3 docs: doc1 (Seq 1), doc2 (Seq 2), and doc3 (Seq 3).
		// Since query reached boundary (Seq 2 >= 2), the cache should be appended, deduplicating doc2.
		changes := getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      10,
			ChangesCtx: base.TestCtx(t),
		})
		require.Len(t, changes, 3)
		assert.Equal(t, "doc1", changes[0].ID)
		assert.Equal(t, "doc2", changes[1].ID)
		assert.Equal(t, "doc3", changes[2].ID)
	})

	t.Run("Query gap before boundary, cache at boundary", func(t *testing.T) {
		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = 2
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

		// Prime cache
		_ = getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t),
		})

		_, _, err := collection.Put(ctx, "doc1", Body{"channels": activeChannel}) // Seq 1
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "docGap", Body{"channels": otherChannel}) // Seq 2
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc2", Body{"channels": activeChannel}) // Seq 3
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc3", Body{"channels": activeChannel}) // Seq 4
		require.NoError(t, err)

		db.WaitForPendingChanges(t)

		// doc1 is pruned, cache contains doc2 and doc3. validFrom is 2 (doc1.Seq + 1 = 2).
		// Querying with Limit=1 should get doc1 (Seq 1) and stop early because limit=1 is met before Seq 2 (validFrom).
		// The query returns exactly `limit` (1) row, so there's no room left to append the cache; only doc1 is returned.
		changes := getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      1,
			ChangesCtx: base.TestCtx(t),
		})
		require.Len(t, changes, 1)
		assert.Equal(t, "doc1", changes[0].ID)
	})

	t.Run("Query at boundary, cache gap after boundary", func(t *testing.T) {
		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = 2
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

		// Prime cache
		_ = getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t),
		})

		_, _, err := collection.Put(ctx, "doc1", Body{"channels": activeChannel}) // Seq 1
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc2", Body{"channels": activeChannel}) // Seq 2
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "docGap", Body{"channels": otherChannel}) // Seq 3
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc3", Body{"channels": activeChannel}) // Seq 4
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc4", Body{"channels": activeChannel}) // Seq 5
		require.NoError(t, err)

		db.WaitForPendingChanges(t)

		// doc1 and doc2 are pruned. Cache contains doc3 and doc4. validFrom is 3 (doc2.Seq + 1 = 3).
		// Querying with Limit=2 gets doc1 (Seq 1) and doc2 (Seq 2) and stops early (highSeq = 2 < 3).
		// The query returns exactly `limit` (2) rows, so there's no room left to append the cache.
		changes := getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      2,
			ChangesCtx: base.TestCtx(t),
		})
		require.Len(t, changes, 2)
		assert.Equal(t, "doc1", changes[0].ID)
		assert.Equal(t, "doc2", changes[1].ID)

		// Querying with Limit=10 gets doc1, doc2 (2 rows, under the limit), leaving room to append
		// the cache (doc3, doc4), so all 4 are returned.
		changes10 := getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      10,
			ChangesCtx: base.TestCtx(t),
		})
		require.Len(t, changes10, 4)
		assert.Equal(t, "doc1", changes10[0].ID)
		assert.Equal(t, "doc2", changes10[1].ID)
		assert.Equal(t, "doc3", changes10[2].ID)
		assert.Equal(t, "doc4", changes10[3].ID)
	})

	t.Run("Gaps in both directions", func(t *testing.T) {
		cacheOptions := DefaultCacheOptions()
		cacheOptions.ChannelCacheMaxLength = 2
		ctx, db, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

		// Prime cache
		_ = getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t),
		})

		_, _, err := collection.Put(ctx, "doc1", Body{"channels": activeChannel}) // Seq 1
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "docGap1", Body{"channels": otherChannel}) // Seq 2
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc2", Body{"channels": activeChannel}) // Seq 3
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "docGap2", Body{"channels": otherChannel}) // Seq 4
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc3", Body{"channels": activeChannel}) // Seq 5
		require.NoError(t, err)
		_, _, err = collection.Put(ctx, "doc4", Body{"channels": activeChannel}) // Seq 6
		require.NoError(t, err)

		db.WaitForPendingChanges(t)

		// doc1 and doc2 are pruned. Cache contains doc3 and doc4. validFrom is 4 (doc2.Seq + 1 = 4).
		// Querying with Limit=2 gets doc1 (Seq 1) and doc2 (Seq 3) and stops early (highSeq = 3 < 4).
		// The query returns exactly `limit` (2) rows, so there's no room left to append the cache.
		changes := getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      2,
			ChangesCtx: base.TestCtx(t),
		})
		require.Len(t, changes, 2)
		assert.Equal(t, "doc1", changes[0].ID)
		assert.Equal(t, "doc2", changes[1].ID)

		// Querying with Limit=10 gets doc1, doc2 (2 rows, under the limit), leaving room to append
		// the cache (doc3, doc4), so all 4 are returned.
		changes10 := getChanges(t, collection, base.SetOf(activeChannel), ChangesOptions{
			Since:      SequenceID{Seq: 0},
			ActiveOnly: true,
			Limit:      10,
			ChangesCtx: base.TestCtx(t),
		})
		require.Len(t, changes10, 4)
		assert.Equal(t, "doc1", changes10[0].ID)
		assert.Equal(t, "doc2", changes10[1].ID)
		assert.Equal(t, "doc3", changes10[2].ID)
		assert.Equal(t, "doc4", changes10[3].ID)
	})
}
