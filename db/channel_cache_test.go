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
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	for w := 0; w < workerCount; w++ {
		workerWg.Add(1)
		go func() {
			changesSuccessCount := 0
			for i := 0; i < getChangesCount; i++ {
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
	for w := 0; w < workerCount; w++ {
		workerWg.Add(1)
		go func() {
			changesSuccessCount := 0
			for i := 0; i < getChangesCount; i++ {
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
