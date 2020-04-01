//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"expvar"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelCacheMaxSize(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	context, err := NewDatabaseContext("db", testBucket.Bucket, false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()
	cache := context.changeCache.getChannelCache()

	// Make channels active
	_, err = cache.GetChanges("TestA", ChangesOptions{})
	require.NoError(t, err)
	_, err = cache.GetChanges("TestB", ChangesOptions{})
	require.NoError(t, err)
	_, err = cache.GetChanges("TestC", ChangesOptions{})
	require.NoError(t, err)
	_, err = cache.GetChanges("TestD", ChangesOptions{})
	require.NoError(t, err)

	// Add some entries to caches, leaving some empty caches
	cache.AddToCache(logEntry(1, "doc1", "1-a", []string{"TestB", "TestC", "TestD"}))
	cache.AddToCache(logEntry(2, "doc2", "1-a", []string{"TestB", "TestC", "TestD"}))
	cache.AddToCache(logEntry(3, "doc3", "1-a", []string{"TestB", "TestC", "TestD"}))
	cache.AddToCache(logEntry(4, "doc4", "1-a", []string{"TestC"}))

	context.UpdateCalculatedStats()

	maxEntries, _ := strconv.Atoi(context.DbStats.StatsCache().Get(base.StatKeyChannelCacheMaxEntries).String())
	assert.Equal(t, 4, maxEntries)
}

func getCacheUtilization(stats *expvar.Map) (active, tombstones, removals int) {
	activeStat := stats.Get(base.StatKeyChannelCacheRevsActive)
	if activeStat != nil {
		active, _ = strconv.Atoi(activeStat.String())
	}

	tombstoneStat := stats.Get(base.StatKeyChannelCacheRevsTombstone)
	if tombstoneStat != nil {
		tombstones, _ = strconv.Atoi(tombstoneStat.String())
	}

	removalStat := stats.Get(base.StatKeyChannelCacheRevsRemoval)
	if removalStat != nil {
		removals, _ = strconv.Atoi(removalStat.String())
	}
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	terminator := make(chan bool)
	defer close(terminator)

	// Define cache with max channels 20, hwm will be 16, low water mark will be 12
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20

	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")

	// Add 16 channels to the cache.  Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 16; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		cache.addChannelCache(channelName)
	}
	// Validate cache size
	assert.Equal(t, 16, cache.channelCaches.Length())

	// Add another channel to cache
	cache.addChannelCache("chan_17")

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 12, cache.channelCaches.Length())

}

func TestChannelCacheCompactInactiveChannels(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	terminator := make(chan bool)
	defer close(terminator)

	// Define cache with max channels 20, watermarks 50/90
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 50

	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")

	// Add 16 channels to the cache.  Mark odd channels as active, even channels as inactive.
	// Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 18; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		cache.addChannelCache(channelName)
		if i%2 == 1 {
			log.Printf("Marking channel %s as active", channelName)
			activeChannels.IncrChannel(channelName)
		}
	}
	// Validate cache size
	assert.Equal(t, 18, cache.channelCaches.Length())

	log.Printf("adding 19th element to cache...")
	// Add another channel to cache, should trigger compaction
	cache.addChannelCache("chan_19")

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 10, cache.channelCaches.Length())

	// Validate active channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		_, isCached := cache.channelCaches.Get(channelName)
		if i%2 == 1 {
			assert.True(t, isCached, fmt.Sprintf("Channel %s was active, should be retained in cache", channelName))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Channel %s was inactive, should be evicted from cache", channelName))
		}
	}

}

// TestChannelCacheCompactNRU tests compaction where a subset of the channels are marked as recently used
// between compact triggers.  In the second compact, NRU channels should have eviction priority.
func TestChannelCacheCompactNRU(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	terminator := make(chan bool)
	defer close(terminator)

	// Define cache with max channels 20, watermarks 50/90
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 70

	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")

	// Add 18 channels to the cache.  Mark channels 1-10 as active
	// Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 18; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		cache.addChannelCache(channelName)
		if i <= 10 {
			log.Printf("Marking channel %s as active", channelName)
			activeChannels.IncrChannel(channelName)
		}
	}
	// Validate cache size
	assert.Equal(t, 18, cache.channelCaches.Length())

	// Add another channel to cache, should trigger compaction
	cache.addChannelCache("chan_19")
	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Expect channels 1-10, 11-15 to be evicted, and all to be marked as NRU during compaction
	assert.Equal(t, 14, cache.channelCaches.Length())

	// Validate recently used channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		_, isCached := cache.channelCaches.Get(channelName)
		if i <= 10 || i > 15 {
			assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached", channelName))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Expected %s to not be cached", channelName))
		}
	}

	// Mark channels 1-5 as recently used
	for i := 1; i <= 5; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		cacheElement, isCached := cache.channelCaches.Get(channelName)
		assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached during recently used update", channelName))
		AsSingleChannelCache(cacheElement).recentlyUsed.Set(true)
	}

	// Add new channels to trigger compaction.  At start of compaction, expect:
	//    Channels 1-5: inactive, recently used (manually updated)
	//    Channels 6-14: inactive, not recently used
	//    Channels 15-19: inactive, recently used (first compact since creation)
	for i := 1; i <= 19; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		if i <= 10 {
			log.Printf("Marking channel %s as inactive", channelName)
			activeChannels.DecrChannel(channelName)
		} else {
			cache.addChannelCache(channelName)
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
		channelName := fmt.Sprintf("chan_%d", i)
		_, isCached := cache.channelCaches.Get(channelName)
		if i <= 5 || i >= 11 {
			assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached", channelName))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Expected %s to not be cached", channelName))
		}
	}
}

// TestChannelCacheHighLoadCache validates behaviour under high query load when the total number of channels is lower than
// or equal to the CompactHighWatermark
func TestChannelCacheHighLoadCacheHit(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelWarn, base.KeyCache)()

	terminator := make(chan bool)
	defer close(terminator)

	// Define cache with max channels 20, watermarks 50/90
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 100
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 70

	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")

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
	cache.AddToCache(logEntry)

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
				channelName := fmt.Sprintf("chan_%d", channelNumber)
				options := ChangesOptions{}
				changes, err := cache.GetChanges(channelName, options)
				if len(changes) == 1 {
					changesSuccessCount++
				}
				assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %s", channelName))
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

	defer base.SetUpTestLogging(base.LevelWarn, base.KeyCache)()

	terminator := make(chan bool)
	defer close(terminator)

	// Define cache with max channels 100, watermarks 90/70
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 100
	options.CompactHighWatermarkPercent = 90
	options.CompactLowWatermarkPercent = 70

	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")

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
	cache.AddToCache(logEntry)

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
				channelName := fmt.Sprintf("chan_%d", channelNumber)
				options := ChangesOptions{}
				changes, err := cache.GetChanges(channelName, options)
				if len(changes) == 1 {
					changesSuccessCount++
				}
				assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %s", channelName))
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
	defer base.SetUpTestLogging(base.LevelWarn, base.KeyCache)()

	terminator := make(chan bool)
	defer close(terminator)

	// Define cache with max channels 20, watermarks 50/100
	options := DefaultCacheOptions().ChannelCacheOptions
	options.MaxNumChannels = 20
	options.CompactHighWatermarkPercent = 100
	options.CompactLowWatermarkPercent = 50

	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	require.NoError(t, err, "Background task error whilst creating channel cache")

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
	cache.AddToCache(logEntry)

	// Issue queries for all channels.  First 20 should end up in the cache, remaining 80 should trigger bypass
	for c := 1; c <= channelCount; c++ {
		channelName := fmt.Sprintf("chan_%d", c)
		options := ChangesOptions{}
		changes, err := cache.GetChanges(channelName, options)
		assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %s", channelName))
		assert.True(t, len(changes) == 1, "Expected one change per channel")
	}

	// check bypass count stat
	bypassCountStat := testStats.Get(base.StatKeyChannelCacheBypassCount)
	require.NotNil(t, bypassCountStat)
	assert.Equal(t, "80", bypassCountStat.String())
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

type testActiveChannels struct {
}

type testQueryHandler struct {
	entries    LogEntries
	queryCount int
	lock       sync.RWMutex
}

func (qh *testQueryHandler) getChangesInChannelFromQuery(channelName string, startSeq, endSeq uint64, limit int, activeOnly bool) (LogEntries, error) {
	queryEntries := make(LogEntries, 0)
	qh.lock.RLock()
	for _, entry := range qh.entries {
		_, ok := entry.Channels[channelName]
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
	defer base.SetUpTestLogging(base.LevelWarn, base.KeyCache)()
	terminator := make(chan bool)
	defer close(terminator)

	options := DefaultCacheOptions().ChannelCacheOptions
	// Specify illegal time interval for background task. Time interval should be > 0
	options.ChannelCacheAge = 0
	testStats := &expvar.Map{}
	queryHandler := &testQueryHandler{}
	activeChannelStat := &expvar.Int{}
	activeChannels := channels.NewActiveChannels(activeChannelStat)
	cache, err := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)
	assert.Error(t, err, "Background task error whilst creating channel cache")
	assert.Nil(t, cache)
	backgroundTaskError, ok := err.(*BackgroundTaskError)
	require.True(t, ok)
	assert.Equal(t, "CleanAgedItems", backgroundTaskError.TaskName)
	assert.Equal(t, options.ChannelCacheAge, backgroundTaskError.Interval)
}
