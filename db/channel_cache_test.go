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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
)

func TestChannelCacheMaxSize(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext(t)
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := context.changeCache.getChannelCache()

	// Make channels active
	cache.GetChanges("TestA", ChangesOptions{})
	cache.GetChanges("TestB", ChangesOptions{})
	cache.GetChanges("TestC", ChangesOptions{})
	cache.GetChanges("TestD", ChangesOptions{})

	// Add some entries to caches, leaving some empty caches
	cache.AddToCache(logEntry(1, "doc1", "1-a", []string{"TestB", "TestC", "TestD"}))
	cache.AddToCache(logEntry(2, "doc2", "1-a", []string{"TestB", "TestC", "TestD"}))
	cache.AddToCache(logEntry(3, "doc3", "1-a", []string{"TestB", "TestC", "TestD"}))
	cache.AddToCache(logEntry(4, "doc4", "1-a", []string{"TestC"}))

	context.UpdateCalculatedStats()

	maxEntries, _ := strconv.Atoi(context.DbStats.StatsCache().Get(base.StatKeyChannelCacheMaxEntries).String())
	assert.Equal(t, 4, maxEntries)
}

func getCacheUtilization(context *DatabaseContext) (active, tombstones, removals int) {
	active, _ = strconv.Atoi(context.DbStats.StatsCache().Get(base.StatKeyChannelCacheRevsActive).String())
	tombstones, _ = strconv.Atoi(context.DbStats.StatsCache().Get(base.StatKeyChannelCacheRevsTombstone).String())
	removals, _ = strconv.Atoi(context.DbStats.StatsCache().Get(base.StatKeyChannelCacheRevsRemoval).String())
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
	cache := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)

	// Add 16 channels to the cache.  Shouldn't trigger compaction (hwm is not exceeded)
	for i := 1; i <= 16; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		cache.addChannelCache(channelName)
	}
	// Validate cache size
	assert.Equal(t, 16, len(cache.channelCaches))
	assert.Equal(t, 16, cache.channelCacheList.Len())

	// Add another channel to cache
	cache.addChannelCache("chan_17")

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 12, len(cache.channelCaches))
	assert.Equal(t, 12, cache.channelCacheList.Len())

}

func TestChannelCacheCompactInactiveChannels(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyCache)()

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
	cache := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)

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
	assert.Equal(t, 18, len(cache.channelCaches))
	assert.Equal(t, 18, cache.channelCacheList.Len())

	log.Printf("adding 19th element to cache...")
	// Add another channel to cache, should trigger compaction
	cache.addChannelCache("chan_19")

	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 10, len(cache.channelCaches))
	assert.Equal(t, 10, cache.channelCacheList.Len())

	// Validate active channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		_, isCached := cache.channelCaches[channelName]
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

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyCache)()

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
	cache := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)

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
	assert.Equal(t, 18, len(cache.channelCaches))
	assert.Equal(t, 18, cache.channelCacheList.Len())

	// Add another channel to cache, should trigger compaction
	cache.addChannelCache("chan_19")
	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Expect channels 1-10, 11-15 to be evicted, and all to be marked as NRU during compaction
	assert.Equal(t, 14, len(cache.channelCaches))
	assert.Equal(t, 14, cache.channelCacheList.Len())

	// Validate recently used channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		_, isCached := cache.channelCaches[channelName]
		if i <= 10 || i > 15 {
			assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached", channelName))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Expected %s to not be cached", channelName))
		}
	}

	// Mark channels 1-5 as recently used
	for i := 1; i <= 5; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		channelCache, isCached := cache.channelCaches[channelName]
		assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached during recently used update", channelName))
		channelCache.recentlyUsed.Set(true)
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
	assert.Equal(t, 14, len(cache.channelCaches))
	assert.Equal(t, 14, cache.channelCacheList.Len())
	// Validate recently used channels have been retained in cache
	for i := 1; i <= 19; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		_, isCached := cache.channelCaches[channelName]
		if i <= 5 || i >= 11 {
			assert.True(t, isCached, fmt.Sprintf("Expected %s to be cached", channelName))
		} else {
			assert.False(t, isCached, fmt.Sprintf("Expected %s to not be cached", channelName))
		}
	}
}

// TestChannelCacheHighLoad validates behaviour when channels are continuously added to the cache
func TestChannelCacheHighLoad(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyCache)()

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
	cache := newChannelCache("testDb", terminator, options, queryHandler, activeChannels, testStats)

	// define channel set, add channels to cache
	channelNames := make([]string, 0)
	for i := 1; i <= 1000; i++ {
		channelName := fmt.Sprintf("chan_%d", i)
		channelNames = append(channelNames, channelName)
	}

	// Seed the query handler with a single doc that's in all the channels
	logEntry := testLogEntryForChannels(1, channelNames)
	queryHandler.seedEntries(LogEntries{logEntry})

	// Start goroutines to add channels to the cache, query channels
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for _, channelName := range channelNames {
			cache.addChannelCache(channelName)
		}
		wg.Done()
	}()

	// Send entry to the cache
	cache.AddToCache(logEntry)

	getChangesSuccessCount := 0
	go func() {
		for _, channelName := range channelNames {
			options := ChangesOptions{}
			changes, err := cache.GetChanges(channelName, options)
			if len(changes) == 1 {
				getChangesSuccessCount++
			}
			assert.NoError(t, err, fmt.Sprintf("Error getting changes for channel %s", channelName))
			assert.True(t, len(changes) == 1, "Expected one change per channel")
		}
		wg.Done()
	}()

	wg.Wait()

	// Wait for compaction to complete
	assert.True(t, waitForCompaction(cache), "Compaction didn't complete in expected time")

	// Validate cache size
	assert.Equal(t, 14, len(cache.channelCaches))
	assert.Equal(t, 14, cache.channelCacheList.Len())

	log.Printf("GetChanges success count: %d", getChangesSuccessCount)
	log.Printf("Query count: %d", queryHandler.queryCount)

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
			if len(queryEntries) >= limit {
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
