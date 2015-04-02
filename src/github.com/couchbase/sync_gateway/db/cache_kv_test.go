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
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbaselabs/go.assert"
)

func testKvCache(bucketname string) (*kvCache, base.Bucket) {
	cacheBucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "walrus:",
		BucketName: bucketname})
	if err != nil {
		log.Fatal("Couldn't connect to cache bucket")
	}
	cache := &kvCache{
		bucket: cacheBucket,
	}
	cache.Init(uint64(0))
	return cache, cacheBucket
}

func channelEntry(seq uint64, docid string, revid string, channelNames []string) *LogEntry {

	channelMap := make(channels.ChannelMap, len(channelNames))
	for _, channel := range channelNames {
		channelMap[channel] = nil
	}

	return &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
		Channels:     channelMap,
	}
}

func TestKvCacheBasic(t *testing.T) {

	base.LogKeys["DCacheSelect"] = true
	base.LogKeys["DCacheChanges"] = true
	cache, bucket := testKvCache("TestKvCacheBasic")

	// Add entry to cache
	cache.AddToCache(channelEntry(1, "foo1", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(50 * time.Millisecond)

	// Verify entry from bucket directly
	sequenceEntry, err := bucket.GetRaw("_cache:seq:1")
	assert.True(t, len(sequenceEntry) > 0)
	assert.True(t, err == nil)

	// Verify read of entry
	entry, err := readCacheEntry(1, bucket)
	assert.Equals(t, entry.Sequence, uint64(1))
	assert.Equals(t, entry.DocID, "foo1")
	assert.Equals(t, entry.RevID, "1-a")
	assertNoError(t, err, "Cache Read")

	// Validate cache entry for channels
	cacheHelper := cache.getCacheHelper("ABC")
	block := cacheHelper.readCacheBlockForSequence(uint64(1))
	assert.Equals(t, block.hasSequence(1), true)
	assert.Equals(t, block.hasSequence(2), false)

	// Validate cache entry for channels
	nbcCacheHelper := cache.getCacheHelper("NBC")
	block = nbcCacheHelper.readCacheBlockForSequence(uint64(1))
	assert.Equals(t, block == nil, true)

	cache.AddToCache(channelEntry(100, "foo2", "1-a", []string{"ABC", "CBS"}))
	cache.AddToCache(channelEntry(500, "foo3", "1-a", []string{"CBS"}))
	cache.AddToCache(channelEntry(501, "foo4", "1-a", []string{"CBS"}))
	cache.AddToCache(channelEntry(502, "foo5", "1-a", []string{"CBS"}))
	cache.AddToCache(channelEntry(503, "foo6", "1-a", []string{"CBS"}))
	cache.AddToCache(channelEntry(504, "foo7", "1-a", []string{"ABC"}))
	time.Sleep(50 * time.Millisecond)
	// Validate retrieval (GetCachedChanges)
	options := ChangesOptions{Since: SequenceID{Seq: 0}}
	_, results := cache.GetCachedChanges("ABC", options)
	assert.Equals(t, len(results), 3)
	assert.Equals(t, results[0].Sequence, uint64(1))
	assert.Equals(t, results[0].DocID, "foo1")
	assert.Equals(t, results[0].RevID, "1-a")

	options = ChangesOptions{Since: SequenceID{Seq: 50}}
	_, results = cache.GetCachedChanges("ABC", options)
	assert.Equals(t, len(results), 2)

	// Validate retrieval (GetChanges)
	options = ChangesOptions{Since: SequenceID{Seq: 0}}
	results, _ = cache.GetChanges("ABC", options)
	assert.Equals(t, len(results), 3)
	assert.Equals(t, results[0].Sequence, uint64(1))
	assert.Equals(t, results[0].DocID, "foo1")
	assert.Equals(t, results[0].RevID, "1-a")

}

func TestKvCacheMultiBlock(t *testing.T) {

	base.LogKeys["DCache"] = true
	cache, bucket := testKvCache("TestKvCacheMultiBlock")

	// Add entry to cache
	cache.AddToCache(channelEntry(10, "foo1", "1-a", []string{"ABC"}))

	// Add entry in later block
	// default cache block size is 10000
	cache.AddToCache(channelEntry(10010, "foo10010", "1-a", []string{"ABC"}))

	// wait for add
	time.Sleep(50 * time.Millisecond)
	// Verify entries from bucket directly
	sequenceEntry, err := bucket.GetRaw("_cache:seq:10")
	assert.True(t, len(sequenceEntry) > 0)
	assert.True(t, err == nil)

	sequenceEntry, err = bucket.GetRaw("_cache:seq:10010")
	assert.True(t, len(sequenceEntry) > 0)
	assert.True(t, err == nil)

	// Validate cache entry for channels
	cacheHelper := cache.getCacheHelper("ABC")
	block := cacheHelper.readCacheBlockForSequence(uint64(1))
	assert.Equals(t, block.hasSequence(10), true)
	block = cacheHelper.readCacheBlockForSequence(uint64(10010))
	assert.Equals(t, block.hasSequence(10010), true)

	// Validate border entries
	cache.AddToCache(channelEntry(19999, "foo19999", "1-a", []string{"ABC"}))
	cache.AddToCache(channelEntry(20000, "foo20000", "1-a", []string{"ABC"}))
	cache.AddToCache(channelEntry(20001, "foo20001", "1-a", []string{"ABC"}))
	time.Sleep(50 * time.Millisecond)
	cacheHelper = cache.getCacheHelper("ABC")
	block = cacheHelper.readCacheBlockForSequence(uint64(19999))
	assert.Equals(t, block.hasSequence(19999), true)
	block = cacheHelper.readCacheBlockForSequence(uint64(20000))
	assert.Equals(t, block.hasSequence(20000), true)
	block = cacheHelper.readCacheBlockForSequence(uint64(20001))
	assert.Equals(t, block.hasSequence(20001), true)

	// wait for add
	time.Sleep(50 * time.Millisecond)
	// Validate changes traverses blocks

	options := ChangesOptions{Since: SequenceID{Seq: 0}}
	_, results := cache.GetCachedChanges("ABC", options)
	assert.Equals(t, len(results), 5)

}
func TestDistributedNotify(t *testing.T) {

	base.LogKeys["DCache"] = true
	base.LogKeys["Changes+"] = true

	// lower the cache polling time to 50ms for testing
	ByteCachePollingTime = 50

	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "*"))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)

	db.changeCache.waitForSequence(2)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("ABC"), options)
	assert.True(t, err == nil)
	feedClosed := false

	// Go-routine to work the feed channel and write to an array for use by assertions
	var changes = make([]*ChangeEntry, 0, 50)
	go func() {
		for feedClosed == false {
			select {
			case entry, ok := <-feed:
				if ok {
					// feed sends nil after each continuous iteration
					if entry != nil {
						log.Println("Changes entry:", entry.Seq)
						changes = append(changes, entry)
					}
				} else {
					log.Println("Closing feed")
					feedClosed = true
				}
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	// Validate the initial sequences arrive as expected
	assert.Equals(t, len(changes), 2)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 0},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	// Test a new arrival on the channel wakes up the changes feed
	WriteDirect(db, []string{"ABC"}, 3)

	db.changeCache.waitForSequence(3)

	time.Sleep(100 * time.Millisecond)
	assert.Equals(t, len(changes), 3)
	assert.True(t, verifyChangesSequences(changes, []string{
		"1", "2", "3"}))

	// Validate an arrival in a different channel doesn't wake up the changes feed
	WriteDirect(db, []string{"NBC"}, 4)
	db.changeCache.waitForSequence(4)
	time.Sleep(100 * time.Millisecond)
	assert.Equals(t, len(changes), 3)
	assert.True(t, verifyChangesSequences(changes, []string{
		"1", "2", "3"}))

	// Validate a notify-triggered arrival uses cached response
	WriteDirect(db, []string{"ABC"}, 5)
	db.changeCache.waitForSequence(5)
	time.Sleep(100 * time.Millisecond)
	assert.Equals(t, len(changes), 4)
	assert.True(t, verifyChangesSequences(changes, []string{
		"1", "2", "3", "5"}))

	close(options.Terminator)
}

func TestCacheClock(t *testing.T) {

	base.LogKeys["DCache"] = true
	cache, _ := testKvCache("TestCacheClock")

	// Add entry to cache
	cache.AddToCache(channelEntry(1, "foo1", "1-a", []string{"ABC", "CBS"}))
	time.Sleep(50 * time.Millisecond)

	cacheHelper := cache.getCacheHelper("ABC")
	clock, err := cacheHelper.getCacheClock()

	assert.Equals(t, clock, uint64(1))
	assert.True(t, err == nil)

}
