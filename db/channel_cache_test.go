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
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func TestDuplicateDocID(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(2, "doc3", "3-a"), false)
	cache.addToCache(e(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	goassert.True(t, err == nil)

	// Add a new revision matching mid-list
	cache.addToCache(e(4, "doc3", "3-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 4}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc5", "doc3"}))
	goassert.True(t, err == nil)

	// Add a new revision matching first
	cache.addToCache(e(5, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 5}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	goassert.True(t, err == nil)

	// Add a new revision matching last
	cache.addToCache(e(6, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 6}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	goassert.True(t, err == nil)

}

func TestLateArrivingSequence(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(3, "doc3", "3-a"), false)
	cache.addToCache(e(5, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 5}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	goassert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(e(2, "doc2", "2-a"))
	cache.addToCache(e(2, "doc2", "2-a"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3, 5}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc5"}))
	goassert.True(t, err == nil)

}

func TestLateSequenceAsFirst(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(5, "doc1", "1-a"), false)
	cache.addToCache(e(10, "doc2", "2-a"), false)
	cache.addToCache(e(15, "doc3", "3-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{5, 10, 15}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3"}))
	goassert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(e(3, "doc0", "0-a"))
	cache.addToCache(e(3, "doc0", "0-a"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{3, 5, 10, 15}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc0", "doc1", "doc2", "doc3"}))
	goassert.True(t, err == nil)

}

func TestDuplicateLateArrivingSequence(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(10, "doc1", "1-a"), false)
	cache.addToCache(e(20, "doc2", "2-a"), false)
	cache.addToCache(e(30, "doc3", "3-a"), false)
	cache.addToCache(e(40, "doc4", "4-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	goassert.True(t, verifyChannelSequences(entries, []uint64{10, 20, 30, 40}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc4"}))
	goassert.True(t, err == nil)

	// Add a late-arriving sequence that should replace earlier sequence
	cache.AddLateSequence(e(25, "doc1", "1-c"))
	cache.addToCache(e(25, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	goassert.True(t, err == nil)

	// Add a late-arriving sequence that should be ignored (later sequence exists for that docID)
	cache.AddLateSequence(e(15, "doc1", "1-b"))
	cache.addToCache(e(15, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	goassert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(e(27, "doc1", "1-d"))
	cache.addToCache(e(27, "doc1", "1-d"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 40}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	goassert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(e(41, "doc4", "4-b"))
	cache.addToCache(e(41, "doc4", "4-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 41}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	goassert.True(t, err == nil)

	// Add late arriving that's duplicate of oldest in cache
	cache.AddLateSequence(e(45, "doc2", "2-b"))
	cache.addToCache(e(45, "doc2", "2-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 4)
	writeEntries(entries)
	goassert.True(t, verifyChannelSequences(entries, []uint64{27, 30, 41, 45}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc4", "doc2"}))
	goassert.True(t, err == nil)

}

func TestPrependChanges(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	// 1. Test prepend to empty cache
	cache := newChannelCache(context, "PrependEmptyCache", 0)
	changesToPrepend := LogEntries{
		e(10, "doc3", "2-a"),
		e(12, "doc2", "2-a"),
		e(14, "doc1", "2-a"),
	}

	numPrepended := cache.prependChanges(changesToPrepend, 5, true)
	goassert.Equals(t, numPrepended, 3)

	// Validate cache
	validFrom, cachedChanges := cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(5))
	goassert.Equals(t, len(cachedChanges), 3)

	// 2. Test prepend to populated cache, with overlap and duplicates
	cache = newChannelCache(context, "PrependPopulatedCache", 0)
	cache.validFrom = 13
	cache.addToCache(e(14, "doc1", "2-a"), false)
	cache.addToCache(e(20, "doc2", "3-a"), false)

	// Prepend
	changesToPrepend = LogEntries{
		e(10, "doc3", "2-a"),
		e(11, "doc5", "2-a"),
		e(12, "doc2", "2-a"),
		e(14, "doc1", "2-a"),
	}

	numPrepended = cache.prependChanges(changesToPrepend, 5, true)
	goassert.Equals(t, numPrepended, 2)

	// Validate cache
	validFrom, cachedChanges = cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(5))
	goassert.Equals(t, len(cachedChanges), 4)
	if len(cachedChanges) == 4 {
		goassert.Equals(t, cachedChanges[0].DocID, "doc3")
		goassert.Equals(t, cachedChanges[0].RevID, "2-a")
		goassert.Equals(t, cachedChanges[1].DocID, "doc5")
		goassert.Equals(t, cachedChanges[1].RevID, "2-a")
		goassert.Equals(t, cachedChanges[2].DocID, "doc1")
		goassert.Equals(t, cachedChanges[2].RevID, "2-a")
		goassert.Equals(t, cachedChanges[3].DocID, "doc2")
		goassert.Equals(t, cachedChanges[3].RevID, "3-a")
	}

	// Write a new revision for a prepended doc to the cache, validate that old entry is removed
	cache.addToCache(e(24, "doc3", "3-a"), false)
	validFrom, cachedChanges = cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(5))
	goassert.Equals(t, len(cachedChanges), 4)
	if len(cachedChanges) == 4 {
		goassert.Equals(t, cachedChanges[0].DocID, "doc5")
		goassert.Equals(t, cachedChanges[0].RevID, "2-a")
		goassert.Equals(t, cachedChanges[1].DocID, "doc1")
		goassert.Equals(t, cachedChanges[1].RevID, "2-a")
		goassert.Equals(t, cachedChanges[2].DocID, "doc2")
		goassert.Equals(t, cachedChanges[2].RevID, "3-a")
		goassert.Equals(t, cachedChanges[3].DocID, "doc3")
		goassert.Equals(t, cachedChanges[3].RevID, "3-a")
	}

	// Prepend empty set, validate validFrom update
	cache.prependChanges(LogEntries{}, 5, true)
	validFrom, cachedChanges = cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(5))
	goassert.Equals(t, len(cachedChanges), 4)

	// 3. Test prepend that exceeds cache capacity
	cache = newChannelCache(context, "PrependToFillCache", 0)
	cache.options.ChannelCacheMaxLength = 5
	cache.validFrom = 13
	cache.addToCache(e(14, "doc1", "2-a"), false)
	cache.addToCache(e(20, "doc2", "3-a"), false)
	cache.addToCache(e(22, "doc3", "3-a"), false)
	cache.addToCache(e(23, "doc4", "3-a"), false)

	// Prepend changes.  Only room for one more in cache.  doc1 and doc2 should be ignored (already in cache), doc 6 should get cached, doc 5 should be discarded.  validFrom should be doc6 (10)
	changesToPrepend = LogEntries{
		e(8, "doc5", "2-a"),
		e(10, "doc6", "2-a"),
		e(12, "doc2", "2-a"),
		e(14, "doc1", "2-a"),
	}

	numPrepended = cache.prependChanges(changesToPrepend, 5, true)
	goassert.Equals(t, numPrepended, 1)

	// Validate cache
	validFrom, cachedChanges = cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(10))
	goassert.Equals(t, len(cachedChanges), 5)
	if len(cachedChanges) == 5 {
		goassert.Equals(t, cachedChanges[0].DocID, "doc6")
		goassert.Equals(t, cachedChanges[0].RevID, "2-a")
		goassert.Equals(t, cachedChanges[1].DocID, "doc1")
		goassert.Equals(t, cachedChanges[1].RevID, "2-a")
		goassert.Equals(t, cachedChanges[2].DocID, "doc2")
		goassert.Equals(t, cachedChanges[2].RevID, "3-a")
		goassert.Equals(t, cachedChanges[3].DocID, "doc3")
		goassert.Equals(t, cachedChanges[3].RevID, "3-a")
		goassert.Equals(t, cachedChanges[4].DocID, "doc4")
		goassert.Equals(t, cachedChanges[4].RevID, "3-a")
	}

	// 4. Test prepend where all docids are already present in cache.  Cache entries shouldn't change, but validFrom is updated
	cache = newChannelCache(context, "PrependDuplicatesOnly", 0)
	cache.validFrom = 13
	cache.addToCache(e(14, "doc1", "2-a"), false)
	cache.addToCache(e(20, "doc2", "3-a"), false)
	cache.addToCache(e(22, "doc3", "3-a"), false)
	cache.addToCache(e(23, "doc4", "3-a"), false)

	changesToPrepend = LogEntries{
		e(8, "doc2", "2-a"),
		e(10, "doc3", "2-a"),
		e(12, "doc4", "2-a"),
		e(14, "doc1", "2-a"),
	}
	numPrepended = cache.prependChanges(changesToPrepend, 5, true)
	goassert.Equals(t, numPrepended, 0)
	validFrom, cachedChanges = cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(5))
	goassert.Equals(t, len(cachedChanges), 4)
	if len(cachedChanges) == 5 {
		goassert.Equals(t, cachedChanges[0].DocID, "doc1")
		goassert.Equals(t, cachedChanges[0].RevID, "2-a")
		goassert.Equals(t, cachedChanges[1].DocID, "doc2")
		goassert.Equals(t, cachedChanges[1].RevID, "3-a")
		goassert.Equals(t, cachedChanges[2].DocID, "doc3")
		goassert.Equals(t, cachedChanges[2].RevID, "3-a")
		goassert.Equals(t, cachedChanges[3].DocID, "doc4")
		goassert.Equals(t, cachedChanges[3].RevID, "3-a")
	}

	// 5. Test prepend for an already full cache
	cache = newChannelCache(context, "PrependFullCache", 0)
	cache.options.ChannelCacheMaxLength = 5
	cache.validFrom = 13
	cache.addToCache(e(14, "doc1", "2-a"), false)
	cache.addToCache(e(20, "doc2", "3-a"), false)
	cache.addToCache(e(22, "doc3", "3-a"), false)
	cache.addToCache(e(23, "doc4", "3-a"), false)
	cache.addToCache(e(25, "doc5", "3-a"), false)

	// Prepend changes, no room for in cache.
	changesToPrepend = LogEntries{
		e(8, "doc5", "2-a"),
		e(10, "doc6", "2-a"),
		e(12, "doc2", "2-a"),
		e(14, "doc1", "2-a"),
	}

	numPrepended = cache.prependChanges(changesToPrepend, 6, true)
	goassert.Equals(t, numPrepended, 0)

	// Validate cache
	validFrom, cachedChanges = cache.getCachedChanges(ChangesOptions{})
	goassert.Equals(t, validFrom, uint64(13))
	goassert.Equals(t, len(cachedChanges), 5)
	if len(cachedChanges) == 5 {
		goassert.Equals(t, cachedChanges[0].DocID, "doc1")
		goassert.Equals(t, cachedChanges[0].RevID, "2-a")
		goassert.Equals(t, cachedChanges[1].DocID, "doc2")
		goassert.Equals(t, cachedChanges[1].RevID, "3-a")
		goassert.Equals(t, cachedChanges[2].DocID, "doc3")
		goassert.Equals(t, cachedChanges[2].RevID, "3-a")
		goassert.Equals(t, cachedChanges[3].DocID, "doc4")
		goassert.Equals(t, cachedChanges[3].RevID, "3-a")
		goassert.Equals(t, cachedChanges[4].DocID, "doc5")
		goassert.Equals(t, cachedChanges[4].RevID, "3-a")
	}
}

func TestChannelCacheRemove(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(2, "doc3", "3-a"), false)
	cache.addToCache(e(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 3)
	goassert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	goassert.True(t, err == nil)

	// Now remove doc1
	cache.Remove([]string{"doc1"}, time.Now())
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 2)
	goassert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	goassert.True(t, err == nil)

	// Try to remove doc5 with a startTime before it was added to ensure it's not removed
	// This will print a debug level log:
	// [DBG] Cache+: Skipping removal of doc "doc5" from cache "Test1" - received after purge
	cache.Remove([]string{"doc5"}, time.Now().Add(-time.Second*5))
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	goassert.Equals(t, len(entries), 2)
	goassert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	goassert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	goassert.True(t, err == nil)
}

func TestChannelCacheStats(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(2, "doc2", "1-a"), false)
	cache.addToCache(e(3, "doc3", "1-a"), false)

	active, tombstones, removals := getCacheUtilization(context)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 0, removals)

	// Update keys already present in the cache, shouldn't modify utilization
	cache.addToCache(e(4, "doc1", "2-a"), false)
	cache.addToCache(e(5, "doc2", "2-a"), false)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 0, removals)

	// Add a removal rev for a doc not previously in the cache
	cache.addToCache(e(6, "doc4", "2-a"), true)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 1, removals)

	// Add a removal rev for a doc previously in the cache
	cache.addToCache(e(7, "doc1", "3-a"), true)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 2, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 2, removals)

	// Add a new tombstone to the cache
	tombstone := e(8, "doc5", "2-a")
	tombstone.SetDeleted()
	cache.addToCache(tombstone, false)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 2, active)
	assert.Equal(t, 1, tombstones)
	assert.Equal(t, 2, removals)

	// Add a tombstone that's also a removal.  Should only be tracked as removal
	tombstone = e(9, "doc6", "2-a")
	tombstone.SetDeleted()
	cache.addToCache(tombstone, true)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 2, active)
	assert.Equal(t, 1, tombstones)
	assert.Equal(t, 3, removals)

	// Tombstone a document id already present in the cache as an active revision
	tombstone = e(10, "doc2", "3-a")
	tombstone.SetDeleted()
	cache.addToCache(tombstone, false)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 1, active)
	assert.Equal(t, 2, tombstones)
	assert.Equal(t, 3, removals)
}

func TestChannelCacheStatsOnPrune(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)
	cache.options.ChannelCacheMaxLength = 5

	// Add more than ChannelCacheMaxLength entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(2, "doc2", "1-a"), true)
	cache.addToCache(e(3, "doc3", "1-a"), false)
	cache.addToCache(e(4, "doc4", "1-a"), true)
	cache.addToCache(e(5, "doc5", "1-a"), false)
	cache.addToCache(e(6, "doc6", "1-a"), true)
	cache.addToCache(e(7, "doc7", "1-a"), false)
	cache.addToCache(e(8, "doc8", "1-a"), true)
	cache.addToCache(e(9, "doc9", "1-a"), false)
	cache.addToCache(e(10, "doc10", "1-a"), true)

	active, tombstones, removals := getCacheUtilization(context)
	assert.Equal(t, 2, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 3, removals)

}

func TestChannelCacheStatsOnPrepend(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)
	cache.options.ChannelCacheMaxLength = 15

	// Add 9 entries to cache, 3 of each type
	cache.addToCache(e(100, "active1", "2-a"), false)
	cache.addToCache(e(102, "active2", "2-a"), false)
	cache.addToCache(e(104, "removal1", "2-a"), true)
	cache.addToCache(et(106, "tombstone1", "2-a"), false)
	cache.addToCache(e(107, "removal2", "2-a"), true)
	cache.addToCache(e(108, "removal3", "2-a"), true)
	cache.addToCache(et(110, "tombstone2", "2-a"), false)
	cache.addToCache(et(111, "tombstone3", "2-a"), false)
	cache.addToCache(e(112, "active3", "2-a"), false)

	active, tombstones, removals := getCacheUtilization(context)
	assert.Equal(t, 3, active)
	assert.Equal(t, 3, tombstones)
	assert.Equal(t, 3, removals)

	// Attempt to prepend entries with later sequences already in cache.  Shouldn't modify stats (note that prepend expects one overlap)
	prependDuplicatesSet := make(LogEntries, 6)
	prependDuplicatesSet[0] = (e(50, "active1", "1-a"))
	prependDuplicatesSet[1] = (et(51, "active2", "1-a"))
	prependDuplicatesSet[2] = (e(52, "removal1", "1-a"))
	prependDuplicatesSet[3] = (et(53, "tombstone1", "1-a"))
	prependDuplicatesSet[4] = (e(54, "tombstone3", "1-a"))
	prependDuplicatesSet[5] = (e(100, "active1", "2-a"))
	cache.prependChanges(prependDuplicatesSet, 50, false)

	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 3, active)
	assert.Equal(t, 3, tombstones)
	assert.Equal(t, 3, removals)

	// Prepend 10 non-duplicates - 5 active, 5 tombstone.  Cache only has room for 6, validate stats
	prependSet := make(LogEntries, 11)
	prependSet[0] = (e(40, "new1", "1-a"))
	prependSet[1] = (et(41, "new2", "1-a"))
	prependSet[2] = (e(42, "new3", "1-a"))
	prependSet[3] = (et(43, "new4", "1-a"))
	prependSet[4] = (e(44, "new5", "1-a"))
	prependSet[5] = (et(45, "new6", "1-a"))
	prependSet[6] = (e(46, "new7", "1-a"))
	prependSet[7] = (et(47, "new8", "1-a"))
	prependSet[8] = (e(48, "new9", "1-a"))
	prependSet[9] = (et(49, "new10", "1-a"))
	prependSet[10] = (e(100, "active1", "2-a"))
	cache.prependChanges(prependSet, 40, false)
	active, tombstones, removals = getCacheUtilization(context)
	assert.Equal(t, 6, active)
	assert.Equal(t, 6, tombstones)
	assert.Equal(t, 3, removals)
}

func TestChannelCacheMaxSize(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	context.changeCache.getChannelCache("TestA") // initialize, don't add any entries
	cacheB := context.changeCache.getChannelCache("TestB")
	cacheC := context.changeCache.getChannelCache("TestC")
	cacheD := context.changeCache.getChannelCache("TestD")

	// Add some entries to caches, leaving some empty cache
	cacheB.addToCache(e(1, "doc1", "1-a"), false)
	cacheB.addToCache(e(2, "doc2", "1-a"), false)
	cacheB.addToCache(e(3, "doc3", "1-a"), false)

	cacheC.addToCache(e(1, "doc1", "1-a"), false)
	cacheC.addToCache(e(2, "doc2", "1-a"), false)
	cacheC.addToCache(e(3, "doc3", "1-a"), false)
	cacheC.addToCache(e(4, "doc4", "1-a"), false)

	cacheD.addToCache(e(1, "doc1", "1-a"), false)
	cacheD.addToCache(e(2, "doc2", "1-a"), false)
	cacheD.addToCache(e(3, "doc3", "1-a"), false)

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

func verifyChannelSequences(entries []*LogEntry, sequences []uint64) bool {
	if len(entries) != len(sequences) {
		log.Printf("verifyChannelSequences: entries size (%v) not equals to sequences size (%v)",
			len(entries), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if entries[index].Sequence != seq {
			log.Printf("verifyChannelSequences: sequence mismatch at index %v, entries=%d, sequences=%d",
				index, entries[index].Sequence, seq)
			return false
		}
	}
	return true
}

func verifyChannelDocIDs(entries []*LogEntry, docIDs []string) bool {
	if len(entries) != len(docIDs) {
		log.Printf("verifyChannelDocIDs: entries size (%v) not equals to DocIDs size (%v)",
			len(entries), len(docIDs))
		return false
	}
	for index, docID := range docIDs {
		if entries[index].DocID != docID {
			log.Printf("verifyChannelDocIDs: DocID mismatch at index %v, entries=%s, DocIDs=%s",
				index, entries[index].DocID, docID)
			return false
		}
	}
	return true
}

func writeEntries(entries []*LogEntry) {
	for index, entry := range entries {
		log.Printf("%d:seq=%d, docID=%s, revID=%s", index, entry.Sequence, entry.DocID, entry.RevID)
	}
}

func BenchmarkChannelCacheUniqueDocs_Ordered(b *testing.B) {

	defer base.DisableTestLogging()()
	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs
	docIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		docIDs[i] = fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], "1-a"), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs5(b *testing.B) {

	defer base.DisableTestLogging()()
	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(5.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs20(b *testing.B) {

	defer base.DisableTestLogging()()
	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(20.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs50(b *testing.B) {

	defer base.DisableTestLogging()()
	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(50.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs80(b *testing.B) {

	defer base.DisableTestLogging()()
	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(80.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs95(b *testing.B) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(95.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheUniqueDocs_Unordered(b *testing.B) {

	defer base.DisableTestLogging()()
	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Benchmark", 0)
	// generate docs
	docs := make([]*LogEntry, b.N)
	r := rand.New(rand.NewSource(99))
	for i := 0; i < b.N; i++ {
		docs[i] = e(uint64(i), fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", i), "1-a")
	}
	// shuffle sequences
	for i := b.N - 1; i >= 0; i-- {
		j := int(r.Float64() * float64(b.N))
		oldSeq := docs[i].Sequence
		docs[i].Sequence = docs[j].Sequence
		docs[j].Sequence = oldSeq
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.addToCache(docs[i], false)
	}
}

func generateDocs(percentInsert float64, N int) ([]string, []string) {

	docIDs := make([]string, N)
	revStrings := make([]string, N)
	revCount := make(map[int]int)
	r := rand.New(rand.NewSource(99))
	uniqueDocs := percentInsert / 100 * float64(N)
	maxRevCount := 0
	for i := 0; i < N; i++ {
		docIndex := int(r.Float64() * uniqueDocs)
		docIDs[i] = fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", docIndex)
		revCount[docIndex]++
		revStrings[i] = fmt.Sprintf("rev-%d", revCount[docIndex])
		if revCount[docIndex] > maxRevCount {
			maxRevCount = revCount[docIndex]
		}
	}
	return docIDs, revStrings
}
