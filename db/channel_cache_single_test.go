/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuplicateDocID(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()

	cache := newSingleChannelCache(context, "Test1", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())

	// Add some entries to cache
	cache.addToCache(testLogEntry(1, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(2, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Add a new revision matching mid-list
	cache.addToCache(testLogEntry(4, "doc3", "3-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 4}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc5", "doc3"}))
	assert.True(t, err == nil)

	// Add a new revision matching first
	cache.addToCache(testLogEntry(5, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	assert.True(t, err == nil)

	// Add a new revision matching last
	cache.addToCache(testLogEntry(6, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 6}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	assert.True(t, err == nil)

}

func TestLateArrivingSequence(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()

	cache := newSingleChannelCache(context, "Test1", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())

	// Add some entries to cache
	cache.addToCache(testLogEntry(1, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(3, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(5, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(testLogEntry(2, "doc2", "2-a"))
	cache.addToCache(testLogEntry(2, "doc2", "2-a"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc5"}))
	assert.True(t, err == nil)

}

func TestLateSequenceAsFirst(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()

	cache := newSingleChannelCache(context, "Test1", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())

	// Add some entries to cache
	cache.addToCache(testLogEntry(5, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(10, "doc2", "2-a"), false)
	cache.addToCache(testLogEntry(15, "doc3", "3-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{5, 10, 15}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(testLogEntry(3, "doc0", "0-a"))
	cache.addToCache(testLogEntry(3, "doc0", "0-a"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 5, 10, 15}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc0", "doc1", "doc2", "doc3"}))
	assert.True(t, err == nil)

}

func TestDuplicateLateArrivingSequence(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()

	cache := newSingleChannelCache(context, "Test1", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())

	// Add some entries to cache
	cache.addToCache(testLogEntry(10, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(20, "doc2", "2-a"), false)
	cache.addToCache(testLogEntry(30, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(40, "doc4", "4-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	assert.True(t, verifyChannelSequences(entries, []uint64{10, 20, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence that should replace earlier sequence
	cache.AddLateSequence(testLogEntry(25, "doc1", "1-c"))
	cache.addToCache(testLogEntry(25, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence that should be ignored (later sequence exists for that docID)
	cache.AddLateSequence(testLogEntry(15, "doc1", "1-b"))
	cache.addToCache(testLogEntry(15, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(testLogEntry(27, "doc1", "1-d"))
	cache.addToCache(testLogEntry(27, "doc1", "1-d"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(testLogEntry(41, "doc4", "4-b"))
	cache.addToCache(testLogEntry(41, "doc4", "4-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 41}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add late arriving that's duplicate of oldest in cache
	cache.AddLateSequence(testLogEntry(45, "doc2", "2-b"))
	cache.addToCache(testLogEntry(45, "doc2", "2-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{27, 30, 41, 45}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc4", "doc2"}))
	assert.True(t, err == nil)

}

func TestPrependChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()

	// 1. Test prepend to empty cache
	cache := newSingleChannelCache(context, "PrependEmptyCache", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	changesToPrepend := LogEntries{
		testLogEntry(10, "doc3", "2-a"),
		testLogEntry(12, "doc2", "2-a"),
		testLogEntry(14, "doc1", "2-a"),
	}

	numPrepended := cache.prependChanges(changesToPrepend, 5, 14)
	assert.Equal(t, 3, numPrepended)

	// Validate cache
	validFrom, cachedChanges := cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 3)

	// 2. Test prepend to populated cache, with overlap and duplicates
	cache = newSingleChannelCache(context, "PrependPopulatedCache", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	cache.validFrom = 13
	cache.addToCache(testLogEntry(14, "doc1", "2-a"), false)
	cache.addToCache(testLogEntry(20, "doc2", "3-a"), false)

	// Prepend
	changesToPrepend = LogEntries{
		testLogEntry(10, "doc3", "2-a"),
		testLogEntry(11, "doc5", "2-a"),
		testLogEntry(12, "doc2", "2-a"),
		testLogEntry(14, "doc1", "2-a"),
	}

	numPrepended = cache.prependChanges(changesToPrepend, 5, 14)
	assert.Equal(t, 2, numPrepended)

	// Validate cache
	validFrom, cachedChanges = cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 4)
	if len(cachedChanges) == 4 {
		assert.Equal(t, "doc3", cachedChanges[0].DocID)
		assert.Equal(t, "2-a", cachedChanges[0].RevID)
		assert.Equal(t, "doc5", cachedChanges[1].DocID)
		assert.Equal(t, "2-a", cachedChanges[1].RevID)
		assert.Equal(t, "doc1", cachedChanges[2].DocID)
		assert.Equal(t, "2-a", cachedChanges[2].RevID)
		assert.Equal(t, "doc2", cachedChanges[3].DocID)
		assert.Equal(t, "3-a", cachedChanges[3].RevID)
	}

	// Write a new revision for a prepended doc to the cache, validate that old entry is removed
	cache.addToCache(testLogEntry(24, "doc3", "3-a"), false)
	validFrom, cachedChanges = cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 4)
	if len(cachedChanges) == 4 {
		assert.Equal(t, "doc5", cachedChanges[0].DocID)
		assert.Equal(t, "2-a", cachedChanges[0].RevID)
		assert.Equal(t, "doc1", cachedChanges[1].DocID)
		assert.Equal(t, "2-a", cachedChanges[1].RevID)
		assert.Equal(t, "doc2", cachedChanges[2].DocID)
		assert.Equal(t, "3-a", cachedChanges[2].RevID)
		assert.Equal(t, "doc3", cachedChanges[3].DocID)
		assert.Equal(t, "3-a", cachedChanges[3].RevID)
	}

	// Prepend empty set, validate validFrom update
	cache.prependChanges(LogEntries{}, 5, 14)
	validFrom, cachedChanges = cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 4)

	// 3. Test prepend that exceeds cache capacity
	cache = newSingleChannelCache(context, "PrependToFillCache", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	cache.options.ChannelCacheMaxLength = 5
	cache.validFrom = 13
	cache.addToCache(testLogEntry(14, "doc1", "2-a"), false)
	cache.addToCache(testLogEntry(20, "doc2", "3-a"), false)
	cache.addToCache(testLogEntry(22, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(23, "doc4", "3-a"), false)

	// Prepend changes.  Only room for one more in cache.  doc1 and doc2 should be ignored (already in cache), doc 6 should get cached, doc 5 should be discarded.  validFrom should be doc6 (10)
	changesToPrepend = LogEntries{
		testLogEntry(8, "doc5", "2-a"),
		testLogEntry(10, "doc6", "2-a"),
		testLogEntry(12, "doc2", "2-a"),
		testLogEntry(14, "doc1", "2-a"),
	}

	numPrepended = cache.prependChanges(changesToPrepend, 5, 14)
	assert.Equal(t, 1, numPrepended)

	// Validate cache
	validFrom, cachedChanges = cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(10), validFrom)
	require.Len(t, cachedChanges, 5)
	if len(cachedChanges) == 5 {
		assert.Equal(t, "doc6", cachedChanges[0].DocID)
		assert.Equal(t, "2-a", cachedChanges[0].RevID)
		assert.Equal(t, "doc1", cachedChanges[1].DocID)
		assert.Equal(t, "2-a", cachedChanges[1].RevID)
		assert.Equal(t, "doc2", cachedChanges[2].DocID)
		assert.Equal(t, "3-a", cachedChanges[2].RevID)
		assert.Equal(t, "doc3", cachedChanges[3].DocID)
		assert.Equal(t, "3-a", cachedChanges[3].RevID)
		assert.Equal(t, "doc4", cachedChanges[4].DocID)
		assert.Equal(t, "3-a", cachedChanges[4].RevID)
	}

	// 4. Test prepend where all docids are already present in cache.  Cache entries shouldn't change, but validFrom is updated
	cache = newSingleChannelCache(context, "PrependDuplicatesOnly", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	cache.validFrom = 13
	cache.addToCache(testLogEntry(14, "doc1", "2-a"), false)
	cache.addToCache(testLogEntry(20, "doc2", "3-a"), false)
	cache.addToCache(testLogEntry(22, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(23, "doc4", "3-a"), false)

	changesToPrepend = LogEntries{
		testLogEntry(8, "doc2", "2-a"),
		testLogEntry(10, "doc3", "2-a"),
		testLogEntry(12, "doc4", "2-a"),
		testLogEntry(14, "doc1", "2-a"),
	}
	numPrepended = cache.prependChanges(changesToPrepend, 5, 14)
	assert.Equal(t, 0, numPrepended)
	validFrom, cachedChanges = cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 4)
	if len(cachedChanges) == 5 {
		assert.Equal(t, "doc1", cachedChanges[0].DocID)
		assert.Equal(t, "2-a", cachedChanges[0].RevID)
		assert.Equal(t, "doc2", cachedChanges[1].DocID)
		assert.Equal(t, "3-a", cachedChanges[1].RevID)
		assert.Equal(t, "doc3", cachedChanges[2].DocID)
		assert.Equal(t, "3-a", cachedChanges[2].RevID)
		assert.Equal(t, "doc4", cachedChanges[3].DocID)
		assert.Equal(t, "3-a", cachedChanges[3].RevID)
	}

	// 5. Test prepend for an already full cache
	cache = newSingleChannelCache(context, "PrependFullCache", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	cache.options.ChannelCacheMaxLength = 5
	cache.validFrom = 13
	cache.addToCache(testLogEntry(14, "doc1", "2-a"), false)
	cache.addToCache(testLogEntry(20, "doc2", "3-a"), false)
	cache.addToCache(testLogEntry(22, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(23, "doc4", "3-a"), false)
	cache.addToCache(testLogEntry(25, "doc5", "3-a"), false)

	// Prepend changes, no room for in cache.
	changesToPrepend = LogEntries{
		testLogEntry(8, "doc5", "2-a"),
		testLogEntry(10, "doc6", "2-a"),
		testLogEntry(12, "doc2", "2-a"),
		testLogEntry(14, "doc1", "2-a"),
	}

	numPrepended = cache.prependChanges(changesToPrepend, 6, 14)
	assert.Equal(t, 0, numPrepended)

	// Validate cache
	validFrom, cachedChanges = cache.GetCachedChanges(ChangesOptions{})
	assert.Equal(t, uint64(13), validFrom)
	require.Len(t, cachedChanges, 5)
	if len(cachedChanges) == 5 {
		assert.Equal(t, "doc1", cachedChanges[0].DocID)
		assert.Equal(t, "2-a", cachedChanges[0].RevID)
		assert.Equal(t, "doc2", cachedChanges[1].DocID)
		assert.Equal(t, "3-a", cachedChanges[1].RevID)
		assert.Equal(t, "doc3", cachedChanges[2].DocID)
		assert.Equal(t, "3-a", cachedChanges[2].RevID)
		assert.Equal(t, "doc4", cachedChanges[3].DocID)
		assert.Equal(t, "3-a", cachedChanges[3].RevID)
		assert.Equal(t, "doc5", cachedChanges[4].DocID)
		assert.Equal(t, "3-a", cachedChanges[4].RevID)
	}
}

func TestChannelCacheRemove(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Test1", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())

	// Add some entries to cache
	cache.addToCache(testLogEntry(1, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(2, "doc3", "3-a"), false)
	cache.addToCache(testLogEntry(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Now remove doc1
	cache.Remove([]string{"doc1"}, time.Now())
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 2)
	assert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	assert.True(t, err == nil)

	// Try to remove doc5 with a startTime before it was added to ensure it's not removed
	// This will print a debug level log:
	// [DBG] Cache+: Skipping removal of doc "doc5" from cache "Test1" - received after purge
	cache.Remove([]string{"doc5"}, time.Now().Add(-time.Second*5))
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	require.Len(t, entries, 2)
	assert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	assert.True(t, err == nil)
}

func TestChannelCacheStats(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()
	testStats := (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache()
	cache := newSingleChannelCache(context, "Test1", 0, testStats)

	// Add some entries to cache
	cache.addToCache(testLogEntry(1, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(2, "doc2", "1-a"), false)
	cache.addToCache(testLogEntry(3, "doc3", "1-a"), false)

	active, tombstones, removals := getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 0, removals)

	// Update keys already present in the cache, shouldn't modify utilization
	cache.addToCache(testLogEntry(4, "doc1", "2-a"), false)
	cache.addToCache(testLogEntry(5, "doc2", "2-a"), false)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 0, removals)

	// Add a removal rev for a doc not previously in the cache
	cache.addToCache(testLogEntry(6, "doc4", "2-a"), true)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 1, removals)

	// Add a removal rev for a doc previously in the cache
	cache.addToCache(testLogEntry(7, "doc1", "3-a"), true)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 2, removals)

	// Add a new tombstone to the cache
	tombstone := testLogEntry(8, "doc5", "2-a")
	tombstone.SetDeleted()
	cache.addToCache(tombstone, false)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 1, tombstones)
	assert.Equal(t, 2, removals)

	// Add a tombstone that's also a removal.  Should only be tracked as removal
	tombstone = testLogEntry(9, "doc6", "2-a")
	tombstone.SetDeleted()
	cache.addToCache(tombstone, true)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 1, tombstones)
	assert.Equal(t, 3, removals)

	// Tombstone a document id already present in the cache as an active revision
	tombstone = testLogEntry(10, "doc2", "3-a")
	tombstone.SetDeleted()
	cache.addToCache(tombstone, false)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 1, active)
	assert.Equal(t, 2, tombstones)
	assert.Equal(t, 3, removals)
}

func TestChannelCacheStatsOnPrune(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()
	testStats := (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache()
	cache := newSingleChannelCache(context, "Test1", 0, testStats)
	cache.options.ChannelCacheMaxLength = 5

	// Add more than ChannelCacheMaxLength entries to cache
	cache.addToCache(testLogEntry(1, "doc1", "1-a"), false)
	cache.addToCache(testLogEntry(2, "doc2", "1-a"), true)
	cache.addToCache(testLogEntry(3, "doc3", "1-a"), false)
	cache.addToCache(testLogEntry(4, "doc4", "1-a"), true)
	cache.addToCache(testLogEntry(5, "doc5", "1-a"), false)
	cache.addToCache(testLogEntry(6, "doc6", "1-a"), true)
	cache.addToCache(testLogEntry(7, "doc7", "1-a"), false)
	cache.addToCache(testLogEntry(8, "doc8", "1-a"), true)
	cache.addToCache(testLogEntry(9, "doc9", "1-a"), false)
	cache.addToCache(testLogEntry(10, "doc10", "1-a"), true)

	active, tombstones, removals := getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 3, removals)

}

func TestChannelCacheStatsOnPrepend(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	require.NoError(t, err)
	defer context.Close()
	testStats := (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache()
	cache := newSingleChannelCache(context, "Test1", 99, testStats)
	cache.options.ChannelCacheMaxLength = 15

	// Add 9 entries to cache, 3 of each type
	cache.addToCache(testLogEntry(100, "active1", "2-a"), false)
	cache.addToCache(testLogEntry(102, "active2", "2-a"), false)
	cache.addToCache(testLogEntry(104, "removal1", "2-a"), true)
	cache.addToCache(et(106, "tombstone1", "2-a"), false)
	cache.addToCache(testLogEntry(107, "removal2", "2-a"), true)
	cache.addToCache(testLogEntry(108, "removal3", "2-a"), true)
	cache.addToCache(et(110, "tombstone2", "2-a"), false)
	cache.addToCache(et(111, "tombstone3", "2-a"), false)
	cache.addToCache(testLogEntry(112, "active3", "2-a"), false)

	active, tombstones, removals := getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 3, tombstones)
	assert.Equal(t, 3, removals)

	// Attempt to prepend entries with later sequences already in cache.  Shouldn't modify stats (note that prepend expects one overlap)
	prependDuplicatesSet := make(LogEntries, 5)
	prependDuplicatesSet[0] = (testLogEntry(50, "active1", "1-a"))
	prependDuplicatesSet[1] = (et(51, "active2", "1-a"))
	prependDuplicatesSet[2] = (testLogEntry(52, "removal1", "1-a"))
	prependDuplicatesSet[3] = (et(53, "tombstone1", "1-a"))
	prependDuplicatesSet[4] = (testLogEntry(54, "tombstone3", "1-a"))
	cache.prependChanges(prependDuplicatesSet, 50, 99)

	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 3, tombstones)
	assert.Equal(t, 3, removals)

	// Prepend 10 non-duplicates - 5 active, 5 tombstone.  Cache only has room for 6, validate stats
	prependSet := make(LogEntries, 11)
	prependSet[0] = (testLogEntry(40, "new1", "1-a"))
	prependSet[1] = (et(41, "new2", "1-a"))
	prependSet[2] = (testLogEntry(42, "new3", "1-a"))
	prependSet[3] = (et(43, "new4", "1-a"))
	prependSet[4] = (testLogEntry(44, "new5", "1-a"))
	prependSet[5] = (et(45, "new6", "1-a"))
	prependSet[6] = (testLogEntry(46, "new7", "1-a"))
	prependSet[7] = (et(47, "new8", "1-a"))
	prependSet[8] = (testLogEntry(48, "new9", "1-a"))
	prependSet[9] = (et(49, "new10", "1-a"))
	prependSet[10] = (et(50, "active1", "1-a"))
	cache.prependChanges(prependSet, 40, 50)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 6, active)
	assert.Equal(t, 6, tombstones)
	assert.Equal(t, 3, removals)
}

func TestBypassSingleChannelCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	terminator := make(chan bool)
	defer close(terminator)

	// Seed the query handler with 100 docs across 10 channels
	queryHandler := &testQueryHandler{}
	for seq := 1; seq <= 100; seq++ {
		channelName := fmt.Sprintf("chan_%d", seq%10)
		queryEntry := testLogEntryForChannels(seq, []string{channelName})
		queryHandler.seedEntries(LogEntries{queryEntry})
	}

	bypassCache := &bypassChannelCache{
		channelName:  "chan_1",
		queryHandler: queryHandler,
	}

	entries, err := bypassCache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.NoError(t, err)
	require.Len(t, entries, 10)

	validFrom, cachedEntries := bypassCache.GetCachedChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equal(t, uint64(math.MaxUint64), validFrom)
	require.Len(t, cachedEntries, 0)
}

func BenchmarkChannelCacheUniqueDocs_Ordered(b *testing.B) {

	base.DisableTestLogging(b)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate doc IDs
	docIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		docIDs[i] = fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(testLogEntry(uint64(i), docIDs[i], "1-a"), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs5(b *testing.B) {

	base.DisableTestLogging(b)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(5.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(testLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs20(b *testing.B) {

	base.DisableTestLogging(b)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(20.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(testLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs50(b *testing.B) {

	base.DisableTestLogging(b)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(50.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(testLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs80(b *testing.B) {

	base.DisableTestLogging(b)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(80.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(testLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs95(b *testing.B) {

	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(95.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(testLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheUniqueDocs_Unordered(b *testing.B) {

	base.DisableTestLogging(b)

	context, err := NewDatabaseContext("db", base.GetTestBucket(b), false, DatabaseContextOptions{})
	require.NoError(b, err)
	defer context.Close()
	cache := newSingleChannelCache(context, "Benchmark", 0, (base.NewSyncGatewayStats()).NewDBStats("", false, false, false).Cache())
	// generate docs
	docs := make([]*LogEntry, b.N)
	r := rand.New(rand.NewSource(99))
	for i := 0; i < b.N; i++ {
		docs[i] = testLogEntry(uint64(i), fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", i), "1-a")
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
