/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package channelcachetest

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/db"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestDuplicateDocID(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collection.GetCollectionID()), 0, dbstats.Cache())
	assert.NotNil(t, cache)

	// Add some entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Add a new revision matching mid-list
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(4, "doc3", "3-b"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 4}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc5", "doc3"}))
	assert.True(t, err == nil)

	// Add a new revision matching first
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	assert.True(t, err == nil)

	// Add a new revision matching last
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(6, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 6}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	assert.True(t, err == nil)

}

func TestLateArrivingSequence(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collection.GetCollectionID()), 0, dbstats.Cache())
	assert.NotNil(t, cache)

	// Add some entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(db.MakeTestLogEntry(2, "doc2", "2-a"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc2", "2-a"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc5"}))
	assert.True(t, err == nil)

}

func TestLateSequenceAsFirst(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collection.GetCollectionID()), 0, dbstats.Cache())
	assert.NotNil(t, cache)

	// Add some entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc2", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(15, "doc3", "3-a"), false)

	entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{5, 10, 15}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(db.MakeTestLogEntry(3, "doc0", "0-a"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc0", "0-a"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 5, 10, 15}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc0", "doc1", "doc2", "doc3"}))
	assert.True(t, err == nil)

}

func TestDuplicateLateArrivingSequence(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collection.GetCollectionID()), 0, dbstats.Cache())
	assert.NotNil(t, cache)

	// Add some entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(20, "doc2", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(30, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(40, "doc4", "4-a"), false)

	entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	assert.True(t, verifyChannelSequences(entries, []uint64{10, 20, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence that should replace earlier sequence
	cache.AddLateSequence(db.MakeTestLogEntry(25, "doc1", "1-c"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(25, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence that should be ignored (later sequence exists for that docID)
	cache.AddLateSequence(db.MakeTestLogEntry(15, "doc1", "1-b"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(15, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(db.MakeTestLogEntry(27, "doc1", "1-d"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(27, "doc1", "1-d"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(db.MakeTestLogEntry(41, "doc4", "4-b"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(41, "doc4", "4-b"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 41}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add late arriving that's duplicate of oldest in cache
	cache.AddLateSequence(db.MakeTestLogEntry(45, "doc2", "2-b"))
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(45, "doc2", "2-b"), false)
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{27, 30, 41, 45}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc4", "doc2"}))
	assert.True(t, err == nil)

}

func TestPrependChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	// 1. Test prepend to empty cache
	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("PrependEmptyCache", collection.GetCollectionID()), 0, dbstats.Cache())
	assert.NotNil(t, cache)

	changesToPrepend := db.LogEntries{
		db.MakeTestLogEntry(10, "doc3", "2-a"),
		db.MakeTestLogEntry(12, "doc2", "2-a"),
		db.MakeTestLogEntry(14, "doc1", "2-a"),
	}

	numPrepended := cache.PrependChangesForTest(t, ctx, changesToPrepend, 5, 14)
	assert.Equal(t, 3, numPrepended)

	// Validate cache
	validFrom, cachedChanges := cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 3)

	// 2. Test prepend to populated cache, with overlap and duplicates
	stats, err = base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err = stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	cache = db.NewSingleChannelCacheForTest(t, collection, channels.NewID("PrependPopulatedCache", collection.GetCollectionID()), 0, dbstats.Cache())
	cache.SetValidFromForTest(t, 13)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(14, "doc1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(20, "doc2", "3-a"), false)

	// Prepend
	changesToPrepend = db.LogEntries{
		db.MakeTestLogEntry(10, "doc3", "2-a"),
		db.MakeTestLogEntry(11, "doc5", "2-a"),
		db.MakeTestLogEntry(12, "doc2", "2-a"),
		db.MakeTestLogEntry(14, "doc1", "2-a"),
	}

	numPrepended = cache.PrependChangesForTest(t, ctx, changesToPrepend, 5, 14)
	assert.Equal(t, 2, numPrepended)

	// Validate cache
	validFrom, cachedChanges = cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
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
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(24, "doc3", "3-a"), false)
	validFrom, cachedChanges = cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
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
	cache.PrependChangesForTest(t, ctx, db.LogEntries{}, 5, 14)
	validFrom, cachedChanges = cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
	assert.Equal(t, uint64(5), validFrom)
	require.Len(t, cachedChanges, 4)

	// 3. Test prepend that exceeds cache capacity
	stats, err = base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err = stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	cache = db.NewSingleChannelCacheForTest(t, collection, channels.NewID("PrependToFillCache", collection.GetCollectionID()), 0, dbstats.Cache())
	cache.OptionsForTest(t).ChannelCacheMaxLength = 5
	cache.SetValidFromForTest(t, 13)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(14, "doc1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(20, "doc2", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(22, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(23, "doc4", "3-a"), false)

	// Prepend changes.  Only room for one more in cache.  doc1 and doc2 should be ignored (already in cache), doc 6 should get cached, doc 5 should be discarded.  validFrom should be doc6 (10)
	changesToPrepend = db.LogEntries{
		db.MakeTestLogEntry(8, "doc5", "2-a"),
		db.MakeTestLogEntry(10, "doc6", "2-a"),
		db.MakeTestLogEntry(12, "doc2", "2-a"),
		db.MakeTestLogEntry(14, "doc1", "2-a"),
	}

	numPrepended = cache.PrependChangesForTest(t, ctx, changesToPrepend, 5, 14)
	assert.Equal(t, 1, numPrepended)

	// Validate cache
	validFrom, cachedChanges = cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
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
	stats, err = base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err = stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	cache = db.NewSingleChannelCacheForTest(t, collection, channels.NewID("PrependDuplicatesOnly", collection.GetCollectionID()), 0, dbstats.Cache())
	cache.SetValidFromForTest(t, 13)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(14, "doc1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(20, "doc2", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(22, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(23, "doc4", "3-a"), false)

	changesToPrepend = db.LogEntries{
		db.MakeTestLogEntry(8, "doc2", "2-a"),
		db.MakeTestLogEntry(10, "doc3", "2-a"),
		db.MakeTestLogEntry(12, "doc4", "2-a"),
		db.MakeTestLogEntry(14, "doc1", "2-a"),
	}
	numPrepended = cache.PrependChangesForTest(t, ctx, changesToPrepend, 5, 14)
	assert.Equal(t, 0, numPrepended)
	validFrom, cachedChanges = cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
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
	stats, err = base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err = stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	cache = db.NewSingleChannelCacheForTest(t, collection, channels.NewID("PrependFullCache", collection.GetCollectionID()), 0, dbstats.Cache())
	cache.OptionsForTest(t).ChannelCacheMaxLength = 5
	cache.SetValidFromForTest(t, 13)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(14, "doc1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(20, "doc2", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(22, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(23, "doc4", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(25, "doc5", "3-a"), false)

	// Prepend changes, no room for in cache.
	changesToPrepend = db.LogEntries{
		db.MakeTestLogEntry(8, "doc5", "2-a"),
		db.MakeTestLogEntry(10, "doc6", "2-a"),
		db.MakeTestLogEntry(12, "doc2", "2-a"),
		db.MakeTestLogEntry(14, "doc1", "2-a"),
	}

	numPrepended = cache.PrependChangesForTest(t, ctx, changesToPrepend, 6, 14)
	assert.Equal(t, 0, numPrepended)

	// Validate cache
	validFrom, cachedChanges = cache.GetCachedChanges(db.GetChangesOptionsWithCtxOnly(t))
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

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
	collectionID := collection.GetCollectionID()

	cacheStats := dbstats.Cache()
	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collectionID), 0, cacheStats)

	// Add some entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc3", "3-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Now remove doc1
	removed := cache.Remove(ctx, collectionID, []string{"doc1"}, time.Now())
	assert.Equal(t, 1, removed, "Remove reports what it removed")
	active, _, _ := getCacheUtilization(cacheStats)
	assert.Equal(t, 2, active, "utilization falls by the number removed")
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 2)
	assert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	assert.True(t, err == nil)

	// Try to remove doc5 with a startTime before it was added to ensure it's not removed
	// This will print a debug level log:
	// [DBG] Cache+: Skipping removal of doc "doc5" from cache "Test1" - received after purge
	removed = cache.Remove(ctx, collectionID, []string{"doc5"}, time.Now().Add(-time.Second*5))
	assert.Equal(t, 0, removed, "a doc received after the purge started is not removed or counted")
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 2)
	assert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	assert.True(t, err == nil)
}

func TestChannelCacheStats(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
	collectionID := collection.GetCollectionID()

	testStats := dbstats.Cache()
	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collectionID), 0, testStats)

	// Add some entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc2", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc3", "1-a"), false)

	active, tombstones, removals := getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 0, removals)

	// Update keys already present in the cache, shouldn't modify utilization
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(4, "doc1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc2", "2-a"), false)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 0, removals)

	// Add a removal rev for a doc not previously in the cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(6, "doc4", "2-a"), true)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 1, removals)

	// Add a removal rev for a doc previously in the cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(7, "doc1", "3-a"), true)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 2, removals)

	// Add a new tombstone to the cache
	tombstone := db.MakeTestLogEntry(8, "doc5", "2-a")
	tombstone.SetDeleted()
	cache.AddToCacheForTest(t, ctx, tombstone, false)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 1, tombstones)
	assert.Equal(t, 2, removals)

	// Add a tombstone that's also a removal.  Should only be tracked as removal
	tombstone = db.MakeTestLogEntry(9, "doc6", "2-a")
	tombstone.SetDeleted()
	cache.AddToCacheForTest(t, ctx, tombstone, true)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 1, tombstones)
	assert.Equal(t, 3, removals)

	// Tombstone a document id already present in the cache as an active revision
	tombstone = db.MakeTestLogEntry(10, "doc2", "3-a")
	tombstone.SetDeleted()
	cache.AddToCacheForTest(t, ctx, tombstone, false)
	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 1, active)
	assert.Equal(t, 2, tombstones)
	assert.Equal(t, 3, removals)
}

func TestChannelCacheStatsOnPrune(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
	collectionID := collection.GetCollectionID()

	testStats := dbstats.Cache()
	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collectionID), 0, testStats)
	cache.OptionsForTest(t).ChannelCacheMaxLength = 5

	// Add more than ChannelCacheMaxLength entries to cache
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc2", "1-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc3", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(4, "doc4", "1-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc5", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(6, "doc6", "1-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(7, "doc7", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(8, "doc8", "1-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(9, "doc9", "1-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc10", "1-a"), true)

	active, tombstones, removals := getCacheUtilization(testStats)
	assert.Equal(t, 2, active)
	assert.Equal(t, 0, tombstones)
	assert.Equal(t, 3, removals)

}

func TestChannelCacheStatsOnPrepend(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
	collectionID := collection.GetCollectionID()

	testStats := dbstats.Cache()
	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collectionID), 99, testStats)
	cache.OptionsForTest(t).ChannelCacheMaxLength = 15

	// Add 9 entries to cache, 3 of each type
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(100, "active1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(102, "active2", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(104, "removal1", "2-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeDeletedTestLogEntry(106, "tombstone1", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(107, "removal2", "2-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(108, "removal3", "2-a"), true)
	cache.AddToCacheForTest(t, ctx, db.MakeDeletedTestLogEntry(110, "tombstone2", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeDeletedTestLogEntry(111, "tombstone3", "2-a"), false)
	cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(112, "active3", "2-a"), false)

	active, tombstones, removals := getCacheUtilization(testStats)
	require.Equal(t, 3, active)
	assert.Equal(t, 3, tombstones)
	assert.Equal(t, 3, removals)

	// Attempt to prepend entries with later sequences already in cache.  Shouldn't modify stats (note that prepend expects one overlap)
	prependDuplicatesSet := make(db.LogEntries, 5)
	prependDuplicatesSet[0] = (db.MakeTestLogEntry(50, "active1", "1-a"))
	prependDuplicatesSet[1] = (db.MakeDeletedTestLogEntry(51, "active2", "1-a"))
	prependDuplicatesSet[2] = (db.MakeTestLogEntry(52, "removal1", "1-a"))
	prependDuplicatesSet[3] = (db.MakeDeletedTestLogEntry(53, "tombstone1", "1-a"))
	prependDuplicatesSet[4] = (db.MakeTestLogEntry(54, "tombstone3", "1-a"))
	cache.PrependChangesForTest(t, ctx, prependDuplicatesSet, 50, 99)

	active, tombstones, removals = getCacheUtilization(testStats)
	assert.Equal(t, 3, active)
	assert.Equal(t, 3, tombstones)
	assert.Equal(t, 3, removals)

	// Prepend 10 non-duplicates - 5 active, 5 tombstone.  Cache only has room for 6, validate stats
	prependSet := make(db.LogEntries, 11)
	prependSet[0] = (db.MakeTestLogEntry(40, "new1", "1-a"))
	prependSet[1] = (db.MakeDeletedTestLogEntry(41, "new2", "1-a"))
	prependSet[2] = (db.MakeTestLogEntry(42, "new3", "1-a"))
	prependSet[3] = (db.MakeDeletedTestLogEntry(43, "new4", "1-a"))
	prependSet[4] = (db.MakeTestLogEntry(44, "new5", "1-a"))
	prependSet[5] = (db.MakeDeletedTestLogEntry(45, "new6", "1-a"))
	prependSet[6] = (db.MakeTestLogEntry(46, "new7", "1-a"))
	prependSet[7] = (db.MakeDeletedTestLogEntry(47, "new8", "1-a"))
	prependSet[8] = (db.MakeTestLogEntry(48, "new9", "1-a"))
	prependSet[9] = (db.MakeDeletedTestLogEntry(49, "new10", "1-a"))
	prependSet[10] = (db.MakeDeletedTestLogEntry(50, "active1", "1-a"))
	cache.PrependChangesForTest(t, ctx, prependSet, 40, 50)
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
	queryHandler := &db.QueryHandlerForTest{}
	for seq := 1; seq <= 100; seq++ {
		channelName := fmt.Sprintf("chan_%d", seq%10)
		queryEntry := db.MakeTestLogEntryForChannels(seq, []string{channelName})
		queryHandler.SeedEntries(db.LogEntries{queryEntry})
	}

	bypassCache := db.NewBypassChannelCacheForTest(t, queryHandler, channels.NewID("chan_1", base.DefaultCollectionID))

	entries, err := bypassCache.GetChanges(base.TestCtx(t), db.GetChangesOptionsWithZeroSeq(t))
	assert.NoError(t, err)
	require.Len(t, entries, 10)

	validFrom, cachedEntries := bypassCache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
	assert.Equal(t, uint64(math.MaxUint64), validFrom)
	require.Len(t, cachedEntries, 0)
}

func BenchmarkChannelCacheUniqueDocs_Ordered(b *testing.B) {

	base.DisableTestLogging(b)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate doc IDs
	docIDs := make([]string, b.N)
	docCount := 0
	for b.Loop() {
		docIDs[docCount] = fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", docCount)
		docCount++
	}

	b.ResetTimer()
	for i := range docCount {
		cache.AddToCacheForTest(b, ctx, db.MakeTestLogEntry(uint64(i), docIDs[i], "1-a"), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs5(b *testing.B) {

	base.DisableTestLogging(b)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)

	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(5.0, b.N)

	for i := 0; b.Loop(); i++ {
		cache.AddToCacheForTest(b, ctx, db.MakeTestLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs20(b *testing.B) {

	base.DisableTestLogging(b)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(20.0, b.N)

	for i := 0; b.Loop(); i++ {
		cache.AddToCacheForTest(b, ctx, db.MakeTestLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs50(b *testing.B) {

	base.DisableTestLogging(b)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(50.0, b.N)

	for i := 0; b.Loop(); i++ {
		cache.AddToCacheForTest(b, ctx, db.MakeTestLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs80(b *testing.B) {

	base.DisableTestLogging(b)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(80.0, b.N)

	for i := 0; b.Loop(); i++ {
		cache.AddToCacheForTest(b, ctx, db.MakeTestLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs95(b *testing.B) {

	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate doc IDs

	docIDs, revStrings := generateDocs(95.0, b.N)

	for i := 0; b.Loop(); i++ {
		cache.AddToCacheForTest(b, ctx, db.MakeTestLogEntry(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheUniqueDocs_Unordered(b *testing.B) {

	base.DisableTestLogging(b)

	database, ctx := db.SetupTestDB(b)
	defer database.Close(ctx)

	stats, err := base.NewSyncGatewayStats()
	require.NoError(b, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(b, err)

	collection := db.GetSingleDatabaseCollection(b, database.DatabaseContext)

	cache := db.NewSingleChannelCacheForTest(b, collection, channels.NewID("Benchmark", collection.GetCollectionID()), 0, dbstats.Cache())
	// generate docs
	docs := make([]*db.LogEntry, b.N)
	r := rand.New(rand.NewSource(99))
	docCount := 0
	for b.Loop() {
		docs[docCount] = db.MakeTestLogEntry(uint64(docCount), fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", docCount), "1-a")
	}
	// shuffle sequences
	for i := docCount - 1; i >= 0; i-- {
		j := int(r.Float64() * float64(docCount))
		oldSeq := docs[i].Sequence
		docs[i].Sequence = docs[j].Sequence
		docs[j].Sequence = oldSeq
	}

	b.ResetTimer()
	for i := range docCount {
		cache.AddToCacheForTest(b, ctx, docs[i], false)
	}
}

func generateDocs(percentInsert float64, N int) ([]string, []string) {

	docIDs := make([]string, N)
	revStrings := make([]string, N)
	revCount := make(map[int]int)
	r := rand.New(rand.NewSource(99))
	uniqueDocs := percentInsert / 100 * float64(N)
	maxRevCount := 0
	for i := range N {
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

func verifyChannelSequences(entries []*db.LogEntry, sequences []uint64) bool {
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

func verifyChannelDocIDs(entries []*db.LogEntry, docIDs []string) bool {
	if len(entries) != len(docIDs) {
		log.Printf("verifyChannelDocIDs: entries size (%v) not equals to DocIDs size (%v)",
			len(entries), len(docIDs))
		return false
	}
	for index, docID := range docIDs {
		if entries[index].DocID != docID {
			log.Printf("verifyChannelDocIDs: DocID mismatch at index %v, entries=%s, DocIDs=%s", index, entries[index].DocID, docID)
			return false
		}
	}
	return true
}

type cvValues struct {
	source  string
	version uint64
}

func verifyCVEntries(entries []*db.LogEntry, cvs []cvValues) bool {
	for index, cv := range cvs {
		if entries[index].SourceID != cv.source {
			return false
		}
		if entries[index].Version != cv.version {
			return false
		}
	}
	return true
}

func writeEntries(entries []*db.LogEntry) {
	for index, entry := range entries {
		log.Printf("%d:seq=%d, docID=%s, revID=%s", index, entry.Sequence, entry.DocID, entry.RevID)
	}
}

// TestSingleChannelCacheOptionResolution pins which value wins when a cache option is supplied:
// a positive value is used, and anything else leaves the built-in default in place.
//
// Every option resolves through the same `if options.X > 0` shape, so the interesting values are
// the boundary (zero) and one either side of it. Existing tests construct a cache with options
// and then assert that construction succeeded, which never establishes which value actually took
// effect - so the assertions here are on the resolved option read back off the cache.
func TestSingleChannelCacheOptionResolution(t *testing.T) {
	const (
		configuredLength   = 7
		configuredChannels = 11
	)
	configuredAge := 23 * time.Second

	// Each option under test, with the default it falls back to and a configured value that is
	// deliberately different from that default.
	intOptions := []struct {
		name         string
		set          func(*db.ChannelCacheOptions, int)
		get          func(*db.ChannelCacheOptions) int
		defaultValue int
		configured   int
	}{
		{
			name:         "ChannelCacheMinLength",
			set:          func(o *db.ChannelCacheOptions, v int) { o.ChannelCacheMinLength = v },
			get:          func(o *db.ChannelCacheOptions) int { return o.ChannelCacheMinLength },
			defaultValue: db.DefaultChannelCacheMinLength,
			configured:   configuredLength,
		},
		{
			name:         "ChannelCacheMaxLength",
			set:          func(o *db.ChannelCacheOptions, v int) { o.ChannelCacheMaxLength = v },
			get:          func(o *db.ChannelCacheOptions) int { return o.ChannelCacheMaxLength },
			defaultValue: db.DefaultChannelCacheMaxLength,
			configured:   configuredLength,
		},
		{
			name:         "MaxNumChannels",
			set:          func(o *db.ChannelCacheOptions, v int) { o.MaxNumChannels = v },
			get:          func(o *db.ChannelCacheOptions) int { return o.MaxNumChannels },
			defaultValue: db.DefaultChannelCacheMaxNumber,
			configured:   configuredChannels,
		},
	}

	durationOptions := []struct {
		name         string
		set          func(*db.ChannelCacheOptions, time.Duration)
		get          func(*db.ChannelCacheOptions) time.Duration
		defaultValue time.Duration
		configured   time.Duration
	}{
		{
			name:         "ChannelCacheAge",
			set:          func(o *db.ChannelCacheOptions, v time.Duration) { o.ChannelCacheAge = v },
			get:          func(o *db.ChannelCacheOptions) time.Duration { return o.ChannelCacheAge },
			defaultValue: db.DefaultChannelCacheAge,
			configured:   configuredAge,
		},
		{
			name:         "LateLogAge",
			set:          func(o *db.ChannelCacheOptions, v time.Duration) { o.LateLogAge = v },
			get:          func(o *db.ChannelCacheOptions) time.Duration { return o.LateLogAge },
			defaultValue: db.DefaultLateLogAge,
			configured:   configuredAge,
		},
	}

	// resolve builds a cache with only the option under test set, and returns its resolved options.
	resolve := func(t *testing.T, apply func(*db.ChannelCacheOptions)) *db.ChannelCacheOptions {
		t.Helper()
		// Start from the zero value, not the defaults, so an unset option is genuinely unset.
		options := db.ChannelCacheOptions{}
		apply(&options)
		cache := db.NewSingleChannelCacheWithOptionsForTest(t, base.TestCtx(t), &db.QueryHandlerForTest{},
			channels.NewID("chanA", base.DefaultCollectionID), 0, options, nil)
		return cache.OptionsForTest(t)
	}

	for _, option := range intOptions {
		t.Run(option.name, func(t *testing.T) {
			// Zero is the boundary: the option is unset, so the default must survive. A `>=`
			// here would take the zero instead.
			resolved := resolve(t, func(o *db.ChannelCacheOptions) { option.set(o, 0) })
			assert.Equal(t, option.defaultValue, option.get(resolved), "zero leaves the default in place")

			// One below the boundary.
			resolved = resolve(t, func(o *db.ChannelCacheOptions) { option.set(o, -1) })
			assert.Equal(t, option.defaultValue, option.get(resolved), "a negative value leaves the default in place")

			// One above: the configured value wins. A `<=` here would discard it.
			resolved = resolve(t, func(o *db.ChannelCacheOptions) { option.set(o, option.configured) })
			assert.Equal(t, option.configured, option.get(resolved), "a positive value replaces the default")
		})
	}

	for _, option := range durationOptions {
		t.Run(option.name, func(t *testing.T) {
			resolved := resolve(t, func(o *db.ChannelCacheOptions) { option.set(o, 0) })
			assert.Equal(t, option.defaultValue, option.get(resolved), "zero leaves the default in place")

			resolved = resolve(t, func(o *db.ChannelCacheOptions) { option.set(o, -time.Second) })
			assert.Equal(t, option.defaultValue, option.get(resolved), "a negative value leaves the default in place")

			resolved = resolve(t, func(o *db.ChannelCacheOptions) { option.set(o, option.configured) })
			assert.Equal(t, option.configured, option.get(resolved), "a positive value replaces the default")
		})
	}

	// Setting one option must not disturb the others, which the per-option cases above cannot
	// show on their own.
	t.Run("other options untouched", func(t *testing.T) {
		resolved := resolve(t, func(o *db.ChannelCacheOptions) { o.ChannelCacheMinLength = configuredLength })
		assert.Equal(t, configuredLength, resolved.ChannelCacheMinLength)
		assert.Equal(t, db.DefaultChannelCacheMaxLength, resolved.ChannelCacheMaxLength)
		assert.Equal(t, db.DefaultChannelCacheAge, resolved.ChannelCacheAge)
		assert.Equal(t, db.DefaultChannelCacheMaxNumber, resolved.MaxNumChannels)
		assert.Equal(t, db.DefaultLateLogAge, resolved.LateLogAge)
	})
}

// newTestCacheStats returns a fresh CacheStats, the three-line preamble every cache test needs.
func newTestCacheStats(t *testing.T) *base.CacheStats {
	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, false, nil, nil)
	require.NoError(t, err)
	return dbstats.Cache()
}

// TestSingleChannelCacheGetChangesComposition covers how GetChanges decides between the cache and
// a query, and how it stitches the two result sets together. The suite asserts that entries come
// back, never how many, from where, or what range the cache claims to be valid for - so every
// threshold in the compose step is unpinned.
func TestSingleChannelCacheGetChangesComposition(t *testing.T) {
	const channelName = "chanA"
	chanID := channels.NewID(channelName, base.DefaultCollectionID)
	queryEntry := func(seq int) *db.LogEntry { return db.MakeTestLogEntryForChannels(seq, []string{channelName}) }

	t.Run("cache valid from exactly startSeq is a hit", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		queryHandler := &db.QueryHandlerForTest{}
		cache := db.NewSingleChannelCacheForTest(t, queryHandler, chanID, 6, stats)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(6, "doc6", "1-a"), false)

		// since=5 makes startSeq 6, equal to validFrom: the cache covers the request exactly.
		entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithSeq(t, db.SequenceID{Seq: 5}))
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, 0, queryHandler.QueryCount(), "an exact cache match must not query")
		assert.Equal(t, int64(1), stats.ChannelCacheHits.Value())
	})

	t.Run("query path leaves pending queries balanced", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		queryHandler := &db.QueryHandlerForTest{}
		queryHandler.SeedEntries(db.LogEntries{queryEntry(1), queryEntry(2)})
		cache := db.NewSingleChannelCacheForTest(t, queryHandler, chanID, 10, stats)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc10", "1-a"), false)

		_, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
		require.NoError(t, err)
		assert.Equal(t, 1, queryHandler.QueryCount())
		assert.Equal(t, int64(1), stats.ChannelCacheMisses.Value())
		assert.Equal(t, int64(0), stats.ChannelCachePendingQueries.Value(), "the gauge must return to zero")
	})

	t.Run("query that hit its limit is not cached beyond its last result", func(t *testing.T) {
		ctx := base.TestCtx(t)
		queryHandler := &db.QueryHandlerForTest{}
		queryHandler.SeedEntries(db.LogEntries{queryEntry(1), queryEntry(2), queryEntry(3)})
		cache := db.NewSingleChannelCacheForTest(t, queryHandler, chanID, 10, newTestCacheStats(t))
		for seq := 10; seq <= 12; seq++ {
			cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(uint64(seq), fmt.Sprintf("doc%d", seq), "1-a"), false)
		}

		// The query stops at its limit, so it has not seen 3..9. Treating the results as valid
		// all the way to endSeq would cache that gap and silently lose those sequences.
		options := db.GetChangesOptionsWithZeroSeq(t)
		options.Limit = 2
		_, err := cache.GetChanges(ctx, options)
		require.NoError(t, err)

		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(10), validFrom, "a limited query must not extend the cache's valid range")
	})

	t.Run("full cache does not widen its valid range", func(t *testing.T) {
		ctx := base.TestCtx(t)
		options := db.ChannelCacheOptions{ChannelCacheMaxLength: 2}
		queryHandler := &db.QueryHandlerForTest{}
		cache := db.NewSingleChannelCacheWithOptionsForTest(t, base.TestCtx(t), queryHandler, chanID, 10, options, newTestCacheStats(t))
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc10", "1-a"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(11, "doc11", "1-a"), false)

		// An empty query result still widens validFrom - prependChanges treats "found nothing"
		// as proof the gap is empty - so the full-cache guard is what holds the range here.
		// This pins that coupling of "room to cache" and "may widen the range" rather than
		// catching an error: widening would not itself be wrong.
		entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
		require.NoError(t, err)
		assert.True(t, verifyChannelSequences(entries, []uint64{10, 11}))

		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(10), validFrom, "a full cache must keep its valid range")
	})

	t.Run("limit truncates the combined result", func(t *testing.T) {
		ctx := base.TestCtx(t)
		queryHandler := &db.QueryHandlerForTest{}
		queryHandler.SeedEntries(db.LogEntries{queryEntry(1), queryEntry(2)})
		cache := db.NewSingleChannelCacheForTest(t, queryHandler, chanID, 10, newTestCacheStats(t))
		for seq := 10; seq <= 14; seq++ {
			cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(uint64(seq), fmt.Sprintf("doc%d", seq), "1-a"), false)
		}

		// 2 from the query plus 5 cached is 7, but only 4 were asked for.
		options := db.GetChangesOptionsWithZeroSeq(t)
		options.Limit = 4
		entries, err := cache.GetChanges(ctx, options)
		require.NoError(t, err)
		require.Len(t, entries, 4)
		assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 10, 11}))
	})

	t.Run("overlapping sequence appears once", func(t *testing.T) {
		ctx := base.TestCtx(t)
		queryHandler := &db.QueryHandlerForTest{}
		for seq := 1; seq <= 10; seq++ {
			queryHandler.SeedEntries(db.LogEntries{queryEntry(seq)})
		}
		cache := db.NewSingleChannelCacheForTest(t, queryHandler, chanID, 10, newTestCacheStats(t))
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc_10", "1-abc"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(11, "doc_11", "1-abc"), false)

		// endSeq is set to cacheValidFrom deliberately, so seq 10 comes back from both sides.
		entries, err := cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
		require.NoError(t, err)
		assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}))
	})
}

// TestSingleChannelCachePruneAge covers pruneCacheAge, the background task that bounds channel
// cache memory by age.
// Entries older than ChannelCacheAge are dropped, except that ChannelCacheMinLength entries are
// always retained regardless of age.
func TestSingleChannelCachePruneAge(t *testing.T) {
	chanID := channels.NewID("chanA", base.DefaultCollectionID)
	const cacheAge = time.Minute

	agedEntry := func(seq uint64, age time.Duration) *db.LogEntry {
		entry := db.MakeTestLogEntry(seq, fmt.Sprintf("doc%d", seq), "1-a")
		received := time.Now().Add(-age)
		entry.TimeReceived = channels.NewFeedTimestamp(&received)
		return entry
	}

	// MinLength 2 gives pruning a floor to stop at; MaxLength 10 keeps length pruning out of it.
	options := db.ChannelCacheOptions{ChannelCacheMinLength: 2, ChannelCacheMaxLength: 10, ChannelCacheAge: cacheAge}

	t.Run("stale entries are pruned down to MinLength", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		cache := db.NewSingleChannelCacheWithOptionsForTest(t, ctx, &db.QueryHandlerForTest{}, chanID, 1, options, stats)
		for seq := uint64(1); seq <= 5; seq++ {
			cache.AddToCacheForTest(t, ctx, agedEntry(seq, 2*cacheAge), false)
		}
		active, _, _ := getCacheUtilization(stats)
		require.Equal(t, 5, active)

		cache.PruneCacheAgeForTest(t, ctx)

		// MinLength is a floor: pruning stops at 2 even though all five are stale.
		require.Len(t, cache.LogsForTest(t), 2)
		assert.True(t, verifyChannelSequences(cache.LogsForTest(t), []uint64{4, 5}))

		// validFrom must advance past the last entry dropped, not to it - the cache no longer
		// holds seq 3, so claiming to be valid from 3 would hide it from a resuming feed.
		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(4), validFrom)

		active, _, _ = getCacheUtilization(stats)
		assert.Equal(t, 2, active, "utilization must fall by the number pruned")
	})

	t.Run("entries within the age are kept", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		cache := db.NewSingleChannelCacheWithOptionsForTest(t, ctx, &db.QueryHandlerForTest{}, chanID, 1, options, stats)
		for seq := uint64(1); seq <= 5; seq++ {
			cache.AddToCacheForTest(t, ctx, agedEntry(seq, 0), false)
		}

		cache.PruneCacheAgeForTest(t, ctx)

		require.Len(t, cache.LogsForTest(t), 5)
		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(1), validFrom)
		active, _, _ := getCacheUtilization(stats)
		assert.Equal(t, 5, active)
	})
}

// TestSingleChannelCacheInsertChange covers the out-of-order insert path: where a change lands
// when its sequence is not greater than the last cached one, and what happens when the cache
// already holds a different revision of the same document.
func TestSingleChannelCacheInsertChange(t *testing.T) {
	chanID := channels.NewID("chanA", base.DefaultCollectionID)

	t.Run("out-of-order sequence is inserted in order", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{}, chanID, 0, stats)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc2", "1-a"), false)

		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc3", "1-a"), false)

		assert.True(t, verifyChannelSequences(cache.LogsForTest(t), []uint64{1, 2, 3}))
		assert.True(t, verifyChannelDocIDs(cache.LogsForTest(t), []string{"doc1", "doc3", "doc2"}))
		active, _, _ := getCacheUtilization(stats)
		assert.Equal(t, 3, active)
	})

	t.Run("duplicate sequence for a cached doc keeps the cached revision", func(t *testing.T) {
		ctx := base.TestCtx(t)
		cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{}, chanID, 0, newTestCacheStats(t))
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc2", "1-a"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc1", "1-a"), false)

		// A redelivery of the same sequence must not displace what is already cached.
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc1", "2-b"), false)

		logs := cache.LogsForTest(t)
		require.Len(t, logs, 2)
		assert.Equal(t, "1-a", logs[1].RevID, "the cached revision must be kept")

		// Utilization is deliberately not asserted here: insertChange increments it from a defer
		// that also fires on this ignore path, so the count reports 3 for a two-entry cache.
		// TODO: CBG-5863 - assert utilization is unchanged once that is fixed.
	})

	t.Run("later revision out of order replaces the earlier entry", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{}, chanID, 0, stats)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc2", "1-a"), false)

		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(3, "doc1", "2-b"), false)

		assert.True(t, verifyChannelSequences(cache.LogsForTest(t), []uint64{3, 5}))
		assert.True(t, verifyChannelDocIDs(cache.LogsForTest(t), []string{"doc1", "doc2"}))

		// One entry replaced another, so the count is unchanged.
		active, _, _ := getCacheUtilization(stats)
		assert.Equal(t, 2, active)
	})

	t.Run("non-adjacent replacement keeps the log sorted", func(t *testing.T) {
		ctx := base.TestCtx(t)
		stats := newTestCacheStats(t)
		cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{}, chanID, 0, stats)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(1, "doc1", "1-a"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(2, "doc2", "1-a"), false)
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(5, "doc3", "1-a"), false)

		// The replaced entry is not adjacent to the insert point, so the shift path runs.
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(4, "doc1", "2-b"), false)

		assert.True(t, verifyChannelSequences(cache.LogsForTest(t), []uint64{2, 4, 5}))
		assert.True(t, verifyChannelDocIDs(cache.LogsForTest(t), []string{"doc2", "doc1", "doc3"}))
		active, _, _ := getCacheUtilization(stats)
		assert.Equal(t, 3, active)
	})
}

// TestSingleChannelCachePrependChanges covers how query results are folded back in front of the
// cache, and what the cache then claims to be valid from. validFrom is the sequence the cache
// asserts it is complete from, so widening it wrongly makes a resuming feed skip changes.
func TestSingleChannelCachePrependChanges(t *testing.T) {
	chanID := channels.NewID("chanA", base.DefaultCollectionID)

	t.Run("empty result widens the valid range when it abuts the cache", func(t *testing.T) {
		ctx := base.TestCtx(t)
		cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{}, chanID, 10, newTestCacheStats(t))
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc10", "1-a"), false)

		// A query over 1-10 that returned nothing proves the gap is empty, so the cache may claim
		// it. changesValidTo is exactly validFrom because GetChanges queries with
		// endSeq = cacheValidFrom, making the boundary here the ordinary case rather than an edge.
		prepended := cache.PrependChangesForTest(t, ctx, db.LogEntries{}, 1, 10)

		assert.Equal(t, 0, prepended)
		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(1), validFrom)
	})

	t.Run("filling an empty cache keeps the caller's valid-from", func(t *testing.T) {
		ctx := base.TestCtx(t)
		options := db.ChannelCacheOptions{ChannelCacheMaxLength: 3}
		cache := db.NewSingleChannelCacheWithOptionsForTest(t, ctx, &db.QueryHandlerForTest{}, chanID, 100, options, newTestCacheStats(t))

		// Exactly MaxLength changes, so nothing is trimmed and the range the caller vouched for
		// stands - validFrom must not collapse to the first entry's sequence.
		changes := db.LogEntries{
			db.MakeTestLogEntry(5, "doc5", "1-a"),
			db.MakeTestLogEntry(6, "doc6", "1-a"),
			db.MakeTestLogEntry(7, "doc7", "1-a"),
		}
		prepended := cache.PrependChangesForTest(t, ctx, changes, 1, 100)

		assert.Equal(t, 3, prepended)
		require.Len(t, cache.LogsForTest(t), 3)
		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(1), validFrom)
	})

	t.Run("changes not reaching the cache are refused", func(t *testing.T) {
		ctx := base.TestCtx(t)
		cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{}, chanID, 10, newTestCacheStats(t))
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(10, "doc10", "1-a"), false)

		// Valid only to 5 leaves 6-9 unaccounted for, so prepending would create a gap.
		prepended := cache.PrependChangesForTest(t, ctx, db.LogEntries{db.MakeTestLogEntry(1, "doc1", "1-a")}, 1, 5)

		assert.Equal(t, 0, prepended)
		require.Len(t, cache.LogsForTest(t), 1)
		validFrom, _ := cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
		assert.Equal(t, uint64(10), validFrom)
	})
}

// TestSingleChannelCacheGetCachedChangesValidFrom pins the valid-from returned alongside a partial
// read. When since lands inside the cached log the caller is told the range starts after the last
// entry it skipped, so a feed resuming there covers the gap exactly - one too low and it re-reads,
// one too high and it misses an entry.
func TestSingleChannelCacheGetCachedChangesValidFrom(t *testing.T) {
	ctx := base.TestCtx(t)
	cache := db.NewSingleChannelCacheForTest(t, &db.QueryHandlerForTest{},
		channels.NewID("chanA", base.DefaultCollectionID), 1, newTestCacheStats(t))
	for seq := uint64(1); seq <= 5; seq++ {
		cache.AddToCacheForTest(t, ctx, db.MakeTestLogEntry(seq, fmt.Sprintf("doc%d", seq), "1-a"), false)
	}

	validFrom, changes := cache.GetCachedChanges(db.GetChangesOptionsWithSeq(t, db.SequenceID{Seq: 2}))

	assert.True(t, verifyChannelSequences(changes, []uint64{3, 4, 5}))
	assert.Equal(t, uint64(3), validFrom, "valid from the sequence after the last one skipped")

	// since below the log returns everything, and the cache's own validFrom stands.
	validFrom, changes = cache.GetCachedChanges(db.GetChangesOptionsWithZeroSeq(t))
	assert.True(t, verifyChannelSequences(changes, []uint64{1, 2, 3, 4, 5}))
	assert.Equal(t, uint64(1), validFrom)
}
