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

	cache := db.NewSingleChannelCacheForTest(t, collection, channels.NewID("Test1", collectionID), 0, dbstats.Cache())

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
	cache.Remove(ctx, collectionID, []string{"doc1"}, time.Now())
	entries, err = cache.GetChanges(ctx, db.GetChangesOptionsWithZeroSeq(t))
	require.Len(t, entries, 2)
	assert.True(t, verifyChannelSequences(entries, []uint64{2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc3", "doc5"}))
	assert.True(t, err == nil)

	// Try to remove doc5 with a startTime before it was added to ensure it's not removed
	// This will print a debug level log:
	// [DBG] Cache+: Skipping removal of doc "doc5" from cache "Test1" - received after purge
	cache.Remove(ctx, collectionID, []string{"doc5"}, time.Now().Add(-time.Second*5))
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
