/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"bytes"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeltaCacheStats verifies that DeltaCacheNumItems, DeltaCacheHit and DeltaCacheMiss
// are correctly incremented as deltas are generated and subsequently retrieved from the cache.
func TestDeltaCacheStats(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync require enterprise edition")
	}
	if base.TestDisableRevCache() {
		t.Skip("test requires delta cache to be in use")
	}
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 10,
		},
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled:          true,
			RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := SafeDocumentName(t, t.Name())
	rev1ID, doc1, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)
	rev1CV := doc1.HLV.GetCurrentVersionString()

	_, doc2, err := collection.Put(ctx, docID, Body{"foo": "baz", BodyRev: rev1ID})
	require.NoError(t, err)
	rev2CV := doc2.HLV.GetCurrentVersionString()

	// All stats should start at zero.
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheMiss.Value())

	// First call: no delta is cached so one is generated and stored — a cache miss.
	delta, _, err := collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)
	require.NotNil(t, delta)
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())

	// Second call with the same rev pair: the generated delta is now cached — a cache hit.
	delta, _, err = collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)
	require.NotNil(t, delta)
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheHit.Value())
}

// TestDeltaCacheStatsWithBypassRevisionCache verifies delta cache stat behaviour when the revision
// cache is bypassed (MaxItemCount == 0). With BypassRevisionCache, UpdateDelta is a no-op so deltas
// are never stored; every GetDelta call must regenerate the delta (always a cache miss) and
// DeltaCacheNumItems remains 0.
func TestDeltaCacheStatsWithBypassRevisionCache(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync requires enterprise edition")
	}
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 0, // triggers BypassRevisionCache
		},
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled:          true,
			RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := SafeDocumentName(t, t.Name())
	rev1ID, doc1, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)
	rev1CV := doc1.HLV.GetCurrentVersionString()

	_, doc2, err := collection.Put(ctx, docID, Body{"foo": "baz", BodyRev: rev1ID})
	require.NoError(t, err)
	rev2CV := doc2.HLV.GetCurrentVersionString()

	// All stats should start at zero.
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheMiss.Value())

	// First call: delta is generated but not stored (UpdateDelta is a no-op) — a cache miss.
	delta, _, err := collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)
	require.NotNil(t, delta)
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())

	// Second call with the same rev pair: delta is still not cached, so another cache miss.
	delta, _, err = collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)
	require.NotNil(t, delta)
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	assert.Equal(t, int64(2), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())
}

func TestNumberBasedEvictionForDeltaCache(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync requires enterprise edition")
	}
	if base.TestDisableRevCache() {
		t.Skip("test requires delta cache to be in use")
	}
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 5,
		},
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled: true,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := SafeDocumentName(t, t.Name())
	_, doc, err := collection.Put(ctx, docID, Body{"test": "data"})
	require.NoError(t, err)

	docRev1, err := collection.getRev(ctx, docID, doc.HLV.GetCurrentVersionString(), 0, nil)
	require.NoError(t, err)

	deltaBytes := make([]byte, 0)
	testDelta := newRevCacheDelta(bytes.Repeat(deltaBytes, 10), doc.HLV.GetCurrentVersionString(), docRev1, false, nil)

	fromCV := Version{
		SourceID: "from",
		Value:    1,
	}
	originalFrom := fromCV
	toCV := Version{
		SourceID: "to",
		Value:    100,
	}
	originalTo := toCV
	for i := 0; i < 5; i++ {
		db.revisionCache.UpdateDelta(ctx, docID, fromCV.String(), toCV.String(), collection.GetCollectionID(), testDelta)
		fromCV.Value++
		toCV.Value++
	}
	// assert that num items in delta is 5
	assert.Equal(t, int64(5), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())
	// attempt to add a delta version that already exists in the cache
	db.revisionCache.UpdateDelta(ctx, docID, fromCV.String(), toCV.String(), collection.GetCollectionID(), testDelta)
	// assert that num items in delta is still 5
	assert.Equal(t, int64(5), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())

	// now add a new version and assert count is still 5
	fromCV.Value++
	toCV.Value++
	db.revisionCache.UpdateDelta(ctx, docID, fromCV.String(), toCV.String(), collection.GetCollectionID(), testDelta)
	assert.Equal(t, int64(5), db.DbStats.DeltaSync().DeltaCacheNumItems.Value())

	// assert that first item is no longer present in cache — adding one more entry beyond
	// capacity evicts the LRU item, which was originalFrom→originalTo (added first).
	orchestrator, ok := db.revisionCache.(*RevisionCacheOrchestrator)
	require.True(t, ok, "expected RevisionCacheOrchestrator")
	evictedDelta := orchestrator.deltaCache.getCachedDelta(ctx, docID, originalFrom.String(), originalTo.String(), collection.GetCollectionID())
	assert.Nil(t, evictedDelta, "first delta should have been evicted from the cache")
}

func TestUpdateDeltaWhenNoDeltaCacheInit(t *testing.T) {

	// setup db with no delta sync enabled, no delta cache will be initialised
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := SafeDocumentName(t, t.Name())
	rev1ID, doc1, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)
	rev1CV := doc1.HLV.GetCurrentVersionString()

	_, doc2, err := collection.Put(ctx, docID, Body{"foo": "baz", BodyRev: rev1ID})
	require.NoError(t, err)
	rev2CV := doc2.HLV.GetCurrentVersionString()
	docRev, _, err := db.revisionCache.Get(ctx, docID, doc2.HLV.GetCurrentVersionString(), collection.GetCollectionID(), RevCacheDontLoadBackupRev)
	require.NoError(t, err)

	// try adding delta when delta sync is off
	deltaBytes := make([]byte, 0)
	testDelta := newRevCacheDelta(bytes.Repeat(deltaBytes, 10), doc1.HLV.GetCurrentVersionString(), docRev, false, nil)
	db.revisionCache.UpdateDelta(ctx, docID, rev1CV, rev2CV, collection.GetCollectionID(), testDelta)
}

// TestDeltaNumberBasedEvictionDecrementsMemoryBytes verifies that when the delta cache
// hits its MaxItemCount limit and number-based eviction removes an item, the memory
// controller correctly decrements the evicted delta's bytes.
func TestDeltaNumberBasedEvictionDecrementsMemoryBytes(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta cache is EE only")
	}

	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	deltaStats := newTestDeltaStats()

	var getDocumentCounter, getRevisionCounter base.SgwIntStat
	bs := &testBackingStore{getDocumentCounter: &getDocumentCounter, getRevisionCounter: &getRevisionCounter}

	// MaxItemCount=2 caps the delta cache at 2 items; MaxBytes=0 so memory-based eviction
	// never fires — only number-based eviction is exercised here.
	opts := &RevisionCacheOptions{MaxItemCount: 2, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(bs, testCollectionID), revStats, deltaStats, true,
	)

	const perDeltaBytes = int64(10) // len("0123456789")
	makeDelta := func() RevisionDelta {
		d := RevisionDelta{DeltaBytes: []byte("0123456789")}
		d.CalculateDeltaBytes()
		return d
	}

	// Fill the delta cache to its item limit.
	orchestrator.UpdateDelta(ctx, "doc1", "from1", "to1", testCollectionID, makeDelta())
	orchestrator.UpdateDelta(ctx, "doc1", "from2", "to2", testCollectionID, makeDelta())
	assert.Equal(t, int64(2), deltaStats.DeltaCacheNumItems.Value())
	assert.Equal(t, 2*perDeltaBytes, revStats.cacheMemoryStat.Value(), "both deltas should be counted")

	// Adding a third delta triggers number-based eviction of the oldest (from1→to1).
	// Before the fix, bytesEvicted was never set so the stat remained at 30 instead of 20.
	orchestrator.UpdateDelta(ctx, "doc1", "from3", "to3", testCollectionID, makeDelta())

	assert.Equal(t, int64(2), deltaStats.DeltaCacheNumItems.Value(), "only 2 deltas should remain")
	assert.Equal(t, 2*perDeltaBytes, revStats.cacheMemoryStat.Value(),
		"evicted delta's bytes must be decremented; stat should equal 2 deltas not 3")
}

// TestGetWithDeltaTriggersMemoryEvictionOnMiss verifies that a backing-store load
// triggered by GetWithDelta (revision cache miss) calls triggerMemoryEviction so that
// memory pressure is relieved.
func TestGetWithDeltaTriggersMemoryEvictionOnMiss(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync and delta cache are EE only")
	}

	ctx := base.TestCtx(t)
	// testBackingStore serves any docID at cv{Value:123, SourceID:"test"}.
	rev1CV := Version{Value: 123, SourceID: "test"}
	rev2CV := Version{Value: 456, SourceID: "test"}

	// testBackingStore produces 118 bytes for a 4-char docID like "doc1".
	const expectedRevBytes = int64(118)
	const expectedDeltaBytes = int64(10) // len("delta12345")
	// maxBytes=118: rev alone just fits (118 NOT > 118); together (128 > 118) they trigger eviction.
	const maxBytes = expectedRevBytes

	revStats := newTestRevCacheStats()
	deltaStats := newTestDeltaStats()

	var getDocumentCounter, getRevisionCounter base.SgwIntStat
	bs := &testBackingStore{getDocumentCounter: &getDocumentCounter, getRevisionCounter: &getRevisionCounter}

	opts := &RevisionCacheOptions{MaxItemCount: 100, MaxBytes: maxBytes}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(bs, testCollectionID), revStats, deltaStats, true,
	)

	// Add the delta first so it carries the older access order.
	delta := RevisionDelta{DeltaBytes: []byte("delta12345")}
	delta.CalculateDeltaBytes()
	orchestrator.UpdateDelta(ctx, "doc1", rev1CV.String(), rev2CV.String(), testCollectionID, delta)
	assert.Equal(t, int64(1), deltaStats.DeltaCacheNumItems.Value())
	assert.Equal(t, expectedDeltaBytes, revStats.cacheMemoryStat.Value(), "only delta bytes counted so far")

	// GetWithDelta causes a revision backing-store miss, adding 118 bytes.
	// Total (128) exceeds maxBytes (118) → triggerMemoryEviction must fire and evict the
	// delta (older access order), leaving only the revision in memory.
	docRev, err := orchestrator.GetWithDelta(ctx, "doc1", rev1CV.String(), rev2CV.String(), testCollectionID)
	require.NoError(t, err)
	require.NotNil(t, docRev.BodyBytes, "revision should be loaded from backing store")

	_, revInCache := orchestrator.Peek(ctx, "doc1", rev1CV.String(), testCollectionID)
	assert.True(t, revInCache, "revision should remain in cache")

	cachedDelta := orchestrator.deltaCache.getCachedDelta(ctx, "doc1", rev1CV.String(), rev2CV.String(), testCollectionID)
	assert.Nil(t, cachedDelta, "delta should have been evicted by memory pressure from GetWithDelta")

	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())
	assert.Equal(t, int64(0), deltaStats.DeltaCacheNumItems.Value())
	assert.Equal(t, expectedRevBytes, revStats.cacheMemoryStat.Value(), "memory stat should equal revision bytes only")
}
