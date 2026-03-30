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
	// attempt to add a delta version that already exsits in the cache
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
	docRev, err := db.revisionCache.Get(ctx, docID, doc2.HLV.GetCurrentVersionString(), collection.GetCollectionID(), RevCacheDontLoadBackupRev)
	require.NoError(t, err)

	// try adding delta when delta sync is off
	deltaBytes := make([]byte, 0)
	testDelta := newRevCacheDelta(bytes.Repeat(deltaBytes, 10), doc1.HLV.GetCurrentVersionString(), docRev, false, nil)
	db.revisionCache.UpdateDelta(ctx, docID, rev1CV, rev2CV, collection.GetCollectionID(), testDelta)
}
