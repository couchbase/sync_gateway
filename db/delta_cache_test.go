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
	"github.com/couchbase/sync_gateway/channels"
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
	docRev, _, err := db.revisionCache.Get(ctx, docID, doc2.HLV.GetCurrentVersionString(), collection.GetCollectionID(), RevCacheDontLoadBackupRev)
	require.NoError(t, err)

	// try adding delta when delta sync is off
	deltaBytes := make([]byte, 0)
	testDelta := newRevCacheDelta(bytes.Repeat(deltaBytes, 10), doc1.HLV.GetCurrentVersionString(), docRev, false, nil)
	db.revisionCache.UpdateDelta(ctx, docID, rev1CV, rev2CV, collection.GetCollectionID(), testDelta)
}

// TestGetDeltaStaleChannelAfterUserXattrUpdate verifies that GetDelta enforces access control using
// the current document channels rather than potentially-stale channel data from a cached delta.
//
// When a user xattr update changes a document's channels without creating a new revision ID
// (createNewRevIDSkipped), DocChanged removes the toRevision from the revision cache but leaves any
// previously-computed delta intact. Without the fix, a user whose access was revoked by the channel
// change could still receive the delta. With the fix, GetDelta fetches fresh channel information for
// the toRevision before honouring a cached delta hit. Both the CV-keyed and revID-keyed delta cache
// paths are exercised.
func TestGetDeltaStaleChannelAfterUserXattrUpdate(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync requires enterprise edition")
	}
	if base.TestDisableRevCache() {
		t.Skip("test requires delta cache to be in use")
	}

	const channelOld = "channelOld"
	const channelNew = "channelNew"

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 100,
		},
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled:          true,
			RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	orchestrator, ok := db.revisionCache.(*RevisionCacheOrchestrator)
	require.True(t, ok, "expected RevisionCacheOrchestrator")

	authenticator := collection.Authenticator(ctx)
	restrictedUser, err := authenticator.NewUser("restricted", "pass", base.SetOf(channelOld))
	require.NoError(t, err)
	authorizedUser, err := authenticator.NewUser("authorized", "pass", base.SetOf(channelNew))
	require.NoError(t, err)

	restrictedCollection := &DatabaseCollectionWithUser{DatabaseCollection: collection.DatabaseCollection, user: restrictedUser}
	authorizedCollection := &DatabaseCollectionWithUser{DatabaseCollection: collection.DatabaseCollection, user: authorizedUser}

	// run exercises one delta cache key type (CV or revID) on a fresh document.
	run := func(t *testing.T, docID, fromRev, toRev string, staleDelta RevisionDelta) {
		t.Helper()
		// Inject a stale delta that simulates the state after a user xattr update moved the document
		// from channelOld to channelNew without creating a new revision.
		orchestrator.deltaCache.addDelta(ctx, docID, fromRev, toRev, collection.GetCollectionID(), staleDelta)

		// Simulate DocChanged evicting the toRevision from the revision cache.
		db.revisionCache.Remove(ctx, docID, toRev, collection.GetCollectionID())

		hitsBefore := db.DbStats.DeltaSync().DeltaCacheHit.Value()

		// Restricted user (channelOld only) must be denied — the document is now in channelNew.
		delta, redactedRev, err := restrictedCollection.GetDelta(ctx, docID, fromRev, toRev)
		require.NoError(t, err)
		assert.Nil(t, delta, "restricted user must not receive delta after channel revocation")
		assert.NotNil(t, redactedRev, "expected a redacted revision for the unauthorized user")

		// Authorized user (channelNew) must still receive the cached delta.
		delta, redactedRev, err = authorizedCollection.GetDelta(ctx, docID, fromRev, toRev)
		require.NoError(t, err)
		assert.NotNil(t, delta, "authorized user must receive delta for the channel they have access to")
		assert.Nil(t, redactedRev, "expected no redaction for the authorized user")
		assert.Equal(t, hitsBefore+1, db.DbStats.DeltaSync().DeltaCacheHit.Value(), "expected a delta cache hit for authorized user")
	}

	t.Run("CV", func(t *testing.T) {
		docID := SafeDocumentName(t, t.Name()+"CV")
		rev1ID, doc1, err := collection.Put(ctx, docID, Body{"channels": []string{channelNew}, "foo": "bar"})
		require.NoError(t, err)
		rev1CV := doc1.HLV.GetCurrentVersionString()

		_, doc2, err := collection.Put(ctx, docID, Body{"channels": []string{channelNew}, "foo": "baz", BodyRev: rev1ID})
		require.NoError(t, err)
		rev2CV := doc2.HLV.GetCurrentVersionString()

		run(t, docID, rev1CV, rev2CV, RevisionDelta{ToCV: rev2CV, DeltaBytes: []byte(`{}`)})
	})

	t.Run("RevID", func(t *testing.T) {
		docID := SafeDocumentName(t, t.Name()+"revID")
		rev1ID, _, err := collection.Put(ctx, docID, Body{"channels": []string{channelNew}, "foo": "bar"})
		require.NoError(t, err)

		rev2ID, _, err := collection.Put(ctx, docID, Body{"channels": []string{channelNew}, "foo": "baz", BodyRev: rev1ID})
		require.NoError(t, err)

		run(t, docID, rev1ID, rev2ID, RevisionDelta{ToRevID: rev2ID, DeltaBytes: []byte(`{}`)})
	})
}

// TestGetDeltaThreeRevisions verifies that GetDelta returns correct deltas for all three rev-pair
// combinations of a three-revision document, and that the second request for each pair is served
// from the delta cache.
func TestGetDeltaThreeRevisions(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync requires enterprise edition")
	}
	if base.TestDisableRevCache() {
		t.Skip("test requires delta cache to be in use")
	}

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 100,
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

	rev2ID, doc2, err := collection.Put(ctx, docID, Body{"foo": "baz", BodyRev: rev1ID})
	require.NoError(t, err)
	rev2CV := doc2.HLV.GetCurrentVersionString()

	// assertDeltaTransforms verifies that applying delta.DeltaBytes to fromBody produces toBody.
	assertDeltaTransforms := func(t *testing.T, delta *RevisionDelta, fromBody, toBody Body) {
		t.Helper()
		require.NotNil(t, delta)
		patched := map[string]interface{}(fromBody)
		var deltaMap map[string]interface{}
		require.NoError(t, base.JSONUnmarshal(delta.DeltaBytes, &deltaMap))
		require.NoError(t, base.Patch(&patched, deltaMap))
		assert.Equal(t, map[string]interface{}(toBody), patched)
	}

	// First request for rev1→rev2: delta must be computed (cache miss) and correct.
	// rev2 is the current document version at this point, so toRev can be loaded directly.
	delta12, _, err := collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)
	assertDeltaTransforms(t, delta12, Body{"foo": "bar"}, Body{"foo": "baz"})
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())

	_, doc3, err := collection.Put(ctx, docID, Body{"foo": "qux", BodyRev: rev2ID})
	require.NoError(t, err)
	rev3CV := doc3.HLV.GetCurrentVersionString()

	// First request for rev2→rev3: delta must be computed (cache miss) and correct.
	// rev3 is the current document version at this point, so toRev can be loaded directly.
	delta23, _, err := collection.GetDelta(ctx, docID, rev2CV, rev3CV)
	require.NoError(t, err)
	assertDeltaTransforms(t, delta23, Body{"foo": "baz"}, Body{"foo": "qux"})
	assert.Equal(t, int64(2), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(0), db.DbStats.DeltaSync().DeltaCacheHit.Value())

	// Second request for rev1→rev2: must be served from the cache and still correct.
	delta12Cached, _, err := collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)
	assertDeltaTransforms(t, delta12Cached, Body{"foo": "bar"}, Body{"foo": "baz"})
	assert.Equal(t, int64(1), db.DbStats.DeltaSync().DeltaCacheHit.Value())

	// Second request for rev2→rev3: must be served from the cache and still correct.
	delta23Cached, _, err := collection.GetDelta(ctx, docID, rev2CV, rev3CV)
	require.NoError(t, err)
	assertDeltaTransforms(t, delta23Cached, Body{"foo": "baz"}, Body{"foo": "qux"})
	assert.Equal(t, int64(2), db.DbStats.DeltaSync().DeltaCacheHit.Value())

	// First request for rev1→rev3: a new pair, so the delta must be computed (cache miss) and correct.
	// rev3 is still the current document version; rev1's body is retrieved from the delta backup store.
	delta13, _, err := collection.GetDelta(ctx, docID, rev1CV, rev3CV)
	require.NoError(t, err)
	assertDeltaTransforms(t, delta13, Body{"foo": "bar"}, Body{"foo": "qux"})
	assert.Equal(t, int64(3), db.DbStats.DeltaSync().DeltaCacheMiss.Value())
	assert.Equal(t, int64(2), db.DbStats.DeltaSync().DeltaCacheHit.Value())
}

func TestAccessCheckOnNonCurrentRevision(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync requires enterprise edition")
	}
	if base.TestDisableRevCache() {
		t.Skip("test requires delta cache to be in use")
	}

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 100,
		},
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled:          true,
			RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	docID := SafeDocumentName(t, t.Name())
	rev1ID, doc1, err := collection.Put(ctx, docID, Body{"channels": "A"})
	require.NoError(t, err)
	rev1CV := doc1.HLV.GetCurrentVersionString()

	rev2ID, doc2, err := collection.Put(ctx, docID, Body{"channels": "A", BodyRev: rev1ID})
	require.NoError(t, err)
	rev2CV := doc2.HLV.GetCurrentVersionString()

	// request delta from rev1->rev2 to cache it
	_, _, err = collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.NoError(t, err)

	// create third revision to make rev2 non-current revision
	_, _, err = collection.Put(ctx, docID, Body{"channels": "A", BodyRev: rev2ID})
	require.NoError(t, err)

	// flush main revision cache
	db.FlushRevisionCacheForTest()

	// Now request delta from rev1->rev2, delta cached but no rev2 in revision cache so fetch is done to see channel access,
	// should return missing error given rev2 is no longer current revision
	_, _, err = collection.GetDelta(ctx, docID, rev1CV, rev2CV)
	require.ErrorContains(t, err, "404 missing")
}
