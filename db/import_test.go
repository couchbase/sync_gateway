/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/require"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
)

func TestFeedImport(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyImport, base.KeyVV)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := t.Name()
	bodyBytes := []byte(`{"foo":"bar"}`)
	body := Body{}
	err := body.Unmarshal(bodyBytes)
	assert.NoError(t, err, "Error unmarshalling body")

	initialImportCount := db.DbStats.SharedBucketImport().ImportCount.Value()
	initialImportFeedProcessedCount := db.DbStats.SharedBucketImport().ImportFeedProcessedCount.Value()

	// Create via the SDK
	writeCas, err := collection.dataStore.WriteCas(ctx, key, 0, 0, bodyBytes, 0)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, initialImportCount+1)
	// processed twice:
	// - initial write
	// - after import
	base.RequireWaitForStat(t, db.DbStats.SharedBucketImport().ImportFeedProcessedCount.Value, initialImportFeedProcessedCount+2)

	// fetch the xattrs directly doc to confirm import (to avoid triggering on-demand import)
	var syncData SyncData
	xattrs, importCas, err := collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName, base.VirtualXattrRevSeqNo, base.VvXattrName})
	require.NoError(t, err)
	syncXattr, ok := xattrs[base.SyncXattrName]
	require.True(t, ok)
	require.NoError(t, base.JSONUnmarshal(syncXattr, &syncData))
	require.NotZero(t, syncData.Sequence, "Sequence should not be zero for imported doc")
	revSeqNo := RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo])
	require.NotZero(t, revSeqNo, "RevSeqNo should not be zero for imported doc")

	// verify mou and rev seqno
	xattrs, _, err = collection.dataStore.GetXattrs(ctx, key, []string{base.MouXattrName, base.VirtualXattrRevSeqNo, base.VvXattrName})
	var mou *MetadataOnlyUpdate
	require.NoError(t, err)
	mouXattr, mouOk := xattrs[base.MouXattrName]
	require.True(t, mouOk)
	require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
	require.Equal(t, base.CasToString(writeCas), mou.PreviousHexCAS)
	require.Equal(t, base.CasToString(importCas), mou.HexCAS)
	// curr revSeqNo should be 2, so prev revSeqNo is 1
	require.Equal(t, revSeqNo-1, mou.PreviousRevSeqNo)
	require.Contains(t, maps.Keys(xattrs), base.VvXattrName)
	var hlv HybridLogicalVector
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
	require.Equal(t, db.EncodedSourceID, hlv.SourceID)

	testCases := []struct {
		name             string
		eccv             bool
		startingCAS      uint64
		expectedSourceID string
	}{
		{
			name:             "ECCV enabled, high cas",
			eccv:             true,
			startingCAS:      math.MaxUint64,
			expectedSourceID: unknownSourceID,
		},
		{
			name:             "ECCV disabled",
			eccv:             false,
			startingCAS:      0,
			expectedSourceID: db.EncodedSourceID,
		},
		{
			name:             "ECCV enabled, low cas",
			eccv:             true,
			startingCAS:      1,
			expectedSourceID: db.EncodedSourceID,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			docID := SafeDocumentName(t, t.Name())
			db.CachedCCVEnabled.Store(testCase.eccv)
			for vBucket := range db.numVBuckets {
				db.CachedCCVStartingCas.Store(base.VBNo(vBucket), testCase.startingCAS)
			}
			initialImportCount := db.DbStats.SharedBucketImport().ImportCount.Value()
			_, err = collection.dataStore.WriteCas(ctx, docID, 0, 0, []byte(`{"foo":"bar"}`), 0)
			require.NoError(t, err)
			base.RequireWaitForStat(t, db.DbStats.SharedBucketImport().ImportCount.Value, initialImportCount+1)

			xattrs, _, err = collection.dataStore.GetXattrs(ctx, docID, []string{base.VvXattrName})
			require.NoError(t, err)
			require.Contains(t, maps.Keys(xattrs), base.VvXattrName)
			require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
			require.Equal(t, testCase.expectedSourceID, hlv.SourceID)
		})
	}
}

// TestOnDemandImport ensures that _mou is written correctly during an on-demand import
func TestOnDemandImport(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyImport)

	// SetupTestDBWithOptions sets autoImport=false
	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{})
	defer db.Close(ctx)

	// On-demand get
	// Create via the SDK
	baseKey := t.Name()
	t.Run("on-demand get", func(t *testing.T) {
		getKey := baseKey + "get"
		bodyBytes := []byte(`{"foo":"bar"}`)
		body := Body{}
		err := body.Unmarshal(bodyBytes)
		assert.NoError(t, err, "Error unmarshalling body")
		collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
		writeCas, err := collection.dataStore.WriteCas(ctx, getKey, 0, 0, bodyBytes, 0)
		require.NoError(t, err)
		startingRevSeqNo, _, err := collection.getRevSeqNo(ctx, getKey)
		require.NoError(t, err)

		// fetch the document to trigger on-demand import
		doc, err := collection.GetDocument(ctx, getKey, DocUnmarshalAll)
		require.NoError(t, err)

		require.NotNil(t, doc.MetadataOnlyUpdate)
		require.Equal(t, base.CasToString(writeCas), doc.MetadataOnlyUpdate.PreviousHexCAS)
		require.Equal(t, base.CasToString(doc.Cas), doc.MetadataOnlyUpdate.HexCAS)
		require.Equal(t, startingRevSeqNo, doc.MetadataOnlyUpdate.PreviousRevSeqNo)
		require.Equal(t, db.EncodedSourceID, doc.HLV.SourceID)
	})

	// On-demand write
	// Create via the SDK
	t.Run("on-demand write", func(t *testing.T) {
		for _, funcName := range []string{"Put", "PutExistingRev", "PutExistingCurrentVersion"} {
			t.Run(funcName, func(t *testing.T) {
				writeKey := baseKey + "_" + funcName
				bodyBytes := []byte(`{"foo":"bar"}`)
				body := Body{}
				err := body.Unmarshal(bodyBytes)
				assert.NoError(t, err, "Error unmarshalling body")
				collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				writeCas, err := collection.dataStore.WriteCas(ctx, writeKey, 0, 0, bodyBytes, 0)
				require.NoError(t, err)

				newDoc := &Document{
					ID: writeKey,
				}
				newDoc.UpdateBodyBytes([]byte(`{"foo": "baz"}`))
				startingRevSeqNo, _, err := collection.getRevSeqNo(ctx, writeKey)
				require.NoError(t, err)

				_, rawBucketDoc, err := collection.GetDocumentWithRaw(ctx, writeKey, DocUnmarshalSync)
				require.NoError(t, err)

				switch funcName {
				case "Put":
					// Update the document to trigger on-demand import.  Write will be a conflict, but import should be performed
					_, doc, err := collection.Put(ctx, writeKey, Body{"foo": "baz"})
					require.Nil(t, doc)
					assertHTTPError(t, err, 409)
				case "PutExistingRev":
					fakeRevID := "1-abc"
					docHistory := []string{fakeRevID}
					noConflicts := true
					forceAllowConflictingTombstone := false
					_, _, err := collection.PutExistingRev(ctx, newDoc, docHistory, noConflicts, forceAllowConflictingTombstone, rawBucketDoc, ExistingVersionWithUpdateToHLV)
					assertHTTPError(t, err, 409)
				case "PutExistingCurrentVersion":
					hlv := NewHybridLogicalVector()
					var legacyRevList []string
					opts := PutDocOptions{
						NewDocHLV:      hlv,
						NewDoc:         newDoc,
						RevTreeHistory: legacyRevList,
						ExistingDoc:    rawBucketDoc,
					}
					_, _, _, err = collection.PutExistingCurrentVersion(ctx, opts)
					assertHTTPError(t, err, 409)
				default:
					require.FailNow(t, fmt.Sprintf("unexpected funcName: %s", funcName))
				}

				// fetch the mou xattr directly doc to confirm import (to avoid triggering on-demand get import)
				// verify mou
				xattrs, importCas, err := collection.dataStore.GetXattrs(ctx, writeKey, []string{base.MouXattrName, base.VvXattrName})
				require.NoError(t, err)
				mouXattr, mouOk := xattrs[base.MouXattrName]
				var mou *MetadataOnlyUpdate
				require.True(t, mouOk)
				require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
				require.Equal(t, base.CasToString(writeCas), mou.PreviousHexCAS)
				require.Equal(t, base.CasToString(importCas), mou.HexCAS)
				require.Equal(t, startingRevSeqNo, mou.PreviousRevSeqNo)
				var hlv HybridLogicalVector
				require.Contains(t, maps.Keys(xattrs), base.VvXattrName)
				require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
				require.Equal(t, db.EncodedSourceID, hlv.SourceID)
			})
		}
	})

	// Verify that the reload performed before an on-demand import (crud.go GetDocumentWithRaw)
	// fetches the _mou xattr, so rawBucketDoc returned to the caller contains it.
	t.Run("on-demand get includes mou xattr in reload", func(t *testing.T) {
		docKey := baseKey + "_mouReload"
		collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

		// SDK write: creates the doc without _sync or _mou
		_, err := collection.dataStore.WriteCas(ctx, docKey, 0, 0, []byte(`{"foo":"bar"}`), 0)
		require.NoError(t, err)

		// First on-demand import via GetDocument: SG writes _sync and _mou
		importedDoc, err := collection.GetDocument(ctx, docKey, DocUnmarshalAll)
		require.NoError(t, err)
		require.NotNil(t, importedDoc.MetadataOnlyUpdate)

		// External SDK write to the body: changes the CRC32 so IsSGWrite returns false on the
		// next read. Rosmar's WriteCas with a non-zero CAS preserves existing xattrs (including _mou).
		_, err = collection.dataStore.WriteCas(ctx, docKey, 0, importedDoc.Cas, []byte(`{"foo":"baz"}`), 0)
		require.NoError(t, err)

		// GetDocumentWithRaw detects a non-SG write, reloads the doc, then imports it.
		// The reload must include _mou in its xattr list so the returned rawBucketDoc is complete.
		_, rawBucketDoc, err := collection.GetDocumentWithRaw(ctx, docKey, DocUnmarshalAll)
		require.NoError(t, err)
		require.NotNil(t, rawBucketDoc)

		// rawBucketDoc.Xattrs reflects the pre-import state fetched at the reload step.
		// Without the fix, _mou was absent from the reload xattr list, so this would be nil
		// even though the bucket has a _mou xattr from the first import.
		mouBytes := rawBucketDoc.Xattrs[base.MouXattrName]
		require.NotNil(t, mouBytes, "_mou must be fetched during the on-demand import reload")
		var mou MetadataOnlyUpdate
		require.NoError(t, base.JSONUnmarshal(mouBytes, &mou))
		require.NotEmpty(t, mou.HexCAS)
	})

	// Verify that GetDocSyncData fetches _revseqno so the on-demand import it triggers
	// records the correct PreviousRevSeqNo in _mou (not 0).
	t.Run("on-demand GetDocSyncData sets correct mou pRev", func(t *testing.T) {
		docKey := baseKey + "_syncDataMou"
		collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

		// SDK write: creates doc without _sync or _mou
		_, err := collection.dataStore.WriteCas(ctx, docKey, 0, 0, []byte(`{"foo":"bar"}`), 0)
		require.NoError(t, err)

		// Capture revSeqNo before import — it must appear as _mou.pRev after import.
		// Without the fix, GetDocSyncData fetches without _revseqno, so doc.RevSeqNo=0
		// and _mou.pRev is written as 0 instead of the correct value.
		startingRevSeqNo, _, err := collection.getRevSeqNo(ctx, docKey)
		require.NoError(t, err)

		// GetDocSyncData detects a non-SG-write and triggers on-demand import.
		_, err = collection.GetDocSyncData(ctx, docKey)
		require.NoError(t, err)

		// Read _mou from the bucket to verify PreviousRevSeqNo was set correctly.
		xattrs, _, err := collection.dataStore.GetXattrs(ctx, docKey, []string{base.MouXattrName})
		require.NoError(t, err)
		var mou MetadataOnlyUpdate
		require.NoError(t, base.JSONUnmarshal(xattrs[base.MouXattrName], &mou))
		require.Equal(t, startingRevSeqNo, mou.PreviousRevSeqNo,
			"_mou.pRev must equal the pre-import revSeqNo, not 0")
	})

	testCases := []struct {
		name             string
		eccv             bool
		startingCAS      uint64
		expectedSourceID string
	}{
		{
			name:             "ECCV enabled, high cas",
			eccv:             true,
			startingCAS:      math.MaxUint64,
			expectedSourceID: unknownSourceID,
		},
		{
			name:             "ECCV disabled",
			eccv:             false,
			startingCAS:      0,
			expectedSourceID: db.EncodedSourceID,
		},
		{
			name:             "ECCV enabled, low cas",
			eccv:             true,
			startingCAS:      1,
			expectedSourceID: db.EncodedSourceID,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			docID := SafeDocumentName(t, t.Name())
			db.CachedCCVEnabled.Store(testCase.eccv)
			for vBucket := range db.numVBuckets {
				db.CachedCCVStartingCas.Store(base.VBNo(vBucket), testCase.startingCAS)
			}
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			_, err := collection.dataStore.WriteCas(ctx, docID, 0, 0, []byte(`{"foo":"bar"}`), 0)
			require.NoError(t, err)

			doc, err := collection.GetDocument(ctx, docID, DocUnmarshalAll)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedSourceID, doc.HLV.SourceID)
		})
	}
}

// There are additional tests that exercise the import functionality in rest/import_test.go

// 1. Write a doc to the bucket
// 2. Build params to migrateMeta (existing doc, raw doc, cas.. sgbucket docs)
// 3. Update doc in the bucket with new expiry
// 4. Call migrateMeta with stale args that have old expiry
// 5. Assert that migrateMeta does the right thing and respects the new expiry value
//
// See SG PR #3109 for more details on motivation for this test
// Tests when preserve expiry is not used (CBS < 7.0.0)
func TestMigrateMetadata(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := "TestMigrateMetadata"
	bodyBytes := rawDocWithSyncMeta()
	body := Body{}
	err := body.Unmarshal(bodyBytes)
	assert.NoError(t, err, "Error unmarshalling body")

	// Create via the SDK with sync metadata intact
	expirySeconds := time.Second * 30
	syncMetaExpiry := time.Now().Add(expirySeconds)
	_, err = collection.dataStore.Add(ctx, key, uint32(syncMetaExpiry.Unix()), bodyBytes)
	assert.NoError(t, err, "Error writing doc w/ expiry")

	// Get the existing bucket doc
	_, existingBucketDoc, err := collection.GetDocWithXattrs(ctx, key, DocUnmarshalAll)
	require.NoError(t, err)
	// Set the expiry value to a stale value (it's about to be stale, since below it will get updated to a later value)
	existingBucketDoc.Expiry = uint32(syncMetaExpiry.Unix())

	// Update doc in the bucket with new expiry
	laterExpirySeconds := time.Second * 60
	laterSyncMetaExpiry := time.Now().Add(laterExpirySeconds)
	updateCallbackFn := func(_ []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
		// This update function will not be "competing" with other updates, so it doesn't need
		// to handle being called back multiple times or performing any merging with existing values.
		exp := uint32(laterSyncMetaExpiry.Unix())
		return bodyBytes, &exp, false, nil
	}
	_, err = collection.dataStore.Update(ctx,
		key,
		uint32(laterSyncMetaExpiry.Unix()),
		updateCallbackFn)

	require.NoError(t, err)

	// Call migrateMeta with stale args that have old stale expiry
	_, err = collection.migrateMetadata(ctx, key, existingBucketDoc, &sgbucket.MutateInOptions{PreserveExpiry: false})
	assert.True(t, err != nil)
	assert.True(t, err == base.ErrCasFailureShouldRetry)

}

// Tests metadata migration where a document with inline sync data has been replicated by XDCR, so also has an
// existing HLV.  Migration should preserve the existing HLV while moving doc._sync to sync xattr
func TestMigrateMetadataWithHLV(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := "TestMigrateMetadata"
	bodyBytes := rawDocWithSyncMeta()
	body := Body{}
	err := body.Unmarshal(bodyBytes)
	assert.NoError(t, err, "Error unmarshalling body")

	hlv := &HybridLogicalVector{}
	require.NoError(t, hlv.AddVersion(CreateVersion("source123", 100)))
	hlv.CurrentVersionCAS = 100
	hlvBytes := base.MustJSONMarshal(t, hlv)
	xattrBytes := map[string][]byte{
		base.VvXattrName: hlvBytes,
	}

	// Create via the SDK with inline sync metadata and an existing _vv xattr
	_, err = collection.dataStore.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrBytes, nil, nil)
	require.NoError(t, err)

	// Get the existing bucket doc
	_, existingBucketDoc, err := collection.GetDocWithXattrs(ctx, key, DocUnmarshalAll)
	require.NoError(t, err)

	// Migrate metadata
	_, err = collection.migrateMetadata(ctx, key, existingBucketDoc, &sgbucket.MutateInOptions{PreserveExpiry: false})
	require.NoError(t, err)

	// Fetch the existing doc, ensure _vv is preserved
	var migratedHLV *HybridLogicalVector
	_, migratedBucketDoc, err := collection.GetDocWithXattrs(ctx, key, DocUnmarshalAll)
	require.NoError(t, err)
	migratedHLVBytes, ok := migratedBucketDoc.Xattrs[base.VvXattrName]
	require.True(t, ok)
	require.NoError(t, base.JSONUnmarshal(migratedHLVBytes, &migratedHLV))
	require.Equal(t, hlv.Version, migratedHLV.Version)
	require.Equal(t, hlv.SourceID, migratedHLV.SourceID)
	require.Equal(t, hlv.CurrentVersionCAS, migratedHLV.CurrentVersionCAS)

	migratedSyncXattrBytes, ok := migratedBucketDoc.Xattrs[base.SyncXattrName]
	require.True(t, ok)
	require.NotZero(t, len(migratedSyncXattrBytes))

}

// This invokes db.importDoc() with two different scenarios:
//
// Scenario 1: normal import
//
// 1. Write doc via SDK that is a pure KV doc, no sync metadata `{"key": "val"}` and with expiry value expiryA
// 2. Perform an update via SDK to update the expiry to expiry value expiryB
// 3. Invoke db.importDoc() and pass it the stale doc from step 1, that has expiryA
// 4. Do a get on the doc and verify that it has the later expiry value expiryB, which verifies it did a CAS retry
//
// Scenario 2: import with migration
//
// - Same as scenario 1, except that in step 1 it writes a doc with sync metadata, so that it excercises the migration code
// - Temporarily set expectedGeneration:2, see https://github.com/couchbase/sync_gateway/issues/3804
func TestImportWithStaleBucketDocCorrectExpiry(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	type testcase struct {
		docBody            []byte
		name               string
		expectedGeneration int
	}
	testCases := []testcase{
		{
			docBody:            rawDocNoMeta(),
			name:               "rawDocNoMeta",
			expectedGeneration: 1,
		},
		{
			docBody:            rawDocWithSyncMeta(),
			name:               "rawDocWithSyncMeta",
			expectedGeneration: 1,
		},
	}

	for _, testCase := range testCases {

		t.Run(testCase.name, func(t *testing.T) {
			key := fmt.Sprintf("TestImportDocWithStaleDoc%-s", testCase.name)
			bodyBytes := testCase.docBody
			body := Body{}
			err := body.Unmarshal(bodyBytes)
			assert.NoError(t, err, "Error unmarshalling body")

			// Create via the SDK
			expiryDuration := time.Minute * 30
			syncMetaExpiry := time.Now().Add(expiryDuration)
			_, err = collection.dataStore.Add(ctx, key, uint32(syncMetaExpiry.Unix()), bodyBytes)
			assert.NoError(t, err, "Error writing doc w/ expiry")

			// Get the existing bucket doc
			existingBucketDoc := getBucketDocument(t, collection.DatabaseCollection, key)
			require.NoError(t, err)

			syncMetaExpiryUnix := syncMetaExpiry.Unix()
			expiry := uint32(syncMetaExpiryUnix)

			// Perform an SDK update to turn existingBucketDoc into a stale doc
			laterExpiryDuration := time.Minute * 60
			laterSyncMetaExpiry := time.Now().Add(laterExpiryDuration)
			updateCallbackFn := func(_ []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
				// This update function will not be "competing" with other updates, so it doesn't need
				// to handle being called back multiple times or performing any merging with existing values.
				exp := uint32(laterSyncMetaExpiry.Unix())
				return bodyBytes, &exp, false, nil
			}
			_, err = collection.dataStore.Update(ctx,
				key,
				uint32(laterSyncMetaExpiry.Unix()),
				updateCallbackFn)

			require.NoError(t, err)

			// Import the doc (will migrate as part of the import since the doc contains sync meta)
			_, errImportDoc := collection.importDoc(ctx, key, body, &expiry, false, 0, existingBucketDoc, ImportOnDemand)
			assert.NoError(t, errImportDoc, "Unexpected error")

			// Make sure the doc in the bucket has expected XATTR
			assertXattrSyncMetaRevGeneration(t, collection.dataStore, key, testCase.expectedGeneration)

			// Verify the expiry has been preserved after the import
			expiry, err = collection.dataStore.GetExpiry(ctx, key)
			require.NoError(t, err, "Error calling GetExpiry()")
			updatedExpiryDuration := base.CbsExpiryToDuration(expiry)
			assert.True(t, updatedExpiryDuration > expiryDuration)
			assert.True(t, updatedExpiryDuration <= laterExpiryDuration)
		})
	}
}

func TestImportWithCasFailureUpdate(t *testing.T) {
	ctx := base.TestCtx(t)

	var db *Database
	var existingBucketDoc *sgbucket.BucketDocument
	var runOnce bool
	type testcase struct {
		callback func(key string)
		docname  string
	}

	syncDataInBodyCallback := func(key string) {
		if runOnce {
			var body map[string]any

			runOnce = false
			valStr := `{
				"field": "value",
				"field2": "val2",
				"_sync": {
					"rev": "2-abc",
					"sequence": 1,
					"recent_sequences": [
						1
					],
					"history": {
						"revs": [
							"2-abc",
							"1-abc"
						],
						"parents": [
							-1,
							0
						],
						"channels": [
							null,
							null
						]
					},
					"cas": "",
					"time_saved": "2017-11-29T12:46:13.456631-08:00"
				}
			}`

			collection, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			cas, _ := collection.dataStore.Get(ctx, key, &body)
			_, err := collection.dataStore.WriteCas(ctx, key, 0, cas, []byte(valStr), sgbucket.Raw)
			assert.NoError(t, err)
		}
	}

	syncDataInXattrCallback := func(key string) {
		if runOnce {

			runOnce = false
			valStr := `{
				"field": "value",
				"field2": "val2"
			}`

			xattrStr := `{
				"rev": "2-abc",
				"sequence": 1,
				"recent_sequences": [
					1
				],
				"history": {
					"revs": [
						"2-abc",
						"1-abc"
					],
					"parents": [
						-1,
						0
					],
					"channels": [
						null,
						null
					]
				},
				"cas": "",
				"time_saved": "2017-11-29T12:46:13.456631-08:00"
			}`

			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

			_, _, cas, _ := collection.dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
			_, err := collection.dataStore.WriteWithXattrs(ctx, key, 0, cas, []byte(valStr), map[string][]byte{base.SyncXattrName: []byte(xattrStr)}, nil, DefaultMutateInOpts())
			require.NoError(t, err)
		}
	}

	testcases := []testcase{
		{
			callback: syncDataInBodyCallback,
			docname:  "syncDataInBody",
		},
		{
			callback: syncDataInXattrCallback,
			docname:  "syncDataInXattr",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.docname, func(t *testing.T) {
			db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), base.LeakyBucketConfig{WriteWithXattrCallback: testcase.callback})
			defer db.Close(ctx)

			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

			bodyBytes := rawDocWithSyncMeta()
			body := Body{}
			err := body.Unmarshal(bodyBytes)
			assert.NoError(t, err, "Error unmarshalling body")

			// Put a doc with inline sync data via sdk
			_, err = collection.dataStore.Add(ctx, testcase.docname, 0, bodyBytes)
			assert.NoError(t, err)

			// Get the existing bucket doc
			_, existingBucketDoc, err = collection.GetDocWithXattrs(ctx, testcase.docname, DocUnmarshalAll)
			assert.NoError(t, err, fmt.Sprintf("Error retrieving doc w/ xattr: %v", err))

			importD := `{"new":"Val"}`
			bodyD := Body{}
			err = bodyD.Unmarshal([]byte(importD))
			assert.NoError(t, err, "Error unmarshalling body")

			runOnce = true
			// Trigger import
			_, err = collection.importDoc(ctx, testcase.docname, bodyD, nil, false, 0, existingBucketDoc, ImportOnDemand)
			assert.NoError(t, err)

			// Check document has the rev and new body
			var bodyOut map[string]any
			rawDoc, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, testcase.docname, []string{base.SyncXattrName})
			assert.NoError(t, err)

			require.Contains(t, maps.Keys(xattrs), base.SyncXattrName)
			var xattrOut map[string]any
			require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattrOut))
			require.NoError(t, base.JSONUnmarshal(rawDoc, &bodyOut))
			assert.Equal(t, "val2", bodyOut["field2"])
			assert.Equal(t, "2-abc", xattrOut["rev"])
		})
	}
}

func rawDocNoMeta() []byte {
	return []byte(`{"field": "value"}`)
}

func rawDocWithSyncMeta() []byte {

	return []byte(`
{
    "_sync": {
        "rev": "1-ca9ad22802b66f662ff171f226211d5c",
        "sequence": 1,
        "recent_sequences": [
            1
        ],
        "history": {
            "revs": [
                "1-ca9ad22802b66f662ff171f226211d5c"
            ],
            "parents": [
                -1
            ],
            "channels": [
                null
            ]
        },
        "cas": "",
        "time_saved": "2017-11-29T12:46:13.456631-08:00"
    },
    "field": "value"
}
`)

}

// Invokes db.importDoc() with a null document body
// Reproduces https://github.com/couchbase/sync_gateway/issues/3774
func TestImportNullDoc(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	key := "TestImportNullDoc"
	var body Body
	rawNull := []byte("null")
	existingDoc := &sgbucket.BucketDocument{Body: rawNull, Cas: 1}

	// Import a null document
	importedDoc, err := collection.importDoc(ctx, key+"1", body, nil, false, 1, existingDoc, ImportOnDemand)
	assert.Equal[error](t, base.ErrEmptyDocument, err)
	assert.True(t, importedDoc == nil, "Expected no imported doc")
}

func TestImportNullDocRaw(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Feed import of null doc
	exp := uint32(0)
	xattrs := map[string][]byte{
		base.SyncXattrName: []byte("{}"),
	}
	importOpts := importDocOptions{
		isDelete: false,
		expiry:   &exp,
		revSeqNo: 1,
		mode:     ImportFromFeed,
	}
	importedDoc, err := collection.ImportDocRaw(ctx, "TestImportNullDoc", []byte("null"), xattrs, importOpts, 1)
	assert.Equal[error](t, base.ErrEmptyDocument, err)
	assert.True(t, importedDoc == nil, "Expected no imported doc")
}

func assertXattrSyncMetaRevGeneration(t *testing.T, dataStore base.DataStore, key string, expectedRevGeneration int) {
	t.Helper()
	_, xattrs, _, err := dataStore.GetWithXattrs(base.TestCtx(t), key, []string{base.SyncXattrName})
	require.NoError(t, err, "Error Getting Xattr")
	require.Contains(t, maps.Keys(xattrs), base.SyncXattrName)
	var syncData SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
	require.NotEmpty(t, syncData.GetRevTreeID())
	generation, _ := ParseRevID(base.TestCtx(t), syncData.GetRevTreeID())
	log.Printf("assertXattrSyncMetaRevGeneration generation: %d rev: %s", generation, syncData.GetRevTreeID())
	assert.True(t, generation == expectedRevGeneration)
}

func TestEvaluateFunction(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport)
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	// Simulate unexpected error invoking import filter for document
	body := Body{"key": "value", "version": "1a"}
	source := "illegal function(doc) {}"
	importFilterFunc := NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err := importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.Error(t, err, "Unexpected token function error")
	assert.False(t, result, "Function evaluation result should be false")

	// Simulate boolean return value from import filter function
	body = Body{"key": "value", "version": "2a"}
	source = `function(doc) { if (doc.version == "2a") { return true; } else { return false; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.True(t, result, "Import filter function should return boolean value true")

	// Simulate non-boolean return value from import filter function; default switch case
	body = Body{"key": "value", "version": "2b"}
	source = `function(doc) { if (doc.version == "2b") { return 1.01; } else { return 0.01; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.Error(t, err, "Import filter function returned non-boolean value")
	assert.False(t, result, "Import filter function evaluation result should be false")

	// Simulate string return value true from import filter function
	body = Body{"key": "value", "version": "1a"}
	source = `function(doc) { if (doc.version == "1a") { return "true"; } else { return "false"; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.True(t, result, "Import filter function should return true")

	// Simulate string return value false from import filter function
	body = Body{"key": "value", "version": "2a"}
	source = `function(doc) { if (doc.version == "1a") { return "true"; } else { return "false"; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.False(t, result, "Import filter function should return false")

	// Simulate strconv.ParseBool: parsing "TruE": invalid syntax
	body = Body{"key": "value", "version": "1a"}
	source = `function(doc) { if (doc.version == "1a") { return "TruE"; } else { return "FaLsE"; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.Error(t, err, `strconv.ParseBool: parsing "TruE": invalid syntax`)
	assert.False(t, result, "Import filter function should return true")
}

func TestImportStampClusterUUID(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires Couchbase Server") // no cluster UUIDs in Walrus
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := "doc1"
	bodyBytes := rawDocNoMeta()

	_, err := collection.dataStore.Add(ctx, key, 0, bodyBytes)
	require.NoError(t, err)

	_, cas, err := collection.dataStore.GetRaw(ctx, key)
	require.NoError(t, err)

	xattrs, _, err := collection.dataStore.GetXattrs(ctx, key, []string{base.VirtualXattrRevSeqNo})
	require.NoError(t, err)
	docXattr, ok := xattrs[base.VirtualXattrRevSeqNo]
	require.True(t, ok)
	revSeqNo := RetrieveDocRevSeqNo(t, docXattr)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyMigrate, base.KeyImport)

	body := Body{}
	err = body.Unmarshal(rawDocNoMeta())
	require.NoError(t, err)
	existingDoc := &sgbucket.BucketDocument{Body: bodyBytes, Cas: cas}

	importedDoc, err := collection.importDoc(ctx, key, body, nil, false, revSeqNo, existingDoc, ImportOnDemand)
	require.NoError(t, err)
	if assert.NotNil(t, importedDoc) {
		require.Len(t, importedDoc.ClusterUUID, 32)
	}

	xattrs, _, err = collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName})
	require.NoError(t, err)
	require.Contains(t, maps.Keys(xattrs), base.SyncXattrName)
	var xattr map[string]any
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattr))
	require.Len(t, xattr["cluster_uuid"].(string), 32)
}

// TestImporNonZeroStart makes sure docs written before sync gateway start get imported
func TestImportNonZeroStart(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)

	doc1 := "doc1"
	revID1 := "1-2a9efe8178aa817f4414ae976aa032d9"

	_, err := bucket.GetSingleDataStore().Add(ctx, doc1, 0, rawDocNoMeta())
	require.NoError(t, err)

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	base.RequireWaitForStat(t, func() int64 {
		return collection.collectionStats.ImportCount.Value()
	}, 1)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().DCPReceivedCount.Value()
	}, 1)

	doc, err := collection.GetDocument(ctx, doc1, DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, revID1, doc.SyncData.GetRevTreeID())
}

// TestImportFeedInvalidInlineSyncMetadata tests avoiding an import error if the metadata is unmarshable
func TestImportFeedInvalidInlineSyncMetadata(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyImport)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	// make sure no documents are imported
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportCount.Value())
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())

	// docs named so they will both be on vBucket 1 in both 64 and 1024 vbuckets
	const (
		doc1 = "bookstand"
		doc2 = "chipchop"
	)
	// write a document with inline sync metadata that not unmarshalable into SyncData. This document will be ignored and logged at debug level.
	// 	[DBG] .. col:sg_test_0 <ud>bookstand</ud> not able to be imported. Error: Could not unmarshal _sync out of document body: json: cannot unmarshal number into Go struct field documentRoot._sync of type db.SyncData
	_, err := bucket.GetSingleDataStore().Add(ctx, doc1, 0, []byte(`{"foo" : "bar", "_sync" : 1 }`))
	require.NoError(t, err)

	// this will be imported
	err = bucket.GetSingleDataStore().Set(ctx, doc2, 0, nil, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
}

func TestImportFeedInvalidSyncMetadata(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyMigrate)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// make sure no documents are imported
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportCount.Value())
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())

	// docs named so they will both be on vBucket 1 in both 64 and 1024 vbuckets
	const (
		doc1 = "bookstand"
		doc2 = "chipchop"
		doc3 = "bookstand2"
		doc4 = "chipchop2"
		doc5 = "bookstand3"
	)

	// this document will be ignored for input with debug logging as follows:
	// 	[DBG] .. col:sg_test_0 <ud>bookstand</ud> not able to be imported. Error: Found _sync xattr ("1"), but could not unmarshal: json: cannot unmarshal number into Go value of type db.SyncData
	casOut, err := bucket.GetSingleDataStore().WriteWithXattrs(ctx, doc1, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`1`)}, nil, nil)
	require.NoError(t, err)

	// sync data with empty history
	_, err = bucket.GetSingleDataStore().WriteWithXattrs(ctx, doc2, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"history": {},"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)}, nil, nil)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 1)

	// sync data with history that current rev doesn't exist in
	_, err = bucket.GetSingleDataStore().WriteWithXattrs(ctx, doc3, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"attachments": {}, "history": {
	   "revs": ["1-ca9ad22802b66f662ff171f226211d5c"],"parents": [-1],"channels": [null]
	 },"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)}, nil, nil)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 2)

	// update bad doc above so it can be imported
	_, err = bucket.GetSingleDataStore().WriteWithXattrs(ctx, doc1, 0, casOut, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"attachments": {}, "history": {
	   "revs": ["1-cd809becc169215072fd567eebd8b8de"],"parents": [-1],"channels": [null]
	 },"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)}, nil, nil)
	require.NoError(t, err)

	// add a document that is able to be imported
	_, err = bucket.GetSingleDataStore().Add(ctx, doc4, 0, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 2)

	// add new doc and update it via sdk to include _sync in body
	_, _, err = collection.Put(ctx, doc5, Body{"foo": "bar"})
	require.NoError(t, err)
	err = bucket.GetSingleDataStore().SetRaw(ctx, doc5, 0, nil, []byte(`{"foo" : "bar", "_sync":"somedata"}`))
	require.NoError(t, err)

	// this will error when calling importDoc() because the _sync data in body will not unmarshal inside migrateMetadata
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 3)

	require.Equal(t, int64(3), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
	require.Equal(t, int64(2), db.DbStats.SharedBucketImport().ImportCount.Value())
}

func TestOnDemandImportPanicInvalidSyncData(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyMigrate)

	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{})
	defer db.Close(ctx)

	doc1ID := t.Name() + "_doc1"
	doc2ID := t.Name() + "_doc2"
	doc3ID := t.Name() + "_doc3"
	doc4ID := t.Name() + "_doc4"

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a doc
	rev1ID, doc, err := collection.Put(ctx, doc1ID, Body{"some": "data"})
	require.NoError(t, err)

	// update sync data to be invalid and try update this doc again to trigger on demand import for write
	xattrUpdate := make(map[string][]byte)
	xattrUpdate[base.SyncXattrName] = []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"history": {},"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)
	_, err = collection.dataStore.UpdateXattrs(ctx, doc1ID, 0, doc.Cas, xattrUpdate, nil)
	require.NoError(t, err)

	_, _, err = collection.Put(ctx, doc1ID, Body{"some": "data", "_rev": rev1ID})
	require.Error(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 1)

	// on demand import for get case
	casOut, err := collection.dataStore.WriteWithXattrs(ctx, doc2ID, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"history": {},"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)}, nil, nil)
	require.NoError(t, err)
	_, err = collection.GetDocument(ctx, doc2ID, DocUnmarshalAll)
	require.Error(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 2)

	// empty sync data in xattr, this will allow import processing to run
	_, err = collection.dataStore.WriteWithXattrs(ctx, doc4ID, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{}`)}, nil, nil)
	require.NoError(t, err)
	_, err = collection.GetDocument(ctx, doc4ID, DocUnmarshalAll)
	require.NoError(t, err)

	// on demand import with empty _sync data in body
	_, err = collection.dataStore.Add(ctx, doc3ID, 0, []byte(`{"some": "data", "_sync": {}}`))
	require.NoError(t, err)
	_, err = collection.GetDocument(ctx, doc3ID, DocUnmarshalAll)
	require.Error(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 3)

	// fix the doc so it can be imported
	_, err = collection.dataStore.WriteWithXattrs(ctx, doc2ID, 0, casOut, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"attachments": {}, "history": {
	   "revs": ["1-cd809becc169215072fd567eebd8b8de"],"parents": [-1],"channels": [null]
	 },"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)}, nil, nil)
	require.NoError(t, err)
	_, err = collection.GetDocument(ctx, doc2ID, DocUnmarshalAll)
	require.NoError(t, err)

	assert.Equal(t, int64(3), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
	assert.Equal(t, int64(2), db.DbStats.SharedBucketImport().ImportCount.Value())
}

func TestMigrateMetadataInvalidSyncData(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyMigrate)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	doc1ID := t.Name() + "_doc1"
	doc2ID := t.Name() + "_doc2"

	collection, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a docs with invalid sync data
	_, err := collection.dataStore.Add(ctx, doc1ID, 0, []byte(`{"some": "data", "_sync": {}}`))
	require.NoError(t, err)
	_, err = collection.dataStore.Add(ctx, doc2ID, 0, []byte(`{"some": "data", "_sync": {"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"history": {},"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 2)
}

func TestImportFeedNonJSONNewDoc(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyImport)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	// make sure no documents are imported
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportCount.Value())
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())

	// docs named so they will both be on vBucket 1 in both 64 and 1024 vbuckets
	const (
		doc1 = "bookstand"
		doc2 = "chipchop"
	)

	// logs because a JSON number is not a JSON object
	// 	[DBG] .. col:sg_test_0 <ud>bookstand</ud> not able to be imported. Error: Could not unmarshal _sync out of document body: json: cannot unmarshal number into Go value of type db.documentRoot
	_, err := bucket.GetSingleDataStore().Add(ctx, doc1, 0, []byte(`1`))
	require.NoError(t, err)

	_, err = bucket.GetSingleDataStore().Add(ctx, doc2, 0, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
}

func TestImportFeedNonJSONExistingDoc(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyMigrate, base.KeyImport)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	// make sure no documents are imported
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportCount.Value())
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())

	// docs named so they will both be on vBucket 1 in both 64 and 1024 vbuckets
	const (
		doc1 = "bookstand"
		doc2 = "chipchop"
	)

	_, err := bucket.GetSingleDataStore().Add(ctx, doc1, 0, []byte(`{"foo": "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)

	// logs and increments ImportErrorCount
	//     [INF] .. col:sg_test_0 Unmarshal error during importDoc json: cannot unmarshal number into Go value of type db.Body
	err = bucket.GetSingleDataStore().Set(ctx, doc1, 0, nil, []byte(`1`))
	require.NoError(t, err)

	_, err = bucket.GetSingleDataStore().Add(ctx, doc2, 0, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 2)
	require.Equal(t, int64(1), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
}

func TestMetadataOnlyUpdate(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	bodyBytes := []byte(`{"foo":"bar"}`)
	body := Body{}
	err := body.Unmarshal(bodyBytes)
	assert.NoError(t, err, "Error unmarshalling body")

	initialImportCount := db.DbStats.SharedBucketImport().ImportCount.Value()

	// 1. Create a document via SGW.  mou should not be updated
	_, _, err = collection.Put(ctx, "sgWrite", body)
	require.NoError(t, err)

	syncData, mou, _ := getSyncAndMou(t, collection, "sgWrite")
	require.NotNil(t, syncData)
	require.Nil(t, mou)
	require.NotZero(t, syncData.Sequence, "Sequence should not be zero for SG write")

	// 2. Create via the SDK
	writeCas, err := collection.dataStore.WriteCas(ctx, "sdkWrite", 0, 0, bodyBytes, 0)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, initialImportCount+1)

	// fetch the xattrs directly doc to confirm import (to avoid triggering on-demand import)
	syncData, mou, importCas := getSyncAndMou(t, collection, "sdkWrite")
	require.NotNil(t, syncData)
	require.NotNil(t, mou)
	require.NotZero(t, syncData.Sequence, "Sequence should not be zero for imported doc")
	previousRev := syncData.GetRevTreeID()

	// verify mou contents
	require.Equal(t, base.CasToString(writeCas), mou.PreviousHexCAS)
	require.Equal(t, base.CasToString(importCas), mou.HexCAS)

	// 3. Update the previous SDK write via SGW, ensure mou isn't updated again
	updatedBody := Body{"_rev": previousRev, "foo": "baz"}
	_, _, err = collection.Put(ctx, "sdkWrite", updatedBody)
	require.NoError(t, err)

	syncData, mou, _ = getSyncAndMou(t, collection, "sdkWrite")
	require.NotNil(t, syncData)
	require.NotZero(t, syncData.Sequence, "Sequence should not be zero for SG write")

	require.Nil(t, mou, "Mou should not be updated on SG write")

}

func TestImportResurrectionMou(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport, base.KeyCRUD)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := "mouResurrection"

	firstBody := Body{"foo": "bar"}
	_, _, err := collection.Put(ctx, docID, firstBody)
	require.NoError(t, err)

	syncData, mou, _ := getSyncAndMou(t, collection, docID)
	require.NotNil(t, syncData)
	require.Nil(t, mou)

	// Update via SDK, expect mou to be created
	err = collection.dataStore.Set(ctx, docID, 0, nil, []byte(`{"foo": "baz"}`))
	require.NoError(t, err)
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	syncData, mou, _ = getSyncAndMou(t, collection, docID)
	require.NotNil(t, mou)
	require.NotNil(t, syncData)

	// Delete via SDK, the mou will be updated by the import process
	require.NoError(t, collection.dataStore.Delete(ctx, docID))
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 2)
	syncData, mou, _ = getSyncAndMou(t, collection, docID)
	require.NotNil(t, mou)
	require.NotNil(t, syncData)

	// replace initial doc, expect mou to be removed
	_, _, err = collection.Put(ctx, docID, firstBody)
	require.NoError(t, err)

	syncData, mou, _ = getSyncAndMou(t, collection, docID)
	require.Nil(t, mou)
	require.NotNil(t, syncData)
}

// TestImportTombstoneWithConflict issues an SDK delete for a document with conflicting, non-tombstoned
// branches, then attempt to fetch the document.  The resulting document should not be treated as a metadata-only
// update, even though it originated with an SDK delete, because the existing non-winning revision body will be
// promoted to winning.
func TestImportConflictWithTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport, base.KeyCRUD)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{
		UnsupportedOptions: &UnsupportedOptions{WarningThresholds: &WarningThresholds{XattrSize: base.Ptr(uint32(base.DefaultWarnThresholdXattrSize))}},
		AllowConflicts:     base.Ptr(true)})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := t.Name()

	// Create rev 1 through SGW
	body := Body{"foo": "bar"}
	rev1ID, _, err := collection.Put(ctx, docID, body)
	require.NoError(t, err)

	// Create rev 2 through SGW
	body["foo"] = "abc"
	_, _, err = collection.PutExistingRevWithBody(ctx, docID, body, []string{"2-abc", rev1ID}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err)

	// Create conflicting rev 2 through SGW
	body["foo"] = "def"
	_, _, err = collection.PutExistingRevWithBody(ctx, docID, body, []string{"2-def", rev1ID}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err)

	docRev, err := collection.GetRev(ctx, docID, "", false, nil)
	require.NoError(t, err)
	require.Equal(t, "2-def", docRev.RevID)

	preImportDocBytes := db.DbStats.Database().DocWritesBytes.Value()
	preImportDocXattrBytes := db.DbStats.Database().DocWritesXattrBytes.Value()

	// Issue delete through SDK
	err = collection.dataStore.Delete(ctx, docID)
	require.NoError(t, err)
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)

	// assert after the import resurrection that doc body was written as doc body is needed for doc resurrection
	assert.Greater(t, db.DbStats.Database().DocWritesBytes.Value(), preImportDocBytes)
	assert.Greater(t, db.DbStats.Database().DocWritesXattrBytes.Value(), preImportDocXattrBytes)

	// Verify that post-import, the document is not a tombstone, and 2-abc has been promoted (GetRev with revID = "" returns active rev)
	docRev, err = collection.GetRev(ctx, docID, "", false, nil)
	require.NoError(t, err)
	require.Equal(t, "2-abc", docRev.RevID)
	require.False(t, docRev.Deleted)

	// Verify that mou was not populated for this import
	syncData, mou, _ := getSyncAndMou(t, collection, docID)
	require.Nil(t, mou)
	require.NotNil(t, syncData)
}

func getSyncAndMou(t *testing.T, collection *DatabaseCollectionWithUser, key string) (syncData *SyncData, mou *MetadataOnlyUpdate, cas uint64) {

	ctx := base.TestCtx(t)

	xattrs, cas, err := collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName, base.MouXattrName})
	require.NoError(t, err)

	if syncXattr, ok := xattrs[base.SyncXattrName]; ok {
		require.NoError(t, base.JSONUnmarshal(syncXattr, &syncData))
	}
	if mouXattr, ok := xattrs[base.MouXattrName]; ok {
		require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
	}
	return syncData, mou, cas

}

func TestImportCancelOnDocWithCorruptSequenceOverImportFeed(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport, base.KeyCRUD)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	// create a sequence much higher than _syc:seqs value
	const corruptSequence = MaxSequencesToRelease + 1000

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := t.Name()
	bodyBytes := []byte(`{"foo":"bar"}`)
	// Create via the SDK
	_, err := collection.dataStore.AddRaw(ctx, key, 0, bodyBytes)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)

	_, xattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
	require.NoError(t, err)

	// corrupt the document sequence
	var newSyncData map[string]any
	err = json.Unmarshal(xattrs[base.SyncXattrName], &newSyncData)
	require.NoError(t, err)
	newSyncData["sequence"] = corruptSequence
	_, err = collection.dataStore.UpdateXattrs(ctx, key, 0, cas, map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, newSyncData)}, DefaultMutateInOpts())
	require.NoError(t, err)

	// sdk update to trigger import
	require.NoError(t, collection.dataStore.SetRaw(ctx, key, 0, nil, []byte(`{"foo":"baz"}`)))

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 1)
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().CorruptSequenceCount.Value()
	}, 1)
}

func TestImportCancelOnDocWithCorruptSequenceOndemand(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport, base.KeyCRUD)
	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))
	db, ctx := setupTestDBForBucket(t, tb)
	key := t.Name()

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, _, err := collection.Put(ctx, key, Body{"foo": "bar"})
	require.NoError(t, err)

	// create a sequence much higher than _syc:seqs value
	const corruptSequence = MaxSequencesToRelease + 1000

	_, xattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
	require.NoError(t, err)

	// corrupt the document sequence
	var newSyncData map[string]any
	err = json.Unmarshal(xattrs[base.SyncXattrName], &newSyncData)
	require.NoError(t, err)
	newSyncData["sequence"] = corruptSequence
	_, err = collection.dataStore.UpdateXattrs(ctx, key, 0, cas, map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, newSyncData)}, DefaultMutateInOpts())
	require.NoError(t, err)

	// sdk update
	require.NoError(t, collection.dataStore.SetRaw(ctx, key, 0, nil, []byte(`{"foo":"baz"}`)))

	// trigger on demand import
	_, err = collection.GetDocument(ctx, key, DocUnmarshalAll)
	require.Error(t, err)
	var httpErr *base.HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusNotFound, httpErr.Status)

	// verify that the document was not imported
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 1)
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().CorruptSequenceCount.Value()
	}, 1)

}

func TestImportWithSyncCVAndNoVV(t *testing.T) {
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := SafeDocumentName(t, t.Name())

	_, doc, err := collection.Put(ctx, docID, Body{"foo": "baz"})
	require.NoError(t, err)

	err = collection.dataStore.RemoveXattrs(ctx, docID, []string{base.VvXattrName}, doc.Cas)
	require.NoError(t, err)

	base.RequireWaitForStat(t, db.DbStats.Database().Crc32MatchCount.Value, 1)

}

// TestGetDocSyncDataOnImportCancelled reproduces a race condition where importDoc
// returns (nil, nil) via ErrImportCancelled when fetching sync data through GetDocSyncData
//
// Race setup:
//  1. An SDK Delete tombstones a doc triggering on demand import for get pathway so OnDemandImportForGet is called with isDelete=true.
//  2. The first import callback succeeds and produces an updatedDoc for the
//     import tombstone write.
//  3. LeakyDataStore.UpdateCallback fires SetRaw resurrects the tombstone as a live document with
//     no _sync xattr, advancing the CAS.
//  4. CAS mismatch detected and retries. On retry it reads the now-live doc
//     (body != nil, no _sync). The CAS mismatch block in the import callback re-fetches
//     the body. Execution falls through to isDelete && doc.GetRevTreeID() == "", which fires and returns ErrImportCancelled.
//  5. importDoc's switch has no return statement in the ErrImportCancelled case, so it
//     falls through to return docOut, nil with docOut==nil.
func TestGetDocSyncDataOnImportCancelled(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport)

	docID := t.Name()

	db, ctx := setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), base.LeakyBucketConfig{})
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docDatastore := collection.GetCollectionDatastore()

	leakyDataStore, ok := base.AsLeakyDataStore(docDatastore)
	require.True(t, ok)

	// resurrectOnce ensures the Set resurrection only fires on the first
	// WriteUpdateWithXattrs attempt for the import, not on the CAS-mismatch retry,
	// preventing an infinite CAS loop.
	var resurrectOnce atomic.Bool
	// importTriggered gates the callback so the SetRaw resurrection only fires
	// during the import's WriteUpdateWithXattrs call, not during the initial Put.
	var importTriggered bool
	leakyDataStore.SetUpdateCallback(func(key string) {
		// UpdateCallback fires AFTER the import callback returns but BEFORE import
		// commits the tombstone write. Calling SetRaw here resurrects the tombstone as a
		// live document with no _sync xattr, advancing the CAS. SGW detects the mismatch and retries. On retry the import
		// callback sees body != nil and _sync RevTreeID == "", satisfying the
		// isDelete && GetRevTreeID() == "" condition that returns ErrImportCancelled.
		if key != docID || !importTriggered {
			return
		}
		if !resurrectOnce.Load() {
			err := docDatastore.Set(ctx, key, 0, nil, []byte(`{"foo":"resurrected"}`))
			require.NoError(t, err)
			resurrectOnce.Store(true)
		}
	})

	// Create doc via SG to establish a _sync xattr with a RevTreeID and a recorded CAS.
	_, _, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)
	db.WaitForPendingChanges(t)

	// SDK-style Delete triggering isSgWrite=false inside GetDocSyncData
	// which will trigger on-demand import with isDelete=true (rawDoc==nil).
	err = docDatastore.Delete(ctx, docID)
	require.NoError(t, err)

	// UpdateCallback now acts only during the import write, not Put.
	importTriggered = true

	// Ensure GetDocSyncData will handle a nil doc returned from an on-demand import event when ErrImportCancelled is returned.
	_, err = collection.GetDocSyncData(ctx, docID)
	require.Error(t, err, "expected an error when import is cancelled mid-flight, not a panic")
	base.RequireDocNotFoundError(t, err)
	require.True(t, resurrectOnce.Load())
}

// getBucketDocument reads the current version of a document and turns it into a sgbucket.BucketDocument. This is
// intended for test use only, since this gets expiry as a separate option.
func getBucketDocument(t *testing.T, collection *DatabaseCollection, docID string) *sgbucket.BucketDocument {
	ctx := base.TestCtx(t)
	xattrNames := append(collection.syncGlobalSyncMouRevSeqNoAndUserXattrKeys(), base.VirtualExpiry)
	body, xattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, docID, xattrNames)
	require.NoError(t, err)
	var expiry uint32
	if expiryBytes, ok := xattrs[base.VirtualExpiry]; ok {
		err := base.JSONUnmarshal(expiryBytes, &expiry)
		require.NoError(t, err)
		delete(xattrs, base.VirtualExpiry)
	}

	return &sgbucket.BucketDocument{
		Body:        body,
		Xattrs:      xattrs,
		Cas:         cas,
		Expiry:      expiry,
		IsTombstone: len(body) == 0,
	}
}

// docRevSeqNo returns the document's current server revision sequence number.
func docRevSeqNo(t *testing.T, collection *DatabaseCollectionWithUser, docID string) uint64 {
	t.Helper()
	xattrs, _, err := collection.dataStore.GetXattrs(base.TestCtx(t), docID, []string{base.VirtualXattrRevSeqNo})
	require.NoError(t, err)
	return RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo])
}

// mouWritePath is one code path that persists a metadata-only update: a mutation that changes a document's
// metadata without changing its body.
type mouWritePath struct {
	name string
	// setup writes the document and leaves it in the state the path under test expects. The document's CAS
	// and revision sequence number as of the end of setup are what _mou then has to point back at.
	setup func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) (revID string)
	// run performs the metadata-only write, and nothing else - the document's body is left as setup wrote
	// it, so _mou has to point back at the state observed between the two.
	run func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID, revID string)
}

// TestMetadataOnlyUpdateWritePaths pins _mou for every code path that writes one.
//
// A metadata-only update states that Sync Gateway changed a document's metadata without touching its body.
// Two properties have to hold for each path: _mou.cas has to name the mutation just made, which is how the
// import feed tells the mutation is Sync Gateway's own rather than an external write to import, and _mou.pCas
// has to name the mutation that last changed the body, so a reader can still identify which version of the
// body the document holds.
//
// Each case then performs a second metadata-only write, from a different path, to check that pCas and pRev
// are both carried forward rather than moved on to the first metadata-only write - they have to keep
// describing the same mutation as each other.
func TestMetadataOnlyUpdateWritePaths(t *testing.T) {
	// sdkBodyWrite writes the document body from outside Sync Gateway, leaving the metadata xattrs in place.
	// The document then needs importing, and the CAS of this write is the one _mou.pCas has to name.
	sdkBodyWrite := func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) {
		_, _, cas, err := collection.dataStore.GetWithXattrs(ctx, docID, []string{base.SyncXattrName})
		require.NoError(t, err)
		_, err = collection.dataStore.WriteCas(ctx, docID, 0, cas, []byte(`{"value": "written outside Sync Gateway"}`), 0)
		require.NoError(t, err)
	}

	putDoc := func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) string {
		revID, _, err := collection.Put(ctx, docID, Body{"value": 1234})
		require.NoError(t, err)
		return revID
	}

	paths := []mouWritePath{
		{
			// ImportDocRaw through OnDemandImportForGet
			name: "on-demand import for get",
			setup: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) string {
				revID := putDoc(t, ctx, collection, docID)
				sdkBodyWrite(t, ctx, collection, docID)
				return revID
			},
			run: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID, _ string) {
				_, err := collection.GetDocument(ctx, docID, DocUnmarshalAll)
				require.NoError(t, err)
			},
		},
		{
			// ImportDoc through OnDemandImportForWrite. The import creates a revision for the external write,
			// so the update that triggered it is rejected as a conflict; the import is the write under test.
			name: "on-demand import for write",
			setup: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) string {
				revID := putDoc(t, ctx, collection, docID)
				sdkBodyWrite(t, ctx, collection, docID)
				return revID
			},
			run: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID, revID string) {
				_, _, err := collection.Put(ctx, docID, Body{BodyRev: revID, "value": 5678})
				require.Error(t, err)
				status, _ := base.ErrorAsHTTPStatus(err)
				require.Equal(t, http.StatusConflict, status)
			},
		},
		{
			// MigrateAttachmentMetadata
			name: "attachment metadata migration",
			setup: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) string {
				revID, _, err := collection.Put(ctx, docID, Body{
					"value":         1234,
					BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
				})
				require.NoError(t, err)

				// put the document into the pre-4.0 layout, with its attachment metadata in the sync xattr
				value, _, err := collection.dataStore.GetRaw(ctx, docID)
				require.NoError(t, err)
				MoveAttachmentXattrFromGlobalToSync(t, collection.GetCollectionDatastore(), docID, value, true)
				return revID
			},
			run: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID, _ string) {
				syncData, _, cas := getSyncAndMou(t, collection, docID)
				require.NotEmpty(t, syncData.AttachmentsPre4dot0)
				require.NoError(t, collection.MigrateAttachmentMetadata(ctx, docID, cas, syncData))
			},
		},
		{
			// CompactDocChannelHistory
			name: "channel history compaction",
			setup: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID string) string {
				// compaction only writes when there is ended channel history to prune, so move the document
				// from one channel to another. The second put is the last write to the body.
				_, err := collection.UpdateSyncFun(ctx, `function(doc){channel(doc.chan);}`)
				require.NoError(t, err)

				rev1ID, _, err := collection.Put(ctx, docID, Body{"value": 1234, "chan": "A"})
				require.NoError(t, err)
				rev2ID, _, err := collection.Put(ctx, docID, Body{BodyRev: rev1ID, "value": 5678, "chan": "B"})
				require.NoError(t, err)
				return rev2ID
			},
			run: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID, _ string) {
				compacted, err := collection.CompactDocChannelHistory(ctx, docID, 100)
				require.NoError(t, err)
				require.NotEmpty(t, compacted, "compaction should have pruned channel history, or no write happens")
			},
		},
		{
			// ResyncDocument
			name:  "resync",
			setup: putDoc,
			run: func(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, docID, _ string) {
				// resync only writes when the sync function changes the document's channels
				_, err := collection.UpdateSyncFun(ctx, `function(doc){channel("resynced");}`)
				require.NoError(t, err)
				require.NoError(t, collection.ResyncDocument(ctx, docID, getBucketDocument(t, collection.DatabaseCollection, docID), false))
			},
		},
	}
	// The remaining path that writes an _mou, restampVersionCAS, does not fit this table: its metadata-only
	// write follows a body write it makes itself, so _mou points at that write rather than at the state
	// setup left behind. It is covered by TestRestampVersionCASMou.

	for _, tc := range paths {
		t.Run(tc.name, func(t *testing.T) {
			dbc, ctx := setupTestDB(t)
			defer dbc.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, dbc)

			_, err := collection.UpdateSyncFun(ctx, `function(doc){channel("initial");}`)
			require.NoError(t, err)

			const docID = "metadataOnlyUpdateDoc"
			revID := tc.setup(t, ctx, collection, docID)

			syncData, mou, bodyCas := getSyncAndMou(t, collection, docID)
			require.NotNil(t, syncData)
			require.Nil(t, mou, "a write of the body should not leave a metadata-only update behind")
			bodyRevSeqNo := docRevSeqNo(t, collection, docID)

			tc.run(t, ctx, collection, docID, revID)

			_, mou, cas := getSyncAndMou(t, collection, docID)
			require.NotNil(t, mou, "a metadata-only write has to record a metadata-only update")
			require.Equal(t, base.CasToString(cas), mou.HexCAS, "_mou.cas has to name this mutation, so it is not imported as an external write")
			require.Equal(t, base.CasToString(bodyCas), mou.PreviousHexCAS, "_mou.pCas has to name the mutation that last changed the body")
			require.Equal(t, bodyRevSeqNo, mou.PreviousRevSeqNo, "_mou.pRev has to name the same mutation as pCas")
			bodyWriteHexCas, bodyWriteRevSeqNo := mou.PreviousHexCAS, mou.PreviousRevSeqNo

			// A second metadata-only write, from a different path than the one under test. Both previous
			// values have to be carried forward, or they stop naming the last write to the body.
			_, err = collection.UpdateSyncFun(ctx, `function(doc){channel("chained");}`)
			require.NoError(t, err)
			require.NoError(t, collection.ResyncDocument(ctx, docID, getBucketDocument(t, collection.DatabaseCollection, docID), false))

			_, mou, cas = getSyncAndMou(t, collection, docID)
			require.NotNil(t, mou)
			require.Equal(t, base.CasToString(cas), mou.HexCAS)
			require.Equal(t, bodyWriteHexCas, mou.PreviousHexCAS, "pCas has to survive a second metadata-only write")
			require.Equal(t, bodyWriteRevSeqNo, mou.PreviousRevSeqNo, "pRev has to survive a second metadata-only write alongside pCas")
		})
	}
}

// TestAttachmentMigrationMouCarriedForward covers attachment metadata migration of a document whose previous
// mutation was already a metadata-only update, as happens to a document replicated by mobile XDCR from a
// cluster that had not migrated its attachment metadata. The migration has to carry the previous values
// forward from that update rather than naming it, so they keep describing the last write to the body.
func TestAttachmentMigrationMouCarriedForward(t *testing.T) {
	dbc, ctx := setupTestDB(t)
	defer dbc.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, dbc)
	ds := collection.GetCollectionDatastore()

	docID := "migrationMouCarriedForward"
	_, _, err := collection.Put(ctx, docID, Body{
		"value":         1234,
		BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	})
	require.NoError(t, err)

	// put the document into the pre-4.0 layout, with its attachment metadata in the sync xattr. This is the
	// last write to the body, so _mou has to point at it throughout.
	value, _, err := ds.GetRaw(ctx, docID)
	require.NoError(t, err)
	MoveAttachmentXattrFromGlobalToSync(t, ds, docID, value, true)
	_, _, bodyCas := getSyncAndMou(t, collection, docID)
	bodyRevSeqNo := docRevSeqNo(t, collection, docID)

	// stamp a metadata-only update over it, the way mobile XDCR does, so the document's most recent mutation
	// is one and the migration below has something to carry forward
	stampedMou := &MetadataOnlyUpdate{
		PreviousHexCAS:   base.CasToString(bodyCas),
		PreviousRevSeqNo: bodyRevSeqNo,
	}
	opts := &sgbucket.MutateInOptions{
		MacroExpansion: []sgbucket.MacroExpansionSpec{sgbucket.NewMacroExpansionSpec(XattrMouCasPath(), sgbucket.MacroCas)},
	}
	_, err = ds.UpdateXattrs(ctx, docID, 0, bodyCas, map[string][]byte{base.MouXattrName: base.MustJSONMarshal(t, stampedMou)}, opts)
	require.NoError(t, err)

	syncData, existingMou, preMigrationCas := getSyncAndMou(t, collection, docID)
	require.NotEmpty(t, syncData.AttachmentsPre4dot0, "the document should still be in the pre-4.0 layout")
	require.Equal(t, base.CasToString(preMigrationCas), existingMou.HexCAS, "the document's last mutation has to be a metadata-only update")

	require.NoError(t, collection.MigrateAttachmentMetadata(ctx, docID, preMigrationCas, syncData))

	_, mou, migratedCas := getSyncAndMou(t, collection, docID)
	require.NotNil(t, mou)
	require.Equal(t, base.CasToString(migratedCas), mou.HexCAS)
	require.Equal(t, base.CasToString(bodyCas), mou.PreviousHexCAS, "pCas has to be carried forward from the update being replaced")
	require.Equal(t, bodyRevSeqNo, mou.PreviousRevSeqNo, "pRev has to be carried forward alongside pCas")
}

// TestImportTombstoneAttachmentMetadata records the attachment metadata an import leaves on a tombstone,
// which is not what a Sync Gateway delete leaves.
//
// A Sync Gateway delete creates a revision with no attachments, so there is no global xattr to marshal and
// updateAndReturnDoc adds _globalSync to XattrsToDelete: the tombstone carries no attachment metadata. An
// import never reaches that delete. updateAndReturnDoc derives isNewDocCreation from currentValue == nil,
// and an SDK delete has already removed the body, so importing an existing tombstone is misclassified as a
// document creation and the delete is suppressed. On the ImportDoc path the hand-built existingBucketDoc
// (db/import.go) carries no _globalSync either, so the guard is missed twice over - fixing one of the two
// leaves the behaviour below unchanged. Whatever the bucket held survives:
//   - already migrated: _globalSync is left in place, so the tombstone keeps attachment metadata that a
//     Sync Gateway delete would have dropped
//   - not yet migrated: there is no global xattr to leave alone, and unmarshalDocumentWithXattrs has already
//     cleared SyncData.AttachmentsPre4dot0, so the tombstone ends up with none
//
// The second case is the one that matches a Sync Gateway delete. The first is a leftover rather than a
// guarantee, and a harmless one: the attachment compaction mark phase skips tombstones (it returns early on
// len(event.Value) == 0), so nothing reads it and the attachment blobs are swept either way.
func TestImportTombstoneAttachmentMetadata(t *testing.T) {
	const (
		attName   = "myatt"
		attDigest = "sha1-Lve95gjOVATpfV8EL5X4nxwjKHE="
		attLength = 12
	)

	testCases := []struct {
		name      string
		unmigrate bool
	}{
		{name: "migrated attachment metadata", unmigrate: false},
		{name: "unmigrated attachment metadata", unmigrate: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// no import listener, so the SDK tombstone created below stays unimported until this test
			// imports it, as OnDemandImportForWrite does when a write lands on top of a tombstone
			dbc, ctx := setupTestDB(t)
			defer dbc.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, dbc)
			ds := collection.GetCollectionDatastore()

			docID := "importTombstoneDoc"
			_, _, err := collection.Put(ctx, docID, bodyWithAttachment())
			require.NoError(t, err)
			require.NotEmpty(t, GetRawGlobalSyncAttachments(t, ds, docID))

			if tc.unmigrate {
				// make the document look like it was written by a pre-4.0 version of Sync Gateway:
				// attachment metadata in _sync.attachments, no _globalSync xattr
				value, _, err := ds.GetRaw(ctx, docID)
				require.NoError(t, err)
				MoveAttachmentXattrFromGlobalToSync(t, ds, docID, value, true)
				require.NotEmpty(t, GetRawSyncXattr(t, ds, docID).AttachmentsPre4dot0)
			}

			// delete through the SDK. The body is removed but the system xattrs are preserved, leaving a
			// tombstone that Sync Gateway did not write and so requires import.
			require.NoError(t, ds.Delete(ctx, docID))

			// load the tombstone without triggering the on-demand import that GetDocument would perform,
			// so ImportDoc is called with the same document state OnDemandImportForWrite passes it
			existingDoc, _, err := collection.getDocWithXattrs(ctx, docID, collection.syncGlobalSyncMouRevSeqNoAndUserXattrKeys(), DocUnmarshalAll)
			require.NoError(t, err)
			require.True(t, existingDoc.Deleted, "SDK delete should leave a body-less document")
			require.NotEmpty(t, existingDoc.Attachments(), "attachment metadata should be visible on the loaded tombstone")

			importedDoc, err := collection.ImportDoc(ctx, docID, existingDoc, importDocOptions{ // nolint:staticcheck
				isDelete: true,
				mode:     ImportOnDemand,
				revSeqNo: existingDoc.RevSeqNo,
			})
			require.NoError(t, err)
			require.True(t, importedDoc.IsDeleted(), "import of an SDK delete should produce a tombstone revision")

			// the pre-4.0 location must not be left populated by the import
			require.Empty(t, GetRawSyncXattr(t, ds, docID).AttachmentsPre4dot0)

			xattrs, _, err := ds.GetXattrs(ctx, docID, []string{base.GlobalXattrName})
			require.True(t, err == nil || base.IsXattrNotFoundError(err), "unexpected error reading global xattr: %v", err)
			atts := attachmentMetaFromGlobalXattr(t, xattrs[base.GlobalXattrName])

			if tc.unmigrate {
				require.Empty(t, atts, "an unmigrated document leaves no attachment metadata, matching a Sync Gateway delete")
				return
			}
			att, ok := atts[attName]
			require.True(t, ok, "attachment %q missing from _globalSync.attachments_meta, found %v", attName, atts)
			require.Equal(t, attDigest, att.Digest)
			require.Equal(t, attLength, att.Length)
		})
	}
}

// TestOnDemandImportForWriteOverLegacyTombstone drives the tombstone branch of ImportDoc through its only
// production caller, OnDemandImportForWrite: a Sync Gateway write landing on an unimported SDK tombstone
// whose attachment metadata was never migrated out of _sync.
//
// The import runs first and advances the document to a new tombstone revision, so the write itself is
// rejected as a conflict - what a client sees when a resurrection races the import of a tombstone. What the
// tombstone is left holding depends on the backing store, because it depends on whether the enclosing write
// re-runs its callback after the import changed the document's CAS:
//   - Couchbase Server: it does not, so the import persists the document the tombstone branch built, which
//     carries no global xattr, and the tombstone ends up with no attachment metadata.
//   - Rosmar: the write retries, re-reading the full xattr set, so the import sees the legacy
//     _sync.attachments merged in and writes them to _globalSync.
//
// The Couchbase Server outcome is the one that matches a Sync Gateway delete, which drops _globalSync
// deliberately - see TestImportTombstoneAttachmentMetadata. Both are deterministic (verified over 25 runs
// against Couchbase Server and 200 against Rosmar), and neither leaves metadata behind in the pre-4.0
// location, which is what this test is here to pin.
func TestOnDemandImportForWriteOverLegacyTombstone(t *testing.T) {
	dbc, ctx := setupTestDB(t)
	defer dbc.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, dbc)
	ds := collection.GetCollectionDatastore()

	docID := "resurrectedLegacyTombstone"
	_, _, err := collection.Put(ctx, docID, bodyWithAttachment())
	require.NoError(t, err)

	// make the document look like it was written by a pre-4.0 version of Sync Gateway: attachment metadata
	// in _sync.attachments, no _globalSync xattr
	value, _, err := ds.GetRaw(ctx, docID)
	require.NoError(t, err)
	MoveAttachmentXattrFromGlobalToSync(t, ds, docID, value, true)
	require.NotEmpty(t, GetRawSyncXattr(t, ds, docID).AttachmentsPre4dot0)

	// delete through the SDK, leaving a tombstone Sync Gateway did not write
	require.NoError(t, ds.Delete(ctx, docID))

	// attempt to resurrect the document. The write triggers OnDemandImportForWrite for the tombstone before
	// applying the update, and is then rejected as a conflict.
	_, _, err = collection.Put(ctx, docID, Body{
		"value":         5678,
		BodyAttachments: map[string]any{"newatt": map[string]any{"content_type": "text/plain", "data": "Z29vZGJ5ZQ=="}},
	})
	require.Error(t, err)
	status, _ := base.ErrorAsHTTPStatus(err)
	require.Equal(t, http.StatusConflict, status)

	// whatever the backing store does with the global xattr, the pre-4.0 location must not be left populated
	require.Empty(t, GetRawSyncXattr(t, ds, docID).AttachmentsPre4dot0)

	if base.UnitTestUrlIsWalrus() {
		requireAttachmentMetadataPreserved(t, ds, docID, "myatt", "sha1-Lve95gjOVATpfV8EL5X4nxwjKHE=", 12)
		return
	}
	xattrs, _, err := ds.GetXattrs(ctx, docID, []string{base.GlobalXattrName})
	require.True(t, err == nil || base.IsXattrNotFoundError(err), "unexpected error reading global xattr: %v", err)
	require.Empty(t, attachmentMetaFromGlobalXattr(t, xattrs[base.GlobalXattrName]),
		"tombstone import against Couchbase Server leaves the tombstone with no attachment metadata")
}

// attachmentMetaFromGlobalXattr unmarshals attachment metadata from a raw _globalSync xattr, returning nil
// for an absent xattr.
func attachmentMetaFromGlobalXattr(t *testing.T, rawGlobalSync []byte) AttachmentMap {
	t.Helper()
	if len(rawGlobalSync) == 0 {
		return nil
	}
	var globalSyncData struct {
		Attachments AttachmentMap `json:"attachments_meta"`
	}
	require.NoError(t, base.JSONUnmarshal(rawGlobalSync, &globalSyncData))
	return globalSyncData.Attachments
}
