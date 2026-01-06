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
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestFeedImport(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

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
	writeCas, err := collection.dataStore.WriteCas(key, 0, 0, bodyBytes, 0)
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
	if db.UseMou() {
		var mou *MetadataOnlyUpdate
		require.NoError(t, err)
		mouXattr, mouOk := xattrs[base.MouXattrName]
		require.True(t, mouOk)
		require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
		require.Equal(t, base.CasToString(writeCas), mou.PreviousHexCAS)
		require.Equal(t, base.CasToString(importCas), mou.HexCAS)
		// curr revSeqNo should be 2, so prev revSeqNo is 1
		require.Equal(t, revSeqNo-1, mou.PreviousRevSeqNo)
	} else {
		// Expect not found fetching mou xattr
		require.Error(t, err)
	}
	require.Contains(t, xattrs, base.VvXattrName)
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
			_, err = collection.dataStore.WriteCas(docID, 0, 0, []byte(`{"foo":"bar"}`), 0)
			require.NoError(t, err)
			base.RequireWaitForStat(t, db.DbStats.SharedBucketImport().ImportCount.Value, initialImportCount+1)

			xattrs, _, err = collection.dataStore.GetXattrs(ctx, docID, []string{base.VvXattrName})
			require.NoError(t, err)
			require.Contains(t, xattrs, base.VvXattrName)
			require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
			require.Equal(t, testCase.expectedSourceID, hlv.SourceID)
		})
	}
}

// TestOnDemandImport ensures that _mou is written correctly during an on-demand import
func TestOnDemandImport(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyImport)
	base.SkipImportTestsIfNotEnabled(t)

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
		writeCas, err := collection.dataStore.WriteCas(getKey, 0, 0, bodyBytes, 0)
		require.NoError(t, err)
		startingRevSeqNo, _, err := collection.getRevSeqNo(ctx, getKey)
		require.NoError(t, err)

		// fetch the document to trigger on-demand import
		doc, err := collection.GetDocument(ctx, getKey, DocUnmarshalAll)
		require.NoError(t, err)

		if db.UseMou() {
			require.NotNil(t, doc.MetadataOnlyUpdate)
			require.Equal(t, base.CasToString(writeCas), doc.MetadataOnlyUpdate.PreviousHexCAS)
			require.Equal(t, base.CasToString(doc.Cas), doc.MetadataOnlyUpdate.HexCAS)
			require.Equal(t, startingRevSeqNo, doc.MetadataOnlyUpdate.PreviousRevSeqNo)
		} else {
			require.Nil(t, doc.MetadataOnlyUpdate)
		}
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
				writeCas, err := collection.dataStore.WriteCas(writeKey, 0, 0, bodyBytes, 0)
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
				if db.UseMou() {
					require.NoError(t, err)
					mouXattr, mouOk := xattrs[base.MouXattrName]
					var mou *MetadataOnlyUpdate
					require.True(t, mouOk)
					require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
					require.Equal(t, base.CasToString(writeCas), mou.PreviousHexCAS)
					require.Equal(t, base.CasToString(importCas), mou.HexCAS)
					require.Equal(t, startingRevSeqNo, mou.PreviousRevSeqNo)
				} else {
					// expect not found fetching mou xattr
					require.Error(t, err)
				}
				var hlv HybridLogicalVector
				require.Contains(t, xattrs, base.VvXattrName)
				require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
				require.Equal(t, db.EncodedSourceID, hlv.SourceID)
			})
		}
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
			_, err := collection.dataStore.WriteCas(docID, 0, 0, []byte(`{"foo":"bar"}`), 0)
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

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

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
	_, err = collection.dataStore.Add(key, uint32(syncMetaExpiry.Unix()), bodyBytes)
	assert.NoError(t, err, "Error writing doc w/ expiry")

	// Get the existing bucket doc
	_, existingBucketDoc, err := collection.GetDocWithXattrs(ctx, key, DocUnmarshalAll)
	require.NoError(t, err)
	// Set the expiry value to a stale value (it's about to be stale, since below it will get updated to a later value)
	existingBucketDoc.Expiry = uint32(syncMetaExpiry.Unix())

	// Update doc in the bucket with new expiry
	laterExpirySeconds := time.Second * 60
	laterSyncMetaExpiry := time.Now().Add(laterExpirySeconds)
	updateCallbackFn := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
		// This update function will not be "competing" with other updates, so it doesn't need
		// to handle being called back multiple times or performing any merging with existing values.
		exp := uint32(laterSyncMetaExpiry.Unix())
		return bodyBytes, &exp, false, nil
	}
	_, err = collection.dataStore.Update(
		key,
		uint32(laterSyncMetaExpiry.Unix()), // it's a bit confusing why the expiry needs to be passed here AND via the callback fn
		updateCallbackFn,
	)
	require.NoError(t, err)

	// Call migrateMeta with stale args that have old stale expiry
	_, err = collection.migrateMetadata(ctx, key, existingBucketDoc, &sgbucket.MutateInOptions{PreserveExpiry: false})
	assert.True(t, err != nil)
	assert.True(t, err == base.ErrCasFailureShouldRetry)

}

// Tests metadata migration where a document with inline sync data has been replicated by XDCR, so also has an
// existing HLV.  Migration should preserve the existing HLV while moving doc._sync to sync xattr
func TestMigrateMetadataWithHLV(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

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

		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {
			key := fmt.Sprintf("TestImportDocWithStaleDoc%-s", testCase.name)
			bodyBytes := testCase.docBody
			body := Body{}
			err := body.Unmarshal(bodyBytes)
			assert.NoError(t, err, "Error unmarshalling body")

			// Create via the SDK
			expiryDuration := time.Minute * 30
			syncMetaExpiry := time.Now().Add(expiryDuration)
			_, err = collection.dataStore.Add(key, uint32(syncMetaExpiry.Unix()), bodyBytes)
			assert.NoError(t, err, "Error writing doc w/ expiry")

			// Get the existing bucket doc
			_, existingBucketDoc, err := collection.GetDocWithXattrs(ctx, key, DocUnmarshalAll)
			assert.NoError(t, err, fmt.Sprintf("Error retrieving doc w/ xattr: %v", err))

			body = Body{}
			err = body.Unmarshal(existingBucketDoc.Body)
			assert.NoError(t, err, "Error unmarshalling body")

			// Set the expiry value
			syncMetaExpiryUnix := syncMetaExpiry.Unix()
			expiry := uint32(syncMetaExpiryUnix)

			// Perform an SDK update to turn existingBucketDoc into a stale doc
			laterExpiryDuration := time.Minute * 60
			laterSyncMetaExpiry := time.Now().Add(laterExpiryDuration)
			updateCallbackFn := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
				// This update function will not be "competing" with other updates, so it doesn't need
				// to handle being called back multiple times or performing any merging with existing values.
				exp := uint32(laterSyncMetaExpiry.Unix())
				return bodyBytes, &exp, false, nil
			}
			_, err = collection.dataStore.Update(
				key,
				uint32(laterSyncMetaExpiry.Unix()), // it's a bit confusing why the expiry needs to be passed here AND via the callback fn
				updateCallbackFn,
			)
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
	if !base.TestUseXattrs() {
		t.Skip("Test only works with a Couchbase server and XATTRS")
	}

	var db *Database
	var existingBucketDoc *sgbucket.BucketDocument
	var runOnce bool
	var ctx context.Context

	type testcase struct {
		callback func(key string)
		docname  string
	}

	syncDataInBodyCallback := func(key string) {
		if runOnce {
			var body map[string]interface{}

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
			cas, _ := collection.dataStore.Get(key, &body)
			_, err := collection.dataStore.WriteCas(key, 0, cas, []byte(valStr), sgbucket.Raw)
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
		t.Run(fmt.Sprintf("%s", testcase.docname), func(t *testing.T) {
			db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), base.LeakyBucketConfig{WriteWithXattrCallback: testcase.callback})
			defer db.Close(ctx)

			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

			bodyBytes := rawDocWithSyncMeta()
			body := Body{}
			err := body.Unmarshal(bodyBytes)
			assert.NoError(t, err, "Error unmarshalling body")

			// Put a doc with inline sync data via sdk
			_, err = collection.dataStore.Add(testcase.docname, 0, bodyBytes)
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
			var bodyOut map[string]interface{}
			rawDoc, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, testcase.docname, []string{base.SyncXattrName})
			assert.NoError(t, err)

			require.Contains(t, xattrs, base.SyncXattrName)
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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled and in integration mode")
	}

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
	assert.Equal(t, base.ErrEmptyDocument, err)
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
	assert.Equal(t, base.ErrEmptyDocument, err)
	assert.True(t, importedDoc == nil, "Expected no imported doc")
}

func assertXattrSyncMetaRevGeneration(t *testing.T, dataStore base.DataStore, key string, expectedRevGeneration int) {
	_, xattrs, _, err := dataStore.GetWithXattrs(base.TestCtx(t), key, []string{base.SyncXattrName})
	require.NoError(t, err, "Error Getting Xattr")
	require.Contains(t, xattrs, base.SyncXattrName)
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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires Couchbase Server") // no cluster UUIDs in Walrus
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := "doc1"
	bodyBytes := rawDocNoMeta()

	_, err := collection.dataStore.Add(key, 0, bodyBytes)
	require.NoError(t, err)

	_, cas, err := collection.dataStore.GetRaw(key)
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
	require.Contains(t, xattrs, base.SyncXattrName)
	var xattr map[string]any
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattr))
	require.Len(t, xattr["cluster_uuid"].(string), 32)
}

// TestImporNonZeroStart makes sure docs written before sync gateway start get imported
func TestImportNonZeroStart(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)
	bucket := base.GetTestBucket(t)

	doc1 := "doc1"
	revID1 := "1-2a9efe8178aa817f4414ae976aa032d9"

	_, err := bucket.GetSingleDataStore().Add(doc1, 0, rawDocNoMeta())
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
	base.SkipImportTestsIfNotEnabled(t)
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
	_, err := bucket.GetSingleDataStore().Add(doc1, 0, []byte(`{"foo" : "bar", "_sync" : 1 }`))
	require.NoError(t, err)

	// this will be imported
	err = bucket.GetSingleDataStore().Set(doc2, 0, nil, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
}

func TestImportFeedInvalidSyncMetadata(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyMigrate)
	base.SkipImportTestsIfNotEnabled(t)
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
	_, err = bucket.GetSingleDataStore().Add(doc4, 0, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 2)

	// add new doc and update it via sdk to include _sync in body
	_, _, err = collection.Put(ctx, doc5, Body{"foo": "bar"})
	require.NoError(t, err)
	err = bucket.GetSingleDataStore().SetRaw(doc5, 0, nil, []byte(`{"foo" : "bar", "_sync":"somedata"}`))
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
	base.SkipImportTestsIfNotEnabled(t)

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
	_, err = collection.dataStore.Add(doc3ID, 0, []byte(`{"some": "data", "_sync": {}}`))
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
	base.SkipImportTestsIfNotEnabled(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	db, ctx := setupTestDBWithOptionsAndImport(t, bucket, DatabaseContextOptions{})
	defer db.Close(ctx)

	doc1ID := t.Name() + "_doc1"
	doc2ID := t.Name() + "_doc2"

	collection, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a docs with invalid sync data
	_, err := collection.dataStore.Add(doc1ID, 0, []byte(`{"some": "data", "_sync": {}}`))
	require.NoError(t, err)
	_, err = collection.dataStore.Add(doc2ID, 0, []byte(`{"some": "data", "_sync": {"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"history": {},"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 2)
}

func TestImportFeedNonJSONNewDoc(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyImport)
	base.SkipImportTestsIfNotEnabled(t)
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
	_, err := bucket.GetSingleDataStore().Add(doc1, 0, []byte(`1`))
	require.NoError(t, err)

	_, err = bucket.GetSingleDataStore().Add(doc2, 0, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
}

func TestImportFeedNonJSONExistingDoc(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyMigrate, base.KeyImport)
	base.SkipImportTestsIfNotEnabled(t)
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

	_, err := bucket.GetSingleDataStore().Add(doc1, 0, []byte(`{"foo": "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)

	// logs and increments ImportErrorCount
	//     [INF] .. col:sg_test_0 Unmarshal error during importDoc json: cannot unmarshal number into Go value of type db.Body
	err = bucket.GetSingleDataStore().Set(doc1, 0, nil, []byte(`1`))
	require.NoError(t, err)

	_, err = bucket.GetSingleDataStore().Add(doc2, 0, []byte(`{"foo" : "bar"}`))
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 2)
	require.Equal(t, int64(1), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
}

func TestMetadataOnlyUpdate(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	if !db.Bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		t.Skip("Test requires multi-xattr subdoc operations, CBS 7.6 or higher")
	}

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
	writeCas, err := collection.dataStore.WriteCas("sdkWrite", 0, 0, bodyBytes, 0)
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
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs because it relies on import")
	}

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
	err = collection.dataStore.Set(docID, 0, nil, []byte(`{"foo": "baz"}`))
	require.NoError(t, err)
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	syncData, mou, _ = getSyncAndMou(t, collection, docID)
	if db.Bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		require.NotNil(t, mou)
	} else {
		require.Nil(t, mou)
	}
	require.NotNil(t, syncData)

	// Delete via SDK, the mou will be updated by the import process
	require.NoError(t, collection.dataStore.Delete(docID))
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 2)
	syncData, mou, _ = getSyncAndMou(t, collection, docID)
	if db.Bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		require.NotNil(t, mou)
	} else {
		require.Nil(t, mou)
	}
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
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs because it relies on import")
	}

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
	err = collection.dataStore.Delete(docID)
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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport, base.KeyCRUD)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	// create a sequence much higher than _syc:seqs value
	const corruptSequence = MaxSequencesToRelease + 1000

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := t.Name()
	bodyBytes := []byte(`{"foo":"bar"}`)
	// Create via the SDK
	_, err := collection.dataStore.AddRaw(key, 0, bodyBytes)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)

	_, xattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
	require.NoError(t, err)

	// corrupt the document sequence
	var newSyncData map[string]interface{}
	err = json.Unmarshal(xattrs[base.SyncXattrName], &newSyncData)
	require.NoError(t, err)
	newSyncData["sequence"] = corruptSequence
	_, err = collection.dataStore.UpdateXattrs(ctx, key, 0, cas, map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, newSyncData)}, DefaultMutateInOpts())
	require.NoError(t, err)

	// sdk update to trigger import
	require.NoError(t, collection.dataStore.SetRaw(key, 0, nil, []byte(`{"foo":"baz"}`)))

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportErrorCount.Value()
	}, 1)
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().CorruptSequenceCount.Value()
	}, 1)
}

func TestImportCancelOnDocWithCorruptSequenceOndemand(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

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
	var newSyncData map[string]interface{}
	err = json.Unmarshal(xattrs[base.SyncXattrName], &newSyncData)
	require.NoError(t, err)
	newSyncData["sequence"] = corruptSequence
	_, err = collection.dataStore.UpdateXattrs(ctx, key, 0, cas, map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, newSyncData)}, DefaultMutateInOpts())
	require.NoError(t, err)

	// sdk update
	require.NoError(t, collection.dataStore.SetRaw(key, 0, nil, []byte(`{"foo":"baz"}`)))

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
