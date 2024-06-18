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
	"fmt"
	"log"
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

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollectionWithUser(t, db)

	key := t.Name()
	bodyBytes := []byte(`{"foo":"bar"}`)
	body := Body{}
	err := body.Unmarshal(bodyBytes)
	assert.NoError(t, err, "Error unmarshalling body")

	initialImportCount := db.DbStats.SharedBucketImport().ImportCount.Value()

	// Create via the SDK
	writeCas, err := collection.dataStore.WriteCas(key, 0, 0, bodyBytes, 0)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, initialImportCount+1)

	// fetch the xattrs directly doc to confirm import (to avoid triggering on-demand import)
	var syncData SyncData
	xattrs, importCas, err := collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName})
	require.NoError(t, err)
	syncXattr, ok := xattrs[base.SyncXattrName]
	require.True(t, ok)
	require.NoError(t, base.JSONUnmarshal(syncXattr, &syncData))
	require.NotZero(t, syncData.Sequence, "Sequence should not be zero for imported doc")

	// verify mou
	xattrs, _, err = collection.dataStore.GetXattrs(ctx, key, []string{base.MouXattrName})
	if db.UseMou() {
		var mou *MetadataOnlyUpdate
		require.NoError(t, err)
		mouXattr, mouOk := xattrs[base.MouXattrName]
		require.True(t, mouOk)
		require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
		require.Equal(t, base.CasToString(writeCas), mou.PreviousCAS)
		require.Equal(t, base.CasToString(importCas), mou.CAS)
	} else {
		// Expect not found fetching mou xattr
		require.Error(t, err)
	}
}

// TestOnDemandImportMou ensures that _mou is written correctly during an on-demand import
func TestOnDemandImportMou(t *testing.T) {
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
		collection := GetSingleDatabaseCollectionWithUser(t, db)
		writeCas, err := collection.dataStore.WriteCas(getKey, 0, 0, bodyBytes, 0)
		require.NoError(t, err)

		// fetch the document to trigger on-demand import
		doc, err := collection.GetDocument(ctx, getKey, DocUnmarshalAll)
		require.NoError(t, err)

		if db.UseMou() {
			require.NotNil(t, doc.metadataOnlyUpdate)
			require.Equal(t, base.CasToString(writeCas), doc.metadataOnlyUpdate.PreviousCAS)
			require.Equal(t, base.CasToString(doc.Cas), doc.metadataOnlyUpdate.CAS)
		} else {
			require.Nil(t, doc.metadataOnlyUpdate)
		}
	})

	// On-demand write
	// Create via the SDK
	t.Run("on-demand write", func(t *testing.T) {
		writeKey := baseKey + "write"
		bodyBytes := []byte(`{"foo":"bar"}`)
		body := Body{}
		err := body.Unmarshal(bodyBytes)
		assert.NoError(t, err, "Error unmarshalling body")
		collection := GetSingleDatabaseCollectionWithUser(t, db)
		writeCas, err := collection.dataStore.WriteCas(writeKey, 0, 0, bodyBytes, 0)
		require.NoError(t, err)

		// Update the document to trigger on-demand import.  Write will be a conflict, but import should be performed
		_, doc, err := collection.Put(ctx, writeKey, Body{"foo": "baz"})
		require.Nil(t, doc)
		assertHTTPError(t, err, 409)

		// fetch the mou xattr directly doc to confirm import (to avoid triggering on-demand get import)
		// verify mou
		xattrs, importCas, err := collection.dataStore.GetXattrs(ctx, writeKey, []string{base.MouXattrName})
		if db.UseMou() {
			require.NoError(t, err)
			mouXattr, mouOk := xattrs[base.MouXattrName]
			var mou *MetadataOnlyUpdate
			require.True(t, mouOk)
			require.NoError(t, base.JSONUnmarshal(mouXattr, &mou))
			require.Equal(t, base.CasToString(writeCas), mou.PreviousCAS)
			require.Equal(t, base.CasToString(importCas), mou.CAS)
		} else {
			// expect not found fetching mou xattr
			require.Error(t, err)
		}
	})

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

	collection := GetSingleDatabaseCollectionWithUser(t, db)

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
	_, existingBucketDoc, err := collection.GetDocWithXattr(ctx, key, DocUnmarshalAll)
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
	_, _, err = collection.migrateMetadata(
		ctx,
		key,
		body,
		existingBucketDoc,
		&sgbucket.MutateInOptions{PreserveExpiry: false},
	)
	assert.True(t, err != nil)
	assert.True(t, err == base.ErrCasFailureShouldRetry)

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

	collection := GetSingleDatabaseCollectionWithUser(t, db)

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
			_, existingBucketDoc, err := collection.GetDocWithXattr(ctx, key, DocUnmarshalAll)
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
			_, errImportDoc := collection.importDoc(ctx, key, body, &expiry, false, existingBucketDoc, ImportOnDemand)
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

			collection := GetSingleDatabaseCollectionWithUser(t, db)
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

			collection := GetSingleDatabaseCollectionWithUser(t, db)

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

			collection := GetSingleDatabaseCollectionWithUser(t, db)

			bodyBytes := rawDocWithSyncMeta()
			body := Body{}
			err := body.Unmarshal(bodyBytes)
			assert.NoError(t, err, "Error unmarshalling body")

			// Put a doc with inline sync data via sdk
			_, err = collection.dataStore.Add(testcase.docname, 0, bodyBytes)
			assert.NoError(t, err)

			// Get the existing bucket doc
			_, existingBucketDoc, err = collection.GetDocWithXattr(ctx, testcase.docname, DocUnmarshalAll)
			assert.NoError(t, err, fmt.Sprintf("Error retrieving doc w/ xattr: %v", err))

			importD := `{"new":"Val"}`
			bodyD := Body{}
			err = bodyD.Unmarshal([]byte(importD))
			assert.NoError(t, err, "Error unmarshalling body")

			runOnce = true

			// Trigger import
			_, err = collection.importDoc(ctx, testcase.docname, bodyD, nil, false, existingBucketDoc, ImportOnDemand)
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

	collection := GetSingleDatabaseCollectionWithUser(t, db)
	key := "TestImportNullDoc"
	var body Body
	rawNull := []byte("null")
	existingDoc := &sgbucket.BucketDocument{Body: rawNull, Cas: 1}

	// Import a null document
	importedDoc, err := collection.importDoc(ctx, key+"1", body, nil, false, existingDoc, ImportOnDemand)
	assert.Equal(t, base.ErrEmptyDocument, err)
	assert.True(t, importedDoc == nil, "Expected no imported doc")
}

func TestImportNullDocRaw(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollectionWithUser(t, db)

	// Feed import of null doc
	exp := uint32(0)
	xattrs := map[string][]byte{
		base.SyncXattrName: []byte("{}"),
	}
	importedDoc, err := collection.ImportDocRaw(ctx, "TestImportNullDoc", []byte("null"), xattrs, false, 1, &exp, ImportFromFeed)
	assert.Equal(t, base.ErrEmptyDocument, err)
	assert.True(t, importedDoc == nil, "Expected no imported doc")
}

func assertXattrSyncMetaRevGeneration(t *testing.T, dataStore base.DataStore, key string, expectedRevGeneration int) {
	_, xattrs, _, err := dataStore.GetWithXattrs(base.TestCtx(t), key, []string{base.SyncXattrName})
	assert.NoError(t, err, "Error Getting Xattr")
	require.Contains(t, xattrs, base.SyncXattrName)
	var xattr map[string]any
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattr))
	revision, ok := xattr["rev"]
	assert.True(t, ok)
	generation, _ := ParseRevID(base.TestCtx(t), revision.(string))
	log.Printf("assertXattrSyncMetaRevGeneration generation: %d rev: %s", generation, revision)
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
	result, err := importFilterFunc.EvaluateFunction(base.TestCtx(t), body, false)
	assert.Error(t, err, "Unexpected token function error")
	assert.False(t, result, "Function evaluation result should be false")

	// Simulate boolean return value from import filter function
	body = Body{"key": "value", "version": "2a"}
	source = `function(doc) { if (doc.version == "2a") { return true; } else { return false; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body, false)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.True(t, result, "Import filter function should return boolean value true")

	// Simulate non-boolean return value from import filter function; default switch case
	body = Body{"key": "value", "version": "2b"}
	source = `function(doc) { if (doc.version == "2b") { return 1.01; } else { return 0.01; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body, false)
	assert.Error(t, err, "Import filter function returned non-boolean value")
	assert.False(t, result, "Import filter function evaluation result should be false")

	// Simulate string return value true from import filter function
	body = Body{"key": "value", "version": "1a"}
	source = `function(doc) { if (doc.version == "1a") { return "true"; } else { return "false"; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body, false)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.True(t, result, "Import filter function should return true")

	// Simulate string return value false from import filter function
	body = Body{"key": "value", "version": "2a"}
	source = `function(doc) { if (doc.version == "1a") { return "true"; } else { return "false"; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body, false)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.False(t, result, "Import filter function should return false")

	// Simulate strconv.ParseBool: parsing "TruE": invalid syntax
	body = Body{"key": "value", "version": "1a"}
	source = `function(doc) { if (doc.version == "1a") { return "TruE"; } else { return "FaLsE"; }}`
	importFilterFunc = NewImportFilterFunction(base.TestCtx(t), source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body, false)
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

	collection := GetSingleDatabaseCollectionWithUser(t, db)

	key := "doc1"
	bodyBytes := rawDocNoMeta()

	_, err := collection.dataStore.Add(key, 0, bodyBytes)
	require.NoError(t, err)

	_, cas, err := collection.dataStore.GetRaw(key)
	require.NoError(t, err)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyMigrate, base.KeyImport)

	body := Body{}
	err = body.Unmarshal(rawDocNoMeta())
	require.NoError(t, err)
	existingDoc := &sgbucket.BucketDocument{Body: bodyBytes, Cas: cas}

	importedDoc, err := collection.importDoc(ctx, key, body, nil, false, existingDoc, ImportOnDemand)
	require.NoError(t, err)
	if assert.NotNil(t, importedDoc) {
		require.Len(t, importedDoc.ClusterUUID, 32)
	}

	xattrs, _, err := collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName})
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

	collection := GetSingleDatabaseCollectionWithUser(t, db)
	base.RequireWaitForStat(t, func() int64 {
		return collection.collectionStats.ImportCount.Value()
	}, 1)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().DCPReceivedCount.Value()
	}, 1)

	doc, err := collection.GetDocument(base.TestCtx(t), doc1, DocUnmarshalAll)
	require.NoError(t, err)
	require.Equal(t, revID1, doc.SyncData.CurrentRev)
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

	// this document will be ignored for input with debug logging as follows:
	// 	[DBG] .. col:sg_test_0 <ud>bookstand</ud> not able to be imported. Error: Found _sync xattr ("1"), but could not unmarshal: json: cannot unmarshal number into Go value of type db.SyncData
	_, err := bucket.GetSingleDataStore().WriteWithXattrs(ctx, doc1, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`1`)}, nil, nil)
	require.NoError(t, err)

	// fix xattrs, and the document is able to be imported
	_, err = bucket.GetSingleDataStore().WriteWithXattrs(ctx, doc2, 0, 0, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{}`)}, nil, nil)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, 1)
	require.Equal(t, int64(0), db.DbStats.SharedBucketImport().ImportErrorCount.Value())
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

	collection := GetSingleDatabaseCollectionWithUser(t, db)

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
	previousRev := syncData.CurrentRev

	// verify mou contents
	require.Equal(t, base.CasToString(writeCas), mou.PreviousCAS)
	require.Equal(t, base.CasToString(importCas), mou.CAS)

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

	collection := GetSingleDatabaseCollectionWithUser(t, db)

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
