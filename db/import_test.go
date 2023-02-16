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
	_, existingBucketDoc, err := collection.GetDocWithXattr(key, DocUnmarshalAll)

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
			_, existingBucketDoc, err := collection.GetDocWithXattr(key, DocUnmarshalAll)
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
			expiry, err = collection.dataStore.GetExpiry(key)
			require.NoError(t, err, "Error calling GetExpiry()")
			updatedExpiryDuration := base.CbsExpiryToDuration(expiry)
			assert.True(t, updatedExpiryDuration > expiryDuration)
			assert.True(t, updatedExpiryDuration <= laterExpiryDuration)
		})
	}
}

func TestImportWithCasFailureUpdate(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
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
			_, err := collection.dataStore.WriteCas(key, 0, 0, cas, []byte(valStr), sgbucket.Raw)
			assert.NoError(t, err)
		}
	}

	syncDataInXattrCallback := func(key string) {
		if runOnce {
			var body map[string]interface{}
			var xattr map[string]interface{}

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
			cas, _ := collection.dataStore.GetWithXattr(key, base.SyncXattrName, "", &body, &xattr, nil)
			_, err := collection.dataStore.WriteCasWithXattr(key, base.SyncXattrName, 0, cas, nil, []byte(valStr), []byte(xattrStr))
			assert.NoError(t, err)
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
			_, existingBucketDoc, err = collection.GetDocWithXattr(testcase.docname, DocUnmarshalAll)
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
			var xattrOut map[string]interface{}

			_, err = collection.dataStore.GetWithXattr(testcase.docname, base.SyncXattrName, "", &bodyOut, &xattrOut, nil)
			assert.NoError(t, err)

			assert.Equal(t, "2-abc", xattrOut["rev"])
			assert.Equal(t, "val2", bodyOut["field2"])
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
	if !base.TestUseXattrs() || base.UnitTestUrlIsWalrus() {
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

	importedDoc, err := collection.ImportDocRaw(ctx, "TestImportNullDoc", []byte("null"), []byte("{}"), nil, false, 1, &exp, ImportFromFeed)
	assert.Equal(t, base.ErrEmptyDocument, err)
	assert.True(t, importedDoc == nil, "Expected no imported doc")
}

func assertXattrSyncMetaRevGeneration(t *testing.T, dataStore base.DataStore, key string, expectedRevGeneration int) {
	xattr := map[string]interface{}{}
	_, err := dataStore.GetWithXattr(key, base.SyncXattrName, "", nil, &xattr, nil)
	assert.NoError(t, err, "Error Getting Xattr")
	revision, ok := xattr["rev"]
	assert.True(t, ok)
	generation, _ := ParseRevID(revision.(string))
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
	importFilterFunc := NewImportFilterFunction(source, 0)
	result, err := importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.Error(t, err, "Unexpected token function error")
	assert.False(t, result, "Function evaluation result should be false")

	// Simulate boolean return value from import filter function
	body = Body{"key": "value", "version": "2a"}
	source = `function(doc) { if (doc.version == "2a") { return true; } else { return false; }}`
	importFilterFunc = NewImportFilterFunction(source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.True(t, result, "Import filter function should return boolean value true")

	// Simulate non-boolean return value from import filter function; default switch case
	body = Body{"key": "value", "version": "2b"}
	source = `function(doc) { if (doc.version == "2b") { return 1.01; } else { return 0.01; }}`
	importFilterFunc = NewImportFilterFunction(source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.Error(t, err, "Import filter function returned non-boolean value")
	assert.False(t, result, "Import filter function evaluation result should be false")

	// Simulate string return value true from import filter function
	body = Body{"key": "value", "version": "1a"}
	source = `function(doc) { if (doc.version == "1a") { return "true"; } else { return "false"; }}`
	importFilterFunc = NewImportFilterFunction(source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.True(t, result, "Import filter function should return true")

	// Simulate string return value false from import filter function
	body = Body{"key": "value", "version": "2a"}
	source = `function(doc) { if (doc.version == "1a") { return "true"; } else { return "false"; }}`
	importFilterFunc = NewImportFilterFunction(source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.NoError(t, err, "Import filter function shouldn't throw any error")
	assert.False(t, result, "Import filter function should return false")

	// Simulate strconv.ParseBool: parsing "TruE": invalid syntax
	body = Body{"key": "value", "version": "1a"}
	source = `function(doc) { if (doc.version == "1a") { return "TruE"; } else { return "FaLsE"; }}`
	importFilterFunc = NewImportFilterFunction(source, 0)
	result, err = importFilterFunc.EvaluateFunction(base.TestCtx(t), body)
	assert.Error(t, err, `strconv.ParseBool: parsing "TruE": invalid syntax`)
	assert.False(t, result, "Import filter function should return true")
}

func TestImportStampClusterUUID(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollectionWithUser(t, db)

	key := "doc1"
	bodyBytes := rawDocNoMeta()

	_, err := collection.dataStore.Add(key, 0, bodyBytes)
	require.NoError(t, err)

	body := Body{}
	err = body.Unmarshal(rawDocNoMeta())
	require.NoError(t, err)
	existingDoc := &sgbucket.BucketDocument{Body: bodyBytes}

	importedDoc, err := collection.importDoc(ctx, key, body, nil, false, existingDoc, ImportOnDemand)
	require.NoError(t, err)
	require.Equal(t, 32, len(importedDoc.ClusterUUID))

	var xattr map[string]string
	_, err = collection.dataStore.GetWithXattr(key, base.SyncXattrName, "", &body, &xattr, nil)
	require.NoError(t, err)
	require.Equal(t, 32, len(xattr["cluster_uuid"]))
}
