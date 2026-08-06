/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// func TransformBucketCredentials(inputUsername, inputPassword, inputBucketname string) (username, password, bucketname string) {

func TestTransformBucketCredentials(t *testing.T) {

	inputUsername := "foo"
	inputPassword := "bar"
	inputBucketName := "baz"

	username, password, bucketname := TransformBucketCredentials(
		inputUsername,
		inputPassword,
		inputBucketName,
	)
	assert.Equal(t, username, inputUsername)
	assert.Equal(t, password, inputPassword)
	assert.Equal(t, bucketname, inputBucketName)

	inputUsername2 := ""
	inputPassword2 := "bar"
	inputBucketName2 := "baz"

	username2, password2, bucketname2 := TransformBucketCredentials(
		inputUsername2,
		inputPassword2,
		inputBucketName2,
	)

	assert.Equal(t, username2, inputBucketName2)
	assert.Equal(t, password2, inputPassword2)
	assert.Equal(t, bucketname2, inputBucketName2)

}

func TestDCPIsMetadataDocument(t *testing.T) {

	metadataCases := []struct {
		name     string
		metaKeys *MetadataKeys
	}{
		{"default meta keys", DefaultMetadataKeys},
		{"default meta keys from empty metaID", NewMetadataKeys("")},
		{"db specific meta keys", NewMetadataKeys("dbname")},
	}
	for _, m := range metadataCases {
		t.Run(m.name, func(t *testing.T) {
			testCases := []struct {
				docName          string
				metadataDocument bool
			}{
				{
					docName:          "doc123",
					metadataDocument: false,
				},
				{
					docName:          m.metaKeys.UnusedSeqKey(1234),
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.UserKey("user1"),
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.RoleKey("role2"),
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.SGCfgPrefix("") + "123",
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.SGCfgPrefix("group") + "123",
					metadataDocument: true,
				},
				{
					docName:          SyncDocPrefix + "unusualSeq",
					metadataDocument: true,
				},
				{
					docName:          SyncFunctionKeyWithoutGroupID,
					metadataDocument: true,
				},
				{
					docName:          DCPCheckpointRootPrefix + "12",
					metadataDocument: true,
				},
				{
					docName:          TxnPrefix + "atrData",
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.DCPCheckpointPrefix("") + "12",
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.DCPCheckpointPrefix("group") + "12",
					metadataDocument: true,
				},
				{
					docName:          m.metaKeys.SyncSeqKey(),
					metadataDocument: true,
				},
			}
			for _, tc := range testCases {
				t.Run(tc.docName, func(t *testing.T) {
					if tc.metadataDocument {
						assert.True(t, isMetadataDocumentName([]byte(tc.docName)))
					} else {
						assert.False(t, isMetadataDocumentName([]byte(tc.docName)))
					}
				})
			}
		})
	}
}

func TestCBGTIndexCreation(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}

	shortDbName := "testDB"
	shortDbImportIndexName, err := GenerateCBGTIndexName(shortDbName, ShardedDCPFeedTypeImport)
	require.NoError(t, err)
	shortDBResyncIndexName, err := GenerateCBGTIndexName(shortDbName, ShardedDCPFeedTypeResync)
	require.NoError(t, err)
	longDbName := "testDB" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789"
	longDbImportIndexName, err := GenerateCBGTIndexName(longDbName, ShardedDCPFeedTypeImport)
	require.NoError(t, err)
	longDBResyncIndexName, err := GenerateCBGTIndexName(longDbName, ShardedDCPFeedTypeResync)
	require.NoError(t, err)
	for _, tc := range []struct {
		name                 string
		dbName               string
		existingLegacyIndex  bool
		existingCurrentIndex bool
		feedID               string
		feedType             ShardedDCPFeedType
		expectedIndexName    string
	}{
		{
			name:                 "nonUpgradeFirstRun-import",
			dbName:               shortDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: false,
			expectedIndexName:    shortDbImportIndexName,
			feedType:             ShardedDCPFeedTypeImport,
		},
		{
			name:                 "nonUpgradeRestart-import",
			dbName:               shortDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: true,
			expectedIndexName:    shortDbImportIndexName,
			feedType:             ShardedDCPFeedTypeImport,
		},
		{
			name:                 "nonUpgradeUnsafeName-import",
			dbName:               longDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: false,
			expectedIndexName:    longDbImportIndexName,
			feedType:             ShardedDCPFeedTypeImport,
		},
		{
			name:                 "nonUpgradeFirstRun-resync",
			dbName:               shortDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: false,
			expectedIndexName:    shortDBResyncIndexName,
			feedType:             ShardedDCPFeedTypeResync,
		},
		{
			name:                 "nonUpgradeRestart-resync",
			dbName:               shortDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: true,
			expectedIndexName:    shortDBResyncIndexName,
			feedType:             ShardedDCPFeedTypeResync,
		},
		{
			name:                 "nonUpgradeUnsafeName-resync",
			dbName:               longDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: false,
			expectedIndexName:    longDBResyncIndexName,
			feedType:             ShardedDCPFeedTypeResync,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

			spec := bucket.BucketSpec

			// Use an in-memory cfg, set up cbgt manager
			ctx = DatabaseLogCtx(ctx, tc.dbName, nil)
			cfg, err := NewCbgtCfgMem()
			require.NoError(t, err)
			context, err := initCBGTManager(ctx, bucket, spec, cfg, "testIndexCreation", tc.dbName, nil)
			assert.NoError(t, err)
			defer context.RemoveFeedCredentials(tc.dbName)

			// Start Manager
			registerType := cbgt.NODE_DEFS_WANTED
			err = context.Manager.Start(registerType)
			require.NoError(t, err)
			defer context.Manager.Stop()

			// Define index type
			configGroup := "configGroup" + t.Name()
			indexType := CBGTIndexTypeSyncGatewayImport + configGroup
			cbgt.RegisterPIndexImplType(indexType,
				&cbgt.PIndexImplType{})

			if tc.existingCurrentIndex {
				// Define an existing CBGT index with current naming
				bucketUUID, _ := bucket.UUID(ctx)
				sourceParams, err := cbgtFeedParams(ctx, ShardedDCPOptions{DBName: tc.dbName})
				require.NoError(t, err)
				legacyIndexName, err := GenerateCBGTIndexName(tc.dbName, tc.feedType)
				require.NoError(t, err)
				indexParams := `{"name": "` + tc.dbName + `"}`
				planParams := cbgt.PlanParams{
					MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
					NumReplicas:            0,  // No replicas required for SG sharded feed
				}

				err = context.Manager.CreateIndex(
					SOURCE_DCP_SG,    // sourceType
					bucket.GetName(), // sourceName
					bucketUUID,       // sourceUUID
					sourceParams,     // sourceParams
					indexType,        // indexType
					legacyIndexName,  // indexName
					indexParams,      // indexParams
					planParams,       // planParams
					"",               // prevIndexUUID
				)
				require.NoError(t, err, "Unable to create legacy-style index")
			}

			indexName, err := GenerateCBGTIndexName(tc.dbName, tc.feedType)
			require.NoError(t, err)

			// Create cbgt index via SG handling
			err = createCBGTIndex(ctx, context, ShardedDCPOptions{
				DBName:            tc.dbName,
				Bucket:            bucket,
				NumPartitions:     16,
				IndexName:         indexName,
				PreviousIndexName: GenerateLegacyImportIndexName(tc.dbName),
				IndexType:         indexType,
			})
			require.NoError(t, err)

			// Verify single index exists, and matches expected naming
			_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			require.Contains(t, maps.Keys(indexDefsMap), tc.expectedIndexName)
			indexDef := indexDefsMap[tc.expectedIndexName]

			assert.False(t, strings.Contains(indexDef.SourceParams, "authUser"), "sourceParams should not include authUser")
			assert.False(t, strings.Contains(indexDef.SourceParams, "authPassword"), "sourceParams should not include authPassword")

		})
	}
}

func TestCBGTIndexCreationSafeLegacyName(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	spec := bucket.BucketSpec
	testDbName := "testDB"

	// Use an in-memory cfg, set up cbgt manager
	cfg, err := NewCbgtCfgMem()
	require.NoError(t, err)
	context, err := initCBGTManager(ctx, bucket, spec, cfg, "testIndexCreation", testDbName, nil)
	assert.NoError(t, err)
	defer context.RemoveFeedCredentials(testDbName)

	// Start Manager
	registerType := cbgt.NODE_DEFS_WANTED
	err = context.Manager.Start(registerType)
	require.NoError(t, err)

	// Define index type
	configGroup := "configGroup" + t.Name()
	indexType := CBGTIndexTypeSyncGatewayImport + configGroup
	cbgt.RegisterPIndexImplType(indexType,
		&cbgt.PIndexImplType{})

	// Define a CBGT index with legacy naming within safe limits
	bucketUUID, _ := bucket.UUID(ctx)
	sourceParams, err := cbgtFeedParams(ctx, ShardedDCPOptions{DBName: testDbName})
	require.NoError(t, err)
	legacyIndexName := GenerateLegacyImportIndexName(testDbName)
	indexParams := `{"name": "` + testDbName + `"}`
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
		NumReplicas:            0,  // No replicas required for SG sharded feed
	}

	err = context.Manager.CreateIndex(
		SOURCE_DCP_SG,    // sourceType
		bucket.GetName(), // sourceName
		bucketUUID,       // sourceUUID
		sourceParams,     // sourceParams
		indexType,        // indexType
		legacyIndexName,  // indexName
		indexParams,      // indexParams
		planParams,       // planParams
		"",               // prevIndexUUID
	)
	require.NoError(t, err, "Unable to create legacy-style index")

	opts := ShardedDCPOptions{
		DBName:        testDbName,
		Bucket:        bucket,
		NumPartitions: 16,
		IndexType:     indexType,
		IndexName:     legacyIndexName, // use legacy name as the primary name for this test
	}
	// Create cbgt index
	err = createCBGTIndex(ctx, context, opts)
	require.NoError(t, err)

	// Verify single index created
	_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Len(t, indexDefsMap, 1)

	// Attempt to recreate index
	err = createCBGTIndex(ctx, context, opts)
	require.NoError(t, err)

	// Verify single index defined (acts as upsert to existing)
	_, indexDefsMap, err = context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	require.Contains(t, maps.Keys(indexDefsMap), legacyIndexName)
}

func TestCBGTIndexCreationUnsafeLegacyName(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	spec := bucket.BucketSpec
	unsafeTestDBName := "testDB" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789"

	// Use an in-memory cfg, set up cbgt manager
	cfg, err := NewCbgtCfgMem()
	require.NoError(t, err)
	context, err := initCBGTManager(ctx, bucket, spec, cfg, "testIndexCreation", unsafeTestDBName, nil)
	assert.NoError(t, err)
	defer context.RemoveFeedCredentials(unsafeTestDBName)

	// Start Manager
	registerType := cbgt.NODE_DEFS_WANTED
	err = context.Manager.Start(registerType)
	require.NoError(t, err)

	// Define index type
	configGroup := "configGroup" + t.Name()
	indexType := CBGTIndexTypeSyncGatewayImport + configGroup
	cbgt.RegisterPIndexImplType(indexType,
		&cbgt.PIndexImplType{})

	// Define a CBGT index with legacy naming not within safe limits
	bucketUUID, _ := bucket.UUID(ctx)
	sourceParams, err := cbgtFeedParams(ctx, ShardedDCPOptions{DBName: unsafeTestDBName})
	require.NoError(t, err)
	legacyIndexName := GenerateLegacyImportIndexName(unsafeTestDBName)
	indexParams := `{"name": "` + unsafeTestDBName + `"}`
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
		NumReplicas:            0,  // No replicas required for SG sharded feed
	}

	err = context.Manager.CreateIndex(
		SOURCE_DCP_SG,    // sourceType
		bucket.GetName(), // sourceName
		bucketUUID,       // sourceUUID
		sourceParams,     // sourceParams
		indexType,        // indexType
		legacyIndexName,  // indexName
		indexParams,      // indexParams
		planParams,       // planParams
		"",               // prevIndexUUID
	)
	require.NoError(t, err, "Unable to create legacy-style index")

	indexName, err := GenerateCBGTIndexName(unsafeTestDBName, ShardedDCPFeedTypeImport)
	require.NoError(t, err)

	opts := ShardedDCPOptions{
		DBName:            unsafeTestDBName,
		Bucket:            bucket,
		NumPartitions:     16,
		IndexType:         indexType,
		IndexName:         indexName, // use legacy name as the primary name for this test
		PreviousIndexName: legacyIndexName,
	}
	// Create cbgt index
	err = createCBGTIndex(ctx, context, opts)
	require.NoError(t, err)

	// Verify single index created
	_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Len(t, indexDefsMap, 1)

	// Attempt to recreate index
	err = createCBGTIndex(ctx, context, opts)
	require.NoError(t, err)

	// Verify single index defined (acts as upsert to existing)
	_, indexDefsMap, err = context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	require.Equal(t, []string{indexName}, slices.Collect(maps.Keys(indexDefsMap)))
}

func TestConcurrentCBGTIndexCreation(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()

	spec := bucket.BucketSpec
	testDBName := "testDB"

	for _, feedType := range []ShardedDCPFeedType{ShardedDCPFeedTypeImport, ShardedDCPFeedTypeResync} {

		useNodePoller := false
		if feedType == ShardedDCPFeedTypeResync {
			useNodePoller = true
		}
		// Use a bucket-backed cfg
		cfg, err := newCfgSG(ctx, dataStore, "", useNodePoller, 10*time.Millisecond)
		require.NoError(t, err)

		// Define index type for db name
		configGroup := "configGroup" + t.Name()
		var indexType string
		if feedType == CBGTIndexTypeSyncGatewayImport {
			indexType = CBGTIndexTypeSyncGatewayImport + configGroup
		} else {
			indexType = CBGTIndexTypeSyncGatewayResync
		}
		cbgt.RegisterPIndexImplType(indexType,
			&cbgt.PIndexImplType{})

		terminator := make(chan struct{})

		// Note: Would need to increase partition count if increasing test concurrency beyond 16
		managerCount := 10

		var managerWg sync.WaitGroup
		managerWg.Add(managerCount)

		for i := range managerCount {
			go func(i int, terminatorChan chan struct{}) {
				// random sleep to hit race conditions that depend on initial creation
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

				ctx := TestCtx(t)
				managerUUID := fmt.Sprintf("%s%d", t.Name(), i)
				context, err := initCBGTManager(ctx, bucket, spec, cfg, managerUUID, testDBName, nil)
				assert.NoError(t, err)

				// StartManager starts the manager and creates the index
				log.Printf("Starting manager for %s", managerUUID)
				indexName, err := GenerateCBGTIndexName(testDBName, feedType)
				require.NoError(t, err)
				opts := ShardedDCPOptions{
					DBName:        testDBName,
					Bucket:        bucket,
					NumPartitions: DefaultImportPartitions,
					IndexType:     indexType,
					IndexName:     indexName,
				}
				startErr := context.StartManager(ctx, opts)
				require.NoError(t, startErr)
				managerWg.Done()

				// ensure all goroutines start the manager before we start closing them
				select {
				case <-terminatorChan:
					context.Manager.Stop()
				case <-time.After(20 * time.Second):
					require.Fail(t, fmt.Sprintf("manager goroutine not terminated: %v", managerUUID))
				}

			}(i, terminator)
		}
		managerWg.Wait()
		close(terminator)
	}
}

// TestCreateCBGTIndexIdempotent verifies createCBGTIndex doesn't rotate the index UUID when a node
// (re-)registers with an index definition that already matches what's persisted - e.g. a second
// node joining, or a node restarting. Covers both feed types, since they share this code path but
// use different Cfg wiring (see useNodePoller below).
func TestCreateCBGTIndexIdempotent(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()
	spec := bucket.BucketSpec
	testDBName := "testDB"

	for _, feedType := range []ShardedDCPFeedType{ShardedDCPFeedTypeImport, ShardedDCPFeedTypeResync} {
		t.Run(string(feedType), func(t *testing.T) {
			// Matches production wiring: import's Cfg is driven by DCP-detected doc changes
			// (database.go), resync's Cfg polls (background_mgr_resync_dcp.go).
			useNodePoller := feedType == ShardedDCPFeedTypeResync
			cfg, err := newCfgSG(ctx, dataStore, "", useNodePoller, 10*time.Millisecond)
			require.NoError(t, err)

			configGroup := "configGroup" + t.Name()
			indexType := CBGTIndexTypeSyncGatewayImport + configGroup
			cbgt.RegisterPIndexImplType(indexType, &cbgt.PIndexImplType{})

			indexName, err := GenerateCBGTIndexName(testDBName, feedType)
			require.NoError(t, err)

			opts := ShardedDCPOptions{
				DBName:        testDBName,
				Bucket:        bucket,
				NumPartitions: DefaultImportPartitions,
				IndexType:     indexType,
				IndexName:     indexName,
			}

			// First node creates the index.
			nodeA, err := initCBGTManager(ctx, bucket, spec, cfg, "nodeA-"+t.Name(), testDBName, nil)
			require.NoError(t, err)
			defer nodeA.Manager.Stop()
			require.NoError(t, nodeA.StartManager(ctx, opts))

			_, indexDefsMap, err := nodeA.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			require.Contains(t, maps.Keys(indexDefsMap), indexName)
			firstUUID := indexDefsMap[indexName].UUID
			require.NotEmpty(t, firstUUID)

			// A second node joins with an identical configuration - must not rotate the index UUID.
			nodeB, err := initCBGTManager(ctx, bucket, spec, cfg, "nodeB-"+t.Name(), testDBName, nil)
			require.NoError(t, err)
			defer nodeB.Manager.Stop()
			require.NoError(t, nodeB.StartManager(ctx, opts))

			_, indexDefsMap, err = nodeB.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			assert.Equal(t, firstUUID, indexDefsMap[indexName].UUID, "second node joining should not rotate the index UUID")

			// The original node re-registering (e.g. a restart) must also be a no-op.
			require.NoError(t, nodeA.StartManager(ctx, opts))
			_, indexDefsMap, err = nodeA.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			assert.Equal(t, firstUUID, indexDefsMap[indexName].UUID, "node re-registering should not rotate the index UUID")

			// A genuine config change (different partition count, so a different
			// MaxPartitionsPerPIndex) must NOT be skipped - the skip-check should only suppress
			// no-op updates, not mask a real change to the index definition.
			changedOpts := opts
			changedOpts.NumPartitions = opts.NumPartitions / 4
			require.NoError(t, nodeA.StartManager(ctx, changedOpts))
			_, indexDefsMap, err = nodeA.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			assert.NotEqual(t, firstUUID, indexDefsMap[indexName].UUID, "a genuine partition-count change should not be skipped")
		})
	}
}

// TestCBGTPersistsParamsVerbatim verifies that CBGT preserves SourceParams/IndexParams
// values verbatim through its config round-trip without dropping or defaulting fields.
// This is critical for cbgtIndexDefUnchanged to reliably detect index definition changes.
// Semantic equality helpers are used instead of raw string comparison because JSON key
// order is not preserved during the CBGT config round-trip.
func TestCBGTPersistsParamsVerbatim(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()
	spec := bucket.BucketSpec
	testDBName := "testDB"

	cfg, err := newCfgSG(ctx, dataStore, "", false, 10*time.Millisecond)
	require.NoError(t, err)

	configGroup := "configGroup" + t.Name()
	indexType := CBGTIndexTypeSyncGatewayImport + configGroup
	cbgt.RegisterPIndexImplType(indexType, &cbgt.PIndexImplType{})

	indexName, err := GenerateCBGTIndexName(testDBName, ShardedDCPFeedTypeImport)
	require.NoError(t, err)

	opts := ShardedDCPOptions{
		DBName:        testDBName,
		Bucket:        bucket,
		NumPartitions: DefaultImportPartitions,
		IndexType:     indexType,
		IndexName:     indexName,
		DestKey:       "testDestKey",
		Collections:   CollectionNames{"scope1": {"collectionA", "collectionB"}},
		EndSeqNos:     map[uint16]uint64{5: 100, 10: 200},
	}

	expectedSourceParams, err := cbgtFeedParams(ctx, opts)
	require.NoError(t, err)
	expectedIndexParams, err := cbgtIndexParams(opts.DestKey)
	require.NoError(t, err)

	node, err := initCBGTManager(ctx, bucket, spec, cfg, "node-"+t.Name(), testDBName, nil)
	require.NoError(t, err)
	defer node.Manager.Stop()
	require.NoError(t, node.StartManager(ctx, opts))

	_, indexDefsMap, err := node.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	require.Contains(t, maps.Keys(indexDefsMap), indexName)
	indexDef := indexDefsMap[indexName]

	// Verify semantic equality because CBGT does not preserve JSON key order during round-trip.
	assert.True(t, SGFeedSourceParamsEqual(expectedSourceParams, indexDef.SourceParams), "cbgt should preserve SourceParams field values, with no defaulting: expected %s, got %s", expectedSourceParams, indexDef.SourceParams)
	assert.True(t, SGFeedIndexParamsEqual(expectedIndexParams, indexDef.Params), "cbgt should preserve IndexParams field values, with no defaulting: expected %s, got %s", expectedIndexParams, indexDef.Params)
	firstUUID := indexDef.UUID
	require.NotEmpty(t, firstUUID)

	// Re-registering with identical parameters should be recognized as unchanged and not rotate the index UUID.
	require.NoError(t, node.StartManager(ctx, opts))
	_, indexDefsMap, err = node.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Equal(t, firstUUID, indexDefsMap[indexName].UUID, "re-registering with identical Collections/EndSeqNos should not rotate the index UUID")
}

// leakyCfg wraps a cbgt.Cfg and can be told to fail the next Get call for a given key, to
// simulate a transient error reading cbgt's persisted metadata (e.g. its index defs document).
type leakyCfg struct {
	cbgt.Cfg
	failNextGetKey atomic.Pointer[string]
}

func (c *leakyCfg) Get(key string, cas uint64) ([]byte, uint64, error) {
	if failKey := c.failNextGetKey.Load(); failKey != nil && *failKey == key {
		if c.failNextGetKey.CompareAndSwap(failKey, nil) {
			return nil, 0, errors.New("simulated transient Cfg read error")
		}
	}
	return c.Cfg.Get(key, cas)
}

func (c *leakyCfg) failNextGet(key string) {
	c.failNextGetKey.Store(&key)
}

// TestCreateCBGTIndexTransientReadErrorTolerated verifies that a transient error reading cbgt's
// persisted index defs (e.g. a brief Cfg/metadata read hiccup) doesn't abort feed startup on a
// node re-registering an index that already exists. getIndexNameAndUUID already tolerates this
// class of error for its legacy-name lookup (discarding it via `_`); this confirms the same
// tolerance holds for the lookup the "already up to date" skip-check in createCBGTIndex relies on.
func TestCreateCBGTIndexTransientReadErrorTolerated(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()
	spec := bucket.BucketSpec
	testDBName := "testDB"

	baseCfg, err := newCfgSG(ctx, dataStore, "", false, 10*time.Millisecond)
	require.NoError(t, err)
	cfg := &leakyCfg{Cfg: baseCfg}

	configGroup := "configGroup" + t.Name()
	indexType := CBGTIndexTypeSyncGatewayImport + configGroup
	cbgt.RegisterPIndexImplType(indexType, &cbgt.PIndexImplType{})

	indexName, err := GenerateCBGTIndexName(testDBName, ShardedDCPFeedTypeImport)
	require.NoError(t, err)

	opts := ShardedDCPOptions{
		DBName:        testDBName,
		Bucket:        bucket,
		NumPartitions: DefaultImportPartitions,
		IndexType:     indexType,
		IndexName:     indexName,
	}

	node, err := initCBGTManager(ctx, bucket, spec, cfg, "node-"+t.Name(), testDBName, nil)
	require.NoError(t, err)
	defer node.Manager.Stop()
	require.NoError(t, node.StartManager(ctx, opts))

	_, indexDefsMap, err := node.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	firstUUID := indexDefsMap[indexName].UUID
	require.NotEmpty(t, firstUUID)

	// Re-register (as if restarting), injecting a transient error into the very next read of
	// cbgt's persisted index defs - the read the skip-check's existingDef lookup depends on.
	cfg.failNextGet(cbgt.INDEX_DEFS_KEY)
	require.NoError(t, node.StartManager(ctx, opts), "a transient index-defs read error should not abort feed startup")

	_, indexDefsMap, err = node.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Equal(t, firstUUID, indexDefsMap[indexName].UUID, "index should be unchanged after tolerating the transient read error")
}

// TestCreateCBGTIndexUpdateRaceWithConcurrentDelete documents a known, currently-unfixed gap
// described in the comment above the skip-check in createCBGTIndex: if the index is deleted by
// another node in the window between this node capturing previousIndexUUID and calling
// Manager.CreateIndex with it, cbgt returns "index missing for update" - an error string
// StartManager's tolerated-error checks don't recognize (only "already exists" and "concurrent
// index definition update" are), so the node's database would fail to come online.
//
// Rather than racing goroutines against each other (non-deterministic), this reproduces the same
// end state deterministically: capture previousIndexUUID exactly as createCBGTIndex does, delete
// the index out from under it (simulating a concurrent second node), then drive CreateIndex with
// the now-stale UUID. If this test starts failing, either the race has been closed (update the
// comment in createCBGTIndex) or cbgt's error text has changed (update StartManager's match).
func TestCreateCBGTIndexUpdateRaceWithConcurrentDelete(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()
	spec := bucket.BucketSpec
	testDBName := "testDB"

	cfg, err := newCfgSG(ctx, dataStore, "", false, 10*time.Millisecond)
	require.NoError(t, err)

	configGroup := "configGroup" + t.Name()
	indexType := CBGTIndexTypeSyncGatewayImport + configGroup
	cbgt.RegisterPIndexImplType(indexType, &cbgt.PIndexImplType{})

	indexName, err := GenerateCBGTIndexName(testDBName, ShardedDCPFeedTypeImport)
	require.NoError(t, err)

	opts := ShardedDCPOptions{
		DBName:        testDBName,
		Bucket:        bucket,
		NumPartitions: DefaultImportPartitions,
		IndexType:     indexType,
		IndexName:     indexName,
	}

	node, err := initCBGTManager(ctx, bucket, spec, cfg, "node-"+t.Name(), testDBName, nil)
	require.NoError(t, err)
	defer node.Manager.Stop()
	require.NoError(t, node.StartManager(ctx, opts))

	// Capture previousIndexUUID and existingDef exactly as createCBGTIndex does on a node
	// re-registering.
	resolvedIndexName, previousIndexUUID, existingDef := node.getIndexNameAndUUID(ctx, indexName, GenerateLegacyImportIndexName(testDBName))
	require.Equal(t, indexName, resolvedIndexName)
	require.NotEmpty(t, previousIndexUUID)
	require.NotNil(t, existingDef)

	// Simulate a concurrent second node deleting the index in the window between that read and
	// the CreateIndex call below.
	_, err = node.Manager.DeleteIndexEx(indexName, "")
	require.NoError(t, err)

	sourceParams, err := cbgtFeedParams(ctx, opts)
	require.NoError(t, err)
	indexParams, err := cbgtIndexParams(opts.DestKey)
	require.NoError(t, err)

	err = node.Manager.CreateIndex(
		SOURCE_DCP_SG,
		node.sourceName,
		node.sourceUUID,
		sourceParams,
		opts.IndexType,
		indexName,
		indexParams,
		existingDef.PlanParams,
		previousIndexUUID,
	)
	require.Error(t, err, "expected cbgt to reject an update against a deleted index")
	assert.Contains(t, err.Error(), "index missing for update")
	assert.NotContains(t, err.Error(), "an index with the same name already exists")
	assert.NotContains(t, err.Error(), "concurrent index definition update")
}

func TestCBGTKvPoolSize(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	spec := bucket.BucketSpec
	spec.Server += "?kv_pool_size=8"

	cfg, err := NewCbgtCfgMem()
	require.NoError(t, err)
	cbgtContext, err := initCBGTManager(ctx, bucket, spec, cfg, t.Name(), "fakeDb", nil)
	assert.NoError(t, err)
	defer cbgtContext.Stop(ctx)
	require.Contains(t, cbgtContext.Manager.Server(), "kv_pool_size=1")
}

func TestCBGTManagerOptions(t *testing.T) {
	testCases := []struct {
		name            string
		server          string
		expectedOptions map[string]string
	}{
		{
			name:   "no options",
			server: "couchbase://127.0.0.1",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:     cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":         "false",
				"feedInitialBootstrapNonTLS": "false",
				"kvConnectionBufferSize":     "16384",
			},
		},
		{
			name:   "network=default",
			server: "couchbase://127.0.0.1?network=default",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:      cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":          "false",
				"feedInitialBootstrapNonTLS":  "false",
				"kvConnectionBufferSize":      "16384",
				"gocbcoreIOConfigNetworkType": "default",
			},
		},
		{
			name:   "network=external",
			server: "couchbase://127.0.0.1?network=external",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:      cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":          "false",
				"feedInitialBootstrapNonTLS":  "false",
				"kvConnectionBufferSize":      "16384",
				"gocbcoreIOConfigNetworkType": "external",
			},
		},
		{
			name:   "network=auto",
			server: "couchbase://127.0.0.1?network=auto",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:      cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":          "false",
				"feedInitialBootstrapNonTLS":  "false",
				"kvConnectionBufferSize":      "16384",
				"gocbcoreIOConfigNetworkType": "auto",
			},
		},
		{
			// kv_buffer_size never overrides the returned kvConnectionBufferSize option - the connection string is
			// passed to cbgt.NewManagerEx separately and cbgt applies it there.
			name:   "kv_buffer_size below implicit size",
			server: "couchbase://127.0.0.1?kv_buffer_size=100",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:     cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":         "false",
				"feedInitialBootstrapNonTLS": "false",
				"kvConnectionBufferSize":     "16384",
			},
		},
		{
			name:   "kv_buffer_size above implicit size",
			server: "couchbase://127.0.0.1?kv_buffer_size=20000",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:     cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":         "false",
				"feedInitialBootstrapNonTLS": "false",
				"kvConnectionBufferSize":     "16384",
			},
		},
		{
			// an unparsable kv_buffer_size is silently ignored, rather than failing the manager options build.
			name:   "kv_buffer_size not an int",
			server: "couchbase://127.0.0.1?kv_buffer_size=notanumber",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:     cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":         "false",
				"feedInitialBootstrapNonTLS": "false",
				"kvConnectionBufferSize":     "16384",
			},
		},
		{
			// multiple values for a single option is also silently ignored.
			name:   "multiple kv_buffer_size values",
			server: "couchbase://127.0.0.1?kv_buffer_size=20000&kv_buffer_size=30000",
			expectedOptions: map[string]string{
				cbgt.FeedAllotmentOption:     cbgt.FeedAllotmentOnePerPIndex,
				"managerLoadDataDir":         "false",
				"feedInitialBootstrapNonTLS": "false",
				"kvConnectionBufferSize":     "16384",
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
			options, err := cbgtManagerOptions(ctx, testCase.server)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOptions, options)
		})
	}
}
