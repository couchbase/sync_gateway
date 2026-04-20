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
	"fmt"
	"log"
	"maps"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			context, err := initCBGTManager(ctx, bucket, spec, cfg, "testIndexCreation", tc.dbName)
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
				bucketUUID, _ := bucket.UUID()
				sourceParams, err := cbgtFeedParams(ctx, nil, tc.dbName)
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
			require.Contains(t, indexDefsMap, tc.expectedIndexName)
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
	context, err := initCBGTManager(ctx, bucket, spec, cfg, "testIndexCreation", testDbName)
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
	bucketUUID, _ := bucket.UUID()
	sourceParams, err := cbgtFeedParams(ctx, nil, testDbName)
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
	require.Contains(t, indexDefsMap, legacyIndexName)
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
	context, err := initCBGTManager(ctx, bucket, spec, cfg, "testIndexCreation", unsafeTestDBName)
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
	bucketUUID, _ := bucket.UUID()
	sourceParams, err := cbgtFeedParams(ctx, nil, unsafeTestDBName)
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
		cfg, err := NewCfgSG(ctx, dataStore, "", useNodePoller)
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
				context, err := initCBGTManager(ctx, bucket, spec, cfg, managerUUID, testDBName)
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

func TestCBGTKvPoolSize(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	spec := bucket.BucketSpec
	spec.Server += "?kv_pool_size=8"

	cfg, err := NewCbgtCfgMem()
	require.NoError(t, err)
	cbgtContext, err := initCBGTManager(ctx, bucket, spec, cfg, t.Name(), "fakeDb")
	assert.NoError(t, err)
	defer cbgtContext.Stop(ctx)
	require.Contains(t, cbgtContext.Manager.Server(), "kv_pool_size=1")
}
