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
	"math/rand"
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

func TestDCPKeyFilter(t *testing.T) {

	testCases := []struct {
		name     string
		metaKeys *MetadataKeys
	}{
		{"default meta keys", DefaultMetadataKeys},
		{"default meta keys from empty metaID", NewMetadataKeys("")},
		{"db specific meta keys", NewMetadataKeys("dbname")},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.name), func(t *testing.T) {

			assert.True(t, dcpKeyFilter([]byte("doc123"), tc.metaKeys))
			assert.True(t, dcpKeyFilter([]byte(tc.metaKeys.UnusedSeqKey(1234)), tc.metaKeys))
			assert.True(t, dcpKeyFilter([]byte(tc.metaKeys.UserKey("user1")), tc.metaKeys))
			assert.True(t, dcpKeyFilter([]byte(tc.metaKeys.UserKey("role2")), tc.metaKeys))
			assert.True(t, dcpKeyFilter([]byte(tc.metaKeys.SGCfgPrefix("")+"123"), tc.metaKeys))
			assert.True(t, dcpKeyFilter([]byte(tc.metaKeys.SGCfgPrefix("group")+"123"), tc.metaKeys))

			assert.False(t, dcpKeyFilter([]byte(SyncDocPrefix+"unusualSeq"), tc.metaKeys))
			assert.False(t, dcpKeyFilter([]byte(SyncFunctionKeyWithoutGroupID), tc.metaKeys))
			assert.False(t, dcpKeyFilter([]byte(DCPCheckpointRootPrefix+"12"), tc.metaKeys))
			assert.False(t, dcpKeyFilter([]byte(TxnPrefix+"atrData"), tc.metaKeys))
			assert.False(t, dcpKeyFilter([]byte(tc.metaKeys.DCPCheckpointPrefix("")+"12"), tc.metaKeys))
			assert.False(t, dcpKeyFilter([]byte(tc.metaKeys.DCPCheckpointPrefix("group")+"12"), tc.metaKeys))
			assert.False(t, dcpKeyFilter([]byte(tc.metaKeys.SyncSeqKey()), tc.metaKeys))
		})
	}

}

func TestCBGTIndexCreation(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}

	shortDbName := "testDB"
	longDbName := "testDB" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789"

	for _, tc := range []struct {
		name                 string
		dbName               string
		existingLegacyIndex  bool
		existingCurrentIndex bool
		expectedIndexName    string
	}{
		{
			name:                 "nonUpgradeFirstRun",
			dbName:               shortDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: false,
			expectedIndexName:    GenerateIndexName(shortDbName),
		},
		{
			name:                 "nonUpgradeRestart",
			dbName:               shortDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: true,
			expectedIndexName:    GenerateIndexName(shortDbName),
		},
		{
			name:                 "nonUpgradeUnsafeName",
			dbName:               longDbName,
			existingLegacyIndex:  false,
			existingCurrentIndex: false,
			expectedIndexName:    GenerateIndexName(longDbName),
		},
		{
			name:                 "upgradeFromSafeLegacy",
			dbName:               shortDbName,
			existingLegacyIndex:  true,
			existingCurrentIndex: false,
			expectedIndexName:    GenerateLegacyIndexName(shortDbName),
		},
		{
			name:                 "upgradeFromUnsafeLegacy",
			dbName:               longDbName,
			existingLegacyIndex:  true,
			existingCurrentIndex: false,
			expectedIndexName:    GenerateIndexName(longDbName),
		},
		{
			name:                 "upgradeFromSafeDualIndex",
			dbName:               shortDbName,
			existingLegacyIndex:  true,
			existingCurrentIndex: true,
			expectedIndexName:    GenerateIndexName(shortDbName),
		},
		{
			name:                 "upgradeFromUnsafeDualIndex",
			dbName:               longDbName,
			existingLegacyIndex:  true,
			existingCurrentIndex: true,
			expectedIndexName:    GenerateIndexName(longDbName),
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

			// Create existing index in legacy format
			if tc.existingLegacyIndex {
				// initCBGTManager will set up the manager with a couchbase:// connection string,
				// while go-couchbase expects a http:// string, causing test setup to fail.
				t.Skip("TODO: can't create indexes of type cbgt.SOURCE_GOCOUCHBASE")
				// Define a CBGT index with legacy naming
				bucketUUID, _ := bucket.UUID()
				sourceParams, err := legacyFeedParams(spec)
				require.NoError(t, err)
				legacyIndexName := GenerateLegacyIndexName(tc.dbName)
				indexParams := `{"name": "` + tc.dbName + `"}`
				planParams := cbgt.PlanParams{
					MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
					NumReplicas:            0,  // No replicas required for SG sharded feed
				}

				err = context.Manager.CreateIndex(
					cbgt.SOURCE_GOCOUCHBASE, // sourceType
					bucket.GetName(),        // sourceName
					bucketUUID,              // sourceUUID
					sourceParams,            // sourceParams
					indexType,               // indexType
					legacyIndexName,         // indexName
					indexParams,             // indexParams
					planParams,              // planParams
					"",                      // prevIndexUUID
				)
				require.NoError(t, err, "Unable to create legacy-style index")
			}

			if tc.existingCurrentIndex {
				// Define an existing CBGT index with current naming
				bucketUUID, _ := bucket.UUID()
				sourceParams, err := cbgtFeedParams(ctx, "", nil, tc.dbName)
				require.NoError(t, err)
				legacyIndexName := GenerateIndexName(tc.dbName)
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

			// Create cbgt index via SG handling
			err = createCBGTIndex(ctx, context, tc.dbName, configGroup, bucket, "", nil, 16)
			require.NoError(t, err)

			// Verify single index exists, and matches expected naming
			_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			assert.Len(t, indexDefsMap, 1)
			indexDef, ok := indexDefsMap[tc.expectedIndexName]
			assert.True(t, ok, "Expected index name"+tc.expectedIndexName+"not found")

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
	sourceParams, err := cbgtFeedParams(ctx, "", nil, testDbName)
	require.NoError(t, err)
	legacyIndexName := GenerateLegacyIndexName(testDbName)
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

	// Create cbgt index
	err = createCBGTIndex(ctx, context, testDbName, configGroup, bucket, "", nil, 16)
	require.NoError(t, err)

	// Verify single index created
	_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Len(t, indexDefsMap, 1)

	// Attempt to recreate index
	err = createCBGTIndex(ctx, context, testDbName, configGroup, bucket, "", nil, 16)
	require.NoError(t, err)

	// Verify single index defined (acts as upsert to existing)
	_, indexDefsMap, err = context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Len(t, indexDefsMap, 1)
	_, ok := indexDefsMap[legacyIndexName]
	assert.True(t, ok)
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
	sourceParams, err := cbgtFeedParams(ctx, "", nil, unsafeTestDBName)
	require.NoError(t, err)
	legacyIndexName := GenerateLegacyIndexName(unsafeTestDBName)
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

	// Create cbgt index
	err = createCBGTIndex(ctx, context, unsafeTestDBName, configGroup, bucket, "", nil, 16)
	require.NoError(t, err)

	// Verify single index created
	_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Len(t, indexDefsMap, 1)

	// Attempt to recreate index
	err = createCBGTIndex(ctx, context, unsafeTestDBName, configGroup, bucket, "", nil, 16)
	require.NoError(t, err)

	// Verify single index defined (acts as upsert to existing)
	_, indexDefsMap, err = context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Len(t, indexDefsMap, 1)
	_, ok := indexDefsMap[legacyIndexName]
	assert.False(t, ok)
	_, ok = indexDefsMap[GenerateIndexName(unsafeTestDBName)]
	assert.True(t, ok)
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

	// Use a bucket-backed cfg
	cfg, err := NewCfgSG(ctx, dataStore, "")
	require.NoError(t, err)

	// Define index type for db name
	configGroup := "configGroup" + t.Name()
	indexType := CBGTIndexTypeSyncGatewayImport + configGroup
	cbgt.RegisterPIndexImplType(indexType,
		&cbgt.PIndexImplType{})

	terminator := make(chan struct{})

	// Note: Would need to increase partition count if increasing test concurrency beyond 16
	managerCount := 10

	var managerWg sync.WaitGroup
	managerWg.Add(managerCount)

	for i := 0; i < managerCount; i++ {
		go func(i int, terminatorChan chan struct{}) {
			// random sleep to hit race conditions that depend on initial creation
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

			ctx := TestCtx(t)
			managerUUID := fmt.Sprintf("%s%d", t.Name(), i)
			context, err := initCBGTManager(ctx, bucket, spec, cfg, managerUUID, testDBName)
			assert.NoError(t, err)

			// StartManager starts the manager and creates the index
			log.Printf("Starting manager for %s", managerUUID)
			startErr := context.StartManager(ctx, testDBName, configGroup, bucket, "", nil, DefaultImportPartitions)
			assert.NoError(t, startErr)
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

// legacyFeedParams format with credentials included
func legacyFeedParams(spec BucketSpec) (string, error) {
	feedParams := cbgt.NewDCPFeedParams()

	// check for basic auth
	if spec.Certpath == "" && spec.Auth != nil {
		username, password, _ := spec.Auth.GetCredentials()
		feedParams.AuthUser = username
		feedParams.AuthPassword = password
	}

	if spec.UseXattrs {
		feedParams.IncludeXAttrs = true
	}

	paramBytes, err := JSONMarshal(feedParams)
	if err != nil {
		return "", err
	}
	return string(paramBytes), nil
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
	defer cbgtContext.Stop()
	require.Contains(t, cbgtContext.Manager.Server(), "kv_pool_size=1")
}
