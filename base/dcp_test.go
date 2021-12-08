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
	"strconv"
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
	assert.True(t, dcpKeyFilter([]byte("doc123")))
	assert.True(t, dcpKeyFilter([]byte(UserPrefix+"user1")))
	assert.True(t, dcpKeyFilter([]byte(RolePrefix+"role2")))
	assert.True(t, dcpKeyFilter([]byte(UnusedSeqPrefix+"1234")))
	assert.True(t, dcpKeyFilter([]byte(SGCfgPrefixWithGroupID("")+"123")))
	assert.True(t, dcpKeyFilter([]byte(SGCfgPrefixWithGroupID("group")+"123")))

	assert.False(t, dcpKeyFilter([]byte(SyncSeqKey)))
	assert.False(t, dcpKeyFilter([]byte(SyncPrefix+"unusualSeq")))
	assert.False(t, dcpKeyFilter([]byte(SyncDataKey)))
	assert.False(t, dcpKeyFilter([]byte(DCPCheckpointPrefix+"12")))
	assert.False(t, dcpKeyFilter([]byte(TxnPrefix+"atrData")))
	assert.False(t, dcpKeyFilter([]byte(DCPCheckpointPrefixWithGroupID("group")+"12")))

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
			bucket := GetTestBucket(t)
			defer bucket.Close()

			spec := bucket.BucketSpec

			// Use an in-memory cfg, set up cbgt manager
			cfg := cbgt.NewCfgMem()
			context, err := initCBGTManager(bucket, spec, cfg, "testIndexCreation", tc.dbName)
			assert.NoError(t, err)
			defer context.RemoveFeedCredentials(tc.dbName)

			// Start Manager
			registerType := cbgt.NODE_DEFS_WANTED
			err = context.Manager.Start(registerType)
			require.NoError(t, err)

			// Define index type
			configGroup := "configGroup" + t.Name()
			indexType := CBGTIndexTypeSyncGatewayImport + configGroup
			cbgt.RegisterPIndexImplType(indexType,
				&cbgt.PIndexImplType{})

			// Create existing index in legacy format
			if tc.existingLegacyIndex {
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
					"couchbase",      // sourceType
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

			if tc.existingCurrentIndex {
				// Define an existing CBGT index with current naming
				bucketUUID, _ := bucket.UUID()
				sourceParams, err := cbgtFeedParams(spec, tc.dbName)
				require.NoError(t, err)
				legacyIndexName := GenerateIndexName(tc.dbName)
				indexParams := `{"name": "` + tc.dbName + `"}`
				planParams := cbgt.PlanParams{
					MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
					NumReplicas:            0,  // No replicas required for SG sharded feed
				}

				err = context.Manager.CreateIndex(
					SOURCE_GOCOUCHBASE_DCP_SG, // sourceType
					bucket.GetName(),          // sourceName
					bucketUUID,                // sourceUUID
					sourceParams,              // sourceParams
					indexType,                 // indexType
					legacyIndexName,           // indexName
					indexParams,               // indexParams
					planParams,                // planParams
					"",                        // prevIndexUUID
				)
				require.NoError(t, err, "Unable to create legacy-style index")
			}

			// Create cbgt index via SG handling
			err = createCBGTIndex(context, tc.dbName, configGroup, bucket, spec, 16)
			require.NoError(t, err)

			// Verify single index exists, and matches expected naming
			_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
			require.NoError(t, err)
			assert.Equal(t, 1, len(indexDefsMap))
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
	bucket := GetTestBucket(t)
	defer bucket.Close()

	spec := bucket.BucketSpec
	testDbName := "testDB"

	// Use an in-memory cfg, set up cbgt manager
	cfg := cbgt.NewCfgMem()
	context, err := initCBGTManager(bucket, spec, cfg, "testIndexCreation", testDbName)
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
	sourceParams, err := cbgtFeedParams(spec, testDbName)
	require.NoError(t, err)
	legacyIndexName := GenerateLegacyIndexName(testDbName)
	indexParams := `{"name": "` + testDbName + `"}`
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
		NumReplicas:            0,  // No replicas required for SG sharded feed
	}

	err = context.Manager.CreateIndex(
		SOURCE_GOCOUCHBASE_DCP_SG, // sourceType
		bucket.GetName(),          // sourceName
		bucketUUID,                // sourceUUID
		sourceParams,              // sourceParams
		indexType,                 // indexType
		legacyIndexName,           // indexName
		indexParams,               // indexParams
		planParams,                // planParams
		"",                        // prevIndexUUID
	)
	require.NoError(t, err, "Unable to create legacy-style index")

	// Create cbgt index
	err = createCBGTIndex(context, testDbName, configGroup, bucket, spec, 16)
	require.NoError(t, err)

	// Verify single index created
	_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(indexDefsMap))

	// Attempt to recreate index
	err = createCBGTIndex(context, testDbName, configGroup, bucket, spec, 16)
	require.NoError(t, err)

	// Verify single index defined (acts as upsert to existing)
	_, indexDefsMap, err = context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(indexDefsMap))
	_, ok := indexDefsMap[legacyIndexName]
	assert.True(t, ok)
}

func TestCBGTIndexCreationUnsafeLegacyName(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	bucket := GetTestBucket(t)
	defer bucket.Close()

	spec := bucket.BucketSpec
	unsafeTestDBName := "testDB" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789" +
		"01234567890123456789012345678901234567890123456789"

	// Use an in-memory cfg, set up cbgt manager
	cfg := cbgt.NewCfgMem()
	context, err := initCBGTManager(bucket, spec, cfg, "testIndexCreation", unsafeTestDBName)
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
	sourceParams, err := cbgtFeedParams(spec, unsafeTestDBName)
	require.NoError(t, err)
	legacyIndexName := GenerateLegacyIndexName(unsafeTestDBName)
	indexParams := `{"name": "` + unsafeTestDBName + `"}`
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
		NumReplicas:            0,  // No replicas required for SG sharded feed
	}

	err = context.Manager.CreateIndex(
		SOURCE_GOCOUCHBASE_DCP_SG, // sourceType
		bucket.GetName(),          // sourceName
		bucketUUID,                // sourceUUID
		sourceParams,              // sourceParams
		indexType,                 // indexType
		legacyIndexName,           // indexName
		indexParams,               // indexParams
		planParams,                // planParams
		"",                        // prevIndexUUID
	)
	require.NoError(t, err, "Unable to create legacy-style index")

	// Create cbgt index
	err = createCBGTIndex(context, unsafeTestDBName, configGroup, bucket, spec, 16)
	require.NoError(t, err)

	// Verify single index created
	_, indexDefsMap, err := context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(indexDefsMap))

	// Attempt to recreate index
	err = createCBGTIndex(context, unsafeTestDBName, configGroup, bucket, spec, 16)
	require.NoError(t, err)

	// Verify single index defined (acts as upsert to existing)
	_, indexDefsMap, err = context.Manager.GetIndexDefs(true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(indexDefsMap))
	_, ok := indexDefsMap[legacyIndexName]
	assert.False(t, ok)
	_, ok = indexDefsMap[GenerateIndexName(unsafeTestDBName)]
	assert.True(t, ok)
}

func TestConcurrentCBGTIndexCreation(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket")
	}
	bucket := GetTestBucket(t)
	defer bucket.Close()

	spec := bucket.BucketSpec
	testDBName := "testDB"

	// Use an bucket-backed cfg
	cfg, err := NewCfgSG(bucket, "")
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

			managerUUID := fmt.Sprintf("%s%d", t.Name(), i)
			context, err := initCBGTManager(bucket, spec, cfg, managerUUID, testDBName)
			assert.NoError(t, err)

			// StartManager starts the manager and creates the index
			log.Printf("Starting manager for %s", managerUUID)
			startErr := context.StartManager(testDBName, configGroup, bucket, spec, DefaultImportPartitions)
			assert.NoError(t, startErr)

			managerWg.Done()
			// ensure all goroutines start the manager before we start closing them
			select {
			case <-terminatorChan:
				context.Manager.Stop()
			case <-time.After(20 * time.Second):
				assert.Fail(t, "manager goroutine not terminated: %v", managerUUID)
			}

		}(i, terminator)
	}
	managerWg.Wait()
	close(terminator)

}

// Compare Atoi vs map lookup for partition conversion
//    BenchmarkPartitionToVbNo/map-16         	100000000	        10.4 ns/op
//    BenchmarkPartitionToVbNo/atoi-16        	500000000	         3.85 ns/op
//    BenchmarkPartitionToVbNo/parseUint-16   	300000000	         5.04 ns/op
func BenchmarkPartitionToVbNo(b *testing.B) {

	//Initialize lookup map
	vbNos := make(map[string]uint16, 1024)
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
		vbNos[vbucketIdStrings[i]] = uint16(i)
	}

	b.Run("map", func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			value := uint16(vbNos["23"])
			if value != uint16(23) {
				b.Fail()
			}
		}
	})

	b.Run("atoi", func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			valueInt, err := strconv.Atoi("23")
			value := uint16(valueInt)
			if err != nil || value != uint16(23) {
				b.Fail()
			}
		}
	})

	b.Run("parseUint", func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			valueUint64, err := strconv.ParseUint("23", 10, 0)
			value := uint16(valueUint64)
			if err != nil || value != uint16(23) {
				b.Fail()
			}
		}
	})

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
