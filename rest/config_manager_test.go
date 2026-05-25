// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapConfig(t *testing.T) {
	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyDCP)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	bootstrapContext := sc.BootstrapContext

	// Get a test bucket for bootstrap testing
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	bucketName := tb.GetName()
	db1Name := "db"
	configGroup1 := "cg1"

	var dbConfig1 *DatabaseConfig

	_, err = bootstrapContext.GetConfig(ctx, bucketName, configGroup1, db1Name, dbConfig1)
	require.Error(t, err)
}

func TestComputeMetadataID(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}

	base.TestRequiresCollections(t)
	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	bootstrapContext := sc.BootstrapContext

	// Get a test bucket for bootstrap testing
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close(ctx)
	}()
	bucketName := tb.GetName()

	registry, err := bootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)

	dbName := "dbName"
	standardMetadataID := dbName

	defaultVersion := "1-abc"
	defaultDbConfig := makeDbConfig(tb.GetName(), dbName, nil)

	// Use defaultMetadataID if database targets the default collection
	metadataID := bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, defaultMetadataID, metadataID)

	// Set _sync:seq in default collection, verify computeMetadataID still returns default ID
	defaultStore := tb.Bucket.DefaultDataStore(ctx)
	syncSeqKey := base.DefaultMetadataKeys.SyncSeqKey()
	_, err = defaultStore.Incr(ctx, syncSeqKey, 1, 0, 0)
	require.NoError(t, err)

	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, defaultMetadataID, metadataID)

	// Add another database to the registry already using defaultMetadataID
	existingDbName := "existingDb"
	existingDbConfig := makeDbConfig(tb.GetName(), existingDbName, nil)
	existingDatabaseConfig := &DatabaseConfig{
		DbConfig:   existingDbConfig,
		Version:    defaultVersion,
		MetadataID: defaultMetadataID,
	}
	_, err = registry.upsertDatabaseConfig(ctx, t.Name(), existingDatabaseConfig)
	require.NoError(t, err)
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

	// remove duplicate from registry for remaining cases
	require.True(t, registry.removeDatabase(t.Name(), existingDbName))

	// Database that includes the default collection (where _sync:seq exists) should use default metadata ID
	defaultAndNamedScopesConfig := ScopesConfig{base.DefaultScope: ScopeConfig{map[string]*CollectionConfig{base.DefaultCollection: {}, "collection1": {}}}}
	defaultDbConfig.Scopes = defaultAndNamedScopesConfig
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, defaultMetadataID, metadataID)

	// If the database has been assigned a metadataID in another config group, that should be used
	multiConfigGroupDbName := "multiConfigGroupDb"
	existingDbConfigOtherConfigGroup := makeDbConfig(tb.GetName(), multiConfigGroupDbName, nil)
	existingDatabaseConfigOtherConfigGroup := &DatabaseConfig{
		DbConfig:   existingDbConfigOtherConfigGroup,
		Version:    defaultVersion,
		MetadataID: multiConfigGroupDbName,
	}
	_, err = registry.upsertDatabaseConfig(ctx, "differentConfigGroup", existingDatabaseConfigOtherConfigGroup)
	require.NoError(t, err)

	newDbConfig := makeDbConfig(tb.GetName(), multiConfigGroupDbName, nil)
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &newDbConfig)
	require.Equal(t, multiConfigGroupDbName, metadataID)

	// Single, non-default collection should use standard metadata ID
	namedOnlyScopesConfig := ScopesConfig{base.DefaultScope: ScopeConfig{map[string]*CollectionConfig{"collection1": {}}}}
	defaultDbConfig.Scopes = namedOnlyScopesConfig
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

	// Write syncInfo to default collection, indicating that default collection is already associated with a different database
	docBody := []byte(`{"metadataID":"foo"}`)
	err = defaultStore.Set(ctx, base.SGSyncInfo, 0, nil, docBody)
	require.NoError(t, err)
	defaultDbConfig.Scopes = nil
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

}

// TestComputeMetadataIDBinaryFormat covers the syncInfo-V1 binary read path through
// computeMetadataID, plus the corrupt-doc fallback. Existing TestComputeMetadataID covers
// the legacy JSON, no-doc, and short-circuit cases
func TestComputeMetadataIDBinaryFormat(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}
	base.TestRequiresCollections(t)

	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer sc.Close(ctx)

	bootstrapContext := sc.BootstrapContext

	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	bucketName := tb.GetName()

	registry, err := bootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)

	dbName := "dbName"
	standardMetadataID := dbName
	defaultDbConfig := makeDbConfig(bucketName, dbName, nil)
	defaultStore := tb.Bucket.DefaultDataStore(ctx)

	resetSyncInfo := func(t *testing.T) {
		err := defaultStore.Delete(ctx, base.SGSyncInfo)
		if err != nil && !base.IsDocNotFoundError(err) {
			require.NoError(t, err)
		}
	}

	t.Run("V1 binary doc with non-default metadataID returns standard ID", func(t *testing.T) {
		resetSyncInfo(t)
		payload := append([]byte{byte(base.SyncInfoTypeV1)}, []byte(`{"metadataID":"foo"}`)...)
		require.NoError(t, defaultStore.SetRaw(ctx, base.SGSyncInfo, 0, nil, payload))

		metadataID := bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
		assert.Equal(t, standardMetadataID, metadataID)
	})

	t.Run("V1 binary doc with default metadataID returns default ID", func(t *testing.T) {
		resetSyncInfo(t)
		payload := append([]byte{byte(base.SyncInfoTypeV1)}, []byte(`{"metadataID":"`+defaultMetadataID+`"}`)...)
		require.NoError(t, defaultStore.SetRaw(ctx, base.SGSyncInfo, 0, nil, payload))

		metadataID := bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
		assert.Equal(t, defaultMetadataID, metadataID)
	})

	t.Run("corrupt doc falls back to standard ID", func(t *testing.T) {
		resetSyncInfo(t)
		require.NoError(t, defaultStore.SetRaw(ctx, base.SGSyncInfo, 0, nil, append([]byte{0xff}, []byte(`{"metadataID":"foo"}`)...)))

		metadataID := bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
		assert.Equal(t, standardMetadataID, metadataID)
	})
}

func TestLongMetadataID(t *testing.T) {

	bootstrapContext := bootstrapContext{}

	shortMetadataID := bootstrapContext.standardMetadataID("dbName")
	assert.Equal(t, "dbName", shortMetadataID)

	maxLengthNonHashedDbName := "longDbName01234567890123456789012345678"
	nonHashedMetadataID := bootstrapContext.standardMetadataID(maxLengthNonHashedDbName)
	assert.Equal(t, maxLengthNonHashedDbName, nonHashedMetadataID)

	longMetadataID := bootstrapContext.standardMetadataID("longDbName012345678901234567890123456789")
	assert.Equal(t, "da551b63fcdb3c725007b0909d48c2255ad1f934", longMetadataID)

	// Ensure no collision on hashed IDs (i.e. attempting to set a db name to a base64 hash will trigger rehashing)
	rehashMetadataID := bootstrapContext.standardMetadataID(longMetadataID)
	assert.NotEqual(t, rehashMetadataID, longMetadataID)

}
