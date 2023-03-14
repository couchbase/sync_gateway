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
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

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
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	bucketName := tb.GetName()
	db1Name := "db"
	configGroup1 := "cg1"

	var dbConfig1 *DatabaseConfig

	_, err = bootstrapContext.GetConfig(bucketName, configGroup1, db1Name, dbConfig1)
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
		tb.Close()
	}()
	bucketName := tb.GetName()

	registry, err := bootstrapContext.getGatewayRegistry(ctx, bucketName)

	dbName := "dbName"
	standardMetadataID := dbName

	defaultVersion := "1-abc"
	defaultDbConfig := makeDbConfig(tb.GetName(), dbName, nil)

	// No sync data in default collection, so should use standard ID
	metadataID := bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

	// Set _sync:seq in default collection, verify computeMetadataID returns default ID
	defaultStore := tb.Bucket.DefaultDataStore()
	syncSeqKey := base.DefaultMetadataKeys.SyncSeqKey()
	_, err = defaultStore.Incr(syncSeqKey, 1, 0, 0)
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
	defaultAndNamedScopesConfig := ScopesConfig{base.DefaultScope: ScopeConfig{map[string]CollectionConfig{base.DefaultCollection: {}, "collection1": {}}}}
	defaultDbConfig.Scopes = defaultAndNamedScopesConfig
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, defaultMetadataID, metadataID)

	// Single, non-default collection should use standard metadata ID
	namedOnlyScopesConfig := ScopesConfig{base.DefaultScope: ScopeConfig{map[string]CollectionConfig{"collection1": {}}}}
	defaultDbConfig.Scopes = namedOnlyScopesConfig
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

	// Write syncInfo to default collection, indicating that default collection is already associated with a different database
	docBody := []byte(`{"foo":"bar"}`)
	err = defaultStore.Set(base.SGSyncInfo, 0, nil, docBody)
	defaultDbConfig.Scopes = nil
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

}

func TestLongMetadataID(t *testing.T) {

	bootstrapContext := bootstrapContext{}

	shortMetadataID := bootstrapContext.standardMetadataID("dbName")
	assert.Equal(t, "dbName", shortMetadataID)

	maxLengthNonHashedDbName := "longDbName012345678901234567890123456789012"
	nonHashedMetadataID := bootstrapContext.standardMetadataID(maxLengthNonHashedDbName)
	assert.Equal(t, maxLengthNonHashedDbName, nonHashedMetadataID)

	longMetadataID := bootstrapContext.standardMetadataID("longDbName0123456789012345678901234567890123")
	assert.Equal(t, "6g2n4W2aWmQLvZIH7JXbv4V0klGFAEvZJ68gTVrgW7A=", longMetadataID)

	// Ensure no collision on hashed IDs (i.e. attempting to set a db name to a base64 hash will trigger rehashing)
	rehashMetadataID := bootstrapContext.standardMetadataID(longMetadataID)
	assert.NotEqual(t, rehashMetadataID, longMetadataID)

}

func TestMetadataStorage(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{})

	require.Equal(t, "db", rt.GetDatabase().Name)
	docName := "_sync:user:db:alice"
	if rt.GetDatabase().OnlyDefaultCollection() {
		docName = "_sync:user:alice"
	}
	metadataStore := rt.Bucket().DefaultDataStore()
	_, _, err := metadataStore.GetRaw(docName)
	require.NoError(t, err)
}
