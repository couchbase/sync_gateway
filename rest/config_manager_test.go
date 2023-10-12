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
	"net/http"
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
		tb.Close(ctx)
	}()
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
	defaultAndNamedScopesConfig := ScopesConfig{base.DefaultScope: ScopeConfig{map[string]*CollectionConfig{base.DefaultCollection: {}, "collection1": {}}}}
	defaultDbConfig.Scopes = defaultAndNamedScopesConfig
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, defaultMetadataID, metadataID)

	// Single, non-default collection should use standard metadata ID
	namedOnlyScopesConfig := ScopesConfig{base.DefaultScope: ScopeConfig{map[string]*CollectionConfig{"collection1": {}}}}
	defaultDbConfig.Scopes = namedOnlyScopesConfig
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

	// Write syncInfo to default collection, indicating that default collection is already associated with a different database
	docBody := []byte(`{"foo":"bar"}`)
	err = defaultStore.Set(base.SGSyncInfo, 0, nil, docBody)
	require.NoError(t, err)
	defaultDbConfig.Scopes = nil
	metadataID = bootstrapContext.computeMetadataID(ctx, registry, &defaultDbConfig)
	assert.Equal(t, standardMetadataID, metadataID)

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

func TestVersionDowngrade(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// rely on AtLeastMinorDowngrade unit test to cover all cases, starting up a db is slow
	testCases := []struct {
		syncGatewayVersion      string
		metadataConfigVersion   string
		expectedRegistryVersion string
		name                    string
		hasError                bool
	}{
		{
			name:                    "equal versions",
			syncGatewayVersion:      "10.0.0",
			metadataConfigVersion:   "10.0.0",
			expectedRegistryVersion: "10.0.0",
			hasError:                false,
		},
		{
			name:                    "minor upgrade",
			syncGatewayVersion:      "10.1.0",
			metadataConfigVersion:   "10.0.0",
			expectedRegistryVersion: "10.1.0",
			hasError:                false,
		},
		{
			name:                    "patch upgrade",
			syncGatewayVersion:      "10.0.1",
			metadataConfigVersion:   "10.0.0",
			expectedRegistryVersion: "10.0.1",
			hasError:                false,
		},
		{
			name:                    "patch downgrade",
			syncGatewayVersion:      "10.0.1",
			metadataConfigVersion:   "10.0.2",
			expectedRegistryVersion: "10.0.2",
			hasError:                false,
		},
		{
			name:                    "minor downgrade",
			syncGatewayVersion:      "10.0.0",
			metadataConfigVersion:   "10.1.0",
			expectedRegistryVersion: "10.1.0",
			hasError:                true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			syncGatewayVersion, err := base.NewComparableVersionFromString(test.syncGatewayVersion)
			require.NoError(t, err)
			rt := NewRestTester(t, &RestTesterConfig{
				PersistentConfig:   true,
				syncGatewayVersion: syncGatewayVersion,
			})
			defer rt.Close()

			_ = rt.Bucket()
			bootstrapContext := rt.RestTesterServerContext.BootstrapContext
			registry, err := bootstrapContext.getGatewayRegistry(rt.Context(), rt.Bucket().GetName())
			require.NoError(t, err)
			require.True(t, syncGatewayVersion.Equal(&registry.SGVersion), "%+v != %+v", syncGatewayVersion, registry.SGVersion)

			metadataConfigVersion, err := base.NewComparableVersionFromString(test.metadataConfigVersion)
			registry.SGVersion = *metadataConfigVersion
			require.NoError(t, err)
			require.NoError(t, bootstrapContext.setGatewayRegistry(rt.Context(), rt.Bucket().GetName(), registry))

			config := rt.NewDbConfig()
			config.StartOffline = base.BoolPtr(true) // start offline to make test faster

			resp := rt.CreateDatabase("db1", config)
			if test.hasError {
				RequireStatus(t, resp, http.StatusInternalServerError)
				require.Contains(t, resp.Body.String(), "has metadata from")
			} else {
				RequireStatus(t, resp, http.StatusCreated)
			}
			registry, err = bootstrapContext.getGatewayRegistry(rt.Context(), rt.Bucket().GetName())
			require.NoError(t, err)

			expectedRegistryVersion, err := base.NewComparableVersionFromString(test.expectedRegistryVersion)
			require.NoError(t, err)

			require.True(t, expectedRegistryVersion.Equal(&registry.SGVersion), "%+v != %+v", expectedRegistryVersion, registry.SGVersion)

		})
	}
}
