// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutomaticConfigUpgrade(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	config := fmt.Sprintf(`{
	"server_tls_skip_verify": %t,
	"interface": ":4444",
	"adminInterface": ":4445",
	"databases": {
		"db": {
			"server": "%s",
			"username": "%s",
			"password": "%s",
			"bucket": "%s"
		}
	}
}`,
		base.TestTLSSkipVerify(),
		base.UnitTestUrl(),
		base.TestClusterUsername(),
		base.TestClusterPassword(),
		tb.GetName(),
	)

	tmpDir := t.TempDir()

	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(config), os.FileMode(0644))
	require.NoError(t, err)

	startupConfig, _, _, _, err := automaticConfigUpgrade(ctx, configPath)
	require.NoError(t, err)

	assert.Equal(t, "", startupConfig.Bootstrap.ConfigGroupID)
	assert.Equal(t, base.UnitTestUrl(), startupConfig.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), startupConfig.Bootstrap.Username)
	assert.Equal(t, base.TestClusterPassword(), startupConfig.Bootstrap.Password)
	assert.Equal(t, ":4444", startupConfig.API.PublicInterface)
	assert.Equal(t, ":4445", startupConfig.API.AdminInterface)

	writtenNewFile, err := os.ReadFile(configPath)
	require.NoError(t, err)

	var writtenFileStartupConfig StartupConfig
	err = json.Unmarshal(writtenNewFile, &writtenFileStartupConfig)
	require.NoError(t, err)

	assert.Equal(t, "", startupConfig.Bootstrap.ConfigGroupID)
	assert.Equal(t, base.UnitTestUrl(), writtenFileStartupConfig.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), writtenFileStartupConfig.Bootstrap.Username)
	assert.Equal(t, base.TestClusterPassword(), writtenFileStartupConfig.Bootstrap.Password)
	assert.Equal(t, ":4444", writtenFileStartupConfig.API.PublicInterface)
	assert.Equal(t, ":4445", writtenFileStartupConfig.API.AdminInterface)

	backupFileName := ""

	err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if strings.Contains(filepath.Base(path), "backup") {
			backupFileName = path
		}
		return nil
	})
	require.NoError(t, err)

	writtenBackupFile, err := os.ReadFile(backupFileName)
	require.NoError(t, err)

	assert.Equal(t, config, string(writtenBackupFile))

	cbs, err := CreateBootstrapConnectionFromStartupConfig(ctx, startupConfig, base.PerUseClusterConnections)
	require.NoError(t, err)

	bootstrapContext := &bootstrapContext{
		Connection: cbs,
	}

	var dbConfig DatabaseConfig
	_, err = bootstrapContext.GetConfig(ctx, tb.GetName(), PersistentConfigDefaultGroupID, "db", &dbConfig)
	require.NoError(t, err)

	assert.Equal(t, "db", dbConfig.Name)
	assert.Equal(t, tb.GetName(), *dbConfig.Bucket)
	assert.Nil(t, dbConfig.Server)
	assert.Equal(t, "", dbConfig.Username)
	assert.Equal(t, "", dbConfig.Password)
}

func TestAutomaticConfigUpgradeError(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	testCases := []struct {
		Name   string
		Config string
	}{
		{
			"Multiple DBs different servers",
			`
				{
					"server_tls_skip_verify": %t,
					"databases": {
						"db": {
							"server": "%s",
							"username": "%s",
							"password": "%s",
							"bucket": "%s"
						},
						"db2": {
							"server": "rand",
							"username": "",
							"password": "",
							"bucket": ""
						}
					}
				}`,
		},
	}

	for _, testCase := range testCases {
		// Create tempdir here to avoid slash operator in t.Name()
		tmpDir := t.TempDir()

		t.Run(testCase.Name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)

			config := fmt.Sprintf(testCase.Config, base.TestTLSSkipVerify(), base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), tb.GetName())

			configPath := filepath.Join(tmpDir, "config.json")
			err := os.WriteFile(configPath, []byte(config), os.FileMode(0644))
			require.NoError(t, err)

			_, _, _, _, err = automaticConfigUpgrade(base.TestCtx(t), configPath)
			assert.Error(t, err)
		})
	}
}

func TestUnmarshalBrokenConfig(t *testing.T) {
	t.Skip("Disabled, CBG-2420")
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()
	resp := rt.SendAdminRequest(http.MethodPut, "/newdb/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	RequireStatus(t, resp, http.StatusCreated)

	// Use underlying connection to unmarshal to untyped config
	cnf := make(map[string]interface{}, 1)
	key := PersistentConfigKey(ctx, rt.ServerContext().Config.Bootstrap.ConfigGroupID, "newdb")
	cas, err := rt.ServerContext().BootstrapContext.Connection.GetMetadataDocument(ctx, tb.GetName(), key, &cnf)
	require.NoError(t, err)

	// Add invalid json fields to the config
	cnf["num_index_replicas"] = "0"

	// Both calls to UpdateMetadataDocument and fetchAndLoadConfigs needed to enter the broken state
	_, err = rt.ServerContext().BootstrapContext.Connection.WriteMetadataDocument(ctx, tb.GetName(), key, cas, &cnf)
	require.NoError(t, err)
	_, err = rt.ServerContext().fetchAndLoadConfigs(rt.Context(), false)
	assert.NoError(t, err)

	resp = rt.SendAdminRequest(http.MethodGet, "/newdb/", "")
	RequireStatus(t, resp, http.StatusNotFound)
	resp = rt.SendAdminRequest(http.MethodDelete, "/newdb/", "")
	RequireStatus(t, resp, http.StatusOK)
}

func TestAutomaticConfigUpgradeExistingConfigAndNewGroup(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	tmpDir := t.TempDir()

	config := fmt.Sprintf(`{
	"server_tls_skip_verify": %t,
	"databases": {
		"db": {
			"server": "%s",
			"username": "%s",
			"password": "%s",
			"bucket": "%s"
		}
	}
}`,
		base.TestTLSSkipVerify(),
		base.UnitTestUrl(),
		base.TestClusterUsername(),
		base.TestClusterPassword(),
		tb.GetName(),
	)
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(config), os.FileMode(0644))
	require.NoError(t, err)

	// Run migration once
	_, _, _, _, err = automaticConfigUpgrade(ctx, configPath)
	require.NoError(t, err)

	updatedConfig := fmt.Sprintf(`{
	"server_tls_skip_verify": %t,
	"databases": {
		"db": {
			"revs_limit": 20000,
			"server": "%s",
			"username": "%s",
			"password": "%s",
			"bucket": "%s"
		}
	}
}`,
		base.TestTLSSkipVerify(),
		base.UnitTestUrl(),
		base.TestClusterUsername(),
		base.TestClusterPassword(),
		tb.GetName(),
	)
	updatedConfigPath := filepath.Join(tmpDir, "config-updated.json")
	err = os.WriteFile(updatedConfigPath, []byte(updatedConfig), os.FileMode(0644))
	require.NoError(t, err)

	// Run migration again to ensure no error and validate it doesn't actually update db
	startupConfig, _, _, _, err := automaticConfigUpgrade(ctx, updatedConfigPath)
	require.NoError(t, err)

	cbs, err := CreateBootstrapConnectionFromStartupConfig(ctx, startupConfig, base.PerUseClusterConnections)
	require.NoError(t, err)

	bootstrapContext := &bootstrapContext{
		Connection: cbs,
	}

	var dbConfig DatabaseConfig
	originalDefaultDbConfigCAS, err := bootstrapContext.GetConfig(ctx, tb.GetName(), PersistentConfigDefaultGroupID, "db", &dbConfig)
	assert.NoError(t, err)

	// Ensure that revs limit hasn't actually been set
	assert.Nil(t, dbConfig.RevsLimit)

	// Now attempt an upgrade for a non-default group ID, and ensure it's written correctly, and separately from the default group.
	const configUpgradeGroupID = "import"

	importConfig := fmt.Sprintf(`{
		"server_tls_skip_verify": %t,
		"config_upgrade_group_id": "%s",
		"databases": {
			"db": {
				"enable_shared_bucket_access": true,
				"import_docs": true,
				"server": "%s",
				"username": "%s",
				"password": "%s",
				"bucket": "%s"
			}
		}
	}`,
		base.TestTLSSkipVerify(),
		configUpgradeGroupID,
		base.UnitTestUrl(),
		base.TestClusterUsername(),
		base.TestClusterPassword(),
		tb.GetName(),
	)
	importConfigPath := filepath.Join(tmpDir, "config-import.json")
	err = os.WriteFile(importConfigPath, []byte(importConfig), os.FileMode(0644))
	require.NoError(t, err)

	startupConfig, _, _, _, err = automaticConfigUpgrade(ctx, importConfigPath)
	// only supported in EE
	if base.IsEnterpriseEdition() {
		require.NoError(t, err)

		// Ensure that startupConfig group ID has been set
		assert.Equal(t, configUpgradeGroupID, startupConfig.Bootstrap.ConfigGroupID)

		// Ensure dbConfig is saved as the specified config group ID
		var dbConfig DatabaseConfig
		_, err = bootstrapContext.GetConfig(ctx, tb.GetName(), configUpgradeGroupID, "db", &dbConfig)
		assert.NoError(t, err)

		// Ensure default has not changed
		dbConfig = DatabaseConfig{}
		defaultDbConfigCAS, err := bootstrapContext.GetConfig(ctx, tb.GetName(), PersistentConfigDefaultGroupID, "db", &dbConfig)
		assert.NoError(t, err)
		assert.Equal(t, originalDefaultDbConfigCAS, defaultDbConfigCAS)
	} else {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only supported in enterprise edition")
		assert.Nil(t, startupConfig)
	}
}

func TestImportFilterEndpoint(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t) // import tests don't work without xattrs

	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	rt.CreateDatabase("db1", rt.NewDbConfig())

	// Ensure we won't fail with an empty import filter
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/import_filter", "")
	RequireStatus(t, resp, http.StatusOK)

	// Add a document
	require.NoError(t, rt.GetSingleDataStore().Set("importDoc1", 0, nil, []byte("{}")))

	// Ensure document is imported based on default import filter
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/importDoc1", "")
	RequireStatus(t, resp, http.StatusOK)

	// Modify the import filter to always reject import
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/import_filter", `function(){return false}`)
	RequireStatus(t, resp, http.StatusOK)

	// Add a document
	require.NoError(t, rt.GetSingleDataStore().Set("importDoc2", 0, nil, []byte("{}")))

	// Ensure document is not imported and is rejected based on updated filter
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/importDoc2", "")
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "Not imported")

	resp = rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/_config/import_filter", "")
	RequireStatus(t, resp, http.StatusOK)

	// Add a document
	require.NoError(t, rt.GetSingleDataStore().Set("importDoc3", 0, nil, []byte("{}")))

	// Ensure document is imported based on default import filter
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/importDoc3", "")
	RequireStatus(t, resp, http.StatusOK)
}

func TestPersistentConfigWithCollectionConflicts(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()
	_ = rt.Bucket()

	threeCollectionScopesConfig := GetCollectionsConfig(t, rt.TestBucket, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(threeCollectionScopesConfig)

	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection3Name := dataStoreNames[2].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}}}}
	collection3ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection3Name: {}}}}
	collection1and2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}, collection2Name: {}}}}
	collection2and3ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}, collection3Name: {}}}}

	// 1. Test collection registry with db create and delete
	// Create db1, with collection1
	collection1DBConfig := rt.NewDbConfig()
	collection1DBConfig.Scopes = collection1ScopesConfig
	RequireStatus(t, rt.CreateDatabase("db1", collection1DBConfig), http.StatusCreated)

	// Verify fetch config
	RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/db1/_config", ""), http.StatusOK)

	// Create db2, with collection 2

	collection2DBConfig := rt.NewDbConfig()
	collection2DBConfig.Scopes = collection2ScopesConfig
	rt.CreateDatabase("db2", collection2DBConfig)

	// Create db1a with collection 1, expect conflict with db1
	RequireStatus(t, rt.CreateDatabase("db1a", collection1DBConfig), http.StatusConflict)

	// Delete db1
	RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/db1/", ""), http.StatusOK)

	// Create db1a with collection 1, should now succeed
	RequireStatus(t, rt.CreateDatabase("db1a", collection1DBConfig), http.StatusCreated)

	// Attempt to recreate db1, expect conflict with db1a
	RequireStatus(t, rt.CreateDatabase("db1", collection1DBConfig), http.StatusConflict)

	// 2. Test collection registry during existing db update
	// Add a new (unused) collection3 to existing database db2, should succeed
	collection2And3DbConfig := rt.NewDbConfig()
	collection2And3DbConfig.Scopes = collection2and3ScopesConfig
	RequireStatus(t, rt.UpsertDbConfig("db2", collection2And3DbConfig), http.StatusCreated)

	// Attempt to add already in use collection (collection2) to existing database db1a, should be rejected as conflict
	collection1And2DbConfig := rt.NewDbConfig()
	collection1And2DbConfig.Scopes = collection1and2ScopesConfig
	RequireStatus(t, rt.UpsertDbConfig("db1a", collection1And2DbConfig), http.StatusConflict)

	// Remove collection 2 from db2 (leaving collection 3 only)
	collection3DbConfig := rt.NewDbConfig()
	collection3DbConfig.Scopes = collection3ScopesConfig
	RequireStatus(t, rt.UpsertDbConfig("db2", collection3DbConfig), http.StatusCreated)

	// Attempt to add collection2 to existing database db1a again, should now succeed
	RequireStatus(t, rt.UpsertDbConfig("db1a", collection1And2DbConfig), http.StatusCreated)

	// 3. default collection tests
	// Add a new db targeting default scope and collection
	defaultCollectionDbConfig := rt.NewDbConfig()
	defaultCollectionDbConfig.Scopes = nil
	RequireStatus(t, rt.CreateDatabase("default1", defaultCollectionDbConfig), http.StatusCreated)

	// Add a second db targeting default scope and collection, expect conflict
	RequireStatus(t, rt.CreateDatabase("default2", defaultCollectionDbConfig), http.StatusConflict)

	// Delete default1
	RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/default1/", ""), http.StatusOK)

	// Create default2 targeting default scope and collection, should now succeed
	RequireStatus(t, rt.CreateDatabase("default2", defaultCollectionDbConfig), http.StatusCreated)
}

// TestPersistentConfigRegistryRollbackAfterDbConfigRollback simulates a vbucket rollback for the dbconfig,
// leaving the registry version ahead of the config.
func TestPersistentConfigRegistryRollbackAfterDbConfigRollback(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	oneCollectionScopesConfig := GetCollectionsConfig(t, tb, 1)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(oneCollectionScopesConfig)

	bucketName := tb.GetName()
	scopeName := dataStoreNames[0].ScopeName()
	groupID := sc.Config.Bootstrap.ConfigGroupID
	bc := sc.BootstrapContext

	// reduce retry timeout for testing
	bc.configRetryTimeout = 1 * time.Millisecond

	// set up ScopesConfigs used by tests
	collection1Name := dataStoreNames[0].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}

	const dbName = "c1_db1"
	collection1db1Config := getTestDatabaseConfig(bucketName, dbName, collection1ScopesConfig, "2-a")
	collection1db1Config.RevsLimit = base.Uint32Ptr(1000)
	cas, err := bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
	require.NoError(t, err)
	configs, err := bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 1)

	db, err := sc.GetDatabase(ctx, dbName)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), int64(db.RevsLimit))

	// simulate a rollback (not exactly - CAS increments, but lowering the config version is enough)
	docID := PersistentConfigKey(ctx, groupID, dbName)
	updatedConfig := *collection1db1Config
	updatedConfig.Version = "1-a"
	updatedConfig.RevsLimit = base.Uint32Ptr(500)
	_, err = bc.Connection.WriteMetadataDocument(ctx, bucketName, docID, cas, &updatedConfig)
	require.NoError(t, err)

	// we've not polled for config updates yet
	db, err = sc.GetDatabase(ctx, dbName)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), int64(db.RevsLimit))

	_, err = sc.fetchAndLoadConfigs(ctx, false)
	require.NoError(t, err)

	db, err = sc.GetDatabase(ctx, dbName)
	require.NoError(t, err)
	assert.Equal(t, int64(500), int64(db.RevsLimit))

	// at this point the config and registry are re-aligned, but let's just write another config update to make sure it's in an updatable state
	_, err = bc.UpdateConfig(ctx, bucketName, groupID, dbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Version = "3-c"
		bucketDbConfig.RevsLimit = base.Uint32Ptr(1234)
		return bucketDbConfig, nil
	})
	require.NoError(t, err)

	_, err = sc.fetchAndLoadConfigs(ctx, false)
	require.NoError(t, err)

	db, err = sc.GetDatabase(ctx, dbName)
	require.NoError(t, err)
	assert.Equal(t, int64(1234), int64(db.RevsLimit))
}

// TestPersistentConfigRegistryRollbackCollectionConflictAfterDbConfigRollback simulates a vbucket rollback for the dbconfig,
// leaving the registry version ahead of the config - but also with a collection conflict occurring in the subsequent rollback.
func TestPersistentConfigRegistryRollbackCollectionConflictAfterDbConfigRollback(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 3)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyConfig)

	tests := []struct {
		name                  string
		multiDatabaseRollback bool
	}{
		{"single database rollback",
			false,
		},
		{"multi database rollback",
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
			defer closeFn()

			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)

			threeCollectionScopesConfig := GetCollectionsConfig(t, tb, 3)
			dataStoreNames := GetDataStoreNamesFromScopesConfig(threeCollectionScopesConfig)

			bucketName := tb.GetName()
			scopeName := dataStoreNames[0].ScopeName()
			groupID := sc.Config.Bootstrap.ConfigGroupID
			bc := sc.BootstrapContext

			// reduce retry timeout for testing
			bc.configRetryTimeout = 1 * time.Millisecond

			// set up ScopesConfigs used by tests
			collection1Name := dataStoreNames[0].CollectionName()
			collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}
			collection2Name := dataStoreNames[1].CollectionName()
			collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}}}}
			collection3Name := dataStoreNames[2].CollectionName()
			collection3ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection3Name: {}}}}

			const dbName1 = "c1_db1"
			collection1db1Config := getTestDatabaseConfig(bucketName, dbName1, collection1ScopesConfig, "2-a")
			_, err := bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
			require.NoError(t, err)

			const dbName2 = "c1_db2"
			collection1db2Config := getTestDatabaseConfig(bucketName, dbName2, collection2ScopesConfig, "2-a")
			db2CAS, err := bc.InsertConfig(ctx, bucketName, groupID, collection1db2Config)
			require.NoError(t, err)

			const dbName3 = "c1_db3"
			collection1db3Config := getTestDatabaseConfig(bucketName, dbName3, collection3ScopesConfig, "2-a")
			db3CAS, err := bc.InsertConfig(ctx, bucketName, groupID, collection1db3Config)
			require.NoError(t, err)

			configs, err := bc.GetDatabaseConfigs(ctx, bucketName, groupID)
			require.NoError(t, err)
			require.Len(t, configs, 3)

			// simulate a rollback (not exactly - CAS increments, but lowering the config version is enough)
			// this time we'll "roll back" to a version of dbconfig that contains a collection now in use by db1
			docID := PersistentConfigKey(ctx, groupID, dbName2)
			updatedDb2Config := *collection1db2Config
			updatedDb2Config.Version = "1-a"
			updatedDb2Config.Scopes = ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}, collection2Name: {}}}}
			_, err = bc.Connection.WriteMetadataDocument(ctx, bucketName, docID, db2CAS, &updatedDb2Config)
			require.NoError(t, err)

			// also roll back db3 to the same collection as db1 if multiDatabaseRollback
			if test.multiDatabaseRollback {
				docID = PersistentConfigKey(ctx, groupID, dbName3)
				updatedDb3Config := *collection1db3Config
				updatedDb3Config.Version = "1-a"
				updatedDb3Config.Scopes = ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}, collection3Name: {}}}}
				_, err = bc.Connection.WriteMetadataDocument(ctx, bucketName, docID, db3CAS, &updatedDb3Config)
				require.NoError(t, err)
			}

			// should expect db2 (and db3) to fail because collection2 is shared by two databases during rollback handling, db1 should (re)load
			count, err := sc.fetchAndLoadConfigs(ctx, false)
			assert.NoError(t, err)
			_, err = sc.GetActiveDatabase(dbName1)
			require.NoError(t, err)
			_, err = sc.GetActiveDatabase(dbName2)
			require.Error(t, err)

			if test.multiDatabaseRollback {
				assert.Equal(t, 1, count) // db1 is still valid
				_, err = sc.GetActiveDatabase(dbName3)
				require.Error(t, err)
				sc.RequireInvalidDatabaseConfigNames(t, []string{dbName2, dbName3})
			} else {
				assert.Equal(t, 2, count) // db1 and db3 still valid
				_, err = sc.GetActiveDatabase(dbName3)
				require.NoError(t, err)
				sc.RequireInvalidDatabaseConfigNames(t, []string{dbName2})
			}

			// invalid databases not accessible, but also don't expect this to do anything like an on-demand load
			resp := BootstrapAdminRequest(t, sc, http.MethodGet, fmt.Sprintf("/%s/_config", dbName2), "")
			resp.RequireStatus(http.StatusNotFound)

			// should be able to put as a new database with a repaired config
			updatedDb2Config.Scopes = ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}}}}
			db2Config := base.MustJSONMarshal(t, updatedDb2Config.DbConfig)
			resp = BootstrapAdminRequest(t, sc, http.MethodPut, fmt.Sprintf("/%s/", dbName2), string(db2Config))
			resp.RequireStatus(http.StatusCreated)

			// database config and registry should now be aligned
			count, err = sc.fetchAndLoadConfigs(ctx, false)
			assert.NoError(t, err)
			assert.Equal(t, 0, count) // both databases valid but already loaded
			_, err = sc.GetActiveDatabase(dbName1)
			require.NoError(t, err)
			_, err = sc.GetActiveDatabase(dbName2)
			require.NoError(t, err)
			if test.multiDatabaseRollback {
				// db3 still invalid
				sc.RequireInvalidDatabaseConfigNames(t, []string{dbName3})
			} else {
				// and no remaining invalid databases
				sc.RequireInvalidDatabaseConfigNames(t, []string{})
			}
		})
	}
}

// TestPersistentConfigRegistryRollbackAfterCreateFailure simulates node failure during an insertConfig operation, leaving
// the registry updated but not the config file.  Verifies rollback and registry cleanup in the following cases:
//  1. GetDatabaseConfigs (triggers rollback)
//  2. InsertConfig for the same db name (triggers rollback, then insert succeeds)
//  3. UpdateConfig for the same db name (triggers rollback, then returns ErrNotFound for the update operation)
//  4. InsertConfig for a different db, with collection conflict with the failed create (should fail with conflict, but succeed after GetDatabaseConfigs runs)
//  5. UpdateConfig to a different db, with collection conflict with the failed create (should fail with conflict, but succeed after GetDatabaseConfigs runs)
//  6. DeleteConfig for the same db name (triggers rollback, then returns ErrNotFound for the delete operation)
func TestPersistentConfigRegistryRollbackAfterCreateFailure(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	threeCollectionScopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(threeCollectionScopesConfig)

	bucketName := tb.GetName()
	scopeName := dataStoreNames[0].ScopeName()
	groupID := sc.Config.Bootstrap.ConfigGroupID
	bc := sc.BootstrapContext

	// reduce retry timeout for testing
	bc.configRetryTimeout = 1 * time.Millisecond

	// SimulateCreateFailure updates the registry with a new config, but doesn't create the associated config file
	simulateCreateFailure := func(t *testing.T, config *DatabaseConfig) {
		registry, err := bc.getGatewayRegistry(ctx, bucketName)
		require.NoError(t, err)
		_, err = registry.upsertDatabaseConfig(ctx, groupID, config)
		require.NoError(t, err)
		require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))
	}

	// set up ScopesConfigs used by tests
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection3Name := dataStoreNames[2].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}}}}
	collection3ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection3Name: {}}}}
	collection1and2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}, collection2Name: {}}}}

	// Case 1. GetDatabaseConfigs should roll back registry after create failure
	collection1db1Config := getTestDatabaseConfig(bucketName, "c1_db1", collection1ScopesConfig, "1-a")
	simulateCreateFailure(t, collection1db1Config)
	configs, err := bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 0)

	// Case 2. InsertConfig with conflicting name should trigger registry rollback and then successful creation
	simulateCreateFailure(t, collection1db1Config)
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
	require.NoError(t, err)

	// Case 3. UpdateConfig on the database after create failure should return not found
	collection2db1Config := getTestDatabaseConfig(bucketName, "c2_db1", collection2ScopesConfig, "2-a")
	simulateCreateFailure(t, collection2db1Config)
	_, err = bc.UpdateConfig(ctx, bucketName, groupID, "c2_db1", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Version = "2-abc"
		return bucketDbConfig, nil
	})
	require.Error(t, err)
	require.True(t, err == base.ErrNotFound)

	// Case 4. InsertConfig with a conflicting collection should return error, but should succeed after next GetDatabaseConfigs
	collection3db1Config := getTestDatabaseConfig(bucketName, "c3_db1", collection3ScopesConfig, "1-a")
	simulateCreateFailure(t, collection3db1Config)
	collection3db2Config := getTestDatabaseConfig(bucketName, "c3_db2", collection3ScopesConfig, "1-b")
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection3db2Config)
	require.Error(t, err) // collection conflict

	configs, err = bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 1)

	// Reattempt insert, should now succeed
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection3db2Config)
	require.NoError(t, err)

	// Case 5. Update different db with conflicting collection after create failure
	// - create failure adding new db 'c2_db2' that has collection 2
	// - attempt to update existing database c1db1 to add collection 2
	collection2db2Config := getTestDatabaseConfig(bucketName, "c2_db2", collection2ScopesConfig, "1-a")
	simulateCreateFailure(t, collection2db2Config)

	_, err = bc.UpdateConfig(ctx, bucketName, groupID, "c1_db1", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Scopes = collection1and2ScopesConfig
		bucketDbConfig.Version = "2-a"
		return bucketDbConfig, nil
	})
	require.Error(t, err) // collection conflict

	// GetDatabaseConfigs should rollback and remove the failed c2_db2
	configs, err = bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 2)

	// Update should now succeed
	_, err = bc.UpdateConfig(ctx, bucketName, groupID, "c1_db1", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Scopes = collection1and2ScopesConfig
		bucketDbConfig.Version = "2-a"
		return bucketDbConfig, nil
	})
	require.NoError(t, err) // collection conflict

	// Remove c3 (clean up for next case)
	deleteErr := bc.DeleteConfig(ctx, bucketName, groupID, "c3_db2")
	require.NoError(t, deleteErr)

	// Case 6. Attempt to delete db after create failure for that db
	//  - create failure for c3_db1 with collection 3
	//  - attempt to delete c3_db1, rollback will remove from registry, then return 'not found' for the attempted delete
	simulateCreateFailure(t, collection3db1Config)
	deleteErr = bc.DeleteConfig(ctx, bucketName, groupID, "c3_db1")
	require.Equal(t, base.ErrNotFound, deleteErr)
}

// TestPersistentConfigRegistryRollbackAfterUpdateFailure simulates node failure during an updateConfig operation, leaving
// the registry updated but not the config file.  Database has collection 1, failed update switches to collection 2.
// Verifies rollback and registry cleanup in the following cases:
//  1. GetDatabaseConfigs (triggers rollback)
//  2. UpdateConfig for the same db name (triggers rollback, then update succeeds)
//  3. InsertConfig for a different db, with collection conflict with the new, failed update (should fail with conflict, but succeed after GetDatabaseConfigs runs)
//  4. InsertConfig for a different db, with collection conflict with the previous version (should fail with conflict, and continue to fail after GetDatabaseConfigs runs)
//  5. DeleteConfig for the same db name (triggers rollback, then successfully deletes)

func TestPersistentConfigRegistryRollbackAfterUpdateFailure(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	threeCollectionScopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(threeCollectionScopesConfig)

	bucketName := tb.GetName()
	scopeName := dataStoreNames[0].ScopeName()
	groupID := sc.Config.Bootstrap.ConfigGroupID

	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection3Name := dataStoreNames[2].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}}}}
	collection3ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection3Name: {}}}}

	bc := sc.BootstrapContext
	// reduce retry timeout for testing
	bc.configRetryTimeout = 1 * time.Millisecond

	// Create database with collection 1
	collection1db1Config := getTestDatabaseConfig(bucketName, "db1", collection1ScopesConfig, "1-a")
	_, err := bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
	require.NoError(t, err)

	// simulateUpdateFailure updates the database registry but doesn't persist the updated config. Simulates
	// node failure between registry update and config update.
	simulateUpdateFailure := func(t *testing.T, config *DatabaseConfig) {
		registry, err := bc.getGatewayRegistry(ctx, bucketName)
		require.NoError(t, err)
		_, err = registry.upsertDatabaseConfig(ctx, groupID, config)
		require.NoError(t, err)
		require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))
	}

	// Case 1. GetDatabaseConfigs should roll back registry after update failure
	collection2db1Config := getTestDatabaseConfig(bucketName, "db1", collection2ScopesConfig, "2-a")
	simulateUpdateFailure(t, collection2db1Config)
	configs, err := bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	require.Equal(t, "1-a", configs[0].Version)

	// Retrieve registry to ensure the previous version has been removed
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	registryDb, ok := registry.getRegistryDatabase(groupID, "db1")
	require.True(t, ok)
	require.Equal(t, "1-a", registryDb.Version)
	require.Nil(t, registryDb.PreviousVersion)

	// Case 2. UpdateConfig with a version that conflicts with the failed update. Should trigger registry rollback and then successful update
	simulateUpdateFailure(t, collection1db1Config)
	_, err = bc.UpdateConfig(ctx, bucketName, groupID, "db1", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Scopes = collection2ScopesConfig
		bucketDbConfig.Version = "2-b"
		return bucketDbConfig, nil
	})
	require.NoError(t, err)
	// Retrieve registry to ensure the previous version has been removed and version updated to the new version
	registry, err = bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	registryDb, ok = registry.getRegistryDatabase(groupID, "db1")
	require.True(t, ok)
	require.Equal(t, "2-b", registryDb.Version)
	require.Nil(t, registryDb.PreviousVersion)

	// Case 3. InsertConfig for a different db with collection conflict with the failed update (should fail with conflict, but succeed after GetDatabaseConfigs runs)
	collection1db1Config_v3 := getTestDatabaseConfig(bucketName, "db1", collection1ScopesConfig, "3-a")
	simulateUpdateFailure(t, collection1db1Config_v3)

	collection1db2Config := getTestDatabaseConfig(bucketName, "db2", collection1ScopesConfig, "1-a")
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection1db2Config)
	require.Error(t, err) // collection conflict

	configs, err = bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 1)

	// Reattempt insert, should now succeed
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection1db2Config)
	require.NoError(t, err)

	// Case 4. InsertConfig for a different db with collection conflict with the version prior to the failed update
	collection3db1Config := getTestDatabaseConfig(bucketName, "db1", collection3ScopesConfig, "3-a")
	simulateUpdateFailure(t, collection3db1Config)

	collection2db3Config := getTestDatabaseConfig(bucketName, "db3", collection1ScopesConfig, "1-a")
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection2db3Config)
	require.Error(t, err) // collection conflict

	configs, err = bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 2)

	// Reattempt insert, should still be in conflict post-rollback
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection2db3Config)
	require.Error(t, err) // collection conflict

	configs, err = bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 2)

	// Case 5. Attempt to delete db after update failure for that db
	simulateUpdateFailure(t, collection3db1Config)
	deleteErr := bc.DeleteConfig(ctx, bucketName, groupID, "db1")
	require.NoError(t, deleteErr)

	// Retrieve registry to ensure the delete was successful
	registry, err = bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	_, ok = registry.getRegistryDatabase(groupID, "db1")
	require.False(t, ok)
}

// TestPersistentConfigRegistryRollbackAfterDeleteFailure simulates node failure during an deleteConfig operation, leaving
// the registry updated but not the config file.
//  1. Attempt retrieval of database after delete fails, should fail with not found.
//  2. Attempt recreation of database with matching version, after delete fails. Should resolve delete and succeed
//  3. Attempt recreation of database with matching generation only, after delete fails. Should resolve delete and succeed
//  4. Attempt recreation of database with earlier version generation, after delete fails.  Should resolve delete and succeed
//  5. Attempt update of database after delete fails.  Should return "database does not exist" error
func TestPersistentConfigRegistryRollbackAfterDeleteFailure(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	threeCollectionScopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(threeCollectionScopesConfig)

	bucketName := tb.GetName()
	scopeName := dataStoreNames[0].ScopeName()
	groupID := sc.Config.Bootstrap.ConfigGroupID

	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection2Name: {}}}}

	// SimulateDeleteFailure updates the registry with a new config, but doesn't create the associated config file
	bc := sc.BootstrapContext

	// reduce retry timeout for testing
	bc.configRetryTimeout = 1 * time.Millisecond

	// Create database with collection 1
	collection1db1Config := getTestDatabaseConfig(bucketName, "db1", collection1ScopesConfig, "1-a")
	_, err := bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
	require.NoError(t, err)

	// simulateDeleteFailure removes the database from the database registry but doesn't remove the associated config file.
	// Simulates node failure between registry update and config removal.
	simulateDeleteFailure := func(t *testing.T, config *DatabaseConfig) {
		registry, err := bc.getGatewayRegistry(ctx, bucketName)
		require.NoError(t, err)
		require.NoError(t, registry.deleteDatabase(groupID, config.Name))
		require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))
	}

	// Case 1. Retrieval of database after delete failure should not find it (matching versions)
	simulateDeleteFailure(t, collection1db1Config)
	configs, err := bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 0)

	// Case 2. Attempt to recreate the config with a matching version generation and digest. Should resolve in-flight delete
	// and then successfully
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
	require.NoError(t, err)

	// Case 3. Attempt to recreate the config with a different version digest. Should resolve in-flight delete
	// and then successfully recreate
	simulateDeleteFailure(t, collection1db1Config)
	collection1db1bConfig := getTestDatabaseConfig(bucketName, "db1", collection1ScopesConfig, "1-b")
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection1db1bConfig)
	require.NoError(t, err)

	// Case 4. Attempt to recreate the config with a different version generation and digest. Should resolve in-flight delete
	// and then successfully recreate
	collection2db2Config := getTestDatabaseConfig(bucketName, "db2", collection2ScopesConfig, "1-a")
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection2db2Config)
	require.NoError(t, err)
	_, err = bc.UpdateConfig(ctx, bucketName, groupID, "db2", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Scopes = collection2ScopesConfig
		bucketDbConfig.Version = "2-a"
		return bucketDbConfig, nil
	})
	require.NoError(t, err)

	simulateDeleteFailure(t, collection2db2Config)
	// Version 2-a is deleted, attempt to recreate as version 1-b.  Expect resolution of in-flight delete and then
	// successfully recreate
	collection2db2bConfig := getTestDatabaseConfig(bucketName, "db2", collection2ScopesConfig, "1-b")
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection2db2bConfig)
	require.NoError(t, err)

	// Case 5. Attempt to update a config after delete failure.
	simulateDeleteFailure(t, collection2db2Config)
	_, err = bc.UpdateConfig(ctx, bucketName, groupID, "db2", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Scopes = collection2ScopesConfig
		bucketDbConfig.Version = "2-a"
		return bucketDbConfig, nil
	})
	require.Equal(t, base.ErrNotFound, err)

}

// TestPersistentConfigSlowCreateFailure simulates a slow insertConfig operation, where another client
// triggers rollback before the config document is updated. Verifies that the original create operation
// fails and returns an appropriate error
func TestPersistentConfigSlowCreateFailure(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	threeCollectionScopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(threeCollectionScopesConfig)

	bucketName := tb.GetName()
	scopeName := dataStoreNames[0].ScopeName()
	groupID := sc.Config.Bootstrap.ConfigGroupID
	bc := sc.BootstrapContext

	// reduce retry timeout for testing
	bc.configRetryTimeout = 1 * time.Millisecond

	// simulateSlowCreate updates the registry with a new config, but doesn't create the associated config file
	simulateSlowCreate := func(t *testing.T, config *DatabaseConfig) {
		registry, err := bc.getGatewayRegistry(ctx, bucketName)
		require.NoError(t, err)
		_, err = registry.upsertDatabaseConfig(ctx, groupID, config)
		require.NoError(t, err)
		require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))
	}

	completeSlowCreate := func(config *DatabaseConfig) error {
		_, insertError := bc.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, config.Name), config)
		return insertError
	}

	// set up ScopesConfigs used by tests
	collection1Name := dataStoreNames[0].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{map[string]*CollectionConfig{collection1Name: {}}}}

	// Case 1. Complete slow create after rollback
	collection1db1Config := getTestDatabaseConfig(bucketName, "db1", collection1ScopesConfig, "1-a")
	simulateSlowCreate(t, collection1db1Config)
	configs, err := bc.GetDatabaseConfigs(ctx, bucketName, groupID)
	require.NoError(t, err)
	require.Len(t, configs, 0)

	err = completeSlowCreate(collection1db1Config)
	require.NoError(t, err)

	// Re-attempt the insert, verify it's not blocked by the slow write of the config file
	_, err = bc.InsertConfig(ctx, bucketName, groupID, collection1db1Config)
	require.NoError(t, err)
}

func TestMigratev30PersistentConfig(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	bucketName := tb.GetName()
	groupID := sc.Config.Bootstrap.ConfigGroupID
	defaultDbName := "defaultDb"
	defaultVersion := "1-abc"
	defaultDbConfig := makeDbConfig(tb.GetName(), defaultDbName, nil)
	defaultDatabaseConfig := &DatabaseConfig{
		DbConfig: defaultDbConfig,
		Version:  defaultVersion,
	}

	_, insertError := sc.BootstrapContext.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	require.NoError(t, insertError)

	migrateErr := sc.migrateV30Configs(ctx)
	require.NoError(t, migrateErr)

	// Fetch the registry, verify database has been migrated
	registry, registryErr := sc.BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, registryErr)
	require.NotNil(t, registry)
	migratedDb, found := registry.getRegistryDatabase(groupID, defaultDbName)
	require.True(t, found)
	require.Equal(t, "1-abc", migratedDb.Version)
	// Verify legacy config has been removed
	_, getError := sc.BootstrapContext.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	base.RequireDocNotFoundError(t, getError)

	// Update the db in the registry, and recreate legacy config.  Verify migration doesn't overwrite
	_, insertError = sc.BootstrapContext.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	require.NoError(t, insertError)
	_, updateError := sc.BootstrapContext.UpdateConfig(ctx, bucketName, groupID, defaultDbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Version = "2-abc"
		return bucketDbConfig, nil
	})
	require.NoError(t, updateError)
	migrateErr = sc.migrateV30Configs(ctx)
	require.NoError(t, migrateErr)
	registry, registryErr = sc.BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, registryErr)
	require.NotNil(t, registry)
	migratedDb, found = registry.getRegistryDatabase(groupID, defaultDbName)
	require.True(t, found)
	require.Equal(t, "2-abc", migratedDb.Version)

	// Verify legacy config has been removed
	_, getError = sc.BootstrapContext.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	base.RequireDocNotFoundError(t, getError)
}

func TestMigratev30PersistentConfigUseXattrStore(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Set up test for persistent config
	config := BootstrapStartupConfigForTest(t)
	config.Unsupported.UseXattrConfig = base.BoolPtr(true)
	// "disable" config polling for this test, to avoid non-deterministic test output based on polling times
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Minute * 10)
	ctx := base.TestCtx(t)
	sc, closeFn := StartServerWithConfig(t, &config)
	defer closeFn()
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	bucketName := tb.GetName()
	groupID := sc.Config.Bootstrap.ConfigGroupID
	defaultDbName := "defaultDb"
	defaultVersion := "1-abc"
	defaultDbConfig := makeDbConfig(tb.GetName(), defaultDbName, nil)
	defaultDatabaseConfig := &DatabaseConfig{
		DbConfig: defaultDbConfig,
		Version:  defaultVersion,
	}

	_, insertError := sc.BootstrapContext.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	require.NoError(t, insertError)

	migrateErr := sc.migrateV30Configs(ctx)
	require.NoError(t, migrateErr)

	// Fetch the registry, verify database has been migrated
	registry, registryErr := sc.BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, registryErr)
	require.NotNil(t, registry)
	migratedDb, found := registry.getRegistryDatabase(groupID, defaultDbName)
	require.True(t, found)
	require.Equal(t, "1-abc", migratedDb.Version)
	// Verify legacy config has been removed
	_, getError := sc.BootstrapContext.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	base.RequireDocNotFoundError(t, getError)

	// Update the db in the registry, and recreate legacy config.  Verify migration doesn't overwrite
	_, insertError = sc.BootstrapContext.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	require.NoError(t, insertError)
	_, updateError := sc.BootstrapContext.UpdateConfig(ctx, bucketName, groupID, defaultDbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Version = "2-abc"
		return bucketDbConfig, nil
	})
	require.NoError(t, updateError)
	migrateErr = sc.migrateV30Configs(ctx)
	require.NoError(t, migrateErr)
	registry, registryErr = sc.BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, registryErr)
	require.NotNil(t, registry)
	migratedDb, found = registry.getRegistryDatabase(groupID, defaultDbName)
	require.True(t, found)
	require.Equal(t, "2-abc", migratedDb.Version)

	// Verify legacy config has been removed
	_, getError = sc.BootstrapContext.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	base.RequireDocNotFoundError(t, getError)

}

// TestMigratev30PersistentConfigCollision sets up a 3.1 database targeting the default collection, then attempts
// migration of another database in the 3.0 format (which also targets the default collection)
func TestMigratev30PersistentConfigCollision(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	bucketName := tb.GetName()
	groupID := sc.Config.Bootstrap.ConfigGroupID

	// Set up a new database targeting the default collection
	newDefaultDbName := "newDefaultDb"
	newDefaultDbConfig := getTestDatabaseConfig(bucketName, newDefaultDbName, DefaultOnlyScopesConfig, "1-a")
	_, err := sc.BootstrapContext.InsertConfig(ctx, bucketName, groupID, newDefaultDbConfig)
	require.NoError(t, err)

	// Insert a legacy db config with a different name directly to the bucket, and attempt to migrate
	defaultDbName := "defaultDb30"
	defaultVersion := "1-abc"
	defaultDbConfig := makeDbConfig(tb.GetName(), defaultDbName, nil)
	defaultDatabaseConfig := &DatabaseConfig{
		DbConfig: defaultDbConfig,
		Version:  defaultVersion,
	}
	_, insertError := sc.BootstrapContext.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	require.NoError(t, insertError)

	// migration should not return error, but legacy config will not be migrated due to collection conflict
	migrateErr := sc.migrateV30Configs(ctx)
	require.NoError(t, migrateErr)

	// Fetch the registry, verify newDefaultDb still exists and defaultDb30 has not been migrated due to collection conflict
	registry, registryErr := sc.BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, registryErr)
	require.NotNil(t, registry)
	migratedDb, found := registry.getRegistryDatabase(groupID, newDefaultDbName)
	require.True(t, found)
	require.Equal(t, "1-a", migratedDb.Version)

	// Verify non-migrated legacy config has not been deleted (since it wasn't successfully migrated)
	_, getErr := sc.BootstrapContext.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), defaultDatabaseConfig)
	require.NoError(t, getErr)
}

// TestLegacyDuplicate tests the behaviour of GetDatabaseConfigs when the same database exists in legacy and non-legacy format
func TestLegacyDuplicate(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := startBootstrapServerWithoutConfigPolling(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	bucketName := tb.GetName()
	groupID := sc.Config.Bootstrap.ConfigGroupID

	// Set up a 3.1 database targeting the default collection
	defaultDbName := "defaultDb"
	newDefaultDbConfig := getTestDatabaseConfig(bucketName, defaultDbName, DefaultOnlyScopesConfig, "3.1")
	_, err := sc.BootstrapContext.InsertConfig(ctx, bucketName, groupID, newDefaultDbConfig)
	require.NoError(t, err)

	// Insert a 3.0 db config for the same database name directly to the bucket
	legacyVersion := "3.0"
	legacyDbConfig := makeDbConfig(tb.GetName(), defaultDbName, nil)
	legacyDatabaseConfig := &DatabaseConfig{
		DbConfig: legacyDbConfig,
		Version:  legacyVersion,
	}
	_, insertError := sc.BootstrapContext.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey30(ctx, groupID), legacyDatabaseConfig)
	require.NoError(t, insertError)

	// Fetch the registry, verify newDefaultDb still exists and defaultDb30 has not been migrated due to collection conflict
	configs, err := sc.BootstrapContext.GetDatabaseConfigs(ctx, tb.GetName(), groupID)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	dbConfig := configs[0]
	assert.Equal(t, "3.1", dbConfig.Version)
}

func getTestDatabaseConfig(bucketName string, dbName string, scopesConfig ScopesConfig, version string) *DatabaseConfig {
	dbConfig := makeDbConfig(bucketName, dbName, scopesConfig)
	return &DatabaseConfig{
		DbConfig: dbConfig,
		Version:  version,
	}
}

func makeDbConfig(bucketName string, dbName string, scopesConfig ScopesConfig) DbConfig {
	numIndexReplicas := uint(0)
	enableXattrs := base.TestUseXattrs()
	dbConfig := DbConfig{
		BucketConfig: BucketConfig{
			Bucket: &bucketName,
		},
		NumIndexReplicas: &numIndexReplicas,
		EnableXattrs:     &enableXattrs,
		Scopes:           scopesConfig,
	}
	if scopesConfig != nil {
		dbConfig.Scopes = scopesConfig
	}
	if dbName != "" {
		dbConfig.Name = dbName
	}
	return dbConfig
}

func TestPersistentConfigNoBucketField(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyConfig)

	b1 := base.GetTestBucket(t)
	defer b1.Close(base.TestCtx(t))
	b1Name := b1.GetName()

	// at the end of the test we'll move config from b1 into b2 to test backup/restore-type migration
	b2 := base.GetTestBucket(t)
	defer b2.Close(base.TestCtx(t))
	b2Name := b2.GetName()

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: b1.NoCloseClone(),
	})
	defer rt.Close()

	dbName := b1Name

	dbConfig := rt.NewDbConfig()
	// will infer from db name in handler and stamp into config (as of CBG-3353)
	dbConfig.Bucket = nil
	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// read back config in bucket to see if bucket field was stamped into the config
	var databaseConfig DatabaseConfig
	groupID := rt.ServerContext().Config.Bootstrap.ConfigGroupID
	configDocID := PersistentConfigKey(base.TestCtx(t), groupID, b1Name)
	_, err := rt.GetDatabase().MetadataStore.Get(configDocID, &databaseConfig)
	require.NoError(t, err)
	require.NotNil(t, databaseConfig.Bucket)
	assert.Equal(t, b1Name, *databaseConfig.Bucket, "bucket field should be stamped into config")

	// manually strip out bucket to test backwards compatibility (older configs don't always have this field set)
	_, err = rt.GetDatabase().MetadataStore.Update(configDocID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		var d DatabaseConfig
		require.NoError(t, base.JSONUnmarshal(current, &d))
		d.Bucket = nil
		newConfig, err := base.JSONMarshal(d)
		return newConfig, nil, false, err
	})
	require.NoError(t, err)

	count, err := rt.ServerContext().fetchAndLoadConfigs(base.TestCtx(t), false)
	require.NoError(t, err)
	require.Equal(t, 1, count, "should have loaded 1 config")

	_, err = rt.UpdatePersistedBucketName(&databaseConfig, &b2Name)
	require.NoError(t, err)

	dbBucketMismatch := base.SyncGatewayStats.GlobalStats.ConfigStat.DatabaseBucketMismatches.Value()

	// expect config to fail to load due to bucket mismatch
	count, err = rt.ServerContext().fetchAndLoadConfigs(base.TestCtx(t), false)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	dbBucketMismatch, _ = base.WaitForStat(t, base.SyncGatewayStats.GlobalStats.ConfigStat.DatabaseBucketMismatches.Value, dbBucketMismatch+1)

	// Move config docs from original bucket to b2 and force a fetch/load (simulate backup/restore or XDCR to different bucket)
	base.MoveDocument(t, base.SGRegistryKey, b2.GetMetadataStore(), b1.GetMetadataStore())
	base.MoveDocument(t, configDocID, b2.GetMetadataStore(), b1.GetMetadataStore())

	// put the bucket for the config back to b1 so we can use the admin API to repair the config (like a real user would have to do)
	_, err = b2.GetMetadataStore().Update(configDocID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		var d DatabaseConfig
		require.NoError(t, base.JSONUnmarshal(current, &d))
		d.Bucket = &b1Name
		newConfig, err := base.JSONMarshal(d)
		return newConfig, nil, false, err
	})
	require.NoError(t, err)

	count, err = rt.ServerContext().fetchAndLoadConfigs(base.TestCtx(t), false)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	dbBucketMismatch, _ = base.WaitForStat(t, base.SyncGatewayStats.GlobalStats.ConfigStat.DatabaseBucketMismatches.Value, dbBucketMismatch+1)

	// repair config
	dbConfig.Bucket = &b2Name

	// /db/_config won't work because db isn't actually loaded
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusNotFound)

	// PUT /db/ will work to repair config
	resp = rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// do another fetch just to be sure that the config won't be unloaded again
	count, err = rt.ServerContext().fetchAndLoadConfigs(base.TestCtx(t), false)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	_, _ = base.WaitForStat(t, base.SyncGatewayStats.GlobalStats.ConfigStat.DatabaseBucketMismatches.Value, dbBucketMismatch+1)
}

// startBootstrapServerWithoutConfigPolling starts a server with config polling disabled, and returns the server context.
func startBootstrapServerWithoutConfigPolling(t *testing.T) (*ServerContext, func()) {
	config := BootstrapStartupConfigForTest(t)
	// "disable" config polling for this test, to avoid non-deterministic test output based on polling times
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Hour * 24)
	return StartServerWithConfig(t, &config)
}
