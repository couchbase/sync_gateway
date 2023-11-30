/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package upgradetest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefaultMetadataID creates an database using the named collections on the default scope, then modifies that database to use
// only the default collection. Verifies that metadata documents are still accessible.
func TestDefaultMetadataIDNamedToDefault(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	rtConfig := &rest.RestTesterConfig{
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	_ = rt.Bucket()

	dbName := "db"

	// Create a database with named collections
	// Update config to remove named collections
	scopesConfig := rest.GetCollectionsConfig(t, rt.TestBucket, 2)

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = scopesConfig

	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	userPayload := `{"password":"letmein",
		"admin_channels":["foo", "bar"]}`

	putResponse := rt.SendAdminRequest("PUT", "/"+dbName+"/_user/bob", userPayload)
	rest.RequireStatus(t, putResponse, 201)
	bobDocName := "_sync:user:db:bob"
	requireBobUserLocation(rt, bobDocName)

	// Update database to only target default collection
	dbConfig.Scopes = rest.DefaultOnlyScopesConfig
	resp = rt.ReplaceDbConfig(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	//  Validate that the user can still be retrieved
	userResponse := rt.SendAdminRequest("GET", "/"+dbName+"/_user/bob", "")
	rest.RequireStatus(t, userResponse, http.StatusOK)

	requireBobUserLocation(rt, bobDocName)
}

// TestDefaultMetadataID creates an upgraded database using the defaultMetadataID, then modifies that database to use
// named collections in the default scope. Verifies that metadata documents are still accessible.
func TestDefaultMetadataIDDefaultToNamed(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	rtConfig := &rest.RestTesterConfig{
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	_ = rt.Bucket()

	dbName := "db"
	// Create a database with default collection,
	// Update config to switch to named collections

	scopesConfig := rest.GetCollectionsConfig(t, rt.TestBucket, 2)
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.DefaultOnlyScopesConfig

	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	userPayload := `{"password":"letmein",
		"admin_channels":["foo", "bar"]}`

	putResponse := rt.SendAdminRequest("PUT", "/"+dbName+"/_user/bob", userPayload)
	rest.RequireStatus(t, putResponse, 201)
	bobDocName := "_sync:user:bob"
	requireBobUserLocation(rt, bobDocName)

	// Update database to only target named collection
	dbConfig.Scopes = scopesConfig
	resp = rt.ReplaceDbConfig(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	//  Validate that the user can still be retrieved
	userResponse := rt.SendAdminRequest("GET", "/"+dbName+"/_user/bob", "")
	rest.RequireStatus(t, userResponse, http.StatusOK)

	requireBobUserLocation(rt, bobDocName)
}

// TestDefaultMetadataID creates an upgraded database using the defaultMetadataID, then modifies that database to use
// named collections in the default scope. Verifies that metadata documents are still accessible.
func TestUpgradeDatabasePreHelium(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)

	rtConfig := &rest.RestTesterConfig{
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	dbName := "db"

	// set legacy docs
	metadataStore := rt.Bucket().DefaultDataStore()
	err := metadataStore.Set(base.DefaultMetadataKeys.SyncSeqKey(), 0, nil, 0)
	require.NoError(t, err)

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.DefaultOnlyScopesConfig

	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	rt.CreateUser("bob", []string{"foo"})
	bobDocName := "_sync:user:bob"
	requireBobUserLocation(rt, bobDocName)

	dbConfigWithScopes := rt.NewDbConfig()
	resp = rt.ReplaceDbConfig(dbName, dbConfigWithScopes)
	//  Validate that the user can still be retrieved
	userResponse := rt.SendAdminRequest("GET", "/"+dbName+"/_user/bob", "")
	rest.RequireStatus(t, userResponse, http.StatusOK)

	rest.RequireStatus(t, resp, http.StatusCreated)
	requireBobUserLocation(rt, bobDocName)

}

// getDbConfigFromLegacyConfig gets the db config suitable for PUT into a new database.
func getDbConfigFromLegacyConfig(rt *rest.RestTester) string {
	// Get the legacy config for upgrade test below
	resp := rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	legacyConfigBytes := resp.BodyBytes()

	var legacyConfig rest.LegacyServerConfig
	err := base.JSONUnmarshal(legacyConfigBytes, &legacyConfig)
	assert.NoError(rt.TB, err)

	// Generate a dbConfig from the legacy startup config using ToStartupConfig, and use it to create a database
	_, dbMap, err := legacyConfig.ToStartupConfig(rt.Context())
	require.NoError(rt.TB, err)

	dbConfig, ok := dbMap["db"]
	require.True(rt.TB, ok)

	// Need to sanitize the db config, but can't use sanitizeDbConfigs because it assumes non-empty server address
	dbConfig.Username = ""
	dbConfig.Password = ""
	dbConfigBytes, err := base.JSONMarshal(dbConfig)
	require.NoError(rt.TB, err)
	return string(dbConfigBytes)

}
func TestLegacyMetadataID(t *testing.T) {

	tb1 := base.GetTestBucket(t)
	// Create a non-persistent rest tester.  Standard RestTester
	// creates a database 'db' targeting the default collection (when !TestUseNamedCollections)
	legacyRT := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1.NoCloseClone(),
		PersistentConfig: false,
	})

	// Create a document in the collection to trigger creation of _sync:seq
	resp := legacyRT.SendAdminRequest("PUT", "/db/testLegacyMetadataID", `{"test":"test"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbConfigString := getDbConfigFromLegacyConfig(legacyRT)
	legacyRT.Close()

	persistentRT := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
	})
	defer persistentRT.Close()

	resp = persistentRT.SendAdminRequest("PUT", "/db/", dbConfigString)
	assert.Equal(t, http.StatusCreated, resp.Code)

	// check if database is online
	dbRoot := persistentRT.GetDatabaseRoot("db")
	require.Equal(t, db.RunStateString[db.DBOnline], dbRoot.State)
}

// TestMetadataIDRenameDatabase verifies that resync is not required when deleting and recreating a database (with a
// different name) targeting only the default collection.
func TestMetadataIDRenameDatabase(t *testing.T) {

	// Create a persistent rest tester with default collection.
	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbName := "db"
	rt.CreateDatabase(dbName, dbConfig)

	// Write a document to ensure _sync:seq is initialized
	resp := rt.SendAdminRequest("PUT", "/"+dbName+"/testRenameDatabase", `{"test":"test"}`)
	require.Equal(t, http.StatusCreated, resp.Code)

	// Delete database
	resp = rt.SendAdminRequest("DELETE", "/"+dbName+"/", "")
	require.Equal(t, http.StatusOK, resp.Code)

	newDbName := "newdb"
	dbConfig.Name = newDbName

	// Create a new db targeting the same bucket
	rt.CreateDatabase(newDbName, dbConfig)

	// check if database is online
	dbRoot := rt.GetDatabaseRoot(newDbName)
	require.Equal(t, db.RunStateString[db.DBOnline], dbRoot.State)
}

// Verifies that matching metadataIDs are computed if two config groups for the same database are upgraded
func TestMetadataIDWithConfigGroups(t *testing.T) {

	tb1 := base.GetTestBucket(t)
	// Create a non-persistent rest tester.  Standard RestTester
	// creates a database 'db' targeting the default collection for legacy config.
	legacyRT := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1.NoCloseClone(),
		PersistentConfig: false,
	})

	// Create a document in the collection to trigger creation of _sync:seq
	resp := legacyRT.SendAdminRequest("PUT", "/db/testLegacyMetadataID", `{"test":"test"}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	dbConfigString := getDbConfigFromLegacyConfig(legacyRT)
	legacyRT.Close()

	group1RT := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
		GroupID:          base.StringPtr("group1"),
	})
	defer group1RT.Close()

	group2RT := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
		GroupID:          base.StringPtr("group2"),
	})
	defer group2RT.Close()

	// Create the database in both RTs, verify that it comes online in both with matching metadata IDs
	resp = group1RT.SendAdminRequest("PUT", "/db/", dbConfigString)
	assert.Equal(t, http.StatusCreated, resp.Code)

	resp = group2RT.SendAdminRequest("PUT", "/db/", dbConfigString)
	assert.Equal(t, http.StatusCreated, resp.Code)

	// check if databases are online
	dbRoot := group1RT.GetDatabaseRoot("db")
	require.Equal(t, db.RunStateString[db.DBOnline], dbRoot.State)

	dbRoot = group2RT.GetDatabaseRoot("db")
	require.Equal(t, db.RunStateString[db.DBOnline], dbRoot.State)
}

func requireBobUserLocation(rt *rest.RestTester, docName string) {
	metadataStore := rt.GetDatabase().Bucket.DefaultDataStore()

	_, _, err := metadataStore.GetRaw(docName)
	require.NoError(rt.TB, err)

}
