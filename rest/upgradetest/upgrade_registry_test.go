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
	"log"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireBootstrapConnection(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("bootstrap connection requires CBS")
	}
}

// TestDefaultMetadataID creates an database using the named collections on the default scope, then modifies that database to use
// only the default collection. Verifies that metadata documents are still accessible.
func TestDefaultMetadataIDNamedToDefault(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	requireBootstrapConnection(t)
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
	requireBootstrapConnection(t)
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
	dbConfig.Scopes = rest.DefaultOnlyScopesConfig

	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	userPayload := `{"password":"letmein",
		"admin_channels":["foo", "bar"]}`

	putResponse := rt.SendAdminRequest("PUT", "/"+dbName+"/_user/bob", userPayload)
	rest.RequireStatus(t, putResponse, 201)
	bobDocName := "_sync:user:db:bob"
	requireBobUserLocation(rt, bobDocName)

	// Update database to only target default collection
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
	requireBootstrapConnection(t)

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

func TestLegacyMetadataID(t *testing.T) {

	if base.TestsUseNamedCollections() {
		t.Skip("This test covers legacy interaction with the default collection")
	}
	testCtx := base.TestCtx(t)

	tb1 := base.GetPersistentTestBucket(t)
	// Create a non-persistent rest tester.  Standard RestTester
	// creates a database 'db' targeting the default collection (when !TestUseNamedCollections)
	legacyRT := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1.NoCloseClone(),
		PersistentConfig: false,
	})

	// Create a document in the collection to trigger creation of _sync:seq
	resp := legacyRT.SendAdminRequest("PUT", "/db/testLegacyMetadataID", `{"test":"test"}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	// Get the legacy config for upgrade test below
	resp = legacyRT.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	legacyConfigBytes := resp.BodyBytes()
	log.Printf("Received legacy config: %s", legacyConfigBytes)
	var legacyConfig rest.LegacyServerConfig
	err := base.JSONUnmarshal(legacyConfigBytes, &legacyConfig)
	require.NoError(t, err)

	legacyRT.Close()

	log.Printf("testing")

	persistentRT := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb1,
		PersistentConfig: true,
	})
	defer persistentRT.Close()

	// Generate a dbConfig from the legacy startup config using ToStartupConfig, and use it to create a database
	_, dbMap, err := legacyConfig.ToStartupConfig(testCtx)
	require.NoError(t, err)

	dbConfig, ok := dbMap["db"]
	require.True(t, ok)

	// Need to sanitize the db config, but can't use sanitizeDbConfigs because it assumes non-empty server address
	dbConfig.Username = ""
	dbConfig.Password = ""
	dbConfigBytes, err := base.JSONMarshal(dbConfig)

	require.NoError(t, err)
	resp = persistentRT.SendAdminRequest("PUT", "/db/", string(dbConfigBytes))
	assert.Equal(t, http.StatusCreated, resp.Code)

	// check if database is online
	dbRoot := persistentRT.GetDatabaseRoot("db")
	require.Equal(t, db.RunStateString[db.DBOnline], dbRoot.State)
}

// TestMetadataIDRenameDatabase verifies that resync is not required when deleting and recreating a database (with a
// different name) targeting only the default collection.
func TestMetadataIDRenameDatabase(t *testing.T) {

	if base.TestsUseNamedCollections() {
		t.Skip("This test covers legacy interaction with the default collection")
	}

	// Create a persistent rest tester.
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
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

func requireBobUserLocation(rt *rest.RestTester, docName string) {
	metadataStore := rt.GetDatabase().Bucket.DefaultDataStore()

	_, _, err := metadataStore.GetRaw(docName)
	require.NoError(rt.TB, err)

}
