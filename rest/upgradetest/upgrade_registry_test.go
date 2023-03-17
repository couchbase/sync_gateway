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
	"github.com/couchbase/sync_gateway/rest"
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

func requireBobUserLocation(rt *rest.RestTester, docName string) {
	metadataStore := rt.GetDatabase().Bucket.DefaultDataStore()

	_, _, err := metadataStore.GetRaw(docName)
	require.NoError(rt.TB, err)

}
