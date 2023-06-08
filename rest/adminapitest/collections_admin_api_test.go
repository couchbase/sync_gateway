// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package adminapitest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestCollectionsSyncImportFunctions(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	})
	defer rt.Close()

	importFilter1 := `function (doc) { console.log('importFilter1'); return true }`
	importFilter2 := `function (doc) { console.log('importFilter2'); return doc.type == 'onprem'}`
	syncFunction1 := `function (doc) { console.log('syncFunction1'); return true }`
	syncFunction2 := `function (doc) { console.log('syncFunction2'); return doc.type == 'onprem'}`

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore1Name, ok := base.AsDataStoreName(dataStore1)
	require.True(t, ok)
	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	dataStore2Name, ok := base.AsDataStoreName(dataStore2)
	require.True(t, ok)
	keyspace1 := fmt.Sprintf("%s.%s.%s", "db", dataStore1Name.ScopeName(), dataStore1Name.CollectionName())
	keyspace2 := fmt.Sprintf("%s.%s.%s", "db", dataStore2Name.ScopeName(), dataStore2Name.CollectionName())

	bucketConfig := fmt.Sprintf(
		`{"bucket": "%s",
		  "scopes": {
			"%s": {
				"collections": {
					"%s": {
        					"import_filter": "%s",
        					"sync": "%s"
					},
					"%s": {
        					"import_filter": "%s",
        					"sync": "%s"
					}
				}
			}
		  },
		  "num_index_replicas": 0,
		  "enable_shared_bucket_access": true,
		  "use_views": false}`,
		tb.GetName(), dataStore1Name.ScopeName(), dataStore1Name.CollectionName(),
		importFilter1,
		syncFunction1, dataStore2Name.CollectionName(),
		importFilter2,
		syncFunction2,
	)
	resp := rt.SendAdminRequest(http.MethodPut, "/db/", bucketConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction2, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, resp.Body.String(), importFilter2)

	// Delete all custom functions
	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	// Make sure all are empty
	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	// Replace the original functions
	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/import_filter", keyspace1), importFilter1)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/import_filter", keyspace2), importFilter2)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/sync", keyspace1), syncFunction1)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/sync", keyspace2), syncFunction2)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	// Validate they are back to original
	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction2, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter2, resp.Body.String())

}

// TestRequireResync tests behaviour when a collection moves between databases.  New database should start offline until
// resync with regenerateSequences=true has been run for the collection
func TestRequireResync(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, until creating a db through the REST API allows the views/walrus/collections combination")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: base.GetPersistentTestBucket(t),
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	_ = rt.Bucket()

	db1Name := "db1"
	db2Name := "db2"

	// Set up scopes configs
	scopesConfig := rest.GetCollectionsConfig(t, rt.TestBucket, 2)

	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)
	scope := dataStoreNames[0].ScopeName()
	collection1 := dataStoreNames[0].CollectionName()
	collection2 := dataStoreNames[1].CollectionName()
	scopeAndCollection1Name := base.ScopeAndCollectionName{Scope: scope, Collection: collection1}

	scopesConfigC1Only := rest.GetCollectionsConfig(t, rt.TestBucket, 2)
	delete(scopesConfigC1Only[scope].Collections, collection2)

	scopesConfigC2Only := rest.GetCollectionsConfig(t, rt.TestBucket, 2)
	delete(scopesConfigC2Only[scope].Collections, collection1)

	// Create a db1 with two collections
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = scopesConfig

	resp := rt.CreateDatabase(db1Name, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Write documents to collection 1 and 2
	ks_db1_c1 := db1Name + "." + scope + "." + collection1
	ks_db1_c2 := db1Name + "." + scope + "." + collection2
	ks_db2_c1 := db2Name + "." + scope + "." + collection1

	resp = rt.SendAdminRequest("PUT", "/"+ks_db1_c1+"/testDoc1", `{"foo":"bar"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/"+ks_db1_c2+"/testDoc2", `{"foo":"bar"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Update db1 to remove collection1
	dbConfig.Scopes = scopesConfigC2Only
	resp = rt.ReplaceDbConfig(db1Name, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Validate that doc can still be retrieved from collection 2
	resp = rt.SendAdminRequest("GET", "/"+ks_db1_c2+"/testDoc2", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Create db2 targeting collection 1
	db2Config := rt.NewDbConfig()
	db2Config.Scopes = scopesConfigC1Only

	resp = rt.CreateDatabase(db2Name, db2Config)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Get status, verify offline
	resp = rt.SendAdminRequest("GET", "/"+db2Name+"/", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	dbRootResponse := rest.DatabaseRoot{}
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &dbRootResponse))
	require.Equal(t, db.RunStateString[db.DBOffline], dbRootResponse.State)
	require.Equal(t, []string{scopeAndCollection1Name.String()}, dbRootResponse.RequireResync)

	// Call _online, verify it doesn't override offline when resync is still required.
	// The online call is async, but subsequent get to status should remain offline
	onlineResponse := rt.SendAdminRequest("POST", "/"+db2Name+"/_online", "")
	rest.RequireStatus(t, onlineResponse, http.StatusOK)
	require.NoError(t, rt.WaitForDatabaseState(db2Name, db.DBOffline))

	needsResync := []string{scope + "." + collection1}
	rest.WaitAndAssertCondition(t, func() bool {
		resp = rt.SendAdminRequest("GET", "/"+db2Name+"/", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		dbRootResponse = rest.DatabaseRoot{}
		require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &dbRootResponse))
		return slices.Equal(needsResync, dbRootResponse.RequireResync)
	}, "expected %+v but got %+v for requireResync", needsResync, dbRootResponse.RequireResync)
	// Run resync for collection
	resyncCollections := make(db.ResyncCollections, 0)
	resyncCollections[scope] = []string{collection1}
	resyncPayload, marshalErr := base.JSONMarshal(resyncCollections)
	require.NoError(t, marshalErr)

	resp = rt.SendAdminRequest("POST", "/"+db2Name+"/_resync?action=start&regenerate_sequences=true", string(resyncPayload))
	rest.RequireStatus(t, resp, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted,
		func(t testing.TB) db.BackgroundProcessState {
			return rt.GetDatabase().ResyncManager.GetRunState()
		})

	// Attempt online again, should now succeed
	onlineResponse = rt.SendAdminRequest("POST", "/"+db2Name+"/_online", "")
	rest.RequireStatus(t, onlineResponse, http.StatusOK)
	require.NoError(t, rt.WaitForDatabaseState(db2Name, db.DBOnline))

	resp = rt.SendAdminRequest("GET", "/"+db2Name+"/", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	dbRootResponse = rest.DatabaseRoot{}
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &dbRootResponse))
	assert.Nil(t, dbRootResponse.RequireResync)

	resp = rt.SendAdminRequest("GET", "/"+ks_db2_c1+"/testDoc1", "")
	rest.RequireStatus(t, resp, http.StatusOK)
}
