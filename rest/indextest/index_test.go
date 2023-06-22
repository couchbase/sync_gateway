// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func requireNoIndexes(t *testing.T, dataStore base.DataStore) {
	collection, err := base.AsCollection(dataStore)
	require.NoError(t, err)
	indexNames, err := collection.GetIndexes()
	require.NoError(t, err)
	require.Len(t, indexNames, 0)

}

func TestSyncGatewayStartupIndexes(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Assert there are no indexes on the datastores, to test server startup
	dsNames, err := bucket.ListDataStores()
	require.NoError(t, err)
	for _, dsName := range dsNames {
		dataStore, err := bucket.NamedDataStore(dsName)
		require.NoError(t, err)
		if !base.TestsDisableGSI() {
			requireNoIndexes(t, dataStore)
		}
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: bucket.NoCloseClone(),
	})
	defer rt.Close()

	_ = rt.Bucket() // initialize RestTester

	if !base.TestsDisableGSI() {
		// use example indexes to make sure metadata and non metadata are created
		indexSyncDocs := "sg_syncDocs"
		indexAccess := "sg_access"
		if base.TestUseXattrs() {
			indexSyncDocs += "_x1"
			indexAccess += "_x1"
		} else {
			indexSyncDocs += "_1"
			indexAccess += "_1"
		}
		metadataCollection, err := base.AsCollection(bucket.DefaultDataStore())
		require.NoError(t, err)
		indexNames, err := metadataCollection.GetIndexes()
		require.NoError(t, err)

		require.Contains(t, indexNames, indexSyncDocs)

		if base.TestsUseNamedCollections() {
			require.NotContains(t, indexNames, indexAccess)
		}
	}

	// tests sg_users index
	t.Run("testUserQueries", func(t *testing.T) {
		users := []string{"alice", "bob"}

		for _, user := range users {
			response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+user, rest.GetUserPayload(t, user, rest.RestTesterDefaultUserPassword, "", rt.GetSingleTestDatabaseCollection(), []string{"ChannelA"}, nil))
			rest.RequireStatus(t, response, http.StatusCreated)
		}
		response := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/", "")
		rest.RequireStatus(t, response, http.StatusOK)

		var responseUsers []string
		err = json.Unmarshal(response.Body.Bytes(), &responseUsers)
		require.NoError(t, err)
		require.Equal(t, users, responseUsers)
	})

	// tests sg_roles index
	t.Run("testRoleQueries", func(t *testing.T) {
		roles := []string{"roleA", "roleB"}

		for _, role := range roles {
			response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+role, rest.GetRolePayload(t, role, rest.RestTesterDefaultUserPassword, rt.GetSingleTestDatabaseCollection(), []string{"ChannelA"}))
			rest.RequireStatus(t, response, http.StatusCreated)
		}
		response := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_role/", "")
		rest.RequireStatus(t, response, http.StatusOK)

		var responseRoles []string
		err = json.Unmarshal(response.Body.Bytes(), &responseRoles)
		require.NoError(t, err)
		require.Equal(t, roles, responseRoles)
	})

}

func TestAsyncInitializeIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Set testing callbacks for async initialization

	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(dbName, collectionName string) {
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	sc.DatabaseInitManager.SetCallbacks(collectionCompleteCallback, nil)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.BoolPtr(true)
	dbConfigPayload, err := json.Marshal(dbConfig)
	dbName := "db"

	keyspace := dbName
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		keyspace = keyspaces[0]
	}
	require.NoError(t, err)

	// Persist config
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")

	// Get config values before taking db offline
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+dbName+"/_config", "")
	resp.RequireStatus(http.StatusOK)
	dbConfigBeforeOffline := resp.Body

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// Check values are updated
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+dbName+"/_config", "")
	resp.RequireResponse(http.StatusOK, dbConfigBeforeOffline)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	log.Printf("closing unblockInit")
	close(unblockInit)

	// Bring the database online
	dbConfig.StartOffline = base.BoolPtr(false)
	dbOnlineConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// wait for db to come online
	var stateCurr string
	for i := 0; i < 100; i++ {
		var dbRootResponse rest.DatabaseRoot
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+dbName+"/", "")
		resp.Unmarshal(&dbRootResponse)
		stateCurr = dbRootResponse.State
		log.Printf("db state: %v", stateCurr)
		if stateCurr == db.RunStateString[db.DBOnline] {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, db.RunStateString[db.DBOnline], stateCurr)

}

//TODO:
//   - remove indexes, add callbacks to verify functionality works while database is offline
//   - similar test that has data populated in the bucket, validate resync
//    - create db, write a bunch of docs
//    - delete db
//    - manually drop indexes  (simulates something like XDCR of populated data into a collection with no data)
//    - create new db, init indexes, run resync without indexes

func makeDbConfig(t *testing.T, tb *base.TestBucket, syncFunction string, importFilter string) rest.DbConfig {

	scopesConfig := rest.GetCollectionsConfig(t, tb, 3)
	for scopeName, scope := range scopesConfig {
		for collectionName, _ := range scope.Collections {
			collectionConfig := rest.CollectionConfig{}
			if syncFunction != "" {
				collectionConfig.SyncFn = &syncFunction
			}
			if importFilter != "" {
				collectionConfig.ImportFilter = &importFilter
			}
			scopesConfig[scopeName].Collections[collectionName] = collectionConfig
		}
	}
	bucketName := tb.GetName()
	numIndexReplicas := uint(0)
	enableXattrs := base.TestUseXattrs()

	dbConfig := rest.DbConfig{
		BucketConfig: rest.BucketConfig{
			Bucket: &bucketName,
		},
		NumIndexReplicas: &numIndexReplicas,
		EnableXattrs:     &enableXattrs,
		Scopes:           scopesConfig,
	}
	return dbConfig
}

func getRESTKeyspaces(dbName string, scopesConfig rest.ScopesConfig) []string {
	keyspaces := make([]string, 0)
	for scopeName, scope := range scopesConfig {
		for collectionName, _ := range scope.Collections {
			keyspaces = append(keyspaces, strings.Join([]string{dbName, scopeName, collectionName}, base.ScopeCollectionSeparator))
		}
	}
	return keyspaces
}
