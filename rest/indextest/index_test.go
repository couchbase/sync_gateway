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
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
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
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

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
		indexRoles := "sg_roles"
		indexUsers := "sg_users"
		if base.TestUseXattrs() {
			indexSyncDocs += "_x1"
			indexAccess += "_x1"
			indexRoles += "_x1"
			indexUsers += "_x1"
		} else {
			indexSyncDocs += "_1"
			indexAccess += "_1"
			indexRoles += "_1"
			indexUsers += "_1"
		}
		metadataCollection, err := base.AsCollection(bucket.DefaultDataStore())
		require.NoError(t, err)
		indexNames, err := metadataCollection.GetIndexes()
		require.NoError(t, err)

		if rt.GetDatabase().UseLegacySyncDocsIndex() {
			require.Contains(t, indexNames, indexSyncDocs)
			require.NotContains(t, indexNames, indexRoles)
			require.NotContains(t, indexNames, indexUsers)
		} else {
			require.NotContains(t, indexNames, indexSyncDocs)
			require.Contains(t, indexNames, indexRoles)
			require.Contains(t, indexNames, indexUsers)
		}

		if base.TestsUseNamedCollections() {
			require.NotContains(t, indexNames, indexAccess)
		}
	}

	// tests sg_users index
	rt.Run("testUserQueries", func(t *testing.T) {
		users := []string{"alice", "bob"}

		for _, user := range users {
			rt.CreateUser(user, []string{"ChannelA"})
		}
		response := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/", "")
		rest.RequireStatus(t, response, http.StatusOK)

		var responseUsers []string
		err = json.Unmarshal(response.Body.Bytes(), &responseUsers)
		require.NoError(t, err)
		require.Equal(t, users, responseUsers)
	})

	// tests sg_roles index
	rt.Run("testRoleQueries", func(t *testing.T) {
		roles := []string{"roleA", "roleB"}

		for _, role := range roles {
			response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+role, rest.GetRolePayload(t, role, rt.GetSingleDataStore(), []string{"ChannelA"}))
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

// TestAsyncInitializeIndexes creates a database and simulates slow index creation (using collectionCompleteCallback).
// Verifies that offline operations can be performed successfully.
func TestAsyncInitializeIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	sc.DatabaseInitManager.SetTestCallbacks(collectionCompleteCallback, nil)

	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.Ptr(true)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	keyspace := dbName
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		keyspace = keyspaces[0]
	}

	// Persist config
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")

	// Get config values before taking db offline
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+dbName+"/_config", "")
	resp.RequireStatus(http.StatusOK)
	dbConfigBeforeOffline := resp.Body

	// Modify import filter and sync function while index init is pending/blocked
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// Check values are updated
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+dbName+"/_config", "")
	resp.RequireResponse(http.StatusOK, dbConfigBeforeOffline)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// unblock initialization
	log.Printf("closing unblockInit")
	close(unblockInit)

	// Bring the database online
	dbConfig.StartOffline = base.Ptr(false)
	dbOnlineConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// wait for db to come online
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)

}

// TestAsyncInitWithResync verifies that resync can run successfully while async index initialization is in progress.
// Handles the case where data has been migrated between buckets but index doesn't yet exist in new bucket.
//  1. Creates a database, writes documents via SG to generate metadata
//  2. Deletes the database
//  3. Manually drops indexes
//  4. Recreates the database with blocking callback for index initialization
//  5. Runs resync while init is blocked/in progress
func TestAsyncInitWithResync(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	if !base.TestUseXattrs() {
		t.Skip("this test uses xattrs for verification of sync metadata")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	// Seed the bucket with some documents
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	syncFunc := "function(doc){ channel(doc.channel1); }"
	dbConfig := makeDbConfig(t, tb, syncFunc, "")
	dbConfig.StartOffline = base.Ptr(false)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	docCollection, err := base.AsCollection(tb.GetSingleDataStore())
	require.NoError(t, err)
	keyspace := dbName + "." + docCollection.ScopeName() + "." + docCollection.CollectionName()

	// Persist config
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)

	for i := range 5 {
		docID := fmt.Sprintf("doc%d", i)
		docBody := `{"channel1":["ABC"], "channel2":["DEF"]}`
		resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+keyspace+"/"+docID, docBody)
		resp.RequireStatus(http.StatusCreated)
	}

	// Delete the database
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodDelete, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusOK)

	rest.DropAllTestIndexesIncludingPrimary(t, tb)

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	sc.DatabaseInitManager.SetTestCallbacks(collectionCompleteCallback, nil)
	// Recreate the database with offline=true and a modified sync function
	syncFunc = "function(doc){ channel(doc.channel2);}"
	dbConfig = makeDbConfig(t, tb, syncFunc, "")
	dbConfig.StartOffline = base.Ptr(true)
	dbConfigPayload, err = json.Marshal(dbConfig)
	require.NoError(t, err)

	// Recreate database
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before calling resync
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")

	// Start resync
	resyncPayload := rest.ResyncPostReqBody{}
	resyncPayload.Scope = base.NewCollectionNames(docCollection)

	payloadBytes, err := json.Marshal(resyncPayload)
	require.NoError(t, err)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_resync", string(payloadBytes))
	resp.RequireStatus(http.StatusOK)

	// Wait for resync to complete
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)

	// verify raw documents in the bucket to validate that resync ran before db came online
	for i := range 5 {
		docID := fmt.Sprintf("doc%d", i)
		requireActiveChannel(t, docCollection, docID, "DEF")
	}

	// unblock initialization
	log.Printf("closing unblockInit")
	close(unblockInit)

	// Bring the database online
	dbConfig.StartOffline = base.Ptr(false)
	dbOnlineConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// wait for db to come online
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)

}

// TestAsyncOnlineOffline verifies that a database that has been brought online can be taken offline
// while async index initialization is still in progress
func TestAsyncOnlineOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	sc.DatabaseInitManager.SetTestCallbacks(collectionCompleteCallback, nil)

	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.Ptr(true)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	keyspace := dbName
	expectedCollectionCount := 1 // metadata store
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		keyspace = keyspaces[0]
		expectedCollectionCount += len(keyspaces)
	}

	// Create database with offline=true
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db, validate db state is offline
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)
	verifyInitializationActive(t, sc, dbName, true)

	// Set up payloads for upserting db state
	onlineConfigUpsert := rest.DbConfig{
		StartOffline: base.Ptr(false),
	}
	dbOnlineConfigPayload, err := json.Marshal(onlineConfigUpsert)
	require.NoError(t, err)

	offlineConfigUpsert := rest.DbConfig{
		StartOffline: base.Ptr(true),
	}
	dbOfflineConfigPayload, err := json.Marshal(offlineConfigUpsert)
	require.NoError(t, err)

	// Take the database online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBStarting)
	verifyInitializationActive(t, sc, dbName, true)

	// Take the database offline while async init is still in progress
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOfflineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)
	verifyInitializationActive(t, sc, dbName, true)

	// Verify offline changes can still be made
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)
	verifyInitializationActive(t, sc, dbName, true)

	// Take the database back online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBStarting)
	verifyInitializationActive(t, sc, dbName, true)

	// Unblock initialization, verify status goes to Online
	close(unblockInit)
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)
	verifyInitializationActive(t, sc, dbName, false)

	// Verify only four collections were initialized (offline/online didn't trigger duplicate initialization)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(expectedCollectionCount), totalCount)

	// Take database back offline after init complete
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOfflineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)

	// Take database back online after init complete, verify successful
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)
	verifyInitializationActive(t, sc, dbName, false)

}

// TestAsyncCreateThenDelete verifies that async initialization of a database will be terminated
// if the database is deleted.  Also verifies that recreating the database succeeds as expected.
func TestAsyncCreateThenDelete(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	firstDatabaseComplete := make(chan error)
	databaseCompleteCount := int64(0)
	databaseCompleteCallback := func(dbName string) {
		count := atomic.AddInt64(&databaseCompleteCount, 1)
		// on first complete, close test channel
		if count == 1 {
			close(firstDatabaseComplete)
		}
	}
	sc.DatabaseInitManager.SetTestCallbacks(collectionCompleteCallback, databaseCompleteCallback)

	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.Ptr(true)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"
	expectedCollectionCount := 1 // metadata store
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		expectedCollectionCount += len(keyspaces)
	}

	// Set up payloads for upserting db state
	onlineConfigUpsert := rest.DbConfig{
		StartOffline: base.Ptr(false),
	}
	dbOnlineConfigPayload, err := json.Marshal(onlineConfigUpsert)
	require.NoError(t, err)

	// Create database with offline=true
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db, validate db state is offline
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)

	// Take the database online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBStarting)

	// Delete the database before unblocking init
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodDelete, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusOK)

	// verify not found
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusNotFound)

	close(unblockInit)

	rest.WaitForChannel(t, firstDatabaseComplete, "waiting for database complete callback")

	// Verify only one collection was initialized asynchronously (in-progress when database was deleted)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(1), totalCount)

	// Recreate the database, then bring online
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)

	// Verify all collections are initialized (1 from deleted run, plus full expected set)
	totalCount = atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(expectedCollectionCount+1), totalCount)
}

// TestSyncOnline verifies that a database that is created with startOffline=false doesn't trigger async initialization
func TestSyncOnline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.Ptr(false)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	// Create database with offline=false
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Verify online
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)

	// Verify database was initialized synchronously
	dbCtx, err := sc.GetDatabase(ctx, dbName)
	require.NoError(t, err)
	assert.True(t, dbCtx.WasInitializedSynchronously)
}

// TestAsyncInitConfigUpdates verifies that a database with in-progress async
// index initialization can accept all config updates from the local node.
// (prior to CBG-4008, operations would block waiting for async init to complete)
func TestAsyncInitConfigUpdates(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	sc.DatabaseInitManager.SetTestCallbacks(collectionCompleteCallback, nil)

	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.Ptr(true)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	keyspace := dbName
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		keyspace = keyspaces[0]
	}

	// Create database with offline=true
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db, validate db state is offline
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)

	// Set up payloads for upserting db state
	onlineConfigUpsert := rest.DbConfig{
		StartOffline: base.Ptr(false),
	}
	dbOnlineConfigPayload, err := json.Marshal(onlineConfigUpsert)
	require.NoError(t, err)

	// Take the database online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBStarting)

	// Attempt to update import filter while in starting mode
	importFilter = "function(doc){ return false; }"
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+keyspace+"/_config/import_filter", importFilter)
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	// Attempt to delete import filter while in starting mode
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodDelete, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, "")

	// Attempt to update sync function while in starting mode
	syncFunc = "function(doc){ channel(doc.type); }"
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+keyspace+"/_config/sync", syncFunc)
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// Attempt to delete sync function while in starting mode
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodDelete, "/"+keyspace+"/_config/sync", "")
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, "")

	// Take the database back online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, sc, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, sc, dbName, db.DBStarting)

	// Unblock initialization, verify status goes to Online
	close(unblockInit)
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)

}

// TestAsyncInitRemoteConfigUpdates verifies that a database with in-progress async
// index initialization can accept config updates made on other nodes (arriving via polling)
// (prior to CBG-4008, operations would block waiting for async init to complete)
func TestAsyncInitRemoteConfigUpdates(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

	// enable config polling to allow testing of cross-node updates
	bootstrapConfig := rest.BootstrapStartupConfigForTest(t)
	bootstrapConfig.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(1 * time.Second)
	sc, closeFn := rest.StartServerWithConfig(t, &bootstrapConfig)
	defer closeFn()

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	initStarted := make(chan error)
	unblockInit := make(chan error)
	collectionCompleteCallback := func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		count := atomic.AddInt64(&collectionCount, 1)
		// On first collection, close initStarted channel
		log.Printf("collection callback count: %v", count)
		if count == 1 {
			log.Printf("closing initStarted")
			close(initStarted)
		}
		rest.WaitForChannel(t, unblockInit, "waiting for test to unblock initialization")
	}
	sc.DatabaseInitManager.SetTestCallbacks(collectionCompleteCallback, nil)

	ctx := base.TestCtx(t)
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbName := "db"
	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.Name = dbName
	dbConfig.StartOffline = base.Ptr(true)

	keyspace := dbName
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		keyspace = keyspaces[0]
	}

	bucketName := tb.GetName()
	groupID := sc.Config.Bootstrap.ConfigGroupID

	// Simulate creation of the database with offline=true on another node
	version, err := rest.GenerateDatabaseConfigVersionID(ctx, "", &dbConfig)
	require.NoError(t, err)
	metadataID, err := sc.BootstrapContext.ComputeMetadataIDForDbConfig(ctx, &dbConfig)
	require.NoError(t, err)

	databaseConfig := dbConfig.ToDatabaseConfig()
	databaseConfig.Version = version
	databaseConfig.MetadataID = metadataID

	_, err = sc.BootstrapContext.InsertConfig(ctx, bucketName, groupID, databaseConfig)
	require.NoError(t, err)

	// Wait for init to start before interacting with the db, validate db state is offline
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")
	waitAndRequireDBState(t, sc, dbName, db.DBOffline)

	log.Printf("keyspace: %v", keyspace)

	// Update the bucket config to bring the database online
	_, err = sc.BootstrapContext.UpdateConfig(ctx, bucketName, groupID, dbName, func(bucketDbConfig *rest.DatabaseConfig) (updatedConfig *rest.DatabaseConfig, err error) {
		bucketDbConfig.StartOffline = base.Ptr(false)
		return bucketDbConfig, nil
	})
	require.NoError(t, err)
	waitAndRequireDBState(t, sc, dbName, db.DBStarting)

	// Attempt to update import filter while in starting mode
	importFilter = "function(doc){ return false; }"
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/"+keyspace+"/_config/import_filter", importFilter)
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	// Update the db config from a remote node, verify change is picked up while in starting state
	_, err = sc.BootstrapContext.UpdateConfig(ctx, bucketName, groupID, dbName, func(bucketDbConfig *rest.DatabaseConfig) (updatedConfig *rest.DatabaseConfig, err error) {
		_, scopeName, collectionName, err := rest.ParseKeyspace(keyspace)
		require.NoError(t, err)
		if scopeName == nil || collectionName == nil {
			bucketDbConfig.ImportFilter = nil
		} else {
			bucketDbConfig.Scopes[*scopeName].Collections[*collectionName].ImportFilter = nil
		}
		return bucketDbConfig, nil
	})
	require.NoError(t, err)

	// Need a wait loop here to wait for config polling to pick up the change
	err = rest.WaitForConditionWithOptions(ctx, func() bool {
		resp = rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+keyspace+"/_config/import_filter", "")
		if resp.StatusCode() == http.StatusOK && resp.Body == "" {
			return true
		} else {
			log.Printf("Waiting for OK and empty filter, current status: %v, filter: %q", resp.StatusCode(), resp.Body)
		}
		return (resp.StatusCode() == http.StatusOK) && resp.Body == ""
	}, 200, 100)
	require.NoError(t, err)

	// Unblock initialization, verify status goes to Online
	close(unblockInit)
	waitAndRequireDBState(t, sc, dbName, db.DBOnline)
}

// verifyInitializationActive verifies the expected value of InitializationActive on db and verbose all_dbs responses
func verifyInitializationActive(t *testing.T, sc *rest.ServerContext, dbName string, expectedValue bool) {

	var dbResult rest.DatabaseRoot
	resp := rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusOK)
	require.NoError(t, base.JSONUnmarshal([]byte(resp.Body), &dbResult))
	require.Equal(t, expectedValue, dbResult.InitializationActive)

	var allDbResult []rest.DbSummary
	allDbResp := rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/_all_dbs?verbose=true", "")
	allDbResp.RequireStatus(http.StatusOK)
	require.NoError(t, base.JSONUnmarshal([]byte(allDbResp.Body), &allDbResult))
	dbFound := false
	for _, dbSummary := range allDbResult {
		if dbSummary.DBName == dbName {
			dbFound = true
			require.Equal(t, expectedValue, dbSummary.InitializationActive)
		}
	}
	require.True(t, dbFound, "Database not found in _all_dbs response")
}

func makeDbConfig(t *testing.T, tb *base.TestBucket, syncFunction string, importFilter string) rest.DbConfig {

	scopesConfig := rest.GetCollectionsConfig(t, tb, 1)
	for scopeName, scope := range scopesConfig {
		for collectionName := range scope.Collections {
			collectionConfig := &rest.CollectionConfig{}
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
	enableXattrs := base.TestUseXattrs()

	dbConfig := rest.DbConfig{
		BucketConfig: rest.BucketConfig{
			Bucket: &bucketName,
		},
		EnableXattrs: &enableXattrs,
		Scopes:       scopesConfig,
		AutoImport:   false, // disable import to streamline index tests and avoid teardown races
	}
	if base.TestsDisableGSI() {
		dbConfig.UseViews = base.Ptr(true)
	} else {
		dbConfig.Index = &rest.IndexConfig{
			NumReplicas: base.Ptr(uint(0)),
		}
	}
	return dbConfig
}

func getRESTKeyspaces(dbName string, scopesConfig rest.ScopesConfig) []string {
	keyspaces := make([]string, 0)
	for scopeName, scope := range scopesConfig {
		for collectionName := range scope.Collections {
			keyspaces = append(keyspaces, strings.Join([]string{dbName, scopeName, collectionName}, base.ScopeCollectionSeparator))
		}
	}
	return keyspaces
}

// waitAndRequireDBState issues BootstrapAdminRequests to monitor db state
func waitAndRequireDBState(t *testing.T, sc *rest.ServerContext, dbName string, targetState uint32) {
	// wait for db to come online
	var stateCurr string
	for range 100 {
		var dbRootResponse rest.DatabaseRoot
		resp := rest.BootstrapAdminRequest(t, sc, http.MethodGet, "/"+dbName+"/", "")
		resp.Unmarshal(&dbRootResponse)
		stateCurr = dbRootResponse.State
		if stateCurr == db.RunStateString[targetState] {
			break
		}
		log.Printf("Waiting for state %s, current state %s", db.RunStateString[targetState], stateCurr)
		time.Sleep(500 * time.Millisecond)
	}
	require.Equal(t, db.RunStateString[targetState], stateCurr)
}

func requireActiveChannel(t *testing.T, dataStore base.DataStore, key string, channelName string) {
	xattrs, _, err := dataStore.GetXattrs(base.TestCtx(t), key, []string{base.SyncXattrName})
	require.NoError(t, err, "Error Getting Xattr as sync data")
	require.Contains(t, xattrs, base.SyncXattrName)
	var xattr db.SyncData
	require.NoError(t, json.Unmarshal(xattrs[base.SyncXattrName], &xattr), "Error unmarshalling sync data")
	channel, ok := xattr.Channels[channelName]
	require.True(t, ok)
	require.Nil(t, channel)
}

func TestPartitionedIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test requires Couchbase Server for GSI")
	}
	if !base.TestUseXattrs() {
		t.Skip("Partitioned indexes are only supported with non xattr indexes")
	}
	if base.TestsDisableGSI() {
		t.Skip("Partitioned indexes are not supported with views")
	}
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		SyncFn:           channels.DocChannelsSyncFunction,
	})
	defer rt.Close()
	dbConfig := rt.NewDbConfig()
	dbConfig.Index = &rest.IndexConfig{
		NumPartitions: base.Ptr(uint32(8)),
		NumReplicas:   base.Ptr(uint(0)),
	}
	rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	rt.CreateUser("alice", []string{"alice"})
	collection, err := base.AsCollection(rt.GetSingleDataStore())
	require.NoError(t, err)

	indexNames, err := collection.GetIndexes()
	require.NoError(t, err)

	expectedIndexNames := []string{
		// non-partitioned indexes
		"sg_access_x1", "sg_roleAccess_x1", "sg_tombstones_x1",
		// partitioned indexes
		"sg_allDocs_x1_p8", "sg_channels_x1_p8",
	}
	if !base.TestsUseNamedCollections() {
		// metadata index, unpartitioned
		expectedIndexNames = append(expectedIndexNames, "sg_users_x1")
		expectedIndexNames = append(expectedIndexNames, "sg_roles_x1")
	}
	require.ElementsMatch(t, expectedIndexNames, indexNames)

	bucket, err := base.AsGocbV2Bucket(rt.Bucket())
	require.NoError(t, err)
	for _, indexName := range expectedIndexNames {
		expectedPartitions := uint32(1)
		if strings.HasSuffix(indexName, "_x1_p8") {
			expectedPartitions = 8
		}
		require.Equal(t, expectedPartitions, db.GetIndexPartitionCount(t, bucket, collection, indexName))
	}
	// verify allDocs index is working
	rt.PutDoc("doc1", `{"channels": ["alice"]}`)
	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_all_docs", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	allDocs := struct {
		Rows []any
	}{}
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &allDocs), "Error unmarshalling all docs response, body: %s", resp.Body)
	require.Len(t, allDocs.Rows, 1)

	// verify channels index is working
	rt.GetDatabase().FlushRevisionCacheForTest()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "alice")
	changes.RequireDocIDs(t, []string{"doc1", "_user/alice"})
}
