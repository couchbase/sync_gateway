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
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func requireNoIndexes(t *testing.T, dataStore base.DataStore) {
	collection, err := base.AsCollection(dataStore)
	require.NoError(t, err)
	indexes, err := collection.GetAllIndexes(base.TestCtx(t))
	require.NoError(t, err)
	require.Len(t, indexes, 0)

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
		if base.TestUseXattrs() {
			indexSyncDocs += "_x1"
			indexAccess += "_x1"
		} else {
			indexSyncDocs += "_1"
			indexAccess += "_1"
		}
		metadataCollection, err := base.AsCollection(bucket.DefaultDataStore())
		require.NoError(t, err)
		indexes, err := metadataCollection.GetAllIndexes(ctx)
		require.NoError(t, err)

		require.Contains(t, indexes, base.N1QLIndex{Name: indexSyncDocs, State: base.IndexStateOnline})

		if base.TestsUseNamedCollections() {
			for _, index := range indexes {
				require.NotEqual(t, index.Name, indexAccess)
			}
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

// TestAsyncInitializeIndexes creates a database and simulates slow index creation (using collectionCompleteCallback).
// Verifies that offline operations can be performed successfully.
func TestAsyncInitializeIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

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
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

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
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.BoolPtr(true)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	keyspace := dbName
	if len(dbConfig.Scopes) > 0 {
		keyspaces := getRESTKeyspaces(dbName, dbConfig.Scopes)
		keyspace = keyspaces[0]
	}

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

	// Modify import filter and sync function while index init is pending/blocked
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

	// unblock initialization
	log.Printf("closing unblockInit")
	close(unblockInit)

	// Bring the database online
	dbConfig.StartOffline = base.BoolPtr(false)
	dbOnlineConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// wait for db to come online
	waitAndRequireDBState(t, dbName, db.DBOnline)

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
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

	// Seed the bucket with some documents
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	syncFunc := "function(doc){ channel(doc.channel1); }"
	dbConfig := makeDbConfig(t, tb, syncFunc, "")
	dbConfig.StartOffline = base.BoolPtr(false)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	docCollection, err := base.AsCollection(tb.GetSingleDataStore())
	require.NoError(t, err)
	keyspace := dbName + "." + docCollection.ScopeName() + "." + docCollection.CollectionName()

	// Persist config
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBOnline)

	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("doc%d", i)
		docBody := `{"channel1":["ABC"], "channel2":["DEF"]}`
		resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/"+keyspace+"/"+docID, docBody)
		resp.RequireStatus(http.StatusCreated)
	}

	// Delete the database
	resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusOK)

	rest.DropAllTestIndexes(t, tb)

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
	// Recreate the database with offline=true and a modified sync function
	syncFunc = "function(doc){ channel(doc.channel2);}"
	dbConfig = makeDbConfig(t, tb, syncFunc, "")
	dbConfig.StartOffline = base.BoolPtr(true)
	dbConfigPayload, err = json.Marshal(dbConfig)
	require.NoError(t, err)

	// Recreate database
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before calling resync
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")

	// Start resync
	resyncPayload := rest.ResyncPostReqBody{}
	resyncPayload.Scope = db.ResyncCollections{
		docCollection.ScopeName(): []string{docCollection.CollectionName()},
	}
	payloadBytes, err := json.Marshal(resyncPayload)
	require.NoError(t, err)

	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_resync", string(payloadBytes))
	resp.RequireStatus(http.StatusOK)

	// Wait for resync to complete
	waitAndRequireDBState(t, dbName, db.DBOffline)

	// verify raw documents in the bucket to validate that resync ran before db came online
	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("doc%d", i)
		requireActiveChannel(t, docCollection, docID, "DEF")
	}

	// unblock initialization
	log.Printf("closing unblockInit")
	close(unblockInit)

	// Bring the database online
	dbConfig.StartOffline = base.BoolPtr(false)
	dbOnlineConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// wait for db to come online
	waitAndRequireDBState(t, dbName, db.DBOnline)

}

// TestAsyncOnlineOffline verifies that a database that has been brought online can be taken offline
// while async index initialization is still in progress
func TestAsyncOnlineOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)

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
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

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
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.BoolPtr(true)
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
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db, validate db state is offline
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	log.Printf("initialization started")
	waitAndRequireDBState(t, dbName, db.DBOffline)

	// Set up payloads for upserting db state
	onlineConfigUpsert := rest.DbConfig{
		StartOffline: base.BoolPtr(false),
	}
	dbOnlineConfigPayload, err := json.Marshal(onlineConfigUpsert)
	require.NoError(t, err)

	offlineConfigUpsert := rest.DbConfig{
		StartOffline: base.BoolPtr(true),
	}
	dbOfflineConfigPayload, err := json.Marshal(offlineConfigUpsert)
	require.NoError(t, err)

	// Take the database online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBStarting)

	// Take the database offline while async init is still in progress
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOfflineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBOffline)

	// Verify offline changes can still be made
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+keyspace+"/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// Take the database back online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBStarting)

	// Unblock initialization, verify status goes to Online
	close(unblockInit)
	waitAndRequireDBState(t, dbName, db.DBOnline)

	// Verify only four collections were initialized (offline/online didn't trigger duplicate initialization)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(expectedCollectionCount), totalCount)

	// Take database back offline after init complete
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOfflineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBOffline)

	// Take database back online after init complete, verify successful
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBOnline)

}

// TestAsyncCreateThenDelete verifies that async initialization of a database will be terminated
// if the database is deleted.  Also verifies that recreating the database succeeds as expected.
func TestAsyncCreateThenDelete(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP)
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
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

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
	firstDatabaseComplete := make(chan error)
	databaseCompleteCount := int64(0)
	databaseCompleteCallback := func(dbName string) {
		count := atomic.AddInt64(&databaseCompleteCount, 1)
		// on first complete, close test channel
		if count == 1 {
			close(firstDatabaseComplete)
		}
	}
	sc.DatabaseInitManager.SetCallbacks(collectionCompleteCallback, databaseCompleteCallback)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.BoolPtr(true)
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
		StartOffline: base.BoolPtr(false),
	}
	dbOnlineConfigPayload, err := json.Marshal(onlineConfigUpsert)
	require.NoError(t, err)

	// Create database with offline=true
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Wait for init to start before interacting with the db, validate db state is offline
	rest.WaitForChannel(t, initStarted, "waiting for initialization to start")
	waitAndRequireDBState(t, dbName, db.DBOffline)

	// Take the database online while async init is still in progress, verify state goes to Starting
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBStarting)

	// Delete the database before unblocking init
	resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusOK)

	// verify not found
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/"+dbName+"/", "")
	resp.RequireStatus(http.StatusNotFound)

	close(unblockInit)

	rest.WaitForChannel(t, firstDatabaseComplete, "waiting for database complete callback")

	// Verify only one collection was initialized asynchronously (in-progress when database was deleted)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(1), totalCount)

	// Recreate the database, then bring online
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBOffline)

	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/"+dbName+"/_config", string(dbOnlineConfigPayload))
	resp.RequireStatus(http.StatusCreated)
	waitAndRequireDBState(t, dbName, db.DBOnline)

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
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

	// Set testing callbacks for async initialization
	collectionCount := int64(0)
	collectionCompleteCallback := func(dbName, collectionName string) {
		_ = atomic.AddInt64(&collectionCount, 1)
	}
	sc.DatabaseInitManager.SetCallbacks(collectionCompleteCallback, nil)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := makeDbConfig(t, tb, syncFunc, importFilter)
	dbConfig.StartOffline = base.BoolPtr(false)
	dbConfigPayload, err := json.Marshal(dbConfig)
	require.NoError(t, err)
	dbName := "db"

	// Create database with offline=false
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/"+dbName+"/", string(dbConfigPayload))
	resp.RequireStatus(http.StatusCreated)

	// Verify online
	waitAndRequireDBState(t, dbName, db.DBOnline)

	// Verify no collections were initialized asynchronously
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(0), totalCount)

}

func makeDbConfig(t *testing.T, tb *base.TestBucket, syncFunction string, importFilter string) rest.DbConfig {

	scopesConfig := rest.GetCollectionsConfig(t, tb, 3)
	for scopeName, scope := range scopesConfig {
		for collectionName, _ := range scope.Collections {
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

// waitAndRequireDBState issues BootstrapAdminRequests to monitor db state
func waitAndRequireDBState(t *testing.T, dbName string, targetState uint32) {
	// wait for db to come online
	var stateCurr string
	for i := 0; i < 100; i++ {
		var dbRootResponse rest.DatabaseRoot
		resp := rest.BootstrapAdminRequest(t, http.MethodGet, "/"+dbName+"/", "")
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
	xattr := db.SyncData{}
	_, err := dataStore.GetWithXattr(base.TestCtx(t), key, base.SyncXattrName, "", nil, &xattr, nil)
	require.NoError(t, err, "Error Getting Xattr as sync data")
	channel, ok := xattr.Channels[channelName]
	require.True(t, ok)
	require.True(t, channel == nil)
}
