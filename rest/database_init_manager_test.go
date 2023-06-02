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
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestDatabaseInitManager(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	initMgr := sc.DatabaseInitManager

	// Get a test bucket for bootstrap testing, and create dbconfig targeting that bucket
	tb := base.GetTestBucket(t)
	defer func() {
		tb.Close()
	}()
	dbName := "dbName"
	var scopesConfig ScopesConfig
	if base.TestsUseNamedCollections() {
		scopesConfig = GetCollectionsConfig(t, tb, 1)
	}
	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)

	// Drop indexes
	dropAllNonPrimaryIndexes(t, tb.GetSingleDataStore())

	// Async index creation
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
	require.NoError(t, err)

	select {
	case <-doneChan:
		log.Printf("done channel was closed")
		// continue
	case <-time.After(10 * time.Second):
		require.Fail(t, "InitializeDatabase didn't complete in 10s")
	}

}

// TestDatabaseInitConfigChangeSameCollections tests modifications made to the database config while init is running.
// Uses initManager callbacks to simulate slow index creation and build.  Tests the following two scenarios:
//  1. InitalizeDatabase called concurrently for the same collection set, verifies that active init worker is identified and reused
//  2. InitalizeDatabase called after previous InitalizeDatabase completes - verifies that new init worker is started
func TestDatabaseInitConfigChangeSameCollections(t *testing.T) {

	base.TestRequiresCollections(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer func() {
		tb.Close()
	}()
	// Drop all test indexes so we can test InitializeDatabase
	dropAllTestIndexes(t, tb)

	// Set up collection names and ScopesConfig for testing
	scopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1and2ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection2Name})

	initMgr := sc.DatabaseInitManager

	// Use waitChannel to have collectionCallback block, to simulate long-running creation
	testSignalChannel := make(chan error)
	singleCollectionInitChannel := make(chan error)
	expectedCollectionCount := int64(3) // default, collection1, collection2
	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.collectionCompleteCallback = func(dbName, collectionName string) {
		log.Printf("Collection complete callback invoked for %s %s", dbName, collectionName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, singleCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", collectionName)) // notify the test that indexes have been created for this collection
			waitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", collectionName))             // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}

	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, collection1and2ScopesConfig)

	// Start first async index creation, blocks after first collection
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
	require.NoError(t, err)

	// Wait for first collection to be initialized
	waitForChannel(t, singleCollectionInitChannel, "first collection init")

	// Make a duplicate call to initialize database, should reuse the existing agent
	duplicateDoneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
	require.NoError(t, err)

	// Unblock collection callback to process all remaining collections
	close(testSignalChannel)

	// Wait for notification on both done channels
	waitForChannel(t, doneChan, "first init done chan")
	waitForChannel(t, duplicateDoneChan, "duplicate init done chan")

	// Verify initialization was only run for two collections
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, expectedCollectionCount, totalCount)

	waitForWorkerDone(t, initMgr, "dbName")

	// Rerun init, should start a new worker for the database and re-verify init for each collection
	rerunDoneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
	require.NoError(t, err)
	waitForChannel(t, rerunDoneChan, "repeated init done chan")
	totalCount = atomic.LoadInt64(&collectionCount)
	require.Equal(t, expectedCollectionCount*2, totalCount)
}

// TestDatabaseInitConfigChangeDifferentCollections tests modifications made to the database config while init is running.
// Uses initManager callbacks to simulate slow index creation and concurrent init requests.  Tests the following scenario:
//  1. InitalizeDatabase called concurrently with a different collection set, verifies that active init worker is
//     stopped and a new one is started
func TestDatabaseInitConfigChangeDifferentCollections(t *testing.T) {

	base.TestRequiresCollections(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer func() {
		tb.Close()
	}()

	// Drop all test indexes so we can test InitializeDatabase
	dropAllTestIndexes(t, tb)

	// Set up collection names and ScopesConfig for testing
	scopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection3Name := dataStoreNames[2].CollectionName()
	collection1and2ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection2Name})
	collection1and3ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection3Name})

	initMgr := sc.DatabaseInitManager

	// Use waitChannel to have collectionCallback block, to simulate long-running creation
	testSignalChannel := make(chan error)
	firstCollectionInitChannel := make(chan error)

	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.collectionCompleteCallback = func(dbName, collectionName string) {
		log.Printf("Collection complete callback invoked for %s %s", dbName, collectionName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", collectionName)) // notify the test that indexes have been created for this collection
			waitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", collectionName))            // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}

	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, collection1and2ScopesConfig)

	// Start first async index creation, should block after first collection
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
	require.NoError(t, err)

	// Wait for first collection to be initialized
	waitForChannel(t, firstCollectionInitChannel, "first collection init")

	// Make a call to initialize database for the same db name, different collections
	modifiedDbConfig := makeDbConfig(tb.GetName(), dbName, collection1and3ScopesConfig)
	modifiedDoneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, modifiedDbConfig.ToDatabaseConfig())
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Unblock second collection for original invocation
	cancelErr := waitForError(t, doneChan, "first init cancellation")
	require.Error(t, cancelErr)

	// Wait for notification on new done channel
	waitForChannel(t, modifiedDoneChan, "modified init done chan")

	// Verify initialization was run for four collections (one prior to cancellation, three for subsequent init)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(4), totalCount)

}

// TestDatabaseInitConcurrentDatabasesSameBucket tests InitializeDatabase running for multiple databases concurrently.
// Uses initManager callbacks to simulate slow index creation and concurrent init requests.
func TestDatabaseInitConcurrentDatabasesSameBucket(t *testing.T) {

	base.TestRequiresCollections(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer func() {
		tb.Close()
	}()

	// Drop all test indexes so we can test InitializeDatabase
	dropAllTestIndexes(t, tb)

	// Set up collection names and ScopesConfig for testing
	scopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection3Name := dataStoreNames[2].CollectionName()
	collection1and2ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection2Name})
	collection3ScopesConfig := makeScopesConfig(scopeName, []string{collection3Name})

	initMgr := sc.DatabaseInitManager

	// Use waitChannel to have collectionCallback block, to simulate long-running creation
	testSignalChannel := make(chan error)
	firstCollectionInitChannel := make(chan error)

	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.collectionCompleteCallback = func(dbName, collectionName string) {
		log.Printf("Collection complete callback invoked for %s %s", dbName, collectionName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", collectionName)) // notify the test that indexes have been created for this collection
			waitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", collectionName))            // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}

	db1Name := "db1Name"
	db1Config := makeDbConfig(tb.GetName(), db1Name, collection1and2ScopesConfig)

	db2Name := "db2Name"
	db2Config := makeDbConfig(tb.GetName(), db2Name, collection3ScopesConfig)

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, db1Config.ToDatabaseConfig())
	require.NoError(t, err)

	// Wait for first collection to be initialized
	waitForChannel(t, firstCollectionInitChannel, "first collection init")

	// Start second async index creation for db2 while first is still running
	doneChan2, err := initMgr.InitializeDatabase(ctx, sc.Config, db2Config.ToDatabaseConfig())
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Wait for notification on both done channels
	waitForChannel(t, doneChan1, "modified init done chan")
	waitForChannel(t, doneChan2, "modified init done chan")

	// Verify initialization was run for 5 collections (three for db1, two for db2)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(5), totalCount)

}

// TestDatabaseInitConcurrentDatabasesDifferentBuckets tests InitializeDatabase running for multiple databases concurrently.
// Uses initManager callbacks to simulate slow index creation and concurrent init requests.
func TestDatabaseInitConcurrentDatabasesDifferentBuckets(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.TestRequiresCollections(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	// Get two test buckets for bootstrap testing, and drop indexes created by bucket pool readier
	tb1 := base.GetTestBucket(t)
	defer func() {
		tb1.Close()
	}()

	// Drop all test indexes so we can test InitializeDatabase
	dropAllTestIndexes(t, tb1)

	// Get two test buckets for bootstrap testing, and drop indexes created by bucket pool readier
	tb2 := base.GetTestBucket(t)
	defer func() {
		tb2.Close()
	}()

	// Drop all test indexes so we can test InitializeDatabase
	dropAllTestIndexes(t, tb2)

	// Set up collection names and ScopesConfig for testing - use same collections for both buckets
	scopesConfig := GetCollectionsConfig(t, tb1, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1and2ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection2Name})

	initMgr := sc.DatabaseInitManager

	// Use waitChannel to have collectionCallback block, to simulate long-running creation
	testSignalChannel := make(chan error)
	firstCollectionInitChannel := make(chan error)
	databaseCompleteChannel := make(chan error)

	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.collectionCompleteCallback = func(dbName, collectionName string) {
		log.Printf("Collection complete callback invoked for %s %s", dbName, collectionName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", collectionName)) // notify the test that indexes have been created for this collection
			waitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", collectionName))            // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}
	initMgr.databaseCompleteCallback = func(dbName string) {
		notifyChannel(t, databaseCompleteChannel, "database complete")
	}

	db1Name := "db1Name"
	db1Config := makeDbConfig(tb1.GetName(), db1Name, collection1and2ScopesConfig)

	db2Name := "db2Name"
	db2Config := makeDbConfig(tb2.GetName(), db2Name, collection1and2ScopesConfig)

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, db1Config.ToDatabaseConfig())
	require.NoError(t, err)

	// Wait for first collection to be initialized
	waitForChannel(t, firstCollectionInitChannel, "first collection init")

	// Start second async index creation for db2 while first is still running
	doneChan2, err := initMgr.InitializeDatabase(ctx, sc.Config, db2Config.ToDatabaseConfig())
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Wait for notification on both done channels
	waitForChannel(t, doneChan1, "modified init done chan")
	waitForChannel(t, doneChan2, "modified init done chan")

	// Wait for db completion notifications for both databases
	waitForChannel(t, databaseCompleteChannel, "database 1 init complete")
	waitForChannel(t, databaseCompleteChannel, "database 2 init complete")

	// Verify initialization was run for 6 collections (three for db1, three for db2)
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(6), totalCount)

}

// TestDatabaseInitTeardownTiming tests scenarios where InitializeDatabase is called during
// the completion phase of a previous async initialization.  Ensures there are no cases where a
// watcher is added but never receives a done notification.
func TestDatabaseInitTeardownTiming(t *testing.T) {

	base.TestRequiresCollections(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer func() {
		tb.Close()
	}()

	// Drop all test indexes so we can test InitializeDatabase
	dropAllTestIndexes(t, tb)

	// Set up collection names and ScopesConfig for testing
	scopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1and2ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection2Name})

	initMgr := sc.DatabaseInitManager

	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.collectionCompleteCallback = func(dbName, collectionName string) {
		atomic.AddInt64(&collectionCount, 1)
	}
	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, collection1and2ScopesConfig)

	var doneChan2 chan error
	databaseCompleteCount := int64(0)
	initMgr.databaseCompleteCallback = func(dbName string) {
		// On first completion, invoke InitializeDatabase with the same collection set post-completion
		currentCount := atomic.LoadInt64(&databaseCompleteCount)
		if currentCount == 0 {
			log.Printf("invoking InitializeDatabase again during teardown")
			var err error
			doneChan2, err = initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
			require.NoError(t, err)
		}
		atomic.AddInt64(&databaseCompleteCount, 1)

	}

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig())
	require.NoError(t, err)

	waitForChannel(t, doneChan1, "done chan 1")
	waitForChannel(t, doneChan2, "done chan 2")

	// Verify initialization was run for 3 collections only
	totalCollectionInitCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(3), totalCollectionInitCount)

	// Expect only a single database complete callback, since init should only have been run once.
	totalDbCompleteCount := atomic.LoadInt64(&databaseCompleteCount)
	require.Equal(t, int64(1), totalDbCompleteCount)

}

// testChannelTimeout can be increased to support step-through debugging
const testChannelTimeout = 10 * time.Second

func waitForChannel(t *testing.T, ch <-chan error, message string) {
	if message != "" {
		log.Printf("[%s] starting wait", message)
		defer func() {
			log.Printf("[%s] completed wait", message)
		}()
	}
	select {
	case err := <-ch:
		if err != nil {
			require.Fail(t, fmt.Sprintf("[%s] channel returned error: %v", message, err))
		}
		return
	case <-time.After(testChannelTimeout):
		require.Fail(t, fmt.Sprintf("[%s] expected channel message did not arrive in 10s", message))
	}
}

func waitForError(t *testing.T, ch <-chan error, message string) error {
	if message != "" {
		log.Printf("[%s] starting wait for error", message)
		defer func() {
			log.Printf("[%s] completed wait for error", message)
		}()
	}
	select {
	case err := <-ch:
		if err == nil {
			require.Fail(t, "[%s] Received non-error message on channel", message)
		}
		return err
	case <-time.After(testChannelTimeout):
		require.Fail(t, fmt.Sprintf("[%s] expected error message did not arrive in 10s", message))
		return nil
	}
}

func notifyChannel(t *testing.T, ch chan<- error, message string) {
	if message != "" {
		log.Printf("[%s] starting notify", message)
		defer func() {
			log.Printf("[%s] completed notify", message)
		}()
	}
	select {
	case ch <- nil:
		return
	case <-time.After(testChannelTimeout):
		require.Fail(t, fmt.Sprintf("[%s] unable to send channel notification within 10s", message))
	}
}

func dropAllTestIndexes(t *testing.T, tb *base.TestBucket) {
	dropAllNonPrimaryIndexes(t, tb.GetMetadataStore())

	dsNames := tb.GetNonDefaultDatastoreNames()
	for i := 0; i < len(dsNames); i++ {
		ds, err := tb.GetNamedDataStore(i)
		require.NoError(t, err)
		dropAllNonPrimaryIndexes(t, ds)
	}
}

// Calls DropAllIndexes to remove all indexes, then restores the primary index for TestBucketPool readier requirements
func dropAllNonPrimaryIndexes(t *testing.T, dataStore base.DataStore) {

	n1qlStore, ok := base.AsN1QLStore(dataStore)
	require.True(t, ok)
	ctx := base.TestCtx(t)
	dropErr := base.DropAllIndexes(ctx, n1qlStore)
	require.NoError(t, dropErr)
	err := n1qlStore.CreatePrimaryIndex(base.PrimaryIndexName, nil)
	require.NoError(t, err, "Unable to recreate primary index")
}

func makeScopesConfig(scopeName string, collectionNames []string) ScopesConfig {

	collectionsConfig := make(CollectionsConfig)
	for _, collectionName := range collectionNames {
		collectionsConfig[collectionName] = CollectionConfig{}
	}
	return ScopesConfig{
		scopeName: ScopeConfig{
			Collections: collectionsConfig,
		},
	}
}

// waitForWorkerDone avoids races when testing db initializations performed serially
func waitForWorkerDone(t *testing.T, manager *DatabaseInitManager, dbName string) {
	for i := 0; i < 1000; i++ {
		if !manager.HasActiveInitialization(dbName) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Worker did not complete in expected time interval for db %s", dbName)
}
