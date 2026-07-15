// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package manualbucketpooltest

import (
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

const testUseLegacySyncDocsIndex = false

// TestDatabaseInitConcurrentDatabasesSameBucket tests InitializeDatabase running for multiple databases sharing a
// single bucket concurrently. Uses initManager callbacks to simulate slow index creation and concurrent init
// requests.
//
// This test lives in the manualbucketpooltest package so that it runs against a dedicated bucket that it creates and
// drops itself, rather than a bucket shared with the rest of the package's tests. That isolation avoids contention on
// the index service (and the associated flakiness) that arose when this test dropped and recreated indexes on a shared
// bucket while other tests were running against it.
func TestDatabaseInitConcurrentDatabasesSameBucket(t *testing.T) {

	rest.RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)
	if sgtest.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - requires bootstrap support")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig, base.KeyQuery)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()
	ctx := base.TestCtx(t)

	// Create a dedicated bucket for this test and its three collections, so we're not contending with other tests
	// on the shared bucket pool's index service.
	tb := base.GTestBucketPool.CreateTestBucket(t)
	defer base.GTestBucketPool.RemoveBucket(tb)
	base.GTestBucketPool.CreateCollections(ctx, tb.Bucket, 3)

	// Set up collection names and ScopesConfig for testing
	scopesConfig := rest.GetCollectionsConfig(t, tb, 3)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)
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
	initMgr.SetTestCallbacks(func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		log.Printf("Collection complete callback invoked for %s %s", dbName, scName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", scName)) // notify the test that indexes have been created for this collection
			rest.WaitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", scName))       // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}, nil)

	db1Name := "db1Name"
	db1Config := makeDbConfig(tb.GetName(), db1Name, collection1and2ScopesConfig)
	require.NoError(t, rest.SetupDbConfigForTest(ctx, &db1Config, db1Name, sc.Config.Bootstrap))
	db1Config.UseSystemMobileMetadataCollection = base.Ptr(true)

	db2Name := "db2Name"
	db2Config := makeDbConfig(tb.GetName(), db2Name, collection3ScopesConfig)
	require.NoError(t, rest.SetupDbConfigForTest(ctx, &db2Config, db2Name, sc.Config.Bootstrap))
	db2Config.UseSystemMobileMetadataCollection = base.Ptr(true)

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, db1Config.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true)
	require.NoError(t, err)

	// Wait for first collection to be initialized
	rest.WaitForChannel(t, firstCollectionInitChannel, "first collection init")

	// Start second async index creation for db2 while first is still running
	doneChan2, err := initMgr.InitializeDatabase(ctx, sc.Config, db2Config.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true)
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Wait for notification on both done channels
	rest.WaitForChannel(t, doneChan1, "modified init done chan")
	rest.WaitForChannel(t, doneChan2, "modified init done chan")

	// Verify initialization/checks were run 7 times total: 3 for db1 and 4 for db2.
	// The distinct collections are _mobile, _default, collection1, collection2, and
	// collection3, but _default and _mobile are checked for both databases.
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(7), totalCount)

}

// makeScopesConfig builds a ScopesConfig for a single scope containing the given collection names.
func makeScopesConfig(scopeName string, collectionNames []string) rest.ScopesConfig {

	collectionsConfig := make(rest.CollectionsConfig)
	for _, collectionName := range collectionNames {
		collectionsConfig[collectionName] = &rest.CollectionConfig{}
	}
	return rest.ScopesConfig{
		scopeName: rest.ScopeConfig{
			Collections: collectionsConfig,
		},
	}
}

// makeDbConfig builds a minimal DbConfig targeting the given bucket, database name and collections.
func makeDbConfig(bucketName string, dbName string, scopesConfig rest.ScopesConfig) rest.DbConfig {
	dbConfig := rest.DbConfig{
		BucketConfig: rest.BucketConfig{
			Bucket: &bucketName,
		},
		Index: &rest.IndexConfig{
			NumReplicas: base.Ptr(uint(0)),
		},
		Scopes: scopesConfig,
	}
	if dbName != "" {
		dbConfig.Name = dbName
	}
	return dbConfig
}

// notifyChannel sends a nil (success) notification on ch, failing the test if it can't be sent within the timeout.
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
	case <-time.After(rest.TestChannelTimeout):
		require.Fail(t, fmt.Sprintf("[%s] unable to send channel notification within %v", message, rest.TestChannelTimeout))
	}
}
