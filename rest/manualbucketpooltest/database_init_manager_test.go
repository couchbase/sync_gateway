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
	"sort"
	"strings"
	"sync"
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

	ctx := base.TestCtx(t)

	// Create a dedicated bucket for this test and its three collections, so we're not contending with other tests
	// on the shared bucket pool's index service.  Created before the server so that it is dropped after closeFn has
	// stopped the init workers - otherwise they keep retrying index creation against a bucket that no longer exists.
	tb := base.GTestBucketPool.CreateTestBucket(t)
	defer base.GTestBucketPool.RemoveBucket(tb)
	base.GTestBucketPool.CreateCollections(ctx, tb.Bucket, 3)

	sc, closeFn := rest.StartBootstrapServer(t)
	defer closeFn()

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

	// Track the latest status reported for every collection so that a done-channel timeout below can report which
	// collection on which database was still in progress (see initProgressTracker).
	progress := newInitProgressTracker()

	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.SetTestCallbacks(func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		progress.record(dbName, scName, status)
		if status != db.CollectionIndexStatusReady {
			return
		}
		log.Printf("Collection complete callback invoked for %s %s", dbName, scName)
		if atomic.CompareAndSwapInt64(&collectionCount, 0, 1) {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", scName)) // notify the test that indexes have been created for this collection
			rest.WaitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", scName))       // wait for the test to unblock before proceeding to the next collection
			return
		}
		atomic.AddInt64(&collectionCount, 1)
	}, nil)

	db1Name := "db1Name"
	db1Config := makeDbConfig(tb.GetName(), db1Name, collection1and2ScopesConfig)
	db1Config.UseSystemMobileMetadataCollection = base.Ptr(true)
	require.NoError(t, rest.SetupDbConfigForTest(ctx, &db1Config, db1Name, sc.Config.Bootstrap))

	db2Name := "db2Name"
	db2Config := makeDbConfig(tb.GetName(), db2Name, collection3ScopesConfig)
	db2Config.UseSystemMobileMetadataCollection = base.Ptr(true)
	require.NoError(t, rest.SetupDbConfigForTest(ctx, &db2Config, db2Name, sc.Config.Bootstrap))

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, db1Config.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Wait for first collection to be initialized
	rest.WaitForChannelWithTimeout(t, firstCollectionInitChannel, "first collection init", rest.TestIndexInitTimeout, progress.snapshot)

	// Start second async index creation for db2 while first is still running
	doneChan2, err := initMgr.InitializeDatabase(ctx, sc.Config, db2Config.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Wait for notification on both done channels.  Each one covers all the remaining collections for that database,
	// so these waits use TestIndexInitTimeout.  On timeout, progress.snapshot() reports the latest status recorded
	// for each collection.
	rest.WaitForChannelWithTimeout(t, doneChan1, "db1 InitializeDatabase done chan", rest.TestIndexInitTimeout, progress.snapshot)
	rest.WaitForChannelWithTimeout(t, doneChan2, "db2 InitializeDatabase done chan", rest.TestIndexInitTimeout, progress.snapshot)

	// Verify initialization/checks were run 7 times total: 3 for db1 and 4 for db2.
	// The distinct collections are _mobile, _default, collection1, collection2, and
	// collection3, but _default and _mobile are checked for both databases.
	// On mismatch, snapshot() lists the latest status for every collection, so any
	// collection not showing Ready (or missing entirely) names the culprit.
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equalf(t, int64(7), totalCount,
		"expected 7 collection initializations (db1: _default, _mobile, %s, %s; db2: _default, _mobile, %s) but got %d. %s",
		collection1Name, collection2Name, collection3Name, totalCount, progress.snapshot())

}

// initProgressTracker records the most recent CollectionIndexStatus reported for each (database, collection) pair via
// the DatabaseInitManager test callback. Init workers for different databases invoke the callback from separate
// goroutines, so access is guarded by a mutex. On a wait timeout, snapshot() renders the recorded state so the failure
// names which collection on which database was still in progress - for example a collection stuck "in progress" on a
// concurrent BUILD INDEX of a shared keyspace - rather than a static guess at the cause.
type initProgressTracker struct {
	mu       sync.Mutex
	statuses map[string]map[base.ScopeAndCollectionName]db.CollectionIndexStatus // dbName -> collection -> latest status
}

func newInitProgressTracker() *initProgressTracker {
	return &initProgressTracker{
		statuses: make(map[string]map[base.ScopeAndCollectionName]db.CollectionIndexStatus),
	}
}

// record stores the latest status reported for a collection on a database.
func (p *initProgressTracker) record(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statuses[dbName] == nil {
		p.statuses[dbName] = make(map[base.ScopeAndCollectionName]db.CollectionIndexStatus)
	}
	p.statuses[dbName][scName] = status
}

// snapshot renders the recorded per-collection statuses as a stable, human-readable string, one database per line.
func (p *initProgressTracker) snapshot() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.statuses) == 0 {
		return "init progress: no collection status callbacks recorded yet"
	}
	dbNames := make([]string, 0, len(p.statuses))
	for dbName := range p.statuses {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)

	var sb strings.Builder
	sb.WriteString("init progress (latest status per collection):")
	for _, dbName := range dbNames {
		statusByName := make(map[string]db.CollectionIndexStatus, len(p.statuses[dbName]))
		scNames := make([]string, 0, len(p.statuses[dbName]))
		for scName, status := range p.statuses[dbName] {
			statusByName[scName.String()] = status
			scNames = append(scNames, scName.String())
		}
		sort.Strings(scNames)
		parts := make([]string, 0, len(scNames))
		for _, scName := range scNames {
			parts = append(parts, fmt.Sprintf("%s=%s", scName, statusByName[scName]))
		}
		sb.WriteString(fmt.Sprintf("\n  %s: %s", dbName, strings.Join(parts, ", ")))
	}
	return sb.String()
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
