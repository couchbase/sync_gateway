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
	"maps"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

const testUseLegacySyncDocsIndex = false

func TestDatabaseInitManager(t *testing.T) {
	RequireN1QLIndexes(t)

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	initMgr := sc.DatabaseInitManager

	ctx := base.TestCtx(t)
	// Get a test bucket for bootstrap testing, and create dbconfig targeting that bucket
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	dbName := "dbName"
	var scopesConfig ScopesConfig
	if base.TestsUseNamedCollections() {
		scopesConfig = GetCollectionsConfig(t, tb, 1)
	}
	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
	require.NoError(t, dbConfig.setup(ctx, dbName, sc.Config.Bootstrap, nil, nil))

	// Drop indexes
	base.DropAllBucketIndexes(t, tb)

	// Async index creation
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	select {
	case err := <-doneChan:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.Fail(t, "InitializeDatabase didn't complete in 10s")
	}

}

// TestDatabaseInitPostMigrationExcludesDefault verifies that once a system-metadata database has completed its
// metadata migration (migrationComplete=true), DatabaseInitManager does not initialize indexes on
// _default._default — its metadata indexes there are vestigial, and building them would be wasteful (or fail if
// the customer has since dropped the collection). This is the DatabaseInitManager-level counterpart to the
// TestBuildCollectionIndexData "migration complete" cases.
func TestDatabaseInitPostMigrationExcludesDefault(t *testing.T) {
	RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// Drop all test indexes so we can test InitializeDatabase
	base.DropAllBucketIndexes(t, tb)

	// Two named data collections with system metadata opted in; _default is NOT a configured collection.
	scopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)

	initMgr := sc.DatabaseInitManager

	// Record every collection the manager initializes.
	var seenLock sync.Mutex
	seen := make(map[base.ScopeAndCollectionName]struct{})
	initMgr.testCollectionStatusUpdateCallback = func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		seenLock.Lock()
		defer seenLock.Unlock()
		seen[scName] = struct{}{}
	}

	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	require.NoError(t, dbConfig.setup(ctx, dbName, sc.Config.Bootstrap, nil, nil))

	// migrationComplete=true models a system-metadata database that has finished migrating off _default.
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, true)
	require.NoError(t, err)
	WaitForChannel(t, doneChan, "post-migration init done chan")

	seenLock.Lock()
	defer seenLock.Unlock()

	// _default must be excluded: its metadata indexes are vestigial post-migration.
	_, defaultSeen := seen[base.DefaultScopeAndCollectionName()]
	require.False(t, defaultSeen, "expected _default._default to be excluded from index init post-migration, got collections: %v", seen)

	// The metadata collection and both configured data collections must still be initialized.
	expected := []base.ScopeAndCollectionName{
		base.MobileSystemScopeAndCollectionName(),
		{Scope: dataStoreNames[0].ScopeName(), Collection: dataStoreNames[0].CollectionName()},
		{Scope: dataStoreNames[1].ScopeName(), Collection: dataStoreNames[1].CollectionName()},
	}
	for _, scName := range expected {
		_, ok := seen[scName]
		require.True(t, ok, "expected collection %s to be initialized, got collections: %v", scName, seen)
	}
	require.Len(t, seen, len(expected), "unexpected collection set: %v", seen)
}

// TestDatabaseInitCollectionsForMetadataStoreMode:
// _default._default is deliberately never a configured data collection here, so it is only ever a
// candidate for index initialization in its metadata role:
//  1. system metadata collection opted in, no legacy metadata in _default._default (fresh deployment, so
//     the database is migration-complete from the outset) - metadata indexes belong on _system._mobile only
//  2. system metadata collection opted in, legacy metadata still in _default._default - migration is still
//     required, so _default remains a dual-read fallback and needs its metadata indexes too
//  3. not opted in - _default._default *is* the metadata store, and _system._mobile is unused
func TestDatabaseInitCollectionsForMetadataStoreMode(t *testing.T) {
	RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)
	base.LongRunningTest(t)

	const dbName = "testdb"

	testCases := []struct {
		name                        string
		useSystemMetadataCollection bool
		legacyMetadataInDefault     bool // seed legacy per-DB metadata in _default._default, so migration is still required
		expectMobileCollectionInit  bool
		expectDefaultCollectionInit bool
	}{
		{
			name:                        "system metadata collection, migration complete",
			useSystemMetadataCollection: true,
			legacyMetadataInDefault:     false,
			expectMobileCollectionInit:  true,
			expectDefaultCollectionInit: false,
		},
		{
			name:                        "system metadata collection, migration required",
			useSystemMetadataCollection: true,
			legacyMetadataInDefault:     true,
			expectMobileCollectionInit:  true,
			expectDefaultCollectionInit: true,
		},
		{
			name:                        "legacy metadata collection",
			useSystemMetadataCollection: false,
			legacyMetadataInDefault:     false,
			expectMobileCollectionInit:  false,
			expectDefaultCollectionInit: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)

			sc, closeFn := StartBootstrapServer(t)
			defer closeFn()

			// Drop the indexes created by the bucket pool, so the indexes found in the bucket afterwards
			// are only the ones this database's initialization built.
			base.DropAllBucketIndexes(t, tb)
			dropSystemCollectionMetadataIndexes(t, tb)

			// Two named data collections - _default._default is not a configured collection.
			scopesConfig := GetCollectionsConfig(t, tb, 2)
			dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)

			// The metadata ID the database will be created with, since its config doesn't include
			// _default._default. Asserted after creation, because the legacy metadata seeded below is
			// keyed by it.
			metadataID := sc.BootstrapContext.standardMetadataID(dbName)
			if testCase.legacyMetadataInDefault {
				// Model a database that predates the system metadata collection. The per-DB sequence
				// counter in _default._default is what the database load probes to decide whether the
				// metadata store still needs _default as a read fallback.
				_, err := tb.DefaultDataStore(ctx).Incr(ctx, base.NewMetadataKeys(metadataID).SyncSeqKey(), 1, 1, 0)
				require.NoError(t, err)
			}

			// Record every collection index initialization completes for.
			var seenLock sync.Mutex
			seen := make(map[base.ScopeAndCollectionName]struct{})
			sc.DatabaseInitManager.testCollectionStatusUpdateCallback = func(_ string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
				if status != db.CollectionIndexStatusReady {
					return
				}
				seenLock.Lock()
				defer seenLock.Unlock()
				seen[scName] = struct{}{}
			}

			dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
			dbConfig.UseSystemMobileMetadataCollection = base.Ptr(testCase.useSystemMetadataCollection)

			// Index initialization runs synchronously for this create (the database isn't started
			// offline and no initialization is already in flight), so it has completed for every
			// collection by the time the request returns.
			resp := BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(base.MustJSONMarshal(t, dbConfig)))
			resp.RequireStatus(http.StatusCreated)

			require.Equal(t, metadataID, sc.GetDatabaseConfig(dbName).MetadataID,
				"metadata ID assigned at database creation must match the one the test derives legacy metadata keys from")

			expected := []base.ScopeAndCollectionName{
				{Scope: dataStoreNames[0].ScopeName(), Collection: dataStoreNames[0].CollectionName()},
				{Scope: dataStoreNames[1].ScopeName(), Collection: dataStoreNames[1].CollectionName()},
			}
			if testCase.expectMobileCollectionInit {
				expected = append(expected, base.MobileSystemScopeAndCollectionName())
			}
			if testCase.expectDefaultCollectionInit {
				expected = append(expected, base.DefaultScopeAndCollectionName())
			}

			seenLock.Lock()
			defer seenLock.Unlock()
			require.ElementsMatch(t, expected, slices.Collect(maps.Keys(seen)), "unexpected set of initialized collections")

			// Corroborate the collection set against the indexes actually in the bucket. _default isn't a
			// configured data collection in any of these cases, so any index in it is a metadata index.
			dataStore, err := tb.NamedDataStore(base.TestCtx(t), base.DefaultScopeAndCollectionName())
			require.NoError(t, err)
			n1qlStore, ok := base.AsN1QLStore(dataStore)
			require.True(t, ok, "expected %s to be a N1QLStore, got %T", base.DefaultScopeAndCollectionName(), dataStore)
			indexes, err := n1qlStore.GetIndexes()
			require.NoError(t, err)
			if testCase.expectDefaultCollectionInit {
				require.NotEmpty(t, indexes, "expected indexes to have been built on %s", base.DefaultScopeAndCollectionName())
			} else {
				require.Empty(t, indexes, "expected no indexes to have been built on %s", base.DefaultScopeAndCollectionName())
			}
			RequireSystemCollectionHasMetadataIndexes(t, tb, testCase.expectMobileCollectionInit)
		})
	}
}

// TestDatabaseInitDefaultCollectionForMetadataStoreMode:
//  1. system metadata collection opted in, no legacy metadata in _default._default (fresh deployment,
//     so the database is migration-complete from the outset) - _default is a data collection only. So it gets
//     the data indexes and _system._mobile gets the metadata indexes
//  2. system metadata collection opted in, legacy metadata still in _default._default - migration is
//     still required, so _default is both the data collection and a metadata dual-read fallback, and
//     needs the full index set alongside _system._mobile's metadata indexes
//  3. not opted in - _default._default is both the data collection and the metadata store, so it needs
//     the full index set, and _system._mobile is unused
func TestDatabaseInitDefaultCollectionForMetadataStoreMode(t *testing.T) {
	RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)
	base.LongRunningTest(t)

	const dbName = "testdb"

	testCases := []struct {
		name                        string
		useSystemMetadataCollection bool
		legacyMetadataInDefault     bool // seed legacy per-DB metadata in _default._default, so migration is still required
		expectMobileCollectionInit  bool
		expectedDefaultIndexes      db.CollectionIndexesType
	}{
		{
			name:                        "system metadata collection, migration complete",
			useSystemMetadataCollection: true,
			legacyMetadataInDefault:     false,
			expectMobileCollectionInit:  true,
			expectedDefaultIndexes:      db.IndexesWithoutMetadata,
		},
		{
			name:                        "system metadata collection, migration required",
			useSystemMetadataCollection: true,
			legacyMetadataInDefault:     true,
			expectMobileCollectionInit:  true,
			expectedDefaultIndexes:      db.IndexesAll,
		},
		{
			name:                        "legacy metadata collection",
			useSystemMetadataCollection: false,
			legacyMetadataInDefault:     false,
			expectMobileCollectionInit:  false,
			expectedDefaultIndexes:      db.IndexesAll,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)

			sc, closeFn := StartBootstrapServer(t)
			defer closeFn()

			// Drop the indexes created by the bucket pool, so the indexes found in the bucket afterwards
			// are only the ones this database's initialization built. It also leaves no principal indexes
			// online anywhere, so the database resolves to the separate users/roles indexes rather than
			// the legacy syncDocs index - which is what the expected index sets below are computed with.
			base.DropAllBucketIndexes(t, tb)
			dropSystemCollectionMetadataIndexes(t, tb)

			if testCase.legacyMetadataInDefault {
				// Model a database that predates the system metadata collection. The per-DB sequence
				// counter in _default._default is what the database load probes to decide whether the
				// metadata store still needs _default as a read fallback. A database that includes
				// _default._default gets the default (unprefixed) metadata ID, asserted after creation.
				_, err := tb.DefaultDataStore(ctx).Incr(ctx, base.NewMetadataKeys(base.DefaultMetadataID).SyncSeqKey(), 1, 1, 0)
				require.NoError(t, err)
			}

			// Record every collection index initialization completes for.
			var seenLock sync.Mutex
			seen := make(map[base.ScopeAndCollectionName]struct{})
			sc.DatabaseInitManager.testCollectionStatusUpdateCallback = func(_ string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
				if status != db.CollectionIndexStatusReady {
					return
				}
				seenLock.Lock()
				defer seenLock.Unlock()
				seen[scName] = struct{}{}
			}

			// No scopes in the config - _default._default is the database's only data collection.
			dbConfig := makeDbConfig(tb.GetName(), dbName, nil)
			dbConfig.UseSystemMobileMetadataCollection = base.Ptr(testCase.useSystemMetadataCollection)

			// Index initialization runs synchronously for this create (the database isn't started
			// offline and no initialization is already in flight), so it has completed for every
			// collection by the time the request returns.
			resp := BootstrapAdminRequest(t, sc, http.MethodPut, "/"+dbName+"/", string(base.MustJSONMarshal(t, dbConfig)))
			resp.RequireStatus(http.StatusCreated)

			require.Equal(t, base.DefaultMetadataID, sc.GetDatabaseConfig(dbName).MetadataID,
				"a database including _default._default must be assigned the default metadata ID, which the legacy metadata seeded by this test is keyed by")

			expected := []base.ScopeAndCollectionName{base.DefaultScopeAndCollectionName()}
			if testCase.expectMobileCollectionInit {
				expected = append(expected, base.MobileSystemScopeAndCollectionName())
			}

			seenLock.Lock()
			defer seenLock.Unlock()
			require.ElementsMatch(t, expected, slices.Collect(maps.Keys(seen)), "unexpected set of initialized collections")

			// The collection set is the same for cases 2 and 3, so the index sets in the bucket are what
			// actually separate the permutations - whether _default carries the metadata (principal)
			// indexes on top of the data indexes it needs as a configured collection.
			requireDefaultCollectionIndexes(t, tb, testCase.expectedDefaultIndexes)
			RequireSystemCollectionHasMetadataIndexes(t, tb, testCase.expectMobileCollectionInit)
		})
	}
}

// requireDefaultCollectionIndexes asserts that the indexes present on _default._default are exactly
// those implied by indexesType, derived from the index definitions so the expectation tracks changes to
// the index set. IndexesAll is the data indexes plus the metadata (principal) indexes;
// IndexesWithoutMetadata is the data indexes alone.
func requireDefaultCollectionIndexes(t *testing.T, tb *base.TestBucket, indexesType db.CollectionIndexesType) {
	t.Helper()
	dataStore, err := tb.NamedDataStore(base.TestCtx(t), base.DefaultScopeAndCollectionName())
	require.NoError(t, err)
	n1qlStore, ok := base.AsN1QLStore(dataStore)
	require.True(t, ok, "expected %s to be a N1QLStore, got %T", base.DefaultScopeAndCollectionName(), dataStore)
	indexes, err := n1qlStore.GetIndexes()
	require.NoError(t, err)
	expected := db.GetIndexNames(db.InitializeIndexOptions{
		MetadataIndexes:     indexesType,
		NumPartitions:       db.DefaultNumIndexPartitions,
		LegacySyncDocsIndex: testUseLegacySyncDocsIndex,
	}, db.GetSGIndexes())
	require.ElementsMatch(t, expected, indexes, "unexpected indexes on %s", base.DefaultScopeAndCollectionName())
}

// RequireSystemCollectionHasMetadataIndexes asserts whether the metadata indexes are present on
// _system._mobile. CBS omits system-scope collections from system:indexes, so this goes via
// system:all_indexes rather than N1QLStore.GetIndexes, which reports nothing for those collections.
func RequireSystemCollectionHasMetadataIndexes(t *testing.T, tb *base.TestBucket, expectIndexes bool) {
	t.Helper()
	gocbBucket, err := base.AsGocbV2Bucket(tb.Bucket)
	require.NoError(t, err)
	n1qlStore, err := base.NewClusterOnlyN1QLStore(gocbBucket.GetCluster(), gocbBucket.BucketName(), base.SystemScope, base.SystemCollectionMobile)
	require.NoError(t, err)
	indexesMeta, err := base.GetSystemCollectionIndexesMeta(base.TestCtx(t), n1qlStore, base.SystemScope, base.SystemCollectionMobile, []string{"sg_users_x1", "sg_roles_x1"})
	require.NoError(t, err)
	if expectIndexes {
		base.RequireKeysEqual(t, []string{"sg_users_x1", "sg_roles_x1"}, indexesMeta, "unexpected metadata indexes on %s", base.MobileSystemScopeAndCollectionName())
	} else {
		require.Empty(t, indexesMeta, "expected no metadata indexes on %s", base.MobileSystemScopeAndCollectionName())
	}
}

// dropSystemCollectionMetadataIndexes drops the metadata indexes the bucket pool builds on
// _system._mobile. base.DropAllBucketIndexes leaves them in place because it enumerates via
// N1QLStore.GetIndexes, which doesn't see system-scope collections - to be fixed by CBG-5585, after
// which this can go away.
func dropSystemCollectionMetadataIndexes(t *testing.T, tb *base.TestBucket) {
	t.Helper()
	ctx := base.TestCtx(t)
	gocbBucket, err := base.AsGocbV2Bucket(tb.Bucket)
	require.NoError(t, err)
	n1qlStore, err := base.NewClusterOnlyN1QLStore(gocbBucket.GetCluster(), gocbBucket.BucketName(), base.SystemScope, base.SystemCollectionMobile)
	require.NoError(t, err)
	for _, indexName := range []string{"sg_users_x1", "sg_roles_x1"} {
		if err := base.DropIndex(ctx, n1qlStore, indexName); err != nil && !base.IsIndexNotFoundError(err) {
			require.NoError(t, err, "error dropping index %s on %s", indexName, base.MobileSystemScopeAndCollectionName())
		}
	}
	// DROP INDEX is asynchronous - wait for the drops to land, so the index initialization under test
	// doesn't skip creating an index that's about to disappear.
	require.Eventually(t, func() bool {
		indexesMeta, err := base.GetSystemCollectionIndexesMeta(ctx, n1qlStore, base.SystemScope, base.SystemCollectionMobile, []string{"sg_users_x1", "sg_roles_x1"})
		return err == nil && len(indexesMeta) == 0
	}, 30*time.Second, 100*time.Millisecond, "metadata indexes on %s were not dropped", base.MobileSystemScopeAndCollectionName())
}

// TestDatabaseInitConfigChangeSameCollections tests modifications made to the database config while init is running.
// Uses initManager callbacks to simulate slow index creation and build.  Tests the following two scenarios:
//  1. InitalizeDatabase called concurrently for the same collection set, verifies that active init worker is identified and reused
//  2. InitalizeDatabase called after previous InitalizeDatabase completes - verifies that new init worker is started
func TestDatabaseInitConfigChangeSameCollections(t *testing.T) {
	RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)
	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	ctx := base.TestCtx(t)

	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	// Drop all test indexes so we can test InitializeDatabase
	base.DropAllBucketIndexes(t, tb)

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
	expectedCollectionCount := int64(4) // _mobile, _default, collection1, collection2
	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	collectionCount := int64(0)
	initMgr.testCollectionStatusUpdateCallback = func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		log.Printf("Collection complete callback invoked for %s %s", dbName, scName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, singleCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", scName)) // notify the test that indexes have been created for this collection
			WaitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", scName))             // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}

	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, collection1and2ScopesConfig)
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	require.NoError(t, dbConfig.setup(ctx, dbName, sc.Config.Bootstrap, nil, nil))

	// Start first async index creation, blocks after first collection
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Wait for first collection to be initialized
	WaitForChannel(t, singleCollectionInitChannel, "first collection init")

	// Make a duplicate call to initialize database, should reuse the existing agent
	duplicateDoneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Unblock collection callback to process all remaining collections
	close(testSignalChannel)

	// Wait for notification on both done channels
	WaitForChannel(t, doneChan, "first init done chan")
	WaitForChannel(t, duplicateDoneChan, "duplicate init done chan")

	// Verify initialization was only run for two collections
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, expectedCollectionCount, totalCount)

	waitForWorkerDone(t, initMgr, "dbName")

	// Rerun init, should start a new worker for the database and re-verify init for each collection
	rerunDoneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)
	WaitForChannel(t, rerunDoneChan, "repeated init done chan")
	totalCount = atomic.LoadInt64(&collectionCount)
	require.Equal(t, expectedCollectionCount*2, totalCount)
}

// TestDatabaseInitConfigChangeDifferentCollections tests modifications made to the database config while init is running.
// Uses initManager callbacks to simulate slow index creation and concurrent init requests.  Tests the following scenario:
//  1. InitalizeDatabase called concurrently with a different collection set, verifies that active init worker is
//     stopped and a new one is started
func TestDatabaseInitConfigChangeDifferentCollections(t *testing.T) {

	base.TestRequiresCollections(t)
	RequireN1QLIndexes(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	base.TestRequiresCollections(t)
	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()

	ctx := base.TestCtx(t)
	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// Drop all test indexes so we can test InitializeDatabase
	base.DropAllBucketIndexes(t, tb)

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
	initMgr.testCollectionStatusUpdateCallback = func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		log.Printf("Collection complete callback invoked for %s %s", dbName, scName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", scName.CollectionName())) // notify the test that indexes have been created for this collection
			WaitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", scName.CollectionName()))            // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}

	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, collection1and2ScopesConfig)
	require.NoError(t, dbConfig.setup(ctx, dbName, sc.Config.Bootstrap, nil, nil))
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)

	// Start first async index creation, should block after first collection
	doneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Wait for first collection to be initialized
	WaitForChannel(t, firstCollectionInitChannel, "first collection init")

	// Make a call to initialize database for the same db name, different collections
	modifiedDbConfig := makeDbConfig(tb.GetName(), dbName, collection1and3ScopesConfig)
	require.NoError(t, modifiedDbConfig.setup(ctx, dbName, sc.Config.Bootstrap, nil, nil))
	modifiedDbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	modifiedDoneChan, err := initMgr.InitializeDatabase(ctx, sc.Config, modifiedDbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Unblock second collection for original invocation
	cancelErr := waitForError(t, doneChan, "first init cancellation")
	require.Error(t, cancelErr)

	// Wait for notification on new done channel
	WaitForChannel(t, modifiedDoneChan, "modified init done chan")

	// Verify initialization was run for four collections (one prior to cancellation, three for subsequent init)
	totalCount := atomic.LoadInt64(&collectionCount)
	// _mobile, _default, collection1, collection2, collection3
	require.Equal(t, int64(5), totalCount)

}

// TestDatabaseInitConcurrentDatabasesSameBucket tests InitializeDatabase running for multiple databases concurrently.
// Uses initManager callbacks to simulate slow index creation and concurrent init requests.
// TestDatabaseInitConcurrentDatabasesDifferentBuckets tests InitializeDatabase running for multiple databases concurrently.
// Uses initManager callbacks to simulate slow index creation and concurrent init requests.
func TestDatabaseInitConcurrentDatabasesDifferentBuckets(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	// Start SG with no databases
	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()
	ctx := base.TestCtx(t)

	// Get two test buckets for bootstrap testing, and drop indexes created by bucket pool readier
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)

	// Drop all test indexes so we can test InitializeDatabase
	base.DropAllBucketIndexes(t, tb1)

	// Get two test buckets for bootstrap testing, and drop indexes created by bucket pool readier
	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)

	// Drop all test indexes so we can test InitializeDatabase
	base.DropAllBucketIndexes(t, tb2)

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
	initMgr.testCollectionStatusUpdateCallback = func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		log.Printf("Collection complete callback invoked for %s %s", dbName, scName)
		currentCount := atomic.LoadInt64(&collectionCount)
		if currentCount == 0 {
			notifyChannel(t, firstCollectionInitChannel, fmt.Sprintf("singleCollectionInit-%s", scName)) // notify the test that indexes have been created for this collection
			WaitForChannel(t, testSignalChannel, fmt.Sprintf("testSignalChannel-%s", scName))            // wait for the test to unblock before proceeding to the next collection
		}
		atomic.AddInt64(&collectionCount, 1)
	}
	initMgr.testDatabaseCompleteCallback = func(dbName string) {
		notifyChannel(t, databaseCompleteChannel, "database complete")
	}

	db1Name := "db1Name"
	db1Config := makeDbConfig(tb1.GetName(), db1Name, collection1and2ScopesConfig)
	require.NoError(t, db1Config.setup(ctx, db1Name, sc.Config.Bootstrap, nil, nil))
	db1Config.UseSystemMobileMetadataCollection = base.Ptr(true)

	db2Name := "db2Name"
	db2Config := makeDbConfig(tb2.GetName(), db2Name, collection1and2ScopesConfig)
	require.NoError(t, db2Config.setup(ctx, db2Name, sc.Config.Bootstrap, nil, nil))
	db2Config.UseSystemMobileMetadataCollection = base.Ptr(true)

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, db1Config.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Wait for first collection to be initialized
	WaitForChannel(t, firstCollectionInitChannel, "first collection init")

	// Start second async index creation for db2 while first is still running
	doneChan2, err := initMgr.InitializeDatabase(ctx, sc.Config, db2Config.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	// Unblock the first InitializeDatabase, should cancel
	close(testSignalChannel)

	// Wait for notification on both done channels
	WaitForChannel(t, doneChan1, "modified init done chan")
	WaitForChannel(t, doneChan2, "modified init done chan")

	// Wait for db completion notifications for both databases
	WaitForChannel(t, databaseCompleteChannel, "database 1 init complete")
	WaitForChannel(t, databaseCompleteChannel, "database 2 init complete")

	// Verify initialization was run for 8 collections (four for db1, four for db2)
	// _mobile, _default, collection1, collection2
	totalCount := atomic.LoadInt64(&collectionCount)
	require.Equal(t, int64(8), totalCount)

}

// TestDatabaseInitTeardownTiming tests scenarios where InitializeDatabase is called during
// the completion phase of a previous async initialization.  Ensures there are no cases where a
// watcher is added but never receives a done notification.
func TestDatabaseInitTeardownTiming(t *testing.T) {

	RequireN1QLIndexes(t)
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyConfig)

	sc, closeFn := StartBootstrapServer(t)
	defer closeFn()
	ctx := base.TestCtx(t)

	// Get a test bucket for bootstrap testing, and drop indexes created by bucket pool readier
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// Drop all test indexes so we can test InitializeDatabase
	base.DropAllBucketIndexes(t, tb)

	// Set up collection names and ScopesConfig for testing
	scopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1and2ScopesConfig := makeScopesConfig(scopeName, []string{collection1Name, collection2Name})

	initMgr := sc.DatabaseInitManager

	// Create collection callback that blocks and waits for test notification the first time a collection is initialized, does not block afterward.
	var collectionCount atomic.Int64
	initMgr.testCollectionStatusUpdateCallback = func(_ string, _ base.ScopeAndCollectionName, status db.CollectionIndexStatus) {
		if status != db.CollectionIndexStatusReady {
			return
		}
		collectionCount.Add(1)
	}
	dbName := "dbName"
	dbConfig := makeDbConfig(tb.GetName(), dbName, collection1and2ScopesConfig)
	require.NoError(t, dbConfig.setup(ctx, dbName, sc.Config.Bootstrap, nil, nil))
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	var databaseCompleteCount atomic.Int64
	initMgr.testDatabaseCompleteCallback = func(dbName string) {
		// On first completion, invoke InitializeDatabase with the same collection set post-completion
		if databaseCompleteCount.Add(1) == 1 {
			defer wg.Done()
			log.Printf("invoking InitializeDatabase again during teardown")
			doneChan2, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
			require.NoError(t, err)
			WaitForChannel(t, doneChan2, "done chan 2")
		}
	}

	// Start first async index creation, should block after first collection
	doneChan1, err := initMgr.InitializeDatabase(ctx, sc.Config, dbConfig.ToDatabaseConfig(), testUseLegacySyncDocsIndex, true, false)
	require.NoError(t, err)

	WaitForChannel(t, doneChan1, "done chan 1")
	wg.Wait()

	// Verify initialization was run for 8 collections, since it runs on 4 collections twice
	// _mobile, _default, collection1, collection2
	require.Equal(t, int64(8), collectionCount.Load())

	// Expect two database complete callbacks, since initialization is run twice
	require.Equal(t, int64(2), databaseCompleteCount.Load())
}

func makeScopesConfig(scopeName string, collectionNames []string) ScopesConfig {

	collectionsConfig := make(CollectionsConfig)
	for _, collectionName := range collectionNames {
		collectionsConfig[collectionName] = &CollectionConfig{}
	}
	return ScopesConfig{
		scopeName: ScopeConfig{
			Collections: collectionsConfig,
		},
	}
}

// waitForWorkerDone avoids races when testing db initializations performed serially
func waitForWorkerDone(t *testing.T, manager *DatabaseInitManager, dbName string) {
	for range 1000 {
		if !manager.HasActiveInitialization(dbName) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Worker did not complete in expected time interval for db %s", dbName)
}

func TestBuildCollectionIndexData(t *testing.T) {
	bootstrapEnabled := &StartupConfig{Bootstrap: BootstrapConfig{UseSystemMetadataCollection: base.Ptr(true)}}
	bootstrapDisabled := &StartupConfig{Bootstrap: BootstrapConfig{UseSystemMetadataCollection: base.Ptr(false)}}

	tests := []struct {
		name                    string
		config                  *DatabaseConfig
		defaultCollectionExists bool
		migrationComplete       bool
		want                    CollectionInitData
		startupConfig           *StartupConfig
	}{
		// Scope variations — verifies index sets are computed correctly per collection layout.
		// Uses per-DB opt-in to show the full output including mobile collection.
		{
			name: "implicit default collection",
			config: &DatabaseConfig{DbConfig: DbConfig{
				UseSystemMobileMetadataCollection: base.Ptr(true),
			}},
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():      db.IndexesAll,
				base.MobileSystemScopeAndCollectionName(): db.IndexesMetadataOnly,
			},
		},
		{
			name: "implicit default collection with mobile collection disabled",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes: nil,
				},
			},
			defaultCollectionExists: true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName(): db.IndexesAll,
			},
		},
		{
			name: "explicit default collection with mobile collection enabled",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes:                            makeScopesConfig(base.DefaultScope, []string{base.DefaultCollection}),
					UseSystemMobileMetadataCollection: base.Ptr(true),
				},
			},
			defaultCollectionExists: true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():      db.IndexesAll,
				base.MobileSystemScopeAndCollectionName(): db.IndexesMetadataOnly,
			},
		},
		{
			name: "explicit default collection with mobile collection disabled",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes: makeScopesConfig(base.DefaultScope, []string{base.DefaultCollection}),
				},
			},
			// a configured _default necessarily exists; callers never report it as absent
			defaultCollectionExists: true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName(): db.IndexesAll,
			},
		},
		{
			name: "one named collection with mobile collection enabled",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes:                            makeScopesConfig("scope1", []string{"collection1"}),
					UseSystemMobileMetadataCollection: base.Ptr(true),
				},
			},
			defaultCollectionExists: true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():                    db.IndexesMetadataOnly,
				base.MobileSystemScopeAndCollectionName():               db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName("scope1", "collection1"): db.IndexesWithoutMetadata,
			},
		},
		{
			name: "one named and explicit default collection",
			config: &DatabaseConfig{DbConfig: DbConfig{
				Scopes:                            makeScopesConfig(base.DefaultScope, []string{base.DefaultCollection, "collection1"}),
				UseSystemMobileMetadataCollection: base.Ptr(true),
			}},
			// a configured _default necessarily exists; callers never report it as absent
			defaultCollectionExists: true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():                             db.IndexesAll,
				base.MobileSystemScopeAndCollectionName():                        db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName(base.DefaultScope, "collection1"): db.IndexesWithoutMetadata,
			},
		},
		// Flag interaction cases — verifies resolveUseSystemMetadataCollection precedence.
		// Uses implicit default collection as a representative scope.
		{
			name:   "per-DB enabled, no startup config",
			config: &DatabaseConfig{DbConfig: DbConfig{UseSystemMobileMetadataCollection: base.Ptr(true)}},
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():      db.IndexesAll,
				base.MobileSystemScopeAndCollectionName(): db.IndexesMetadataOnly,
			},
		},
		{
			name: "one named and explicit default collection with mobile collection disabled",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes: makeScopesConfig(base.DefaultScope, []string{base.DefaultCollection, "collection1"}),
				},
			},
			defaultCollectionExists: true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():                             db.IndexesAll,
				base.NewScopeAndCollectionName(base.DefaultScope, "collection1"): db.IndexesWithoutMetadata,
			},
		},
		{
			// per-DB false falls through to the cluster flag, which is true
			name:          "bootstrap enabled, per-DB explicitly disabled",
			startupConfig: bootstrapEnabled,
			config:        &DatabaseConfig{DbConfig: DbConfig{UseSystemMobileMetadataCollection: base.Ptr(false)}},
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():      db.IndexesAll,
				base.MobileSystemScopeAndCollectionName(): db.IndexesMetadataOnly,
			},
		},
		{
			name:          "bootstrap disabled, per-DB unset",
			startupConfig: bootstrapDisabled,
			config:        &DatabaseConfig{},
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName(): db.IndexesAll,
			},
		},
		{
			// per-DB true always wins, regardless of cluster flag
			name:          "bootstrap disabled, per-DB explicitly enabled",
			startupConfig: bootstrapDisabled,
			config:        &DatabaseConfig{DbConfig: DbConfig{UseSystemMobileMetadataCollection: base.Ptr(true)}},
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():      db.IndexesAll,
				base.MobileSystemScopeAndCollectionName(): db.IndexesMetadataOnly,
			},
		},
		{
			// _default dropped after migration (e.g. system metadata collection in use): metadata
			// indexes must not be targeted at the now-missing _default._default collection.
			name: "named collection with _default dropped",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes:                            makeScopesConfig("scope1", []string{"collection1"}),
					UseSystemMobileMetadataCollection: base.Ptr(true),
				},
			},
			defaultCollectionExists: false,
			want: CollectionInitData{
				base.MobileSystemScopeAndCollectionName():               db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName("scope1", "collection1"): db.IndexesWithoutMetadata,
			},
		},
		// migrationComplete gating — verifies _default is included only while it's still serving
		// metadata (legacy store, or dual-read fallback during migration) and excluded once a
		// system-metadata database has migrated off it.
		{
			// Legacy metadata (system metadata disabled): _default._default IS the metadata store, so
			// it must be indexed even though it's not a configured collection. Regression guard: a
			// system-metadata-only gate would wrongly drop it here.
			name: "legacy metadata: named collection, _default is metadata store",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes: makeScopesConfig("scope1", []string{"collection1"}),
				},
			},
			defaultCollectionExists: true,
			migrationComplete:       false,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():                    db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName("scope1", "collection1"): db.IndexesWithoutMetadata,
			},
		},
		{
			// migrationComplete has no effect in legacy mode — _default is still the metadata store and
			// stays indexed regardless. Locks the invariant-independence of the migratedOffDefault gate.
			name: "legacy metadata: migrationComplete ignored, _default still indexed",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes: makeScopesConfig("scope1", []string{"collection1"}),
				},
			},
			defaultCollectionExists: true,
			migrationComplete:       true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():                    db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName("scope1", "collection1"): db.IndexesWithoutMetadata,
			},
		},
		{
			// System metadata in use and migration complete: _default's metadata indexes are vestigial
			// and must be dropped even though _default still physically exists.
			name: "system metadata: migration complete, _default still exists",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes:                            makeScopesConfig("scope1", []string{"collection1"}),
					UseSystemMobileMetadataCollection: base.Ptr(true),
				},
			},
			defaultCollectionExists: true,
			migrationComplete:       true,
			want: CollectionInitData{
				base.MobileSystemScopeAndCollectionName():               db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName("scope1", "collection1"): db.IndexesWithoutMetadata,
			},
		},
		{
			// Both gates agree _default should be excluded: migration complete AND the collection dropped.
			name: "system metadata: migration complete, _default dropped",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes:                            makeScopesConfig("scope1", []string{"collection1"}),
					UseSystemMobileMetadataCollection: base.Ptr(true),
				},
			},
			defaultCollectionExists: false,
			migrationComplete:       true,
			want: CollectionInitData{
				base.MobileSystemScopeAndCollectionName():               db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName("scope1", "collection1"): db.IndexesWithoutMetadata,
			},
		},
		{
			// Implicit default collection (no scopes configured): _default is a configured data collection
			// by default, so post-migration it needs only its data indexes — same reasoning as the explicit
			// case below, reached through the no-scopes branch.
			name: "system metadata: migration complete, implicit default collection",
			config: &DatabaseConfig{DbConfig: DbConfig{
				UseSystemMobileMetadataCollection: base.Ptr(true),
			}},
			defaultCollectionExists: true,
			migrationComplete:       true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():      db.IndexesWithoutMetadata,
				base.MobileSystemScopeAndCollectionName(): db.IndexesMetadataOnly,
			},
		},
		{
			// migrationComplete is meaningless in legacy mode — _default is still the metadata store as
			// well as the data collection, so it keeps the full index set. Callers never report
			// migrationComplete=true here (the metadata store isn't dual), so this locks the branch
			// against a future caller that does.
			name:                    "legacy metadata: migrationComplete ignored, implicit default collection",
			config:                  &DatabaseConfig{},
			defaultCollectionExists: true,
			migrationComplete:       true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName(): db.IndexesAll,
			},
		},
		{
			// _default is a configured data collection but metadata has migrated to _system._mobile, so it
			// needs only its data indexes — its metadata indexes would be vestigial. The data role
			// (configured) and metadata role (migrated off) are independent, hence IndexesWithoutMetadata.
			name: "system metadata: migration complete, _default is a configured data collection",
			config: &DatabaseConfig{
				DbConfig: DbConfig{
					Scopes:                            makeScopesConfig(base.DefaultScope, []string{base.DefaultCollection, "collection1"}),
					UseSystemMobileMetadataCollection: base.Ptr(true),
				},
			},
			defaultCollectionExists: true,
			migrationComplete:       true,
			want: CollectionInitData{
				base.DefaultScopeAndCollectionName():                             db.IndexesWithoutMetadata,
				base.MobileSystemScopeAndCollectionName():                        db.IndexesMetadataOnly,
				base.NewScopeAndCollectionName(base.DefaultScope, "collection1"): db.IndexesWithoutMetadata,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := buildCollectionIndexData(test.startupConfig, test.config, test.defaultCollectionExists, test.migrationComplete)
			assert.Equalf(t, test.want, actual, "buildCollectionIndexData(startup=%v, config=%v, defaultCollectionExists=%v, migrationComplete=%v)", test.startupConfig, test.config, test.defaultCollectionExists, test.migrationComplete)
		})
	}
}
