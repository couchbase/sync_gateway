/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package manualbucketpooltest

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFreshDeploymentWithDroppedDefaultCollection verifies that a database can be created and
// fully functional on a bucket where _default._default has been dropped before the database is
// created. This exercises fresh deployments that start with use_system_metadata_collection=true
// and never had _default._default, or post-migration deployments where the operator dropped it.
func TestFreshDeploymentWithDroppedDefaultCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)

	// Create a fresh bucket outside the pool so we can safely drop _default._default without
	// corrupting a shared pool bucket that other tests rely on.
	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	const testScope = "sg_test_0"
	const testCollection = "sg_test_0"

	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: testScope, Collection: testCollection}))

	// Drop _default._default before the database is created to simulate a fresh deployment
	// where the collection was never present or was dropped post-migration.
	// Rosmar does not support dropping the default collection, so this step is CBS-only.
	if !base.UnitTestUrlIsWalrus() {
		require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{
			Scope: base.DefaultScope, Collection: base.DefaultCollection,
		}), "dropping _default._default should succeed on Couchbase Server")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	// Create a database with use_system_metadata_collection=true targeting the named collection.
	// Since _default._default is absent, the server must use _system._mobile for all metadata.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	dbConfig.Scopes = rest.ScopesConfig{
		testScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				testCollection: {},
			},
		},
	}
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// REST CRUD: create and read a document via the SG admin REST API.
	rt.CreateTestDoc("crud-doc1")
	rt.GetDoc("crud-doc1")

	// Import: write a doc directly to the named data store (bypassing SG), then GET it via the
	// admin REST API to trigger on-demand import and verify SG can import without _default._default.
	namedDataStore, err := tb.NamedDataStore(ctx, base.ScopeAndCollectionName{Scope: testScope, Collection: testCollection})
	require.NoError(t, err)
	_, err = namedDataStore.Add(ctx, "import-doc1", 0, map[string]any{"channels": []string{"*"}, "value": "imported"})
	require.NoError(t, err, "direct SDK write to named collection must succeed")
	_, importedBody := rt.GetDoc("import-doc1")
	assert.Equal(t, "imported", importedBody["value"], "on-demand import must succeed with _default._default absent")
}

// TestMigrationThenDropDefaultCollection exercises the full migration-then-drop lifecycle on a
// fresh bucket created outside the pool. The flow is:
//
//  1. Create a database with use_system_metadata_collection=false so metadata lands in _default._default.
//  2. Write users, placing real principal docs in _default._default.
//  3. Opt into system metadata, drive migration to completion (per-DB and bucket-wide).
//  4. Drop _default._default (CBS only; rosmar cannot drop the default collection).
//  5. Force a database reload and assert the database comes back online and still services requests.
func TestMigrationThenDropDefaultCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)

	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	const testScope = "sg_test_0"
	const testCollection = "sg_test_0"
	const dbName = "db"

	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: testScope, Collection: testCollection}))

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	// 1. Legacy mode: metadata lands in _default._default.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbConfig.Scopes = rest.ScopesConfig{
		testScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				testCollection: {},
			},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// 2. Write users so there is real metadata in _default._default to migrate.
	for _, user := range []string{"alice", "bob"} {
		resp := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/_user/"+user,
			fmt.Sprintf(`{"name":%q,"password":"letmein","admin_channels":["*"]}`, user))
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// 3. Opt into system metadata collection, flush this node's applied-config gate, then drive
	// migration to completion for the per-DB entry and the bucket-wide bootstrap docs.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, dbConfig), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_metadata_migration?action=start", ""), http.StatusOK)
	migStatus := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbName)
	assert.Zero(t, migStatus.DocsFailed, "migration must complete with no per-doc failures")
	rt.WaitForBucketMetadataMigrationComplete(rt.Bucket().GetName())

	// 4. Drop _default._default. Rosmar does not support dropping the default collection.
	if !base.UnitTestUrlIsWalrus() {
		require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{
			Scope: base.DefaultScope, Collection: base.DefaultCollection,
		}), "dropping _default._default should succeed on Couchbase Server after migration")
	}

	// 5. Force a database reload via a benign config change (bump revs_limit). The opt-in is
	// irreversible, so it remains true in the updated config.
	dbConfig.RevsLimit = base.Ptr(uint32(100))
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, dbConfig), http.StatusCreated)
	rt.WaitForDatabaseState(dbName, "Online")
	require.Eventually(t, func() bool {
		reloaded, err := rt.ServerContext().GetDatabase(ctx, dbName)
		return err == nil && reloaded.RevsLimit == 100
	}, 10*time.Second, 100*time.Millisecond, "config reload must be applied before asserting post-drop behaviour")

	// 5. Principals must still be readable — reads go through _system._mobile.
	for _, user := range []string{"alice", "bob"} {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+dbName+"/_user/"+user, "")
		rest.RequireStatus(t, resp, http.StatusOK)
		assert.Contains(t, resp.Body.String(), fmt.Sprintf(`"name":%q`, user))
	}

	// 6. Document round-trip through the named collection confirms the data path is functional.
	rt.CreateTestDoc("doc1")
	rt.GetDoc("doc1")
}

// TestFreshDeploymentWithDroppedDefaultCollectionTwoDatabases extends
// TestFreshDeploymentWithDroppedDefaultCollection by adding a second database on the same bucket
// that also uses use_system_metadata_collection=true from inception, so neither database ever runs
// metadata migration. Probes that two sibling databases can initialise and serve traffic without
// _default._default being present on the bucket.
func TestFreshDeploymentWithDroppedDefaultCollectionTwoDatabases(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)

	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	const (
		scope       = "sg_test_0"
		collection1 = "sg_test_0"
		collection2 = "sg_test_1"
	)

	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection1}))
	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection2}))

	// Seed a document in collection2 before the database is created to verify on-demand import
	// works correctly for a sibling DB that never saw _default._default.
	dataStore2, err := tb.NamedDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection2})
	require.NoError(t, err)
	_, err = dataStore2.Add(ctx, "pre-existing-doc", 0, map[string]any{"channels": []string{"*"}, "value": "pre-existing"})
	require.NoError(t, err)

	if !base.UnitTestUrlIsWalrus() {
		require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{
			Scope: base.DefaultScope, Collection: base.DefaultCollection,
		}), "dropping _default._default should succeed on Couchbase Server")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	makeDBConfig := func(col string) rest.DbConfig {
		dbConfig := rt.NewDbConfig()
		dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
		dbConfig.Scopes = rest.ScopesConfig{
			scope: rest.ScopeConfig{
				Collections: rest.CollectionsConfig{col: {}},
			},
		}
		return dbConfig
	}

	rest.RequireStatus(t, rt.CreateDatabase("db1", makeDBConfig(collection1)), http.StatusCreated)
	rest.RequireStatus(t, rt.CreateDatabase("db2", makeDBConfig(collection2)), http.StatusCreated)

	// Neither database should have started a migration — they write directly to _system._mobile.
	for _, dbName := range []string{"db1", "db2"} {
		dbCtx, getErr := rt.ServerContext().GetDatabase(ctx, dbName)
		require.NoError(t, getErr)
		require.NotNil(t, dbCtx.MetadataMigrationManager)
		assert.Equal(t, db.BackgroundProcessState(""), dbCtx.MetadataMigrationManager.GetRunState(),
			"%s must not start metadata migration on a fresh bucket", dbName)
	}

	for _, tc := range []struct{ dbName, col string }{
		{"db1", collection1},
		{"db2", collection2},
	} {
		keyspace := fmt.Sprintf("%s.%s.%s", tc.dbName, scope, tc.col)

		// REST CRUD
		rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/"+keyspace+"/crud-doc", `{"channels":["*"],"value":"hello"}`), http.StatusCreated)
		body := rt.GetDocBodyFromKeyspace(keyspace, "crud-doc")
		assert.Equal(t, "hello", body["value"], "%s REST CRUD must work without _default._default", tc.dbName)

		// User write and read — exercises the principal path into _system._mobile.
		resp := rt.SendAdminRequest(http.MethodPut, "/"+tc.dbName+"/_user/u1",
			`{"name":"u1","password":"letmein","admin_channels":["*"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		resp = rt.SendAdminRequest(http.MethodGet, "/"+tc.dbName+"/_user/u1", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		assert.Contains(t, resp.Body.String(), `"name":"u1"`, "%s user must be readable after creation", tc.dbName)
	}

	// On-demand import of the pre-seeded document in db2 (written before db2 existed).
	importBody := rt.GetDocBodyFromKeyspace(fmt.Sprintf("db2.%s.%s", scope, collection2), "pre-existing-doc")
	assert.Equal(t, "pre-existing", importBody["value"], "on-demand import of pre-existing doc must work for db2")
}

// TestMigrationThenDropDefaultCollectionTwoDatabases extends TestMigrationThenDropDefaultCollection
// by adding a second database on the same bucket after migration is complete and _default._default
// is dropped. The second database uses use_system_metadata_collection=true from inception and must
// work without ever accessing _default._default.
func TestMigrationThenDropDefaultCollectionTwoDatabases(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)

	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	const (
		scope       = "sg_test_0"
		collection1 = "sg_test_0"
		collection2 = "sg_test_1"
		db1Name     = "db1"
		db2Name     = "db2"
	)

	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection1}))
	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection2}))

	// Seed a document in collection2 before db2 is created to confirm import works on a sibling
	// DB whose collection never touched _default._default.
	dataStore2, err := tb.NamedDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection2})
	require.NoError(t, err)
	_, err = dataStore2.Add(ctx, "pre-existing-doc", 0, map[string]any{"channels": []string{"*"}, "value": "pre-existing"})
	require.NoError(t, err)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	// 1. Create db1 in legacy mode so its metadata lands in _default._default.
	dbConfig1 := rt.NewDbConfig()
	dbConfig1.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbConfig1.Scopes = rest.ScopesConfig{
		scope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{collection1: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(db1Name, dbConfig1), http.StatusCreated)

	// 2. Write users into db1, placing principal docs in _default._default.
	for _, user := range []string{"alice", "bob"} {
		resp := rt.SendAdminRequest(http.MethodPut, "/"+db1Name+"/_user/"+user,
			fmt.Sprintf(`{"name":%q,"password":"letmein","admin_channels":["*"]}`, user))
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// 3. Opt db1 into system metadata, drive migration to completion.
	dbConfig1.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(db1Name, dbConfig1), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/"+db1Name+"/_metadata_migration?action=start", ""), http.StatusOK)
	migStatus := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, db1Name)
	assert.Zero(t, migStatus.DocsFailed, "db1 migration must complete with no per-doc failures")
	rt.WaitForBucketMetadataMigrationComplete(rt.Bucket().GetName())

	// 4. Drop _default._default (CBS only).
	if !base.UnitTestUrlIsWalrus() {
		require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{
			Scope: base.DefaultScope, Collection: base.DefaultCollection,
		}), "dropping _default._default should succeed on Couchbase Server after migration")
	}

	// 5. Create db2 on the same bucket with use_system_metadata_collection=true from the start.
	// _default._default is absent; db2 must initialise solely from _system._mobile.
	dbConfig2 := rt.NewDbConfig()
	dbConfig2.UseSystemMobileMetadataCollection = base.Ptr(true)
	dbConfig2.Scopes = rest.ScopesConfig{
		scope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{collection2: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(db2Name, dbConfig2), http.StatusCreated)

	// db2 must never start a migration — it writes directly to _system._mobile.
	db2Ctx, err := rt.ServerContext().GetDatabase(ctx, db2Name)
	require.NoError(t, err)
	require.NotNil(t, db2Ctx.MetadataMigrationManager)
	assert.Equal(t, db.BackgroundProcessState(""), db2Ctx.MetadataMigrationManager.GetRunState(),
		"db2 must not start metadata migration when bootstrapped on a migrated bucket")

	// 6. Reload db1 via a benign config change to exercise coming back online without _default._default.
	dbConfig1.RevsLimit = base.Ptr(uint32(100))
	rest.RequireStatus(t, rt.UpsertDbConfig(db1Name, dbConfig1), http.StatusCreated)
	rt.WaitForDatabaseState(db1Name, "Online")
	require.Eventually(t, func() bool {
		reloaded, getErr := rt.ServerContext().GetDatabase(ctx, db1Name)
		return getErr == nil && reloaded.RevsLimit == 100
	}, 10*time.Second, 100*time.Millisecond, "db1 config reload must be applied")

	// 7. db1 principals must still be readable after reload without _default._default.
	for _, user := range []string{"alice", "bob"} {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+db1Name+"/_user/"+user, "")
		rest.RequireStatus(t, resp, http.StatusOK)
		assert.Contains(t, resp.Body.String(), fmt.Sprintf(`"name":%q`, user),
			"db1 user must be readable after reload; reads should go through _system._mobile")
	}

	// 8. db2 user write and read — confirms its principal path reaches _system._mobile, not _default._default.
	resp := rt.SendAdminRequest(http.MethodPut, "/"+db2Name+"/_user/carol",
		`{"name":"carol","password":"letmein","admin_channels":["*"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodGet, "/"+db2Name+"/_user/carol", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"carol"`)

	// db1 principals must not bleed into db2.
	resp = rt.SendAdminRequest(http.MethodGet, "/"+db2Name+"/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)

	// 9. CRUD and import for both databases.
	for _, tc := range []struct{ dbName, col string }{
		{db1Name, collection1},
		{db2Name, collection2},
	} {
		keyspace := fmt.Sprintf("%s.%s.%s", tc.dbName, scope, tc.col)
		rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/"+keyspace+"/doc1", `{"channels":["*"],"value":"hello"}`), http.StatusCreated)
		body := rt.GetDocBodyFromKeyspace(keyspace, "doc1")
		assert.Equal(t, "hello", body["value"], "%s CRUD must work", tc.dbName)
	}

	// On-demand import of the pre-seeded document in db2.
	importBody := rt.GetDocBodyFromKeyspace(fmt.Sprintf("db2.%s.%s", scope, collection2), "pre-existing-doc")
	assert.Equal(t, "pre-existing", importBody["value"], "on-demand import must work for db2 without _default._default")
}

// TestDropDefaultCollectionDuringMigration is an adversarial test that races dropping
// _default._default against an in-progress metadata migration. The expected outcome is that
// the migration manager terminates with an error state rather than hanging or panicking, and
// that the SG process remains alive and can still service HTTP requests.
//
// CBS only: rosmar does not support dropping the default collection.
func TestDropDefaultCollectionDuringMigration(t *testing.T) {
	base.TestRequiresCollections(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS only: rosmar cannot drop _default._default")
	}

	ctx := base.TestCtx(t)
	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	const testScope = "sg_test_0"
	const testCollection = "sg_test_0"
	const dbName = "db"

	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: testScope, Collection: testCollection}))

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	// 1. Create in legacy mode so all metadata lands in _default._default.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbConfig.Scopes = rest.ScopesConfig{
		testScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{testCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// 2. Write many users to give the migration scan enough work that there is a realistic
	//    window to drop _default._default before the scan finishes.
	const numUsers = 100
	for i := range numUsers {
		resp := rt.SendAdminRequest(http.MethodPut,
			fmt.Sprintf("/%s/_user/user%d", dbName, i),
			fmt.Sprintf(`{"name":"user%d","password":"letmein","admin_channels":["*"]}`, i))
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// 3. Opt into system metadata collection and start migration.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, dbConfig), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_metadata_migration?action=start", ""), http.StatusOK)

	// 4. Drop _default._default immediately — deliberately racing the active migration scan.
	//    Whether the drop lands before the Scan() call (causing a scan-open error) or during
	//    iteration (causing per-doc fetch errors), the migration must reach a terminal state.
	require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{
		Scope: base.DefaultScope, Collection: base.DefaultCollection,
	}), "dropping _default._default must succeed on Couchbase Server")

	// 5. Wait for migration to leave the running state. Allow generous timeout: if the scan
	//    already fetched all key IDs before the drop, the manager may exhaust all 16 passes
	//    with per-doc errors before declaring error state.
	var migStatus db.MigrationManagerResponse
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+dbName+"/_metadata_migration", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		require.NoError(c, base.JSONUnmarshal(resp.BodyBytes(), &migStatus))
		assert.NotEqual(c, db.BackgroundProcessStateRunning, migStatus.State)
		assert.NotEqual(c, db.BackgroundProcessStateStopping, migStatus.State)
	}, 3*time.Minute, 500*time.Millisecond, "migration must reach a terminal state after fallback collection is dropped")

	// 6. The terminal state must be either error (drop won the race) or completed (migration
	//    finished before the drop took effect). In either case there must be no silent hang.
	t.Logf("migration terminal state after mid-run drop: %s (passes=%d docs_failed=%d)",
		migStatus.State, migStatus.Passes, migStatus.DocsFailed)
	assert.Contains(t,
		[]db.BackgroundProcessState{db.BackgroundProcessStateError, db.BackgroundProcessStateCompleted},
		migStatus.State,
		"migration must end in error or completed, not an unknown terminal state")

	// 7. The SG process must still be alive and serving requests regardless of migration outcome.
	//    New writes go directly to the primary (_system._mobile), so a doc round-trip is a
	//    stronger liveness signal than a database-info GET.
	rt.CreateTestDoc("liveness-doc")
	rt.GetDoc("liveness-doc")
}
