/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package defaultcollectiondroptest

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

// TestDatabaseSurvivesDefaultCollectionDropAfterMetadataMigration exercises the theory that once a
// database has fully migrated its metadata into _system._mobile, the _default collection is no
// longer required and can be dropped without preventing the database from coming back online.
//
// The flow mirrors a production upgrade:
//
//  1. Create a database whose documents with use_system_metadata_collection=false.
//     In this mode the database's metadata (users, roles,
//     sync-function doc, sequence counters, registry, etc.) lands in _default._default.
//  2. Write a few users so there is real metadata sitting in _default._default.
//  3. Opt the database into the system metadata collection and drive the migration to completion,
//     waiting for BOTH the per-DB status and the bucket-wide bootstrap status to report complete.
//     After this, all metadata has moved to _system._mobile and _default._default should be unused.
//  4. Drop the _default collection.
//  5. Force a full database reload (via a benign config update) so the DatabaseContext is torn down
//     and rebuilt, reopening its metadata store and collections from scratch.
//  6. Assert the database comes back Online and still services requests (principals readable, a doc
//     round-trips through the named collection).
//
// This can only run against Couchbase Server: rosmar refuses to drop _default
func TestDatabaseSurvivesDefaultCollectionDropAfterMetadataMigration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("requires Couchbase Server: rosmar cannot drop the _default collection, and this scenario only matters on CBS")
	}
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// The database's documents live in a named collection (sg_test_0.sg_test_0), NOT in
	// _default._default.
	namedDataStore, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	namedCollection := base.ScopeAndCollectionName{Scope: namedDataStore.ScopeName(), Collection: namedDataStore.CollectionName()}
	namedDatastore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	namedCollection2 := base.ScopeAndCollectionName{Scope: namedDatastore2.ScopeName(), Collection: namedDatastore2.CollectionName()}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	const dbName = "db"

	// 1. Legacy mode: opt-in disabled, so metadata is written to _default._default.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbConfig.Scopes = rest.ScopesConfig{
		namedCollection.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				namedCollection.CollectionName(): {},
			},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// 2. Write a few users. In legacy mode these `_sync:user:...` docs land in _default._default.
	for _, user := range []string{"alice", "bob", "carol"} {
		resp := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/_user/"+user,
			fmt.Sprintf(`{"name":%q,"password":"letmein123","admin_channels":["*"]}`, user))
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Confirm the legacy metadata really is in _default._default — otherwise the rest of the test
	// (drop _default, expect survival) would be meaningless.
	dbCtx, err := rt.ServerContext().GetDatabase(ctx, dbName)
	require.NoError(t, err)
	fallback := tb.Bucket.DefaultDataStore(ctx)
	aliceKey := dbCtx.MetadataKeys.UserKey("alice")
	exists, err := fallback.Exists(ctx, aliceKey)
	require.NoError(t, err)
	require.Truef(t, exists, "user key %q must exist in _default._default before migration", aliceKey)

	// 3a. Opt into the system metadata collection. This rebuilds the MetadataStore as the
	// dual-collection wrapper (primary=_system._mobile, fallback=_default._default).
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, dbConfig), http.StatusCreated)

	// 3b. Drive the migration. The manual start gates on config-fully-applied, so flush this node's
	// applied config version first, then kick off and wait for the per-DB status to report complete.
	rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_metadata_migration?action=start", ""), http.StatusOK)
	migStatus := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbName)
	assert.Zero(t, migStatus.DocsFailed, "migration must complete with no per-doc failures")

	// 3c. Wait for the bucket-WIDE bootstrap migration to complete. Bootstrap only flips to complete
	// once every per-DB entry is complete (here, our single DB), covering the bucket-global docs
	// (_sync:registry, _sync:dbconfig:*, cbgt cfg). This is the "wait for migration to finish for the
	// whole bucket" step.
	rt.WaitForBucketMetadataMigrationComplete(rt.Bucket().GetName())

	// Sanity: the migrated metadata now lives on _system._mobile and is gone from _default._default.
	systemDS, err := rt.Bucket().NamedDataStore(ctx, base.MobileSystemScopeAndCollectionName())
	require.NoError(t, err)
	onPrimary, err := systemDS.Exists(ctx, aliceKey)
	require.NoError(t, err)
	require.True(t, onPrimary, "user key must be on _system._mobile after migration")
	onFallback, err := fallback.Exists(ctx, aliceKey)
	require.NoError(t, err)
	require.False(t, onFallback, "user key must be removed from _default._default after migration")

	// 4. Drop _default.
	require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: base.DefaultCollection}),
		"dropping _default should succeed on Couchbase Server")

	// 5. Force a full database reload by pushing a benign config change (bump revs_limit). The opt-in
	// must stay true (it is irreversible).
	dbConfig.RevsLimit = base.Ptr(uint32(100))
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, dbConfig), http.StatusCreated)

	// 6a. The database must come back Online.
	rt.WaitForDatabaseState(dbName, "Online")

	// Guard against a flaky-fast reload: ensure the reload actually took effect.
	require.Eventually(t, func() bool {
		reloaded, getErr := rt.ServerContext().GetDatabase(ctx, dbName)
		return getErr == nil && reloaded.RevsLimit == 100
	}, 10*time.Second, 100*time.Millisecond, "config reload (revs_limit bump) should have been applied")

	// 6b. Principals must still be readable — these reads go through the migrated _system._mobile
	// metadata store, not the dropped _default collection.
	for _, user := range []string{"alice", "bob", "carol"} {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+dbName+"/_user/"+user, "")
		rest.RequireStatus(t, resp, http.StatusOK)
		assert.Contains(t, resp.Body.String(), fmt.Sprintf(`"name":%q`, user))
	}

	// 6c. End-to-end document round-trip through the named collection: the data path must be fully
	// functional with _default gone.
	keyspace := fmt.Sprintf("%s.%s.%s", dbName, namedCollection.ScopeName(), namedCollection.CollectionName())
	resp := rt.SendAdminRequest(http.MethodPut, "/"+keyspace+"/doc1", `{"channels":["*"],"value":"hello"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodGet, "/"+keyspace+"/doc1", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"value":"hello"`)

	// Try create a new db against the bucket with no _default without opting into the system metadata collection.
	// This should fail because the legacy metadata store is gone.
	newDbConfig := rt.NewDbConfig()
	newDbConfig.Scopes = rest.ScopesConfig{
		namedCollection2.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				namedCollection2.CollectionName(): {},
			},
		},
	}
	newDbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp = rt.CreateDatabase("newdb", newDbConfig)
	rest.RequireStatus(t, resp, http.StatusInternalServerError)
	assert.Contains(t, resp.Body.String(), "_default._default does not exist on bucket")
}
