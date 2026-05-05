// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package indextest – Couchbase-Server-only tests for dual MetadataStore query
// deduplication and index initialisation.  The package-level TestMain in main_test.go
// already skips this entire package when not running against CBS with GSI.

package indextest

import (
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDualMetadataStoreIndexes verifies that InitializeDualMetadataStoreIndexes creates the
// required principal indexes on both the primary (_system._mobile) and fallback
// (_default._default) datastores.
func TestDualMetadataStoreIndexes(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })

	// GetMobileSystemDataStore skips if the backing store does not support system collections.
	primaryStore := bucket.GetMobileSystemDataStore()
	fallbackStore := bucket.DefaultDataStore()

	ms := base.NewMetadataStore(primaryStore, fallbackStore)

	indexOptions := db.InitializeIndexOptions{
		NumReplicas:                0,
		LegacySyncDocsIndex:        false,
		UseXattrs:                  base.TestUseXattrs(),
		NumPartitions:              db.DefaultNumIndexPartitions,
		WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
		MetadataIndexes:            db.IndexesAll,
	}
	require.NoError(t, db.InitializeDualMetadataStoreIndexes(t, ctx, ms, indexOptions))

	// Determine the expected principal index names
	expectedIndexes := []string{
		"sg_users_x1",
		"sg_roles_x1",
	}

	gocbBucket, err := base.AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	// Verify that each expected index is present and online on both stores.
	for _, store := range []base.DataStore{primaryStore, fallbackStore} {

		var indexesMeta map[string]base.IndexMeta
		if store.GetName() == primaryStore.GetName() {
			n1qlStore, err := base.NewClusterOnlyN1QLStore(
				gocbBucket.GetCluster(),
				gocbBucket.BucketName(),
				store.ScopeName(),
				store.CollectionName(),
			)
			require.NoError(t, err)
			indexesMeta, err = base.GetSystemCollectionIndexesMeta(ctx, n1qlStore, base.SystemScope, base.SystemCollectionMobile, expectedIndexes)
			require.NoError(t, err)
		} else {
			fallbackN1QLStore, ok := base.AsN1QLStore(fallbackStore)
			require.True(t, ok)
			indexesMeta, err = base.GetIndexesMeta(ctx, fallbackN1QLStore, expectedIndexes)
			require.NoError(t, err)
		}

		for _, indexName := range expectedIndexes {
			meta, found := indexesMeta[indexName]
			assert.True(t, found, "index %s missing on %s.%s", indexName, store.ScopeName(), store.CollectionName())
			if found {
				assert.Equal(t, base.IndexStateOnline, meta.State,
					"index %s not online on %s.%s", indexName, store.ScopeName(), store.CollectionName())
			}
		}
	}
}

// TestQueryPrincipalsDualMetadataStore verifies that QueryPrincipals and QueryUsers correctly
// deduplicate results when the DatabaseContext uses a *base.MetadataStore (migration-in-progress
// scenario), preferring the primary datastore version when the same document exists in both.
//
// Test data:
//   - "alice": exists only in primary (name="alice")
//   - "bob":   exists in both; primary has name="bob-primary", fallback has name="bob-fallback"
//   - "carol": exists only in fallback (name="carol")
//
// Expected: three unique results, with "bob" returning the primary-store name value.
func TestQueryPrincipalsDualMetadataStore(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })

	// GetMobileSystemDataStore skips if the backing store does not support system collections.
	primaryStore := bucket.GetMobileSystemDataStore()
	fallbackStore := bucket.DefaultDataStore()
	ms := base.NewMetadataStore(primaryStore, fallbackStore)

	// Initialise data indexes on the default collection (used as the CBL sync data scope).
	setupIndexes(t, bucket, testIndexCreationOptions{
		numPartitions:          db.DefaultNumIndexPartitions,
		useLegacySyncDocsIndex: false,
		useXattrs:              true,
	})

	// Initialise principal indexes on BOTH the primary and fallback metadata stores.
	indexOptions := db.InitializeIndexOptions{
		NumReplicas:                0,
		LegacySyncDocsIndex:        false,
		UseXattrs:                  true,
		NumPartitions:              db.DefaultNumIndexPartitions,
		WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
	}
	require.NoError(t, db.InitializeDualMetadataStoreIndexes(t, ctx, ms, indexOptions))

	// Build a DatabaseContext that uses the dual MetadataStore with the default collection for
	// sync metadata
	dbOptions := getDatabaseContextOptions(false)
	dbOptions.Scopes = db.GetScopesOptions(t, bucket, 2)
	dbOptions.EnableXattr = true
	dbOptions.MetadataStore = ms

	database, dbCtx := db.CreateTestDatabase(t, bucket, dbOptions)
	t.Cleanup(func() { database.Close(dbCtx) })

	metaKeys := base.DefaultMetadataKeys

	// writeUser writes a minimal user doc directly to the specified store, bypassing the
	// authenticator.  The "name" field deliberately differs between primary and fallback for
	// "bob" so that the test can verify which version is returned by the query.
	writeUser := func(t *testing.T, store base.DataStore, docKey, name string) {
		t.Helper()
		body, marshalErr := base.JSONMarshal(map[string]string{"name": name})
		require.NoError(t, marshalErr)
		added, setErr := store.AddRaw(docKey, 0, body)
		require.NoError(t, setErr)
		require.True(t, added, "expected document %s to be added (not already present)", docKey)
	}

	aliceKey := metaKeys.UserKey("alice")
	bobKey := metaKeys.UserKey("bob")
	carolKey := metaKeys.UserKey("carol")

	writeUser(t, primaryStore, aliceKey, "alice")       // alice: primary only
	writeUser(t, primaryStore, bobKey, "bob-primary")   // bob in primary
	writeUser(t, fallbackStore, bobKey, "bob-fallback") // bob also in fallback (duplicate)
	writeUser(t, fallbackStore, carolKey, "carol")      // carol: fallback only

	t.Run("QueryPrincipals", func(t *testing.T) {
		iter, err := database.QueryPrincipals(dbCtx, "", 0)
		require.NoError(t, err)

		results := make(map[string]db.PrincipalRow)
		var row db.PrincipalRow
		for iter.Next(dbCtx, &row) {
			results[row.Id] = row
			row = db.PrincipalRow{}
		}
		require.NoError(t, iter.Close())

		require.Len(t, results, 3, "expected exactly 3 unique principals after deduplication")
		require.Contains(t, results, aliceKey, "alice should be present")
		require.Contains(t, results, bobKey, "bob should be present")
		require.Contains(t, results, carolKey, "carol should be present")

		// Primary version of bob must be returned.
		assert.Equal(t, "bob-primary", results[bobKey].Name,
			"primary datastore version of bob should be preferred")
	})

	t.Run("QueryUsers", func(t *testing.T) {
		iter, err := database.QueryUsers(dbCtx, "", 0)
		require.NoError(t, err)

		results := make(map[string]db.QueryUsersRow)
		var row db.QueryUsersRow
		for iter.Next(dbCtx, &row) {
			results[row.ID] = row
			row = db.QueryUsersRow{}
		}
		require.NoError(t, iter.Close())

		require.Len(t, results, 3, "expected exactly 3 unique users after deduplication")
		require.Contains(t, results, aliceKey)
		require.Contains(t, results, bobKey)
		require.Contains(t, results, carolKey)

		assert.Equal(t, "bob-primary", results[bobKey].Name,
			"primary datastore version of bob should be preferred")
	})
}

// TestQueryUsersRealDocsDualMetadataStore is a higher-level integration test that creates real
// user documents via auth.Authenticator on both the primary (_system._mobile) and fallback
// (_default._default) stores, then calls QueryUsers directly (the same code path as the
// /_user/ REST endpoint) and asserts that:
//
//   - All users from both stores appear in the results.
//   - The same username present in both stores is deduplicated to a single entry.
//   - The primary-store version is preferred when a duplicate exists.
//
// Test data:
//   - "alice": primary store only  (email="alice@primary.example")
//   - "bob":   both stores; primary has email="bob@primary.example", fallback has "bob@fallback.example"
//   - "carol": fallback store only (email="carol@fallback.example")
//
// Expected: three unique results with bob's email originating from the primary store.
func TestQueryUsersRealDocsDualMetadataStore(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })

	// GetMobileSystemDataStore skips if the backing store does not support system collections.
	primaryStore := bucket.GetMobileSystemDataStore()
	fallbackStore := bucket.DefaultDataStore()
	ms := base.NewMetadataStore(primaryStore, fallbackStore)

	// Initialise data indexes on the default collection (used as the CBL sync data scope).
	setupIndexes(t, bucket, testIndexCreationOptions{
		numPartitions:          db.DefaultNumIndexPartitions,
		useLegacySyncDocsIndex: false,
		useXattrs:              true,
	})

	// Initialise principal indexes on BOTH the primary and fallback metadata stores.
	indexOptions := db.InitializeIndexOptions{
		NumReplicas:                0,
		LegacySyncDocsIndex:        false,
		UseXattrs:                  true,
		NumPartitions:              db.DefaultNumIndexPartitions,
		WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
	}
	require.NoError(t, db.InitializeDualMetadataStoreIndexes(t, ctx, ms, indexOptions))

	// Build a DatabaseContext that uses the dual MetadataStore with the default collection for
	// sync metadata.
	dbOptions := getDatabaseContextOptions(false)
	dbOptions.Scopes = db.GetScopesOptions(t, bucket, 2)
	dbOptions.EnableXattr = true
	dbOptions.MetadataStore = ms

	database, dbCtx := db.CreateTestDatabase(t, bucket, dbOptions)
	t.Cleanup(func() { database.Close(dbCtx) })

	// authOpts mirrors the core options used by DatabaseContext.Authenticator() but targeting
	// an individual store directly. channelComputer is nil because channel computation is not
	// exercised in this test.
	authOpts := auth.AuthenticatorOptions{
		LogCtx:   ctx,
		MetaKeys: base.DefaultMetadataKeys,
	}

	// createAndSaveUser writes a real user document to the given datastore via
	// auth.Authenticator, producing the full document structure that QueryUsers expects.
	// A store-specific email is used to distinguish which version is returned after
	// deduplication.
	createAndSaveUser := func(t *testing.T, store base.DataStore, username, email string) {
		t.Helper()
		authr := auth.NewAuthenticator(store, nil, authOpts)
		user, err := authr.NewUser(username, "password", nil)
		require.NoError(t, err)
		require.NoError(t, user.SetEmail(email))
		require.NoError(t, authr.Save(user))
	}

	createAndSaveUser(t, primaryStore, "alice", "alice@primary.example")   // alice: primary only
	createAndSaveUser(t, primaryStore, "bob", "bob@primary.example")       // bob: written to primary
	createAndSaveUser(t, fallbackStore, "bob", "bob@fallback.example")     // bob: also in fallback (duplicate)
	createAndSaveUser(t, fallbackStore, "carol", "carol@fallback.example") // carol: fallback only

	// QueryUsers is called by the /_user/ REST endpoint. Calling it directly here exercises
	// the full dual-store query and deduplication path without the HTTP layer.
	iter, err := database.QueryUsers(dbCtx, "", 0)
	require.NoError(t, err)

	results := make(map[string]db.QueryUsersRow)
	var row db.QueryUsersRow
	for iter.Next(dbCtx, &row) {
		results[row.Name] = row
		row = db.QueryUsersRow{} // reset for next iteration
	}
	require.NoError(t, iter.Close())

	require.Len(t, results, 3, "expected exactly 3 unique users after deduplication across both datastores")
	require.Contains(t, results, "alice", "alice (primary store only) should be present")
	require.Contains(t, results, "bob", "bob should appear exactly once after deduplication")
	require.Contains(t, results, "carol", "carol (fallback store only) should be present")

	// The primary-store version of bob must take precedence over the fallback-store version.
	assert.Equal(t, "bob@primary.example", results["bob"].Email,
		"primary datastore version of bob should be preferred over fallback")
}

// TestGetUsersPaginationDualMetadataStore verifies that GetUsers correctly paginates through
// all users when the internal QueryPaginationLimit is much smaller than the total number of
// users.  Users are spread across the primary and fallback metadata stores with deliberate
// duplicates to validate that:
//
//   - The pagination loop inside GetUsers fetches every page until exhausted.
//   - Duplicate usernames present in both stores are deduplicated to a single entry.
//   - The primary-store version is preferred for duplicates.
//   - The final result set contains exactly the expected number of unique users.
func TestGetUsersPaginationDualMetadataStore(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })

	primaryStore := bucket.GetMobileSystemDataStore()
	fallbackStore := bucket.DefaultDataStore()
	ms := base.NewMetadataStore(primaryStore, fallbackStore)

	// Initialise data indexes on the default collection.
	setupIndexes(t, bucket, testIndexCreationOptions{
		numPartitions:          db.DefaultNumIndexPartitions,
		useLegacySyncDocsIndex: false,
		useXattrs:              true,
	})

	// Initialise principal indexes on both the primary and fallback metadata stores.
	indexOptions := db.InitializeIndexOptions{
		NumReplicas:                0,
		LegacySyncDocsIndex:        false,
		UseXattrs:                  true,
		NumPartitions:              db.DefaultNumIndexPartitions,
		WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
	}
	require.NoError(t, db.InitializeDualMetadataStoreIndexes(t, ctx, ms, indexOptions))

	// Use a very small pagination limit to force multiple pagination rounds inside GetUsers.
	const paginationLimit = 3

	dbOptions := getDatabaseContextOptions(false)
	dbOptions.Scopes = db.GetScopesOptions(t, bucket, 2)
	dbOptions.EnableXattr = true
	dbOptions.MetadataStore = ms
	dbOptions.QueryPaginationLimit = paginationLimit

	database, dbCtx := db.CreateTestDatabase(t, bucket, dbOptions)
	t.Cleanup(func() { database.Close(dbCtx) })

	authOpts := auth.AuthenticatorOptions{
		LogCtx:   ctx,
		MetaKeys: base.DefaultMetadataKeys,
	}

	createAndSaveUser := func(t *testing.T, store base.DataStore, username, email string) {
		t.Helper()
		authr := auth.NewAuthenticator(store, nil, authOpts)
		user, err := authr.NewUser(username, "password", nil)
		require.NoError(t, err)
		require.NoError(t, user.SetEmail(email))
		require.NoError(t, authr.Save(user))
	}

	// ── Seed users ──
	// Primary-only users (10 users).
	for i := range 10 {
		name := fmt.Sprintf("primary-only-%03d", i)
		createAndSaveUser(t, primaryStore, name, name+"@primary.example")
	}

	// Fallback-only users (10 users).
	for i := range 10 {
		name := fmt.Sprintf("fallback-only-%03d", i)
		createAndSaveUser(t, fallbackStore, name, name+"@fallback.example")
	}

	// Duplicate users present in both stores (5 users).
	// Primary version has @primary.example, fallback has @fallback.example.
	for i := range 5 {
		name := fmt.Sprintf("dup-user-%03d", i)
		createAndSaveUser(t, primaryStore, name, name+"@primary.example")
		createAndSaveUser(t, fallbackStore, name, name+"@fallback.example")
	}

	// Total unique users: 10 (primary-only) + 10 (fallback-only) + 5 (duplicates) = 25.
	const expectedUniqueUsers = 25

	// Call GetUsers with limit=0 (no result cap) — pagination is driven by QueryPaginationLimit.
	users, err := database.GetUsers(dbCtx, 0)
	require.NoError(t, err)
	require.Len(t, users, expectedUniqueUsers,
		"GetUsers should return all %d unique users across paginated queries (pagination limit=%d)",
		expectedUniqueUsers, paginationLimit)

	// Build a lookup of returned users keyed by name for dedup and priority assertions.
	usersByName := make(map[string]auth.PrincipalConfig, len(users))
	for _, u := range users {
		require.NotNil(t, u.Name)
		_, exists := usersByName[*u.Name]
		assert.False(t, exists, "duplicate user %q returned by GetUsers", *u.Name)
		usersByName[*u.Name] = u
	}

	// Verify all primary-only users are present.
	for i := range 10 {
		name := fmt.Sprintf("primary-only-%03d", i)
		assert.Contains(t, usersByName, name)
	}

	// Verify all fallback-only users are present.
	for i := range 10 {
		name := fmt.Sprintf("fallback-only-%03d", i)
		assert.Contains(t, usersByName, name)
	}

	// Verify duplicate users are present with primary-store email (primary takes priority).
	for i := range 5 {
		name := fmt.Sprintf("dup-user-%03d", i)
		require.Contains(t, usersByName, name)
		require.NotNil(t, usersByName[name].Email)
		assert.Equal(t, name+"@primary.example", *usersByName[name].Email,
			"duplicate user %q should use the primary-store version", name)
	}
}

// TestQueryRolesDualMetadataStore verifies that QueryRoles and QueryAllRoles correctly
// deduplicate role results when using a dual MetadataStore. Roles are written to both
// the primary and fallback stores, and the primary version is preferred for duplicates.
//
// Test data:
//   - "admin":     primary only
//   - "editor":    both stores; primary has name="editor" (written first), fallback also has it
//   - "viewer":    fallback only
//
// Expected: three unique roles with "editor" from primary store.
func TestQueryRolesDualMetadataStore(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })

	primaryStore := bucket.GetMobileSystemDataStore()
	fallbackStore := bucket.DefaultDataStore()
	ms := base.NewMetadataStore(primaryStore, fallbackStore)

	setupIndexes(t, bucket, testIndexCreationOptions{
		numPartitions:          db.DefaultNumIndexPartitions,
		useLegacySyncDocsIndex: false,
		useXattrs:              true,
	})

	indexOptions := db.InitializeIndexOptions{
		NumReplicas:                0,
		LegacySyncDocsIndex:        false,
		UseXattrs:                  true,
		NumPartitions:              db.DefaultNumIndexPartitions,
		WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
	}
	require.NoError(t, db.InitializeDualMetadataStoreIndexes(t, ctx, ms, indexOptions))

	dbOptions := getDatabaseContextOptions(false)
	dbOptions.Scopes = db.GetScopesOptions(t, bucket, 2)
	dbOptions.EnableXattr = true
	dbOptions.MetadataStore = ms

	database, dbCtx := db.CreateTestDatabase(t, bucket, dbOptions)
	t.Cleanup(func() { database.Close(dbCtx) })

	authOpts := auth.AuthenticatorOptions{
		LogCtx:   ctx,
		MetaKeys: base.DefaultMetadataKeys,
	}

	createAndSaveRole := func(t *testing.T, store base.DataStore, roleName string) {
		t.Helper()
		authr := auth.NewAuthenticator(store, nil, authOpts)
		role, err := authr.NewRole(roleName, nil)
		require.NoError(t, err)
		require.NoError(t, authr.Save(role))
	}

	createAndSaveRole(t, primaryStore, "admin")   // primary only
	createAndSaveRole(t, primaryStore, "editor")  // both stores
	createAndSaveRole(t, fallbackStore, "editor") // duplicate in fallback
	createAndSaveRole(t, fallbackStore, "viewer") // fallback only

	metaKeys := base.DefaultMetadataKeys

	// drainIter collects all rows from a query iterator into a map keyed by META().id.
	drainIter := func(t *testing.T, iter sgbucket.QueryResultIterator) map[string]db.PrincipalRow {
		t.Helper()
		results := make(map[string]db.PrincipalRow)
		var row db.PrincipalRow
		for iter.Next(dbCtx, &row) {
			results[row.Id] = row
			row = db.PrincipalRow{}
		}
		require.NoError(t, iter.Close())
		return results
	}

	t.Run("QueryRoles", func(t *testing.T) {
		iter, err := database.QueryRoles(dbCtx, "", 0)
		require.NoError(t, err)

		results := drainIter(t, iter)

		adminKey := metaKeys.RoleKey("admin")
		editorKey := metaKeys.RoleKey("editor")
		viewerKey := metaKeys.RoleKey("viewer")

		require.Len(t, results, 3, "expected exactly 3 unique roles after deduplication")
		assert.Contains(t, results, adminKey, "admin should be present")
		assert.Contains(t, results, editorKey, "editor should be present")
		assert.Contains(t, results, viewerKey, "viewer should be present")
	})

	t.Run("QueryAllRoles", func(t *testing.T) {
		iter, err := database.QueryAllRoles(dbCtx, "", 0)
		require.NoError(t, err)

		results := drainIter(t, iter)

		adminKey := metaKeys.RoleKey("admin")
		editorKey := metaKeys.RoleKey("editor")
		viewerKey := metaKeys.RoleKey("viewer")

		require.Len(t, results, 3, "expected exactly 3 unique roles after deduplication")
		assert.Contains(t, results, adminKey, "admin should be present")
		assert.Contains(t, results, editorKey, "editor should be present")
		assert.Contains(t, results, viewerKey, "viewer should be present")
	})
}
