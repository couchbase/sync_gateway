//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package attachmentmigrationtest

import (
	"fmt"
	"net/http"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigrationJobStartOnDbStart:
//   - Create a db
//   - Grab attachment migration manager and assert it has run upon db startup
//   - Assert job has written syncInfo metaVersion as expected to the bucket
func TestMigrationJobStartOnDbStart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := rt.Context()

	ds := rt.GetSingleDataStore()
	dbCtx := rt.GetDatabase()

	mgr := dbCtx.AttachmentMigrationManager

	// wait for migration job to finish
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateCompleted)

	// assert that sync info with metadata version written to the collection
	db.AssertSyncInfoMetaVersion(t, ds)
}

// TestChangeDbCollectionsRestartMigrationJob:
//   - Add docs before job starts, this will test that the dcp checkpoint are correctly reset upon db update later in test
//   - Create db with collection one
//   - Assert the attachment migration job is running
//   - Update db config to include a new collection
//   - Assert job runs/completes
//   - As the job should've purged dcp collections upon new collection being added to db we expect some added docs
//     to be processed twice in the job, so we can assert that the job has processed more docs than we added
//   - Assert sync info: metaVersion is written to BOTH collections in the db config
func TestChangeDbCollectionsRestartMigrationJob(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()
	ctx := rt.Context()
	_ = rt.Bucket()

	const (
		dbName         = "db1"
		totalDocsAdded = 8000
	)

	ds0, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	ds1, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	opts := &sgbucket.MutateInOptions{}

	// add some docs (with xattr so they won't be ignored in the background job) to both collections
	// we want to add large number of docs to stop the migration job from finishing before we can assert on state
	bodyBytes := []byte(`{"some": "body"}`)
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds0.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)

		_, writeErr = ds1.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfigC1Only := rest.GetCollectionsConfig(t, tb, 2)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfigC1Only)
	scope := dataStoreNames[0].ScopeName()
	collection1 := dataStoreNames[0].CollectionName()
	collection2 := dataStoreNames[1].CollectionName()
	delete(scopesConfigC1Only[scope].Collections, collection2)

	scopesConfigBothCollection := rest.GetCollectionsConfig(t, tb, 2)

	// Create a db1 with one collection initially
	dbConfig := rt.NewDbConfig()
	// ensure import is off to stop the docs we add from being imported by sync gateway, this could cause extra overhead
	// on the migration job (more doc writes going to bucket). We want to avoid for purpose of this test
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigC1Only

	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx := rt.GetDatabase()
	mgr := dbCtx.AttachmentMigrationManager
	scNames := base.ScopeAndCollectionNames{base.ScopeAndCollectionName{Scope: scope, Collection: collection1}}
	assert.ElementsMatch(t, scNames, dbCtx.RequireAttachmentMigration)
	// wait for migration job to start
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateRunning)

	// update db config to include second collection
	dbConfig = rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigBothCollection
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// wait for attachment migration job to start and finish
	dbCtx = rt.GetDatabase()
	mgr = dbCtx.AttachmentMigrationManager
	scNames = append(scNames, base.ScopeAndCollectionName{Scope: scope, Collection: collection2})
	assert.ElementsMatch(t, scNames, dbCtx.RequireAttachmentMigration)
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateRunning)

	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateCompleted)

	var mgrStatus db.AttachmentMigrationManagerResponse
	stat, err := mgr.GetStatus(ctx)
	require.NoError(t, err)
	require.NoError(t, base.JSONUnmarshal(stat, &mgrStatus))
	// assert that number of docs precessed is greater than the total docs added, this will be because when updating
	// the db config to include a new collection this should force reset of DCP checkpoints and start DCP feed from 0 again
	assert.Greater(t, mgrStatus.DocsProcessed, int64(totalDocsAdded))

	// assert that sync info with metadata version written to both collections
	db.AssertSyncInfoMetaVersion(t, ds0)
	db.AssertSyncInfoMetaVersion(t, ds1)
}

// TestMigrationNewCollectionToDbNoRestart:
//   - Create db with one collection
//   - Wait for attachment migration job to finish on that single collection
//   - Assert syncInfo: metaVersion is present in collection
//   - Update db config to include new collection
//   - Assert that the attachment migration task is restarted but only on the one (new) collection
//   - We can do this though asserting the new run only process amount of docs added in second collection
//     after update to db config + assert on collections requiring migration
//   - Assert that syncInfo: metaVersion is written for new collection (and is still present in original collection)
func TestMigrationNewCollectionToDbNoRestart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()
	ctx := rt.Context()
	_ = rt.Bucket()

	const (
		dbName                = "db1"
		totalDocsAddedCollOne = 10
		totalDocsAddedCollTwo = 10
	)

	ds0, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	ds1, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	opts := &sgbucket.MutateInOptions{}

	// add some docs (with xattr so they won't be ignored in the background job) to both collections
	bodyBytes := []byte(`{"some": "body"}`)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds0.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)

		_, writeErr = ds1.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfigC1Only := rest.GetCollectionsConfig(t, tb, 2)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfigC1Only)
	scope := dataStoreNames[0].ScopeName()
	collection2 := dataStoreNames[1].CollectionName()
	delete(scopesConfigC1Only[scope].Collections, collection2)

	// Create a db1 with one collection initially
	dbConfig := rt.NewDbConfig()
	// ensure import is off to stop the docs we add from being imported by sync gateway, this could cause extra overhead
	// on the migration job (more doc writes going to bucket). We want to avoid for purpose of this test
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigC1Only
	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx := rt.GetDatabase()
	mgr := dbCtx.AttachmentMigrationManager
	assert.Len(t, dbCtx.RequireAttachmentMigration, 1)
	// wait for migration job to finish on single collection
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateCompleted)

	var mgrStatus db.AttachmentMigrationManagerResponse
	stat, err := mgr.GetStatus(ctx)
	require.NoError(t, err)
	require.NoError(t, base.JSONUnmarshal(stat, &mgrStatus))
	// assert that number of docs precessed is equal to docs in collection 1
	assert.Equal(t, int64(totalDocsAddedCollOne), mgrStatus.DocsProcessed)

	// assert sync info meta version exists for this collection
	db.AssertSyncInfoMetaVersion(t, ds0)

	// create db with second collection, background job should only run on new collection added given
	// existent of sync info meta version on collection 1
	scopesConfigBothCollection := rest.GetCollectionsConfig(t, tb, 2)
	dbConfig = rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigBothCollection
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx = rt.GetDatabase()
	mgr = dbCtx.AttachmentMigrationManager
	assert.Len(t, dbCtx.RequireAttachmentMigration, 1)
	// wait for migration job to finish on the new collection
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateCompleted)

	mgrStatus = db.AttachmentMigrationManagerResponse{}
	stat, err = mgr.GetStatus(ctx)
	require.NoError(t, err)
	require.NoError(t, base.JSONUnmarshal(stat, &mgrStatus))
	// assert that number of docs precessed is equal to docs in collection 2 (not the total number of docs added across
	// the collections, as we'd expect if the process had reset)
	assert.Equal(t, int64(totalDocsAddedCollTwo), mgrStatus.DocsProcessed)

	// assert that sync info with metadata version written to both collections
	db.AssertSyncInfoMetaVersion(t, ds0)
	db.AssertSyncInfoMetaVersion(t, ds1)
}

// TestMigrationNoReRunStartStopDb:
//   - Create db
//   - Wait for attachment migration task to finish
//   - Update db config to trigger reload of db
//   - Assert that the migration job is not re-run (docs processed is the same as before + collections
//     requiring migration is empty)
func TestMigrationNoReRunStartStopDb(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()
	ctx := rt.Context()
	_ = rt.Bucket()

	const (
		dbName         = "db1"
		totalDocsAdded = 20
	)

	ds0, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	ds1, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	opts := &sgbucket.MutateInOptions{}

	// add some docs (with xattr so they won't be ignored in the background job) to both collections
	bodyBytes := []byte(`{"some": "body"}`)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds0.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)

		_, writeErr = ds1.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfigBothCollection := rest.GetCollectionsConfig(t, tb, 2)
	dbConfig := rt.NewDbConfig()
	// ensure import is off to stop the docs we add from being imported by sync gateway, this could cause extra overhead
	// on the migration job (more doc writes going to bucket). We want to avoid for purpose of this test
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigBothCollection
	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx := rt.GetDatabase()
	assert.Len(t, dbCtx.RequireAttachmentMigration, 2)
	mgr := dbCtx.AttachmentMigrationManager
	// wait for migration job to finish on both collections
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateCompleted)

	var mgrStatus db.AttachmentMigrationManagerResponse
	stat, err := mgr.GetStatus(ctx)
	require.NoError(t, err)
	require.NoError(t, base.JSONUnmarshal(stat, &mgrStatus))
	// assert that number of docs precessed is equal to docs in collection 1
	assert.Equal(t, int64(totalDocsAdded), mgrStatus.DocsProcessed)

	// assert that sync info with metadata version written to both collections
	db.AssertSyncInfoMetaVersion(t, ds0)
	db.AssertSyncInfoMetaVersion(t, ds1)

	// reload db config with a config change
	dbConfig = rt.NewDbConfig()
	dbConfig.AutoImport = true
	dbConfig.Scopes = scopesConfigBothCollection
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx = rt.GetDatabase()
	mgr = dbCtx.AttachmentMigrationManager
	// assert that the job remains in completed state (not restarted)
	mgrStatus = db.AttachmentMigrationManagerResponse{}
	stat, err = mgr.GetStatus(ctx)
	require.NoError(t, err)
	require.NoError(t, base.JSONUnmarshal(stat, &mgrStatus))
	assert.Equal(t, db.BackgroundProcessStateCompleted, mgrStatus.State)
	assert.Equal(t, int64(totalDocsAdded), mgrStatus.DocsProcessed)
	assert.Len(t, dbCtx.RequireAttachmentMigration, 0)
}

// TestStartMigrationAlreadyRunningProcess:
//   - Create db
//   - Wait for migration job to start
//   - Attempt to start job again on manager, assert we get error
func TestStartMigrationAlreadyRunningProcess(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 1)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := rest.NewRestTester(t, rtConfig)
	defer rt.Close()
	ctx := rt.Context()
	_ = rt.Bucket()

	const (
		dbName = "db1"
	)

	ds0, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	opts := &sgbucket.MutateInOptions{}

	// add some docs (with xattr so they won't be ignored in the background job) to both collections
	// we want to add large number of docs to stop the migration job from finishing before we can try start the job
	// again (whilst already running)
	bodyBytes := []byte(`{"some": "body"}`)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds0.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfig := rest.GetCollectionsConfig(t, tb, 1)
	dbConfig := rt.NewDbConfig()
	// ensure import is off to stop the docs we add from being imported by sync gateway, this could cause extra overhead
	// on the migration job (more doc writes going to bucket). We want to avoid for purpose of this test
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfig
	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)
	dbCtx := rt.GetDatabase()
	nodeMgr := dbCtx.AttachmentMigrationManager
	// wait for migration job to start
	db.RequireBackgroundManagerState(t, ctx, nodeMgr, db.BackgroundProcessStateRunning)

	err = nodeMgr.Start(ctx, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Process already running")
}
