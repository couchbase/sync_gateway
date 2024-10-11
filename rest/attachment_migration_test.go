//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationJobStartOnDbStart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := base.TestCtx(t)

	ds := rt.GetSingleDataStore()
	dbCtx := rt.GetDatabase()

	mgr := dbCtx.AttachmentMigrationManager

	// wait for migration job to finish
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateCompleted)

	// assert that sync info with metadata version written to the collection
	syncInf := base.SyncInfo{}
	_, err := ds.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)

}

func TestChangeDbCollectionsRestartMigrationJob(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	base.LongRunningTest(t)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()
	ctx := base.TestCtx(t)
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
	bodyBytes := []byte(`{"some": "body"}`)
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds0.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("%s_%d", t.Name()+"greg", i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds1.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfigC1Only := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfigC1Only)
	scope := dataStoreNames[0].ScopeName()
	collection2 := dataStoreNames[1].CollectionName()
	delete(scopesConfigC1Only[scope].Collections, collection2)

	scopesConfigBothCollection := GetCollectionsConfig(t, tb, 2)

	// Create a db1 with one collection initially
	dbConfig := rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigC1Only

	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	dbCtx := rt.GetDatabase()
	mgr := dbCtx.AttachmentMigrationManager
	assert.Len(t, dbCtx.RequireAttachmentMigration, 1)
	// wait for migration job to start
	db.RequireBackgroundManagerState(t, ctx, mgr, db.BackgroundProcessStateRunning)

	// update db config to include second collection
	dbConfig = rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigBothCollection
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// wait for attachment migration job to start and finish
	dbCtx = rt.GetDatabase()
	mgr = dbCtx.AttachmentMigrationManager
	assert.Len(t, dbCtx.RequireAttachmentMigration, 2)
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
	syncInf := base.SyncInfo{}
	_, err = ds0.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)
	syncInf = base.SyncInfo{}
	_, err = ds1.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)
}

func TestMigrationNewCollectionToDbNoRestart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()
	ctx := base.TestCtx(t)
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
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", "greg", i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds1.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfigC1Only := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfigC1Only)
	scope := dataStoreNames[0].ScopeName()
	collection2 := dataStoreNames[1].CollectionName()
	delete(scopesConfigC1Only[scope].Collections, collection2)

	// Create a db1 with one collection initially
	dbConfig := rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigC1Only
	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

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
	syncInf := base.SyncInfo{}
	_, err = ds0.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)

	// create db with second collection, background job should only run on new collection added given
	// existent of sync info meta version on collection 1
	scopesConfigBothCollection := GetCollectionsConfig(t, tb, 2)
	dbConfig = rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigBothCollection
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

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
	syncInf = base.SyncInfo{}
	_, err = ds0.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)
	syncInf = base.SyncInfo{}
	_, err = ds1.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)
}

func TestMigrationNoReRunStartStopDb(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()
	ctx := base.TestCtx(t)
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
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", "greg", i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds1.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfigBothCollection := GetCollectionsConfig(t, tb, 2)
	dbConfig := rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfigBothCollection
	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

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
	syncInf := base.SyncInfo{}
	_, err = ds0.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)
	syncInf = base.SyncInfo{}
	_, err = ds1.Get(base.SGSyncInfo, &syncInf)
	require.NoError(t, err)
	assert.Equal(t, "4.0", syncInf.MetaDataVersion)

	// reload db config with a config change
	dbConfig = rt.NewDbConfig()
	dbConfig.AutoImport = true
	dbConfig.Scopes = scopesConfigBothCollection
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

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

func TestStartMigrationAlreadyRunningProcess(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 1)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := NewRestTester(t, rtConfig)
	defer rt.Close()
	ctx := base.TestCtx(t)
	_ = rt.Bucket()

	const (
		dbName = "db1"
	)

	ds0, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	opts := &sgbucket.MutateInOptions{}

	// add some docs (with xattr so they won't be ignored in the background job) to both collections
	bodyBytes := []byte(`{"some": "body"}`)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		xattrsInput := map[string][]byte{
			"_xattr": []byte(`{"some":"xattr"}`),
		}
		_, writeErr := ds0.WriteWithXattrs(ctx, key, 0, 0, bodyBytes, xattrsInput, nil, opts)
		require.NoError(t, writeErr)
	}

	scopesConfig := GetCollectionsConfig(t, tb, 1)
	dbConfig := rt.NewDbConfig()
	dbConfig.AutoImport = false
	dbConfig.Scopes = scopesConfig
	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
	dbCtx := rt.GetDatabase()
	nodeMgr := dbCtx.AttachmentMigrationManager
	// wait for migration job to start
	db.RequireBackgroundManagerState(t, ctx, nodeMgr, db.BackgroundProcessStateRunning)

	err = nodeMgr.Start(ctx, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Process already running")
}
