//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachmentMigrationTaskMixMigratedAndNonMigratedDocs(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create some docs with attachments defined
	for i := 0; i < 10; i++ {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, doc, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
		assert.NotNil(t, doc.SyncData.Attachments)
	}

	// Move some subset of the documents attachment metadata from global sync to sync data
	for j := 0; j < 5; j++ {
		key := fmt.Sprintf("%s_%d", t.Name(), j)
		value, xattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName, base.GlobalXattrName})
		require.NoError(t, err)
		syncXattr, ok := xattrs[base.SyncXattrName]
		assert.True(t, ok)
		globalXattr, ok := xattrs[base.GlobalXattrName]
		assert.True(t, ok)

		var attachs GlobalSyncData
		err = base.JSONUnmarshal(globalXattr, &attachs)
		require.NoError(t, err)

		MoveAttachmentXattrFromGlobalToSync(t, ctx, key, cas, value, syncXattr, attachs.GlobalAttachments, true, collection.dataStore)
	}

	attachMigrationMgr := NewAttachmentMigrationManager(db.DatabaseContext)
	require.NotNil(t, attachMigrationMgr)

	err := attachMigrationMgr.Start(ctx, nil)
	require.NoError(t, err)

	// wait for task to complete
	RequireBackgroundManagerState(t, ctx, attachMigrationMgr, BackgroundProcessStateCompleted)

	// assert that the subset (5) of the docs were changed, all created docs were processed (10)
	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	assert.Equal(t, int64(10), stats.DocsProcessed)
	assert.Equal(t, int64(5), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, base.ProductAPIVersion, syncInfo.MetaDataVersion)

}

func getAttachmentMigrationStats(migrationManager BackgroundManagerProcessI) AttachmentMigrationManagerResponse {
	var resp AttachmentMigrationManagerResponse
	rawStatus, _, _ := migrationManager.GetProcessStatus(BackgroundManagerStatus{})
	_ = base.JSONUnmarshal(rawStatus, &resp)
	return resp
}

func TestAttachmentMigrationManagerResumeStoppedMigration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	base.LongRunningTest(t)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create some docs with attachments defined, a large number is needed to allow us to stop the migration midway
	// through without it completing first
	for i := 0; i < 4000; i++ {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, doc, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
		require.NotNil(t, doc.SyncData.Attachments)
	}
	attachMigrationMgr := NewAttachmentMigrationManager(db.DatabaseContext)
	require.NotNil(t, attachMigrationMgr)

	err := attachMigrationMgr.Start(ctx, nil)
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
			if stats.DocsProcessed >= 200 {
				err = attachMigrationMgr.Stop()
				require.NoError(t, err)
				break
			}
			time.Sleep(1 * time.Microsecond)
		}
	}()

	RequireBackgroundManagerState(t, ctx, attachMigrationMgr, BackgroundProcessStateStopped)

	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	require.Less(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version is not present
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.Error(t, err)

	// Resume process
	err = attachMigrationMgr.Start(ctx, nil)
	require.NoError(t, err)

	RequireBackgroundManagerState(t, ctx, attachMigrationMgr, BackgroundProcessStateCompleted)

	stats = getAttachmentMigrationStats(attachMigrationMgr.Process)
	require.GreaterOrEqual(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version doc has been written to the database collection
	syncInfo = base.SyncInfo{}
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, base.ProductAPIVersion, syncInfo.MetaDataVersion)
}

func TestAttachmentMigrationManagerNoDocsToMigrate(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a doc with no attachments defined but through sync gateway, so it will have sync data
	docBody := Body{
		"value": "doc",
	}
	key := fmt.Sprintf("%s_%d", t.Name(), 1)
	_, _, err := collection.Put(ctx, key, docBody)
	require.NoError(t, err)

	// add new doc with no sync data (SDK write, no import)
	key = fmt.Sprintf("%s_%d", t.Name(), 2)
	_, err = collection.dataStore.Add(key, 0, []byte(`{"test":"doc"}`))
	require.NoError(t, err)

	attachMigrationMgr := NewAttachmentMigrationManager(db.DatabaseContext)
	require.NotNil(t, attachMigrationMgr)

	err = attachMigrationMgr.Start(ctx, nil)
	require.NoError(t, err)

	// wait for task to complete
	RequireBackgroundManagerState(t, ctx, attachMigrationMgr, BackgroundProcessStateCompleted)

	// assert that the two added docs above were processed but not changed
	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	// no docs should be changed, only one has xattr defined thus should only have one of the two docs processed
	assert.Equal(t, int64(1), stats.DocsProcessed)
	assert.Equal(t, int64(0), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, base.ProductAPIVersion, syncInfo.MetaDataVersion)
}

func TestMigrationManagerDocWithSyncAndGlobalAttachmentMetadata(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docBody := Body{
		"value":         1234,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	key := t.Name()
	_, _, err := collection.Put(ctx, key, docBody)
	require.NoError(t, err)

	xattrs, cas, err := collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName, base.GlobalXattrName})
	require.NoError(t, err)
	require.Contains(t, xattrs, base.GlobalXattrName)
	require.Contains(t, xattrs, base.SyncXattrName)

	var syncData SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
	// define some attachment meta on sync data
	syncData.Attachments = AttachmentsMeta{}
	att := map[string]interface{}{
		"stub": true,
	}
	syncData.Attachments["someAtt.txt"] = att

	updateXattrs := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, syncData),
	}
	_, err = collection.dataStore.UpdateXattrs(ctx, key, 0, cas, updateXattrs, DefaultMutateInOpts())
	require.NoError(t, err)

	attachMigrationMgr := NewAttachmentMigrationManager(db.DatabaseContext)
	require.NotNil(t, attachMigrationMgr)

	err = attachMigrationMgr.Start(ctx, nil)
	require.NoError(t, err)

	// wait for task to complete
	RequireBackgroundManagerState(t, ctx, attachMigrationMgr, BackgroundProcessStateCompleted)

	// assert that the two added docs above were processed but not changed
	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	assert.Equal(t, int64(1), stats.DocsProcessed)
	assert.Equal(t, int64(1), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, base.ProductAPIVersion, syncInfo.MetaDataVersion)

	xattrs, _, err = collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName, base.GlobalXattrName})
	require.NoError(t, err)
	require.Contains(t, xattrs, base.GlobalXattrName)
	require.Contains(t, xattrs, base.SyncXattrName)

	var globalSync GlobalSyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.GlobalXattrName], &globalSync))
	syncData = SyncData{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))

	require.NotNil(t, globalSync.GlobalAttachments)
	assert.NotNil(t, globalSync.GlobalAttachments["someAtt.txt"])
	assert.NotNil(t, globalSync.GlobalAttachments["myatt"])
	assert.Nil(t, syncData.Attachments)
}
