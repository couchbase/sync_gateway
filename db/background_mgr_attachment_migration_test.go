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

	attachMigrationMgr := NewAttachmentMigrationManager(db.MetadataStore, db.MetadataKeys)
	require.NotNil(t, attachMigrationMgr)

	options := map[string]interface{}{
		"database": db,
	}
	err := attachMigrationMgr.Start(ctx, options)
	require.NoError(t, err)

	// wait for task to complete
	err = WaitForConditionWithOptions(t, func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := attachMigrationMgr.GetStatus(ctx)
		_ = base.JSONUnmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateCompleted
	}, 200, 200)
	require.NoError(t, err)

	// assert that the subset (5) of the docs were changed, all created docs were processed (10)
	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	assert.Equal(t, int64(10), stats.DocsProcessed)
	assert.Equal(t, int64(5), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, int8(4), syncInfo.MetaDataVersion)

}

func getAttachmentMigrationStats(resyncManager BackgroundManagerProcessI) ResyncManagerResponseDCP {
	var resp ResyncManagerResponseDCP
	rawStatus, _, _ := resyncManager.GetProcessStatus(BackgroundManagerStatus{})
	_ = base.JSONUnmarshal(rawStatus, &resp)
	return resp
}

func rawDocWithAttachmentAndSyncMeta() []byte {
	return []byte(`{
   "_sync": {
      "rev": "1-5fc93bd36377008f96fdae2719c174ed",
      "sequence": 2,
      "recent_sequences": [
         2
      ],
      "history": {
         "revs": [
            "1-5fc93bd36377008f96fdae2719c174ed"
         ],
         "parents": [
            -1
         ],
         "channels": [
            null
         ]
      },
      "cas": "",
      "attachments": {
         "hi.txt": {
            "revpos": 1,
            "content_type": "text/plain",
            "length": 2,
            "stub": true,
            "digest": "sha1-witfkXg0JglCjW9RssWvTAveakI="
         }
      },
      "time_saved": "2021-09-01T17:33:03.054227821Z"
   },
  "key": "value"
}`)
}

func createDocWithLegacyAttachment(t *testing.T, collection *DatabaseCollectionWithUser, docID string, rawDoc []byte, attKey string, attBody []byte) {
	// Write attachment directly to the bucket.
	_, err := collection.dataStore.Add(attKey, 0, attBody)
	require.NoError(t, err)

	body := Body{}
	err = body.Unmarshal(rawDoc)
	require.NoError(t, err, "Error unmarshalling body")

	// Write raw document to the bucket.
	_, err = collection.dataStore.Add(docID, 0, rawDoc)
	require.NoError(t, err)
}

func TestAttachmentMigrationTaskInlineSyncData(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create doc with legacy attachment + inline sync data
	inlineDoc := "doc1"
	attBody := []byte(`hi`)
	digest := Sha1DigestKey(attBody)
	attKey := MakeAttachmentKey(AttVersion1, inlineDoc, digest)
	rawDoc := rawDocWithAttachmentAndSyncMeta()
	createDocWithLegacyAttachment(t, collection, inlineDoc, rawDoc, attKey, attBody)

	// create a doc with attachment defined (no inline sync data)
	docBody := Body{
		"value":         1234,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	docWithXattrs := t.Name()
	_, doc, err := collection.Put(ctx, docWithXattrs, docBody)
	require.NoError(t, err)
	assert.NotNil(t, doc.SyncData.Attachments)

	value, xattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, docWithXattrs, []string{base.SyncXattrName, base.GlobalXattrName})
	require.NoError(t, err)
	syncXattr, ok := xattrs[base.SyncXattrName]
	assert.True(t, ok)
	globalXattr, ok := xattrs[base.GlobalXattrName]
	assert.True(t, ok)

	var attachs GlobalSyncData
	err = base.JSONUnmarshal(globalXattr, &attachs)
	require.NoError(t, err)

	// move doc attachment metadata back to sync data for testing purposes
	MoveAttachmentXattrFromGlobalToSync(t, ctx, docWithXattrs, cas, value, syncXattr, attachs.GlobalAttachments, true, collection.dataStore)

	attachMigrationMgr := NewAttachmentMigrationManager(db.MetadataStore, db.MetadataKeys)
	require.NotNil(t, attachMigrationMgr)

	options := map[string]interface{}{
		"database": db,
	}
	err = attachMigrationMgr.Start(ctx, options)
	require.NoError(t, err)

	// wait for task to complete
	err = WaitForConditionWithOptions(t, func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := attachMigrationMgr.GetStatus(ctx)
		_ = base.JSONUnmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateCompleted
	}, 200, 200)
	require.NoError(t, err)

	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	assert.Equal(t, int64(2), stats.DocsProcessed)
	assert.Equal(t, int64(2), stats.DocsChanged)

	// assert attachment metadata is not present in sync xattr but is present in global sync xattr
	_, xattrs, cas, err = collection.dataStore.GetWithXattrs(ctx, docWithXattrs, []string{base.SyncXattrName, base.GlobalXattrName})
	require.NoError(t, err)
	syncXattr, ok = xattrs[base.SyncXattrName]
	assert.True(t, ok)
	globalXattr, ok = xattrs[base.GlobalXattrName]
	assert.True(t, ok)

	attachs = GlobalSyncData{}
	require.NoError(t, base.JSONUnmarshal(globalXattr, &attachs))
	require.NotNil(t, attachs.GlobalAttachments)
	var syncData SyncData
	require.NoError(t, base.JSONUnmarshal(syncXattr, &syncData))
	require.Nil(t, syncData.Attachments)

	_, xattrs, cas, err = collection.dataStore.GetWithXattrs(ctx, inlineDoc, []string{base.SyncXattrName, base.GlobalXattrName})
	require.NoError(t, err)
	syncXattr, ok = xattrs[base.SyncXattrName]
	assert.True(t, ok)
	globalXattr, ok = xattrs[base.GlobalXattrName]
	assert.True(t, ok)

	attachs = GlobalSyncData{}
	require.NoError(t, base.JSONUnmarshal(globalXattr, &attachs))
	require.NotNil(t, attachs.GlobalAttachments)
	syncData = SyncData{}
	require.NoError(t, base.JSONUnmarshal(syncXattr, &syncData))
	require.Nil(t, syncData.Attachments)

	// assert that the sync info metadata version doc has been written to the database collection
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, int8(4), syncInfo.MetaDataVersion)

}

func TestAttachmentMigrationManagerResumeStoppedMigration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create some docs with attachments defined
	for i := 0; i < 4000; i++ {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, doc, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
		assert.NotNil(t, doc.SyncData.Attachments)
	}
	attachMigrationMgr := NewAttachmentMigrationManager(db.MetadataStore, db.MetadataKeys)
	require.NotNil(t, attachMigrationMgr)

	options := map[string]interface{}{
		"database": db,
	}
	err := attachMigrationMgr.Start(ctx, options)
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			stats := getResyncStats(attachMigrationMgr.Process)
			if stats.DocsProcessed >= 200 {
				err = attachMigrationMgr.Stop()
				require.NoError(t, err)
				break
			}
			time.Sleep(1 * time.Microsecond)
		}
	}()

	err = WaitForConditionWithOptions(t, func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := attachMigrationMgr.GetStatus(ctx)
		_ = base.JSONUnmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateStopped
	}, 200, 200)
	require.NoError(t, err)

	stats := getAttachmentMigrationStats(attachMigrationMgr.Process)
	require.Less(t, stats.DocsProcessed, int64(2000))

	// assert that the sync info metadata version is not present
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.Error(t, err)

	// Resume process
	err = attachMigrationMgr.Start(ctx, options)
	require.NoError(t, err)

	err = WaitForConditionWithOptions(t, func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := attachMigrationMgr.GetStatus(ctx)
		_ = base.JSONUnmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateCompleted
	}, 200, 200)
	require.NoError(t, err)

	stats = getAttachmentMigrationStats(attachMigrationMgr.Process)
	require.GreaterOrEqual(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version doc has been written to the database collection
	syncInfo = base.SyncInfo{}
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, int8(4), syncInfo.MetaDataVersion)
}
