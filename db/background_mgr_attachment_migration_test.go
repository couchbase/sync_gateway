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
	"maps"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestAttachmentMigrationTaskMixMigratedAndNonMigratedDocs(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create some docs with attachments defined
	for i := range 10 {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, doc, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
		assert.NotNil(t, doc.Attachments())
	}

	// Move some subset of the documents attachment metadata from global sync to sync data
	for j := range 5 {
		key := fmt.Sprintf("%s_%d", t.Name(), j)
		value, _, err := collection.dataStore.GetRaw(ctx, key)
		require.NoError(t, err)

		MoveAttachmentXattrFromGlobalToSync(t, collection.dataStore, key, value, true)
	}

	err := db.AttachmentMigrationManager.Start(ctx, AttachmentMigrationOptions{})
	require.NoError(t, err)

	// wait for task to complete
	RequireBackgroundManagerState(t, db.AttachmentMigrationManager, BackgroundProcessStateCompleted)

	// assert that the subset (5) of the docs were changed, all created docs were processed (10)
	stats := getAttachmentMigrationStats(t, db)
	assert.Equal(t, int64(10), stats.DocsProcessed)
	assert.Equal(t, int64(5), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)

}

func getAttachmentMigrationStats(t *testing.T, db *Database) AttachmentMigrationManagerResponse {
	var resp AttachmentMigrationManagerResponse
	rawStatus, err := db.AttachmentMigrationManager.GetStatus(base.TestCtx(t))
	require.NoError(t, err)
	require.NoError(t, base.JSONUnmarshal(rawStatus, &resp))
	return resp
}

// waitForAttachmentMigrationDocsProcessed waits until the resync manager has processed at least the specified count of documents.
func waitForAttachmentMigrationDocsProcessed(t testing.TB, db *Database, count int64) {
	// this intentionally uses a very short poll interval to catch progress as quickly as possible
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// Poll the local status so the wait can stop as soon as the requested progress is observed,
		// without waiting for the cluster status' periodic update.
		rawStatus, _, err := db.AttachmentMigrationManager.Process.GetProcessStatus(BackgroundManagerStatus{}, nil)
		assert.NoError(c, err)
		var stats AttachmentMigrationManagerResponse
		require.NoError(c, base.JSONUnmarshal(rawStatus, &stats))
		assert.GreaterOrEqual(c, stats.DocsProcessed, count)
	}, 1*time.Minute, 10*time.Millisecond)
}

func TestAttachmentMigrationManagerResumeStoppedMigration(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create some docs with attachments defined, a large number is needed to allow us to stop the migration midway
	// through without it completing first
	for i := range 4000 {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, doc, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
		require.NotNil(t, doc.Attachments())
	}

	err := db.AttachmentMigrationManager.Start(ctx, AttachmentMigrationOptions{})
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitForAttachmentMigrationDocsProcessed(t, db, 200)
		err = db.AttachmentMigrationManager.Stop(ctx)
		require.NoError(t, err)
	}()

	RequireBackgroundManagerState(t, db.AttachmentMigrationManager, BackgroundProcessStateStopped)

	stats := getAttachmentMigrationStats(t, db)
	require.Less(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version is not present
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(ctx, base.SGSyncInfo, &syncInfo)
	require.Error(t, err)

	// Resume process
	err = db.AttachmentMigrationManager.Start(ctx, AttachmentMigrationOptions{})
	require.NoError(t, err)

	RequireBackgroundManagerState(t, db.AttachmentMigrationManager, BackgroundProcessStateCompleted)

	stats = getAttachmentMigrationStats(t, db)
	require.GreaterOrEqual(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)
}

func TestAttachmentMigrationManagerNoDocsToMigrate(t *testing.T) {
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
	_, err = collection.dataStore.Add(ctx, key, 0, []byte(`{"test":"doc"}`))
	require.NoError(t, err)

	err = db.AttachmentMigrationManager.Start(ctx, AttachmentMigrationOptions{})
	require.NoError(t, err)

	// wait for task to complete
	RequireBackgroundManagerState(t, db.AttachmentMigrationManager, BackgroundProcessStateCompleted)

	// assert that the two added docs above were processed but not changed
	stats := getAttachmentMigrationStats(t, db)
	// no docs should be changed, only one has xattr defined thus should only have one of the two docs processed
	assert.Equal(t, int64(1), stats.DocsProcessed)
	assert.Equal(t, int64(0), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)
}

func TestMigrationManagerDocWithSyncAndGlobalAttachmentMetadata(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docBody := Body{
		"value":         1234,
		BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	key := t.Name()
	_, _, err := collection.Put(ctx, key, docBody)
	require.NoError(t, err)

	xattrs, cas, err := collection.dataStore.GetXattrs(ctx, key, []string{base.SyncXattrName, base.GlobalXattrName})
	require.NoError(t, err)
	require.Contains(t, maps.Keys(xattrs), base.GlobalXattrName)
	require.Contains(t, maps.Keys(xattrs), base.SyncXattrName)

	var syncData SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
	// define some attachment meta on sync data
	syncData.AttachmentsPre4dot0 = AttachmentsMeta{}
	att := map[string]any{
		"stub":   true,
		"digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
		"length": 11,
		"revpos": 1,
	}
	syncData.AttachmentsPre4dot0["someAtt.txt"] = att

	updateXattrs := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, syncData),
	}
	_, err = collection.dataStore.UpdateXattrs(ctx, key, 0, cas, updateXattrs, DefaultMutateInOpts())
	require.NoError(t, err)

	err = db.AttachmentMigrationManager.Start(ctx, AttachmentMigrationOptions{})
	require.NoError(t, err)

	// wait for task to complete
	RequireBackgroundManagerState(t, db.AttachmentMigrationManager, BackgroundProcessStateCompleted)

	// assert that the two added docs above were processed but not changed
	stats := getAttachmentMigrationStats(t, db)
	assert.Equal(t, int64(1), stats.DocsProcessed)
	assert.Equal(t, int64(1), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)

	require.Equal(t, AttachmentMap{
		"someAtt.txt": {
			Stub:   true,
			Digest: "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
			Length: 11,
			Revpos: 1,
		},
		"myatt": {
			Stub:        true,
			ContentType: "text/plain",
			Version:     2,
			Length:      12,
			Digest:      "sha1-Lve95gjOVATpfV8EL5X4nxwjKHE=",
			Revpos:      1,
		},
	}, GetRawGlobalSyncAttachments(t, collection.dataStore, key))
	require.Empty(t, GetRawSyncXattr(t, collection.dataStore, key).AttachmentsPre4dot0)
}

func TestAttachmentMigrationCheckpointPrefix(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	migrationID := "1234"
	testCases := []struct {
		name          string
		collectionIDs []uint32
		groupID       string
		expected      string
	}{
		{
			name:          "default collection, no group id",
			collectionIDs: []uint32{base.DefaultCollectionID},
			groupID:       "",
			expected:      "_sync:dcp_ck::sg:att_migration:1234",
		},
		{
			name:          "default collection, group ID=foo",
			collectionIDs: []uint32{base.DefaultCollectionID},
			groupID:       "foo",
			expected:      "_sync:dcp_ck:foo::sg:att_migration:1234",
		},
		{
			name:          "default collection + collection 1, no group id",
			collectionIDs: []uint32{base.DefaultCollectionID, 1},
			groupID:       "",
			expected:      "_sync:dcp_ck::sg:att_migration:1234",
		},
		{
			name:          "default collection + collection 1, group ID=foo",
			collectionIDs: []uint32{base.DefaultCollectionID, 1},
			groupID:       "foo",
			expected:      "_sync:dcp_ck:foo::sg:att_migration:1234",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			autoImport := false
			db, err := NewDatabaseContext(
				ctx,
				"db",
				bucket.NoCloseClone(),
				autoImport,
				DatabaseContextOptions{
					Scopes:  GetScopesOptions(t, bucket, 1),
					GroupID: test.groupID,
				},
			)
			require.NoError(t, err)
			defer db.Close(ctx)

			mgr := NewAttachmentMigrationManager(db)
			clientOptions := mgr.Process.(*AttachmentMigrationManager).getDCPClientOptions(migrationID, db.collectionNameSet(), func(sgbucket.FeedEvent) bool {
				require.FailNow(t, "DCP callback should not be called")
				return false
			}, db.MetadataStore)

			dcpClient, err := base.NewDCPClient(
				ctx,
				bucket,
				clientOptions,
			)
			require.NoError(t, err)
			require.Equal(t, test.expected, dcpClient.GetMetadataKeyPrefix())
		})
	}
}

// TestAttachmentMigrationWritesV1SyncInfoAtCcv41 verifies the end-to-end wiring from
// DatabaseContext.ClusterCompatVersion through AttachmentMigrationManager.Run into
// base.SetSyncInfoMetaVersion — when ccv>=4.1 the syncInfo doc is written with the V1
// version-byte prefix.
func TestAttachmentMigrationWritesV1SyncInfoAtCcv41(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	ccv := base.NewClusterCompatVersion(4, 1)
	db.ClusterCompatVersionFunc = func() *base.ClusterCompatVersion { return &ccv }

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	for i := range 3 {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, _, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
	}
	// move one doc's attachment metadata so the migration has work to do
	key := fmt.Sprintf("%s_0", t.Name())
	value, _, err := collection.dataStore.GetRaw(ctx, key)
	require.NoError(t, err)
	MoveAttachmentXattrFromGlobalToSync(t, collection.dataStore, key, value, true)

	mgr := NewAttachmentMigrationManager(db.DatabaseContext)
	require.NotNil(t, mgr)
	require.NoError(t, mgr.Start(ctx, AttachmentMigrationOptions{}))
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateCompleted)

	raw, _, err := collection.dataStore.GetRaw(ctx, base.SGSyncInfo)
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.Equal(t, byte(base.SyncInfoTypeV1), raw[0], "expected V1 prefix byte from attachment migration write at ccv 4.1")
}

// TestAttachmentMigrationCheckpointsRemovedOnCompletion demonstrates the other half of CBG-5041:
// nothing purges DCP checkpoints when a background task finishes successfully.
//
// BackgroundManager's terminal transition persists the final status and deletes the heartbeat doc,
// but never touches checkpoints. The only automatic cleanup in the codebase is a side effect inside
// the gocb client - deactivateVbucket purges when the last vBucket's stream ends cleanly and the
// feed is one-shot. RosmarDCPClient.Close has no equivalent, so on the default unit test backing
// store a completed migration leaves its checkpoint behind forever.
//
// This test therefore fails on rosmar/walrus and passes against Couchbase Server, which is exactly
// the inconsistency the ticket asks us to remove.
func TestAttachmentMigrationCheckpointsRemovedOnCompletion(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	for i := range 10 {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		_, doc, err := collection.Put(ctx, fmt.Sprintf("%s_%d", t.Name(), i), docBody)
		require.NoError(t, err)
		require.NotNil(t, doc.Attachments())
	}

	mgr := db.AttachmentMigrationManager
	process := mgr.Process.(*AttachmentMigrationManager)

	require.NoError(t, mgr.Start(ctx, AttachmentMigrationOptions{}))
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateCompleted)

	migrationID := getAttachmentMigrationStats(t, db).MigrationID
	require.NotEmpty(t, migrationID)

	require.Empty(t, existingDCPCheckpoints(t, ctx, db.DatabaseContext, process.getCheckpointPrefix(migrationID), db.dcpFeedMode()),
		"completed migration run %q left its DCP checkpoints behind", migrationID)
}

// TestAttachmentMigrationResetPurgesStoppedRunCheckpoints is the durable reset test for attachment
// migration - the counterpart to TestAttachmentCompactionResetPurgesStoppedRunCheckpoints.
//
// A run stopped part way through keeps its checkpoints on every backend, because it never reaches the
// gocb client's implicit purge in deactivateVbucket. That is also the realistic reason an operator
// resets: the previous run got stuck. Reset must purge those checkpoints, and does not.
//
// Migration fails here in a subtler way than compaction. It *does* call purgeCheckpoints on every
// fresh run, but aimed at the wrong ID. The order in BackgroundManager.start is resetStatus() ->
// Init() -> Run():
//
//  1. ResetStatus nils both a.CollectionIDs and a.MigrationID
//  2. Init's reset branch sets a.MigrationID to a new UUID, never reading statusDoc.MigrationID
//  3. Run calls resetDCPMetadataIfNeeded, where matchingCollectionIDs compares the now-nil
//     CollectionIDs against the live ones and so always mismatches, always firing the purge - against
//     a.MigrationID, which by then is the *new* ID
//
// So it deletes checkpoints that do not exist yet, logs "Purging invalid checkpoints for background
// task run <new ID>" as though it worked, and leaves the stopped run's documents orphaned.
func TestAttachmentMigrationResetPurgesStoppedRunCheckpoints(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// a large number of docs is needed to stop the migration midway through without it completing first
	for i := range 4000 {
		docBody := Body{
			"value":         1234,
			BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		_, doc, err := collection.Put(ctx, fmt.Sprintf("%s_%d", t.Name(), i), docBody)
		require.NoError(t, err)
		require.NotNil(t, doc.Attachments())
	}

	mgr := db.AttachmentMigrationManager
	process := mgr.Process.(*AttachmentMigrationManager)
	feedMode := db.dcpFeedMode()

	require.NoError(t, mgr.Start(ctx, AttachmentMigrationOptions{}))
	wg := sync.WaitGroup{}
	defer base.WaitWithTimeout(t, &wg, 30*time.Second)
	wg.Go(func() {
		waitForAttachmentMigrationDocsProcessed(t, db, 200)
		require.NoError(t, mgr.Stop(ctx))
	})
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateStopped)

	stoppedMigrationID := getAttachmentMigrationStats(t, db).MigrationID
	require.NotEmpty(t, stoppedMigrationID)

	stoppedPrefix := process.getCheckpointPrefix(stoppedMigrationID)
	require.NotEmpty(t, existingDCPCheckpoints(t, ctx, db.DatabaseContext, stoppedPrefix, feedMode),
		"precondition: a stopped migration should have persisted checkpoints")

	// reset - this abandons the stopped run's ID rather than resuming it
	require.NoError(t, mgr.Start(ctx, AttachmentMigrationOptions{Reset: true}))
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateCompleted)

	require.NotEqual(t, stoppedMigrationID, getAttachmentMigrationStats(t, db).MigrationID,
		"reset should have started a new migration run")

	require.Empty(t, existingDCPCheckpoints(t, ctx, db.DatabaseContext, stoppedPrefix, feedMode),
		"reset left behind the checkpoints for abandoned migration run %q", stoppedMigrationID)
}
