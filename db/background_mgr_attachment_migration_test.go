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
		value, _, err := collection.dataStore.GetRaw(key)
		require.NoError(t, err)

		MoveAttachmentXattrFromGlobalToSync(t, collection.dataStore, key, value, true)
	}

	err := db.AttachmentCompactionManager.Start(ctx, nil)
	require.NoError(t, err)

	// wait for task to complete
	stats := waitForAttachmentMigrationState(t, db, BackgroundProcessStateCompleted)

	// assert that the subset (5) of the docs were changed, all created docs were processed (10)
	assert.Equal(t, int64(10), stats.DocsProcessed)
	assert.Equal(t, int64(5), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)

}

func TestAttachmentMigrationManagerResumeStoppedMigration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}

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

	err := db.AttachmentMigrationManager.Start(ctx, nil)
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			stats := GetAttachmentMigrationStatus(t, db.DatabaseContext)

			if stats.DocsProcessed >= 200 {
				err = db.AttachmentMigrationManager.Stop()
				require.NoError(t, err)
				break
			}
			time.Sleep(1 * time.Microsecond)
		}
	}()

	stats := waitForAttachmentMigrationState(t, db, BackgroundProcessStateStopped)
	require.Less(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version is not present
	var syncInfo base.SyncInfo
	_, err = collection.dataStore.Get(base.SGSyncInfo, &syncInfo)
	require.Error(t, err)

	// Resume process
	err = db.AttachmentMigrationManager.Start(ctx, nil)
	require.NoError(t, err)

	stats = waitForAttachmentMigrationState(t, db, BackgroundProcessStateCompleted)
	require.GreaterOrEqual(t, stats.DocsProcessed, int64(4000))

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)
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

	err = db.AttachmentCompactionManager.Start(ctx, nil)
	require.NoError(t, err)

	stats := waitForAttachmentMigrationState(t, db, BackgroundProcessStateCompleted)

	// assert that the two added docs above were processed but not changed
	// no docs should be changed, only one has xattr defined thus should only have one of the two docs processed
	assert.Equal(t, int64(1), stats.DocsProcessed)
	assert.Equal(t, int64(0), stats.DocsChanged)

	// assert that the sync info metadata version doc has been written to the database collection
	AssertSyncInfoMetaVersion(t, collection.dataStore)
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
		BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
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

	err = db.AttachmentMigrationManager.Start(ctx, nil)
	require.NoError(t, err)

	stats := waitForAttachmentMigrationState(t, db, BackgroundProcessStateCompleted)

	// assert that the two added docs above were processed but not changed
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
	base.TestRequiresDCPResync(t)
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
			expected:      fmt.Sprintf("_sync:dcp_ck::sg-%v:att_migration:1234", base.ProductAPIVersion),
		},
		{
			name:          "default collection, group ID=foo",
			collectionIDs: []uint32{base.DefaultCollectionID},
			groupID:       "foo",
			expected:      fmt.Sprintf("_sync:dcp_ck:foo::sg-%v:att_migration:1234", base.ProductAPIVersion),
		},
		{
			name:          "default collection + collection 1, no group id",
			collectionIDs: []uint32{base.DefaultCollectionID, 1},
			groupID:       "",
			expected:      fmt.Sprintf("_sync:dcp_ck::sg-%v:att_migration:1234", base.ProductAPIVersion),
		},
		{
			name:          "default collection + collection 1, group ID=foo",
			collectionIDs: []uint32{base.DefaultCollectionID, 1},
			groupID:       "foo",
			expected:      fmt.Sprintf("_sync:dcp_ck:foo::sg-%v:att_migration:1234", base.ProductAPIVersion),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
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
			clientOptions := getMigrationDCPClientOptions(
				db,
				migrationID,
				test.collectionIDs,
			)

			b, err := base.AsGocbV2Bucket(bucket)
			require.NoError(t, err)
			dcpClient, err := base.NewDCPClient(
				ctx,
				nil,
				*clientOptions,
				b,
			)
			require.NoError(t, err)
			require.Equal(t, test.expected, dcpClient.GetMetadataKeyPrefix())
		})
	}
}

// waitForAttachmentMigrationState waits for the attachment migration manager to reach the expected state, and returns
// the latest stats for the process. Fails the test harness if the expectedState is not reached.
func waitForAttachmentMigrationState(t *testing.T, db *Database, expectedState BackgroundProcessState) AttachmentMigrationManagerResponse {
	RequireBackgroundManagerState(t, db.AttachmentMigrationManager, expectedState)
	return GetAttachmentMigrationStatus(t, db.DatabaseContext)
}
