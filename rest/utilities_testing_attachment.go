// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"maps"
	"slices"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/require"
)

// WaitForAttachmentCompactionStatus waits for the expectedState of the attachment compaction background job to be
// reached by polling the REST API until that state
// is reached. Fails test harness if it is not reached within timeout.
func (rt *RestTester) WaitForAttachmentCompactionStatus(expectedState db.BackgroundProcessState) db.AttachmentManagerResponse {
	rt.TB().Helper()
	return waitForBackgroundManagerState[db.AttachmentManagerResponse](rt, "/{{.db}}/_compact?type=attachment", expectedState)
}

func CreateLegacyAttachmentDoc(t *testing.T, ctx context.Context, collection *db.DatabaseCollectionWithUser, docID string, body []byte, attID string, attBody []byte) string {
	if !base.TestUseXattrs() {
		t.Skip("Requires xattrs")
	}
	attDigest := db.Sha1DigestKey(attBody)

	attDocID := db.MakeAttachmentKey(db.AttVersion1, docID, attDigest)
	dataStore := collection.GetCollectionDatastore()
	_, err := dataStore.AddRaw(ctx, attDocID, 0, attBody)
	require.NoError(t, err)

	var unmarshalledBody db.Body
	err = base.JSONUnmarshal(body, &unmarshalledBody)
	require.NoError(t, err)

	_, _, err = collection.Put(ctx, docID, unmarshalledBody)
	require.NoError(t, err)

	_, err = dataStore.WriteUpdateWithXattrs(ctx, docID, []string{base.SyncXattrName}, 0, nil, nil, func(doc []byte, xattrs map[string][]byte, _ uint64) (sgbucket.UpdatedDoc, error) {
		attachmentSyncData := map[string]any{
			attID: map[string]any{
				"content_type": "application/json",
				"digest":       attDigest,
				"length":       2,
				"revpos":       2,
				"stub":         true,
			},
		}

		attachmentSyncDataBytes, err := base.JSONMarshal(attachmentSyncData)
		require.NoError(t, err)
		require.Contains(t, maps.Keys(xattrs), base.SyncXattrName)
		xattr := xattrs[base.SyncXattrName]
		xattr, err = base.InjectJSONPropertiesFromBytes(xattr, base.KVPairBytes{
			Key: "attachments",
			Val: attachmentSyncDataBytes,
		})
		require.NoError(t, err)
		return sgbucket.UpdatedDoc{
			Doc: doc,
			Xattrs: map[string][]byte{
				base.SyncXattrName: xattr,
			},
		}, nil
	})
	require.NoError(t, err)

	return attDocID
}

// WaitForAttachmentMigrationStatus waits for the expectedState to be reached for the attachment migration background
// job by polling the REST API until that state is reached. Fails test harness if it is not reached within timeout.
func (rt *RestTester) WaitForAttachmentMigrationStatus(state db.BackgroundProcessState) db.AttachmentMigrationManagerResponse {
	rt.TB().Helper()
	response := waitForBackgroundManagerState[db.AttachmentMigrationManagerResponse](rt, "/{{.db}}/_attachment_migration", state)
	// Wait for heartbeat doc removal if the state change will result in its removal. Allows calling start
	// immediately after db.BackgroundProcessStateStopped without error.
	if !slices.Contains([]db.BackgroundProcessState{db.BackgroundProcessStateRunning, db.BackgroundProcessStateStopping}, state) {
		db.WaitForBackgroundManagerHeartbeatDocRemoval(rt.TB(), rt.GetDatabase().AttachmentMigrationManager)
	}
	return response
}
