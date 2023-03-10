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
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (rt *RestTester) WaitForAttachmentCompactionStatus(t *testing.T, state db.BackgroundProcessState) db.AttachmentManagerResponse {
	var response db.AttachmentManagerResponse
	err := rt.WaitForConditionWithOptions(func() bool {
		resp := rt.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
		RequireStatus(t, resp, http.StatusOK)

		err := base.JSONUnmarshal(resp.BodyBytes(), &response)
		assert.NoError(t, err)

		return response.State == state
	}, 90, 1000)
	assert.NoError(t, err)

	return response
}

func CreateLegacyAttachmentDoc(t *testing.T, ctx context.Context, collection *db.DatabaseCollectionWithUser, dataStore base.DataStore, docID string, body []byte, attID string, attBody []byte) string {
	if !base.TestUseXattrs() {
		t.Skip("Requires xattrs")
	}
	attDigest := db.Sha1DigestKey(attBody)

	attDocID := db.MakeAttachmentKey(db.AttVersion1, docID, attDigest)
	_, err := dataStore.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	var unmarshalledBody Body
	err = base.JSONUnmarshal(body, &unmarshalledBody)
	require.NoError(t, err)

	_, _, err = collection.Put(ctx, docID, db.Body(unmarshalledBody))
	require.NoError(t, err)

	_, err = dataStore.WriteUpdateWithXattr(docID, base.SyncXattrName, "", 0, nil, nil, func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
		attachmentSyncData := map[string]interface{}{
			attID: map[string]interface{}{
				"content_type": "application/json",
				"digest":       attDigest,
				"length":       2,
				"revpos":       2,
				"stub":         true,
			},
		}

		attachmentSyncDataBytes, err := base.JSONMarshal(attachmentSyncData)
		require.NoError(t, err)

		xattr, err = base.InjectJSONPropertiesFromBytes(xattr, base.KVPairBytes{
			Key: "attachments",
			Val: attachmentSyncDataBytes,
		})
		require.NoError(t, err)

		return doc, xattr, false, nil, nil
	})
	require.NoError(t, err)

	return attDocID
}
