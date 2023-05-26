/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlipProveAttachmentV2 ensures that CBL's proveAttachment for deduplication is working correctly even for v2 attachments which aren't de-duped on the server side.
func TestBlipProveAttachmentV2(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()
	rtConfig := RestTesterConfig{
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		SupportedBLIPProtocols: []string{db.BlipCBMobileReplicationV2},
	})
	require.NoError(t, err)
	defer btc.Close()

	err = btc.StartPull()
	assert.NoError(t, err)

	const (
		doc1ID = "doc1"
		doc2ID = "doc2"
	)

	const (
		attachmentName = "hello.txt"
		attachmentData = "hello world"
	)

	var (
		attachmentDataB64 = base64.StdEncoding.EncodeToString([]byte(attachmentData))
		attachmentDigest  = "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="
	)

	// Create two docs with the same attachment data on SG - v2 attachments intentionally result in two copies,
	// CBL will still de-dupe attachments based on digest, so will still try proveAttachmnet for the 2nd.
	doc1Body := fmt.Sprintf(`{"greetings":[{"hi": "alice"}],"_attachments":{"%s":{"data":"%s"}}}`, attachmentName, attachmentDataB64)
	response := rt.SendAdminRequest(http.MethodPut, "/db/"+doc1ID, doc1Body)
	RequireStatus(t, response, http.StatusCreated)
	doc1RevID := RespRevID(t, response)

	data, ok := btc.WaitForRev(doc1ID, doc1RevID)
	require.True(t, ok)
	bodyTextExpected := fmt.Sprintf(`{"greetings":[{"hi":"alice"}],"_attachments":{"%s":{"revpos":1,"length":%d,"stub":true,"digest":"%s"}}}`, attachmentName, len(attachmentData), attachmentDigest)
	require.JSONEq(t, bodyTextExpected, string(data))

	// create doc2 now that we know the client has the attachment
	doc2Body := fmt.Sprintf(`{"greetings":[{"howdy": "bob"}],"_attachments":{"%s":{"data":"%s"}}}`, attachmentName, attachmentDataB64)
	response = rt.SendAdminRequest(http.MethodPut, "/db/"+doc2ID, doc2Body)
	RequireStatus(t, response, http.StatusCreated)
	doc2RevID := RespRevID(t, response)

	data, ok = btc.WaitForRev(doc2ID, doc2RevID)
	require.True(t, ok)
	bodyTextExpected = fmt.Sprintf(`{"greetings":[{"howdy":"bob"}],"_attachments":{"%s":{"revpos":1,"length":%d,"stub":true,"digest":"%s"}}}`, attachmentName, len(attachmentData), attachmentDigest)
	require.JSONEq(t, bodyTextExpected, string(data))

	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value())
	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.CBLReplicationPull().RevErrorCount.Value())
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.CBLReplicationPull().AttachmentPullCount.Value())
	assert.Equal(t, int64(len(attachmentData)), rt.GetDatabase().DbStats.CBLReplicationPull().AttachmentPullBytes.Value())
}

// TestBlipProveAttachmentV2Push ensures that CBL's attachment deduplication is ignored for push replications - resulting in new server-side digests and duplicated attachment data (v2 attachment format).
func TestBlipProveAttachmentV2Push(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()
	rtConfig := RestTesterConfig{
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		SupportedBLIPProtocols: []string{db.BlipCBMobileReplicationV2},
	})
	require.NoError(t, err)
	defer btc.Close()

	const (
		doc1ID = "doc1"
		doc2ID = "doc2"
	)

	const (
		attachmentName = "hello.txt"
		attachmentData = "hello world"
	)

	var (
		attachmentDataB64 = base64.StdEncoding.EncodeToString([]byte(attachmentData))
		// attachmentDigest  = "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="
	)

	// Create two docs with the same attachment data on the client - v2 attachments intentionally result in two copies stored on the server, despite the client being able to share the data for both.
	doc1Body := fmt.Sprintf(`{"greetings":[{"hi": "alice"}],"_attachments":{"%s":{"data":"%s"}}}`, attachmentName, attachmentDataB64)
	doc1revID, err := btc.PushRev(doc1ID, "", []byte(doc1Body))
	require.NoError(t, err)

	err = rt.WaitForRev(doc1ID, doc1revID)
	require.NoError(t, err)

	// create doc2 now that we know the server has the attachment - SG should still request the attachment data from the client.
	doc2Body := fmt.Sprintf(`{"greetings":[{"howdy": "bob"}],"_attachments":{"%s":{"data":"%s"}}}`, attachmentName, attachmentDataB64)
	doc2RevID, err := btc.PushRev(doc2ID, "", []byte(doc2Body))
	require.NoError(t, err)

	err = rt.WaitForRev(doc2ID, doc2RevID)
	require.NoError(t, err)

	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.CBLReplicationPush().DocPushCount.Value())
	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.CBLReplicationPush().DocPushErrorCount.Value())
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushCount.Value())
	assert.Equal(t, int64(2*len(attachmentData)), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushBytes.Value())
}
