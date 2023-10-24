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
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test pushing and pulling v2 attachments with v2 client
// 1. Create test client.
// 2. Start continuous push and pull replication in client
// 3. Create doc with attachment in SGW
// 4. Update doc in the test client and keep the same attachment stub.
// 5. Have that update pushed via the continuous replication started in step 2
func TestBlipPushPullV2AttachmentV2Client(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: base.BoolPtr(true),
				},
			},
		},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	opts := &BlipTesterClientOpts{}
	opts.SupportedBLIPProtocols = []string{db.BlipCBMobileReplicationV2}
	btc, err := NewBlipTesterClientOptsWithRT(t, rt, opts)
	require.NoError(t, err)
	defer btc.Close()

	err = btc.StartPull()
	assert.NoError(t, err)
	const docID = "doc1"

	// Create doc revision with attachment on SG.
	bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	version := rt.PutDoc(docID, bodyText)

	data, ok := btc.WaitForVersion(docID, version)
	assert.True(t, ok)
	bodyTextExpected := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	require.JSONEq(t, bodyTextExpected, string(data))

	// Update the replicated doc at client along with keeping the same attachment stub.
	bodyText = `{"greetings":[{"hi":"bob"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	version, err = btc.PushRev(docID, version, []byte(bodyText))
	require.NoError(t, err)

	// Wait for the document to be replicated at SG
	_, ok = btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	respBody := rt.GetDocVersion(docID, version)

	assert.Equal(t, docID, respBody[db.BodyId])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 1)
	assert.Equal(t, map[string]interface{}{"hi": "bob"}, greetings[0])

	attachments, ok := respBody[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, attachments, 1)
	hello, ok := attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])
	assert.True(t, hello["stub"].(bool))

	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushCount.Value())
	assert.Equal(t, int64(11), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushBytes.Value())
}

// Test pushing and pulling v2 attachments with v3 client
// 1. Create test client.
// 2. Start continuous push and pull replication in client
// 3. Create doc with attachment in SGW
// 4. Update doc in the test client and keep the same attachment stub.
// 5. Have that update pushed via the continuous replication started in step 2
func TestBlipPushPullV2AttachmentV3Client(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: base.BoolPtr(true),
				},
			},
		},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	err = btc.StartPull()
	assert.NoError(t, err)
	const docID = "doc1"

	// Create doc revision with attachment on SG.
	bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	version := rt.PutDoc(docID, bodyText)

	data, ok := btc.WaitForVersion(docID, version)
	assert.True(t, ok)
	bodyTextExpected := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	require.JSONEq(t, bodyTextExpected, string(data))

	// Update the replicated doc at client along with keeping the same attachment stub.
	bodyText = `{"greetings":[{"hi":"bob"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	version, err = btc.PushRev(docID, version, []byte(bodyText))
	require.NoError(t, err)

	// Wait for the document to be replicated at SG
	_, ok = btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	respBody := rt.GetDocVersion(docID, version)

	assert.Equal(t, docID, respBody[db.BodyId])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 1)
	assert.Equal(t, map[string]interface{}{"hi": "bob"}, greetings[0])

	attachments, ok := respBody[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, attachments, 1)
	hello, ok := attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])
	assert.True(t, hello["stub"].(bool))

	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushCount.Value())
	assert.Equal(t, int64(11), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushBytes.Value())
}

// TestBlipProveAttachmentV2 ensures that CBL's proveAttachment for deduplication is working correctly even for v2 attachments which aren't de-duped on the server side.
func TestBlipProveAttachmentV2(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
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
	doc1Version := rt.PutDoc(doc1ID, doc1Body)

	data, ok := btc.WaitForVersion(doc1ID, doc1Version)
	require.True(t, ok)
	bodyTextExpected := fmt.Sprintf(`{"greetings":[{"hi":"alice"}],"_attachments":{"%s":{"revpos":1,"length":%d,"stub":true,"digest":"%s"}}}`, attachmentName, len(attachmentData), attachmentDigest)
	require.JSONEq(t, bodyTextExpected, string(data))

	// create doc2 now that we know the client has the attachment
	doc2Body := fmt.Sprintf(`{"greetings":[{"howdy": "bob"}],"_attachments":{"%s":{"data":"%s"}}}`, attachmentName, attachmentDataB64)
	doc2Version := rt.PutDoc(doc2ID, doc2Body)

	data, ok = btc.WaitForVersion(doc2ID, doc2Version)
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
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
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
	doc1Version, err := btc.PushRev(doc1ID, EmptyDocVersion(), []byte(doc1Body))
	require.NoError(t, err)

	err = rt.WaitForVersion(doc1ID, doc1Version)
	require.NoError(t, err)

	// create doc2 now that we know the server has the attachment - SG should still request the attachment data from the client.
	doc2Body := fmt.Sprintf(`{"greetings":[{"howdy": "bob"}],"_attachments":{"%s":{"data":"%s"}}}`, attachmentName, attachmentDataB64)
	doc2Version, err := btc.PushRev(doc2ID, EmptyDocVersion(), []byte(doc2Body))
	require.NoError(t, err)

	err = rt.WaitForVersion(doc2ID, doc2Version)
	require.NoError(t, err)

	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.CBLReplicationPush().DocPushCount.Value())
	assert.Equal(t, int64(0), rt.GetDatabase().DbStats.CBLReplicationPush().DocPushErrorCount.Value())
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushCount.Value())
	assert.Equal(t, int64(2*len(attachmentData)), rt.GetDatabase().DbStats.CBLReplicationPush().AttachmentPushBytes.Value())
}

func TestBlipPushPullNewAttachmentCommonAncestor(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	err = btc.StartPull()
	assert.NoError(t, err)
	const docID = "doc1"

	// CBL creates revisions 1-abc,2-abc on the client, with an attachment associated with rev 2.
	bodyText := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	err = btc.StoreRevOnClient(docID, "2-abc", []byte(bodyText))
	require.NoError(t, err)

	bodyText = `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":2,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	revId, err := btc.PushRevWithHistory(docID, "", []byte(bodyText), 2, 0)
	require.NoError(t, err)
	assert.Equal(t, "2-abc", revId)

	// Wait for the documents to be replicated at SG
	_, ok := btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"?rev="+revId, "")
	assert.Equal(t, http.StatusOK, resp.Code)

	// CBL updates the doc w/ two more revisions, 3-abc, 4-abc,
	// these are sent to SG as 4-abc, history:[4-abc,3-abc,2-abc], the attachment has revpos=2
	bodyText = `{"greetings":[{"hi":"bob"}],"_attachments":{"hello.txt":{"revpos":2,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	revId, err = btc.PushRevWithHistory(docID, revId, []byte(bodyText), 2, 0)
	require.NoError(t, err)
	assert.Equal(t, "4-abc", revId)

	// Wait for the document to be replicated at SG
	_, ok = btc.pushReplication.WaitForMessage(4)
	assert.True(t, ok)

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"?rev="+revId, "")
	assert.Equal(t, http.StatusOK, resp.Code)

	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))

	assert.Equal(t, docID, respBody[db.BodyId])
	assert.Equal(t, "4-abc", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 1)
	assert.Equal(t, map[string]interface{}{"hi": "bob"}, greetings[0])

	attachments, ok := respBody[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, attachments, 1)
	hello, ok := attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(2), hello["revpos"])
	assert.True(t, hello["stub"].(bool))

	// Check the number of sendProveAttachment/sendGetAttachment calls.
	require.NotNil(t, btc.pushReplication.replicationStats)
	assert.Equal(t, int64(1), btc.pushReplication.replicationStats.GetAttachment.Value())
	assert.Equal(t, int64(0), btc.pushReplication.replicationStats.ProveAttachment.Value())
}
func TestBlipPushPullNewAttachmentNoCommonAncestor(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	err = btc.StartPull()
	assert.NoError(t, err)
	const docID = "doc1"

	// CBL creates revisions 1-abc, 2-abc, 3-abc, 4-abc on the client, with an attachment associated with rev 2.
	// rev tree pruning on the CBL side, so 1-abc no longer exists.
	// CBL replicates, sends to client as 4-abc history:[4-abc, 3-abc, 2-abc], attachment has revpos=2
	bodyText := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	err = btc.StoreRevOnClient(docID, "2-abc", []byte(bodyText))
	require.NoError(t, err)

	bodyText = `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":2,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	revId, err := btc.PushRevWithHistory(docID, "2-abc", []byte(bodyText), 2, 0)
	require.NoError(t, err)
	assert.Equal(t, "4-abc", revId)

	// Wait for the document to be replicated at SG
	_, ok := btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"?rev="+revId, "")
	assert.Equal(t, http.StatusOK, resp.Code)

	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))

	assert.Equal(t, docID, respBody[db.BodyId])
	assert.Equal(t, "4-abc", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 1)
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[0])

	attachments, ok := respBody[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, attachments, 1)
	hello, ok := attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(4), hello["revpos"])
	assert.True(t, hello["stub"].(bool))

	// Check the number of sendProveAttachment/sendGetAttachment calls.
	require.NotNil(t, btc.pushReplication.replicationStats)
	assert.Equal(t, int64(1), btc.pushReplication.replicationStats.GetAttachment.Value())
	assert.Equal(t, int64(0), btc.pushReplication.replicationStats.ProveAttachment.Value())
}

// Test Attachment replication behavior described here: https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol
// - Put attachment via blip
// - Verifies that getAttachment won't return attachment "out of context" of a rev request
// - Get attachment via REST and verifies it returns the correct content
func TestPutAttachmentViaBlipGetViaRest(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t,
		BlipTesterSpec{
			connectingUsername: "user1",
			connectingPassword: "1234",
		})
	require.NoError(t, err)
	defer bt.Close()

	attachmentBody := "attach"
	digest := db.Sha1DigestKey([]byte(attachmentBody))

	input := SendRevWithAttachmentInput{
		docId:            "doc",
		revId:            "1-rev1",
		attachmentName:   "myAttachment",
		attachmentLength: len(attachmentBody),
		attachmentBody:   attachmentBody,
		attachmentDigest: digest,
	}
	bt.SendRevWithAttachment(input)

	// Try to fetch the attachment directly via getAttachment, expected to fail w/ 403 error for security reasons
	// since it's not in the context of responding to a "rev" request from the peer.
	getAttachmentRequest := bt.newRequest()
	getAttachmentRequest.SetProfile(db.MessageGetAttachment)
	getAttachmentRequest.Properties[db.GetAttachmentDigest] = input.attachmentDigest
	getAttachmentRequest.Properties[db.GetAttachmentID] = input.docId
	sent := bt.sender.Send(getAttachmentRequest)
	if !sent {
		panic(fmt.Sprintf("Failed to send request for doc: %v", input.docId))
	}
	getAttachmentResponse := getAttachmentRequest.Response()
	errorCode, hasErrorCode := getAttachmentResponse.Properties["Error-Code"]
	assert.Equal(t, "403", errorCode) // "Attachment's doc not being synced"
	assert.True(t, hasErrorCode)

	// Get the attachment via REST api and make sure it matches the attachment pushed earlier
	response := bt.restTester.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/%s/%s", input.docId, input.attachmentName), ``)
	assert.Equal(t, input.attachmentBody, response.Body.String())

}
func TestPutAttachmentViaBlipGetViaBlip(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	require.NoError(t, err)
	defer bt.Close()

	attachmentBody := "attach"
	digest := db.Sha1DigestKey([]byte(attachmentBody))

	// Send revision with attachment
	input := SendRevWithAttachmentInput{
		docId:            "doc",
		revId:            "1-rev1",
		attachmentName:   "myAttachment",
		attachmentLength: len(attachmentBody),
		attachmentBody:   attachmentBody,
		attachmentDigest: digest,
	}
	sent, _, _ := bt.SendRevWithAttachment(input)
	assert.True(t, sent)

	// Get all docs and attachment via subChanges request
	allDocs, ok := bt.WaitForNumDocsViaChanges(1)
	require.True(t, ok)

	// make assertions on allDocs -- make sure attachment is present w/ expected body
	require.Len(t, allDocs, 1)
	retrievedDoc := allDocs[input.docId]

	// doc assertions
	assert.Equal(t, input.docId, retrievedDoc.ID())
	assert.Equal(t, input.revId, retrievedDoc.RevID())

	// attachment assertions
	attachments, err := retrievedDoc.GetAttachments()
	assert.True(t, err == nil)
	assert.Equal(t, 1, len(attachments))
	retrievedAttachment := attachments[input.attachmentName]
	require.NotNil(t, retrievedAttachment)
	assert.Equal(t, input.attachmentBody, string(retrievedAttachment.Data))
	assert.Equal(t, len(attachmentBody), retrievedAttachment.Length)
	assert.Equal(t, retrievedAttachment.Digest, input.attachmentDigest)

}

// TestBlipAttachNameChange tests CBL handling - attachments with changed names are sent as stubs, and not new attachments
func TestBlipAttachNameChange(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	client1, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client1.Close()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync, base.KeySyncMsg, base.KeyWebSocket, base.KeyWebSocketFrame, base.KeyHTTP, base.KeyCRUD)

	attachmentA := []byte("attachmentA")
	attachmentAData := base64.StdEncoding.EncodeToString(attachmentA)
	digest := db.Sha1DigestKey(attachmentA)

	// Push initial attachment data
	version, err := client1.PushRev("doc", EmptyDocVersion(), []byte(`{"key":"val","_attachments":{"attachment": {"data":"`+attachmentAData+`"}}}`))
	require.NoError(t, err)

	// Confirm attachment is in the bucket
	attachmentAKey := db.MakeAttachmentKey(2, "doc", digest)
	bucketAttachmentA, _, err := rt.GetSingleDataStore().GetRaw(attachmentAKey)
	require.NoError(t, err)
	require.EqualValues(t, bucketAttachmentA, attachmentA)

	// Simulate changing only the attachment name over CBL
	// Use revpos 2 to simulate revpos bug in CBL 2.8 - 3.0.0
	version, err = client1.PushRev("doc", version, []byte(`{"key":"val","_attachments":{"attach":{"revpos":2,"content_type":"","length":11,"stub":true,"digest":"`+digest+`"}}}`))
	require.NoError(t, err)
	err = rt.WaitForVersion("doc", version)
	require.NoError(t, err)

	// Check if attachment is still in bucket
	bucketAttachmentA, _, err = rt.GetSingleDataStore().GetRaw(attachmentAKey)
	assert.NoError(t, err)
	assert.Equal(t, bucketAttachmentA, attachmentA)

	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/doc/attach", "")
	RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, attachmentA, resp.BodyBytes())
}

// TestBlipLegacyAttachNameChange ensures that CBL name changes for legacy attachments are handled correctly
func TestBlipLegacyAttachNameChange(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()
	client1, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client1.Close()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync, base.KeySyncMsg, base.KeyWebSocket, base.KeyWebSocketFrame, base.KeyHTTP, base.KeyCRUD)

	// Create document in the bucket with a legacy attachment
	docID := "doc"
	attBody := []byte(`hi`)
	digest := db.Sha1DigestKey(attBody)
	attKey := db.MakeAttachmentKey(db.AttVersion1, docID, digest)
	rawDoc := rawDocWithAttachmentAndSyncMeta()

	// Create a document with legacy attachment.
	CreateDocWithLegacyAttachment(t, rt, docID, rawDoc, attKey, attBody)

	// Get the document and grab the revID.
	docVersion, _ := rt.GetDoc(docID)

	// Store the document and attachment on the test client
	err = client1.StoreRevOnClient(docID, docVersion.RevID, rawDoc)

	require.NoError(t, err)
	client1.AttachmentsLock().Lock()
	client1.Attachments()[digest] = attBody
	client1.AttachmentsLock().Unlock()

	// Confirm attachment is in the bucket
	attachmentAKey := db.MakeAttachmentKey(1, "doc", digest)
	bucketAttachmentA, _, err := rt.GetSingleDataStore().GetRaw(attachmentAKey)
	require.NoError(t, err)
	require.EqualValues(t, bucketAttachmentA, attBody)

	// Simulate changing only the attachment name over CBL
	// Use revpos 2 to simulate revpos bug in CBL 2.8 - 3.0.0
	docVersion, err = client1.PushRev("doc", docVersion, []byte(`{"key":"val","_attachments":{"attach":{"revpos":2,"content_type":"test/plain","length":2,"stub":true,"digest":"`+digest+`"}}}`))
	require.NoError(t, err)

	err = rt.WaitForVersion("doc", docVersion)
	require.NoError(t, err)

	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/doc/attach", "")
	RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, attBody, resp.BodyBytes())
}

// TestBlipLegacyAttachDocUpdate ensures that CBL updates for documents associated with legacy attachments are handled correctly
func TestBlipLegacyAttachDocUpdate(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()
	client1, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client1.Close()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync, base.KeySyncMsg, base.KeyWebSocket, base.KeyWebSocketFrame, base.KeyHTTP, base.KeyCRUD)

	// Create document in the bucket with a legacy attachment.  Properties here align with rawDocWithAttachmentAndSyncMeta
	docID := "doc"
	attBody := []byte(`hi`)
	digest := db.Sha1DigestKey(attBody)
	attKey := db.MakeAttachmentKey(db.AttVersion1, docID, digest)
	attName := "hi.txt"
	rawDoc := rawDocWithAttachmentAndSyncMeta()

	// Create a document with legacy attachment.
	CreateDocWithLegacyAttachment(t, rt, docID, rawDoc, attKey, attBody)

	version, _ := rt.GetDoc(docID)

	// Store the document and attachment on the test client
	err = client1.StoreRevOnClient(docID, version.RevID, rawDoc)
	require.NoError(t, err)
	client1.AttachmentsLock().Lock()
	client1.Attachments()[digest] = attBody
	client1.AttachmentsLock().Unlock()

	// Confirm attachment is in the bucket
	attachmentAKey := db.MakeAttachmentKey(1, "doc", digest)
	dataStore := rt.GetSingleDataStore()
	bucketAttachmentA, _, err := dataStore.GetRaw(attachmentAKey)
	require.NoError(t, err)
	require.EqualValues(t, bucketAttachmentA, attBody)

	// Update the document, leaving body intact
	version, err = client1.PushRev("doc", version, []byte(`{"key":"val1","_attachments":{"`+attName+`":{"revpos":2,"content_type":"text/plain","length":2,"stub":true,"digest":"`+digest+`"}}}`))
	require.NoError(t, err)

	err = rt.WaitForVersion("doc", version)
	require.NoError(t, err)

	resp := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/doc/%s", attName), "")
	RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, attBody, resp.BodyBytes())

	// Validate that the attachment hasn't been migrated to V2
	v1Key := db.MakeAttachmentKey(1, "doc", digest)
	v1Body, _, err := dataStore.GetRaw(v1Key)
	require.NoError(t, err)
	require.EqualValues(t, attBody, v1Body)

	v2Key := db.MakeAttachmentKey(2, "doc", digest)
	_, _, err = dataStore.GetRaw(v2Key)
	require.Error(t, err)
	// Confirm correct type of error for both integration test and Walrus
	if !errors.Is(err, sgbucket.MissingError{Key: v2Key}) {
		var keyValueErr *gocb.KeyValueError
		require.True(t, errors.As(err, &keyValueErr))
		//require.Equal(t, keyValueErr.StatusCode, memd.StatusKeyNotFound)
		require.Equal(t, keyValueErr.DocumentID, v2Key)
	}
}

// TestAttachmentComputeStat:
//   - Setup blip test client
//   - Start pull replication
//   - add doc with attachment
//   - wait for doc to replicate and assert on attachment stat
func TestAttachmentComputeStat(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	opts := &BlipTesterClientOpts{}
	opts.SupportedBLIPProtocols = []string{db.BlipCBMobileReplicationV2}
	btc, err := NewBlipTesterClientOptsWithRT(t, rt, opts)
	require.NoError(t, err)
	defer btc.Close()
	syncProcessCompute := btc.rt.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value()

	err = btc.StartPull()
	assert.NoError(t, err)
	const docID = "doc1"

	// Create doc revision with attachment on SG.
	bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	version := rt.PutDoc(docID, bodyText)

	// Wait for the document to be replicated to client.
	data, ok := btc.WaitForVersion(docID, version)
	assert.True(t, ok)
	bodyTextExpected := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	require.JSONEq(t, bodyTextExpected, string(data))

	// assert the attachment read compute stat is incremented
	require.Greater(t, btc.rt.GetDatabase().DbStats.DatabaseStats.SyncProcessCompute.Value(), syncProcessCompute)

}
