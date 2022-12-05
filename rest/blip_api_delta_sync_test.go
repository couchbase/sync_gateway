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

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlipDeltaSyncPushAttachment tests updating a doc that has an attachment with a delta that doesn't modify the attachment.

func TestBlipDeltaSyncPushAttachment(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	if !base.IsEnterpriseEdition() {
		t.Skip("Delta test requires EE")
	}

	const docID = "pushAttachmentDoc"

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		}},
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	// Push first rev
	revID, err := btc.PushRev(docID, "", []byte(`{"key":"val"}`))
	require.NoError(t, err)

	// Push second rev with an attachment (no delta yet)
	attData := base64.StdEncoding.EncodeToString([]byte("attach"))
	revID, err = btc.PushRev(docID, revID, []byte(`{"key":"val","_attachments":{"myAttachment":{"data":"`+attData+`"}}}`))
	require.NoError(t, err)

	syncData, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), docID)
	require.NoError(t, err)

	assert.Len(t, syncData.Attachments, 1)
	_, found := syncData.Attachments["myAttachment"]
	assert.True(t, found)

	// Turn deltas on
	btc.ClientDeltas = true

	// Get existing body with the stub attachment, insert a new property and push as delta.
	body, found := btc.GetRev(docID, revID)
	require.True(t, found)
	newBody, err := base.InjectJSONPropertiesFromBytes(body, base.KVPairBytes{Key: "update", Val: []byte(`true`)})
	require.NoError(t, err)
	revID, err = btc.PushRev(docID, revID, newBody)
	require.NoError(t, err)

	syncData, err = rt.GetDatabase().GetSingleDatabaseCollection().GetDocSyncData(base.TestCtx(t), docID)
	require.NoError(t, err)

	assert.Len(t, syncData.Attachments, 1)
	_, found = syncData.Attachments["myAttachment"]
	assert.True(t, found)
}

// Test pushing and pulling new attachments through delta sync
// 1. Create test client that have deltas enabled
// 2. Start continuous push and pull replication in client
// 3. Make sure that sync gateway is running with delta sync on, in enterprise edition
// 4. Create doc with attachment in SGW
// 5. Update doc in the test client by adding another attachment
// 6. Have that update pushed using delta sync via the continuous replication started in step 2
func TestBlipDeltaSyncPushPullNewAttachment(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	if !base.IsEnterpriseEdition() {
		t.Skip("Delta test requires EE")
	}
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	btc.ClientDeltas = true
	err = btc.StartPull()
	assert.NoError(t, err)
	const docID = "doc1"

	// Create doc1 rev 1-77d9041e49931ceef58a1eef5fd032e8 on SG with an attachment
	bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	response := rt.SendAdminRequest(http.MethodPut, "/db/"+docID, bodyText)
	assert.Equal(t, http.StatusCreated, response.Code)

	// Wait for the document to be replicated at the client
	revId := RespRevID(t, response)
	data, ok := btc.WaitForRev(docID, revId)
	assert.True(t, ok)
	bodyTextExpected := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	require.JSONEq(t, bodyTextExpected, string(data))

	// Update the replicated doc at client by adding another attachment.
	bodyText = `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="},"world.txt":{"data":"bGVsbG8gd29ybGQ="}}}`
	revId, err = btc.PushRev(docID, revId, []byte(bodyText))
	require.NoError(t, err)
	assert.Equal(t, "2-abc", revId)

	// Wait for the document to be replicated at SG
	_, ok = btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	resp := rt.SendAdminRequest(http.MethodGet, "/db/"+docID+"?rev="+revId, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))

	assert.Equal(t, docID, respBody[db.BodyId])
	assert.Equal(t, "2-abc", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 1)
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[0])

	attachments, ok := respBody[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, attachments, 2)
	hello, ok := attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])
	assert.Equal(t, true, hello["stub"])

	world, ok := attachments["world.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-qiF39gVoGPFzpRQkNYcY9u3wx9Y=", world["digest"])
	assert.Equal(t, float64(11), world["length"])
	assert.Equal(t, float64(2), world["revpos"])
	assert.Equal(t, true, world["stub"])
}

// TestBlipDeltaSyncNewAttachmentPull tests that adding a new attachment in SG and replicated via delta sync adds the attachment
// to the temporary "allowedAttachments" map.
func TestBlipDeltaSyncNewAttachmentPull(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-10000d5ec533b29b117e60274b1e3653 on SG with the first attachment
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}], "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	assert.Equal(t, http.StatusCreated, resp.Code)
	fmt.Println(resp.Body.String())

	data, ok = client.WaitForRev("doc1", "2-10000d5ec533b29b117e60274b1e3653")
	assert.True(t, ok)
	var dataMap map[string]interface{}
	assert.NoError(t, base.JSONUnmarshal(data, &dataMap))
	atts, ok := dataMap[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, atts, 1)
	hello, ok := atts["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(2), hello["revpos"])
	assert.Equal(t, true, hello["stub"])

	// message #3 is the getAttachment message that is sent in-between rev processing
	msg, ok := client.pullReplication.WaitForMessage(3)
	assert.True(t, ok)
	assert.NotEqual(t, blip.ErrorType, msg.Type(), "Expected non-error blip message type")

	// Check EE is delta, and CE is full-body replication
	// msg, ok = client.pullReplication.WaitForMessage(5)
	msg, ok = client.WaitForBlipRevMessage("doc1", "2-10000d5ec533b29b117e60274b1e3653")
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"_attachments":[{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}}]}`, string(msgBody))
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"_attachments":[{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}}]}`, string(msgBody))
		assert.Contains(t, string(msgBody), `"_attachments":{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}}`)
		assert.Contains(t, string(msgBody), `"greetings":[{"hello":"world!"},{"hi":"alice"}]`)
	}

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev=2-10000d5ec533b29b117e60274b1e3653", "")
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))
	assert.Equal(t, "doc1", respBody[db.BodyId])
	assert.Equal(t, "2-10000d5ec533b29b117e60274b1e3653", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 2)
	assert.Equal(t, map[string]interface{}{"hello": "world!"}, greetings[0])
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[1])
	atts = respBody[db.BodyAttachments].(map[string]interface{})
	assert.Len(t, atts, 1)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(2), hello["revpos"])
	assert.Equal(t, true, hello["stub"])

	// assert.Equal(t, `{"_attachments":{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}},"_id":"doc1","_rev":"2-10000d5ec533b29b117e60274b1e3653","greetings":[{"hello":"world!"},{"hi":"alice"}]}`, resp.Body.String())
}

// TestBlipDeltaSyncPull tests that a simple pull replication uses deltas in EE,
// and checks that full body replication still happens in CE.
func TestBlipDeltaSyncPull(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var deltaSentCount int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaSentCount = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 12345678901234567890}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(data))

	msg, ok := client.WaitForBlipRevMessage("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)

	// Check EE is delta, and CE is full-body replication
	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
		assert.Equal(t, deltaSentCount+1, rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value())
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(msgBody))

		var afterDeltaSyncCount int64
		if rt.GetDatabase().DbStats.DeltaSync() != nil {
			afterDeltaSyncCount = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
		}

		assert.Equal(t, deltaSentCount, afterDeltaSyncCount)
	}
}

// TestBlipDeltaSyncPullResend tests that a simple pull replication that uses a delta a client rejects will resend the revision in full.
func TestBlipDeltaSyncPullResend(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skip("Enterprise-only test for delta sync")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// create doc1 rev 1
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)
	rev1ID := RespRevID(t, resp)

	deltaSentCount := rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	// reject deltas built ontop of rev 1
	client.rejectDeltasForSrcRev = rev1ID

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	data, ok := client.WaitForRev("doc1", rev1ID)
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev="+rev1ID, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 12345678901234567890}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)
	rev2ID := RespRevID(t, resp)

	data, ok = client.WaitForRev("doc1", rev2ID)
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)

	// Check the request was initially sent with the correct deltaSrc property
	assert.Equal(t, rev1ID, msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
	assert.Equal(t, deltaSentCount+1, rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value())

	msg, ok = client.WaitForBlipRevMessage("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)

	// Check the resent request was NOT sent with a deltaSrc property
	assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was NOT the delta
	msgBody, err = msg.Body()
	assert.NoError(t, err)
	assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(msgBody))
}

// TestBlipDeltaSyncPullRemoved tests a simple pull replication that drops a document out of the user's channel.
func TestBlipDeltaSyncPullRemoved(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:               "alice",
		Channels:               []string{"public"},
		ClientDeltas:           true,
		SupportedBLIPProtocols: []string{db.BlipCBMobileReplicationV2},
	})
	require.NoError(t, err)
	defer client.Close()

	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-1513b53e2738671e634d9dd111f48de0
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// create doc1 rev 2-ff91e11bc1fd12bbb4815a06571859a9
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-1513b53e2738671e634d9dd111f48de0", `{"channels": ["private"], "greetings": [{"hello": "world!"}, {"hi": "bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-ff91e11bc1fd12bbb4815a06571859a9")
	assert.True(t, ok)
	assert.Equal(t, `{"_removed":true}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"_removed":true}`, string(msgBody))
}

// TestBlipDeltaSyncPullTombstoned tests a simple pull replication that deletes a document.
//
// Sync Gateway: creates rev-1 and then tombstones it in rev-2
// Client:       continuously pulls, pulling rev-1 as normal, and then rev-2 which should be a tombstone, even though a delta was requested
// ┌──────────────┐ ┌─────────┐            ┌─────────┐
// │ Sync Gateway ├─┤ + rev-1 ├────────────┤ - rev-2 ├────■
// └──────────────┘ └─────────┤            └─────────┤
// ┌──────────────┐ ┌─────────┼──────────────────────┼──┐
// │     Client 1 ├─┤         ▼      continuous      ▼  ├─■
// └──────────────┘ └───────────────────────────────────┘
func TestBlipDeltaSyncPullTombstoned(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var deltaCacheHitsStart int64
	var deltaCacheMissesStart int64
	var deltasRequestedStart int64
	var deltasSentStart int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedStart = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentStart = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:     "alice",
		Channels:     []string{"public"},
		ClientDeltas: true,
	})
	require.NoError(t, err)
	defer client.Close()

	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-e89945d756a1d444fa212bffbbb31941
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// tombstone doc1 at rev 2-2db70833630b396ef98a3ec75b3e90fc
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev=1-1513b53e2738671e634d9dd111f48de0", "")
	assert.Equal(t, http.StatusOK, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{}`, string(msgBody))
	assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted])

	var deltaCacheHitsEnd int64
	var deltaCacheMissesEnd int64
	var deltasRequestedEnd int64
	var deltasSentEnd int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	if sgUseDeltas {
		assert.Equal(t, deltaCacheHitsStart, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart+1, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart+1, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart, deltasSentEnd) // "_removed" docs are not counted as a delta
	} else {
		assert.Equal(t, deltaCacheHitsStart, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart, deltasSentEnd)
	}
}

// TestBlipDeltaSyncPullTombstonedStarChan tests two clients can perform a simple pull replication that deletes a document when the user has access to the star channel.
//
// Sync Gateway: creates rev-1 and then tombstones it in rev-2
// Client 1:     continuously pulls, and causes the tombstone delta for rev-2 to be cached
// Client 2:     runs two one-shots, once initially to pull rev-1, and finally for rev-2 after the tombstone delta has been cached
// ┌──────────────┐ ┌─────────┐            ┌─────────┐
// │ Sync Gateway ├─┤ + rev-1 ├─┬──────────┤ - rev-2 ├─┬───────────■
// └──────────────┘ └─────────┤ │          └─────────┤ │
// ┌──────────────┐ ┌─────────┼─┼────────────────────┼─┼─────────┐
// │     Client 1 ├─┤         ▼ │    continuous      ▼ │         ├─■
// └──────────────┘ └───────────┼──────────────────────┼─────────┘
// ┌──────────────┐           ┌─┼─────────┐          ┌─┼─────────┐
// │     Client 2 ├───────────┤ ▼ oneshot ├──────────┤ ▼ oneshot ├─■
// └──────────────┘           └───────────┘          └───────────┘
func TestBlipDeltaSyncPullTombstonedStarChan(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyCache, base.KeySync, base.KeySyncMsg)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var deltaCacheHitsStart int64
	var deltaCacheMissesStart int64
	var deltasRequestedStart int64
	var deltasSentStart int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedStart = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentStart = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	client1, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:     "client1",
		Channels:     []string{"*"},
		ClientDeltas: true,
	})
	require.NoError(t, err)
	defer client1.Close()

	client2, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:     "client2",
		Channels:     []string{"*"},
		ClientDeltas: true,
	})
	require.NoError(t, err)
	defer client2.Close()

	err = client1.StartPull()
	require.NoError(t, err)

	// create doc1 rev 1-e89945d756a1d444fa212bffbbb31941
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client1.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// Have client2 get only rev-1 and then stop replicating
	err = client2.StartOneshotPull()
	assert.NoError(t, err)
	data, ok = client2.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// tombstone doc1 at rev 2-2db70833630b396ef98a3ec75b3e90fc
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev=1-1513b53e2738671e634d9dd111f48de0", `{"test": true"`)
	assert.Equal(t, http.StatusOK, resp.Code)

	data, ok = client1.WaitForRev("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	msg, ok := client1.WaitForBlipRevMessage("doc1", "2-ed278cbc310c9abeea414da15d0b2cac") // docid, revid to get the message
	assert.True(t, ok)
	if !assert.Equal(t, db.MessageRev, msg.Profile()) {
		t.Logf("unexpected profile for message %v in %v",
			msg.SerialNumber(), client1.pullReplication.GetMessages())
	}
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	if !assert.Equal(t, `{}`, string(msgBody)) {
		t.Logf("unexpected body for message %v in %v",
			msg.SerialNumber(), client1.pullReplication.GetMessages())
	}
	if !assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted]) {
		t.Logf("unexpected deleted property for message %v in %v",
			msg.SerialNumber(), client1.pullReplication.GetMessages())
	}

	// Sync Gateway will have cached the tombstone delta, so client 2 should be able to retrieve it from the cache
	err = client2.StartOneshotPull()
	assert.NoError(t, err)

	data, ok = client2.WaitForRev("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	msg, ok = client2.WaitForBlipRevMessage("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	if !assert.Equal(t, db.MessageRev, msg.Profile()) {
		t.Logf("unexpected profile for message %v in %v",
			msg.SerialNumber(), client2.pullReplication.GetMessages())
	}
	msgBody, err = msg.Body()
	assert.NoError(t, err)
	if !assert.Equal(t, `{}`, string(msgBody)) {
		t.Logf("unexpected body for message %v in %v",
			msg.SerialNumber(), client2.pullReplication.GetMessages())
	}
	if !assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted]) {
		t.Logf("unexpected deleted property for message %v in %v",
			msg.SerialNumber(), client2.pullReplication.GetMessages())
	}

	var deltaCacheHitsEnd int64
	var deltaCacheMissesEnd int64
	var deltasRequestedEnd int64
	var deltasSentEnd int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	if sgUseDeltas {
		assert.Equal(t, deltaCacheHitsStart+1, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart+1, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart+2, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart+2, deltasSentEnd)
	} else {
		assert.Equal(t, deltaCacheHitsStart, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart, deltasSentEnd)
	}
}

// TestBlipDeltaSyncPullRevCache tests that a simple pull replication uses deltas in EE,
// Second pull validates use of rev cache for previously generated deltas.
func TestBlipDeltaSyncPullRevCache(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skipf("Skipping enterprise-only delta sync test.")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// Perform a one-shot pull as client 2 to pull down the first revision

	client2, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client2.Close()

	client2.ClientDeltas = true
	err = client2.StartOneshotPull()
	assert.NoError(t, err)

	data, ok = client2.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": "bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))

	msg, ok := client.WaitForBlipRevMessage("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)

	// Check EE is delta
	// Check the request was sent with the correct deltaSrc property
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

	deltaCacheHits := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
	deltaCacheMisses := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()

	// Run another one shot pull to get the 2nd revision - validate it comes as delta, and uses cached version
	client2.ClientDeltas = true
	err = client2.StartOneshotPull()
	assert.NoError(t, err)

	msg2, ok := client2.WaitForBlipRevMessage("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)

	// Check the request was sent with the correct deltaSrc property
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg2.Properties[db.RevMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody2, err := msg2.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody2))

	updatedDeltaCacheHits := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
	updatedDeltaCacheMisses := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()

	assert.Equal(t, deltaCacheHits+1, updatedDeltaCacheHits)
	assert.Equal(t, deltaCacheMisses, updatedDeltaCacheMisses)

}

// TestBlipDeltaSyncPush tests that a simple push replication handles deltas in EE,
// and checks that full body replication is still supported in CE.
func TestBlipDeltaSyncPush(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-abc on client
	newRev, err := client.PushRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
	assert.NoError(t, err)
	assert.Equal(t, "2-abc", newRev)

	// Check EE is delta, and CE is full-body replication
	msg, ok := client.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

		// Validate that generation of a delta didn't mutate the revision body in the revision cache
		docRev, cacheErr := rt.GetDatabase().GetSingleDatabaseCollection().GetRevisionCacheForTest().Get(base.TestCtx(t), "doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", db.RevCacheOmitBody, db.RevCacheOmitDelta)
		assert.NoError(t, cacheErr)
		assert.NotContains(t, docRev.BodyBytes, "bob")
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
	}

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))
	assert.Equal(t, "doc1", respBody[db.BodyId])
	assert.Equal(t, "2-abc", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 3)
	assert.Equal(t, map[string]interface{}{"hello": "world!"}, greetings[0])
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[1])
	assert.Equal(t, map[string]interface{}{"howdy": "bob"}, greetings[2])

	// tombstone doc1 (gets rev 3-f3be6c85e0362153005dae6f08fc68bb)
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)

	data, ok = client.WaitForRev("doc1", "3-fcc2db8cdbf1831799b7a39bb57edd71")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	var deltaPushDocCountStart int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaPushDocCountStart = rt.GetDatabase().DbStats.DeltaSync().DeltaPushDocCount.Value()
	}
	revID, err := client.PushRev("doc1", "3-fcc2db8cdbf1831799b7a39bb57edd71", []byte(`{"undelete":true}`))

	if base.IsEnterpriseEdition() {
		// Now make the client push up a delta that has the parent of the tombstone.
		// This is not a valid scenario, and is actively prevented on the CBL side.
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Can't use delta. Found tombstone for doc")
		assert.Equal(t, "", revID)
	} else {
		// Pushing a full body revision on top of a tombstone is valid.
		// CBL clients should fall back to this. The test client doesn't.
		assert.NoError(t, err)
		assert.Equal(t, "4-abc", revID)
	}

	var deltaPushDocCountEnd int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaPushDocCountEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaPushDocCount.Value()
	}
	assert.Equal(t, deltaPushDocCountStart, deltaPushDocCountEnd)
}

// TestBlipNonDeltaSyncPush tests that a client that doesn't support deltas can push to a SG that supports deltas (either CE or EE)
func TestBlipNonDeltaSyncPush(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = false
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-abcxyz on client
	newRev, err := client.PushRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
	assert.NoError(t, err)
	assert.Equal(t, "2-abc", newRev)

	// Check EE is delta, and CE is full-body replication
	msg, ok := client.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	// Check the request was NOT sent with a deltaSrc property
	assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was NOT the delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), `{"howdy":"bob"}`)
}
