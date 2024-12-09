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
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		}},
		GuestEnabled: true,
	}

	const docID = "pushAttachmentDoc"

	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		// Push first rev
		version, err := btcRunner.PushRev(btc.id, docID, EmptyDocVersion(), []byte(`{"key":"val"}`))
		require.NoError(t, err)

		// Push second rev with an attachment (no delta yet)
		attData := base64.StdEncoding.EncodeToString([]byte("attach"))

		version, err = btcRunner.PushRev(btc.id, docID, version, []byte(`{"key":"val","_attachments":{"myAttachment":{"data":"`+attData+`"}}}`))
		require.NoError(t, err)

		collection, ctx := rt.GetSingleTestDatabaseCollection()
		syncData, err := collection.GetDocSyncData(ctx, docID)
		require.NoError(t, err)

		assert.Len(t, syncData.Attachments, 1)
		_, found := syncData.Attachments["myAttachment"]
		assert.True(t, found)

		// Turn deltas on
		btc.ClientDeltas = true

		// Get existing body with the stub attachment, insert a new property and push as delta.
		body, found := btcRunner.GetVersion(btc.id, docID, version)
		require.True(t, found)

		newBody, err := base.InjectJSONPropertiesFromBytes(body, base.KVPairBytes{Key: "update", Val: []byte(`true`)})
		require.NoError(t, err)

		_, err = btcRunner.PushRev(btc.id, docID, version, newBody)
		require.NoError(t, err)

		syncData, err = collection.GetDocSyncData(ctx, docID)
		require.NoError(t, err)

		assert.Len(t, syncData.Attachments, 1)
		_, found = syncData.Attachments["myAttachment"]
		assert.True(t, found)
	})
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

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		btc.ClientDeltas = true
		btcRunner.StartPull(btc.id)
		const docID = "doc1"

		// Create doc1 rev 1-77d9041e49931ceef58a1eef5fd032e8 on SG with an attachment
		bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
		// put doc directly needs to be here
		version := rt.PutDocDirectly(docID, JsonToMap(t, bodyText))
		data := btcRunner.WaitForVersion(btc.id, docID, version)

		bodyTextExpected := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
		require.JSONEq(t, bodyTextExpected, string(data))

		// Update the replicated doc at client by adding another attachment.
		bodyText = `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="},"world.txt":{"data":"bGVsbG8gd29ybGQ="}}}`
		version, err := btcRunner.PushRev(btc.id, docID, version, []byte(bodyText))
		require.NoError(t, err)

		// Wait for the document to be replicated at SG
		btc.pushReplication.WaitForMessage(2)

		respBody := rt.GetDocVersion(docID, version)

		assert.Equal(t, docID, respBody[db.BodyId])
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
	})
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
	btcRunner := NewBlipTesterClientRunner(t)
	const doc1ID = "doc1"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		client.ClientDeltas = true
		btcRunner.StartPull(client.id)

		// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
		version := rt.PutDocDirectly(doc1ID, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))

		data := btcRunner.WaitForVersion(client.id, doc1ID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// create doc1 rev 2-10000d5ec533b29b117e60274b1e3653 on SG with the first attachment
		version2 := rt.UpdateDocDirectly(doc1ID, version, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}], "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`))

		data = btcRunner.WaitForVersion(client.id, doc1ID, version2)
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
		msg := client.pullReplication.WaitForMessage(3)
		assert.NotEqual(t, blip.ErrorType, msg.Type(), "Expected non-error blip message type")

		// Check EE is delta, and CE is full-body replication
		// msg, ok = client.pullReplication.WaitForMessage(5)
		msg = btcRunner.WaitForBlipRevMessage(client.id, doc1ID, version2)
		// Delta sync only works for Version vectors, CBG-3748 (backwards compatibility for revID)
		sgCanUseDeltas := base.IsEnterpriseEdition() && client.UseHLV()
		if sgCanUseDeltas {
			// Check the request was sent with the correct deltaSrc property
			client.AssertDeltaSrcProperty(t, msg, version)
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
			assert.Contains(t, string(msgBody), `"stub":true`)
			assert.Contains(t, string(msgBody), `"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="`)
			assert.Contains(t, string(msgBody), `"revpos":2`)
			assert.Contains(t, string(msgBody), `"length":11`)
			assert.Contains(t, string(msgBody), `"greetings":[{"hello":"world!"},{"hi":"alice"}]`)
		}

		respBody := rt.GetDocVersion(doc1ID, version2)
		assert.Equal(t, doc1ID, respBody[db.BodyId])
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
	})
}

// TestBlipDeltaSyncPull tests that a simple pull replication uses deltas in EE,
// and checks that full body replication still happens in CE.
func TestBlipDeltaSyncPull(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	const docID = "doc1"
	var deltaSentCount int64
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			rtConfig)
		defer rt.Close()
		if rt.GetDatabase().DbStats.DeltaSync() != nil {
			deltaSentCount = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
		}

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		client.ClientDeltas = true
		btcRunner.StartPull(client.id)

		// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
		version := rt.PutDocDirectly(docID, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))

		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
		version2 := rt.UpdateDocDirectly(docID, version, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 1234567890123}]}`))

		data = btcRunner.WaitForVersion(client.id, docID, version2)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":1234567890123}]}`, string(data))
		msg := btcRunner.WaitForBlipRevMessage(client.id, docID, version2)

		// Check EE is delta, and CE is full-body replication
		// Delta sync only works for Version vectors, CBG-3748 (backwards compatibility for revID)
		sgCanUseDeltas := base.IsEnterpriseEdition() && client.UseHLV()
		if sgCanUseDeltas {
			// Check the request was sent with the correct deltaSrc property
			client.AssertDeltaSrcProperty(t, msg, version)
			// Check the request body was the actual delta
			msgBody, err := msg.Body()
			assert.NoError(t, err)
			assert.Equal(t, `{"greetings":{"2-":[{"howdy":1234567890123}]}}`, string(msgBody))
			assert.Equal(t, deltaSentCount+1, rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value())
		} else {
			// Check the request was NOT sent with a deltaSrc property
			assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
			// Check the request body was NOT the delta
			msgBody, err := msg.Body()
			assert.NoError(t, err)
			assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":1234567890123}]}}`, string(msgBody))
			assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":1234567890123}]}`, string(msgBody))

			var afterDeltaSyncCount int64
			if rt.GetDatabase().DbStats.DeltaSync() != nil {
				afterDeltaSyncCount = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
			}

			assert.Equal(t, deltaSentCount, afterDeltaSyncCount)
		}
	})
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
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // delta sync not implemented for rev tree replication, CBG-3748
	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()

		docID := "doc1"
		// create doc1 rev 1
		docVersion1 := rt.PutDocDirectly(docID, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))

		deltaSentCount := rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		// reject deltas built ontop of rev 1
		if client.UseHLV() {
			client.rejectDeltasForSrcRev = docVersion1.CV.String()
		} else {
			client.rejectDeltasForSrcRev = docVersion1.RevTreeID
		}

		client.ClientDeltas = true
		btcRunner.StartPull(client.id)
		data := btcRunner.WaitForVersion(client.id, docID, docVersion1)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// create doc1 rev 2
		docVersion2 := rt.UpdateDocDirectly(docID, docVersion1, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 1234567890123}]}`))

		data = btcRunner.WaitForVersion(client.id, docID, docVersion2)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":1234567890123}]}`, string(data))

		msg := client.pullReplication.WaitForMessage(5)

		// Check the request was initially sent with the correct deltaSrc property
		client.AssertDeltaSrcProperty(t, msg, docVersion1)

		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":1234567890123}]}}`, string(msgBody))
		assert.Equal(t, deltaSentCount+1, rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value())

		msg = btcRunner.WaitForBlipRevMessage(client.id, docID, docVersion2)

		// Check the resent request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err = msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":1234567890123}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":1234567890123}]}`, string(msgBody))
	})
}

// TestBlipDeltaSyncPullRemoved tests a simple pull replication that drops a document out of the user's channel.
func TestBlipDeltaSyncPullRemoved(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: &sgUseDeltas,
				},
			},
		},
		SyncFn: channels.DocChannelsSyncFunction,
	}
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[VersionVectorSubtestName] = true // test requires v2 subprotocol
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "alice",
			Channels:               []string{"public"},
			ClientDeltas:           true,
			SupportedBLIPProtocols: []string{db.CBMobileReplicationV2.SubprotocolString()},
		})
		defer client.Close()

		btcRunner.StartPull(client.id)

		// create doc1 rev 1-1513b53e2738671e634d9dd111f48de0
		version := rt.PutDocDirectly(docID, JsonToMap(t, `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`))

		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Contains(t, string(data), `"channels":["public"]`)
		assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

		// create doc1 rev 2-ff91e11bc1fd12bbb4815a06571859a9
		version = rt.UpdateDocDirectly(docID, version, JsonToMap(t, `{"channels": ["private"], "greetings": [{"hello": "world!"}, {"hi": "bob"}]}`))

		data = btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"_removed":true}`, string(data))

		msg := client.pullReplication.WaitForMessage(5)
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"_removed":true}`, string(msgBody))
	})
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
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: &sgUseDeltas,
				},
			},
		},
		SyncFn: channels.DocChannelsSyncFunction,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	var deltaCacheHitsStart int64
	var deltaCacheMissesStart int64
	var deltasRequestedStart int64
	var deltasSentStart int64

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			rtConfig)
		defer rt.Close()

		if rt.GetDatabase().DbStats.DeltaSync() != nil {
			deltaCacheHitsStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
			deltaCacheMissesStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
			deltasRequestedStart = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
			deltasSentStart = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
		}

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "alice",
			Channels:               []string{"public"},
			ClientDeltas:           true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer client.Close()

		btcRunner.StartPull(client.id)

		const docID = "doc1"
		// create doc1 rev 1-e89945d756a1d444fa212bffbbb31941
		version := rt.PutDocDirectly(docID, JsonToMap(t, `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`))
		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Contains(t, string(data), `"channels":["public"]`)
		assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

		// tombstone doc1 at rev 2-2db70833630b396ef98a3ec75b3e90fc
		version = rt.DeleteDocDirectly(docID, version)

		data = btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{}`, string(data))

		msg := client.pullReplication.WaitForMessage(5)
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
		// delta sync not implemented for rev tree replication, CBG-3748
		sgCanUseDelta := base.IsEnterpriseEdition() && client.UseHLV()
		if sgCanUseDelta {
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
	})
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
	rtConfig := &RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}}
	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			rtConfig)
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
		client1 := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "client1",
			Channels:               []string{"*"},
			ClientDeltas:           true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer client1.Close()

		client2 := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "client2",
			Channels:               []string{"*"},
			ClientDeltas:           true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer client2.Close()

		btcRunner.StartPull(client1.id)

		// create doc1 rev 1-e89945d756a1d444fa212bffbbb31941
		version := rt.PutDocDirectly(docID, JsonToMap(t, `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`))

		data := btcRunner.WaitForVersion(client1.id, docID, version)
		assert.Contains(t, string(data), `"channels":["public"]`)
		assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

		// Have client2 get only rev-1 and then stop replicating
		btcRunner.StartOneshotPull(client2.id)
		data = btcRunner.WaitForVersion(client2.id, docID, version)
		assert.Contains(t, string(data), `"channels":["public"]`)
		assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

		// tombstone doc1 at rev 2-2db70833630b396ef98a3ec75b3e90fc
		version = rt.DeleteDocDirectly(docID, version)

		data = btcRunner.WaitForVersion(client1.id, docID, version)
		assert.Equal(t, `{}`, string(data))
		msg := btcRunner.WaitForBlipRevMessage(client1.id, docID, version)

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
		btcRunner.StartOneshotPull(client2.id)
		data = btcRunner.WaitForVersion(client2.id, docID, version)
		assert.Equal(t, `{}`, string(data))
		msg = btcRunner.WaitForBlipRevMessage(client2.id, docID, version)

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

		// delta sync not implemented for rev tree replication, CBG-3748
		sgCanUseDelta := base.IsEnterpriseEdition() && client1.UseHLV()
		if sgCanUseDelta {
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
	})
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
	const docID = "doc1"
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()
		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		client.ClientDeltas = true
		sgCanUseDeltas := base.IsEnterpriseEdition() && client.UseHLV()
		btcRunner.StartPull(client.id)

		// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
		version1 := rt.PutDocDirectly(docID, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))

		data := btcRunner.WaitForVersion(client.id, docID, version1)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// Perform a one-shot pull as client 2 to pull down the first revision
		client2 := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client2.Close()

		client2.ClientDeltas = true
		btcRunner.StartOneshotPull(client2.id)
		data = btcRunner.WaitForVersion(client2.id, docID, version1)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
		version2 := rt.UpdateDocDirectly(docID, version1, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": "bob"}]}`))

		data = btcRunner.WaitForVersion(client.id, docID, version2)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))
		msg := btcRunner.WaitForBlipRevMessage(client.id, docID, version2)

		// Check EE is delta
		// Check the request was sent with the correct deltaSrc property
		// delta sync not implemented for rev tree replication, CBG-3748
		if sgCanUseDeltas {
			client.AssertDeltaSrcProperty(t, msg, version1)
		} else {
			assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		}
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		if sgCanUseDeltas {
			assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		} else {
			assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
		}

		deltaCacheHits := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMisses := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()

		// Run another one shot pull to get the 2nd revision - validate it comes as delta, and uses cached version
		client2.ClientDeltas = true
		btcRunner.StartOneshotPull(client2.id)
		msg2 := btcRunner.WaitForBlipRevMessage(client2.id, docID, version2)

		// Check the request was sent with the correct deltaSrc property
		if sgCanUseDeltas {
			client2.AssertDeltaSrcProperty(t, msg2, version1)
		} else {
			assert.Equal(t, "", msg2.Properties[db.RevMessageDeltaSrc])
		}
		// Check the request body was the actual delta
		msgBody2, err := msg2.Body()
		assert.NoError(t, err)
		if sgCanUseDeltas {
			assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody2))
		} else {
			assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody2))
		}

		updatedDeltaCacheHits := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		updatedDeltaCacheMisses := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()

		// delta sync not implemented for rev tree replication, CBG-3748
		if sgCanUseDeltas {
			assert.Equal(t, deltaCacheHits+1, updatedDeltaCacheHits)
			assert.Equal(t, deltaCacheMisses, updatedDeltaCacheMisses)
		} else {
			assert.Equal(t, deltaCacheHits, updatedDeltaCacheHits)
			assert.Equal(t, deltaCacheMisses, updatedDeltaCacheMisses)
		}
	})
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
	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()
		collection, _ := rt.GetSingleTestDatabaseCollection()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()
		client.ClientDeltas = true
		sgCanUseDeltas := base.IsEnterpriseEdition() && client.UseHLV()

		btcRunner.StartPull(client.id)

		// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
		version := rt.PutDocDirectly(docID, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))

		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))
		// create doc1 rev 2-abc on client
		newRev, err := btcRunner.PushRev(client.id, docID, version, []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
		assert.NoError(t, err)

		// Check EE is delta, and CE is full-body replication
		msg := client.waitForReplicationMessage(collection, 2)

		if base.IsEnterpriseEdition() && sgCanUseDeltas {
			// Check the request was sent with the correct deltaSrc property
			client.AssertDeltaSrcProperty(t, msg, version)
			// Check the request body was the actual delta
			msgBody, err := msg.Body()
			assert.NoError(t, err)
			assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

			collection, ctx := rt.GetSingleTestDatabaseCollection()
			// Validate that generation of a delta didn't mutate the revision body in the revision cache
			docRev, cacheErr := collection.GetRevisionCacheForTest().GetWithRev(ctx, "doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", db.RevCacheOmitDelta)
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

		respBody := rt.GetDocVersion(docID, newRev)
		assert.Equal(t, "doc1", respBody[db.BodyId])
		greetings := respBody["greetings"].([]interface{})
		assert.Len(t, greetings, 3)
		assert.Equal(t, map[string]interface{}{"hello": "world!"}, greetings[0])
		assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[1])
		assert.Equal(t, map[string]interface{}{"howdy": "bob"}, greetings[2])

		// tombstone doc1 (gets rev 3-f3be6c85e0362153005dae6f08fc68bb)
		deletedVersion := rt.DeleteDocDirectly(docID, newRev)

		data = btcRunner.WaitForVersion(client.id, docID, deletedVersion)
		assert.Equal(t, `{}`, string(data))

		var deltaPushDocCountStart int64

		if rt.GetDatabase().DbStats.DeltaSync() != nil {
			deltaPushDocCountStart = rt.GetDatabase().DbStats.DeltaSync().DeltaPushDocCount.Value()
		}

		_, err = btcRunner.PushRev(client.id, docID, deletedVersion, []byte(`{"undelete":true}`))

		if base.IsEnterpriseEdition() && sgCanUseDeltas {
			// Now make the client push up a delta that has the parent of the tombstone.
			// This is not a valid scenario, and is actively prevented on the CBL side.
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Can't use delta. Found tombstone for doc")
		} else {
			// Pushing a full body revision on top of a tombstone is valid.
			// CBL clients should fall back to this. The test client doesn't.
			assert.NoError(t, err)
		}

		var deltaPushDocCountEnd int64

		if rt.GetDatabase().DbStats.DeltaSync() != nil {
			deltaPushDocCountEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaPushDocCount.Value()
		}
		assert.Equal(t, deltaPushDocCountStart, deltaPushDocCountEnd)
	})
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
	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()
		collection, _ := rt.GetSingleTestDatabaseCollection()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		client.ClientDeltas = false
		btcRunner.StartPull(client.id)

		// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
		version := rt.PutDocDirectly(docID, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))

		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))
		// create doc1 rev 2-abcxyz on client
		newRev, err := btcRunner.PushRev(client.id, docID, version, []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
		assert.NoError(t, err)
		// Check EE is delta, and CE is full-body replication
		msg := client.waitForReplicationMessage(collection, 2)

		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))

		body := rt.GetDocVersion("doc1", newRev)
		require.Equal(t, "bob", body["greetings"].([]interface{})[2].(map[string]interface{})["howdy"])
	})
}
