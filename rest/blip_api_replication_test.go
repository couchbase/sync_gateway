// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationBroadcastTickerChange(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Skipping test that requires xattrs")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			CacheConfig: &CacheConfig{
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxWaitPending: base.Ptr(uint32(100)),
				},
			},
		}},
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	docID := t.Name() + "_doc1"
	docID2 := t.Name() + "_doc2"

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()
		ctx := base.TestCtx(t)

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		btcRunner.StartPull(client.id)

		// create doc1 on SG and wait to replicate to client
		versionDoc1 := rt.PutDocDirectly(docID, JsonToMap(t, `{"test": "value"}`))
		btcRunner.WaitForVersion(client.id, docID, versionDoc1)

		// alter sync data of this doc to artificially create skipped sequences
		ds := rt.GetSingleDataStore()
		xattrs, cas, err := ds.GetXattrs(ctx, docID, []string{base.SyncXattrName})
		require.NoError(t, err)

		var retrievedXattr map[string]interface{}
		require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
		retrievedXattr["sequence"] = uint64(20)
		newXattrVal := map[string][]byte{
			base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
		}

		_, err = ds.UpdateXattrs(ctx, docID, 0, cas, newXattrVal, nil)
		require.NoError(t, err)

		// wait for value to move from pending to cache and skipped list to fill
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			rt.GetDatabase().UpdateCalculatedStats(ctx)
			assert.Equal(c, int64(1), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
			assert.True(c, rt.GetDatabase().BroadcastSlowMode.Load())
		}, time.Second*10, time.Millisecond*100)

		// assert new change added still replicates to client
		versionDoc2 := rt.PutDocDirectly(docID2, JsonToMap(t, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`))
		btcRunner.WaitForVersion(client.id, docID2, versionDoc2)

		// update doc1 that will trigger unused seq release to clear skipped and assert that update is received
		versionDoc1 = rt.UpdateDocDirectly(docID, versionDoc1, JsonToMap(t, `{"test": "new value"}`))
		btcRunner.WaitForVersion(client.id, docID, versionDoc1)

		// assert skipped is cleared and skipped sequence broadcast is not sent
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			rt.GetDatabase().UpdateCalculatedStats(ctx)
			assert.Equal(c, int64(0), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
			assert.False(c, rt.GetDatabase().BroadcastSlowMode.Load())
		}, time.Second*10, time.Millisecond*100)
	})
}

// TestBlipClientPushAndPullReplication sets up a bidi replication for a BlipTesterClient, writes documents on SG and the client and ensures they replicate.
func TestBlipClientPushAndPullReplication(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelTrace, base.KeyAll)

	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{}},
		GuestEnabled:   true,
	}
	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		btcRunner.StartPull(client.id)
		btcRunner.StartPush(client.id)

		// create doc1 on SG
		docBody := db.Body{"greetings": []map[string]interface{}{{"hello": "world!"}, {"hi": "alice"}}}
		version := rt.PutDocDirectly(docID, docBody)

		// wait for doc on client
		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// update doc1 on client
		newRev := btcRunner.AddRev(client.id, docID, &version, []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))

		// wait for update to arrive on SG
		rt.WaitForVersion(docID, newRev)

		body := rt.GetDocBody(docID)
		require.Equal(t, "bob", body["greetings"].([]interface{})[2].(map[string]interface{})["howdy"])
	})
}
