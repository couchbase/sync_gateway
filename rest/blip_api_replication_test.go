// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"math"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestReplicationBroadcastTickerChange(t *testing.T) {
	base.LongRunningTest(t)

	if !base.TestUseXattrs() {
		t.Skip("Skipping test that requires xattrs")
	}
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
		versionDoc1 := rt.PutDoc(docID, `{"test": "value"}`)
		btcRunner.WaitForVersion(client.id, docID, versionDoc1)

		// Artificial sequence jump with CAS expansion to prevent version mismatch on continuous pull replication.
		versionDoc1 = forceSequenceJump(rt, docID, 19)

		// wait for value to move from pending to cache and skipped list to fill
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			rt.GetDatabase().UpdateCalculatedStats(ctx)
			assert.Equal(c, int64(1), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
			assert.True(c, rt.GetDatabase().BroadcastSlowMode.Load())
		}, time.Second*10, time.Millisecond*100)

		// assert new change added still replicates to client
		versionDoc2 := rt.PutDoc(docID2, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
		btcRunner.WaitForVersion(client.id, docID2, versionDoc2)

		// update doc1 that will trigger unused seq release to clear skipped and assert that update is received
		versionDoc1 = rt.UpdateDoc(docID, versionDoc1, `{"test": "new value"}`)
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
	base.LongRunningTest(t)

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
		version := rt.PutDoc(docID, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`)

		// wait for doc on client
		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// update doc1 on client
		newRev := btcRunner.AddRev(client.id, docID, &version, []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))

		// wait for update to arrive on SG
		rt.WaitForVersion(docID, newRev)

		body := rt.GetDocBody(docID)
		require.Equal(t, "bob", body["greetings"].([]any)[2].(map[string]any)["howdy"])
	})
}

// forceSequenceJump updates the sequence of the document like it would be via CRUD API. The body is not
// changed so the revtree ID is not updated, but the version vector is updated.
func forceSequenceJump(rt *RestTester, docID string, sequenceOffset uint64) DocVersion {
	ds := rt.GetSingleDataStore()
	ctx := rt.Context()
	t := rt.TB()

	var lastRevTreeID string
	var currentSource string
	var hlvPresent bool
	var newHLVVersion uint64

	writeUpdateFunc := func(currentDoc []byte, currentXattrs map[string][]byte, _ uint64) (sgbucket.UpdatedDoc, error) {
		syncBytes, syncPresent := currentXattrs[base.SyncXattrName]
		if !syncPresent {
			return sgbucket.UpdatedDoc{}, fmt.Errorf("missing _sync xattr")
		}
		var retrievedSync db.SyncData
		require.NoError(t, base.JSONUnmarshal(syncBytes, &retrievedSync))

		lastRevTreeID = retrievedSync.RevAndVersion.RevTreeID
		currentSource = retrievedSync.RevAndVersion.CurrentSource

		// Modify sequence
		retrievedSync.Sequence += sequenceOffset

		// Set expand placeholder for Walrus mock macro-expansion
		retrievedSync.Cas = "expand"

		newXattrs := map[string][]byte{}

		spec := []sgbucket.MacroExpansionSpec{
			sgbucket.NewMacroExpansionSpec("_sync.cas", sgbucket.MacroCas),
		}

		// Update _vv if present
		vvBytes, vvPresent := currentXattrs[base.VvXattrName]
		hlvPresent = vvPresent
		if vvPresent {
			var retrievedVV db.HybridLogicalVector
			require.NoError(t, base.JSONUnmarshal(vvBytes, &retrievedVV))

			// Generate the next HLV version the same way a real write would: HLC, floored on the existing
			// value for this source, to guarantee it's monotonically increasing.
			newHLVVersion = rt.GetDatabase().GetHLCValueForTest(retrievedVV.Version)
			retrievedVV.Version = newHLVVersion
			retrievedSync.RevAndVersion.CurrentVersion = string(base.Uint64CASToLittleEndianHex(newHLVVersion))

			retrievedVV.CurrentVersionCAS = math.MaxUint64 // Set expand placeholder for Walrus mock macro-expansion

			newXattrs[base.VvXattrName] = base.MustJSONMarshal(t, retrievedVV)

			spec = append(spec, sgbucket.NewMacroExpansionSpec("_vv.cvCas", sgbucket.MacroCas))
		}

		newXattrs[base.SyncXattrName] = base.MustJSONMarshal(t, retrievedSync)

		return sgbucket.UpdatedDoc{
			Doc:    currentDoc,
			Xattrs: newXattrs,
			Spec:   spec,
		}, nil
	}

	_, err := ds.WriteUpdateWithXattrs(ctx, docID, []string{base.SyncXattrName, base.VvXattrName}, 0, nil, &sgbucket.MutateInOptions{}, writeUpdateFunc)
	require.NoError(t, err)

	if hlvPresent && currentSource == "" {
		require.FailNow(t, "hlvPresent is true but currentSource is empty")
	}

	var cv db.Version
	if hlvPresent {
		cv = db.CreateVersion(currentSource, newHLVVersion)
	}
	return DocVersion{
		RevTreeID: lastRevTreeID,
		CV:        cv,
	}
}
