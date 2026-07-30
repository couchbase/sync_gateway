/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestBlipConflictResolution covers CBL-side conflict resolution over BLIP, including the norev-clobber
// guard (CBG-5547), as subtests sharing one RestTester/database.
func TestBlipConflictResolution(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyCache, base.KeyCRUD, base.KeySGTest)

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig:  true,
		GuestEnabled:      true, // pulled as guest by PullConflictNoRevLosesBody
		SyncFn:            channels.DocChannelsSyncFunction,
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.DeltaSync = &DeltaSyncConfig{Enabled: base.Ptr(true)}
	dbConfig.AutoImport = false
	RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	testCases := []struct {
		name string
		test func(t *testing.T, rt *RestTester)
	}{
		{"PullConflict", testBlipPullConflict},
		{"PullConflictNoRevLosesBody", testBlipPullConflictNoRevLosesBody},
		{"NoRevOnCorruptHistoryDelta", testBlipNoRevOnCorruptHistoryDelta},
		{"NoRevIgnoredSingleChangesEntry", testBlipNoRevIgnoredSingleChangesEntry},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { tc.test(t, rt) })
	}
}

func testBlipPullConflict(t *testing.T, rt *RestTester) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true

	btcRunner.Run(func(t *testing.T) {
		const (
			alice   = "alice"
			cblBody = `{"actor": "cbl"}`
		)
		docID := SafeDocumentName(t, t.Name())
		rt.CreateUser(alice, []string{"*"})
		sgVersion := rt.PutDoc(docID, `{"actor": "sg", "channels": ["shared"]}`)
		rt.WaitForPendingChanges()

		opts := &BlipTesterClientOpts{
			Username: alice,
		}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		client := btcRunner.SingleCollection(btc.id)
		preConflictCBLVersion := btcRunner.AddRev(btc.id, docID, EmptyDocVersion(), []byte(cblBody))
		require.NotEqual(t, sgVersion, preConflictCBLVersion)
		_, preConflictHLV, _ := client.GetDoc(docID)
		require.Empty(t, preConflictHLV.PreviousVersions)
		require.Empty(t, preConflictHLV.MergeVersions)

		btcRunner.StartOneshotPull(btc.id)

		// expect resolution as CBL wins (local wins)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			body, postConflictHLV, _ := client.GetDoc(docID)
			assert.Equal(c, db.HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           preConflictCBLVersion.CV.Value,
				SourceID:          preConflictCBLVersion.CV.SourceID,
				PreviousVersions: db.HLVVersions{
					sgVersion.CV.SourceID: sgVersion.CV.Value,
				},
			}, *postConflictHLV)
			assert.Equal(c, string(body), cblBody)
		}, time.Second*10, time.Millisecond*10)
	})
}

// testBlipPullConflictNoRevLosesBody asserts a bodyless norev can't overwrite a client's known-good revision
// during conflict resolution.
func testBlipPullConflictNoRevLosesBody(t *testing.T, rt *RestTester) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // requires HLV-based conflict detection

	btcRunner.Run(func(t *testing.T) {
		const cblBody = `{"actor": "cbl"}`
		docID := SafeDocumentName(t, t.Name())

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()

		client := btcRunner.SingleCollection(btc.id)
		cblVersion := btcRunner.AddRev(btc.id, docID, EmptyDocVersion(), []byte(cblBody))

		// SG writes its own conflicting revision afterwards, so it genuinely wins LWW resolution.
		sgVersion := rt.PutDoc(docID, `{"actor": "sg", "channels": ["shared"]}`)
		require.Greater(t, sgVersion.CV.Value, cblVersion.CV.Value)
		rt.WaitForPendingChanges()

		// Force SG to fail to retrieve its own revision body, so the pull can only deliver a bodyless norev.
		rt.GetDatabase().FlushRevisionCacheForTest()
		leakyDataStore, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore(rt.Context()))
		require.True(t, ok)
		leakyDataStore.SetGetRawCallback(func(string) error { return gocb.ErrDocumentNotFound })
		leakyDataStore.SetGetWithXattrCallback(func(string) error { return gocb.ErrDocumentNotFound })
		t.Cleanup(func() {
			// reset the leaky callbacks so they don't leak into other subtests sharing this RestTester
			leakyDataStore.SetGetRawCallback(nil)
			leakyDataStore.SetGetWithXattrCallback(nil)
		})

		btcRunner.StartOneshotPull(btc.id)

		btcRunner.WaitForPullNoRevMessage(btc.id, docID, sgVersion)

		body, _, _ := client.GetDoc(docID)
		require.Equal(t, []byte(cblBody), body, "norev overwrote known-good revision with no body")
	})
}

// testBlipNoRevIgnoredSingleChangesEntry asserts that once a bodyless norev is ignored because the client
// already holds real content for the doc (CBG-5547), the client's push-changes iteration (OneShotChangesSince)
// still surfaces the document exactly once. A stale second _seqStore entry left behind for the ignored norev's
// sequence previously caused the same revision to be proposed twice during push replication.
func testBlipNoRevIgnoredSingleChangesEntry(t *testing.T, rt *RestTester) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // requires HLV-based conflict detection

	btcRunner.Run(func(t *testing.T) {
		const cblBody = `{"actor": "cbl"}`
		docID := SafeDocumentName(t, t.Name())

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()

		client := btcRunner.SingleCollection(btc.id)
		cblVersion := btcRunner.AddRev(btc.id, docID, EmptyDocVersion(), []byte(cblBody))

		// SG writes its own conflicting revision afterwards, so it genuinely wins LWW resolution.
		sgVersion := rt.PutDoc(docID, `{"actor": "sg", "channels": ["shared"]}`)
		require.Greater(t, sgVersion.CV.Value, cblVersion.CV.Value)
		rt.WaitForPendingChanges()

		// Force SG to fail to retrieve its own revision body, so the pull can only deliver a bodyless norev.
		rt.GetDatabase().FlushRevisionCacheForTest()
		leakyDataStore, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore(rt.Context()))
		require.True(t, ok)
		leakyDataStore.SetGetRawCallback(func(string) error { return gocb.ErrDocumentNotFound })
		leakyDataStore.SetGetWithXattrCallback(func(string) error { return gocb.ErrDocumentNotFound })
		t.Cleanup(func() {
			// reset the leaky callbacks so they don't leak into other subtests sharing this RestTester
			leakyDataStore.SetGetRawCallback(nil)
			leakyDataStore.SetGetWithXattrCallback(nil)
		})

		btcRunner.StartOneshotPull(btc.id)
		btcRunner.WaitForPullNoRevMessage(btc.id, docID, sgVersion)

		var matches []proposeChangeBatchEntry
		for _, change := range client.OneShotChangesSince(rt.Context(), 0) {
			if change.docID == docID {
				matches = append(matches, *change)
			}
		}
		require.Len(t, matches, 1, "expected exactly one push-changes entry for docID %q, got %#v", docID, matches)
	})
}

// testBlipNoRevOnCorruptHistoryDelta asserts a norev papering over corrupt server-side revision history
// doesn't clobber a real revision the client already pulled for the same document.
func testBlipNoRevOnCorruptHistoryDelta(t *testing.T, rt *RestTester) {
	base.TestRequiresDeltaSync(t)
	btcRunner := NewBlipTesterClientRunner(t)
	// a norev message is only sent on delta sync when v2 protocol is used, otherwise the deleted flag on a changes
	// message is used
	btcRunner.RunSubprotocolV2(func(t *testing.T) {
		const (
			user     = "corruptHistoryUser"
			channelA = "A"
		)
		rt.CreateUser(user, []string{channelA})
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:     user,
			ClientDeltas: true,
		})
		defer btc.Close()

		ctx := rt.Context()
		docID := SafeDocumentName(t, t.Name())
		docRev1 := rt.CreateDocNoHLV(docID, db.Body{"delta": true, "channels": []string{channelA}})
		btcRunner.StartOneshotPull(btc.id)
		btcRunner.WaitForVersion(btc.id, docID, DocVersion{RevTreeID: docRev1.GetRevTreeID()})
		seq, err := rt.GetDatabase().NextSequence(ctx)
		require.NoError(t, err)
		// document contains an invalid revtree
		//
		// 3-c is a child of 3-d, a revision must be one or more generations higher than it its parent, not equal
		badSyncRaw := `{
			"cas": "expand",
			"channel_set":  [
				{
					"end": {{.rev3seq}},
					"name": "A",
					"start": {{.rev1seq}}
				}
			],
			"channels": {
				"A": {
					"rev": "{{.rev1}}",
					"seq": {{.rev3seq}}
				}
			},
			"channel_set_history": null,
			"history": {
				"parents": [
					3,
					0,
					-1,
					2
				],
				"revs": [
					"3-d",
					"3-c",
					"{{.rev1}}",
					"2-b"
				]
			},
			"rev": "3-c",
			"sequence": {{.rev3seq}},
			"value_crc32c": "expand"
		}`
		tmpl := template.Must(template.New("badSync").Option("missingkey=error").Parse(badSyncRaw))
		var badSyncData bytes.Buffer
		require.NoError(t, tmpl.Execute(&badSyncData, map[string]any{
			"rev1":    docRev1.GetRevTreeID(),
			"rev1seq": docRev1.Sequence,
			"rev3seq": seq,
		}))

		mutateInOptions := db.DefaultMutateInOpts()
		_, err = rt.GetSingleDataStore().WriteWithXattrs(
			ctx,
			docID,
			0,
			docRev1.Cas,
			[]byte(`{"key":"value"}`),
			map[string][]byte{
				base.SyncXattrName: badSyncData.Bytes(),
			},
			nil,
			mutateInOptions,
		)
		require.NoError(t, err)

		rt.WaitForPendingChanges()
		expectedVersion := DocVersion{RevTreeID: "3-c"}

		btcRunner.StartOneshotPull(btc.id)
		btcRunner.WaitForPullNoRevMessage(btc.id, docID, expectedVersion)
	})
}
