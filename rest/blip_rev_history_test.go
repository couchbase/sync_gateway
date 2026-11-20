// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestBlipPullRevHistoryAfterChange changes a doc that a client holds, then makes sure that the rev message the client
// pulls has the same history whether it is sent as a full rev, a delta cache miss or a delta cache hit. Most changes
// remove the doc from the user's channels, so the rev is a removal.
func TestBlipPullRevHistoryAfterChange(t *testing.T) {
	const (
		docID      = "doc1"
		rev1Body   = `{"group_id": 1, "channels": ["group-1"]}`
		fullRev    = "full rev"
		cacheMiss  = "delta cache miss"
		cacheHit   = "delta cache hit"
		requireAcc = `function (doc, oldDoc, meta) {
  if (doc._deleted) {
    if (oldDoc && oldDoc.group_id != null) {
      requireAccess("group-" + oldDoc.group_id);
      channel("group-" + oldDoc.group_id);
    }
    return;
  }
  if (doc.group_id == null) {
    throw({ forbidden: "group_id is required" });
  }
  var groupChannel = "group-" + doc.group_id;
  requireAccess(groupChannel);
  channel(groupChannel);
}`
	)
	// deleteInBucket deletes the doc in the bucket, as a N1QL DELETE would, and waits for the import.
	deleteInBucket := func(t *testing.T, rt *RestTester, _ DocVersion) {
		importsBefore := rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value()
		require.NoError(t, rt.GetSingleDataStore().Delete(rt.Context(), docID))
		base.RequireWaitForStat(t, rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value, importsBefore+1)
	}
	update := func(body string) func(*testing.T, *RestTester, DocVersion) {
		return func(t *testing.T, rt *RestTester, version DocVersion) {
			rt.UpdateDoc(docID, version, body)
		}
	}

	changes := []struct {
		name        string
		syncFn      string
		change      func(t *testing.T, rt *RestTester, version DocVersion)
		wantDeleted bool
		wantRemoval bool
	}{
		{name: "tombstone stays in channel", syncFn: `function (doc, oldDoc) { channel("group-1"); }`, change: deleteInBucket, wantDeleted: true},
		{name: "tombstone channel from oldDoc", syncFn: requireAcc, change: deleteInBucket, wantDeleted: true, wantRemoval: true},
		{name: "tombstone in no channel", syncFn: `function (doc, oldDoc) { if (doc._deleted) { return; } channel("group-1"); }`, change: deleteInBucket, wantDeleted: true, wantRemoval: true},
		{name: "live doc stays in channel", syncFn: channels.DocChannelsSyncFunction, change: update(`{"group_id": 1, "v": 2, "channels": ["group-1"]}`)},
		{name: "live doc moved to other channel", syncFn: channels.DocChannelsSyncFunction, change: update(`{"group_id": 1, "channels": ["group-2"]}`), wantRemoval: true},
	}
	variants := []struct {
		name string
		// legacyClientRev gives the client rev 1 as a revtree ID only, so it is a legacy remote.
		legacyClientRev bool
		// legacyServerRev writes rev 1 without an HLV, as a write before the upgrade would.
		legacyServerRev bool
	}{
		{name: "client has cv"},
		{name: "client has revtree id", legacyClientRev: true},
		{name: "server rev 1 has no hlv", legacyClientRev: true, legacyServerRev: true},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			btcRunner := NewBlipTesterClientRunner(t)
			if variant.legacyClientRev {
				btcRunner.SkipSubtest[RevtreeSubtestName] = true // every rev is a revtree rev on a V3 client
			}
			btcRunner.Run(func(t *testing.T) {
				for _, change := range changes {
					for _, path := range []string{fullRev, cacheMiss, cacheHit} {
						t.Run(change.name+"/"+path, func(t *testing.T) {
							deltas := path != fullRev
							if deltas && !base.IsEnterpriseEdition() {
								t.Skip("Delta sync only supported in EE")
							}
							rt := NewRestTester(t, &RestTesterConfig{
								SyncFn:     change.syncFn,
								AutoImport: new(true),
								DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
									DeltaSync: &DeltaSyncConfig{Enabled: new(deltas)},
								}},
							})
							defer rt.Close()
							rt.CreateUser("alice", []string{"group-1"})
							rt.CreateUser("bob", []string{"group-1", "group-2"})

							newClient := func(username string) *BlipTesterClient {
								client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
									Username:        username,
									ClientDeltas:    deltas,
									SendRevocations: true, // removals of live docs only go to clients that ask for revocations
									SourceID:        username + "Source",
								})
								t.Cleanup(client.Close)
								return client
							}

							var version1 DocVersion
							if variant.legacyServerRev {
								body := db.Body{"group_id": 1, "channels": []string{"group-1"}}
								version1 = rt.CreateDocNoHLV(docID, body).ExtractDocVersion()
							} else {
								// A client writes rev 1, so that the next server write has HLV history.
								writer := newClient("bob")
								clientVersion1 := btcRunner.AddRev(writer.id, docID, EmptyDocVersion(), []byte(rev1Body))
								btcRunner.StartPush(writer.id)
								if writer.UseHLV() {
									rt.WaitForVersionHLVOnly(docID, clientVersion1)
								} else {
									rt.WaitForVersionRevIDOnly(docID, clientVersion1)
								}
								version1, _ = rt.GetDoc(docID)
							}
							rev1Seq := rt.GetDocumentSequence(docID)

							alice := newClient("alice")
							if variant.legacyClientRev {
								btcRunner.AddRevTreeRev(alice.id, docID, version1.RevTreeID, EmptyDocVersion(), []byte(rev1Body))
							} else {
								btcRunner.StartOneshotPull(alice.id)
								btcRunner.WaitForVersion(alice.id, docID, version1)
							}

							change.change(t, rt, version1)
							rt.WaitForPendingChanges()
							doc := rt.GetDocument(docID)
							version2 := doc.ExtractDocVersion()
							hlvHistory := doc.HLV.ToHistoryForHLV()
							if alice.UseHLV() && !variant.legacyServerRev {
								require.NotEmpty(t, hlvHistory)
							}

							deltaSrc, toRev := version1.RevTreeID, version2.RevTreeID
							if alice.UseHLV() {
								toRev = version2.CV.String()
								if !variant.legacyClientRev {
									deltaSrc = version1.CV.String()
								}
							}
							if path == cacheHit {
								collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
								delta, _, err := collection.GetDelta(ctx, docID, deltaSrc, toRev)
								require.NoError(t, err)
								require.NotNil(t, delta, "admin GetDelta should cache the delta")
							}
							var missesBefore int64
							if deltas {
								missesBefore = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
							}

							// A pull from sequence 0 leaves out removals, so pull from rev 1's sequence.
							btcRunner.StartPullSince(alice.id, BlipTesterPullOptions{Since: strconv.FormatUint(rev1Seq, 10), Continuous: true})
							msg := btcRunner.WaitForPullRevMessage(alice.id, docID, version2)

							switch path {
							case cacheMiss:
								require.Equal(t, missesBefore+1, rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value(), "GetDelta should build the delta")
							case cacheHit:
								require.Equal(t, missesBefore, rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value(), "GetDelta should use the cached delta")
							}
							if change.wantDeleted {
								assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted])
							} else {
								assert.Empty(t, msg.Properties[db.RevMessageDeleted])
							}
							if change.wantRemoval {
								assert.Empty(t, msg.Properties[db.RevMessageDeltaSrc], "a removal is never sent as a delta")
								if !change.wantDeleted {
									body, err := msg.Body()
									require.NoError(t, err)
									assert.Equal(t, db.RemovedRedactedDocument, string(body))
								}
							}

							var wantHistory string
							switch {
							case !alice.UseHLV():
								wantHistory = version1.RevTreeID
							case variant.legacyClientRev:
								wantHistory = strings.Join([]string{version2.RevTreeID, version1.RevTreeID}, ",")
								if hlvHistory != "" {
									wantHistory = hlvHistory + "," + wantHistory
								}
							default:
								wantHistory = hlvHistory
							}
							assert.Equal(t, wantHistory, msg.Properties[db.RevMessageHistory])
						})
					}
				}
			})
		})
	}
}
