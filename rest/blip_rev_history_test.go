// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
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
	changes := []struct {
		name   string
		syncFn string
		// rev2Body is the body of rev 2. If it is empty, rev 2 is a tombstone.
		rev2Body    string
		wantRemoval bool
		// needsImport is set when only an import makes the change, because an import runs the sync function without
		// oldDoc. A variant that writes rev 2 without an HLV makes the change through Sync Gateway instead.
		needsImport bool
	}{
		{name: "tombstone stays in channel", syncFn: `function (doc, oldDoc) { channel("group-1"); }`},
		{name: "tombstone channel from oldDoc", syncFn: requireAcc, wantRemoval: true, needsImport: true},
		{name: "tombstone in no channel", syncFn: `function (doc, oldDoc) { if (doc._deleted) { return; } channel("group-1"); }`, wantRemoval: true},
		{name: "live doc stays in channel", syncFn: channels.DocChannelsSyncFunction, rev2Body: `{"group_id": 1, "v": 2, "channels": ["group-1"]}`},
		{name: "live doc moved to other channel", syncFn: channels.DocChannelsSyncFunction, rev2Body: `{"group_id": 1, "channels": ["group-2"]}`, wantRemoval: true},
	}
	variants := []struct {
		name string
		// legacyClientRev gives the client rev 1 as a revtree ID only, so it is a legacy remote.
		legacyClientRev bool
		// serverRevsWithoutHLV is how many server revs are written without an HLV, as writes before the upgrade would.
		serverRevsWithoutHLV int
	}{
		{name: "client has cv"},
		{name: "client has revtree id", legacyClientRev: true},
		{name: "server rev 1 has no hlv", legacyClientRev: true, serverRevsWithoutHLV: 1},
		{name: "server revs have no hlv", legacyClientRev: true, serverRevsWithoutHLV: 2},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			btcRunner := NewBlipTesterClientRunner(t)
			if variant.legacyClientRev {
				btcRunner.SkipSubtest[RevtreeSubtestName] = true // every rev is a revtree rev on a V3 client
			}
			btcRunner.Run(func(t *testing.T) {
				for _, change := range changes {
					if change.needsImport && variant.serverRevsWithoutHLV == 2 {
						continue
					}
					for _, path := range revSendPaths {
						t.Run(change.name+"/"+string(path), func(t *testing.T) {
							path.skipIfUnsupported(t)
							deltas := path.deltas()
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
							if variant.serverRevsWithoutHLV > 0 {
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

							wantDeleted := change.rev2Body == ""
							switch {
							case variant.serverRevsWithoutHLV == 2:
								body := db.Body{}
								if wantDeleted {
									body[db.BodyDeleted] = true
								} else {
									require.NoError(t, base.JSONUnmarshal([]byte(change.rev2Body), &body))
								}
								body[db.BodyRev] = version1.RevTreeID
								rt.CreateDocNoHLV(docID, body)
							case wantDeleted:
								// Delete the doc in the bucket, as a N1QL DELETE would, and wait for the import.
								importsBefore := rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value()
								require.NoError(t, rt.GetSingleDataStore().Delete(rt.Context(), docID))
								base.RequireWaitForStat(t, rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value, importsBefore+1)
							default:
								rt.UpdateDoc(docID, version1, change.rev2Body)
							}
							rt.WaitForPendingChanges()
							doc := rt.GetDocument(docID)
							version2 := DocVersion{RevTreeID: doc.GetRevTreeID()}
							var hlvHistory string
							if doc.HLV != nil {
								version2 = doc.ExtractDocVersion()
								hlvHistory = doc.HLV.ToHistoryForHLV()
							}
							if alice.UseHLV() && variant.serverRevsWithoutHLV == 0 {
								require.NotEmpty(t, hlvHistory)
							}
							rev2HasHLV := variant.serverRevsWithoutHLV < 2
							require.Equal(t, rev2HasHLV, doc.HLV != nil)

							deltaSrc, toRev := version1.RevTreeID, version2.RevTreeID
							if alice.UseHLV() && rev2HasHLV {
								toRev = version2.CV.String()
								if !variant.legacyClientRev {
									deltaSrc = version1.CV.String()
								}
							}
							countsBefore := path.prepare(rt, docID, deltaSrc, toRev)

							// A pull from sequence 0 leaves out removals, so pull from rev 1's sequence.
							btcRunner.StartPullSince(alice.id, BlipTesterPullOptions{Since: strconv.FormatUint(rev1Seq, 10), Continuous: true})
							msg := btcRunner.WaitForPullRevMessage(alice.id, docID, version2)
							path.requireUsed(rt, countsBefore)
							if wantDeleted {
								assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted])
							} else {
								assert.Empty(t, msg.Properties[db.RevMessageDeleted])
							}
							if change.wantRemoval {
								assert.Empty(t, msg.Properties[db.RevMessageDeltaSrc], "a removal is never sent as a delta")
								if !wantDeleted {
									body, err := msg.Body()
									require.NoError(t, err)
									assert.Equal(t, db.RemovedRedactedDocument, string(body))
								}
							}

							var wantHistory string
							switch {
							case !alice.UseHLV() || !rev2HasHLV:
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

// TestISGRPullRemovedTombstoneHistory deletes a doc on the passive peer so that the tombstone leaves the replication
// user's channels, then makes sure that the active peer stores the tombstone as a child of rev 1. The passive peer
// sends the removal as a full rev, a delta cache miss or a delta cache hit, and on V4 the active peer builds its
// revtree from the revTree property.
func TestISGRPullRemovedTombstoneHistory(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	const (
		docID         = "doc1"
		replicationID = "pull"
		// A tombstone is in no channel, so it leaves the replication user's channels.
		syncFn = `function (doc, oldDoc) { if (doc._deleted) { return; } channel(doc.channels); }`
	)
	runner := NewSGRTestRunner(t)
	runner.Run(func(t *testing.T) {
		for _, path := range revSendPaths {
			t.Run(string(path), func(t *testing.T) {
				path.skipIfUnsupported(t)
				deltas := path.deltas()
				peers := runner.SetupSGRPeersWithOptions(t, TestISGRPeerOpts{
					UseDeltas:         deltas,
					UserChannelAccess: []string{"group-1"},
					PassiveSyncFn:     syncFn,
				})
				activeRT, passiveRT := peers.ActiveRT, peers.PassiveRT

				// On V3 the active peer makes its own CV, so only the revtree ID is replicated.
				waitForVersion, waitForTombstone := activeRT.WaitForVersionRevIDOnly, activeRT.WaitForTombstoneRevIDOnly
				if runner.IsV4Protocol() {
					waitForVersion, waitForTombstone = activeRT.WaitForVersion, activeRT.WaitForTombstone
				}

				version1 := passiveRT.PutDoc(docID, `{"channels": ["group-1"]}`)
				resp := activeRT.SendAdminRequest(http.MethodPost, "/{{.db}}/_replication/", MarshalConfig(t, db.ReplicationConfig{
					ID:                     replicationID,
					Direction:              db.ActiveReplicatorTypePull,
					Remote:                 peers.PassiveDBURL,
					Continuous:             true,
					ConflictResolutionType: db.ConflictResolverDefault,
					DeltaSyncEnabled:       deltas,
					CollectionsEnabled:     base.TestsUseNamedCollections(),
				}))
				RequireStatus(t, resp, http.StatusCreated)
				activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
				waitForVersion(docID, version1)
				activeRT.StopReplication(replicationID)

				version2 := passiveRT.DeleteDoc(docID, version1)
				passiveRT.WaitForPendingChanges()
				deltaSrc, toRev := version1.RevTreeID, version2.RevTreeID
				if runner.IsV4Protocol() {
					deltaSrc, toRev = version1.CV.String(), version2.CV.String()
				}
				countsBefore := path.prepare(passiveRT, docID, deltaSrc, toRev)

				activeRT.StartReplication(replicationID)
				waitForTombstone(docID, version2)
				activeRT.StopReplication(replicationID)
				path.requireUsed(passiveRT, countsBefore)

				doc := activeRT.GetDocument(docID)
				assert.True(t, doc.IsDeleted())
				rev2Info, ok := doc.History[version2.RevTreeID]
				require.True(t, ok, "active peer should hold rev %s", version2.RevTreeID)
				assert.Equal(t, version1.RevTreeID, rev2Info.Parent)
				assert.ElementsMatch(t, []string{version2.RevTreeID}, doc.History.GetLeaves())
			})
		}
	})
}

// revSendPath is how Sync Gateway sends a rev to a peer that holds an earlier rev of the doc.
type revSendPath string

const (
	revSendFullRev   revSendPath = "full rev"
	revSendCacheMiss revSendPath = "delta cache miss"
	revSendCacheHit  revSendPath = "delta cache hit"
)

var revSendPaths = []revSendPath{revSendFullRev, revSendCacheMiss, revSendCacheHit}

// skipIfUnsupported skips a delta path on CE, which has no delta sync.
func (p revSendPath) skipIfUnsupported(t *testing.T) {
	if p.deltas() && !base.IsEnterpriseEdition() {
		t.Skip("Delta sync only supported in EE")
	}
}

// deltas returns true if delta sync must be enabled for this path.
func (p revSendPath) deltas() bool {
	return p != revSendFullRev
}

// deltaCacheCounts holds the delta cache stats from before the rev is sent.
type deltaCacheCounts struct {
	hits, misses int64
}

// prepare caches the delta from deltaSrc to toRev for a cache hit, then returns the delta cache counts for requireUsed.
// Call it after the rev is written and before the peer asks for it.
func (p revSendPath) prepare(rt *RestTester, docID, deltaSrc, toRev string) deltaCacheCounts {
	rt.TB().Helper()
	if !p.deltas() {
		return deltaCacheCounts{}
	}
	if p == revSendCacheHit {
		collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
		delta, _, err := collection.GetDelta(ctx, docID, deltaSrc, toRev)
		require.NoError(rt.TB(), err)
		require.NotNil(rt.TB(), delta, "admin GetDelta should cache the delta")
	}
	stats := rt.GetDatabase().DbStats.DeltaSync()
	return deltaCacheCounts{hits: stats.DeltaCacheHit.Value(), misses: stats.DeltaCacheMiss.Value()}
}

// requireUsed requires that rt sent the rev by this path since prepare returned before.
func (p revSendPath) requireUsed(rt *RestTester, before deltaCacheCounts) {
	rt.TB().Helper()
	if !p.deltas() {
		return
	}
	stats := rt.GetDatabase().DbStats.DeltaSync()
	switch p {
	case revSendCacheMiss:
		require.Equal(rt.TB(), before.misses+1, stats.DeltaCacheMiss.Value(), "GetDelta should build the delta")
		require.Equal(rt.TB(), before.hits, stats.DeltaCacheHit.Value(), "GetDelta should not find a cached delta")
	case revSendCacheHit:
		require.Equal(rt.TB(), before.misses, stats.DeltaCacheMiss.Value(), "GetDelta should not build a delta")
		require.Equal(rt.TB(), before.hits+1, stats.DeltaCacheHit.Value(), "GetDelta should use the cached delta")
	}
}
