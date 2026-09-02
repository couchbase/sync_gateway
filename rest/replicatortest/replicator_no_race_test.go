//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// Tests that can't run under the race detector.  Consider moving back to replicator_test.go after CBG-5472.
//
// base.SyncGatewayStats.DbStats is a process-global map keyed by database name, so two RestTesters sharing a
// name share one entry - the later registration replaces the earlier, and DatabaseContext.Close clears
// whichever *DbStats the entry holds rather than its own.  Harmless with two contexts, since the last to
// register is the last to close.  A config reload adds a third, so one database's close clears stats still
// being written by another that is open (SubscribeCfgChanges -> InitializeReplication), once per stat field.
//go:build !race
// +build !race

package replicatortest

import (
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplicationHeartbeatRemovalPushWithConfigReload is a push variant of TestReplicationHeartbeatRemoval
// that races the heartbeat-driven node removal against a db config reload, and checks the cluster settles:
// both nodes re-register with one replication each, later writes reach the remote, both report running.
func TestReplicationHeartbeatRemovalPushWithConfigReload(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only (replication rebalance)")
	}

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		t.Cleanup(reduceTestCheckpointInterval(50 * time.Millisecond))
		t.Cleanup(db.SuspendSequenceBatching())

		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

		docABC1 := rest.SafeDocumentName(t, t.Name()+"ABC1")
		docDEF1 := rest.SafeDocumentName(t, t.Name()+"DEF1")
		_ = activeRT.PutDoc(docABC1, `{"source":"activeRT","channels":["ABC"]}`)
		_ = activeRT.PutDoc(docDEF1, `{"source":"activeRT","channels":["DEF"]}`)
		activeRT.WaitForPendingChanges()

		activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePush, []string{"ABC"}, true, db.ConflictResolverDefault, "")
		activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePush, []string{"DEF"}, true, db.ConflictResolverDefault, "")
		activeRT.WaitForAssignedReplications(2)
		activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
		activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

		changesResults := remoteRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, []string{docABC1, docDEF1})

		activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
		defer activeRT2.Close()

		activeRT.WaitForAssignedReplications(1)
		activeRT2.WaitForAssignedReplications(1)

		activeRTUUID := activeRT.GetDatabase().UUID
		activeRT2UUID := activeRT2.GetDatabase().UUID
		activeRTMgr := activeRT.GetDatabase().SGReplicateMgr
		activeRT2Mgr := activeRT2.GetDatabase().SGReplicateMgr
		dbName := activeRT.GetDatabase().Name
		currentConfig := activeRT.ServerContext().GetDatabaseConfig(dbName).DatabaseConfig

		// The production combination: PUT /db/_config landing while node membership flaps.  Reload the
		// current config unchanged, to avoid unrelated REST-layer config validation quirks.
		var wg sync.WaitGroup
		wg.Go(func() {
			// Can't distinguish "removed" from "skipped" - updateCluster returns nil once stopped.
			assert.NoError(t, activeRTMgr.RemoveNode(activeRT2UUID))
		})
		wg.Go(func() {
			err := activeRT.ServerContext().ReloadDatabaseWithConfig(base.NewNonCancelCtx(), currentConfig)
			t.Logf("ReloadDatabaseWithConfig error: %v", err)
		})
		base.WaitWithTimeout(t, &wg, time.Minute)

		// The reload already removed activeRTUUID, since each DatabaseContext registers a fresh one, so
		// re-removing it would no-op.  Re-reading also confirms the reload happened.
		reloadedUUID := activeRT.GetDatabase().UUID
		require.NotEqual(t, activeRTUUID, reloadedUUID, "config reload did not replace the DatabaseContext")

		// Wait for the reloaded context to register, or the removal races registration and no-ops again.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			cluster, err := activeRT2Mgr.GetSGRCluster()
			if !assert.NoError(c, err) {
				return
			}
			_, ok := cluster.Nodes[reloadedUUID]
			assert.True(c, ok)
		}, time.Second*20, time.Millisecond*100, "reloaded node never registered in the cluster cfg")

		assert.NoError(t, activeRT2Mgr.RemoveNode(reloadedUUID))

		// Wait for nodes to re-register, re-fetching the manager in case the reload replaced it.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			clusterDef, err := activeRT.GetDatabase().SGReplicateMgr.GetSGRCluster()
			if !assert.NoError(c, err) {
				return
			}
			assert.Len(c, clusterDef.Nodes, 2)
		}, time.Second*20, time.Millisecond*100, "Nodes did not re-register after removal")

		activeRT.WaitForAssignedReplications(1)
		activeRT2.WaitForAssignedReplications(1)

		docABC2 := rest.SafeDocumentName(t, t.Name()+"ABC2")
		_ = activeRT.PutDoc(docABC2, `{"source":"activeRT","channels":["ABC"]}`)
		docDEF2 := rest.SafeDocumentName(t, t.Name()+"DEF2")
		_ = activeRT.PutDoc(docDEF2, `{"source":"activeRT","channels":["DEF"]}`)

		// Post-reload writes must still reach the remote, whichever node now owns the replications.
		changesResults = remoteRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

		// Status is shared across nodes, so either node running is enough.
		activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
		activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)
	})
}
