// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package adminapitest

import (
	"net/http"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/sgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resyncStatusOn returns the resync status that node reports for dbName via GET /{db}/_resync.
func resyncStatusOn(t *testing.T, node *rest.RestTester, dbName string) db.ResyncManagerResponseDCP {
	t.Helper()
	resp := node.SendAdminRequest(http.MethodGet, "/"+dbName+"/_resync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var status db.ResyncManagerResponseDCP
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &status))
	return status
}

// waitUntilAnyNodeBlocked waits for whichever armed node reaches its first user document.
// With a single resync partition exactly one node owns the pindex and cbgt's planner decides
// which, so every node is armed and the first one to block wins.
func waitUntilAnyNodeBlocked(t *testing.T, pausers ...*resyncPauser) {
	t.Helper()

	anyBlocked := make(chan struct{})
	giveUp := make(chan struct{})
	t.Cleanup(func() { close(giveUp) })

	var once sync.Once
	for _, pauser := range pausers {
		go func(pauser *resyncPauser) {
			select {
			case <-pauser.blocked:
				once.Do(func() { close(anyBlocked) })
			case <-giveUp:
			}
		}(pauser)
	}

	base.RequireChanClosed(t, anyBlocked, "no node blocked at a resync document")
}

// TestResyncStopFromNonCoordinatingNode checks whether a resync started on one node can be
// stopped through a different node of the same cluster.
//
// GET /{db}/_resync reports cluster-wide status (BackgroundManager.GetStatus reads the
// bucket-persisted status), so every node reports "running". POST /{db}/_resync?action=stop
// gates on node-local h.db.State (handlePostResync), which only the node that served
// action=start ever moves to DBResyncing.
//
// A resyncPauser holds the resync open at its first document so the stop always arrives while
// the resync is genuinely in flight, rather than depending on it being slow enough to catch.
//
// The same check covers both resync modes, which differ in how a stop is meant to reach the
// other nodes:
//
//   - distributed (EE + Couchbase Server, db.useShardedDCP() true -> multiNode
//     BackgroundManager): stopProcess writes "stopping" to the shared status doc and the other
//     nodes' pollers pick it up.
//   - non-distributed (CE, or rosmar -> singleNode BackgroundManager): markStop writes
//     ShouldStop to the heartbeat doc to signal the other nodes directly.
func TestResyncStopFromNonCoordinatingNode(t *testing.T) {
	const dbName = "db"

	distributed := base.IsEnterpriseEdition() && !base.UnitTestUrlIsWalrus()
	t.Logf("resync mode under test: distributed=%t (enterprise=%t, walrus=%t)",
		distributed, base.IsEnterpriseEdition(), base.UnitTestUrlIsWalrus())

	// Every node needs a leaky bucket: a distributed resync shards its work across the
	// cluster, so holding it in flight takes a pauser on each node, not just the starter.
	rtc := rest.NewRestTesterCluster(t, &rest.RestTesterClusterConfig{
		NumNodes:          2,
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	t.Cleanup(func() { rtc.Close(base.TestCtx(t)) })

	coordinator, other := rtc.Node(0), rtc.Node(1)

	dbConfig := rest.DbConfig{
		BucketConfig: rest.BucketConfig{Bucket: base.Ptr(coordinator.Bucket().GetName())},
		Index:        &rest.IndexConfig{NumReplicas: base.Ptr(uint(0))},
		UseViews:     base.Ptr(base.TestsDisableGSI()),
		// A single partition keeps the resync on one DCP worker, which the pauser requires.
		Unsupported: &db.UnsupportedOptions{ResyncPartitions: base.Ptr(uint16(1))},
	}
	rest.RequireStatus(t, coordinator.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// The other nodes have to load the persisted config before they can serve the database.
	_, err := rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)
	require.Contains(t, other.ServerContext().AllDatabaseNames(), dbName,
		"second node did not discover the database from the shared bucket")

	// All docs on vBucket 0 so a single DCP worker processes them serially, otherwise
	// concurrent pauser callbacks double-close its channel.
	docIDs := sgtest.VBucketDocIDs(t, coordinator.Bucket(), 0, 5)
	for _, docID := range docIDs {
		coordinator.PutDoc(docID, `{"foo":"bar"}`)
	}

	// A changed sync function is what gives resync documents to rewrite.
	dbConfig.Sync = base.Ptr(`function(doc){channel("ABC");}`)
	rest.RequireStatus(t, coordinator.ReplaceDbConfig(dbName, dbConfig), http.StatusCreated)
	_, err = rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)

	// _offline is node-local, so each node has to be taken offline individually: resync
	// requires DBOffline on the node handling the request.
	rtc.ForEachNode(func(node *rest.RestTester) {
		rest.RequireStatus(t, node.SendAdminRequest(http.MethodPost, "/"+dbName+"/_offline", ""), http.StatusOK)
	})

	// Arm every node: a distributed resync processes documents on whichever node owns the
	// pindex, so pausing only the coordinator would let the other node run the work to
	// completion before the stop is attempted.
	var pausers []*resyncPauser
	rtc.ForEachNode(func(node *rest.RestTester) {
		pauser := newResyncPauser(node)
		t.Cleanup(pauser.Close)
		pauser.Pause()
		pausers = append(pausers, pauser)
	})
	releaseAll := func() {
		for _, pauser := range pausers {
			pauser.Close()
		}
	}

	rest.RequireStatus(t, coordinator.SendAdminRequest(http.MethodPost,
		"/"+dbName+"/_resync?action=start&reset=true", ""), http.StatusOK)

	// Block at the first document, so the resync cannot finish before the stop is attempted.
	waitUntilAnyNodeBlocked(t, pausers...)

	coordinatorStatus := coordinator.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateRunning, dbName)
	otherStatus := other.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateRunning, dbName)
	require.Equal(t, coordinatorStatus.ResyncID, otherStatus.ResyncID,
		"nodes report different resync runs, so they are not observing the same operation")
	t.Logf("both nodes report running while paused mid-document, resync_id=%q", coordinatorStatus.ResyncID)

	// The behaviour under test: stop through the node that did NOT start the resync.
	stopOnOther := other.SendAdminRequest(http.MethodPost, "/"+dbName+"/_resync?action=stop", "")
	t.Logf("stop via non-coordinating node -> %d: %s", stopOnOther.Code, stopOnOther.Body.String())

	statusAfterOther := resyncStatusOn(t, other, dbName)
	t.Logf("status on non-coordinating node after its stop attempt: %q", statusAfterOther.State)

	if stopOnOther.Code == http.StatusOK {
		// If the stop is accepted it must actually take effect, on both nodes.
		releaseAll()
		coordinator.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateStopped, dbName)
		other.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateStopped, dbName)
		return
	}

	// A rejected stop must not leave the two endpoints on the same node contradicting each
	// other: reporting "running" from GET while refusing action=stop as "not running" gives a
	// client no way to act on the status it just read.
	assert.NotEqualf(t, db.BackgroundProcessStateRunning, statusAfterOther.State,
		"node reported resync %q from GET /_resync but rejected action=stop with %d: %s",
		statusAfterOther.State, stopOnOther.Code, stopOnOther.Body.String())

	// The coordinating node must still be able to stop its own resync, and because the resync
	// is pinned at its first document the stop is guaranteed to interrupt work in progress.
	stopOnCoordinator := coordinator.SendAdminRequest(http.MethodPost, "/"+dbName+"/_resync?action=stop", "")
	t.Logf("stop via coordinating node -> %d: %s", stopOnCoordinator.Code, stopOnCoordinator.Body.String())
	rest.RequireStatus(t, stopOnCoordinator, http.StatusOK)

	releaseAll()
	stopped := coordinator.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateStopped, dbName)
	t.Logf("after stop on coordinating node: state=%q docs_processed=%d/%d",
		stopped.State, stopped.DocsProcessed, len(docIDs))

	// How promptly an accepted stop takes effect differs between the two modes, so only the
	// non-distributed mode is held to interrupting work in progress. A distributed resync
	// observes the stop through the shared status doc rather than purely locally, and with a
	// workload this small it can finish every remaining document first - which is why this is
	// logged above rather than asserted for both modes.
	if !distributed {
		assert.Less(t, stopped.DocsProcessed, int64(len(docIDs)),
			"resync was paused at its first document, so a stop must have left documents unprocessed")
	}
}
