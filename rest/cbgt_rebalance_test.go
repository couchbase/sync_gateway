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
	"strings"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// findPIndexOwningVbucket returns the name of the PIndex in snapshot whose SourcePartitions
// includes the given vbucket, and true if found.
func findPIndexOwningVbucket(snapshot map[string]db.ImportPartitionInfo, vbucket string) (string, bool) {
	for name, info := range snapshot {
		for vb := range strings.SplitSeq(info.SourcePartitions, ",") {
			if vb == vbucket {
				return name, true
			}
		}
	}
	return "", false
}

// assertPartitionsFullyCoveredExactlyOnce asserts every name in allPartitions appears in exactly
// one of the given snapshots (never dropped, never duplicated across nodes).
func assertPartitionsFullyCoveredExactlyOnce(t *testing.T, allPartitions map[string]db.ImportPartitionInfo, snapshots ...map[string]db.ImportPartitionInfo) {
	t.Helper()
	total := 0
	for _, snapshot := range snapshots {
		total += len(snapshot)
	}
	assert.Equal(t, len(allPartitions), total, "expected every partition to be accounted for exactly once across all nodes")

	for name := range allPartitions {
		owners := 0
		for _, snapshot := range snapshots {
			if _, ok := snapshot[name]; ok {
				owners++
			}
		}
		assert.Equal(t, 1, owners, "expected PIndex %s to be running on exactly one node", name)
	}
}

// assertUnmovedPIndexesUnchanged asserts that any partition present in both before and after for
// the same node kept its UUID (no spurious PIndex/DCP feed teardown+recreate).
func assertUnmovedPIndexesUnchanged(t *testing.T, nodeLabel string, before, after map[string]db.ImportPartitionInfo) {
	t.Helper()
	var restarted []string
	for name, beforeInfo := range before {
		afterInfo, stillHere := after[name]
		if !stillHere {
			continue // legitimately moved to another node
		}
		if afterInfo.UUID != beforeInfo.UUID {
			restarted = append(restarted, name)
		}
	}
	assert.Empty(t, restarted, "PIndexes that remained on %s should not have been torn down/recreated: %v", nodeLabel, restarted)
}

// readCbgtImportIndexUUID reads the persisted cbgt IndexDefs doc directly from the metadata store
// and returns the UUID of the import feed's IndexDef. cbgt's PlanPIndexName embeds this UUID, so a
// change here invalidates every PlanPIndexName for the whole index at once.
func readCbgtImportIndexUUID(t *testing.T, rt *RestTester, dbName string) string {
	t.Helper()
	dbc := rt.GetDatabase()
	indexName, err := base.GenerateCBGTIndexName(dbName, base.ShardedDCPFeedTypeImport)
	require.NoError(t, err)

	cfgKey := dbc.MetadataKeys.SGCfgPrefix(dbc.Options.GroupID) + cbgt.INDEX_DEFS_KEY
	var raw []byte
	_, err = dbc.MetadataStore.Get(base.TestCtx(t), cfgKey, &raw)
	require.NoError(t, err)

	var indexDefs cbgt.IndexDefs
	require.NoError(t, base.JSONUnmarshal(raw, &indexDefs))
	def, ok := indexDefs.IndexDefs[indexName]
	require.True(t, ok, "index %q not found in persisted IndexDefs", indexName)
	return def.UUID
}

// TestCbgtRebalanceOnNodeJoinPreservesUnmovedPIndexes verifies that when a second Sync Gateway node
// joins an import-sharded database, only the cbgt PIndexes that actually move to the new node have
// their local PIndex/DCP feed torn down and recreated. PIndexes that remain assigned to the
// original node - including whichever one owns vbucket 0 - must keep running the same underlying
// PIndex instance (same PIndex.UUID) rather than being spuriously restarted, and every PIndex must
// be accounted for on exactly one node at all times (never dropped, never duplicated).
func TestCbgtRebalanceOnNodeJoinPreservesUnmovedPIndexes(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("import partitions / sharded DCP require EE")
	}
	if base.UnitTestUrlIsWalrus() {
		t.Skip("sharded DCP feed is not supported by rosmar")
	}

	ctx := base.TestCtx(t)
	rtc := NewRestTesterCluster(t, &RestTesterClusterConfig{NumNodes: 1})
	defer rtc.Close(ctx)

	const dbName = "db"
	// Must evenly divide the bucket's vbuckets (1024), otherwise cbgt creates an extra pindex
	// for the remainder (see comment in TestResyncImportPartitionsPassthrough).
	const numPartitions = 4
	const vbucket0 = "0"

	dbConfig := dbConfigForTestBucket(rtc.testBucket)
	dbConfig.AutoImport = true
	dbConfig.ImportPartitions = base.Ptr(uint16(numPartitions))

	node0 := rtc.Node(0)

	resp := node0.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// Wait for all partitions to land on node0, since it's the only node running the database so far.
	base.RequireWaitForStat(t, node0.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, numPartitions)

	beforeSnapshot := node0.GetDatabase().ImportPartitionSnapshot(t)
	require.Len(t, beforeSnapshot, numPartitions)

	vb0PIndexName, found := findPIndexOwningVbucket(beforeSnapshot, vbucket0)
	require.True(t, found, "expected to find the PIndex owning vbucket 0 on node0 before rebalance")
	vb0UUIDBefore := beforeSnapshot[vb0PIndexName].UUID
	t.Logf("vbucket 0 is owned by PIndex %q (UUID %q) on node0 before rebalance", vb0PIndexName, vb0UUIDBefore)

	indexUUIDBefore := readCbgtImportIndexUUID(t, node0, dbName)
	t.Logf("cbgt IndexDef UUID before node1 joins: %s", indexUUIDBefore)

	// Second node joins: it discovers the persisted db config and starts its own import feed,
	// registering itself as a cbgt node - this should trigger a rebalance of the existing PIndexes.
	node1 := rtc.AddNode()

	indexUUIDAfter := readCbgtImportIndexUUID(t, node0, dbName)
	t.Logf("cbgt IndexDef UUID right after node1 joins: %s", indexUUIDAfter)
	if indexUUIDAfter != indexUUIDBefore {
		t.Logf("cbgt IndexDef UUID CHANGED when node1 joined (%s -> %s) - this invalidates every PlanPIndexName for the whole index at once, forcing a full teardown/rebuild of all PIndexes on all nodes, not just the ones that should move", indexUUIDBefore, indexUUIDAfter)
	}

	// Wait for the rebalance to converge on an even split of the partitions across both nodes.
	const halfPartitions = numPartitions / 2
	base.RequireWaitForStat(t, node0.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, halfPartitions, "expected half the partitions on node0 after rebalance")
	base.RequireWaitForStat(t, node1.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, halfPartitions, "expected half the partitions on node1 after rebalance")

	afterSnapshotNode0 := node0.GetDatabase().ImportPartitionSnapshot(t)
	afterSnapshotNode1 := node1.GetDatabase().ImportPartitionSnapshot(t)
	require.Len(t, afterSnapshotNode0, halfPartitions)
	require.Len(t, afterSnapshotNode1, halfPartitions)

	// Every partition (by name) must be running on exactly one node - never both, never neither.
	assertPartitionsFullyCoveredExactlyOnce(t, beforeSnapshot, afterSnapshotNode0, afterSnapshotNode1)

	// Whether vbucket 0 actually stays on node0 isn't deterministic: cbgt's rebalance planner
	// picks a rotation start node by hashing the index name against the node UUIDs
	// (BlancePlanPIndexes in manager_planner.go), and each node generates a fresh random UUID
	// per db open (cbgt.NewUUID() in database.go), so it varies run to run. So branch on the
	// outcome rather than asserting a fixed placement - either is legitimate, but a PIndex that
	// didn't move must keep its UUID (no spurious teardown/recreate), which is what we're
	// actually testing here.
	if vb0After, stillOnNode0 := afterSnapshotNode0[vb0PIndexName]; stillOnNode0 {
		t.Logf("vbucket 0's PIndex %q stayed on node0 after rebalance", vb0PIndexName)
		assert.Equal(t, vb0UUIDBefore, vb0After.UUID,
			"vbucket 0's PIndex stayed on node0 but was torn down/recreated (UUID changed) - its DCP stream closed and reopened unnecessarily")
	} else if vb0AfterOnNode1, movedToNode1 := afterSnapshotNode1[vb0PIndexName]; movedToNode1 {
		t.Logf("vbucket 0's PIndex %q moved to node1 (new UUID %q) - its stream legitimately closed on node0 and opened on node1", vb0PIndexName, vb0AfterOnNode1.UUID)
	} else {
		t.Fatalf("vbucket 0's PIndex %q is not running on either node after rebalance converged", vb0PIndexName)
	}

	// Broader check across every partition: any PIndex remaining on node0 must not have been
	// torn down and recreated (same assertion as above, generalized to all partitions).
	assertUnmovedPIndexesUnchanged(t, "node0", beforeSnapshot, afterSnapshotNode0)
}

// TestCbgtRebalanceOnThirdNodeJoinOnlyMovesMinimalShare verifies that growing a 2-node cluster to
// 3 nodes only reshuffles the minimal set of PIndexes needed to rebalance - PIndexes that stay on
// their existing node (whether node0 or node1) must not be torn down and recreated just because
// the cluster grew again. This is the same invariant as
// TestCbgtRebalanceOnNodeJoinPreservesUnmovedPIndexes, but exercised across a second rebalance
// with more nodes in play, which is where a bug that treats every join as "replan everything from
// scratch" would first show up.
func TestCbgtRebalanceOnThirdNodeJoinOnlyMovesMinimalShare(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("import partitions / sharded DCP require EE")
	}
	if base.UnitTestUrlIsWalrus() {
		t.Skip("sharded DCP feed is not supported by rosmar")
	}

	ctx := base.TestCtx(t)
	rtc := NewRestTesterCluster(t, &RestTesterClusterConfig{NumNodes: 1})
	defer rtc.Close(ctx)

	const dbName = "db"
	// Must evenly divide the bucket's vbuckets (1024), otherwise cbgt creates an extra pindex for
	// the remainder (see comment in TestResyncImportPartitionsPassthrough). 1024 has no factor of
	// 3, so an even 3-way split isn't possible - this test doesn't assert exact per-node counts
	// after the third node joins, only that coverage stays exact and nothing unmoved restarts.
	const numPartitions = 8

	dbConfig := dbConfigForTestBucket(rtc.testBucket)
	dbConfig.AutoImport = true
	dbConfig.ImportPartitions = base.Ptr(uint16(numPartitions))

	node0 := rtc.Node(0)
	resp := node0.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// Wait for all partitions to land on node0, since it's the only node running the database so far.
	base.RequireWaitForStat(t, node0.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, numPartitions)
	allPartitions := node0.GetDatabase().ImportPartitionSnapshot(t)
	require.Len(t, allPartitions, numPartitions)

	// Second node joins - with 2 nodes, an even split is possible and expected.
	node1 := rtc.AddNode()
	const halfPartitions = numPartitions / 2
	base.RequireWaitForStat(t, node0.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, halfPartitions, "expected half the partitions on node0 after node1 joins")
	base.RequireWaitForStat(t, node1.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, halfPartitions, "expected half the partitions on node1 after node1 joins")

	afterNode1JoinsNode0 := node0.GetDatabase().ImportPartitionSnapshot(t)
	afterNode1JoinsNode1 := node1.GetDatabase().ImportPartitionSnapshot(t)
	assertPartitionsFullyCoveredExactlyOnce(t, allPartitions, afterNode1JoinsNode0, afterNode1JoinsNode1)
	assertUnmovedPIndexesUnchanged(t, "node0", allPartitions, afterNode1JoinsNode0)

	// Third node joins. Since an even 3-way split isn't possible for this partition count, wait
	// for full convergence (every partition accounted for exactly once, node2 has picked up a
	// share) rather than a fixed per-node stat value.
	node2 := rtc.AddNode()

	var afterNode2JoinsNode0, afterNode2JoinsNode1, afterNode2JoinsNode2 map[string]db.ImportPartitionInfo
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		n0 := node0.GetDatabase().ImportPartitionSnapshot(t)
		n1 := node1.GetDatabase().ImportPartitionSnapshot(t)
		n2 := node2.GetDatabase().ImportPartitionSnapshot(t)

		converged := assert.Greater(c, len(n2), 0, "expected node2 to have picked up a share of the partitions")
		converged = assert.Equal(c, numPartitions, len(n0)+len(n1)+len(n2)) && converged
		for name := range allPartitions {
			owners := 0
			for _, snapshot := range []map[string]db.ImportPartitionInfo{n0, n1, n2} {
				if _, ok := snapshot[name]; ok {
					owners++
				}
			}
			converged = assert.Equal(c, 1, owners, "expected PIndex %s to be running on exactly one node", name) && converged
		}

		if converged {
			afterNode2JoinsNode0, afterNode2JoinsNode1, afterNode2JoinsNode2 = n0, n1, n2
		}
	}, 30*time.Second, 250*time.Millisecond)
	require.NotNil(t, afterNode2JoinsNode0, "rebalance after third node join never reached a fully-converged, consistent state")

	assertPartitionsFullyCoveredExactlyOnce(t, allPartitions, afterNode2JoinsNode0, afterNode2JoinsNode1, afterNode2JoinsNode2)
	// The key assertion: node0 and node1 only shed PIndexes to node2, they don't restart the ones
	// they keep - i.e. this wasn't treated as a from-scratch replan of the whole cluster.
	assertUnmovedPIndexesUnchanged(t, "node0", afterNode1JoinsNode0, afterNode2JoinsNode0)
	assertUnmovedPIndexesUnchanged(t, "node1", afterNode1JoinsNode1, afterNode2JoinsNode1)
}
