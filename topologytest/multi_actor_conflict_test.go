// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// clockTieSubtestName names the forceClockTie dimension run by each TestMultiActorConflict* test below.
func clockTieSubtestName(forceClockTie bool) string {
	if forceClockTie {
		return "ClockTie"
	}
	return "default"
}

// TestMultiActorConflictCreate
//  1. create document on each peer with different contents
//  2. start replications
//  3. wait for documents to exist with a matching CV for Couchbase Lite peers, and a full HLV match for non Couchbase
//     Lite peers. The body should match.
//
// The forceClockTie dimension forces every controllable peer's HLC clock to the same fixed timestamp
// immediately before step 1, so the creates in step 1 tie on HLV Value (see forceHLCClockTieForTest) instead of
// relying on real clock jitter to hit that case.
func TestMultiActorConflictCreate(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			for _, forceClockTie := range []bool{false, true} {
				t.Run(clockTieSubtestName(forceClockTie), func(t *testing.T) {
					if forceClockTie && controllableClockPeerCount(topologySpec) < 2 {
						t.Skipf("only %d controllable (Sync Gateway/Couchbase Lite) peer(s) in this topology, need at least 2 to force an HLC clock tie", controllableClockPeerCount(topologySpec))
					}

					collectionName, topology := setupTests(t, topologySpec)
					if forceClockTie {
						forceHLCClockTieForTest(t, topology)
					}

					docID := getDocID(t)
					docVersion := createConflictingDocs(collectionName, docID, topology)
					topology.StartReplications()
					waitForCVAndBody(t, collectionName, docID, docVersion, topology)
				})
			}
		})
	}
}

// TestMultiActorConflictUpdate
//  1. create document on each peer with different contents
//  2. start replications
//  3. wait for documents to exist with a matching CV for Couchbase Lite peers, and a full HLV match for non Couchbase
//     Lite peers. The body should match.
//  4. stop replications
//  5. update documents on all peers, with unique body contents.
//  6. start replications
//  7. wait for documents to exist with a matching CV for Couchbase Lite peers, and a full HLV match for non Couchbase
//     Lite peers. The body should match.
//
// The forceClockTie dimension forces every controllable peer's HLC clock to the same fixed timestamp
// immediately before step 5, so the updates in step 5 tie on HLV Value instead of relying on real clock jitter
// to hit that case.
func TestMultiActorConflictUpdate(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			for _, forceClockTie := range []bool{false, true} {
				t.Run(clockTieSubtestName(forceClockTie), func(t *testing.T) {
					if forceClockTie && controllableClockPeerCount(topologySpec) < 2 {
						t.Skipf("only %d controllable (Sync Gateway/Couchbase Lite) peer(s) in this topology, need at least 2 to force an HLC clock tie", controllableClockPeerCount(topologySpec))
					}

					collectionName, topology := setupTests(t, topologySpec)

					docID := getDocID(t)
					docVersion := createConflictingDocs(collectionName, docID, topology)

					topology.StartReplications()
					waitForCVAndBody(t, collectionName, docID, docVersion, topology)

					topology.StopReplications()

					if forceClockTie {
						forceHLCClockTieForTest(t, topology)
					}
					docVersion = updateConflictingDocs(collectionName, docID, topology)
					topology.StartReplications()
					waitForCVAndBody(t, collectionName, docID, docVersion, topology)
				})
			}
		})
	}
}

// TestMultiActorConflictDelete
// 1. create document on each peer with different contents
// 2. start replications
// 3. wait for documents to exist with hlv sources equal to the number of active peers
// 4. stop replications
// 5. delete documents on all peers
// 6. start replications
// 7. assert that the documents are deleted on all peers and have hlv sources equal to the number of active peers
//
// The forceClockTie dimension forces every controllable peer's HLC clock to the same fixed timestamp
// immediately before step 5, so the concurrent deletes in step 5 tie on HLV Value instead of relying on real
// clock jitter to hit that case.
func TestMultiActorConflictDelete(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			for _, forceClockTie := range []bool{false, true} {
				t.Run(clockTieSubtestName(forceClockTie), func(t *testing.T) {
					if forceClockTie && controllableClockPeerCount(topologySpec) < 2 {
						t.Skipf("only %d controllable (Sync Gateway/Couchbase Lite) peer(s) in this topology, need at least 2 to force an HLC clock tie", controllableClockPeerCount(topologySpec))
					}

					collectionName, topology := setupTests(t, topologySpec)
					docID := getDocID(t)
					docVersion := createConflictingDocs(collectionName, docID, topology)

					topology.StartReplications()
					waitForCVAndBody(t, collectionName, docID, docVersion, topology)

					topology.StopReplications()
					if forceClockTie {
						forceHLCClockTieForTest(t, topology)
					}
					deleteConflictDocs(collectionName, docID, topology)

					topology.StartReplications()
					waitForConvergingTombstones(t, collectionName, docID, topology)
				})
			}
		})
	}
}

// TestMultiActorConflictResurrect
//  1. create document on each peer with different contents
//  2. start replications
//  3. wait for documents to exist with hlv sources equal to the number of active peers and the document body is
//     equivalent to the last write.
//  4. stop replications
//  5. delete documents on all peers
//  6. start replications
//  7. assert that the documents are deleted on all peers and that there is a converging tombstone. In this case,
//     there is no assertion other than deletion for Couchbase Lite peers, but there is a full HLV assertion for other
//     peer types.
//  8. stop replications
//  9. resurrect documents on all peers with unique contents
//  10. start replications
//  11. assert that the documents are resurrected on all peers and have matching hlvs for non Couchbase Lite peers and
//     matching CV for Couchbase Lite peers.
//
// The forceClockTie dimension forces every controllable peer's HLC clock to the same fixed timestamp
// immediately before step 9, so the concurrent resurrections in step 9 tie on HLV Value instead of relying on
// real clock jitter to hit that case.
func TestMultiActorConflictResurrect(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			for _, forceClockTie := range []bool{false, true} {
				t.Run(clockTieSubtestName(forceClockTie), func(t *testing.T) {
					if forceClockTie && controllableClockPeerCount(topologySpec) < 2 {
						t.Skipf("only %d controllable (Sync Gateway/Couchbase Lite) peer(s) in this topology, need at least 2 to force an HLC clock tie", controllableClockPeerCount(topologySpec))
					}

					collectionName, topology := setupTests(t, topologySpec)

					docID := getDocID(t)
					docVersion := createConflictingDocs(collectionName, docID, topology)

					topology.StartReplications()
					waitForCVAndBody(t, collectionName, docID, docVersion, topology)

					topology.StopReplications()
					deleteConflictDocs(collectionName, docID, topology)

					topology.StartReplications()

					waitForConvergingTombstones(t, collectionName, docID, topology)
					topology.StopReplications()

					if forceClockTie {
						forceHLCClockTieForTest(t, topology)
					}
					resurrectVersion := updateConflictingDocs(collectionName, docID, topology)
					topology.StartReplications()

					waitForCVAndBody(t, collectionName, docID, resurrectVersion, topology)
				})
			}
		})
	}
}
