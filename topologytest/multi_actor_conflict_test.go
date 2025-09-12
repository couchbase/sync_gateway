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
)

// TestMultiActorConflictCreate
//  1. create document on each peer with different contents
//  2. start replications
//  3. wait for documents to exist with a matching CV for Couchbase Lite peers, and a full HLV match for non Couchbase
//     Lite peers. The body should match.
func TestMultiActorConflictCreate(t *testing.T) {
	skipMobileJenkinsCBG4826(t)
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			docID := getDocID(t)
			docVersion := createConflictingDocs(collectionName, docID, topology)
			topology.StartReplications()
			waitForCVAndBody(t, collectionName, docID, docVersion, topology)
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
func TestMultiActorConflictUpdate(t *testing.T) {
	skipMobileJenkinsCBG4826(t)
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)

			docID := getDocID(t)
			docVersion := createConflictingDocs(collectionName, docID, topology)

			topology.StartReplications()
			waitForCVAndBody(t, collectionName, docID, docVersion, topology)

			topology.StopReplications()

			docVersion = updateConflictingDocs(collectionName, docID, topology)
			topology.StartReplications()
			waitForCVAndBody(t, collectionName, docID, docVersion, topology)
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
func TestMultiActorConflictDelete(t *testing.T) {
	skipMobileJenkinsCBG4826(t)
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			docID := getDocID(t)
			docVersion := createConflictingDocs(collectionName, docID, topology)

			topology.StartReplications()
			waitForCVAndBody(t, collectionName, docID, docVersion, topology)

			topology.StopReplications()
			deleteConflictDocs(collectionName, docID, topology)

			topology.StartReplications()
			waitForConvergingTombstones(t, collectionName, docID, topology)
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
func TestMultiActorConflictResurrect(t *testing.T) {
	skipMobileJenkinsCBG4826(t)
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
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

			resurrectVersion := updateConflictingDocs(collectionName, docID, topology)
			topology.StartReplications()

			waitForCVAndBody(t, collectionName, docID, resurrectVersion, topology)
		})
	}
}
