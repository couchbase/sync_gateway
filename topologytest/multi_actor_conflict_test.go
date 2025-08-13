// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"strings"
	"testing"
)

// TestMultiActorConflictCreate
// 1. create document on each peer with different contents
// 2. start replications
// 3. wait for documents to exist with hlv sources equal to the number of active peers
func TestMultiActorConflictCreate(t *testing.T) {
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, docID, topology)
			topology.StartReplications()
			// Can not assert on full HLV here. CV should converge, but CBL actors can have PV that does not match that of the other peers.
			//        + - - - - - - +      +- - - - - - -+
			//        '  cluster A  '      '  cluster B  '
			//        ' +---------+ '      ' +---------+ '
			//        ' |  cbs1   | ' <--> ' |  cbs2   | '
			//        ' +---------+ '      ' +---------+ '
			//        ' +---------+ '      ' +---------+ '
			//        ' |   sg1   | '      ' |   sg2   | '
			//        ' +---------+ '      ' +---------+ '
			//        + - - - - - - +      +- - - - - - -+
			//              ^                     ^
			//              |                     |
			//              |                     |
			//              v                     v
			//          +---------+          +---------+
			//          |   cbl1  |          |   cbl2  |
			//          +---------+          +---------+
			// Couchbase Server, since conflict resolution in XDCR will overwrite the HLV.
			// 1. sg1 creates unique document cv: 1@rosmar1
			// 2. sg2 creates unique document cv: 2@rosmar2
			// 3. cbl1 pulls 1@rosmar1
			// 4. cbl2 pull 2@rosmar2
			// 5. cbs1 pulls 2@rosmar2, overwriting cv:1@rosmar1
			// 6. cbl1 pulls 2@rosmar2, creating cv: 2@rosmar2, pv:1@rosmar1 overwriting
			// Final state:
			//    - cv:2@rosmar2 on cbs1, cbs2, cbl2
			//    - cv:2@rosmar2, pv:1@rosmar1 on cbl1
			waitForCVAndBody(t, collectionName, docID, docVersion, topology)
		})
	}
}

// TestMultiActorConflictUpdate
// 1. create document on each peer with different contents
// 2. start replications
// 3. wait for documents to exist with hlv sources equal to the number of active peers
// 4. stop replications
// 5. update documents on all peers
// 6. start replications
// 7. assert that the documents are deleted on all peers and have hlv sources equal to the number of active peers
func TestMultiActorConflictUpdate(t *testing.T) {
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		if strings.Contains(topologySpec.description, "CBL") {
			t.Skip("CBL actor can generate conflicts and push replication fails with conflict for doc in blip tester CBL-4267")
		}
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, docID, topology)

			topology.StartReplications()
			waitForVersionAndBody(t, collectionName, docID, docVersion, topology)

			topology.StopReplications()

			docVersion = updateConflictingDocs(t, collectionName, docID, topology)
			topology.StartReplications()
			waitForVersionAndBody(t, collectionName, docID, docVersion, topology)
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
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		if strings.Contains(topologySpec.description, "CBL") {
			t.Skip("CBL actor can generate conflicts and push replication fails with conflict for doc in blip tester CBL-4267")
		}
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, docID, topology)

			topology.StartReplications()
			waitForVersionAndBody(t, collectionName, docID, docVersion, topology)

			topology.StopReplications()
			lastWrite := deleteConflictDocs(t, collectionName, docID, topology)

			topology.StartReplications()
			waitForTombstoneVersion(t, collectionName, docID, lastWrite, topology)
		})
	}
}

// TestMultiActorConflictResurrect
// 1. create document on each peer with different contents
// 2. start replications
// 3. wait for documents to exist with hlv sources equal to the number of active peers and the document body is equivalent to the last write
// 4. stop replications
// 5. delete documents on all peers
// 6. start replications
// 7. assert that the documents are deleted on all peers and have hlv sources equal to the number of active peers
// 8. stop replications
// 9. resurrect documents on all peers with unique contents
// 10. start replications
// 11. assert that the documents are resurrected on all peers and have hlv sources equal to the number of active peers and the document body is equivalent to the last write
func TestMultiActorConflictResurrect(t *testing.T) {
	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		if strings.Contains(topologySpec.description, "CBL") {
			t.Skip("CBL actor can generate conflicts and push replication fails with conflict for doc in blip tester CBL-4267")
		}
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, docID, topology)

			topology.StartReplications()
			waitForVersionAndBody(t, collectionName, docID, docVersion, topology)

			topology.StopReplications()
			lastWrite := deleteConflictDocs(t, collectionName, docID, topology)

			topology.StartReplications()

			waitForTombstoneVersion(t, collectionName, docID, lastWrite, topology)
			topology.StopReplications()

			lastWriteVersion := updateConflictingDocs(t, collectionName, docID, topology)
			topology.StartReplications()

			waitForVersionAndBody(t, collectionName, docID, lastWriteVersion, topology)
		})
	}
}
