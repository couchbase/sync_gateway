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

	"github.com/couchbase/sync_gateway/base"
)

// TestMultiActorConflictCreate
// 1. create document on each peer with different contents
// 2. start replications
// 3. wait for documents to exist with hlv sources equal to the number of active peers
func TestMultiActorConflictCreate(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)
			replications.Stop()

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)
			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, docID, docVersion)

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
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, topology := range append(simpleTopologies, Topologies...) {
		if strings.Contains(topology.description, "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)
			replications.Stop()

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)

			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, docID, docVersion)

			replications.Stop()

			docVersion = updateConflictingDocs(t, collectionName, peers, docID, topology.description)
			replications.Start()
			// FIXME: CBG-4417 this can be replaced with waitForVersionAndBody when implicit HLV exists
			waitForVersionAndBodyOnNonActivePeers(t, collectionName, docID, peers, docVersion)
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
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, topology := range append(simpleTopologies, Topologies...) {
		if strings.Contains(topology.description, "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)
			replications.Stop()

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)

			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, docID, docVersion)

			replications.Stop()
			lastWrite := deleteConflictDocs(t, collectionName, peers, docID)

			replications.Start()
			waitForDeletion(t, collectionName, peers, docID, lastWrite.updatePeer)
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
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, topology := range append(simpleTopologies, Topologies...) {
		if strings.Contains(topology.description, "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)
			replications.Stop()

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)

			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, docID, docVersion)

			replications.Stop()
			lastWrite := deleteConflictDocs(t, collectionName, peers, docID)

			replications.Start()

			waitForDeletion(t, collectionName, peers, docID, lastWrite.updatePeer)
			replications.Stop()

			lastWriteVersion := updateConflictingDocs(t, collectionName, peers, docID, topology.description)
			replications.Start()

			waitForVersionAndBodyOnNonActivePeers(t, collectionName, docID, peers, lastWriteVersion)
		})
	}
}