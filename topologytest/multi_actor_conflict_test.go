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
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)
			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)
			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, replications, docID, docVersion)

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
	for _, topology := range append(simpleTopologies, Topologies...) {
		if strings.Contains(topology.description, "CBL") {
			t.Skip("CBL actor can generate conflicts and push replication fails with conflict for doc in blip tester CBL-4267")
		}
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)

			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, replications, docID, docVersion)

			replications.Stop()

			docVersion = updateConflictingDocs(t, collectionName, peers, docID, topology.description)
			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, replications, docID, docVersion)
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
	for _, topology := range append(simpleTopologies, Topologies...) {
		if strings.Contains(topology.description, "CBL") {
			t.Skip("CBL actor can generate conflicts and push replication fails with conflict for doc in blip tester CBL-4267")
		}
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)
			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)

			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, replications, docID, docVersion)

			replications.Stop()
			lastWrite := deleteConflictDocs(t, collectionName, peers, docID)

			replications.Start()
			waitForTombstoneVersion(t, collectionName, peers, replications, docID, lastWrite)
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
	for _, topology := range append(simpleTopologies, Topologies...) {
		if strings.Contains(topology.description, "CBL") {
			t.Skip("CBL actor can generate conflicts and push replication fails with conflict for doc in blip tester CBL-4267")
		}
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, replications := setupTests(t, topology)

			docID := getDocID(t)
			docVersion := createConflictingDocs(t, collectionName, peers, docID, topology.description)

			replications.Start()
			waitForVersionAndBody(t, collectionName, peers, replications, docID, docVersion)

			replications.Stop()
			lastWrite := deleteConflictDocs(t, collectionName, peers, docID)

			replications.Start()

			waitForTombstoneVersion(t, collectionName, peers, replications, docID, lastWrite)
			replications.Stop()

			lastWriteVersion := updateConflictingDocs(t, collectionName, peers, docID, topology.description)
			replications.Start()

			waitForVersionAndBody(t, collectionName, peers, replications, docID, lastWriteVersion)
		})
	}
}
