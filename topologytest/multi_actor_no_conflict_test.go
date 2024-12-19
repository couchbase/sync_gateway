// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// TestMultiActorUpdate tests that a single actor can update a document that was created on a different peer.
// 1. start replications
// 2. create documents on each peer, to be updated by each other peer
// 3. wait for all documents to be replicated
// 4. update each document on a single peer, documents exist in pairwise create peer and update peer
// 5. wait for the hlv for updated documents to synchronized
func TestMultiActorUpdate(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, _ := setupTests(t, topology)

			for createPeerName, createPeer := range peers.ActivePeers() {
				for updatePeerName, updatePeer := range peers {
					docID := getDocID(t) + "_create=" + createPeerName + ",update=" + updatePeerName
					body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "createPeer": "%s", "updatePeer": "%s", "topology": "%s", "action": "create"}`, createPeerName, createPeerName, updatePeer, topology.description))
					createVersion := createPeer.CreateDocument(collectionName, docID, body1)
					waitForVersionAndBody(t, collectionName, peers, docID, createVersion)

					newBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "createPeer": "%s", "updatePeer": "%s", "topology": "%s", "action": "update"}`, updatePeerName, createPeerName, updatePeerName, topology.description))
					updateVersion := updatePeer.WriteDocument(collectionName, docID, newBody)

					waitForVersionAndBody(t, collectionName, peers, docID, updateVersion)
				}
			}
		})
	}
}

// TestMultiActorDelete tests that a single actor can update a document that was created on a different peer.
// 1. start replications
// 2. create documents on each peer, to be updated by each other peer
// 3. wait for all documents to be replicated
// 4. delete each document on a single peer, documents exist in pairwise create peer and update peer
// 5. wait for the hlv for updated documents to synchronized
func TestMultiActorDelete(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, _ := setupTests(t, topology)

			for createPeerName, createPeer := range peers.ActivePeers() {
				for deletePeerName, deletePeer := range peers {
					// CBG-4432: implement delete document in blip tester
					if deletePeer.Type() == PeerTypeCouchbaseLite {
						continue
					}

					docID := getDocID(t) + "_create=" + createPeerName + ",update=" + deletePeerName
					body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "createPeer": "%s", "deletePeer": "%s", "topology": "%s", "action": "create"}`, createPeerName, createPeerName, deletePeer, topology.description))
					createVersion := createPeer.CreateDocument(collectionName, docID, body1)
					waitForVersionAndBody(t, collectionName, peers, docID, createVersion)

					deleteVersion := deletePeer.DeleteDocument(collectionName, docID)
					t.Logf("deleteVersion: %+v\n", deleteVersion) // FIXME: verify hlv in CBG-4416
					waitForDeletion(t, collectionName, peers, docID, deletePeerName)
				}
			}
		})
	}
}

// TestMultiActorResurrect tests that a single actor can update a document that was created on a different peer.
// 1. start replications
// 2. create documents on each peer, to be updated by each other peer
// 3. wait for all documents to be replicated
// 4. delete each document on a single peer, documents exist in pairwise create peer and update peer
// 5. wait for the hlv for updated documents to synchronized
// 6. resurrect each document on a single peer
// 7. wait for the hlv for updated documents to be synchronized
func TestMultiActorResurrect(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBG-4419: this test fails xdcr with: could not write doc: cas mismatch: expected 0, really 1 -- xdcr.(*rosmarManager).processEvent() at rosmar_xdcr.go:201")
	}
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, _ := setupTests(t, topology)

			for createPeerName, createPeer := range peers.ActivePeers() {
				for deletePeerName, deletePeer := range peers {
					// CBG-4432: implement delete document in blip tester
					if deletePeer.Type() == PeerTypeCouchbaseLite {
						continue
					}
					for resurrectPeerName, resurrectPeer := range peers {
						docID := getDocID(t) + "_create=" + createPeerName + ",delete=" + deletePeerName + ",resurrect=" + resurrectPeerName
						body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "createPeer": "%s", "deletePeer": "%s", "resurrectPeer": "%s", "topology": "%s", "action": "create"}`, createPeerName, createPeerName, deletePeer, resurrectPeer, topology.description))
						createVersion := createPeer.CreateDocument(collectionName, docID, body1)
						waitForVersionAndBody(t, collectionName, peers, docID, createVersion)

						deleteVersion := deletePeer.DeleteDocument(collectionName, docID)
						t.Logf("deleteVersion: %+v\n", deleteVersion) // FIXME: verify hlv in CBG-4416
						waitForDeletion(t, collectionName, peers, docID, deletePeerName)

						resBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "createPeer": "%s", "deletePeer": "%s", "resurrectPeer": "%s", "topology": "%s", "action": "resurrect"}`, resurrectPeerName, createPeerName, deletePeer, resurrectPeer, topology.description))
						resurrectVersion := resurrectPeer.WriteDocument(collectionName, docID, resBody)
						waitForVersionAndBody(t, collectionName, peers, docID, resurrectVersion)
					}
				}
			}
		})
	}
}
