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

// TestSingleActorCreate tests creating a document with a single actor in different topologies.
// 1. start replications
// 2. create document on a single active peer (version1)
// 3. wait for convergence on other peers
func TestSingleActorCreate(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for activePeerID, activePeer := range topology.ActivePeers() {
				topology.Run(t, fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					docID := getDocID(t)
					docBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerID, topologySpec.description))
					docVersion := activePeer.CreateDocument(collectionName, docID, docBody)
					waitForVersionAndBody(t, collectionName, docID, docVersion, topology)
				})
			}
		})
	}
}

// TestSingleActorUpdate tests updating a document on a single actor and ensuring the matching hlv exists on all peers.
// 1. start replications
// 2. create document on a single active peer (version1)
// 3. wait for convergence on other peers
// 4. update document on a single active peer (version2)
// 5. wait for convergence on other peers
func TestSingleActorUpdate(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for activePeerID, activePeer := range topology.ActivePeers() {
				topology.Run(t, fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					docID := getDocID(t)
					body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerID, topology.specDescription))
					createVersion := activePeer.CreateDocument(collectionName, docID, body1)

					waitForVersionAndBody(t, collectionName, docID, createVersion, topology)

					body2 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "update"}`, activePeerID, topology.specDescription))
					updateVersion := activePeer.WriteDocument(collectionName, docID, body2)

					waitForVersionAndBody(t, collectionName, docID, updateVersion, topology)
				})
			}
		})
	}
}

// TestSingleActorDelete tests deletion of a documents on an active peer and makes sure the deletion and hlv matches on all peers.
// 1. start replications
// 2. create document on a single active peer (version1)
// 3. wait for convergence on other peers
// 4. delete document on a single active peer (version2)
// 5. wait for convergence on other peers for a deleted document with correct hlv
func TestSingleActorDelete(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for activePeerID, activePeer := range topology.ActivePeers() {
				topology.Run(t, fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {

					docID := getDocID(t)
					body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerID, topology.specDescription))
					createVersion := activePeer.CreateDocument(collectionName, docID, body1)

					waitForVersionAndBody(t, collectionName, docID, createVersion, topology)

					deleteVersion := activePeer.DeleteDocument(collectionName, docID)
					waitForTombstoneVersion(t, collectionName, docID, BodyAndVersion{docMeta: deleteVersion, updatePeer: activePeerID}, topology)
				})
			}
		})
	}
}

// TestSingleActorResurrect tests resurrect a document with a single actor in different topologies.
// 1. start replications
// 2. create document on a single active peer (version1)
// 3. wait for convergence on other peers
// 4. delete document on a single active peer (version2)
// 5. wait for convergence on other peers for a deleted document with correct hlv
// 6. resurrect document on a single active peer (version3)
// 7. wait for convergence on other peers for a resurrected document with correct hlv
func TestSingleActorResurrect(t *testing.T) {
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for activePeerID, activePeer := range topology.ActivePeers() {
				topology.Run(t, fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					docID := getDocID(t)
					body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerID, topology.specDescription))
					createVersion := activePeer.CreateDocument(collectionName, docID, body1)
					waitForVersionAndBody(t, collectionName, docID, createVersion, topology)

					deleteVersion := activePeer.DeleteDocument(collectionName, docID)
					waitForTombstoneVersion(t, collectionName, docID, BodyAndVersion{docMeta: deleteVersion, updatePeer: activePeerID}, topology)

					body2 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "resurrect"}`, activePeerID, topology.specDescription))
					resurrectVersion := activePeer.WriteDocument(collectionName, docID, body2)
					waitForVersionAndBody(t, collectionName, docID, resurrectVersion, topology)
				})
			}
		})
	}
}
