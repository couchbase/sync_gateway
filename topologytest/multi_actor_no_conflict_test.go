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
	"strings"
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
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for createPeerName, createPeer := range topology.ActivePeers() {
				for updatePeerName, updatePeer := range topology.ActivePeers() {
					topology.Run(t, "create="+createPeerName+",update="+updatePeerName, func(t *testing.T) {
						docID := getDocID(t) + "_create=" + createPeerName + ",update=" + updatePeerName
						body1 := fmt.Appendf(nil, `{"activePeer": "%s", "createPeer": "%s", "updatePeer": "%s", "topology": "%s", "action": "create"}`, createPeerName, createPeerName, updatePeer, topology.specDescription)
						createVersion := createPeer.CreateDocument(collectionName, docID, body1)
						waitForVersionAndBody(t, collectionName, docID, createVersion, topology)

						newBody := fmt.Appendf(nil, `{"activePeer": "%s", "createPeer": "%s", "updatePeer": "%s", "topology": "%s", "action": "update"}`, updatePeerName, createPeerName, updatePeerName, topology.specDescription)
						updateVersion := updatePeer.WriteDocument(collectionName, docID, newBody)

						waitForVersionAndBody(t, collectionName, docID, updateVersion, topology)
					})
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
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for createPeerName, createPeer := range topology.ActivePeers() {
				for deletePeerName, deletePeer := range topology.ActivePeers() {
					topology.Run(t, "create="+createPeerName+",delete="+deletePeerName, func(t *testing.T) {
						docID := getDocID(t) + "_create=" + createPeerName + ",update=" + deletePeerName
						body1 := fmt.Appendf(nil, `{"activePeer": "%s", "createPeer": "%s", "deletePeer": "%s", "topology": "%s", "action": "create"}`, createPeerName, createPeerName, deletePeer, topology.specDescription)
						createVersion := createPeer.CreateDocument(collectionName, docID, body1)
						waitForVersionAndBody(t, collectionName, docID, createVersion, topology)

						deleteVersion := deletePeer.DeleteDocument(collectionName, docID)
						waitForTombstoneVersion(t, collectionName, docID, BodyAndVersion{docMeta: deleteVersion, updatePeer: deletePeerName}, topology)
					})
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
	base.LongRunningTest(t)

	for _, topologySpec := range append(simpleTopologySpecifications, TopologySpecifications...) {
		t.Run(topologySpec.description, func(t *testing.T) {
			collectionName, topology := setupTests(t, topologySpec)
			topology.StartReplications()
			for createPeerName, createPeer := range topology.ActivePeers() {
				for deletePeerName, deletePeer := range topology.ActivePeers() {
					for resurrectPeerName, resurrectPeer := range topology.ActivePeers() {
						topology.Run(t, fmt.Sprintf("create=%s,delete=%s,resurrect=%s", createPeerName, deletePeerName, resurrectPeerName), func(t *testing.T) {
							docID := getDocID(t) + "_create=" + createPeerName + ",delete=" + deletePeerName + ",resurrect=" + resurrectPeerName
							body1 := fmt.Appendf(nil, `{"activePeer": "%s", "createPeer": "%s", "deletePeer": "%s", "resurrectPeer": "%s", "topology": "%s", "action": "create"}`, createPeerName, createPeerName, deletePeer, resurrectPeer, topologySpec.description)
							createVersion := createPeer.CreateDocument(collectionName, docID, body1)
							waitForVersionAndBody(t, collectionName, docID, createVersion, topology)

							deleteVersion := deletePeer.DeleteDocument(collectionName, docID)
							waitForTombstoneVersion(t, collectionName, docID, BodyAndVersion{docMeta: deleteVersion, updatePeer: deletePeerName}, topology)

							resBody := fmt.Appendf(nil, `{"activePeer": "%s", "createPeer": "%s", "deletePeer": "%s", "resurrectPeer": "%s", "topology": "%s", "action": "resurrect"}`, resurrectPeerName, createPeerName, deletePeer, resurrectPeer, topology.specDescription)
							resurrectVersion := resurrectPeer.WriteDocument(collectionName, docID, resBody)
							// in the case of a Couchbase Server resurrection, the hlv is lost since all system xattrs are lost on a resurrection
							// if cbs resurrect, if delete AND resurrecting peer is server side peer (cbs or sgw) the all docs will converge for version expected
							// if cbs resurrect and delete AND resurrecting peer is NOT server side peer (lite), then need to wait for tombstone convergence first
							if resurrectPeer.Type() == PeerTypeCouchbaseServer {
								if strings.Contains(topologySpec.description, "CBL") {
									if conflictNotExpectedOnCBL(deletePeer, resurrectPeer, deletePeerName, resurrectPeerName) {
										// if no cbl conflict is expected we can wait on CV and body
										waitForCVAndBody(t, collectionName, docID, resurrectVersion, topology)
									} else {
										// if cbl conflict is expected we need to wait for tombstone convergence
										waitForConvergingTombstones(t, collectionName, docID, topology)
									}
								} else {
									waitForCVAndBody(t, collectionName, docID, resurrectVersion, topology)
								}
							} else {
								waitForVersionAndBody(t, collectionName, docID, resurrectVersion, topology)
							}
						})
					}
				}
			}
		})
	}
}
