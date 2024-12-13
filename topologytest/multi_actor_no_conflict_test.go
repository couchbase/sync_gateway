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
)

func TestMultiActorCreate(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			collectionName, peers, _ := setupTests(t, topology)
			docVersionList := make(map[string]BodyAndVersion)
			for activePeerName, activePeer := range peers {
				docID := getDocID(t) + "_" + activePeerName
				docBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerName, topology.description))
				docVersion := activePeer.CreateDocument(collectionName, docID, docBody)
				docVersionList[activePeerName] = docVersion
			}
			for peerName := range peers {
				docID := getDocID(t) + "_" + peerName
				docBodyAndVersion := docVersionList[peerName]
				waitForVersionAndBody(t, collectionName, peers, docID, docBodyAndVersion)
			}
		})
	}
}

func TestMultiActorUpdate(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			if strings.Contains(topology.description, "CBL") {
				t.Skip("Skipping Couchbase Lite test, returns unexpected body in proposeChanges: [304], CBG-4257")
			}
			collectionName, peers, _ := setupTests(t, topology)

			docVersionList := make(map[string]BodyAndVersion)
			for activePeerName, activePeer := range peers {
				docID := getDocID(t) + "_" + activePeerName
				body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerName, topology.description))
				createVersion := activePeer.CreateDocument(collectionName, docID, body1)
				waitForVersionAndBody(t, collectionName, peers, docID, createVersion)

				newBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "update"}`, activePeerName, topology.description))
				updateVersion := activePeer.WriteDocument(collectionName, docID, newBody)
				// store update version along with doc body and the current peer the update came in on
				docVersionList[activePeerName] = updateVersion
			}
			// loop through peers again and assert all peers have updates
			for peerName := range peers.SortedPeers() {
				docID := getDocID(t) + "_" + peerName
				docBodyAndVersion := docVersionList[peerName]
				// FIXME: CBG-4417 this can be replaced with waitForVersionAndBody when implicit HLV exists
				waitForVersionAndBodyOnNonActivePeers(t, collectionName, docID, peers, docBodyAndVersion)
			}

		})
	}
}

func TestMultiActorDelete(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			if strings.Contains(topology.description, "CBL") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}
			collectionName, peers, _ := setupTests(t, topology)

			for peerName := range peers {
				docID := getDocID(t) + "_" + peerName
				body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, docID, topology.description))
				createVersion := peers[peerName].CreateDocument(collectionName, docID, body1)
				waitForVersionAndBody(t, collectionName, peers, docID, createVersion)

				for deletePeerName, deletePeer := range peers {
					if deletePeerName == peerName {
						// continue till we find peer that write didn't originate from
						continue
					}
					deleteVersion := deletePeer.DeleteDocument(collectionName, docID)
					t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
					t.Logf("waiting for document %s deletion on all peers", docID)
					waitForDeletion(t, collectionName, peers, docID, deletePeerName)
					break
				}
			}
		})
	}
}

func TestMultiActorResurrect(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			if strings.Contains(topology.description, "CBL") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}

			collectionName, peers, _ := setupTests(t, topology)

			docVersionList := make(map[string]BodyAndVersion)
			for activePeerName, activePeer := range peers {
				docID := getDocID(t) + "_" + activePeerName
				body1 := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, activePeerName, topology.description))
				createVersion := activePeer.CreateDocument(collectionName, docID, body1)
				t.Logf("createVersion: %+v for docID: %s", createVersion, docID)
				waitForVersionAndBody(t, collectionName, peers, docID, createVersion)

				deleteVersion := activePeer.DeleteDocument(collectionName, docID)
				t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
				t.Logf("waiting for document %s deletion on all peers", docID)
				waitForDeletion(t, collectionName, peers, docID, activePeerName)
				// recreate doc and assert it arrives at all peers
				resBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "resurrect"}`, activePeerName, topology.description))
				updateVersion := activePeer.WriteDocument(collectionName, docID, resBody)
				docVersionList[activePeerName] = updateVersion
			}

			for updatePeerName := range peers {
				docID := getDocID(t) + "_" + updatePeerName
				docVersion := docVersionList[updatePeerName]
				// FIXME: CBG-4417 this can be replaced with waitForVersionAndBody when implicit HLV exists
				waitForVersionAndBodyOnNonActivePeers(t, collectionName, docID, peers, docVersion)
			}
		})
	}
}
