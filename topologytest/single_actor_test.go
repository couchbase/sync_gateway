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

// TestSingleActorCreate tests creating a document with a single actor in different topologies.
// 1. start replications
// 2. create document on a single active peer (version1)
// 3. wait for convergence on other peers
func TestSingleActorCreate(t *testing.T) {
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			peers, _ := setupTests(t, topology)
			for _, activePeerID := range topology.PeerNames() {
				t.Run(fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					updatePeersT(t, peers)
					tc := singleActorTest{topology: topology, activePeerID: activePeerID}
					docID := getDocID(t)
					docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, activePeerID, tc.description()))
					docVersion := peers[activePeerID].CreateDocument(getSingleDsName(), docID, docBody)
					waitForVersionAndBody(t, tc, peers, docID, docVersion)
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
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			peers, _ := setupTests(t, topology)
			for _, activePeerID := range topology.PeerNames() {
				t.Run(fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					updatePeersT(t, peers)
					tc := singleActorTest{topology: topology, activePeerID: activePeerID}
					if strings.HasPrefix(tc.activePeerID, "cbl") {
						t.Skip("Skipping Couchbase Lite test, returns unexpected body in proposeChanges: [304], CBG-4257")
					}

					docID := getDocID(t)
					body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
					createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), docID, body1)

					waitForVersionAndBody(t, tc, peers, docID, createVersion)

					body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 2}`, tc.activePeerID, tc.description()))
					updateVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), docID, body2)
					t.Logf("createVersion: %+v, updateVersion: %+v", createVersion.docMeta, updateVersion.docMeta)
					t.Logf("waiting for document version 2 on all peers")

					waitForVersionAndBody(t, tc, peers, docID, updateVersion)
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
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			peers, _ := setupTests(t, topology)
			for _, activePeerID := range topology.PeerNames() {
				t.Run(fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					updatePeersT(t, peers)
					tc := singleActorTest{topology: topology, activePeerID: activePeerID}

					if strings.HasPrefix(tc.activePeerID, "cbl") {
						t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
					}

					docID := getDocID(t)
					body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
					createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), docID, body1)

					waitForVersionAndBody(t, tc, peers, docID, createVersion)

					deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), docID)
					t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion.docMeta, deleteVersion)
					t.Logf("waiting for document deletion on all peers")
					waitForDeletion(t, tc, peers, docID, tc.activePeerID)
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
	for _, topology := range append(simpleTopologies, Topologies...) {
		t.Run(topology.description, func(t *testing.T) {
			peers, _ := setupTests(t, topology)
			for _, activePeerID := range topology.PeerNames() {
				t.Run(fmt.Sprintf("actor=%s", activePeerID), func(t *testing.T) {
					updatePeersT(t, peers)
					tc := singleActorTest{topology: topology, activePeerID: activePeerID}

					if strings.HasPrefix(tc.activePeerID, "cbl") {
						t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
					}

					docID := getDocID(t)
					body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
					createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), docID, body1)
					waitForVersionAndBody(t, tc, peers, docID, createVersion)

					deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), docID)
					t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
					t.Logf("waiting for document deletion on all peers")
					waitForDeletion(t, tc, peers, docID, tc.activePeerID)

					body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": "resurrection"}`, tc.activePeerID, tc.description()))
					resurrectVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), docID, body2)
					t.Logf("createVersion: %+v, deleteVersion: %+v, resurrectVersion: %+v", createVersion.docMeta, deleteVersion, resurrectVersion.docMeta)
					t.Logf("waiting for document resurrection on all peers")

					// Couchbase Lite peers do not know how to push a deletion yet, so we need to filter them out CBG-4257
					nonCBLPeers := make(map[string]Peer)
					for peerName, peer := range peers {
						if !strings.HasPrefix(peerName, "cbl") {
							nonCBLPeers[peerName] = peer
						}
					}
					waitForVersionAndBody(t, tc, peers, docID, resurrectVersion)
				})
			}
		})
	}
}
