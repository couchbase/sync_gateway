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
	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			peers, _ := setupTests(t, tc.topology)

			var docVersionList []BodyAndVersion

			// grab sorted peer list and create a list to store expected version,
			// doc body
			for _, peerName := range tc.PeerNames() {
				docID := getDocID(t) + "_" + peerName
				docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, peerName, tc.description()))
				docVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, docBody)
				docVersionList = append(docVersionList, docVersion)
			}
			for i, peerName := range tc.PeerNames() {
				docID := getDocID(t) + "_" + peerName
				docBodyAndVersion := docVersionList[i]
				waitForVersionAndBody(t, tc, peers, docID, docBodyAndVersion)
			}
		})
	}
}

func TestMultiActorUpdate(t *testing.T) {
	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.Contains(tc.description(), "CBL") {
				t.Skip("Skipping Couchbase Lite test, returns unexpected body in proposeChanges: [304], CBG-4257")
			}
			peers, _ := setupTests(t, tc.topology)

			// grab sorted peer list and create a list to store expected version,
			// doc body and the peer the write came from
			var docVersionList []BodyAndVersion

			for _, peerName := range tc.PeerNames() {
				docID := getDocID(t) + "_" + peerName
				body1 := []byte(fmt.Sprintf(`{"originPeer": "%s", "topology": "%s", "write": 1}`, peerName, tc.description()))
				createVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, body1)
				waitForVersionAndBody(t, tc, peers, docID, createVersion)

				newBody := []byte(fmt.Sprintf(`{"originPeer": "%s", "topology": "%s", "write": 2}`, peerName, tc.description()))
				updateVersion := peers[peerName].WriteDocument(tc.collectionName(), docID, newBody)
				// store update version along with doc body and the current peer the update came in on
				docVersionList = append(docVersionList, updateVersion)
			}
			// loop through peers again and assert all peers have updates
			for i, peerName := range tc.PeerNames() {
				docID := getDocID(t) + "_" + peerName
				docBodyAndVersion := docVersionList[i]
				waitForVersionAndBodyOnNonActivePeers(t, tc, docID, peers, docBodyAndVersion)
			}

		})
	}
}

func TestMultiActorDelete(t *testing.T) {
	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.Contains(tc.description(), "CBL") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}
			peers, _ := setupTests(t, tc.topology)

			for peerName := range peers {
				docID := getDocID(t) + "_" + peerName
				body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, docID, tc.description()))
				createVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, body1)
				waitForVersionAndBody(t, tc, peers, docID, createVersion)

				for _, deletePeer := range tc.PeerNames() {
					if deletePeer == peerName {
						// continue till we find peer that write didn't originate from
						continue
					}
					deleteVersion := peers[deletePeer].DeleteDocument(tc.collectionName(), docID)
					t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
					t.Logf("waiting for document %s deletion on all peers", docID)
					waitForDeletion(t, tc, peers, docID, deletePeer)
					break
				}
			}
		})
	}
}

func TestMultiActorResurrect(t *testing.T) {
	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.Contains(tc.description(), "CBL") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}

			peers, _ := setupTests(t, tc.topology)

			var docVersionList []BodyAndVersion
			for _, peerName := range tc.PeerNames() {
				docID := getDocID(t) + "_" + peerName
				body1 := []byte(fmt.Sprintf(`{"topology": "%s","writePeer": "%s"}`, tc.description(), peerName))
				createVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, body1)
				t.Logf("createVersion: %+v for docID: %s", createVersion, docID)
				waitForVersionAndBody(t, tc, peers, docID, createVersion)

				deleteVersion := peers[peerName].DeleteDocument(tc.collectionName(), docID)
				t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
				t.Logf("waiting for document %s deletion on all peers", docID)
				waitForDeletion(t, tc, peers, docID, peerName)
				// recreate doc and assert it arrives at all peers
				resBody := []byte(fmt.Sprintf(`{"topology": "%s", "write": "resurrection on peer %s"}`, tc.description(), peerName))
				updateVersion := peers[peerName].WriteDocument(tc.collectionName(), docID, resBody)
				docVersionList = append(docVersionList, updateVersion)
			}

			for i, updatePeer := range tc.PeerNames() {
				docID := getDocID(t) + "_" + updatePeer
				docVersion := docVersionList[i]
				waitForVersionAndBodyOnNonActivePeers(t, tc, docID, peers, docVersion)
			}
		})
	}
}
