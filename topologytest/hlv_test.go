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
	"github.com/couchbase/sync_gateway/db"
	"golang.org/x/exp/maps"

	"github.com/stretchr/testify/require"
)

func getSingleDsName() base.ScopeAndCollectionName {
	if base.TestsUseNamedCollections() {
		return base.ScopeAndCollectionName{Scope: "sg_test_0", Collection: "sg_test_0"}
	}
	return base.DefaultScopeAndCollectionName()
}

// singleActorTest represents a test case for a single actor in a given topology.
type singleActorTest struct {
	topology     Topology
	activePeerID string
}

// description returns a human-readable description of the test case.
func (t singleActorTest) description() string {
	return fmt.Sprintf("%s_actor=%s", t.topology.description, t.activePeerID)
}

// docID returns a unique document ID for the test case.
func (t singleActorTest) docID() string {
	return fmt.Sprintf("doc_%s", strings.ReplaceAll(t.description(), " ", "_"))
}

// PeerNames returns the names of all peers in the test case's topology, sorted deterministically.
func (t singleActorTest) PeerNames() []string {
	return t.topology.PeerNames()
}

// collectionName returns the collection name for the test case.
func (t singleActorTest) collectionName() base.ScopeAndCollectionName {
	return getSingleDsName()
}

// getSingleActorTestCase returns a list of test cases in the matrix for all topologies * active peers.
func getSingleActorTestCase() []singleActorTest {
	var tests []singleActorTest
	for _, tc := range append(simpleTopologies, Topologies...) {
		for _, activePeerID := range tc.PeerNames() {
			tests = append(tests, singleActorTest{topology: tc, activePeerID: activePeerID})
		}
	}
	return tests
}

// TestHLVCreateDocumentSingleActor tests creating a document with a single actor in different topologies.
func TestHLVCreateDocumentSingleActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	for _, tc := range getSingleActorTestCase() {
		t.Run(tc.description(), func(t *testing.T) {
			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, tc.activePeerID, tc.description()))
			docVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(), docBody)
			waitForVersionAndBody(t, tc, peers, docVersion, docBody)
		})
	}
}

// TestHLVUpdateDocumentSingleActor tests creating a document with a single actor in different topologies.
func TestHLVUpdateDocumentSingleActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	for _, tc := range getSingleActorTestCase() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.HasPrefix(tc.activePeerID, "cbl") {
				t.Skip("Skipping Couchbase Lite test, returns unexpected body in proposeChanges: [304], CBG-4257")
			}
			if base.UnitTestUrlIsWalrus() {
				t.Skip("rosmar consistent failure CBG-4365")
			} else {
				t.Skip("intermittent failure in Couchbase Server CBG-4329")
			}
			if base.UnitTestUrlIsWalrus() {
				t.Skip("rosmar failure CBG-4365")
			}
			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 2}`, tc.activePeerID, tc.description()))
			updateVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), tc.docID(), body2)
			t.Logf("createVersion: %+v, updateVersion: %+v", createVersion, updateVersion)
			t.Logf("waiting for document version 2 on all peers")
			waitForVersionAndBody(t, tc, peers, updateVersion, body2)
		})
	}
}

// TestHLVDeleteDocumentSingleActor tests creating a document with a single actor in different topologies.
func TestHLVDeleteDocumentSingleActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyVV)
	for _, tc := range getSingleActorTestCase() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.HasPrefix(tc.activePeerID, "cbl") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}
			if !base.UnitTestUrlIsWalrus() {
				t.Skip("intermittent failure in Couchbase Server CBG-4329")
			}
			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), tc.docID())
			t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
			t.Logf("waiting for document deletion on all peers")
			waitForDeletion(t, tc, peers)
		})
	}
}

// TestHLVResurrectDocumentSingleActor tests resurrect a document with a single actor in different topologies.
func TestHLVResurrectDocumentSingleActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyVV)
	for _, tc := range getSingleActorTestCase() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.HasPrefix(tc.activePeerID, "cbl") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}
			t.Skip("Skipping resurection tests CBG-4366")

			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), tc.docID())
			t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
			t.Logf("waiting for document deletion on all peers")
			waitForDeletion(t, tc, peers)

			body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": "resurrection"}`, tc.activePeerID, tc.description()))
			resurrectVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), tc.docID(), body2)
			t.Logf("createVersion: %+v, deleteVersion: %+v, resurrectVersion: %+v", createVersion, deleteVersion, resurrectVersion)
			t.Logf("waiting for document resurrection on all peers")

			// Couchbase Lite peers do not know how to push a deletion yet, so we need to filter them out CBG-4257
			nonCBLPeers := make(map[string]Peer)
			for peerName, peer := range peers {
				if !strings.HasPrefix(peerName, "cbl") {
					nonCBLPeers[peerName] = peer
				}
			}
			waitForVersionAndBody(t, tc, peers, resurrectVersion, body2)
		})
	}
}

func requireBodyEqual(t *testing.T, expected []byte, actual db.Body) {
	actual = actual.DeepCopy(base.TestCtx(t))
	stripInternalProperties(actual)
	require.JSONEq(t, string(expected), string(base.MustJSONMarshal(t, actual)))
}

func stripInternalProperties(body db.Body) {
	delete(body, "_rev")
	delete(body, "_id")
}

func waitForVersionAndBody(t *testing.T, testCase singleActorTest, peers map[string]Peer, expectedVersion DocMetadata, expectedBody []byte) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		peer := peers[peerName]
		t.Logf("waiting for doc version on %s, written from %s", peer, testCase.activePeerID)
		body := peer.WaitForDocVersion(testCase.collectionName(), testCase.docID(), expectedVersion)
		requireBodyEqual(t, expectedBody, body)
	}
}

func waitForDeletion(t *testing.T, testCase singleActorTest, peers map[string]Peer) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		if strings.HasPrefix(peerName, "cbl") {
			t.Logf("skipping deletion check for Couchbase Lite peer %s, CBG-4257", peerName)
			continue
		}
		peer := peers[peerName]
		t.Logf("waiting for doc to be deleted on %s, written from %s", peer, testCase.activePeerID)
		peer.WaitForDeletion(testCase.collectionName(), testCase.docID())
	}
}
