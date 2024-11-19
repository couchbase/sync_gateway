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

type ActorTest interface {
	description() string
	docID(peerName string) string
	collectionName() base.ScopeAndCollectionName
	getOriginalActor() string // get actor that the doc was first written on in topology
}

var _ ActorTest = &singleActorTest{}
var _ ActorTest = &multiActorTest{}

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
func (t singleActorTest) docID(_ string) string {
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

func (t singleActorTest) getOriginalActor() string {
	return t.activePeerID
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

// multiActorTest represents a test case for a single actor in a given topology.
type multiActorTest struct {
	topology      Topology
	originalActor string
}

// description returns a human-readable description of the test case.
func (t multiActorTest) description() string {
	return fmt.Sprintf("%s_multi_actor", t.topology.description)
}

// docID returns a unique document ID for the test case+actor combination.
func (t multiActorTest) docID(peerName string) string {
	return fmt.Sprintf("doc_%s_%s", strings.ReplaceAll(t.description(), " ", "_"), peerName)
}

// collectionName returns the collection name for the test case.
func (t multiActorTest) collectionName() base.ScopeAndCollectionName {
	return getSingleDsName()
}

func (t multiActorTest) getOriginalActor() string {
	return t.originalActor
}

func getMultiActorTestCases() []multiActorTest {
	var tests []multiActorTest
	for _, tc := range append(simpleTopologies, Topologies...) {
		tests = append(tests, multiActorTest{topology: tc})
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
			docVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(tc.activePeerID), docBody)
			waitForVersionAndBody(t, tc, peers, docVersion, docBody)
		})
	}
}

func TestHLVCreateDocumentMultiActor(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)

	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			peers, _ := setupTests(t, tc.topology, "")

			for peerName, _ := range peers {
				tc.originalActor = peerName
				docID := tc.docID(tc.getOriginalActor())
				docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, peerName, tc.description()))
				docVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, docBody)
				waitForVersionAndBody(t, tc, peers, docVersion, docBody)
			}
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
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(tc.activePeerID), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 2}`, tc.activePeerID, tc.description()))
			updateVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), tc.docID(tc.activePeerID), body2)
			t.Logf("createVersion: %+v, updateVersion: %+v", createVersion, updateVersion)
			t.Logf("waiting for document version 2 on all peers")
			waitForVersionAndBody(t, tc, peers, updateVersion, body2)
		})
	}
}

func TestHLVUpdateDocumentMultiActor(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)

	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.Contains(tc.description(), "CBL") {
				t.Skip("Skipping Couchbase Lite test, returns unexpected body in proposeChanges: [304], CBG-4257")
			}
			if base.UnitTestUrlIsWalrus() {
				t.Skip("rosmar consistent failure CBG-4365")
			} else {
				t.Skip("intermittent failure in Couchbase Server CBG-4329")
			}
			peers, _ := setupTests(t, tc.topology, "")

			for peerName, _ := range peers {
				tc.originalActor = peerName
				docID := tc.docID(tc.getOriginalActor())
				body1 := []byte(fmt.Sprintf(`{"originPeer": "%s", "topology": "%s", "writePeer": "%s"}`, peerName, tc.description(), peerName))
				createVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, body1)
				waitForVersionAndBody(t, tc, peers, createVersion, body1)

				peerList := maps.Keys(peers)
				for _, updatePeer := range peerList {
					if updatePeer == peerName {
						// skip current peer in topology
						continue
					}
					newBody := []byte(fmt.Sprintf(`{"originPeer": "%s", "topology": "%s", "writePeer": "%s"}`, peerName, tc.description(), updatePeer))
					updateVersion := peers[updatePeer].WriteDocument(tc.collectionName(), docID, newBody)
					t.Logf("createVersion: %+v, updateVersion: %+v", createVersion, updateVersion)
					t.Logf("waiting for document %s peer write %s on all peers", docID, updatePeer)
					waitForVersionAndBodyOnNonActivePeers(t, tc, peers, updateVersion, newBody, updatePeer)
				}
			}
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
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(tc.activePeerID), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), tc.docID(tc.activePeerID))
			t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
			t.Logf("waiting for document deletion on all peers")
			waitForDeletion(t, tc, peers)
		})
	}
}

func TestHLVDeleteDocumentMultiActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyVV)
	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.Contains(tc.description(), "CBL") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}
			if !base.UnitTestUrlIsWalrus() {
				t.Skip("intermittent failure in Couchbase Server CBG-4329")
			}
			peers, _ := setupTests(t, tc.topology, "")

			for peerName, _ := range peers {
				tc.originalActor = peerName
				docID := tc.docID(tc.getOriginalActor())
				body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s","writePeer": "%s"}`, docID, tc.description(), peerName))
				createVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, body1)
				waitForVersionAndBody(t, tc, peers, createVersion, body1)

				// map iteration is indeterminate, somewhat random peer will delete the doc and assert we see this deletion on all other peers
				peerList := maps.Keys(peers)

				for _, deletePeer := range peerList {
					if deletePeer == peerName {
						// continue till we find peer that write didn't originate from
						continue
					}
					deleteVersion := peers[deletePeer].DeleteDocument(tc.collectionName(), docID)
					t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
					t.Logf("waiting for document %s deletion on all peers", docID)
					waitForDeletion(t, tc, peers)
					break
				}
			}
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
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(tc.activePeerID), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), tc.docID(tc.activePeerID))
			t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
			t.Logf("waiting for document deletion on all peers")
			waitForDeletion(t, tc, peers)

			body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": "resurrection"}`, tc.activePeerID, tc.description()))
			resurrectVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), tc.docID(tc.activePeerID), body2)
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

func TestHLVResurrectDocumentMultiActor(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyVV)
	for _, tc := range getMultiActorTestCases() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.Contains(tc.description(), "CBL") {
				t.Skip("Skipping Couchbase Lite test, does not know how to push a deletion yet CBG-4257")
			}

			t.Skip("skipped resurrection test, intermittent failures CBG-4372")

			peers, _ := setupTests(t, tc.topology, "")

			for peerName, _ := range peers {
				tc.originalActor = peerName
				docID := tc.docID(tc.getOriginalActor())
				body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s","writePeer": "%s"}`, docID, tc.description(), peerName))
				createVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, body1)
				waitForVersionAndBody(t, tc, peers, createVersion, body1)

				peerList := maps.Keys(peers)
				for _, updatePeer := range peerList {
					deleteVersion := peers[updatePeer].DeleteDocument(tc.collectionName(), docID)
					t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion, deleteVersion)
					t.Logf("waiting for document %s deletion on all peers", docID)
					waitForDeletion(t, tc, peers)
					// recreate doc and assert it arrives at all peers
					resBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": "resurrection on peer %s"}`, tc.originalActor, tc.description(), updatePeer))
					updateVersion := peers[updatePeer].WriteDocument(tc.collectionName(), docID, resBody)
					waitForVersionAndBodyOnNonActivePeers(t, tc, peers, updateVersion, resBody, updatePeer)
				}
			}
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

func waitForVersionAndBody(t *testing.T, testCase ActorTest, peers map[string]Peer, expectedVersion DocMetadata, expectedBody []byte) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		peer := peers[peerName]
		t.Logf("waiting for doc version on %s, written from %s", peer, testCase.getOriginalActor())
		body := peer.WaitForDocVersion(testCase.collectionName(), testCase.docID(testCase.getOriginalActor()), expectedVersion)
		requireBodyEqual(t, expectedBody, body)
	}
}

// waitForVersionAndBodyOnNonActivePeers waits for a document version on all peers that the update didn't occur on
func waitForVersionAndBodyOnNonActivePeers(t *testing.T, testCase ActorTest, peers map[string]Peer, expectedVersion DocMetadata, expectedBody []byte, writingPeer string) {
	delete(peers, writingPeer)
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		peer := peers[peerName]
		t.Logf("waiting for doc version on %s, update written from %s", peer, writingPeer)
		body := peer.WaitForDocVersion(testCase.collectionName(), testCase.docID(testCase.getOriginalActor()), expectedVersion)
		requireBodyEqual(t, expectedBody, body)
	}
}

func waitForDeletion(t *testing.T, testCase ActorTest, peers map[string]Peer) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		if strings.HasPrefix(peerName, "cbl") {
			t.Logf("skipping deletion check for Couchbase Lite peer %s, CBG-4257", peerName)
			continue
		}
		peer := peers[peerName]
		t.Logf("waiting for doc to be deleted on %s, written from %s", peer, testCase.getOriginalActor())
		peer.WaitForDeletion(testCase.collectionName(), testCase.docID(testCase.getOriginalActor()))
	}
}
