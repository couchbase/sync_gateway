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
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

type ActorTest interface {
	PeerNames() []string
	description() string
	collectionName() base.ScopeAndCollectionName
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
func (t singleActorTest) singleDocID() string {
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

// multiActorTest represents a test case for a single actor in a given topology.
type multiActorTest struct {
	topology Topology
}

// PeerNames returns the names of all peers in the test case's topology, sorted deterministically.
func (t multiActorTest) PeerNames() []string {
	return t.topology.PeerNames()
}

// description returns a human-readable description of the test case.
func (t multiActorTest) description() string {
	return fmt.Sprintf("%s_multi_actor", t.topology.description)
}

// docID returns a unique document ID for the test case+actor combination.
func (t multiActorTest) singleDocID() string {
	return fmt.Sprintf("doc_%s", strings.ReplaceAll(t.description(), " ", "_"))
}

// collectionName returns the collection name for the test case.
func (t multiActorTest) collectionName() base.ScopeAndCollectionName {
	return getSingleDsName()
}

func getMultiActorTestCases() []multiActorTest {
	var tests []multiActorTest
	for _, tc := range append(simpleTopologies, Topologies...) {
		tests = append(tests, multiActorTest{topology: tc})
	}
	return tests
}

// BodyAndVersion struct to hold doc update information to assert on
type BodyAndVersion struct {
	docMeta    DocMetadata
	body       []byte // expected body for version
	updatePeer string // the peer this particular document version mutation originated from
}

func stopPeerReplications(t *testing.T, peerReplications []PeerReplication) {
	for _, replication := range peerReplications {
		replication.Stop()
	}
}

func startPeerReplications(t *testing.T, peerReplications []PeerReplication) {
	for _, replication := range peerReplications {
		replication.Start()
	}
}

// TestHLVCreateDocumentSingleActor tests creating a document with a single actor in different topologies.
func TestHLVCreateDocumentSingleActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	for _, tc := range getSingleActorTestCase() {
		t.Run(tc.description(), func(t *testing.T) {
			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			docID := tc.singleDocID()
			docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, tc.activePeerID, tc.description()))
			docVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), docID, docBody)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)
		})
	}
}

// TestHLVCreateDocumentMultiActorConflict:
//   - Create conflicting docs on each peer
//   - Wait for docs last write to be replicated to all other peers
func TestHLVCreateDocumentMultiActorConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Panics against rosmar, CBG-4378")
	} else {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, tc := range getMultiActorTestCases() {
		if strings.Contains(tc.description(), "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication, CBG-4257")
		}
		t.Run(tc.description(), func(t *testing.T) {
			peers, replications := setupTests(t, tc.topology, "")

			stopPeerReplications(t, replications)

			docID := tc.singleDocID()
			docVersion := createConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)

			waitForVersionAndBody(t, tc, peers, docID, docVersion)

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
			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			docID := tc.singleDocID()
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
}

// TestHLVUpdateDocumentMultiActorConflict:
//   - Create conflicting docs on each peer
//   - Start replications
//   - Wait for last write to be replicated to all peers
//   - Stop replications
//   - Update all doc on all peers
//   - Start replications and wait for last update to be replicated to all peers
func TestHLVUpdateDocumentMultiActorConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Panics against rosmar, CBG-4378")
	} else {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, tc := range getMultiActorTestCases() {
		if strings.Contains(tc.description(), "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(tc.description(), func(t *testing.T) {
			peers, replications := setupTests(t, tc.topology, "")
			stopPeerReplications(t, replications)

			docID := tc.singleDocID()
			docVersion := createConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)

			stopPeerReplications(t, replications)
			docVersion = updateConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)
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

			docID := tc.singleDocID()
			body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), docID, body1)

			waitForVersionAndBody(t, tc, peers, docID, createVersion)

			deleteVersion := peers[tc.activePeerID].DeleteDocument(tc.collectionName(), docID)
			t.Logf("createVersion: %+v, deleteVersion: %+v", createVersion.docMeta, deleteVersion)
			t.Logf("waiting for document deletion on all peers")
			waitForDeletion(t, tc, peers, docID, tc.activePeerID)
		})
	}
}

// TestHLVDeleteDocumentMultiActorConflict:
//   - Create conflicting docs on each peer
//   - Start replications
//   - Wait for last write to be replicated to all peers
//   - Stop replications
//   - Delete docs on all peers
//   - Start replications and assert doc is deleted on all peers
func TestHLVDeleteDocumentMultiActorConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Panics against rosmar, CBG-4378")
	} else {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, tc := range getMultiActorTestCases() {
		if strings.Contains(tc.description(), "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(tc.description(), func(t *testing.T) {
			peers, replications := setupTests(t, tc.topology, "")
			stopPeerReplications(t, replications)

			docID := tc.singleDocID()
			docVersion := createConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)

			stopPeerReplications(t, replications)
			lastWrite := deleteConflictDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForDeletion(t, tc, peers, docID, lastWrite.updatePeer)
		})
	}
}

// TestHLVUpdateDeleteDocumentMultiActorConflict:
//   - Create conflicting docs on each peer
//   - Start replications
//   - Wait for last write to be replicated to all peers
//   - Stop replications
//   - Update docs on all peers, then delete the doc on one peer
//   - Start replications and assert doc is deleted on all peers (given the delete was the last write)
func TestHLVUpdateDeleteDocumentMultiActorConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Panics against rosmar, CBG-4378")
	} else {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, tc := range getMultiActorTestCases() {
		if strings.Contains(tc.description(), "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(tc.description(), func(t *testing.T) {
			peerList := tc.PeerNames()
			peers, replications := setupTests(t, tc.topology, "")
			stopPeerReplications(t, replications)

			docID := tc.singleDocID()
			docVersion := createConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)

			stopPeerReplications(t, replications)

			_ = updateConflictingDocs(t, tc, peers, docID)

			lastPeer := peerList[len(peerList)-1]
			deleteVersion := peers[lastPeer].DeleteDocument(tc.collectionName(), docID)
			t.Logf("deleteVersion: %+v", deleteVersion)

			startPeerReplications(t, replications)
			waitForDeletion(t, tc, peers, docID, lastPeer)
		})
	}
}

// TestHLVDeleteUpdateDocumentMultiActorConflict:
//   - Create conflicting docs on each peer
//   - Start replications
//   - Wait for last write to be replicated to all peers
//   - Stop replications
//   - Delete docs on all peers, then update the doc on one peer
//   - Start replications and assert doc update is replicated to all peers (given the update was the last write)
func TestHLVDeleteUpdateDocumentMultiActorConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Panics against rosmar, CBG-4378")
	} else {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, tc := range getMultiActorTestCases() {
		if strings.Contains(tc.description(), "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(tc.description(), func(t *testing.T) {
			peerList := tc.PeerNames()
			peers, replications := setupTests(t, tc.topology, "")
			stopPeerReplications(t, replications)

			docID := tc.singleDocID()
			docVersion := createConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)

			stopPeerReplications(t, replications)

			deleteConflictDocs(t, tc, peers, docID)

			// grab last peer in topology to write an update on
			lastPeer := peerList[len(peerList)-1]
			docBody := []byte(fmt.Sprintf(`{"topology": "%s", "write": 2}`, tc.description()))
			docUpdateVersion := peers[lastPeer].WriteDocument(tc.collectionName(), docID, docBody)
			t.Logf("updateVersion: %+v", docVersion.docMeta)
			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docUpdateVersion)
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

			docID := tc.singleDocID()
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
}

// TestHLVResurrectDocumentMultiActorConflict:
//   - Create conflicting docs on each peer
//   - Start replications
//   - Wait for last write to be replicated to all peers
//   - Stop replications
//   - Delete docs on all peers, start replications assert that doc is deleted on all peers
//   - Stop replications
//   - Resurrect doc on all peers
//   - Start replications and wait for last resurrection operation to be replicated to all peers
func TestHLVResurrectDocumentMultiActorConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyVV)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Panics against rosmar, CBG-4378")
	} else {
		t.Skip("Flakey failures on multi actor conflicting writes, CBG-4379")
	}
	for _, tc := range getMultiActorTestCases() {
		if strings.Contains(tc.description(), "CBL") {
			// Test case flakes given the WaitForDocVersion function only waits for a docID on the cbl peer. We need to be
			// able to wait for a specific version to arrive over pull replication
			t.Skip("We need to be able to wait for a specific version to arrive over pull replication + unexpected body in proposeChanges: [304] issue, CBG-4257")
		}
		t.Run(tc.description(), func(t *testing.T) {
			peers, replications := setupTests(t, tc.topology, "")
			stopPeerReplications(t, replications)

			docID := tc.singleDocID()
			docVersion := createConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)
			waitForVersionAndBody(t, tc, peers, docID, docVersion)

			stopPeerReplications(t, replications)
			lastWrite := deleteConflictDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)

			waitForDeletion(t, tc, peers, docID, lastWrite.updatePeer)

			stopPeerReplications(t, replications)

			// resurrect on
			lastWriteVersion := updateConflictingDocs(t, tc, peers, docID)

			startPeerReplications(t, replications)

			waitForVersionAndBody(t, tc, peers, docID, lastWriteVersion)
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

func waitForVersionAndBody(t *testing.T, testCase ActorTest, peers map[string]Peer, docID string, expectedVersion BodyAndVersion) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		peer := peers[peerName]
		t.Logf("waiting for doc version on %s, written from %s", peer, expectedVersion.updatePeer)
		body := peer.WaitForDocVersion(testCase.collectionName(), docID, expectedVersion.docMeta)
		requireBodyEqual(t, expectedVersion.body, body)
	}
}

func waitForDeletion(t *testing.T, testCase ActorTest, peers map[string]Peer, docID string, deleteActor string) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		if strings.HasPrefix(peerName, "cbl") {
			t.Logf("skipping deletion check for Couchbase Lite peer %s, CBG-4257", peerName)
			continue
		}
		peer := peers[peerName]
		t.Logf("waiting for doc to be deleted on %s, written from %s", peer, deleteActor)
		peer.WaitForDeletion(testCase.collectionName(), docID)
	}
}

// removeSyncGatewayBackingPeers will check if there is sync gateway in topology, if so will track the backing CBS
// so we can skip creating docs on these peers (avoiding conflicts between docs created on the SGW and cbs)
func removeSyncGatewayBackingPeers(peers map[string]Peer) map[string]bool {
	peersToRemove := make(map[string]bool)
	if peers["sg1"] != nil {
		// remove the backing store from doc update cycle to avoid conflicts on creating the document in bucket
		peersToRemove["cbs1"] = true
	}
	if peers["sg2"] != nil {
		// remove the backing store from doc update cycle to avoid conflicts on creating the document in bucket
		peersToRemove["cbs2"] = true
	}
	return peersToRemove
}

// createConflictingDocs will create a doc on each peer of the same doc ID to create conflicting documents, then
// returns the last peer to have a doc created on it
func createConflictingDocs(t *testing.T, tc multiActorTest, peers map[string]Peer, docID string) (lastWrite BodyAndVersion) {
	backingPeers := removeSyncGatewayBackingPeers(peers)
	var documentVersion []BodyAndVersion
	for _, peerName := range tc.PeerNames() {
		if backingPeers[peerName] {
			continue
		}
		docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, peerName, tc.description()))
		docVersion := peers[peerName].CreateDocument(tc.collectionName(), docID, docBody)
		t.Logf("createVersion: %+v", docVersion.docMeta)
		documentVersion = append(documentVersion, docVersion)
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// updateConflictingDocs will update a doc on each peer of the same doc ID to create conflicting document mutations, then
// returns the last peer to have a doc updated on it
func updateConflictingDocs(t *testing.T, tc multiActorTest, peers map[string]Peer, docID string) (lastWrite BodyAndVersion) {
	backingPeers := removeSyncGatewayBackingPeers(peers)
	var documentVersion []BodyAndVersion
	for _, peerName := range tc.PeerNames() {
		if backingPeers[peerName] {
			continue
		}
		docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 2}`, peerName, tc.description()))
		docVersion := peers[peerName].WriteDocument(tc.collectionName(), docID, docBody)
		t.Logf("updateVersion: %+v", docVersion.docMeta)
		documentVersion = append(documentVersion, docVersion)
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// deleteConflictDocs will delete a doc on each peer of the same doc ID to create conflicting document deletions, then
// returns the last peer to have a doc deleted on it
func deleteConflictDocs(t *testing.T, tc multiActorTest, peers map[string]Peer, docID string) (lastWrite BodyAndVersion) {
	backingPeers := removeSyncGatewayBackingPeers(peers)
	var documentVersion []BodyAndVersion
	for _, peerName := range tc.PeerNames() {
		if backingPeers[peerName] {
			continue
		}
		deleteVersion := peers[peerName].DeleteDocument(tc.collectionName(), docID)
		t.Logf("deleteVersion: %+v", deleteVersion)
		documentVersion = append(documentVersion, BodyAndVersion{docMeta: deleteVersion, updatePeer: peerName})
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}
