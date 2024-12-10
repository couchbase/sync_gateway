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

// PeerNames returns the names of all peers in the test case's topology, sorted deterministically.
func (t singleActorTest) PeerNames() []string {
	return t.topology.PeerNames()
}

// collectionName returns the collection name for the test case.
func (t singleActorTest) collectionName() base.ScopeAndCollectionName {
	return getSingleDsName()
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

func (b BodyAndVersion) GoString() string {
	return fmt.Sprintf("%#v body:%s, updatePeer:%s", b.docMeta, string(b.body), b.updatePeer)
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
		t.Logf("waiting for doc version %#v on %s, written from %s", expectedVersion, peer, expectedVersion.updatePeer)
		body := peer.WaitForDocVersion(testCase.collectionName(), docID, expectedVersion.docMeta)
		requireBodyEqual(t, expectedVersion.body, body)
	}
}

func waitForVersionAndBodyOnNonActivePeers(t *testing.T, testCase ActorTest, docID string, peers map[string]Peer, expectedVersion BodyAndVersion) {
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		if peerName == expectedVersion.updatePeer {
			// skip peer the write came from
			continue
		}
		peer := peers[peerName]
		t.Logf("waiting for doc version %#v on %s, update written from %s", expectedVersion, peer, expectedVersion.updatePeer)
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
	documentVersion := make([]BodyAndVersion, 0, len(peers))
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
	documentVersion := make([]BodyAndVersion, 0, len(peers))
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

// getDocID returns a unique doc ID for the test case
func getDocID(t *testing.T) string {
	return fmt.Sprintf("doc_%s", strings.ReplaceAll(t.Name(), " ", "_"))
}
