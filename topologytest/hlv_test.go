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
)

// getSingleDsName returns the default scope and collection name for tests
func getSingleDsName() base.ScopeAndCollectionName {
	if base.TestsUseNamedCollections() {
		return base.ScopeAndCollectionName{Scope: "sg_test_0", Collection: "sg_test_0"}
	}
	return base.DefaultScopeAndCollectionName()
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

// requireBodyEqual compares bodies, removing private properties that might exist.
func requireBodyEqual(t *testing.T, expected []byte, actual db.Body) {
	actual = actual.DeepCopy(base.TestCtx(t))
	stripInternalProperties(actual)
	require.JSONEq(t, string(expected), string(base.MustJSONMarshal(t, actual)))
}

func stripInternalProperties(body db.Body) {
	delete(body, "_rev")
	delete(body, "_id")
}

// waitForVersionAndBody waits for a document to reach a specific version on all peers.
func waitForVersionAndBody(t *testing.T, dsName base.ScopeAndCollectionName, docID string, expectedVersion BodyAndVersion, topology Topology) {
	t.Logf("waiting for doc version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range topology.SortedPeers() {
		t.Logf("waiting for doc version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		body := peer.WaitForDocVersion(dsName, docID, expectedVersion.docMeta, topology)
		requireBodyEqual(t, expectedVersion.body, body)
	}
}

// waitForCVAndBody waits for a document to reach a specific cv on all peers.
func waitForCVAndBody(t *testing.T, dsName base.ScopeAndCollectionName, docID string, expectedVersion BodyAndVersion, topology Topology) {
	t.Logf("waiting for doc version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range topology.SortedPeers() {
		t.Logf("waiting for doc version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		body := peer.WaitForCV(dsName, docID, expectedVersion.docMeta, topology)
		requireBodyEqual(t, expectedVersion.body, body)
	}
}
func waitForTombstoneVersion(t *testing.T, dsName base.ScopeAndCollectionName, docID string, expectedVersion BodyAndVersion, topology Topology) {
	t.Logf("waiting for tombstone version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range topology.SortedPeers() {
		t.Logf("waiting for tombstone version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		peer.WaitForTombstoneVersion(dsName, docID, expectedVersion.docMeta, topology)
	}
}

// createConflictingDocs will create a doc on each peer of the same doc ID to create conflicting documents, then
// returns the last peer to have a doc created on it
func createConflictingDocs(t *testing.T, dsName base.ScopeAndCollectionName, docID string, topology Topology) (lastWrite BodyAndVersion) {
	var documentVersion []BodyAndVersion
	for peerName, peer := range topology.peers.NonImportSortedPeers() {
		if peer.Type() == PeerTypeCouchbaseLite {
			// FIXME: Skipping Couchbase Lite tests for multi actor conflicts, CBG-4434
			continue
		}
		docBody := fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, peerName, topology.specDescription)
		docVersion := peer.CreateDocument(dsName, docID, []byte(docBody))
		t.Logf("%s - createVersion: %#v", peerName, docVersion.docMeta)
		documentVersion = append(documentVersion, docVersion)
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// updateConflictingDocs will update a doc on each peer of the same doc ID to create conflicting document mutations, then
// returns the last peer to have a doc updated on it.
func updateConflictingDocs(t *testing.T, dsName base.ScopeAndCollectionName, docID string, topology Topology) (lastWrite BodyAndVersion) {
	var documentVersion []BodyAndVersion
	for peerName, peer := range topology.peers.NonImportSortedPeers() {
		docBody := fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "update"}`, peerName, topology.specDescription)
		docVersion := peer.WriteDocument(dsName, docID, []byte(docBody))
		t.Logf("updateVersion: %#v", docVersion.docMeta)
		documentVersion = append(documentVersion, docVersion)
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// deleteConflictDocs will delete a doc on each peer of the same doc ID to create conflicting document deletions, then
// returns the last peer to have a doc deleted on it
func deleteConflictDocs(t *testing.T, dsName base.ScopeAndCollectionName, docID string, topology Topology) (lastWrite BodyAndVersion) {
	var documentVersion []BodyAndVersion
	for peerName, peer := range topology.peers.NonImportSortedPeers() {
		deleteVersion := peer.DeleteDocument(dsName, docID)
		t.Logf("deleteVersion: %#v", deleteVersion)
		documentVersion = append(documentVersion, BodyAndVersion{docMeta: deleteVersion, updatePeer: peerName})
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// getDocID returns a unique doc ID for the test case. Note: when running with Couchbase Server and -count > 1, this will return duplicate IDs for count 2 and higher and they can conflict due to the way bucket pool works.
func getDocID(t *testing.T) string {
	name := strings.TrimPrefix(t.Name(), "Test") // shorten doc name
	replaceChars := []string{" ", "/"}
	for _, char := range replaceChars {
		name = strings.ReplaceAll(name, char, "_")
	}
	return fmt.Sprintf("doc_%s", name)
}
