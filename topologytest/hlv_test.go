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
	"github.com/stretchr/testify/assert"
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
// This is used for scenarios where it's valid for the full HLV to not converge. This includes cases where
// CBL conflict resolution results in additional history in the CBL version of the HLV that may not be pushed
// to CBS (e.g. remote wins)
//
// See following example:
//
//	+- - - - - - -+      +- - - - - - -+
//	'  cluster A  '      '  cluster B  '
//	' +---------+ '      ' +---------+ '
//	' |  cbs1   | ' <--> ' |  cbs2   | '
//	' +---------+ '      ' +---------+ '
//	' +---------+ '      ' +---------+ '
//	' |   sg1   | '      ' |   sg2   | '
//	' +---------+ '      ' +---------+ '
//	+- - - - -- - +      +- - - - - - -+
//	    ^                     ^
//	    |                     |
//	    |                     |
//	    v                     v
//	+---------+          +---------+
//	|   cbl1  |          |   cbl2  |
//	+---------+          +---------+
//
// Couchbase Server, since conflict resolution in XDCR will overwrite the HLV.
// 1. sg1 creates unique document cv: 1@rosmar1
// 2. sg2 creates unique document cv: 2@rosmar2
// 3. cbl1 pulls 1@rosmar1
// 4. cbl2 pull 2@rosmar2
// 5. cbs1 pulls 2@rosmar2, overwriting cv:1@rosmar1
// 6. cbl1 pulls 2@rosmar2, creating cv: 2@rosmar2, pv:1@rosmar1 overwriting
// Final state:
//   - cv:2@rosmar2 on cbs1, cbs2, cbl2
//   - cv:2@rosmar2, pv:1@rosmar1 on cbl1
func waitForCVAndBody(t *testing.T, dsName base.ScopeAndCollectionName, docID string, expectedVersion BodyAndVersion, topology Topology) {
	t.Logf("waiting for doc version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range topology.SortedPeers() {
		t.Logf("waiting for doc version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		var body db.Body
		if peer.Type() == PeerTypeCouchbaseLite {
			body = peer.WaitForCV(dsName, docID, expectedVersion.docMeta, topology)
		} else {
			body = peer.WaitForDocVersion(dsName, docID, expectedVersion.docMeta, topology)
		}
		requireBodyEqual(t, expectedVersion.body, body)
	}
}

// waitForConvergingTombstones waits for all peers to have a tombstone document for a given doc ID. This is the
// equivalent function to waitForCVAndBody if the expected document is a tombstone.
//
// Couchbase Server and Sync Gateway peers will have matching HLVs due XDCR conflict resolution always overwriting
// HLVs. However, it is possible that XDCR will replicate a tombstone from one Couchbase Server to antoher Couchbase
// Server and update its HLV. Since tombstones are not imported by Sync Gateway, this CV will not be replicated to
// Couchbase Server.
//
// In this case, all peers will have a tombstone for this document, but no assertions can be made on Couchbase Lite
// peers. See following example:
//
//	+- - - - - - -+      +- - - - - - -+
//	'  cluster A  '      '  cluster B  '
//	' +---------+ '      ' +---------+ '
//	' |  cbs1   | ' <--> ' |  cbs2   | '
//	' +---------+ '      ' +---------+ '
//	' +---------+ '      ' +---------+ '
//	' |   sg1   | '      ' |   sg2   | '
//	' +---------+ '      ' +---------+ '
//	+- - - - - - -+      +- - - - - - -+
//	    ^                     ^
//	    |                     |
//	    |                     |
//	    v                     v
//	+---------+          +---------+
//	|   cbl1  |          |   cbl2  |
//	+---------+          +---------+
//
// There is a converging document + HLV on all peers.
//  1. cbl1 deletes document cv: 5@cbl1
//  2. cbl2 deletes document cv: 6@cbl2
//  3. sg1 deletes document cv: 7@rosmar1
//  4. sg2 deletes document cv: 8@rosmar2
//  5. cbl2 pulls from sg2, creates 8@rosmar2;6@cbl2
//  6. cbl1 pulls from sg1, creates 7@rosmar1;5@cbl1
//  7. cbs1 pulls from cbs2, creating cv:8@rosmar2. This version isn't imported, so doesn't get recognized as needing
//     to replicate to Couchbase Lite.
//
// Final state:
//   - CBS1, CBS2: 8@rosmar2
//   - CBL1: 7@rosmar1;5@cbl1
//   - CBL2: 8@rosmar2;6@cbl2
func waitForConvergingTombstones(t *testing.T, dsName base.ScopeAndCollectionName, docID string, topology Topology) {
	t.Logf("waiting for converging tombstones")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		nonCBLVersions := make(map[string]DocMetadata)
		for peerName, peer := range topology.SortedPeers() {
			meta, body, exists := peer.GetDocumentIfExists(dsName, docID)
			if !assert.True(c, exists, "doc %s does not exist on peer %s", docID, peer) {
				return
			}
			if !assert.Nil(c, body, "expected tombstone for doc %s on peer %s", docID, peer) {
				return
			}
			if peer.Type() != PeerTypeCouchbaseLite {
				nonCBLVersions[peerName] = meta
			}
		}
		var nonCBLVersion *DocMetadata
		for peer, version := range nonCBLVersions {
			if nonCBLVersion == nil {
				nonCBLVersion = &version
				continue
			}
			if topology.CompareRevTreeOnly() {
				assertRevTreeIDEqual(c, dsName, docID, peer, version, nil, *nonCBLVersion, topology)
			} else {
				assertHLVEqual(c, dsName, docID, peer, version, nil, *nonCBLVersion, topology)
			}
		}
	}, totalWaitTime, pollInterval)
}

// waitForTombstoneVersion waits for a tombstone document with a particular HLV to be present on all peers.
func waitForTombstoneVersion(t *testing.T, dsName base.ScopeAndCollectionName, docID string, expectedVersion BodyAndVersion, topology Topology) {
	t.Logf("waiting for tombstone version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range topology.SortedPeers() {
		t.Logf("waiting for tombstone version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		peer.WaitForTombstoneVersion(dsName, docID, expectedVersion.docMeta, topology)
	}
}

// createConflictingDocs will create a doc on each peer of the same doc ID to create conflicting documents, then
// returns the last peer to have a doc created on it
func createConflictingDocs(dsName base.ScopeAndCollectionName, docID string, topology Topology) (lastWrite BodyAndVersion) {
	var documentVersion []BodyAndVersion
	for peerName, peer := range topology.peers.NonImportSortedPeers() {
		docBody := fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, peerName, topology.specDescription)
		docVersion := peer.CreateDocument(dsName, docID, []byte(docBody))
		documentVersion = append(documentVersion, docVersion)
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// updateConflictingDocs will update a doc on each peer of the same doc ID to create conflicting document mutations, then
// returns the last peer to have a doc updated on it.
func updateConflictingDocs(dsName base.ScopeAndCollectionName, docID string, topology Topology) (lastWrite BodyAndVersion) {
	var documentVersion []BodyAndVersion
	for peerName, peer := range topology.peers.NonImportSortedPeers() {
		docBody := fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "update"}`, peerName, topology.specDescription)
		docVersion := peer.WriteDocument(dsName, docID, []byte(docBody))
		documentVersion = append(documentVersion, docVersion)
	}
	index := len(documentVersion) - 1
	lastWrite = documentVersion[index]

	return lastWrite
}

// deleteConflictDocs will delete a doc on each peer of the same doc ID to create conflicting document deletions, then
// returns the last peer to have a doc deleted on it
func deleteConflictDocs(dsName base.ScopeAndCollectionName, docID string, topology Topology) (lastWrite BodyAndVersion) {
	var documentVersion []BodyAndVersion
	for peerName, peer := range topology.peers.NonImportSortedPeers() {
		deleteVersion := peer.DeleteDocument(dsName, docID)
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
