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
func waitForVersionAndBody(t *testing.T, dsName base.ScopeAndCollectionName, peers Peers, replications Replications, docID string, expectedVersion BodyAndVersion) {
	t.Logf("waiting for doc version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range peers.SortedPeers() {
		t.Logf("waiting for doc version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		body := peer.WaitForDocVersion(dsName, docID, expectedVersion.docMeta, replications)
		requireBodyEqual(t, expectedVersion.body, body)
	}
}

func waitForTombstoneVersion(t *testing.T, dsName base.ScopeAndCollectionName, peers Peers, replications Replications, docID string, expectedVersion BodyAndVersion) {
	t.Logf("waiting for tombstone version on all peers, written from %s: %#v", expectedVersion.updatePeer, expectedVersion)
	for _, peer := range peers.SortedPeers() {
		t.Logf("waiting for tombstone version on peer %s, written from %s: %#v", peer, expectedVersion.updatePeer, expectedVersion)
		peer.WaitForTombstoneVersion(dsName, docID, expectedVersion.docMeta, replications)
	}
}

// waitForConvergingVersion waits for the same document version to reach all peers.
func waitForConvergingVersion(t *testing.T, dsName base.ScopeAndCollectionName, peers Peers, docID string) {
	t.Logf("waiting for converged doc versions across all peers")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for peerAid, peerA := range peers.SortedPeers() {
			docMetaA, bodyA := peerA.GetDocument(dsName, docID)
			for peerBid, peerB := range peers.SortedPeers() {
				if peerAid == peerBid {
					continue
				}
				docMetaB, bodyB := peerB.GetDocument(dsName, docID)
				cvA, cvB := docMetaA.CV(t), docMetaB.CV(t)
				require.Equalf(c, cvA, cvB, "CV mismatch: %s:%#v != %s:%#v", peerAid, docMetaA, peerBid, docMetaB)
				require.Equalf(c, bodyA, bodyB, "body mismatch: %s:%s != %s:%s", peerAid, bodyA, peerBid, bodyB)
			}
		}
	}, totalWaitTime, pollInterval)
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

// createConflictingDocs will create a doc on each peer of the same doc ID to create conflicting documents.
// It is not known at this stage which write the "winner" will be, since conflict resolution can happen at replication time which may not be LWW, or may be LWW but with a new value.
func createConflictingDocs(t *testing.T, dsName base.ScopeAndCollectionName, peers Peers, docID, topologyDescription string) {
	backingPeers := removeSyncGatewayBackingPeers(peers)
	for peerName, peer := range peers {
		if backingPeers[peerName] {
			continue
		}
		docBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "create"}`, peerName, topologyDescription))
		docVersion := peer.CreateDocument(dsName, docID, docBody)
		t.Logf("%s - createVersion: %#v", peerName, docVersion.docMeta)
	}
}

// updateConflictingDocs will update a doc on each peer of the same doc ID to create conflicting document mutations
func updateConflictingDocs(t *testing.T, dsName base.ScopeAndCollectionName, peers Peers, docID, topologyDescription string) {
	backingPeers := removeSyncGatewayBackingPeers(peers)
	for peerName, peer := range peers {
		if backingPeers[peerName] {
			continue
		}
		docBody := []byte(fmt.Sprintf(`{"activePeer": "%s", "topology": "%s", "action": "update"}`, peerName, topologyDescription))
		docVersion := peer.WriteDocument(dsName, docID, docBody)
		t.Logf("updateVersion: %#v", docVersion.docMeta)
	}
}

// deleteConflictDocs will delete a doc on each peer of the same doc ID to create conflicting document deletions
func deleteConflictDocs(t *testing.T, dsName base.ScopeAndCollectionName, peers Peers, docID string) {
	backingPeers := removeSyncGatewayBackingPeers(peers)
	for peerName, peer := range peers {
		if backingPeers[peerName] {
			continue
		}
		deleteVersion := peer.DeleteDocument(dsName, docID)
		t.Logf("deleteVersion: %#v", deleteVersion)
	}
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
