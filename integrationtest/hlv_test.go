// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package integrationtest

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"

	"github.com/stretchr/testify/require"
)

func getSingleDsName() base.ScopeAndCollectionName {
	if base.TestsUseNamedCollections() {
		return base.ScopeAndCollectionName{Scope: "sg_test_0", Collection: "sg_test_0"}
	}
	return base.DefaultScopeAndCollectionName()
}

// TestHLVCreateDocumentSingleActor tests creating a document with a single actor in different topologies.
func TestHLVCreateDocumentSingleActor(t *testing.T) {
	collectionName := getSingleDsName()
	for i, tc := range Topologies {
		t.Run(tc.description, func(t *testing.T) {
			for peerID := range tc.peers {
				t.Run("actor="+peerID, func(t *testing.T) {
					peers := createPeers(t, tc.peers)
					replications := CreatePeerReplications(t, peers, tc.replications)
					for _, replication := range replications {
						// temporarily start the replication before writing the document, limitation of CouchbaseLiteMockPeer as active peer since WriteDocument is calls PushRev
						replication.Start()
					}
					docID := fmt.Sprintf("doc_%d_%s", i, peerID)

					docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, peerID, tc.description))
					docVersion := peers[peerID].WriteDocument(collectionName, docID, docBody)
					for _, peer := range peers {
						t.Logf("waiting for doc version on %s, written from %s", peer, peerID)
						body := peer.WaitForDocVersion(collectionName, docID, docVersion)
						// remove internal properties to do a comparison
						stripInternalProperties(body)
						require.JSONEq(t, string(docBody), string(base.MustJSONMarshal(t, body)))
					}
				})
			}
		})
	}
}

func stripInternalProperties(body db.Body) {
	delete(body, "_rev")
	delete(body, "_id")
}
