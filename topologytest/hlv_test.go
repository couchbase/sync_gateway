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
	"slices"
	"strings"
	"testing"

	"golang.org/x/exp/maps"

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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCRUD, base.KeyImport)
	collectionName := getSingleDsName()
	for _, tc := range append(simpleTopologies, Topologies...) {
		// sort peers to ensure deterministic test order
		peerNames := maps.Keys(tc.peers)
		slices.Sort(peerNames)
		t.Run(tc.description, func(t *testing.T) {
			for _, activePeerID := range peerNames {
				t.Run("actor="+activePeerID, func(t *testing.T) {
					peers, replications := setupTests(t, tc.peers, tc.replications)
					// Skip tests not working yet
					if tc.skipIf != nil {
						tc.skipIf(t, activePeerID, peers)
					}
					for _, replication := range replications {
						// temporarily start the replication before writing the document, limitation of CouchbaseLiteMockPeer as active peer since WriteDocument is calls PushRev
						replication.Start()
					}
					docID := fmt.Sprintf("doc_%s_%s", strings.ReplaceAll(tc.description, " ", "_"), activePeerID)

					t.Logf("writing document %s from %s", docID, activePeerID)
					docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, activePeerID, tc.description))
					docVersion := peers[activePeerID].WriteDocument(collectionName, docID, docBody)

					// for single actor, use the docVersion that was written, but if there is a SG running, wait for import
					for _, peerName := range peerNames {
						peer := peers[peerName]

						t.Logf("waiting for doc version on %s, written from %s", peer, activePeerID)
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
