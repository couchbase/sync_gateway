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
	"time"

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
		t.Run(tc.description, func(t *testing.T) {
			for _, activePeerID := range tc.PeerNames() {
				t.Run("actor="+activePeerID, func(t *testing.T) {
					peers, _ := setupTests(t, tc, activePeerID)
					docID := fmt.Sprintf("doc_%s_%s", strings.ReplaceAll(tc.description, " ", "_"), activePeerID)

					t.Logf("writing document %s from %s", docID, activePeerID)
					docBody := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s"}`, activePeerID, tc.description))
					docVersion := peers[activePeerID].CreateDocument(collectionName, docID, docBody)

					for _, peerName := range tc.PeerNames() {
						peer := peers[peerName]

						t.Logf("waiting for doc version on %s, written from %s", peer, activePeerID)
						body := peer.WaitForDocVersion(collectionName, docID, docVersion)
						requireBodyEqual(t, docBody, body)
						stripInternalProperties(body)
						require.JSONEq(t, string(docBody), string(base.MustJSONMarshal(t, body)))
					}
				})
			}
		})
	}
}

// TestHLVCreateDocumentSingleActor tests creating a document with a single actor in different topologies.
func TestHLVUpdateDocumentSingleActor(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCRUD, base.KeyImport)
	collectionName := getSingleDsName()
	for i, tc := range append(simpleTopologies, Topologies...) {
		if i == 0 {
			continue
		}
		t.Run(tc.description, func(t *testing.T) {
			for _, activePeerID := range tc.PeerNames() {
				if strings.HasPrefix(activePeerID, "cbl") {
					t.Skip("Skipping CBL active peer, gives 304")
				}
				t.Run("actor="+activePeerID, func(t *testing.T) {
					peers, _ := setupTests(t, tc, activePeerID)
					docID := fmt.Sprintf("doc_%s_%s", strings.ReplaceAll(tc.description, " ", "_"), activePeerID)

					t.Logf("creating document %s from %s", docID, activePeerID)
					body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, activePeerID, tc.description))
					createVersion := peers[activePeerID].CreateDocument(collectionName, docID, body1)

					t.Logf("waiting for document version 1 on all peers")
					for _, peerName := range tc.PeerNames() {
						peer := peers[peerName]

						t.Logf("waiting for doc version on %s, written from %s", peer, activePeerID)
						body := peer.WaitForDocVersion(collectionName, docID, createVersion)
						requireBodyEqual(t, body1, body)
					}

					time.Sleep(1 * time.Second)
					t.Logf("HONK HONK HONK updating document %s from %s", docID, activePeerID)
					body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 2}`, activePeerID, tc.description))
					updateVersion := peers[activePeerID].WriteDocument(collectionName, docID, body2)
					t.Logf("createVersion: %+v, updateVersion: %+v", createVersion, updateVersion)
					t.Logf("waiting for document version 2 on all peers")
					for _, peerName := range tc.PeerNames() {
						peer := peers[peerName]

						t.Logf("waiting for doc version on %s, written from %s", peer, activePeerID)
						body := peer.WaitForDocVersion(collectionName, docID, updateVersion)
						requireBodyEqual(t, body2, body)
					}
				})
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
