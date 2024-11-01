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
	"github.com/couchbase/sync_gateway/rest"
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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeySGTest)
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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeySGTest)
	for _, tc := range getSingleActorTestCase() {
		t.Run(tc.description(), func(t *testing.T) {
			if strings.HasPrefix(tc.activePeerID, "cbl") {
				t.Skip("Skipping Couchbase Lite test, returns unexpected body in proposeChanges: [304]")
			}
			peers, _ := setupTests(t, tc.topology, tc.activePeerID)

			body1 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 1}`, tc.activePeerID, tc.description()))
			createVersion := peers[tc.activePeerID].CreateDocument(tc.collectionName(), tc.docID(), body1)

			waitForVersionAndBody(t, tc, peers, createVersion, body1)

			t.Logf("HONK HONK HONK updating document %s from %s", tc.docID(), tc.activePeerID)
			body2 := []byte(fmt.Sprintf(`{"peer": "%s", "topology": "%s", "write": 2}`, tc.activePeerID, tc.description()))
			updateVersion := peers[tc.activePeerID].WriteDocument(tc.collectionName(), tc.docID(), body2)
			t.Logf("createVersion: %+v, updateVersion: %+v", createVersion, updateVersion)
			t.Logf("waiting for document version 2 on all peers")
			waitForVersionAndBody(t, tc, peers, updateVersion, body2)
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

func waitForVersionAndBody(t *testing.T, testCase singleActorTest, peers map[string]Peer, expectedVersion rest.DocVersion, expectedBody []byte) {
	// sort peer names to make tests more deterministic
	peerNames := maps.Keys(peers)
	for _, peerName := range peerNames {
		peer := peers[peerName]
		t.Logf("waiting for doc version on %s, written from %s", peer, testCase.activePeerID)
		body := peer.WaitForDocVersion(testCase.collectionName(), testCase.docID(), expectedVersion)
		requireBodyEqual(t, expectedBody, body)
	}
}
