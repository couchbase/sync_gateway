/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cbltest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/cbltestclient"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestCBLPushAndPull replicates both ways between Sync Gateway and a real Couchbase Lite, and checks
// the two agree on each document's body and current version.
//
// It is the smallest end-to-end use of cbltestclient, and doubles as a template for tests that need
// a real client rather than BlipTesterClient: Sync Gateway runs in a RestTester behind a real HTTP
// listener, because the test server's replicator is a separate process that has to dial it.
func TestCBLPushAndPull(t *testing.T) {
	server := cbltestclient.GetServer(t, "")

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	ctx := base.TestCtx(t)
	// "*" grants every channel, so the test is independent of the sync function's channel routing,
	// which differs between the default and named collections RestTester may give us.
	const username = "alice"
	rt.CreateUser(username, []string{"*"})

	listener := httptest.NewServer(rt.TestPublicHandler())
	defer listener.Close()
	endpoint := "ws" + strings.TrimPrefix(listener.URL, "http") + "/" + rt.GetDatabase().Name

	// The Couchbase Lite database has to hold the same collection RestTester replicates.
	dataStore := rt.GetSingleDataStore()
	collection := dataStore.ScopeName() + "." + dataStore.CollectionName()
	cblDatabase := cblDatabaseName(t.Name())
	require.NoError(t, server.RegisterDatabases(map[string]cbltestclient.DatabaseSpec{
		cblDatabase: {Collections: []string{collection}},
	}))
	defer server.UnregisterDatabases(cblDatabase)
	require.NoError(t, server.Reset(ctx, t.Name()))

	const sgDocID, cblDocID = "sg-doc", "cbl-doc"
	rt.PutDoc(sgDocID, `{"written_by": "sync gateway"}`)
	require.NoError(t, server.Client.UpdateDatabase(ctx, cblDatabase, []cbltestclient.DatabaseUpdateItem{{
		Type:              cbltestclient.UpdateTypeUpdate,
		Collection:        collection,
		DocumentID:        cblDocID,
		UpdatedProperties: []map[string]any{{"written_by": "couchbase lite"}},
	}}))

	// A one-shot replicator stops by itself once it has pushed and pulled everything there is.
	replicatorID, err := server.Client.StartReplicator(ctx, cbltestclient.ReplicatorConfig{
		Database:       cblDatabase,
		Collections:    []cbltestclient.ReplicationCollection{{Names: []string{collection}}},
		Endpoint:       endpoint,
		ReplicatorType: cbltestclient.ReplicatorTypePushAndPull,
		Authenticator: &cbltestclient.Authenticator{
			Type:     cbltestclient.AuthenticatorTypeBasic,
			Username: username,
			Password: rest.RestTesterDefaultUserPassword,
		},
	}, false)
	require.NoError(t, err)
	defer func() {
		// Stopping a replicator that has already stopped is harmless, and one left running would
		// keep dialling a listener this test has closed.
		assert.NoError(t, server.Client.StopReplicator(ctx, replicatorID))
	}()

	var status cbltestclient.ReplicatorStatus
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, err = server.Client.ReplicatorStatus(ctx, replicatorID)
		assert.NoError(c, err)
		assert.Equal(c, cbltestclient.ReplicatorActivityStopped, status.Activity)
	}, 30*time.Second, 50*time.Millisecond, "replicator did not finish; test server log:\n%s", server.LogTail())
	require.Nil(t, status.Error, "replicator failed; test server log:\n%s", server.LogTail())

	for _, docID := range []string{sgDocID, cblDocID} {
		t.Run(docID, func(t *testing.T) {
			sgVersion, sgBody := rt.GetDoc(docID)
			cblDoc, err := server.Client.GetDocument(ctx, cblDatabase, cbltestclient.DocumentRef{Collection: collection, ID: docID})
			require.NoError(t, err)

			assert.Equal(t, docID, cblDoc.ID())
			assert.Equal(t, sgBody["written_by"], cblDoc.Properties()["written_by"])

			// The current version is what each side compares to decide whether a document has
			// changed, so a mismatch here is a compatibility bug even when the bodies agree.
			cblHLV, _, err := cblDoc.HLV()
			require.NoError(t, err)
			assert.Equal(t, sgVersion.CV, *cblHLV.ExtractCurrentVersionFromHLV())
		})
	}

	// Both sides should hold exactly the two documents this test wrote, and nothing else.
	response := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_all_docs", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var sgAllDocs struct {
		Rows []struct {
			ID string `json:"id"`
		} `json:"rows"`
	}
	require.NoError(t, base.JSONUnmarshal(response.BodyBytes(), &sgAllDocs))
	sgDocIDs := make([]string, 0, len(sgAllDocs.Rows))
	for _, row := range sgAllDocs.Rows {
		sgDocIDs = append(sgDocIDs, row.ID)
	}
	assert.ElementsMatch(t, []string{sgDocID, cblDocID}, sgDocIDs)

	cblAllDocs, err := server.Client.GetAllDocuments(ctx, cblDatabase, []string{collection})
	require.NoError(t, err)
	cblDocIDs := make([]string, 0, len(cblAllDocs[collection]))
	for _, entry := range cblAllDocs[collection] {
		cblDocIDs = append(cblDocIDs, entry.ID)
	}
	assert.ElementsMatch(t, []string{sgDocID, cblDocID}, cblDocIDs)
}

// cblDatabaseName turns a test name into a Couchbase Lite database name, which the test server
// uses as a directory name.
func cblDatabaseName(testName string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, testName)
}
