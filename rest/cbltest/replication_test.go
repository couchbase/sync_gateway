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
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/cbltestclient"
	"github.com/couchbase/sync_gateway/testing/require"
)

const (
	waitTime     = 30 * time.Second
	pollInterval = 50 * time.Millisecond
)

// TestCBLPushAndPull replicates both ways between Sync Gateway and a real Couchbase Lite, and checks
// the two agree on each document's body and current version.
//
// It is the smallest end-to-end use of cbltestclient, and doubles as a template for tests that need
// a real client rather than BlipTesterClient.
func TestCBLPushAndPull(t *testing.T) {
	c := newCBLTest(t)

	const sgDocID, cblDocID = "sg-doc", "cbl-doc"
	c.rt.PutDoc(sgDocID, `{"written_by": "sync gateway"}`)
	c.putCBLDoc(cblDocID, map[string]any{"written_by": "couchbase lite"})

	// A one-shot replicator stops by itself once it has pushed and pulled everything there is.
	replicatorID := c.startReplicator(cbltestclient.ReplicatorTypePushAndPull, false)
	c.waitForStopped(replicatorID)

	for _, docID := range []string{sgDocID, cblDocID} {
		t.Run(docID, func(t *testing.T) {
			sgVersion, sgBody := c.rt.GetDoc(docID)
			cblDoc, err := c.server.Client.GetDocument(c.ctx, c.database, c.ref(docID))
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
	response := c.rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_all_docs", "")
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

	cblAllDocs, err := c.server.Client.GetAllDocuments(c.ctx, c.database, []string{c.collection})
	require.NoError(t, err)
	cblDocIDs := make([]string, 0, len(cblAllDocs[c.collection]))
	for _, entry := range cblAllDocs[c.collection] {
		cblDocIDs = append(cblDocIDs, entry.ID)
	}
	assert.ElementsMatch(t, []string{sgDocID, cblDocID}, cblDocIDs)
}

// TestCBLSeparatePushAndPull runs a continuous push and a continuous pull as two replicators on one
// database, stops them, and starts them again.  That is how the topology tests model a Couchbase
// Lite peer: each direction is its own replication that can be stopped and restarted on its own.
func TestCBLSeparatePushAndPull(t *testing.T) {
	c := newCBLTest(t)

	const sgDocID, cblDocID = "sg-doc", "cbl-doc"
	sgVersion := c.rt.PutDoc(sgDocID, `{"rev": 1}`)
	cblVersion := c.putCBLDoc(cblDocID, map[string]any{"rev": float64(1)})

	push := c.startReplicator(cbltestclient.ReplicatorTypePush, true)
	pull := c.startReplicator(cbltestclient.ReplicatorTypePull, true)
	c.waitForCBLVersion(sgDocID, sgVersion.CV)
	c.waitForSGVersion(cblDocID, cblVersion)

	c.stopReplicator(push)
	c.stopReplicator(pull)

	// Written while nothing is replicating, so these only arrive if the restarted replicators pick
	// up from where they stopped.
	sgVersion = c.rt.UpdateDoc(sgDocID, sgVersion, `{"rev": 2}`)
	cblVersion = c.putCBLDoc(cblDocID, map[string]any{"rev": float64(2)})

	c.startReplicator(cbltestclient.ReplicatorTypePush, true)
	c.startReplicator(cbltestclient.ReplicatorTypePull, true)
	c.waitForCBLVersion(sgDocID, sgVersion.CV)
	c.waitForSGVersion(cblDocID, cblVersion)
}

// TestCBLDeleteReplicates deletes a document on each side and checks the delete reaches the other.
//
// The test server cannot read back a tombstone, so on the Couchbase Lite side this can only check
// that the document is gone, not which version deleted it.  On the Sync Gateway side the tombstone
// is readable, so its current version is checked to come from Couchbase Lite.
func TestCBLDeleteReplicates(t *testing.T) {
	c := newCBLTest(t)

	const sgDocID, cblDocID = "sg-doc", "cbl-doc"
	sgVersion := c.rt.PutDoc(sgDocID, `{"written_by": "sync gateway"}`)
	cblVersion := c.putCBLDoc(cblDocID, map[string]any{"written_by": "couchbase lite"})

	c.startReplicator(cbltestclient.ReplicatorTypePushAndPull, true)
	c.waitForCBLVersion(sgDocID, sgVersion.CV)
	c.waitForSGVersion(cblDocID, cblVersion)

	c.rt.DeleteDoc(sgDocID, sgVersion)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		_, err := c.server.Client.GetDocument(c.ctx, c.database, c.ref(sgDocID))
		assert.True(ct, cbltestclient.IsDocumentNotFound(err), "expected %s to be deleted on Couchbase Lite, got %v", sgDocID, err)
	}, waitTime, pollInterval, "%s", c.server.LogTail())

	require.NoError(t, c.server.Client.UpdateDatabase(c.ctx, c.database, []cbltestclient.DatabaseUpdateItem{{
		Type:       cbltestclient.UpdateTypeDelete,
		Collection: c.collection,
		DocumentID: cblDocID,
	}}))
	collection, ctx := c.rt.GetSingleTestDatabaseCollectionWithUser()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		doc, err := collection.GetDocument(ctx, cblDocID, db.DocUnmarshalAll)
		if !assert.NoError(ct, err) {
			return
		}
		assert.True(ct, doc.IsDeleted(), "expected %s to be a tombstone on Sync Gateway", cblDocID)
		cv := doc.ExtractDocVersion().CV
		assert.Equal(ct, cblVersion.SourceID, cv.SourceID, "the tombstone should have been written by Couchbase Lite")
		assert.Greater(ct, cv.Value, cblVersion.Value, "the tombstone should be newer than the document it deletes")
	}, waitTime, pollInterval, "%s", c.server.LogTail())
}

// cblTest is a RestTester behind a real HTTP listener, with a user that can see every channel, and
// an empty Couchbase Lite database holding the collection the RestTester replicates.  The listener
// is needed because the test server's replicator is a separate process that has to dial it.
type cblTest struct {
	t      *testing.T
	ctx    context.Context
	server *cbltestclient.Server
	rt     *rest.RestTester
	// endpoint is the websocket URL of the Sync Gateway database.
	endpoint string
	// collection is the "<scope>.<collection>" both sides replicate.
	collection string
	// database is the name of the Couchbase Lite database.
	database string
}

const username = "alice"

func newCBLTest(t *testing.T) *cblTest {
	// Started before the RestTester so that a test with no test server built skips straight away.
	// The database is made once the RestTester says which collection to replicate.
	server := cbltestclient.NewServer(t, cbltestclient.Options{})

	rt := rest.NewRestTester(t, nil)
	t.Cleanup(rt.Close)
	// "*" grants every channel, so the tests are independent of the sync function's channel
	// routing, which differs between the default and named collections RestTester may give us.
	rt.CreateUser(username, []string{"*"})

	listener := httptest.NewServer(rt.TestPublicHandler())
	t.Cleanup(listener.Close)

	dataStore := rt.GetSingleDataStore()
	c := &cblTest{
		t:          t,
		ctx:        base.TestCtx(t),
		server:     server,
		rt:         rt,
		endpoint:   "ws" + strings.TrimPrefix(listener.URL, "http") + "/" + rt.GetDatabase().Name,
		collection: dataStore.ScopeName() + "." + dataStore.CollectionName(),
		database:   cblDatabaseName(t.Name()),
	}
	require.NoError(t, server.Client.Reset(c.ctx, t.Name(), map[string]cbltestclient.DatabaseSpec{
		c.database: {Collections: []string{c.collection}},
	}))
	return c
}

func (c *cblTest) ref(docID string) cbltestclient.DocumentRef {
	return cbltestclient.DocumentRef{Collection: c.collection, ID: docID}
}

// startReplicator starts a replicator to the Sync Gateway database.  It is stopped when the test
// ends, since one left running would keep dialling a listener the test has closed.
func (c *cblTest) startReplicator(replicatorType cbltestclient.ReplicatorType, continuous bool) string {
	c.t.Helper()
	replicatorID, err := c.server.Client.StartReplicator(c.ctx, cbltestclient.ReplicatorConfig{
		Database:       c.database,
		Collections:    []cbltestclient.ReplicationCollection{{Names: []string{c.collection}}},
		Endpoint:       c.endpoint,
		ReplicatorType: replicatorType,
		Continuous:     continuous,
		Authenticator: &cbltestclient.Authenticator{
			Type:     cbltestclient.AuthenticatorTypeBasic,
			Username: username,
			Password: rest.RestTesterDefaultUserPassword,
		},
	}, false)
	require.NoError(c.t, err)
	// Stopping a replicator that has already stopped is harmless.
	c.t.Cleanup(func() { assert.NoError(c.t, c.server.Client.StopReplicator(c.ctx, replicatorID)) })
	return replicatorID
}

// stopReplicator stops a replicator and waits until it has.
func (c *cblTest) stopReplicator(replicatorID string) {
	c.t.Helper()
	require.NoError(c.t, c.server.Client.StopReplicator(c.ctx, replicatorID))
	c.waitForStopped(replicatorID)
}

// waitForStopped waits for a replicator to stop, and fails the test if it stopped with an error.
func (c *cblTest) waitForStopped(replicatorID string) {
	c.t.Helper()
	var status cbltestclient.ReplicatorStatus
	require.EventuallyWithT(c.t, func(ct *assert.CollectT) {
		var err error
		status, err = c.server.Client.ReplicatorStatus(c.ctx, replicatorID)
		assert.NoError(ct, err)
		assert.Equal(ct, cbltestclient.ReplicatorActivityStopped, status.Activity)
	}, waitTime, pollInterval, "replicator did not stop; %s", c.server.LogTail())
	require.Nil(c.t, status.Error, "replicator failed; %s", c.server.LogTail())
}

// putCBLDoc writes properties to a document on Couchbase Lite, and returns the current version it
// ended up with.
func (c *cblTest) putCBLDoc(docID string, properties map[string]any) db.Version {
	c.t.Helper()
	require.NoError(c.t, c.server.Client.UpdateDatabase(c.ctx, c.database, []cbltestclient.DatabaseUpdateItem{{
		Type:              cbltestclient.UpdateTypeUpdate,
		Collection:        c.collection,
		DocumentID:        docID,
		UpdatedProperties: []map[string]any{properties},
	}}))
	return c.cblVersion(c.t, docID)
}

// cblVersion returns a document's current version on Couchbase Lite.
func (c *cblTest) cblVersion(t require.TestingT, docID string) db.Version {
	doc, err := c.server.Client.GetDocument(c.ctx, c.database, c.ref(docID))
	require.NoError(t, err)
	hlv, _, err := doc.HLV()
	require.NoError(t, err)
	return *hlv.ExtractCurrentVersionFromHLV()
}

// waitForCBLVersion waits for a document on Couchbase Lite to reach a current version.
func (c *cblTest) waitForCBLVersion(docID string, want db.Version) {
	c.t.Helper()
	require.EventuallyWithT(c.t, func(ct *assert.CollectT) {
		doc, err := c.server.Client.GetDocument(c.ctx, c.database, c.ref(docID))
		if !assert.NoError(ct, err) {
			return
		}
		hlv, _, err := doc.HLV()
		if !assert.NoError(ct, err) {
			return
		}
		assert.Equal(ct, want, *hlv.ExtractCurrentVersionFromHLV())
	}, waitTime, pollInterval, "%s did not reach %s on Couchbase Lite; %s", docID, want, c.server.LogTail())
}

// waitForSGVersion waits for a document on Sync Gateway to reach a current version.
func (c *cblTest) waitForSGVersion(docID string, want db.Version) {
	c.t.Helper()
	collection, ctx := c.rt.GetSingleTestDatabaseCollectionWithUser()
	require.EventuallyWithT(c.t, func(ct *assert.CollectT) {
		doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		if !assert.NoError(ct, err) {
			return
		}
		assert.Equal(ct, want, doc.ExtractDocVersion().CV)
	}, waitTime, pollInterval, "%s did not reach %s on Sync Gateway; %s", docID, want, c.server.LogTail())
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
