// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// testCollection is the collection the smoke tests create.  The default collection is enough here:
// this is exercising the control API, not Sync Gateway's collection handling.
const testCollection = "_default._default"

// requireTestServerDatabase gets the shared test server and resets it so that it holds one empty
// database named after the test.
func requireTestServerDatabase(t *testing.T) (*Server, string) {
	t.Helper()
	server := GetServer(t)
	database := safeDatabaseName(t.Name())
	require.NoError(t, server.Client.Reset(base.TestCtx(t), t.Name(), map[string]DatabaseSpec{
		database: {Collections: []string{testCollection}},
	}))
	return server, database
}

// safeDatabaseName turns a test name into something usable as a Couchbase Lite database name,
// which ends up as a directory name on the test server.
func safeDatabaseName(name string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, name)
}

// putDocument writes properties to a document, creating it if it does not exist.
func putDocument(t *testing.T, server *Server, database, docID string, properties map[string]any) {
	t.Helper()
	require.NoError(t, server.Client.UpdateDatabase(base.TestCtx(t), database, []DatabaseUpdateItem{{
		Type:              UpdateTypeUpdate,
		Collection:        testCollection,
		DocumentID:        docID,
		UpdatedProperties: []map[string]any{properties},
	}}))
}

// deleteDocument deletes a document, leaving a tombstone.
func deleteDocument(t *testing.T, server *Server, database, docID string) {
	t.Helper()
	require.NoError(t, server.Client.UpdateDatabase(base.TestCtx(t), database, []DatabaseUpdateItem{{
		Type:       UpdateTypeDelete,
		Collection: testCollection,
		DocumentID: docID,
	}}))
}

func TestTestServerStartup(t *testing.T) {
	server := GetServer(t)
	ctx := base.TestCtx(t)

	info, err := server.Client.ServerInfo(ctx)
	require.NoError(t, err)
	assert.Equal(t, "couchbase-lite-c", info.CBL)
	assert.Equal(t, info.Version, server.Version)
}

func TestTestServerDocumentRoundTrip(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)
	docID := "doc1"
	ref := DocumentRef{Collection: testCollection, ID: docID}

	putDocument(t, server, database, docID, map[string]any{"foo": "bar", "count": float64(1)})

	doc, err := server.Client.GetDocument(ctx, database, ref)
	require.NoError(t, err)
	assert.Equal(t, docID, doc.ID())
	assert.Equal(t, map[string]any{"foo": "bar", "count": float64(1)}, doc.Properties())

	// A real 4.x client writes a version vector Sync Gateway's own parser understands.  If this
	// ever fails, Couchbase Lite and Sync Gateway have diverged on the format, which is exactly
	// the sort of thing a real client is here to catch.
	hlv, legacyRevs, err := doc.HLV()
	require.NoError(t, err)
	assert.Empty(t, legacyRevs, "a 4.x client should produce no legacy revtree IDs")
	sourceID := hlv.SourceID
	assert.NotEmpty(t, sourceID)
	assert.NotEmpty(t, hlv.Version)
	assert.Empty(t, hlv.PreviousVersions, "a document written once has no previous versions")

	// A second write stays on the same source with a newer version.
	putDocument(t, server, database, docID, map[string]any{"count": float64(2)})

	updated, err := server.Client.GetDocument(ctx, database, ref)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar", "count": float64(2)}, updated.Properties())

	updatedHLV, _, err := updated.HLV()
	require.NoError(t, err)
	assert.Equal(t, sourceID, updatedHLV.SourceID, "the client keeps its own source ID across writes")
	assert.Greater(t, updatedHLV.Version, hlv.Version)
}

func TestTestServerDeletedDocumentIsNotFound(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)
	docID := "doc1"

	putDocument(t, server, database, docID, map[string]any{"foo": "bar"})
	deleteDocument(t, server, database, docID)

	// GetDocument reports a deleted document the same way it reports one that never existed.
	_, err := server.Client.GetDocument(ctx, database, DocumentRef{Collection: testCollection, ID: docID})
	require.Error(t, err)
	assert.True(t, IsDocumentNotFound(err), "expected a not-found error, got %v", err)

	_, err = server.Client.GetDocument(ctx, database, DocumentRef{Collection: testCollection, ID: "never-existed"})
	require.Error(t, err)
	assert.True(t, IsDocumentNotFound(err), "expected a not-found error, got %v", err)
}

func TestTestServerGetAllDocuments(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)

	putDocument(t, server, database, "doc1", map[string]any{"id": "doc1"})
	putDocument(t, server, database, "doc2", map[string]any{"id": "doc2"})
	deleteDocument(t, server, database, "doc2")

	all, err := server.Client.GetAllDocuments(ctx, database, []string{testCollection})
	require.NoError(t, err)
	// Deleted documents are not listed.
	require.Len(t, all[testCollection], 1)
	assert.Equal(t, "doc1", all[testCollection][0].ID)
	assert.NotEmpty(t, all[testCollection][0].Rev)
}

func TestTestServerResetClearsDatabases(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)

	putDocument(t, server, database, "doc1", map[string]any{"foo": "bar"})

	require.NoError(t, server.Client.Reset(ctx, t.Name(), map[string]DatabaseSpec{
		database: {Collections: []string{testCollection}},
	}))

	all, err := server.Client.GetAllDocuments(ctx, database, []string{testCollection})
	require.NoError(t, err)
	assert.Empty(t, all[testCollection], "reset recreates the databases empty")
}

func TestTestServerReplicatorLifecycle(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)

	// Nothing is listening on port 1, so the replicator connects, fails, and sits OFFLINE.  That is
	// enough to exercise start, status and stop without needing a Sync Gateway: what is under test
	// here is the control API, not replication itself.
	replicatorID, err := server.Client.StartReplicator(ctx, ReplicatorConfig{
		Database:       database,
		Collections:    []ReplicationCollection{{Names: []string{testCollection}}},
		Endpoint:       "ws://127.0.0.1:1/unreachable",
		ReplicatorType: ReplicatorTypePushAndPull,
		Continuous:     true,
	}, false)
	require.NoError(t, err)
	require.NotEmpty(t, replicatorID)

	status, err := server.Client.ReplicatorStatus(ctx, replicatorID)
	require.NoError(t, err)
	assert.NotEqual(t, ReplicatorActivityStopped, status.Activity)

	require.NoError(t, server.Client.StopReplicator(ctx, replicatorID))

	// Stopping is asynchronous: the request is accepted, and the replicator reaches STOPPED after.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, err := server.Client.ReplicatorStatus(ctx, replicatorID)
		assert.NoError(c, err)
		assert.Equal(c, ReplicatorActivityStopped, status.Activity)
	}, 30*time.Second, 50*time.Millisecond, "replicator did not stop")
}

func TestTestServerStopUnknownReplicator(t *testing.T) {
	server, _ := requireTestServerDatabase(t)
	err := server.Client.StopReplicator(base.TestCtx(t), "no-such-replicator")
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Message, "Replicator Not Found")
}
