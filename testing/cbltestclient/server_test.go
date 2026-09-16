// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ShutdownPool(context.Background())
	os.Exit(code)
}

// testCollection is the collection the smoke tests create.  The default collection is enough here:
// this is exercising the control API, not Sync Gateway's collection handling.
const testCollection = "_default._default"

// requireTestServerDatabase gets the shared test server, registers a database named after the
// test, and resets the server so that database exists and is empty.
//
// Reset deletes every database on the server, so this is also why each test registers and then
// unregisters: two tests that both left a database behind would resurrect each other's.
func requireTestServerDatabase(t *testing.T) (*Server, string) {
	t.Helper()
	server := GetServer(t, "")
	database := safeDatabaseName(t.Name())
	require.NoError(t, server.RegisterDatabases(map[string]DatabaseSpec{
		database: {Collections: []string{testCollection}},
	}))
	t.Cleanup(func() { server.UnregisterDatabases(database) })
	require.NoError(t, server.Reset(base.TestCtx(t), t.Name()))
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

func TestTestServerStartup(t *testing.T) {
	server := GetServer(t, "")
	ctx := base.TestCtx(t)

	info, err := server.Client.ServerInfo(ctx)
	require.NoError(t, err)
	assert.Equal(t, "couchbase-lite-c", info.CBL)
	// Sync Gateway only tests against Enterprise, and only the 4.x clients replicate with version
	// vectors - startServer rejects anything else, so this is checking that check.
	assert.True(t, info.IsEnterprise(), "test server should be an Enterprise Edition build: %q", info.AdditionalInfo)
	assert.Equal(t, 4, info.MajorVersion())
	assert.Equal(t, info.Version, server.Version)
}

func TestTestServerDocumentRoundTrip(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)
	docID := "doc1"
	ref := DocumentRef{Collection: testCollection, ID: docID}

	create, err := ReplaceBodyUpdate(testCollection, docID, nil, map[string]any{"foo": "bar", "count": float64(1)})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{create}))

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

	// Updating replaces the body rather than merging into it, and stays on the same source with a
	// newer version.
	update, err := ReplaceBodyUpdate(testCollection, docID, doc.Properties(), map[string]any{"replaced": true})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{update}))

	updated, err := server.Client.GetDocument(ctx, database, ref)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"replaced": true}, updated.Properties())

	updatedHLV, _, err := updated.HLV()
	require.NoError(t, err)
	assert.Equal(t, sourceID, updatedHLV.SourceID, "the client keeps its own source ID across writes")
	assert.Greater(t, updatedHLV.Version, hlv.Version)
}

func TestTestServerDeletedDocumentIsNotFound(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)
	docID := "doc1"
	ref := DocumentRef{Collection: testCollection, ID: docID}

	create, err := ReplaceBodyUpdate(testCollection, docID, nil, map[string]any{"foo": "bar"})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{create}))
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{DeleteUpdate(testCollection, docID)}))

	// The control API has no way to read a tombstone: GetDocument reports a deleted document the
	// same way it reports one that never existed, so a caller that needs to tell them apart has to
	// use a snapshot.
	_, err = server.Client.GetDocument(ctx, database, ref)
	require.Error(t, err)
	assert.True(t, IsDocumentNotFound(err), "expected a not-found error, got %v", err)

	_, err = server.Client.GetDocument(ctx, database, DocumentRef{Collection: testCollection, ID: "never-existed"})
	require.Error(t, err)
	assert.True(t, IsDocumentNotFound(err), "expected a not-found error, got %v", err)
}

func TestTestServerVerifyDeletedDocument(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)
	docID := "doc1"
	ref := DocumentRef{Collection: testCollection, ID: docID}

	create, err := ReplaceBodyUpdate(testCollection, docID, nil, map[string]any{"foo": "bar"})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{create}))

	snapshot, err := server.Client.SnapshotDocuments(ctx, database, []DocumentRef{ref})
	require.NoError(t, err)

	// Verifying against a snapshot is the only way to assert a document was deleted, since
	// GetDocument cannot see a tombstone.  It answers whether, not which version.
	result, err := server.Client.VerifyDocuments(ctx, database, snapshot, []DatabaseUpdateItem{DeleteUpdate(testCollection, docID)})
	require.NoError(t, err)
	assert.False(t, result.Result, "the document has not been deleted yet: %s", result.Description)

	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{DeleteUpdate(testCollection, docID)}))
	result, err = server.Client.VerifyDocuments(ctx, database, snapshot, []DatabaseUpdateItem{DeleteUpdate(testCollection, docID)})
	require.NoError(t, err)
	assert.True(t, result.Result, "%s", result.Description)
}

func TestTestServerGetAllDocuments(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)

	updates := make([]DatabaseUpdateItem, 0, 2)
	for _, docID := range []string{"doc1", "doc2"} {
		update, err := ReplaceBodyUpdate(testCollection, docID, nil, map[string]any{"id": docID})
		require.NoError(t, err)
		updates = append(updates, update)
	}
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, updates))
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{DeleteUpdate(testCollection, "doc2")}))

	all, err := server.Client.GetAllDocuments(ctx, database, []string{testCollection})
	require.NoError(t, err)
	// Deleted documents are not listed.
	require.Len(t, all[testCollection], 1)
	assert.Equal(t, "doc1", all[testCollection][0].ID)
	assert.NotEmpty(t, all[testCollection][0].Rev)
}

func TestTestServerPurgeLeavesNoTombstone(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)
	docID := "doc1"

	create, err := ReplaceBodyUpdate(testCollection, docID, nil, map[string]any{"foo": "bar"})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{create}))
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{PurgeUpdate(testCollection, docID)}))

	// A purged document can be recreated from scratch, which is what makes purge the right way to
	// clean up a probe document that must not replicate.
	recreate, err := ReplaceBodyUpdate(testCollection, docID, nil, map[string]any{"recreated": true})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{recreate}))

	doc, err := server.Client.GetDocument(ctx, database, DocumentRef{Collection: testCollection, ID: docID})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"recreated": true}, doc.Properties())
}

func TestTestServerResetClearsDatabases(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)

	create, err := ReplaceBodyUpdate(testCollection, "doc1", nil, map[string]any{"foo": "bar"})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{create}))

	require.NoError(t, server.Reset(ctx, t.Name()))

	all, err := server.Client.GetAllDocuments(ctx, database, []string{testCollection})
	require.NoError(t, err)
	assert.Empty(t, all[testCollection], "reset recreates the registered databases empty")
}

func TestTestServerRunQuery(t *testing.T) {
	server, database := requireTestServerDatabase(t)
	ctx := base.TestCtx(t)

	create, err := ReplaceBodyUpdate(testCollection, "doc1", nil, map[string]any{"foo": "bar"})
	require.NoError(t, err)
	require.NoError(t, server.Client.UpdateDatabase(ctx, database, []DatabaseUpdateItem{create}))

	results, err := server.Client.RunQuery(ctx, database, "SELECT meta().id FROM _default")
	require.NoError(t, err)
	assert.NotNil(t, results)
}

func TestTestServerStopKillsTheProcess(t *testing.T) {
	// Goes around the pool so the process can be stopped without taking the shared server down
	// with it, which every other test in this package is using.
	binary, err := ResolveBinary(DefaultVersion())
	if err != nil {
		requireOrSkipTestServer(t, err)
	}
	if !binary.SupportsPortFlag {
		t.Skip("Test server has a compiled-in port, so a second one cannot be started alongside the pooled server")
	}
	ctx := base.TestCtx(t)

	server, err := startServer(ctx, binary)
	require.NoError(t, err)
	filesDir := server.filesDir

	server.stop(ctx)
	assert.True(t, server.hasExited(), "the process should have been reaped by the time stop returns")
	assert.NoDirExists(t, filesDir, "the data directory should be removed with the server")

	// Stopping twice is normal: the pool stops every server it started, and a caller may already
	// have stopped one.
	server.stop(ctx)
}
