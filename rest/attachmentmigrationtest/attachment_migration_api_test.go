/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package attachmentmigrationtest

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

func TestAttachmentMigrationAPI(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false, // turn off import feed to stop the feed migrating attachments
		}},
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Perform GET as automatic migration kicks in upon db start
	resp := rt.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var migrationStatus db.AttachmentMigrationManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &migrationStatus)
	require.NoError(t, err)
	// could be running or completed depending on timing of when GET is performed
	require.Contains(t, []db.BackgroundProcessState{db.BackgroundProcessStateRunning, db.BackgroundProcessStateCompleted}, migrationStatus.State)
	assert.Equal(t, int64(0), migrationStatus.DocsChanged)
	assert.Equal(t, int64(0), migrationStatus.DocsProcessed)
	assert.Empty(t, migrationStatus.LastErrorMessage)

	// Wait for run on startup to complete
	_ = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	// add some docs for migration
	numDocs, legacyKeys := addDocsForMigrationProcess(t, ctx, collection, rt.Bucket())

	// Pause migration at the first legacy doc so the duplicate start request below is
	// guaranteed to arrive while the run is genuinely still in progress.
	pauser := newMigrationPauser(rt)
	defer pauser.Close()
	pauser.Pause(legacyKeys[0])

	// kick off migration
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()

	// attempt to kick off again, should error
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)

	pauser.Release()

	// Wait for run to complete
	_ = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	// Perform GET after migration has been ran, ensure it starts in valid 'stopped' state
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	migrationStatus = db.AttachmentMigrationManagerResponse{}
	err = base.JSONUnmarshal(resp.BodyBytes(), &migrationStatus)
	require.NoError(t, err)
	require.Equal(t, db.BackgroundProcessStateCompleted, migrationStatus.State)
	assert.Equal(t, numDocs/2, migrationStatus.DocsChanged)
	// With GSI test bucket pool, a past document might sneak in in the case it was:
	// mutated & deleted but did not pass the snapshot boundary.
	assert.GreaterOrEqual(t, migrationStatus.DocsProcessed, numDocs)
	assert.Empty(t, migrationStatus.LastErrorMessage)
}

func TestAttachmentMigrationAbort(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false, // turn off import feed to stop the feed migrating attachments
		}},
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Wait for run on startup to complete
	_ = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	// Create one doc with legacy attachment metadata — enough for migration to block on.
	docID := t.Name()
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, docID, []byte(`{"value":1234}`), "att", []byte("att body"))

	// Pause migration mid-document so stop arrives while it is genuinely in-flight.
	pauser := newMigrationPauser(rt)
	defer pauser.Close()
	pauser.Pause(docID)

	// start migration
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()

	// stop the migration job
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()

	status := rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateStopped)
	assert.Equal(t, int64(1), status.DocsChanged)
}

func TestAttachmentMigrationReset(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false, // turn off import feed to stop the feed migrating attachments
		}},
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Wait for run on startup to complete
	_ = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	// add some docs for migration
	numDocs, legacyKeys := addDocsForMigrationProcess(t, ctx, collection, rt.Bucket())

	// Pause migration at the first legacy doc so stop arrives while it is genuinely in-flight.
	docID := legacyKeys[0]
	pauser := newMigrationPauser(rt)
	defer pauser.Close()
	pauser.Pause(docID)

	// start migration
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()
	migrationID := rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateRunning).MigrationID

	// Stop migration
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()
	rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateStopped)

	// make sure status is stopped
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var migrationStatus db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &migrationStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, migrationStatus.State)

	// On a real cluster, stopping isn't instant, so any number of legacyKeys may already be
	// migrated by now. Use a fresh doc instead of guessing which legacyKey is still unmigrated.
	resetDocID := base.VBucket0DocIDs(t, rt.Bucket(), 6)[5]
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, resetDocID, []byte(`{"value":1234}`), "att", []byte("att body"))
	numDocs++

	pauser.Pause(resetDocID)

	// reset migration run
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?reset=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()
	status := rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateRunning)
	assert.NotEqual(t, migrationID, status.MigrationID)
	pauser.Release()

	// wait to complete
	status = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)
	// assert all docs are processed again
	assert.GreaterOrEqual(t, status.DocsProcessed, numDocs)
}

func TestAttachmentMigrationMultiNode(t *testing.T) {
	tb := base.GetTestBucket(t)
	noCloseTB := tb.NoCloseClone()

	dbCfg := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		AutoImport: false, // turn off import feed to stop the feed migrating attachments
	}}
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: noCloseTB,
		DatabaseConfig:   dbCfg,
	})
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.LeakyBucketClone(base.LeakyBucketConfig{}),
		DatabaseConfig:   dbCfg,
	})
	defer rt2.Close()
	defer rt1.Close()
	collection, ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

	// Wait for startup run to complete, assert completed status is on both nodes
	_ = rt1.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)
	_ = rt2.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	// Create legacy attachment docs all on vBucket 0. This ensures the DCP worker for vBucket 0
	// processes them sequentially: blocking the first doc keeps the rest queued, so doneChan
	// cannot close before the terminator fires and the select can reliably pick "stopped".
	vb0IDs := base.VBucket0DocIDs(t, rt1.Bucket(), 5)
	for _, id := range vb0IDs {
		rest.CreateLegacyAttachmentDoc(t, ctx, collection, id, []byte(`{}`), "att", []byte("att body"))
	}

	// Pause migration at the first vBucket 0 doc so stop arrives while it is genuinely in-flight.
	// noCloseTB is already a LeakyBucket so its datastore satisfies the LeakyDataStore check.
	pauser := newMigrationPauser(rt1)
	defer pauser.Close()
	pauser.Pause(vb0IDs[0])

	// kick off migration on node 1
	resp := rt1.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()
	status := rt1.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateRunning)
	migrationID := status.MigrationID

	// While node 1's migration is running, attempting to start on node 2 must return 503.
	// This verifies the cluster-aware heartbeat lock blocks concurrent starts across nodes.
	resp = rt2.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)

	// stop migration
	resp = rt1.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()
	_ = rt1.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateStopped)

	// assert that node 2 also has stopped status
	var rt2MigrationStatus db.AttachmentMigrationManagerResponse
	resp = rt2.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err := base.JSONUnmarshal(resp.BodyBytes(), &rt2MigrationStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, rt2MigrationStatus.State)

	// Add a fresh legacy attachment now that the run is genuinely stopped. On a real cluster (and
	// even Rosmar), stopping isn't instant, so the original vBucket 0 docs may already be migrated
	// by this point -- a doc created after Stopped is confirmed is guaranteed unmigrated, giving the
	// resumed run below something to genuinely block on.
	resumeDocID := base.VBucket0DocIDs(t, rt1.Bucket(), 6)[5]
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, resumeDocID, []byte(`{}`), "att", []byte("att body"))

	// Pause again, bound to rt2's own leaky datastore this time -- the resumed run below performs
	// its writes through rt2, so a pauser bound to rt1 wouldn't intercept them.
	pauser2 := newMigrationPauser(rt2)
	defer pauser2.Close()
	pauser2.Pause(resumeDocID)

	// kick off migration run again on node 2. Should resume and have same migration id.
	resp = rt2.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser2.WaitUntilBlocked()
	status = rt2.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateRunning)
	assert.Equal(t, migrationID, status.MigrationID)
	pauser2.Release()

	// Wait for run to be marked as complete on both nodes
	status = rt1.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)
	assert.Equal(t, migrationID, status.MigrationID)
	_ = rt2.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)
}

// addDocsForMigrationProcess creates numDocs docs and converts the first half to the legacy
// attachment format needing migration. Returns the doc count and the legacy docs' keys.
func addDocsForMigrationProcess(t *testing.T, ctx context.Context, collection *db.DatabaseCollectionWithUser, bucket base.Bucket) (int64, []string) {
	numDocs := int64(10)
	legacyCount := numDocs / 2

	keys := make([]string, numDocs)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s_%d", t.Name(), i)
	}
	// Put the legacy docs on vBucket 0 so a single DCP worker processes them serially — required
	// by tests that pause migration on one legacy doc and expect the rest to stay unmigrated
	// until released; otherwise other DCP workers could migrate them concurrently.
	copy(keys, base.VBucket0DocIDs(t, bucket, int(legacyCount)))
	legacyKeys := keys[:legacyCount]

	for _, key := range keys {
		docBody := db.Body{
			"value":            1234,
			db.BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		_, doc, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
		require.Equal(t, db.AttachmentsMeta{
			"myatt": map[string]any{
				"content_type": "text/plain",
				"digest":       "sha1-Lve95gjOVATpfV8EL5X4nxwjKHE=",
				"length":       12,
				"revpos":       1,
				"stub":         true,
				"ver":          2,
			},
		}, doc.Attachments())
		require.Equal(t, db.AttachmentMap{
			"myatt": {
				ContentType: "text/plain",
				Digest:      "sha1-Lve95gjOVATpfV8EL5X4nxwjKHE=",
				Length:      12,
				Revpos:      1,
				Stub:        true,
				Version:     2,
			},
		}, db.GetRawGlobalSyncAttachments(t, collection.GetCollectionDatastore(), key))
		require.Empty(t, db.GetRawSyncXattr(t, collection.GetCollectionDatastore(), key).AttachmentsPre4dot0)
	}

	// Move the legacy subset's attachment metadata from global sync to sync data
	for _, key := range legacyKeys {
		value, _, err := collection.GetCollectionDatastore().GetRaw(ctx, key)
		require.NoError(t, err)

		db.MoveAttachmentXattrFromGlobalToSync(t, collection.GetCollectionDatastore(), key, value, true)
	}
	return numDocs, legacyKeys
}

// migrationPauser blocks attachment migration at a specific document. Can be Paused and
// Released multiple times across a test.
type migrationPauser struct {
	t           testing.TB
	blocked     chan struct{}
	blockCh     chan struct{}
	ds          *base.LeakyDataStore
	callbackSet atomic.Bool
}

func newMigrationPauser(rt *rest.RestTester) *migrationPauser {
	leakyDS, ok := base.AsLeakyDataStore(rt.GetSingleDataStore())
	require.True(rt.TB(), ok, "datastore must be a LeakyDataStore")
	return &migrationPauser{
		t:  rt.TB(),
		ds: leakyDS,
	}
}

// Pause arms the pauser to block migration at docID. Call Release before pausing again.
func (p *migrationPauser) Pause(docID string) {
	if !p.callbackSet.CompareAndSwap(false, true) {
		require.FailNow(p.t, "migrationPauser.Pause called while already paused; call Release first")
	}
	p.blocked = make(chan struct{})
	p.blockCh = make(chan struct{})
	p.ds.SetUpdateXattrsCallback(func(key string) {
		if key != docID {
			return
		}
		close(p.blocked)
		// Runs on the migration job's goroutine, so use the goroutine-safe wait.
		sgtest.RequireChanClosedFromCallback(p.t, p.blockCh)
	})
}

// WaitUntilBlocked blocks until migration is paused at docID.
func (p *migrationPauser) WaitUntilBlocked() {
	p.t.Helper()
	base.RequireChanClosed(p.t, p.blocked)
}

// Release clears the callback and unblocks the paused doc. Fails the test if not currently paused.
func (p *migrationPauser) Release() {
	if !p.release() {
		require.FailNow(p.t, "migrationPauser.Release called while not paused")
	}
}

// Close releases the pauser and resets the LeakyBucket callback.
func (p *migrationPauser) Close() {
	p.release()
}

// release clears the callback and unblocks the paused doc if currently paused, reporting whether
// it was paused.
func (p *migrationPauser) release() bool {
	if !p.callbackSet.CompareAndSwap(true, false) {
		return false
	}
	p.ds.SetUpdateXattrsCallback(nil)
	close(p.blockCh)
	return true
}
