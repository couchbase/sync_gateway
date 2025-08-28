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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachmentMigrationAPI(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false, // turn off import feed to stop the feed migrating attachments
		}},
	})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Perform GET as automatic migration kicks in upon db start
	resp := rt.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var migrationStatus db.AttachmentMigrationManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &migrationStatus)
	require.NoError(t, err)
	require.Equal(t, db.BackgroundProcessStateRunning, migrationStatus.State)
	assert.Equal(t, int64(0), migrationStatus.DocsChanged)
	assert.Equal(t, int64(0), migrationStatus.DocsProcessed)
	assert.Empty(t, migrationStatus.LastErrorMessage)

	// Wait for run on startup to complete
	_ = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)

	// add some docs for migration
	addDocsForMigrationProcess(t, ctx, collection)

	// kick off migration
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// attempt to kick off again, should error
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)

	// Wait for run to complete
	_ = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)

	// Perform GET after migration has been ran, ensure it starts in valid 'stopped' state
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	migrationStatus = db.AttachmentMigrationManagerResponse{}
	err = base.JSONUnmarshal(resp.BodyBytes(), &migrationStatus)
	require.NoError(t, err)
	require.Equal(t, db.BackgroundProcessStateCompleted, migrationStatus.State)
	assert.Equal(t, int64(5), migrationStatus.DocsChanged)
	// With GSI test bucket pool, a past document might sneak in in the case it was:
	// mutated & deleted but did not pass the snapshot boundary.
	assert.GreaterOrEqual(t, migrationStatus.DocsProcessed, int64(10))
	assert.Empty(t, migrationStatus.LastErrorMessage)
}

func TestAttachmentMigrationAbort(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false, // turn off import feed to stop the feed migrating attachments
		}},
	})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Wait for run on startup to complete
	_ = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)

	// add some docs to arrive over dcp
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		docBody := db.Body{
			"value": 1234,
		}
		_, _, err := collection.Put(ctx, key, docBody)
		require.NoError(t, err)
	}

	// start migration
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// stop the migration job
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	status := rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateStopped)
	assert.Equal(t, int64(0), status.DocsChanged)
}

func TestAttachmentMigrationReset(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false, // turn off import feed to stop the feed migrating attachments
		}},
	})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Wait for run on startup to complete
	_ = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)

	// add some docs for migration
	addDocsForMigrationProcess(t, ctx, collection)

	// start migration
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateRunning)
	migrationID := status.MigrationID

	// Stop migration
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateStopped)

	// make sure status is stopped
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var migrationStatus db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &migrationStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, migrationStatus.State)

	// reset migration run
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?reset=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateRunning)
	assert.NotEqual(t, migrationID, status.MigrationID)

	// wait to complete
	status = rt.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)
	// assert all 10 docs are processed again
	assert.Equal(t, int64(10), status.DocsProcessed)
}

func TestAttachmentMigrationMultiNode(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support DCP client, pending CBG-4249")
	}
	tb := base.GetTestBucket(t)
	noCloseTB := tb.NoCloseClone()

	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: noCloseTB,
	})
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb,
	})
	defer rt2.Close()
	defer rt1.Close()
	collection, ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

	// Wait for startup run to complete, assert completed status is on both nodes
	_ = rt1.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)
	_ = rt2.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)

	// add some docs for migration
	addDocsForMigrationProcess(t, ctx, collection)

	// kick off migration on node 1
	resp := rt1.SendAdminRequest("POST", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt1.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateRunning)
	migrationID := status.MigrationID

	// stop migration
	resp = rt1.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	_ = rt1.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateStopped)

	// assert that node 2 also has stopped status
	var rt2MigrationStatus db.AttachmentMigrationManagerResponse
	resp = rt2.SendAdminRequest("GET", "/{{.db}}/_attachment_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err := base.JSONUnmarshal(resp.BodyBytes(), &rt2MigrationStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, rt2MigrationStatus.State)

	// kick off migration run again on node 2. Should resume and have same migration id
	resp = rt2.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	_ = rt2.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateRunning)

	// assert starting on another node when already running should error
	resp = rt1.SendAdminRequest("POST", "/{{.db}}/_attachment_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)

	// Wait for run to be marked as complete on both nodes
	status = rt1.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)
	assert.Equal(t, migrationID, status.MigrationID)
	_ = rt2.WaitForAttachmentMigrationStatus(t, db.BackgroundProcessStateCompleted)
}

func addDocsForMigrationProcess(t *testing.T, ctx context.Context, collection *db.DatabaseCollectionWithUser) {
	for i := 0; i < 10; i++ {
		docBody := db.Body{
			"value":            1234,
			db.BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
		}
		key := fmt.Sprintf("%s_%d", t.Name(), i)
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

	// Move some subset of the documents attachment metadata from global sync to sync data
	for j := 0; j < 5; j++ {
		key := fmt.Sprintf("%s_%d", t.Name(), j)
		value, _, err := collection.GetCollectionDatastore().GetRaw(key)
		require.NoError(t, err)

		db.MoveAttachmentXattrFromGlobalToSync(t, collection.GetCollectionDatastore(), key, value, true)
	}
}
