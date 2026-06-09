// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResyncWithoutIndexes(t *testing.T) {
	var createdIndexes []string
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		LeakyBucketConfig: &base.LeakyBucketConfig{
			CreateIndexIfNotExistsCallback: func(indexName string) {
				createdIndexes = append(createdIndexes, indexName)
			},
		},
	})
	defer rt.Close()

	dbName := "db"

	// CBG-4615: parametrize test to use legacy sync docs index, or users and roles indexes
	rest.RequireStatus(t, rt.CreateDatabase(dbName, rt.NewDbConfig()), http.StatusCreated)
	// create test doc to change sequence number
	rt.CreateTestDoc("doc1")

	rt.SyncFn = `function(doc, oldDoc) {channel("A")}`
	config := rt.NewDbConfig()
	config.StartOffline = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, config), http.StatusCreated)
	rt.WaitForDBInitializationCompleted(dbName)

	if !base.TestsDisableGSI() {
		base.DropAllBucketIndexes(t, rt.TestBucket)
	}

	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_resync?action=start", ""), http.StatusOK)

	resyncStatus := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	require.Equal(t, int64(1), resyncStatus.DocsChanged)

	if base.TestsDisableGSI() {
		return
	}
	if rt.GetDatabase().UseLegacySyncDocsIndex() {
		require.ElementsMatch(t, []string{"sg_syncDocs_x1"}, createdIndexes)
	} else {
		require.ElementsMatch(t, []string{"sg_users_x1", "sg_roles_x1"}, createdIndexes)
	}

}

// TestResyncCrossNode verifies that two nodes sharing the same bucket both transition to offline
// when one node takes the database offline (via persistent config), and that resync started on
// one node completes and is visible to both nodes.
func TestResyncCrossNode(t *testing.T) {
	db.UpdateDatabaseStatePolling(t, 100*time.Millisecond)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	}

	rt1 := rest.NewRestTester(t, rtConfig)
	defer rt1.Close()

	rt2 := rest.NewRestTester(t, rtConfig)
	defer rt2.Close()

	const dbName = "db"

	// Create database on node 1
	dbConfig := rt1.NewDbConfig()
	rest.RequireStatus(t, rt1.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// Node 2 discovers database from the shared bucket
	rt2.ServerContext().ForceDbConfigsReload(t, ctx)
	rt2.WaitForDatabaseState(dbName, db.RunStateString[db.DBOnline])

	// Seed 100 documents via node 1
	const numDocs = 100
	for i := range numDocs {
		rt1.CreateTestDoc(fmt.Sprintf("doc%d", i))
	}

	// Take node 1 offline — persists StartOffline=true to the shared bucket
	rt1.TakeDbOffline()

	// Node 2 picks up offline state from the shared bucket via config reload
	rt2.ServerContext().ForceDbConfigsReload(t, ctx)
	rt2.WaitForDatabaseState(dbName, db.RunStateString[db.DBOffline])

	// Start resync on node 1
	resp := rt1.SendAdminRequest(http.MethodPost, "/{{.db}}/_resync?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Both nodes observe the completed resync status via the shared bucket
	status := rt1.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateCompleted, dbName)
	require.GreaterOrEqual(t, status.DocsProcessed, int64(numDocs))

	// rt1 local stats: processed ≥ numDocs, nothing changed (same sync fn),
	// DocsTargeted resets to 0 on completion, no errors
	rt1DB := rt1.GetDatabase()
	assert.GreaterOrEqual(t, rt1DB.DbStats.Database().ResyncNumProcessed.Value(), int64(numDocs))
	assert.Equal(t, int64(0), rt1DB.DbStats.Database().ResyncNumChanged.Value())
	assert.Equal(t, int64(0), rt1DB.DbStats.Database().ResyncDocsTargeted.Value()) // reset to 0 after completion
	assert.Equal(t, int64(0), rt1DB.DbStats.Database().ResyncErrorsTotal.Value())

	rt2.WaitForResyncDCPStatusForDB(db.BackgroundProcessStateCompleted, dbName)

	// rt2 local stats: 0 in non-distributed mode (rt2 processed no vbuckets);
	// in distributed mode rt2 would have non-zero ResyncNumProcessed
	rt2DB := rt2.GetDatabase()
	assert.Equal(t, int64(0), rt2DB.DbStats.Database().ResyncNumChanged.Value())
	assert.Equal(t, int64(0), rt2DB.DbStats.Database().ResyncDocsTargeted.Value())
	assert.Equal(t, int64(0), rt2DB.DbStats.Database().ResyncErrorsTotal.Value())
}
