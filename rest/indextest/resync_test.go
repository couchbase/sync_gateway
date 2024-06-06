// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func TestResyncWithoutIndexes(t *testing.T) {
	base.TestRequiresDCPResync(t)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true})
	defer rt.Close()

	dbName := "db"

	rest.RequireStatus(t, rt.CreateDatabase(dbName, rt.NewDbConfig()), http.StatusCreated)
	// create test doc to change sequence number
	rt.CreateTestDoc("doc1")

	rt.SyncFn = `function(doc, oldDoc) {channel("A")}`
	rest.RequireStatus(t, rt.UpsertDbConfig(dbName, rt.NewDbConfig()), http.StatusCreated)

	rt.TakeDbOffline()

	if !base.TestsDisableGSI() {
		for _, collection := range rt.GetDatabase().CollectionByID {
			n1qlStore, ok := base.AsN1QLStore(collection.GetCollectionDatastore())
			require.True(t, ok)
			require.NoError(t, base.DropAllIndexes(rt.Context(), n1qlStore))
		}
	}

	rt.TakeDbOffline()
	// gocb pipeline boostrap errors can occur before this stage
	warningsBeforeResync := base.SyncGatewayStats.GlobalStats.ResourceUtilization.WarnCount.Value()
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_resync?action=start", ""), http.StatusOK)
	resyncStatus := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
	require.Equal(t, int64(1), resyncStatus.DocsChanged)
	require.Equal(t, int64(0), base.SyncGatewayStats.GlobalStats.ResourceUtilization.WarnCount.Value()-warningsBeforeResync)

	defaultDataStore, ok := base.AsN1QLStore(rt.Bucket().DefaultDataStore())
	require.True(t, ok)

	ctx := rt.Context()
	if !base.TestsDisableGSI() {
		// the sync docs index)
		numIndexes, err := defaultDataStore.GetIndexes(ctx)
		require.NoError(t, err)
		require.Len(t, numIndexes, 1)

		for _, collection := range rt.GetDatabase().CollectionByID {
			n1qlStore, ok := base.AsN1QLStore(collection.GetCollectionDatastore())
			require.True(t, ok)
			numIndexes, err := n1qlStore.GetIndexes(ctx)
			require.NoError(t, err)
			if collection.IsDefaultCollection() {
				require.Len(t, numIndexes, 1, "Expected 1 index for default collection")
			}
			require.Len(t, numIndexes, 0, "Expected 0 indexes for non-default collection")
		}
	}
}
