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
		rest.DropAllTestIndexesIncludingPrimary(t, rt.TestBucket)
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
