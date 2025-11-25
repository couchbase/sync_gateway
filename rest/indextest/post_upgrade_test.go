// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func TestPostUpgrade(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar doesn't support N1QLStore")
	}
	base.RequireNumTestDataStores(t, 2)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	defaultDataStore, ok := bucket.DefaultDataStore().(base.N1QLStore)
	require.True(t, ok, "Default data store should be N1QLDataStore")
	// create legacy syncDocs index + all non metadata indexes
	require.NoError(t, db.InitializeIndexes(ctx, defaultDataStore, db.InitializeIndexOptions{
		NumReplicas:         0,
		UseXattrs:           base.TestUseXattrs(),
		NumPartitions:       db.DefaultNumIndexPartitions,
		LegacySyncDocsIndex: true,
		MetadataIndexes:     db.IndexesAll,
	}))
	// create non SG indexes to make sure they aren't removed by post upgrade
	require.NoError(t, defaultDataStore.CreatePrimaryIndex(ctx, "sg_primary", &base.N1qlIndexOptions{}))
	require.NoError(t, defaultDataStore.CreateIndex(ctx, "sg_nonSGIndex", "val", "val > 3", &base.N1qlIndexOptions{}))

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: bucket.NoCloseClone(),
	})
	defer rt.Close()

	const (
		db1Name = "db1"
		db2Name = "db2"
		db3Name = "db3"
	)
	// create a database with the default collection
	db1Config := rt.NewDbConfig()
	db1Config.Scopes = nil
	rest.RequireStatus(t, rt.CreateDatabase(db1Name, db1Config), http.StatusCreated)
	db1 := rt.ServerContext().AllDatabases()[db1Name]
	require.True(t, db1.UseLegacySyncDocsIndex())

	// create sg_users_x1 and sg_roles_x1 indexes
	require.NoError(t, db.InitializeIndexes(ctx, defaultDataStore, db.InitializeIndexOptions{
		NumReplicas:         0,
		UseXattrs:           base.TestUseXattrs(),
		NumPartitions:       db.DefaultNumIndexPartitions,
		LegacySyncDocsIndex: false,
		MetadataIndexes:     db.IndexesMetadataOnly,
	}))

	// create two databases with the same scope but different collections
	ds2Name := bucket.GetNonDefaultDatastoreNames()[0]
	db2Config := rt.NewDbConfig()
	db2Config.Scopes = rest.ScopesConfig{
		ds2Name.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				ds2Name.CollectionName(): {},
			},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(db2Name, db2Config), http.StatusCreated)
	db2 := rt.ServerContext().AllDatabases()[db2Name]
	require.False(t, db2.UseLegacySyncDocsIndex())

	ds3Name := bucket.GetNonDefaultDatastoreNames()[1]
	db3Config := rt.NewDbConfig()
	db3Config.Scopes = rest.ScopesConfig{
		ds2Name.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				ds3Name.CollectionName(): {},
			},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(db3Name, db3Config), http.StatusCreated)
	db3 := rt.ServerContext().AllDatabases()[db3Name]
	require.False(t, db3.UseLegacySyncDocsIndex())

	t.Run("no-op post-upgrade", func(t *testing.T) {
		for _, preview := range []bool{true, false} {
			t.Run("preview="+strconv.FormatBool(preview), func(t *testing.T) {
				url := "/_post_upgrade"
				if preview {
					url += "?preview=true"
				}
				resp := rt.SendAdminRequest(http.MethodPost, url, "")
				rest.RequireStatus(t, resp, http.StatusOK)
				var output rest.PostUpgradeResponse
				require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &output))
			})
		}
	})

	// delete the database with the default collection, which will allow sg_syncDocs_x1 and all non-metadata indexes from default collection to be removed
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/"+db1Name+"/", ""), http.StatusOK)
	for _, preview := range []bool{true, false} {
		t.Run("preview="+strconv.FormatBool(preview), func(t *testing.T) {
			url := "/_post_upgrade"
			if preview {
				url += "?preview=true"
			}
			resp := rt.SendAdminRequest(http.MethodPost, url, "")
			rest.RequireStatus(t, resp, http.StatusOK)
			var output rest.PostUpgradeResponse
			require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &output))
			// unused indexes created by InitializeIndexes, like a database that was targeting a default collection
			expectedRemovedIndexes := []string{
				"`_default`.`_default`.sg_access_x1",
				"`_default`.`_default`.sg_allDocs_x1",
				"`_default`.`_default`.sg_channels_x1",
				"`_default`.`_default`.sg_roleAccess_x1",
				"`_default`.`_default`.sg_syncDocs_x1",
				"`_default`.`_default`.sg_tombstones_x1",
			}
			require.Equal(t, rest.PostUpgradeResponse{
				Result: rest.PostUpgradeResult{
					db2Name: rest.PostUpgradeDatabaseResult{
						RemovedDDocs:   []string{},
						RemovedIndexes: expectedRemovedIndexes,
					},
					db3Name: rest.PostUpgradeDatabaseResult{
						RemovedDDocs:   []string{},
						RemovedIndexes: expectedRemovedIndexes,
					},
				},
				Preview: preview,
			}, output)

		})
	}
}
