// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChangeIndexPartitions ensures that users can change the number of partitions without any downtime using the documented steps.
//  1. Have a database already running with an undesired number of partitions.
//  2. Use Index Init API to initialize a new set of indexes with different partitions.
//  3. Use the regular db config API to change the number of partitions to match the set created.
//  4. Ensure the database starts up using the indexes created in step 2 without any delay.
//  5. Use the _post_upgrade endpoint to clean up the old set of indexes.
func TestChangeIndexPartitions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}
	if !base.TestUseXattrs() {
		t.Skip("To simplify testing to allow for exact string matching on indexes, skip for xattrs")
	}

	// requires index init for many subtests

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery, base.KeyHTTP)

	const (
		dbName = "db"
	)

	tests := []struct {
		name                string
		usePersistentConfig bool
		initialPartitions   *uint32
		newPartitions       uint32
		removedIndexes      []string
	}{
		{
			name:                "non-persistent config 2 to 4",
			usePersistentConfig: false,
			initialPartitions:   base.Ptr(uint32(2)),
			newPartitions:       4,
			removedIndexes: []string{
				"sg_allDocs_x1_p2",
				"sg_channels_x1_p2",
			},
		},
		{
			name:                "persistent config 2 to 4",
			usePersistentConfig: true,
			initialPartitions:   base.Ptr(uint32(2)),
			newPartitions:       4,
			removedIndexes: []string{
				"sg_allDocs_x1_p2",
				"sg_channels_x1_p2",
			},
		},
		{
			name:              "nil to 4",
			initialPartitions: nil,
			newPartitions:     4,
			removedIndexes: []string{
				"sg_allDocs_x1",
				"sg_channels_x1",
			},
		},
		{
			name:              "1 to 4",
			initialPartitions: base.Ptr(uint32(1)),
			newPartitions:     4,
			removedIndexes: []string{
				"sg_allDocs_x1",
				"sg_channels_x1",
			},
		},
		{
			name:              "4 to 1",
			initialPartitions: base.Ptr(uint32(4)),
			newPartitions:     1,
			removedIndexes: []string{
				"sg_allDocs_x1_p4",
				"sg_channels_x1_p4",
			},
		},
		{
			name:              "2 to 2",
			initialPartitions: base.Ptr(uint32(2)),
			newPartitions:     2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				rt       *rest.RestTester
				dbConfig rest.DbConfig
			)
			if test.usePersistentConfig {
				rt = rest.NewRestTesterPersistentConfigNoDB(t)
				dbConfig = rt.NewDbConfig()
				dbConfig.Index.NumPartitions = test.initialPartitions
				rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)
			} else {
				rt = rest.NewRestTester(t, &rest.RestTesterConfig{
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						Index: &rest.IndexConfig{
							NumPartitions: test.initialPartitions,
						},
					}},
				})
				_ = rt.Bucket() // init db
				dbConfig = rt.DatabaseConfig.DbConfig
			}
			defer rt.Close()
			database := rt.GetDatabase()
			assertNumSGIndexPartitions(t, database)
			require.Equal(t, test.initialPartitions, rt.GetDatabase().Options.NumIndexPartitions)

			// init new indexes with different partitions
			resp := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/%s/_index_init", dbName), fmt.Sprintf(`{"num_partitions":%d}`, test.newPartitions))
			rest.RequireStatus(t, resp, http.StatusOK)

			// wait for indexes to be ready using init api
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				resp := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_index_init", dbName), "")
				rest.AssertStatus(t, resp, http.StatusOK)
				var body db.AsyncIndexInitManagerResponse
				err := base.JSONUnmarshal(resp.BodyBytes(), &body)
				require.NoError(c, err)
				require.Empty(c, body.LastErrorMessage)
				require.Equal(c, db.BackgroundProcessStateCompleted, body.State)
				require.GreaterOrEqual(c, len(body.IndexStatus), 1, "expected at least one scope (maybe two if `_default` and a named scope)")
				require.LessOrEqual(c, len(body.IndexStatus), 2, "expected at most two scopes (maybe one if `_default` only)")
				for _, collections := range body.IndexStatus {
					for _, status := range collections {
						assert.Equal(c, db.CollectionIndexStatusReady, status)
					}
				}
			}, 1*time.Minute, 1*time.Second)
			assertNumSGIndexPartitions(t, database)

			// update db config with new partition count - shouldn't create indexes at this point since they already exist
			if dbConfig.Index == nil {
				dbConfig.Index = &rest.IndexConfig{}
			}
			dbConfig.Index.NumPartitions = base.Ptr(test.newPartitions)
			rest.RequireStatus(t, rt.ReplaceDbConfig(dbName, dbConfig), http.StatusCreated)
			require.Equal(t, test.newPartitions, *rt.GetDatabase().Options.NumIndexPartitions)

			// CBG-4565 cleanup old indexes
			resp = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
			rest.RequireStatus(t, resp, http.StatusOK)
			var body rest.PostUpgradeResponse
			err := base.JSONUnmarshal(resp.BodyBytes(), &body)
			collection := db.GetSingleDatabaseCollection(t, database)
			var removedIndexes []string
			for _, index := range test.removedIndexes {
				removedIndexes = append(removedIndexes, fmt.Sprintf("`%s`.`%s`.%s", collection.ScopeName, collection.Name, index))
			}

			expectedOutput := rest.PostUpgradeResponse{
				Result: rest.PostUpgradeResult{
					"db": rest.PostUpgradeDatabaseResult{
						RemovedDDocs:   []string{},
						RemovedIndexes: removedIndexes,
					},
				},
			}
			require.NoError(t, err)
			require.Equal(t, expectedOutput, body)
		})
	}
}

// requireNumSGIndexPartitions ensures that the number of partitions for SG indexes is as expected. Some indexes aren't partitioned.
func assertNumSGIndexPartitions(t testing.TB, database *db.DatabaseContext) {
	gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
	require.NoError(t, err)
	re := regexp.MustCompile(`sg_(?:allDocs|channels)_x1(?:_p(\d+))?$`)
	for _, dsName := range []sgbucket.DataStoreName{db.GetSingleDatabaseCollection(t, database).GetCollectionDatastore(), database.MetadataStore} {
		allIndexes, err := gocbBucket.GetCluster().Bucket(gocbBucket.BucketName()).Scope(dsName.ScopeName()).Collection(dsName.CollectionName()).QueryIndexes().GetAllIndexes(nil)
		require.NoError(t, err)
		require.Greaterf(t, len(allIndexes), 0, "expected at least one index for datastore %s", dsName)
		for _, index := range allIndexes {
			// only two SG indexes are partitioned currently
			partitionsFromNameStrs := re.FindStringSubmatch(index.Name)
			if len(partitionsFromNameStrs) > 1 && partitionsFromNameStrs[1] != "" {
				expectedPartitionsFromName, err := strconv.ParseInt(partitionsFromNameStrs[1], 10, 64)
				require.NoError(t, err)
				require.Greaterf(t, int(expectedPartitionsFromName), 1, "expected at least one partition for %s", index.Name)
				assert.NotEqualf(t, "", index.Partition, "expected partition clause for %s", index.Name)
				assert.Equal(t, int(expectedPartitionsFromName), int(db.GetIndexPartitionCount(t, gocbBucket, dsName, index.Name)))
			} else {
				assert.Equal(t, "", index.Partition)
				assert.True(t, strings.HasSuffix(index.Name, "_x1"), "expected no partitions for %+v", index)
				assert.Equal(t, 1, int(db.GetIndexPartitionCount(t, gocbBucket, dsName, index.Name)))
			}
		}
	}
}

func TestChangeIndexPartitionsErrors(t *testing.T) {
	tests := []struct {
		name          string
		action        string
		body          string
		expectedError string
	}{
		{
			name:          "invalid action",
			action:        "invalid",
			body:          `{"num_partitions":2}`,
			expectedError: `action "invalid" not supported... must be either 'start' or 'stop'`,
		},
		{
			name:          "empty num_partitions",
			body:          `{}`,
			expectedError: `at least one of num_partitions or create_separate_principal_indexes is required`,
		},
		{
			name:          "invalid num_partitions",
			body:          `{"num_partitions":0}`,
			expectedError: `num_partitions must be greater than 0`,
		},
		{
			name:          "bad json",
			body:          `{num_partitions:2}`,
			expectedError: `Bad JSON`,
		},
		{
			name:          "wrong format valid json",
			body:          `2`,
			expectedError: `Bad JSON`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := rest.NewRestTester(t, nil)
			defer rt.Close()

			resp := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/{{.db}}/_index_init?action=%s", test.action), test.body)
			var httpError struct {
				Reason string `json:"reason"`
			}
			err := base.JSONUnmarshal(resp.BodyBytes(), &httpError)
			require.NoError(t, err, "Failed to unmarshal HTTP error: %v", resp.BodyBytes())
			rest.AssertStatus(t, resp, http.StatusBadRequest)
			assert.Contains(t, httpError.Reason, test.expectedError)
		})
	}
}

func TestChangeIndexPartitionsDbOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.StartOffline = base.Ptr(true)
	rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	rt.ServerContext().DatabaseInitManager.SetInitializeIndexesFunc(t, getNoopInitializeIndexes())

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":2}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	db.RequireBackgroundManagerState(t, rt.GetDatabase().AsyncIndexInitManager, db.BackgroundProcessStateCompleted)
}

func TestChangeIndexPartitionsStartStopAndRestart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}

	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	rt.ServerContext().DatabaseInitManager.SetInitializeIndexesFunc(t, getNoopBlockingInitializeIndexes())
	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":2}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// wait for stopped state
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_index_init", "")
		rest.AssertStatus(t, resp, http.StatusOK)
		var body db.AsyncIndexInitManagerResponse
		err := base.JSONUnmarshal(resp.BodyBytes(), &body)
		require.NoError(c, err)
		assert.Equal(c, db.BackgroundProcessStateStopped, body.State, "body: %#+v", body)
		require.Equal(c, "", body.LastErrorMessage, "expected no error when stopping, got: %s", body.LastErrorMessage)
	}, 1*time.Minute, 1*time.Second)
	rt.ServerContext().DatabaseInitManager.SetInitializeIndexesFunc(t, getNoopInitializeIndexes())

	// restart with new params
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":3}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	db.RequireBackgroundManagerState(t, rt.GetDatabase().AsyncIndexInitManager, db.BackgroundProcessStateCompleted)
}

func TestChangeIndexPartitionsWithViews(t *testing.T) {
	base.TestRequiresViews(t)
	// force views - doesn't matter what mode test framework is in since we're not actually using any
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{UseViews: base.Ptr(true)}}})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":2}`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	rest.AssertHTTPErrorReason(t, resp, http.StatusBadRequest, "_index_init is a GSI-only feature and is not supported when using views")
}

func TestChangeIndexSeparatePrincipalIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}

	// requires index init

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	n1qlStore, ok := base.AsN1QLStore(rt.Bucket().DefaultDataStore())
	require.True(t, ok)

	ctx := base.TestCtx(t)
	require.NoError(t, db.InitializeIndexes(ctx, n1qlStore,
		db.InitializeIndexOptions{
			NumReplicas:         0,
			LegacySyncDocsIndex: true, // force use of legacy sync docs index
			UseXattrs:           base.TestUseXattrs(),
			MetadataIndexes:     db.IndexesMetadataOnly,
			NumPartitions:       1,
		}))

	indexNameSuffix := "_1"
	if base.TestUseXattrs() {
		indexNameSuffix = "_x1"
	}
	userIdx := "sg_users" + indexNameSuffix
	roleIdx := "sg_roles" + indexNameSuffix
	syncDocsIdx := "sg_syncDocs" + indexNameSuffix

	// prior to database creation, there should be only syncDocs
	indexes, err := n1qlStore.GetIndexes()
	require.NoError(t, err)
	require.Contains(t, indexes, syncDocsIdx)
	require.NotContains(t, indexes, userIdx)
	require.NotContains(t, indexes, roleIdx)

	rest.RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	// ensure we have legacy sync docs index
	require.True(t, rt.GetDatabase().UseLegacySyncDocsIndex())

	// after database creation, there should be only syncDocs
	indexes, err = n1qlStore.GetIndexes()
	require.NoError(t, err)
	require.Contains(t, indexes, syncDocsIdx)
	require.NotContains(t, indexes, userIdx)
	require.NotContains(t, indexes, roleIdx)

	// this call should not create new indexes
	runIndexInit(rt, `{"create_separate_principal_indexes":false}`)
	indexes, err = n1qlStore.GetIndexes()
	require.NoError(t, err)
	require.NotContains(t, indexes, userIdx)
	require.NotContains(t, indexes, roleIdx)
	require.Contains(t, indexes, syncDocsIdx)

	// this should create new indexes
	runIndexInit(rt, `{"create_separate_principal_indexes":true}`)
	indexes, err = n1qlStore.GetIndexes()
	require.NoError(t, err)
	require.Contains(t, indexes, syncDocsIdx)
	require.Contains(t, indexes, userIdx)
	require.Contains(t, indexes, roleIdx)

	rt.TakeDbOffline()
	rt.TakeDbOnline()
	require.False(t, rt.GetDatabase().UseLegacySyncDocsIndex())

	// CBG-4608 cleanup old indexes
	//resp = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	//rest.RequireStatus(t, resp, http.StatusOK)
	//var body rest.PostUpgradeResponse
	//err := base.JSONUnmarshal(resp.BodyBytes(), &body)
	//require.NoError(t, err)
	//require.Lenf(t, body.Result, 1, "expected one database in post upgrade response")
	//require.Lenf(t, body.Result[dbName].RemovedIndexes, 1, "expected one syncDocs index to be removed")

	//indexes, err = n1qlStore.GetIndexes()
	//require.NoError(t, err)
	//require.NotContains(t, indexes, syncDocsIdx)
	//require.Contains(t, indexes, userIdx)
	//require.Contains(t, indexes, roleIdx)
}

// runIndexInit is a helper function to run the index init process.
func runIndexInit(rt *rest.RestTester, body string) {
	t := rt.TB()
	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", body)
	rest.RequireStatus(t, resp, http.StatusOK)

	db.RequireBackgroundManagerState(t, rt.GetDatabase().AsyncIndexInitManager, db.BackgroundProcessStateCompleted)
}

// getNoopBlockingInitializeIndexes is a replacement initialize index function that blocks forever. It is cancelled when DatabaseInitManager.Cancel is called. Used to avoid churn on n1ql nodes.
func getNoopBlockingInitializeIndexes() rest.InitializeIndexesFunc {
	return func(ctx context.Context, _ base.N1QLStore, _ db.InitializeIndexOptions) error {
		<-ctx.Done()
		return nil
	}
}

// getNoopInitializeIndexes is a replacement initialize index function that returns immediately. Used to avoid churn on
// on n1ql nodes.
func getNoopInitializeIndexes() rest.InitializeIndexesFunc {
	return func(context.Context, base.N1QLStore, db.InitializeIndexOptions) error {
		return nil
	}
}
