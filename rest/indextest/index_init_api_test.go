// Copyright 2025-Present Couchbase, Inc.
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

	// requires index init for many subtests
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery, base.KeyHTTP)

	const (
		dbName = "db"
	)

	tests := []struct {
		name                string
		usePersistentConfig bool
		initialPartitions   *uint32
		newPartitions       uint32
	}{
		{
			name:                "non-persistent config 2 to 4",
			usePersistentConfig: false,
			initialPartitions:   base.Ptr(uint32(2)),
			newPartitions:       4,
		},
		{
			name:                "persistent config 2 to 4",
			usePersistentConfig: true,
			initialPartitions:   base.Ptr(uint32(2)),
			newPartitions:       4,
		},
		{
			name:              "nil to 4",
			initialPartitions: nil,
			newPartitions:     4,
		},
		{
			name:              "1 to 4",
			initialPartitions: base.Ptr(uint32(1)),
			newPartitions:     4,
		},
		{
			name:              "4 to 1",
			initialPartitions: base.Ptr(uint32(4)),
			newPartitions:     1,
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

			// CBG-4565 cleanup old indexes
			//resp = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
			//rest.RequireStatus(t, resp, http.StatusOK)
			//var body rest.PostUpgradeResponse
			//err := base.JSONUnmarshal(resp.BodyBytes(), &body)
			//require.NoError(t, err)
			//require.Lenf(t, body.Result, 1, "expected one database in post upgrade response")
			//require.Lenf(t, body.Result[dbName].RemovedIndexes, 2, "expected two indexes to be removed")
		})
	}
}

// assertNumSGIndexPartitions ensures that the number of partitions for SG indexes is as expected. Some indexes aren't partitioned.
func assertNumSGIndexPartitions(t testing.TB, database *db.DatabaseContext) {
	gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
	require.NoError(t, err)
	re, err := regexp.Compile(`sg_(?:allDocs|channels)_x1(?:_p(\d+))?$`)
	require.NoError(t, err)
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
			expectedError: `at least one of num_partitions or separate_principal_indexes is required`,
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

func TestChangeIndexPartitionsSameNumber(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}

	// requires index init
	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{Index: &rest.IndexConfig{NumPartitions: base.Ptr(uint32(2))}}}})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":2}`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	rest.AssertHTTPErrorReason(t, resp, http.StatusBadRequest, "num_partitions is already 2")
}

func TestChangeIndexPartitionsDbOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}

	// requires index init
	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rt.TakeDbOffline()

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":2}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	// wait for completion
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_index_init", "")
		rest.AssertStatus(t, resp, http.StatusOK)
		var body db.AsyncIndexInitManagerResponse
		err := base.JSONUnmarshal(resp.BodyBytes(), &body)
		require.NoError(c, err)
		require.Equal(c, db.BackgroundProcessStateCompleted, body.State)
	}, 1*time.Minute, 1*time.Second)
}

func TestChangeIndexPartitionsStartStopAndRestart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test only works against Couchbase Server with GSI enabled")
	}

	// requires index init
	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

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
		require.Equal(c, db.BackgroundProcessStateStopped, body.State)
	}, 1*time.Minute, 1*time.Second)

	// restart with new params
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"num_partitions":3}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	// wait for completion
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_index_init", "")
		rest.AssertStatus(t, resp, http.StatusOK)
		var body db.AsyncIndexInitManagerResponse
		err := base.JSONUnmarshal(resp.BodyBytes(), &body)
		require.NoError(c, err)
		require.Equal(c, db.BackgroundProcessStateCompleted, body.State)
	}, 1*time.Minute, 1*time.Second)
}

func TestChangeIndexPartitionsWithViews(t *testing.T) {
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

	indexNameSuffix := "_1"
	if base.TestUseXattrs() {
		indexNameSuffix = "_x1"
	}

	// requires index init
	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	n1qlStore, ok := base.AsN1QLStore(rt.GetDatabase().MetadataStore)
	require.True(t, ok)
	indexes, err := n1qlStore.GetIndexes()
	require.NoError(t, err)
	assert.Contains(t, indexes, "sg_syncDocs"+indexNameSuffix)
	assert.NotContains(t, indexes, "sg_users"+indexNameSuffix)
	assert.NotContains(t, indexes, "sg_roles"+indexNameSuffix)

	// ensure we have legacy sync docs index running in the test by default - this may need to be explicitly configured in future changes?
	require.True(t, rt.GetDatabase().UseLegacySyncDocsIndex())

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_index_init", `{"separate_principal_indexes":true}`)
	rest.RequireStatus(t, resp, http.StatusOK)

	// wait for completion
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_index_init", "")
		rest.AssertStatus(t, resp, http.StatusOK)
		var body db.AsyncIndexInitManagerResponse
		err := base.JSONUnmarshal(resp.BodyBytes(), &body)
		require.NoError(c, err)
		require.Equal(c, db.BackgroundProcessStateCompleted, body.State)
	}, 1*time.Minute, 1*time.Second)

	indexes, err = n1qlStore.GetIndexes()
	require.NoError(t, err)
	assert.Contains(t, indexes, "sg_syncDocs"+indexNameSuffix)
	assert.Contains(t, indexes, "sg_users"+indexNameSuffix)
	assert.Contains(t, indexes, "sg_roles"+indexNameSuffix)

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
	//assert.NotContains(t, indexes, "sg_syncDocs"+indexNameSuffix)
	//assert.Contains(t, indexes, "sg_users"+indexNameSuffix)
	//assert.Contains(t, indexes, "sg_roles"+indexNameSuffix)
}
