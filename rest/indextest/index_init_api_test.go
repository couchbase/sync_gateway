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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyConfig, base.KeyQuery)

	const (
		dbName            = "db"
		initialPartitions = uint32(2)
		newPartitions     = uint32(4)
	)

	dbConfig := rest.DbConfig{
		Index: &rest.IndexConfig{
			NumPartitions: base.Ptr(initialPartitions),
		},
	}
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: dbConfig},
	})
	defer rt.Close()
	database := rt.GetDatabase()
	assertNumSGIndexPartitions(t, database)

	// init new indexes with different partitions
	resp := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/%s/_index_init", dbName), fmt.Sprintf(`{"num_partitions":%d}`, newPartitions))
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
		require.GreaterOrEqual(c, len(body.IndexStatus), 1, "expected at least one scope (maybe two if `_default` plu a named scope)")
		require.LessOrEqual(c, len(body.IndexStatus), 2, "expected at most two scopes (maybe one if `_default` only)")
		for _, collections := range body.IndexStatus {
			for _, status := range collections {
				assert.Equal(c, db.CollectionIndexStatusReady, status)
			}
		}
	}, 10*time.Second, 1*time.Second)
	assertNumSGIndexPartitions(t, database)

	// update db config - shouldn't create indexes at this point since they already exist
	dbConfig.Index.NumPartitions = base.Ptr(newPartitions)
	rt.UpsertDbConfig(dbName, dbConfig)

	// cleanup old indexes
	//resp = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	//rest.RequireStatus(t, resp, http.StatusOK)
	//var body rest.PostUpgradeResponse
	//err := base.JSONUnmarshal(resp.BodyBytes(), &body)
	//require.NoError(t, err)
	//require.Lenf(t, body.Result, 1, "expected one database in post upgrade response")
	//require.Lenf(t, body.Result[dbName].RemovedIndexes, 2, "expected two indexes to be removed")
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
