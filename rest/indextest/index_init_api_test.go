package indextest

import (
	"fmt"
	"net/http"
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
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	require.False(t, base.TestsDisableGSI(), "Test requires GSI to be enabled")
	const (
		dbName            = "db"
		initialPartitions = uint32(2)
		newPartitions     = uint32(4)
	)

	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.Index.NumPartitions = base.Ptr(initialPartitions)
	rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	database := rt.GetDatabase()
	assertNumSGIndexPartitions(t, initialPartitions, database)

	// init new indexes with different partitions
	resp := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/%s/_index_init", dbName), fmt.Sprintf(`{"num_partitions":%d}`, newPartitions))
	rest.RequireStatus(t, resp, http.StatusOK)

	// wait for indexes to be ready using init api
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_index_init", dbName), fmt.Sprintf(`{"num_partitions":%d}`, newPartitions))
		rest.AssertStatus(t, resp, http.StatusOK)
		var body db.AsyncIndexInitManagerResponse
		err := base.JSONUnmarshal(resp.BodyBytes(), &body)
		require.NoError(c, err)
		require.Empty(c, body.LastErrorMessage)
		require.Equal(c, db.BackgroundProcessStateCompleted, body.State)
		require.Lenf(c, body.IndexStatus, 1, "expected one scope")
		for _, collections := range body.IndexStatus {
			require.Len(c, collections, len(rt.GetDatabase().CollectionByID), "expected matching number of collections")
			for _, status := range collections {
				assert.Equal(c, db.IndexStatusReady, status)
			}
		}
	}, 1*time.Minute, 1*time.Second)
	// more checks
	assertNumSGIndexPartitions(t, newPartitions, database)

	// update db config - shouldn't create indexes at this point since they already exist
	dbConfig.Index.NumPartitions = base.Ptr(newPartitions)
	rt.UpsertDbConfig(dbName, dbConfig)
	// TODO: stat to check if we tried rebuilding the indexes?

	// cleanup old indexes
	resp = rt.SendAdminRequest(http.MethodPost, "/_post_upgrade", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var body rest.PostUpgradeResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &body)
	require.NoError(t, err)
	require.Lenf(t, body.Result, 1, "expected one database in post upgrade response")
	require.Lenf(t, body.Result[dbName].RemovedIndexes, 2, "expected two indexes to be removed")
}

// assertNumSGIndexPartitions ensures that the number of partitions for SG indexes is as expected. Some indexes aren't partitioned.
func assertNumSGIndexPartitions(t testing.TB, expectedPartitions uint32, database *db.DatabaseContext) {
	gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
	require.NoError(t, err)
	for _, dsName := range []sgbucket.DataStoreName{db.GetSingleDatabaseCollection(t, database).GetCollectionDatastore(), database.MetadataStore} {
		allIndexes, err := gocbBucket.GetCluster().Bucket(gocbBucket.BucketName()).Scope(dsName.ScopeName()).Collection(dsName.CollectionName()).QueryIndexes().GetAllIndexes(nil)
		require.NoError(t, err)
		require.Greaterf(t, len(allIndexes), 0, "expected at least one index for datastore %s", dsName)
		for _, index := range allIndexes {
			if strings.HasPrefix(index.Name, "sg_allDocs") || strings.HasPrefix(index.Name, "sg_channels") {
				assert.True(t, strings.HasSuffix(index.Name, fmt.Sprintf("x1_p%d", expectedPartitions)), "expected %d partitions for %+v", expectedPartitions, index)
				assert.NotEqual(t, "", index.Partition)
				assert.Equal(t, int(expectedPartitions), int(db.GetIndexPartitionCount(t, gocbBucket, dsName, index.Name)))
			} else {
				assert.Equal(t, "", index.Partition)
				assert.True(t, strings.HasSuffix(index.Name, "_x1"), "expected no partitions for %+v", index)
				assert.Equal(t, 1, int(db.GetIndexPartitionCount(t, gocbBucket, dsName, index.Name)))
			}
		}
	}
}
