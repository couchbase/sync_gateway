package db

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

const indexDefs = `{
  "uuid": "438836eb24955a74",
  "indexDefs": {
    "db0x2d9928b7_index": {
      "type": "syncGateway-import-",
      "name": "db0x2d9928b7_index",
      "uuid": "438836eb24955a74",
      "sourceType": "couchbase-dcp-sg",
      "sourceName": "%[1]s",
      "sourceUUID": "%[2]s",
      "planParams": {
        "maxPartitionsPerPIndex": 64
      },
      "params": {
        "destKey": "%[1]s_import"
      },
      "sourceParams": {
        "includeXAttrs": true,
        "sg_dbname": "%[1]s"
      }
    }
  },
  "implVersion": "5.5.0"
}`

const nodeDefs = `{
  "uuid": "5f2462bb9e15a7b2",
  "nodeDefs": {
    "680cce1d31e72b69": {
      "hostPort": "680cce1d31e72b69",
      "uuid": "680cce1d31e72b69",
      "implVersion": "5.5.0",
      "tags": [
        "feed",
        "janitor",
        "pindex",
        "planner"
      ],
      "container": "",
      "weight": 1,
      "extras": ""
    }
  },
  "implVersion": "5.5.0"
}`

const planPIndexes = `{
  "uuid": "554a51c130c9b945",
  "planPIndexes": {
    "db0x2d9928b7_index_438836eb24955a74_103fc5fb": {
      "name": "db0x2d9928b7_index_438836eb24955a74_103fc5fb",
      "uuid": "20623badea9024a1",
      "indexType": "syncGateway-import-",
      "indexName": "db0x2d9928b7_index",
      "indexUUID": "438836eb24955a74",
      "sourceType": "couchbase-dcp-sg",
      "sourceName": "%[1]s",
      "sourceUUID": "%[2]s",
      "sourcePartitions": "$SourcePartitions",
      "nodes": {
        "680cce1d31e72b69": {
          "canRead": true,
          "canWrite": true,
          "priority": 0
        }
      },
      "indexParams": {
        "destKey": "%[1]s_import"
      },
      "sourceParams": {
        "includeXAttrs": true,
        "sg_dbname": "%[1]s"
      }
    },
    "db0x2d9928b7_index_438836eb24955a74_1076212c": {
      "name": "db0x2d9928b7_index_438836eb24955a74_1076212c",
      "uuid": "617290ccf3f3e7d3",
      "indexType": "syncGateway-import-",
      "indexName": "db0x2d9928b7_index",
      "indexUUID": "438836eb24955a74",
      "sourceType": "couchbase-dcp-sg",
      "sourceName": "%[1]s",
      "sourceUUID": "%[2]s",
      "sourcePartitions": "$SourcePartitions",
      "nodes": {
        "680cce1d31e72b69": {
          "canRead": true,
          "canWrite": true,
          "priority": 0
        }
      },
      "indexParams": {
        "destKey": "%[1]s_import"
      },
      "sourceParams": {
        "includeXAttrs": true,
        "sg_dbname": "%[1]s"
      }
    },
    "db0x2d9928b7_index_438836eb24955a74_120a6de6": {
      "name": "db0x2d9928b7_index_438836eb24955a74_120a6de6",
      "uuid": "3ccf34ea23fb9727",
      "indexType": "syncGateway-import-",
      "indexName": "db0x2d9928b7_index",
      "indexUUID": "438836eb24955a74",
      "sourceType": "couchbase-dcp-sg",
      "sourceName": "%[1]s",
      "sourceUUID": "%[2]s",
      "sourcePartitions": "$SourcePartitions",
      "nodes": {
        "680cce1d31e72b69": {
          "canRead": true,
          "canWrite": true,
          "priority": 0
        }
      },
      "indexParams": {
        "destKey": "%[1]s_import"
      },
      "sourceParams": {
        "includeXAttrs": true,
        "sg_dbname": "%[1]s"
      }
    },
    "db0x2d9928b7_index_438836eb24955a74_15a93994": {
      "name": "db0x2d9928b7_index_438836eb24955a74_15a93994",
      "uuid": "4c167ac08c668923",
      "indexType": "syncGateway-import-",
      "indexName": "db0x2d9928b7_index",
      "indexUUID": "438836eb24955a74",
      "sourceType": "couchbase-dcp-sg",
      "sourceName": "%[1]s",
      "sourceUUID": "%[2]s",
      "sourcePartitions": "$SourcePartitions",
      "nodes": {
        "680cce1d31e72b69": {
          "canRead": true,
          "canWrite": true,
          "priority": 0
        }
      },
      "indexParams": {
        "destKey": "%[1]s_import"
      },
      "sourceParams": {
        "includeXAttrs": true,
        "sg_dbname": "%[1]s"
      }
    }
  },
  "implVersion": "5.5.0",
  "warnings": {
    "db0x2d9928b7_index": []
  }
}`

func TestShardedDCPUpgradeHelium(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	base.TestRequiresCollections(t)
	if !base.IsEnterpriseEdition() {
		t.Skip("EE-only test")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyDCP, base.KeyImport, base.KeyCluster)
	tb := base.GetTestBucket(t)
	defer tb.Close()
	bucketUUID, err := tb.UUID()
	require.NoError(t, err, "get bucket UUID")

	const (
		testScopeName      = "foo"
		testCollectionName = "bar"
	)
	err = base.CreateTestBucketScopesAndCollections(base.TestCtx(t), tb, map[string][]string{
		testScopeName: {testCollectionName},
	})
	require.NoError(t, err, "create test bucket scopes and collections")

	// We need to create a new bucket based on a spec with collections, rather than just adding them to the existing
	// bucket's spec, otherwise they won't be reflected in the TestBucket's underlying bucket's spec, and so won't
	// make it into StartShardedDCPFeed.
	collectionsBucketSpec := tb.BucketSpec
	collectionsBucketSpec.Scope = base.StringPtr(testScopeName)
	collectionsBucketSpec.Collection = base.StringPtr(testCollectionName)

	collectionsBucket, err := ConnectToBucket(collectionsBucketSpec)
	require.NoError(t, err, "ConnectToBucket w/ collectionsBucketSpec")
	defer collectionsBucket.Close()

	numVBuckets, err := tb.GetMaxVbno()
	require.NoError(t, err)

	const numPartitions = 4

	require.NoError(t, tb.SetRaw(base.SyncPrefix+"cfgindexDefs", 0, nil, []byte(fmt.Sprintf(indexDefs, tb.GetName(), bucketUUID))))
	require.NoError(t, tb.SetRaw(base.SyncPrefix+"cfgnodeDefs-known", 0, nil, []byte(nodeDefs)))
	require.NoError(t, tb.SetRaw(base.SyncPrefix+"cfgnodeDefs-wanted", 0, nil, []byte(nodeDefs)))
	planPIndexesJSON := preparePlanPIndexesJSON(t, tb, numVBuckets, numPartitions)
	require.NoError(t, tb.SetRaw(base.SyncPrefix+"cfgplanPIndexes", 0, nil, []byte(planPIndexesJSON)))

	// Write a doc before starting the dbContext to check that import works
	const (
		testDoc1 = "testDoc1"
		testDoc2 = "testDoc2"
	)
	require.NoError(t, tb.SetRaw(testDoc1, 0, nil, []byte(`{}`)))

	// Check that the presence of an older node is picked up, and that we cannot start the feed with collections.
	db, err := NewDatabaseContext(tb.GetName(), collectionsBucket, true, DatabaseContextOptions{
		GroupID:     "",
		EnableXattr: true,
		UseViews:    base.TestsDisableGSI(),
		ImportOptions: ImportOptions{
			ImportPartitions: numPartitions,
		},
		skipStartHeartbeatChecking: true,
	})
	require.Error(t, err, "NewDatabaseContext with collections and old node present should error")

	// Start it without collections
	db, err = NewDatabaseContext(tb.GetName(), tb.NoCloseClone(), true, DatabaseContextOptions{
		GroupID:     "",
		EnableXattr: true,
		UseViews:    base.TestsDisableGSI(),
		ImportOptions: ImportOptions{
			ImportPartitions: numPartitions,
		},
	})
	require.NoError(t, err, "NewDatabaseContext *without* collections")
	defer db.Close()

	// Wait until cbgt removes the old (non-existent) node from the config
	err, _ = base.RetryLoop("wait for non-existent node to be removed", func() (shouldRetry bool, err error, value interface{}) {
		nodes, _, err := cbgt.CfgGetNodeDefs(db.CfgSG, cbgt.NODE_DEFS_KNOWN)
		if err != nil {
			return false, err, nil
		}
		for uuid := range nodes.NodeDefs {
			if uuid != db.UUID {
				return true, nil, nil
			}
		}
		return false, nil, nil
	}, base.CreateSleeperFunc(100, 100))
	require.NoError(t, err, "node wait retry loop")

	// assert that the doc we created before starting this node gets imported once all the pindexes are reassigned
	require.NoError(t, db.WaitForPendingChanges(base.TestCtx(t)))
	doc, err := db.GetDocument(base.TestCtx(t), testDoc1, DocUnmarshalAll)
	require.NoError(t, err, "GetDocument 1")
	require.NotNil(t, doc, "GetDocument 1")

	// verify that now we *can* re-create the db with collections
	db.Close()
	db, err = NewDatabaseContext(tb.GetName(), collectionsBucket, true, DatabaseContextOptions{
		GroupID:     "",
		EnableXattr: true,
		UseViews:    base.TestsDisableGSI(),
		ImportOptions: ImportOptions{
			ImportPartitions: numPartitions,
		},
	})
	require.NoError(t, err, "NewDatabaseContext *with* collections")
	defer db.Close()

	// Write a doc to the test collection to check that import still works
	require.NoError(t, collectionsBucket.SetRaw(testDoc2, 0, nil, []byte(`{}`)))
	require.NoError(t, db.WaitForPendingChanges(base.TestCtx(t)))
	doc, err = db.GetDocument(base.TestCtx(t), testDoc2, DocUnmarshalAll)
	require.NoError(t, err, "GetDocument 2")
	require.NotNil(t, doc, "GetDocument 2")
}

// preparePlanPIndexesJSON processes the hard-coded planPIndexes value and replaces the $SourcePartitions with increasing
// vBucket numbers.
func preparePlanPIndexesJSON(t *testing.T, tb *base.TestBucket, numVBuckets uint16, numPartitions int) string {
	t.Helper()

	uuid, err := tb.UUID()
	require.NoError(t, err, "get bucket UUID")
	planPIndexesJSON := fmt.Sprintf(planPIndexes, tb.GetName(), uuid)
	numVBsPerPartition := int(numVBuckets) / numPartitions
	highestVBNo := 0

	for strings.Contains(planPIndexesJSON, "$SourcePartitions") {
		if highestVBNo == int(numVBuckets) {
			t.Fatalf("Test misconfigured = numPartitions is %d, but planPIndexes has more instances of '$SourcePartitions'", numPartitions)
		}
		vBs := make([]string, 0, numVBsPerPartition)
		prevHighestVBNo := highestVBNo
		for ; highestVBNo < (prevHighestVBNo + numVBsPerPartition); highestVBNo++ {
			vBs = append(vBs, strconv.Itoa(highestVBNo))
		}
		sourcePartitionsStr := strings.Join(vBs, ",")
		planPIndexesJSON = strings.Replace(planPIndexesJSON, "$SourcePartitions", sourcePartitionsStr, 1)
	}
	if highestVBNo != int(numVBuckets) {
		t.Fatalf("Test misconfigured = numPartitions is %d, but planPIndexes doesn't have enough '$SourcePartitions'", numPartitions)
	}
	return planPIndexesJSON
}
