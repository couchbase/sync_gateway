package db

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
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
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Requires Couchbase Server")
	}
	if !base.IsEnterpriseEdition() {
		t.Skip("EE-only test")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyDCP, base.KeyImport, base.KeyCluster)
	tb := base.GetTestBucket(t)
	defer tb.Close()
	bucketUUID, err := tb.UUID()
	require.NoError(t, err, "get bucket UUID")

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

	t.Log("TEST: setup complete")

	// Start this retry loop asynchronously because the change may happen before NewDatabaseContext returns
	db, err := NewDatabaseContext(tb.GetName(), tb, true, DatabaseContextOptions{
		GroupID:                    "",
		EnableXattr:                true,
		UseViews:                   base.TestsDisableGSI(),
		skipStartHeartbeatChecking: true,
		ImportOptions: ImportOptions{
			ImportPartitions: numPartitions,
		},
	})
	require.NoError(t, err, "NewDatabaseContext")
	defer db.Close()

	//pIndexes, _, err := cbgt.CfgGetPlanPIndexes(db.CfgSG)
	//require.NoError(t, err)
	// TODO: sometimes cbgt adds and immediately removes a PIndex causing this to fail
	//assert.Len(t, pIndexes.PlanPIndexes, numPartitions, "number of planPIndexes should match partitions after DbContext start")

	require.NoError(t, db.WaitForPendingChanges(base.TestCtx(t)))
	doc, err := db.GetDocument(base.TestCtx(t), testDoc1, DocUnmarshalAll)
	require.NoError(t, err, "GetDocument 1")
	require.NotNil(t, doc, "GetDocument 1")

	assert.True(t, db.ImportListener.cbgtContext.LithiumCompat.IsTrue(), "LithiumCompat")

	require.NoError(t, db.Heartbeater.StartCheckingHeartbeats())

	// Now wait for it to pick up that the Lithium "node" isn't actually heartbeating
	err, _ = base.RetryLoop("wait for LithiumCompat to become false", func() (shouldRetry bool, err error, value interface{}) {
		if db == nil {
			return true, nil, nil
		}
		return db.ImportListener.cbgtContext.LithiumCompat.IsTrue(), nil, nil
		// will take around ~10 seconds at least
	}, base.CreateSleeperFunc(200, 10))
	assert.NoError(t, err, "LithiumCompat never became false")

	//pIndexes, _, err = cbgt.CfgGetPlanPIndexes(db.CfgSG)
	//require.NoError(t, err)
	// TODO: sometimes cbgt adds and immediately removes a PIndex causing this to fail
	//assert.Len(t, pIndexes.PlanPIndexes, numPartitions, "number of planPIndexes should match partitions after LithiumCompat change")

	// And write another doc to check that import still works
	require.NoError(t, tb.SetRaw(testDoc2, 0, nil, []byte(`{}`)))
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
