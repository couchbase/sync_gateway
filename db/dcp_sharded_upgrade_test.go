// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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

// Check that, on a rolling upgrade, the existing index definitions are preserved and the DCP feed does not start from zero.
func TestShardedDCPUpgrade(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if !base.IsEnterpriseEdition() {
		t.Skip("EE-only test")
	}
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Heartbeat listener requires Couchbase Server")
	}

	// Use default collection namespace since this will make sure we are upgrading from 3.0 -> post 3.0
	tb := base.GetTestBucketDefaultCollection(t)
	defer tb.Close()

	tc, err := base.AsCollection(tb)
	require.NoError(t, err)
	require.Equal(t, base.DefaultCollection, tc.Name())
	bucketUUID, err := tb.UUID()
	require.NoError(t, err, "get bucket UUID")

	numVBuckets, err := tb.GetMaxVbno()
	require.NoError(t, err)

	const (
		numPartitions = 4
		indexName     = "db0x2d9928b7_index"
	)

	require.NoError(t, tb.SetRaw(base.SyncDocPrefix+"cfgindexDefs", 0, nil, []byte(fmt.Sprintf(indexDefs, tb.GetName(), bucketUUID))))
	require.NoError(t, tb.SetRaw(base.SyncDocPrefix+"cfgnodeDefs-known", 0, nil, []byte(nodeDefs)))
	require.NoError(t, tb.SetRaw(base.SyncDocPrefix+"cfgnodeDefs-wanted", 0, nil, []byte(nodeDefs)))
	planPIndexesJSON := preparePlanPIndexesJSON(t, tb, numVBuckets, numPartitions)
	require.NoError(t, tb.SetRaw(base.SyncDocPrefix+"cfgplanPIndexes", 0, nil, []byte(planPIndexesJSON)))

	// Write a doc before starting the dbContext to check that import works
	const (
		testDoc1 = "testDoc1"
		testDoc2 = "testDoc2"
	)
	require.NoError(t, tb.SetRaw(testDoc1, 0, nil, []byte(`{}`)))

	ctx := base.TestCtx(t)
	db, err := NewDatabaseContext(ctx, tb.GetName(), tb.NoCloseClone(), true, DatabaseContextOptions{
		GroupID:     "",
		EnableXattr: true,
		UseViews:    base.TestsDisableGSI(),
		ImportOptions: ImportOptions{
			ImportPartitions: numPartitions,
		},
	})
	require.NoError(t, err, "NewDatabaseContext")
	defer db.Close(ctx)
	ctx = db.AddDatabaseLogContext(ctx)
	collection := db.GetSingleDatabaseCollection()

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
	require.NoError(t, err)

	err, _ = base.RetryLoop("wait for all pindexes to be reassigned", func() (shouldRetry bool, err error, value interface{}) {
		pIndexes, _, err := cbgt.CfgGetPlanPIndexes(db.CfgSG)
		if err != nil {
			return false, nil, err
		}
		for _, plan := range pIndexes.PlanPIndexes {
			for nodeUUID := range plan.Nodes {
				if nodeUUID != db.UUID {
					return true, nil, nil
				}
			}
		}
		return false, nil, nil
	}, base.CreateSleeperFunc(100, 100))
	require.NoError(t, err)

	// assert that the doc we created before starting this node gets imported once all the pindexes are reassigned
	require.NoError(t, collection.WaitForPendingChanges(ctx))
	doc, err := collection.GetDocument(ctx, testDoc1, DocUnmarshalAll)
	require.NoError(t, err, "GetDocument 1")
	require.NotNil(t, doc, "GetDocument 1")

	// Write a doc to the test bucket to check that import still works
	require.NoError(t, tb.SetRaw(testDoc2, 0, nil, []byte(`{}`)))
	require.NoError(t, collection.WaitForPendingChanges(ctx))
	doc, err = collection.GetDocument(ctx, testDoc2, DocUnmarshalAll)
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
			t.Fatalf("Test misconfigured - numPartitions is %d, but planPIndexes has more instances of '$SourcePartitions'", numPartitions)
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
		t.Fatalf("Test misconfigured - numPartitions is %d, but planPIndexes doesn't have enough '$SourcePartitions'", numPartitions)
	}
	return planPIndexesJSON
}
