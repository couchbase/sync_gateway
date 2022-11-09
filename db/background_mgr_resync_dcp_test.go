//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResyncDCPInit(t *testing.T) {

	testCases := []struct {
		title               string
		initialClusterState ResyncManagerStatusDocDCP
		forceReset          bool
		shouldCreateNewRun  bool
	}{
		{
			title:              "Initialize new run with empty cluster state",
			forceReset:         false,
			shouldCreateNewRun: true,
		},
		{
			title: "Reinitialize existing run",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateStopped,
					},
					ResyncID:      uuid.NewString(),
					DocsChanged:   10,
					DocsProcessed: 20,
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs: []uint64{1},
				},
			},
			forceReset:         false,
			shouldCreateNewRun: false,
		},
		{
			title: "Reinitialize completed run",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateCompleted,
					},
					ResyncID:      uuid.NewString(),
					DocsChanged:   10,
					DocsProcessed: 20,
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs: []uint64{1},
				},
			},
			forceReset:         false,
			shouldCreateNewRun: true,
		},
		{
			title: "Force restart existing run",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateStopped,
					},
					ResyncID:      uuid.NewString(),
					DocsChanged:   10,
					DocsProcessed: 20,
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs: []uint64{1},
				},
			},
			forceReset:         true,
			shouldCreateNewRun: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)

			resyncMrg := NewResyncManagerDCP(db.Bucket)
			require.NotNil(t, resyncMrg)

			// this gets called by background manager in each Start call.
			// We have to manually call this for tests only to reset docsChanged/docsProcessed counters
			defer resyncMrg.resetStatus()

			options := make(map[string]interface{})
			if testCase.forceReset {
				options["reset"] = true
			}

			var clusterData []byte
			var err error

			// Only marshal initialClusterState if clusterState is set to non empty struct
			// otherwise clusterData is zero value of ResyncManagerStatusDocDCP
			// which make `Init` to reinitialize run from existing cluster data
			if testCase.initialClusterState.ResyncID != "" {
				clusterData, err = json.Marshal(testCase.initialClusterState)
				require.NoError(t, err)
			}

			err = resyncMrg.Process.Init(context.TODO(), options, clusterData)
			require.NoError(t, err)
			rawStatus, err := resyncMrg.GetStatus()
			require.NoError(t, err)

			var response ResyncManagerResponseDCP
			err = json.Unmarshal(rawStatus, &response)
			assert.NoError(t, err)

			assert.NotEmpty(t, response.ResyncID)

			if testCase.shouldCreateNewRun {
				assert.NotEqual(t, testCase.initialClusterState.ResyncID, response.ResyncID)
				assert.Equal(t, 0, response.DocsChanged)
				assert.Equal(t, 0, response.DocsProcessed)
			} else {
				assert.Equal(t, testCase.initialClusterState.ResyncID, response.ResyncID)
				assert.Equal(t, testCase.initialClusterState.DocsChanged, response.DocsChanged)
				assert.Equal(t, testCase.initialClusterState.DocsProcessed, response.DocsProcessed)
			}
		})
	}
}

func TestResycnManagerDCPStopInMidWay(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	base.LongRunningTest(t)

	docsToCreate := 10_000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
	defer db.Close(ctx)

	resyncMrg := NewResyncManagerDCP(db.Bucket)
	require.NotNil(t, resyncMrg)
	defer resyncMrg.resetStatus()

	options := make(map[string]interface{})
	options["database"] = db
	options["regenerateSequences"] = false

	err := resyncMrg.Start(ctx, options)
	require.NoError(t, err)
	go func() {
		time.Sleep(2 * time.Second)
		_ = resyncMrg.Stop()
	}()

	err = WaitForConditionWithOptions(func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := resyncMrg.GetStatus()
		_ = json.Unmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateStopped
	}, 2000, 10)
	require.NoError(t, err)
	// Give some time to persist stats
	time.Sleep(1 * time.Second)

	stats := getResyncStats(resyncMrg.Process)
	assert.Less(t, stats.DocsProcessed, docsToCreate, "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
	assert.Less(t, stats.DocsChanged, docsToCreate)

	assert.Less(t, int(db.DbStats.Database().SyncFunctionCount.Value()), docsToCreate)
}

func TestResycnManagerDCP(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	base.LongRunningTest(t)

	t.Run("Resync without updating resync function", func(t *testing.T) {
		docsToCreate := 100
		db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, false)
		defer db.Close(ctx)

		resyncMrg := NewResyncManagerDCP(db.Bucket)
		require.NotNil(t, resyncMrg)

		options := make(map[string]interface{})
		options["database"] = db
		options["regenerateSequences"] = false

		err := resyncMrg.Start(ctx, options)
		require.NoError(t, err)

		err = WaitForConditionWithOptions(func() bool {
			var status BackgroundManagerStatus
			rawStatus, _ := resyncMrg.GetStatus()
			_ = json.Unmarshal(rawStatus, &status)
			return status.State == BackgroundProcessStateCompleted
		}, 20, 100)
		require.NoError(t, err)

		stats := getResyncStats(resyncMrg.Process)
		assert.Equal(t, docsToCreate, stats.DocsProcessed)
		assert.Equal(t, 0, stats.DocsChanged)

		assert.Equal(t, int(db.DbStats.Database().SyncFunctionCount.Value()), docsToCreate)
	})

	t.Run("Resync with updated resync function", func(t *testing.T) {
		docsToCreate := 100
		db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
		defer db.Close(ctx)

		resyncMrg := NewResyncManagerDCP(db.Bucket)
		require.NotNil(t, resyncMrg)

		options := make(map[string]interface{})
		options["database"] = db
		options["regenerateSequences"] = false

		err := resyncMrg.Start(ctx, options)
		require.NoError(t, err)

		err = WaitForConditionWithOptions(func() bool {
			var status BackgroundManagerStatus
			rawStatus, _ := resyncMrg.GetStatus()
			_ = json.Unmarshal(rawStatus, &status)
			return status.State == BackgroundProcessStateCompleted
		}, 20, 100)
		require.NoError(t, err)

		stats := getResyncStats(resyncMrg.Process)
		assert.Equal(t, docsToCreate, stats.DocsProcessed)
		assert.Equal(t, docsToCreate, stats.DocsChanged)

		assert.Equal(t, docsToCreate, int(db.DbStats.Database().SyncFunctionCount.Value()))
	})
}

func setupTestDBForResyncWithDocs(t *testing.T, docsToCreate int, updateResyncFuncAfterDocsAdded bool) (*Database, context.Context) {
	db, ctx := setupTestDB(t)
	db.Options.QueryPaginationLimit = 100
	syncFn := `
function sync(doc, oldDoc){
	channel("channel." + "ABC");
}
`
	_, err := db.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create the docs that will be marked and not swept
	body := map[string]interface{}{"foo": "bar"}
	for i := 0; i < docsToCreate; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, _, err := collection.Put(ctx, key, body)
		require.NoError(t, err)
	}

	assert.Equal(t, docsToCreate, int(db.DbStats.Database().SyncFunctionCount.Value()))
	db.DbStats.Database().SyncFunctionCount.Set(0)

	if updateResyncFuncAfterDocsAdded {
		syncFn = `
function sync(doc, oldDoc){
	channel("channel." + "ABC123");
}
`

		_, err = db.UpdateSyncFun(ctx, syncFn)
		require.NoError(t, err)
	}
	return db, ctx
}

// helper function to Unmarshal BackgroundProcess state into ResyncManagerResponseDCP
func getResyncStats(resyncManager BackgroundManagerProcessI) ResyncManagerResponseDCP {
	var resp ResyncManagerResponseDCP
	rawStatus, _, _ := resyncManager.GetProcessStatus(BackgroundManagerStatus{})
	_ = json.Unmarshal(rawStatus, &resp)
	return resp
}
