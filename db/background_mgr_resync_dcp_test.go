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
	"log"
	"sync"
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

			resycMgr := NewResyncManagerDCP(db.MetadataStore, base.TestUseXattrs(), db.MetadataKeys)
			require.NotNil(t, resycMgr)
			db.ResyncManager = resycMgr

			defer func() {
				_ = resycMgr.Stop()
				// this gets called by background manager in each Start call.
				// We have to manually call this for tests only to reset docsChanged/docsProcessed counters
				resycMgr.resetStatus()
			}()

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

			err = resycMgr.Process.Init(context.TODO(), options, clusterData)
			require.NoError(t, err)

			response := getResyncStats(resycMgr.Process)
			assert.NotEmpty(t, response.ResyncID)

			if testCase.shouldCreateNewRun {
				assert.NotEqual(t, testCase.initialClusterState.ResyncID, response.ResyncID)
				assert.Equal(t, int64(0), response.DocsChanged)
				assert.Equal(t, int64(0), response.DocsProcessed)
			} else {
				assert.Equal(t, testCase.initialClusterState.ResyncID, response.ResyncID)
				assert.Equal(t, testCase.initialClusterState.DocsChanged, response.DocsChanged)
				assert.Equal(t, testCase.initialClusterState.DocsProcessed, response.DocsProcessed)
			}
		})
	}
}

func TestResyncManagerDCPStopInMidWay(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	base.LongRunningTest(t)

	docsToCreate := 1000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
	defer db.Close(ctx)

	resycMgr := NewResyncManagerDCP(db.MetadataStore, base.TestUseXattrs(), db.MetadataKeys)

	require.NotNil(t, resycMgr)
	db.ResyncManager = resycMgr

	options := map[string]interface{}{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	err := resycMgr.Start(ctx, options)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = WaitForConditionWithOptions(func() bool {
			stats := getResyncStats(resycMgr.Process)
			if stats.DocsProcessed > 300 {
				err = resycMgr.Stop()
				require.NoError(t, err)
				return true
			}
			return false
		}, 200, 100)
		require.NoError(t, err)
	}()

	err = WaitForConditionWithOptions(func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := resycMgr.GetStatus()
		_ = json.Unmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateStopped
	}, 200, 100)
	require.NoError(t, err)

	stats := getResyncStats(resycMgr.Process)
	assert.Less(t, stats.DocsProcessed, int64(docsToCreate), "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
	assert.Less(t, stats.DocsChanged, int64(docsToCreate))

	assert.Less(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	assert.Greater(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(300))
	wg.Wait()
}

func TestResyncManagerDCPStart(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	t.Run("Resync without updating sync function", func(t *testing.T) {
		docsToCreate := 100
		db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, false)
		defer db.Close(ctx)

		resyncMgr := NewResyncManagerDCP(db.MetadataStore, base.TestUseXattrs(), db.MetadataKeys)

		require.NotNil(t, resyncMgr)
		db.ResyncManager = resyncMgr

		options := map[string]interface{}{
			"database":            db,
			"regenerateSequences": false,
			"collections":         ResyncCollections{},
		}
		err := resyncMgr.Start(ctx, options)
		require.NoError(t, err)

		err = WaitForConditionWithOptions(func() bool {
			var status BackgroundManagerStatus
			rawStatus, _ := resyncMgr.GetStatus()
			_ = json.Unmarshal(rawStatus, &status)
			return status.State == BackgroundProcessStateCompleted
		}, 200, 200)
		require.NoError(t, err)

		stats := getResyncStats(resyncMgr.Process)
		assert.Equal(t, int64(docsToCreate), stats.DocsProcessed)
		assert.Equal(t, int64(0), stats.DocsChanged)

		assert.Equal(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	})

	t.Run("Resync with updated sync function", func(t *testing.T) {
		docsToCreate := 100
		db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
		defer db.Close(ctx)

		resyncMgr := NewResyncManagerDCP(db.MetadataStore, base.TestUseXattrs(), db.MetadataKeys)
		require.NotNil(t, resyncMgr)

		initialStats := getResyncStats(resyncMgr.Process)
		log.Printf("initialStats: processed[%v] changed[%v]", initialStats.DocsProcessed, initialStats.DocsChanged)

		options := map[string]interface{}{
			"database":            db,
			"regenerateSequences": false,
			"collections":         ResyncCollections{},
		}

		err := resyncMgr.Start(ctx, options)
		require.NoError(t, err)

		err = WaitForConditionWithOptions(func() bool {
			var status BackgroundManagerStatus
			rawStatus, _ := resyncMgr.GetStatus()
			_ = json.Unmarshal(rawStatus, &status)
			return status.State == BackgroundProcessStateCompleted
		}, 200, 200)
		require.NoError(t, err)

		stats := getResyncStats(resyncMgr.Process)
		// If there are tombstones from older docs which have been deleted from the bucket, processed docs will
		// be greater than DocsChanged
		assert.LessOrEqual(t, int64(docsToCreate), stats.DocsProcessed)
		assert.Equal(t, int64(docsToCreate), stats.DocsChanged)

		deltaOk := assert.InDelta(t, int64(docsToCreate), db.DbStats.Database().SyncFunctionCount.Value(), 2)
		assert.True(t, deltaOk, "DCP stream has processed some documents more than once than allowed delta. Try rerunning the test.")
	})
}

func TestResyncManagerDCPRunTwice(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	base.LongRunningTest(t)

	docsToCreate := 1000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, false)
	defer db.Close(ctx)

	resycMgr := NewResyncManagerDCP(db.MetadataStore, base.TestUseXattrs(), db.MetadataKeys)
	require.NotNil(t, resycMgr)
	db.ResyncManager = resycMgr

	options := map[string]interface{}{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	err := resycMgr.Start(ctx, options)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	// Attempt to Start running process
	go func() {
		defer wg.Done()
		err := WaitForConditionWithOptions(func() bool {
			stats := getResyncStats(resycMgr.Process)
			return stats.DocsProcessed > 100
		}, 100, 200)
		require.NoError(t, err)

		err = resycMgr.Start(ctx, options)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Process already running")
	}()

	err = WaitForConditionWithOptions(func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := resycMgr.GetStatus()
		_ = json.Unmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateCompleted
	}, 200, 200)
	require.NoError(t, err)

	stats := getResyncStats(resycMgr.Process)

	// If there are tombstones from a previous test which have been deleted from the bucket, processed docs will
	// be greater than DocsChanged
	require.LessOrEqual(t, int64(docsToCreate), stats.DocsProcessed)
	assert.Equal(t, int64(0), stats.DocsChanged)

	assert.Equal(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	wg.Wait()
}

func TestResycnManagerDCPResumeStoppedProcess(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	base.LongRunningTest(t)

	docsToCreate := 5000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
	defer db.Close(ctx)

	resycMgr := NewResyncManagerDCP(db.MetadataStore, base.TestUseXattrs(), db.MetadataKeys)
	require.NotNil(t, resycMgr)
	db.ResyncManager = resycMgr

	options := map[string]interface{}{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	err := resycMgr.Start(ctx, options)
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			stats := getResyncStats(resycMgr.Process)
			if stats.DocsProcessed >= 2000 {
				err = resycMgr.Stop()
				require.NoError(t, err)
				break
			}
			time.Sleep(1 * time.Microsecond)
		}
	}()

	err = WaitForConditionWithOptions(func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := resycMgr.GetStatus()
		_ = json.Unmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateStopped
	}, 200, 200)
	require.NoError(t, err)

	stats := getResyncStats(resycMgr.Process)
	require.Less(t, stats.DocsProcessed, int64(docsToCreate), "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
	assert.Less(t, stats.DocsChanged, int64(docsToCreate))

	// Resume process
	err = resycMgr.Start(ctx, options)
	require.NoError(t, err)

	err = WaitForConditionWithOptions(func() bool {
		var status BackgroundManagerStatus
		rawStatus, _ := resycMgr.GetStatus()
		_ = json.Unmarshal(rawStatus, &status)
		return status.State == BackgroundProcessStateCompleted
	}, 200, 200)
	require.NoError(t, err)

	stats = getResyncStats(resycMgr.Process)
	assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
	assert.Equal(t, int64(docsToCreate), stats.DocsChanged)

	assert.GreaterOrEqual(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	wg.Wait()
}

// helper function to insert documents equals to docsToCreate, and update sync function if updateResyncFuncAfterDocsAdded set to true
func setupTestDBForResyncWithDocs(t *testing.T, docsToCreate int, updateResyncFuncAfterDocsAdded bool) (*Database, context.Context) {
	db, ctx := setupTestDB(t)
	db.Options.QueryPaginationLimit = 100
	syncFn := `
function sync(doc, oldDoc){
	channel("channel.ABC");
}
`
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	_, err := collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

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
	channel("channel.ABC123");
}
`
		_, err = collection.UpdateSyncFun(ctx, syncFn)
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
