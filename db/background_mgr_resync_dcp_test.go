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
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
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
			title: "Restart existing run new Collection",
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
					VBUUIDs:       []uint64{1},
					CollectionIDs: []uint32{123},
				},
			},
			forceReset:         false,
			shouldCreateNewRun: true,
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

			defer func() {
				_ = db.ResyncManager.Stop()
				// this gets called by background manager in each Start call.
				// We have to manually call this for tests only to reset docsChanged/docsProcessed counters
				db.ResyncManager.resetStatus()
			}()

			options := make(map[string]any)
			options["database"] = db
			options["collections"] = ResyncCollections{}
			if testCase.forceReset {
				options["reset"] = true
			}

			var clusterData []byte
			var err error

			// Only marshal initialClusterState if clusterState is set to non empty struct
			// otherwise clusterData is zero value of ResyncManagerStatusDocDCP
			// which make `Init` to reinitialize run from existing cluster data
			if testCase.initialClusterState.ResyncID != "" {
				// if this is unset from the test case, stamp the collection ID we have - difficult to reliably predict this ahead of time
				if len(testCase.initialClusterState.CollectionIDs) == 0 {
					testCase.initialClusterState.CollectionIDs = slices.Collect(maps.Keys(db.CollectionByID))
				}

				clusterData, err = json.Marshal(testCase.initialClusterState)
				require.NoError(t, err)
			}

			err = db.ResyncManager.Process.Init(ctx, options, clusterData)
			require.NoError(t, err)

			response := getResyncStats(t, db)
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

	docsToCreate := 1000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
	defer db.Close(ctx)

	options := map[string]any{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	err := db.ResyncManager.Start(ctx, options)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitForResyncDocsProcessed(t, db, 300)
		require.NoError(t, db.ResyncManager.Stop())
	}()

	stats := waitForResyncState(t, db, BackgroundProcessStateStopped)
	assert.Less(t, stats.DocsProcessed, int64(docsToCreate), "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
	assert.Less(t, stats.DocsChanged, int64(docsToCreate))

	assert.Less(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	assert.Greater(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(300))
	wg.Wait()
}

func TestResyncManagerDCPStart(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	t.Run("Resync without updating sync function", func(t *testing.T) {
		docsToCreate := 100
		db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, false)
		defer db.Close(ctx)

		dbc, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
		scopeAndCollectionName := dbc.ScopeAndCollectionName()
		scopeName := scopeAndCollectionName.ScopeName()
		collectionName := scopeAndCollectionName.CollectionName()

		options := map[string]any{
			"database":            db,
			"regenerateSequences": false,
			"collections":         ResyncCollections{},
		}
		require.NoError(t, db.ResyncManager.Start(ctx, options))
		stats := waitForResyncState(t, db, BackgroundProcessStateCompleted)

		assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate)) // may be processing tombstones from previous tests
		assert.Equal(t, int64(0), stats.DocsChanged)

		assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
		assert.Equal(t, db.DbStats.Database().ResyncNumChanged.Value(), int64(0))

		cs, err := db.DbStats.CollectionStat(scopeName, collectionName)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, cs.ResyncNumProcessed.Value(), int64(docsToCreate))
		assert.Equal(t, int64(0), cs.ResyncNumChanged.Value())

		assert.Equal(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	})

	t.Run("Resync with updated sync function", func(t *testing.T) {
		docsToCreate := 100
		db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
		defer db.Close(ctx)

		dbc, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
		scopeAndCollectionName := dbc.ScopeAndCollectionName()
		scopeName := scopeAndCollectionName.ScopeName()
		collectionName := scopeAndCollectionName.CollectionName()

		initialStats := getResyncStats(t, db)
		log.Printf("initialStats: processed[%v] changed[%v]", initialStats.DocsProcessed, initialStats.DocsChanged)

		options := map[string]any{
			"database":            db,
			"regenerateSequences": false,
			"collections":         ResyncCollections{},
		}

		err := db.ResyncManager.Start(ctx, options)
		require.NoError(t, err)

		RequireBackgroundManagerState(t, db.ResyncManager, BackgroundProcessStateCompleted)

		stats := getResyncStats(t, db)
		// If there are tombstones from older docs which have been deleted from the bucket, processed docs will
		// be greater than DocsChanged
		assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
		assert.Equal(t, int64(docsToCreate), stats.DocsChanged)

		assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
		assert.Equal(t, db.DbStats.Database().ResyncNumChanged.Value(), int64(docsToCreate))

		cs, err := db.DbStats.CollectionStat(scopeName, collectionName)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, cs.ResyncNumProcessed.Value(), int64(docsToCreate))
		assert.Equal(t, int64(docsToCreate), cs.ResyncNumChanged.Value())

		deltaOk := assert.InDelta(t, int64(docsToCreate), db.DbStats.Database().SyncFunctionCount.Value(), 2)
		assert.True(t, deltaOk, "DCP stream has processed some documents more than once than allowed delta. Try rerunning the test.")
	})
}

func TestResyncManagerDCPRunTwice(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	docsToCreate := 1000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, false)
	defer db.Close(ctx)

	options := map[string]any{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	err := db.ResyncManager.Start(ctx, options)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	// Attempt to Start running process
	go func() {
		defer wg.Done()
		waitForResyncDocsProcessed(t, db, 100)

		err = db.ResyncManager.Start(ctx, options)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Process already running")
	}()

	stats := waitForResyncState(t, db, BackgroundProcessStateCompleted)

	// If there are tombstones from a previous test which have been deleted from the bucket, processed docs will
	// be greater than DocsChanged
	require.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
	assert.Equal(t, int64(0), stats.DocsChanged)

	assert.Equal(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	wg.Wait()
}

func TestResyncManagerDCPResumeStoppedProcess(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	docsToCreate := 5000
	db, ctx := setupTestDBForResyncWithDocs(t, docsToCreate, true)
	defer db.Close(ctx)

	options := map[string]any{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	err := db.ResyncManager.Start(ctx, options)
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitForResyncDocsProcessed(t, db, 2000)
		require.NoError(t, db.ResyncManager.Stop())
	}()

	stats := waitForResyncState(t, db, BackgroundProcessStateStopped)

	require.Less(t, stats.DocsProcessed, int64(docsToCreate), "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
	assert.Less(t, stats.DocsChanged, int64(docsToCreate))

	// Resume process
	err = db.ResyncManager.Start(ctx, options)
	require.NoError(t, err)

	stats = waitForResyncState(t, db, BackgroundProcessStateCompleted)

	assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
	assert.Equal(t, int64(docsToCreate), stats.DocsChanged)

	assert.GreaterOrEqual(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
	wg.Wait()
}

// TestResyncManagerDCPResumeStoppedProcessChangeCollections starts a resync with a single collection, stops it, and re-runs with an additional collection.
// Expects the resync process to reset with a new ID, and new checkpoints, and reprocess the full set of documents across both collections.
func TestResyncManagerDCPResumeStoppedProcessChangeCollections(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelDebug)
	base.TestRequiresCollections(t)

	docsPerCollection := 5000
	const numCollections = 2
	totalDocCount := docsPerCollection * numCollections

	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))
	dbOptions := DatabaseContextOptions{}
	dbOptions.Scopes = GetScopesOptions(t, tb, numCollections)

	db, ctx := SetupTestDBForBucketWithOptions(t, tb, dbOptions)
	defer db.Close(ctx)

	dbCollections := make([]*DatabaseCollectionWithUser, numCollections)
	for i, scName := range db.DataStoreNames() {
		col, err := db.GetDatabaseCollectionWithUser(scName.ScopeName(), scName.CollectionName())
		require.NoError(t, err)
		require.NotNil(t, col)

		// required to avoid missing audit fields in PUT
		ctx := col.AddCollectionContext(ctx)

		_, err = col.UpdateSyncFun(ctx, `function sync(doc){channel("channel.ABC");}`)
		require.NoError(t, err)

		// create docs
		for i := range docsPerCollection {
			_, _, err := col.Put(ctx, fmt.Sprintf("%s_%d", t.Name(), i), Body{"foo": "bar"})
			require.NoError(t, err)
		}

		changed, err := col.UpdateSyncFun(ctx, `function sync(doc){channel("channel.DEF");}`)
		require.NoError(t, err)
		require.True(t, changed)

		dbCollections[i] = col
	}

	options := map[string]any{
		"database":            db,
		"regenerateSequences": false,
		"collections": ResyncCollections{
			dbCollections[0].ScopeName: []string{
				dbCollections[0].Name,
			},
		},
	}

	err := db.ResyncManager.Start(ctx, options)
	require.NoError(t, err)

	// Attempt to Stop Process
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			stats := getResyncStats(t, db)
			if stats.DocsProcessed >= 2000 {
				err = db.ResyncManager.Stop()
				require.NoError(t, err)
				break
			}
			time.Sleep(1 * time.Microsecond)
		}
	}()

	stats := waitForResyncState(t, db, BackgroundProcessStateStopped)

	require.Less(t, stats.DocsProcessed, int64(docsPerCollection), "DocsProcessed is equal to docs created. Consider setting docsPerCollection > %d.", docsPerCollection)
	assert.Less(t, stats.DocsChanged, int64(docsPerCollection))

	firstDocsChanged := stats.DocsChanged

	require.GreaterOrEqual(t, len(dbCollections), 2)
	options["collections"] = ResyncCollections{
		dbCollections[0].ScopeName: []string{
			dbCollections[0].Name,
			dbCollections[1].Name,
		},
	}

	// Resume process
	err = db.ResyncManager.Start(ctx, options)
	require.NoError(t, err)

	stats = waitForResyncState(t, db, BackgroundProcessStateCompleted)

	assert.GreaterOrEqual(t, stats.DocsProcessed, int64(totalDocCount))
	assert.Equal(t, int64(totalDocCount), stats.DocsChanged+firstDocsChanged)

	assert.GreaterOrEqual(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(totalDocCount))
	wg.Wait()
}

// helper function to insert documents equals to docsToCreate, and update sync function if updateResyncFuncAfterDocsAdded set to true
func setupTestDBForResyncWithDocs(t testing.TB, docsToCreate int, updateResyncFuncAfterDocsAdded bool) (*Database, context.Context) {
	db, ctx := setupTestDB(t)
	syncFn := `
function sync(doc, oldDoc){
	channel("channel.ABC");
}
`
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, err := collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	// Create the docs that will be marked and not swept
	body := map[string]any{"foo": "bar"}
	for i := range docsToCreate {
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

// TestResyncMou ensures that resync updates create mou, and preserve pcas in mou in the case where resync is reprocessing an import
func TestResyncMou(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	if !base.TestUseXattrs() {
		t.Skip("_mou is written to xattrs only")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	if !db.Bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		t.Skip("Test requires multi-xattr subdoc operations, CBS 7.6 or higher")
	}

	initialImportCount := db.DbStats.SharedBucketImport().ImportCount.Value()
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docBody := Body{"foo": "bar"}

	// Create a document via SGW.  mou should not be updated
	_, doc, err := collection.Put(ctx, "sgWrite", docBody)
	require.NoError(t, err)
	sgWriteCas := doc.Cas

	syncData, mou, _ := getSyncAndMou(t, collection, "sgWrite")
	require.NotNil(t, syncData)
	require.Nil(t, mou)

	// 2. Create via the SDK
	_, err = collection.dataStore.WriteCas("sdkWrite", 0, 0, docBody, 0)
	require.NoError(t, err)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.SharedBucketImport().ImportCount.Value()
	}, initialImportCount+1)

	syncData, initialSDKMou, _ := getSyncAndMou(t, collection, "sdkWrite")
	require.NotNil(t, syncData)
	require.NotNil(t, initialSDKMou)

	// Update sync function and run resync
	syncFn := `
function sync(doc, oldDoc){
	channel("resync_channel");
}`
	resyncStats := runResync(t, ctx, db, collection, syncFn)
	assert.Equal(t, int64(2), resyncStats.DocsChanged)

	var cas uint64
	syncData, mou, cas = getSyncAndMou(t, collection, "sgWrite")
	require.NotNil(t, syncData)
	require.NotNil(t, mou)
	require.Equal(t, base.CasToString(sgWriteCas), mou.PreviousHexCAS)
	require.Equal(t, base.CasToString(cas), mou.HexCAS)

	syncData, mou, cas = getSyncAndMou(t, collection, "sdkWrite")
	require.NotNil(t, syncData)
	require.NotNil(t, mou)
	require.Equal(t, initialSDKMou.PreviousHexCAS, mou.PreviousHexCAS)
	require.NotEqual(t, initialSDKMou.HexCAS, mou.HexCAS)
	require.Equal(t, base.CasToString(cas), mou.HexCAS)

	// Run resync a second time with a new sync function.  mou.cas should be updated, mou.pCas should not change.
	syncFn = `
function sync(doc, oldDoc){
	channel("resync_channel_again");
}`
	resyncStats = runResync(t, ctx, db, collection, syncFn)
	assert.Equal(t, int64(2), resyncStats.DocsChanged)

	syncData, mou, cas = getSyncAndMou(t, collection, "sgWrite")
	require.NotNil(t, syncData)
	require.NotNil(t, mou)
	require.Equal(t, base.CasToString(sgWriteCas), mou.PreviousHexCAS)
	require.Equal(t, base.CasToString(cas), mou.HexCAS)

	syncData, mou, cas = getSyncAndMou(t, collection, "sdkWrite")
	require.NotNil(t, syncData)
	require.NotNil(t, mou)
	require.Equal(t, initialSDKMou.PreviousHexCAS, mou.PreviousHexCAS)
	require.NotEqual(t, initialSDKMou.HexCAS, mou.HexCAS)
	require.Equal(t, base.CasToString(cas), mou.HexCAS)
}

func runResync(t *testing.T, ctx context.Context, db *Database, collection *DatabaseCollectionWithUser, syncFn string) (stats ResyncManagerResponseDCP) {

	_, err := collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	initialStats := getResyncStats(t, db)
	log.Printf("initialStats: processed[%v] changed[%v]", initialStats.DocsProcessed, initialStats.DocsChanged)

	options := map[string]any{
		"database":            db,
		"regenerateSequences": false,
		"collections":         ResyncCollections{},
	}

	require.NoError(t, db.ResyncManager.Start(ctx, options))
	return waitForResyncState(t, db, BackgroundProcessStateCompleted)
}

// helper function to Unmarshal BackgroundProcess state into ResyncManagerResponseDCP
func getResyncStats(t testing.TB, db *Database) ResyncManagerResponseDCP {
	var resp ResyncManagerResponseDCP
	rawStatus, _, err := db.ResyncManager.Process.GetProcessStatus(BackgroundManagerStatus{})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(rawStatus, &resp))
	return resp
}

// waitForResyncState waits for the resync manager to reach the desired state, and then returns the status.
func waitForResyncState(t testing.TB, db *Database, desiredState BackgroundProcessState) ResyncManagerResponseDCP {
	RequireBackgroundManagerState(t, db.ResyncManager, desiredState)
	return getResyncStats(t, db)
}

// waitForResyncDocsProcessed waits until the resync manager has processed more than the specified count of documents.
func waitForResyncDocsProcessed(t testing.TB, db *Database, count int64) {
	// this intentionally uses a very short poll interval to catch progress as quickly as possible
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats := getResyncStats(t, db)
		assert.Greater(c, stats.DocsProcessed, count)
	}, 10*time.Second, 1*time.Millisecond)
}

func TestResyncCheckpointPrefix(t *testing.T) {
	base.TestRequiresDCPResync(t)
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	resyncID := "1234"
	testCases := []struct {
		name          string
		collectionIDs []uint32
		groupID       string
		expected      string
	}{
		{
			name:          "default collection, no group id",
			collectionIDs: []uint32{base.DefaultCollectionID},
			groupID:       "",
			expected:      fmt.Sprintf("_sync:dcp_ck::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name:          "default collection, group id=foo",
			collectionIDs: []uint32{base.DefaultCollectionID},
			groupID:       "foo",
			expected:      fmt.Sprintf("_sync:dcp_ck:foo::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name:          "default collection + collection 1, no group id",
			collectionIDs: []uint32{base.DefaultCollectionID, 1},
			groupID:       "",
			expected:      fmt.Sprintf("_sync:dcp_ck::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name:          "default collection + collection 1, group id=foo",
			collectionIDs: []uint32{base.DefaultCollectionID, 1},
			groupID:       "foo",
			expected:      fmt.Sprintf("_sync:dcp_ck:foo::sg-%v:resync:1234", base.ProductAPIVersion),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			autoImport := false
			db, err := NewDatabaseContext(
				ctx,
				"db",
				bucket.NoCloseClone(),
				autoImport,
				DatabaseContextOptions{
					Scopes:  GetScopesOptions(t, bucket, 1),
					GroupID: test.groupID,
				},
			)
			require.NoError(t, err)
			defer db.Close(ctx)
			clientOptions := getResyncDCPClientOptions(
				test.collectionIDs,
				db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID),
			)

			dcpFeedKey := GenerateResyncDCPStreamName(resyncID)
			b, err := base.AsGocbV2Bucket(bucket)
			require.NoError(t, err)
			dcpClient, err := base.NewDCPClient(
				ctx,
				dcpFeedKey,
				nil,
				*clientOptions,
				b,
			)
			require.NoError(t, err)
			require.Equal(t, test.expected, dcpClient.GetMetadataKeyPrefix())
		})
	}
}
