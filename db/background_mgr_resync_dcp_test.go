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

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResyncDCPInit(t *testing.T) {
	testCases := []struct {
		title                string
		initialClusterState  ResyncManagerStatusDocDCP
		forceReset           bool
		shouldCreateNewRun   bool
		expectedDocsTargeted uint64
	}{
		{
			title:                "Initialize new run with empty cluster state",
			forceReset:           false,
			shouldCreateNewRun:   true,
			expectedDocsTargeted: 0, // no docs in DB
		},
		{
			title: "Reinitialize existing run",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateStopped,
					},
					ResyncID: uuid.NewString(),
					resyncStats: resyncStats{
						DocsChanged:   10,
						DocsProcessed: 20,
						DocsTargeted:  50,
					},
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs: []uint64{1},
				},
			},
			forceReset:           false,
			shouldCreateNewRun:   false,
			expectedDocsTargeted: 50, // restored from persisted meta
		},
		{
			title: "Restart existing run new Collection",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateStopped,
					},
					ResyncID: uuid.NewString(),
					resyncStats: resyncStats{
						DocsChanged:   10,
						DocsProcessed: 20,
					},
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs:       []uint64{1},
					CollectionIDs: []uint32{123},
				},
			},
			forceReset:           false,
			shouldCreateNewRun:   true,
			expectedDocsTargeted: 0, // no docs in DB
		},
		{
			title: "Reinitialize completed run",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateCompleted,
					},
					ResyncID: uuid.NewString(),
					resyncStats: resyncStats{
						DocsChanged:   10,
						DocsProcessed: 20,
					},
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs: []uint64{1},
				},
			},
			forceReset:           false,
			shouldCreateNewRun:   true,
			expectedDocsTargeted: 0, // no docs in DB
		},
		{
			title: "Force restart existing run",
			initialClusterState: ResyncManagerStatusDocDCP{
				ResyncManagerResponseDCP: ResyncManagerResponseDCP{
					BackgroundManagerStatus: BackgroundManagerStatus{
						State: BackgroundProcessStateStopped,
					},
					ResyncID: uuid.NewString(),
					resyncStats: resyncStats{
						DocsChanged:   10,
						DocsProcessed: 20,
					},
				},
				ResyncManagerMeta: ResyncManagerMeta{
					VBUUIDs: []uint64{1},
				},
			},
			forceReset:           true,
			shouldCreateNewRun:   true,
			expectedDocsTargeted: 0, // no docs in DB
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)

			defer func() {
				_ = db.ResyncManager.Stop(ctx)
				// this gets called by background manager in each Start call.
				// We have to manually call this for tests only to reset docsChanged/docsProcessed counters
				db.ResyncManager.resetStatus()
			}()

			options := make(map[string]any)
			options["collections"] = base.NewCollectionNames()
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
			assert.Equal(t, testCase.expectedDocsTargeted, response.DocsTargeted)
			assert.Equal(t, testCase.expectedDocsTargeted, uint64(db.DbStats.Database().ResyncDocsTargeted.Value()))

			assert.Equal(t, int64(0), db.DbStats.Database().ResyncNumChanged.Value())
			assert.Equal(t, int64(0), db.DbStats.Database().ResyncNumProcessed.Value())
		})
	}
}

func TestResyncManagerDCPStopInMidWay(t *testing.T) {
	for _, testCase := range ResyncTestModes() {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.Distributed {
				t.Skip("Enable in CBG-5419")
			}
			docsToCreate := 1000
			if base.UnitTestUrlIsWalrus() {
				// rosmar runs too quickly, increase doc count
				docsToCreate *= 5
			}
			db, ctx := setupTestDBForResyncWithDocs(t, testDBForResyncOptions{
				docsToCreate:                 docsToCreate,
				updateSyncFuncAfterDocsAdded: true,
				distributed:                  testCase.Distributed,
			})
			defer db.Close(ctx)

			options := map[string]any{
				"regenerateSequences": false,
				"collections":         base.NewCollectionNames(),
			}

			err := db.ResyncManager.Start(ctx, options)
			require.NoError(t, err)
			wg := sync.WaitGroup{}
			defer base.WaitWithTimeout(t, &wg, 30*time.Second)
			wg.Go(func() {
				waitForResyncDocsProcessed(t, db, 300)
				require.NoError(t, db.ResyncManager.Stop(ctx))
			})

			stats := waitForResyncState(t, db, BackgroundProcessStateStopped)
			assert.Less(t, stats.DocsProcessed, int64(docsToCreate), "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
			assert.Less(t, stats.DocsChanged, int64(docsToCreate))

			assert.Less(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
			assert.Less(t, db.DbStats.Database().ResyncNumChanged.Value(), int64(docsToCreate))

			assert.Less(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
			assert.Greater(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(300))
		})
	}
}

func TestResyncManagerDCPStart(t *testing.T) {
	for _, testCase := range ResyncTestModes() {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Run("Resync without updating sync function", func(t *testing.T) {
				if testCase.Distributed {
					t.Skip("Enable in CBG-5419")
				}
				docsToCreate := 100
				db, ctx := setupTestDBForResyncWithDocs(t, testDBForResyncOptions{
					docsToCreate: docsToCreate,
					distributed:  testCase.Distributed,
				})
				defer db.Close(ctx)

				dbc, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				scopeAndCollectionName := dbc.ScopeAndCollectionName()
				scopeName := scopeAndCollectionName.ScopeName()
				collectionName := scopeAndCollectionName.CollectionName()

				options := map[string]any{
					"regenerateSequences": false,
					"collections":         base.NewCollectionNames(),
				}
				require.NoError(t, db.ResyncManager.Start(ctx, options))
				stats := waitForResyncState(t, db, BackgroundProcessStateCompleted)

				assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate)) // may be processing tombstones from previous tests
				assert.Equal(t, int64(0), stats.DocsChanged)
				assert.GreaterOrEqual(t, stats.DocsTargeted, uint64(docsToCreate))
				assert.GreaterOrEqual(t, int64(0), db.DbStats.Database().ResyncDocsTargeted.Value()) // resync finished so reset back to 0

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
				db, ctx := setupTestDBForResyncWithDocs(t, testDBForResyncOptions{
					docsToCreate:                 docsToCreate,
					updateSyncFuncAfterDocsAdded: true,
					distributed:                  testCase.Distributed,
				})
				defer db.Close(ctx)

				dbc, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				scopeAndCollectionName := dbc.ScopeAndCollectionName()
				scopeName := scopeAndCollectionName.ScopeName()
				collectionName := scopeAndCollectionName.CollectionName()

				initialStats := getResyncStats(t, db)
				log.Printf("initialStats: processed[%v] changed[%v]", initialStats.DocsProcessed, initialStats.DocsChanged)

				options := map[string]any{
					"regenerateSequences": false,
					"collections":         base.NewCollectionNames(),
				}

				err := db.ResyncManager.Start(ctx, options)
				require.NoError(t, err)

				RequireBackgroundManagerState(t, db.ResyncManager, BackgroundProcessStateCompleted)

				stats := getResyncStats(t, db)
				// If there are tombstones from older docs which have been deleted from the bucket, processed docs will
				// be greater than DocsChanged
				assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
				assert.Equal(t, int64(docsToCreate), stats.DocsChanged)
				assert.GreaterOrEqual(t, stats.DocsTargeted, uint64(docsToCreate))
				assert.Equal(t, int64(0), db.DbStats.Database().ResyncDocsTargeted.Value()) // resync finished so reset back to 0

				assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
				assert.Equal(t, db.DbStats.Database().ResyncNumChanged.Value(), int64(docsToCreate))

				cs, err := db.DbStats.CollectionStat(scopeName, collectionName)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, cs.ResyncNumProcessed.Value(), int64(docsToCreate))
				assert.Equal(t, int64(docsToCreate), cs.ResyncNumChanged.Value())

				// CBG-5418: debug flake of why distributed resync sometimes processes more documents than expected
				if !testCase.Distributed {
					deltaOk := assert.InDelta(t, int64(docsToCreate), db.DbStats.Database().SyncFunctionCount.Value(), 2)
					assert.True(t, deltaOk, "DCP stream has processed some documents more than once than allowed delta. Try rerunning the test.")
				}
			})
		})
	}
}

func TestResyncManagerDCPRunTwice(t *testing.T) {
	for _, testCase := range ResyncTestModes() {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.Distributed {
				t.Skip("Enable in CBG-5419")
			}
			docsToCreate := 1000
			// rosmar runs too quickly, increase doc count
			if base.UnitTestUrlIsWalrus() {
				docsToCreate *= 10
			}
			db, ctx := setupTestDBForResyncWithDocs(t, testDBForResyncOptions{
				docsToCreate: docsToCreate,
				distributed:  testCase.Distributed,
			})
			defer db.Close(ctx)

			options := map[string]any{
				"regenerateSequences": false,
				"collections":         base.NewCollectionNames(),
			}

			err := db.ResyncManager.Start(ctx, options)
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			defer base.WaitWithTimeout(t, &wg, 30*time.Second)
			// Attempt to Start running process
			wg.Go(func() {
				waitForResyncDocsProcessed(t, db, 100)

				err := db.ResyncManager.Start(ctx, options)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "Process already running")
			})

			stats := waitForResyncState(t, db, BackgroundProcessStateCompleted)

			// If there are tombstones from a previous test which have been deleted from the bucket, processed docs will
			// be greater than DocsChanged
			require.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
			assert.Equal(t, int64(0), stats.DocsChanged)

			assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
			assert.Equal(t, int64(0), db.DbStats.Database().ResyncNumChanged.Value())

			assert.Equal(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
		})
	}
}

func TestResyncManagerDCPResumeStoppedProcess(t *testing.T) {
	for _, testCase := range ResyncTestModes() {
		t.Run(testCase.Name, func(t *testing.T) {
			docsToCreate := 5000
			// rosmar runs too quickly, increase doc count
			if base.UnitTestUrlIsWalrus() {
				docsToCreate *= 2
			}
			db, ctx := setupTestDBForResyncWithDocs(t, testDBForResyncOptions{
				docsToCreate:                 docsToCreate,
				updateSyncFuncAfterDocsAdded: true,
				distributed:                  testCase.Distributed,
			})
			defer db.Close(ctx)

			options := map[string]any{
				"regenerateSequences": false,
				"collections":         base.NewCollectionNames(),
			}

			err := db.ResyncManager.Start(ctx, options)
			require.NoError(t, err)

			// Attempt to Stop Process
			wg := sync.WaitGroup{}
			defer base.WaitWithTimeout(t, &wg, 30*time.Second)
			wg.Go(func() {
				waitForResyncDocsProcessed(t, db, 2000)
				require.NoError(t, db.ResyncManager.Stop(ctx))
			})

			stats := waitForResyncState(t, db, BackgroundProcessStateStopped)

			require.Less(t, stats.DocsProcessed, int64(docsToCreate), "DocsProcessed is equal to docs created. Consider setting docsToCreate > %d.", docsToCreate)
			assert.Less(t, stats.DocsChanged, int64(docsToCreate))
			// DocsTargeted is computed once at run start and should be >= docsToCreate
			assert.GreaterOrEqual(t, stats.DocsTargeted, uint64(docsToCreate))
			assert.GreaterOrEqual(t, uint64(db.DbStats.Database().ResyncDocsTargeted.Value()), uint64(docsToCreate))
			initialDocsTargeted := stats.DocsTargeted

			assert.Less(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
			assert.Less(t, db.DbStats.Database().ResyncNumChanged.Value(), int64(docsToCreate))

			// Resume process
			err = db.ResyncManager.Start(ctx, options)
			require.NoError(t, err)

			stats = waitForResyncState(t, db, BackgroundProcessStateCompleted)

			assert.GreaterOrEqual(t, stats.DocsProcessed, int64(docsToCreate))
			assert.Equal(t, int64(docsToCreate), stats.DocsChanged)
			// DocsTargeted is preserved from the original run start, even after resume
			assert.Equal(t, initialDocsTargeted, stats.DocsTargeted)
			assert.Equal(t, int64(0), db.DbStats.Database().ResyncDocsTargeted.Value()) // resync finished so reset back to 0

			assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(docsToCreate))
			assert.Equal(t, int64(docsToCreate), db.DbStats.Database().ResyncNumChanged.Value())

			assert.GreaterOrEqual(t, db.DbStats.Database().SyncFunctionCount.Value(), int64(docsToCreate))
		})
	}
}

// TestResyncManagerDCPResumeStoppedProcessChangeCollections starts a resync with a single collection, stops it, and re-runs with an additional collection.
// Expects the resync process to reset with a new ID, and new checkpoints, and reprocess the full set of documents across both collections.
func TestResyncManagerDCPResumeStoppedProcessChangeCollections(t *testing.T) {
	base.TestRequiresCollections(t)
	for _, testCase := range ResyncTestModes() {
		t.Run(testCase.Name, func(t *testing.T) {

			docsPerCollection := int64(1000)
			// rosmar runs too quickly, increase doc count
			if base.UnitTestUrlIsWalrus() || testCase.Distributed {
				// Evalute in CBG-5419 whether increasing doc count is necessary
				docsPerCollection *= 5
			}
			const numCollections = 2
			totalDocCount := docsPerCollection * numCollections

			tb := base.GetTestBucket(t)
			defer tb.Close(base.TestCtx(t))
			dbOptions := DatabaseContextOptions{}
			dbOptions.Scopes = GetScopesOptions(t, tb, numCollections)

			db, ctx := SetupTestDBForBucketWithOptions(t, tb, dbOptions)
			defer db.Close(ctx)

			rs, ok := db.ResyncManager.Process.(*ResyncManagerDCP)
			require.True(t, ok)

			if testCase.Distributed {
				rs.Distributed = true
			} else {
				require.False(t, rs.Distributed, "Expected this database ResyncManager to be in non distributed mode")
			}

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
				"regenerateSequences": false,
				"collections":         base.NewCollectionNames(dbCollections[0].dataStore),
			}

			err := db.ResyncManager.Start(ctx, options)
			require.NoError(t, err)

			// Attempt to Stop Process after 1/5 of docs are processed but before it is completed
			wg := sync.WaitGroup{}
			defer base.WaitWithTimeout(t, &wg, 30*time.Second)
			wg.Go(func() {
				waitForResyncDocsProcessed(t, db, docsPerCollection/5)
				err = db.ResyncManager.Stop(ctx)
				require.NoError(t, err)
			})

			stats := waitForResyncState(t, db, BackgroundProcessStateStopped)

			require.Less(t, stats.DocsProcessed, docsPerCollection, "DocsProcessed is equal to docs created. Consider setting docsPerCollection > %d.", docsPerCollection)
			assert.Less(t, stats.DocsChanged, docsPerCollection)

			assert.Less(t, db.DbStats.Database().ResyncNumProcessed.Value(), docsPerCollection)
			assert.Less(t, db.DbStats.Database().ResyncNumChanged.Value(), docsPerCollection)

			firstDocsChanged := stats.DocsChanged

			require.GreaterOrEqual(t, len(dbCollections), 2)
			options["collections"] = db.collectionNames()

			// Resume process
			err = db.ResyncManager.Start(ctx, options)
			require.NoError(t, err)

			stats = waitForResyncState(t, db, BackgroundProcessStateCompleted)

			assert.GreaterOrEqual(t, stats.DocsProcessed, totalDocCount)
			assert.Equal(t, totalDocCount, stats.DocsChanged+firstDocsChanged)

			assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), totalDocCount)
			assert.Equal(t, totalDocCount, db.DbStats.Database().ResyncNumChanged.Value())

			assert.GreaterOrEqual(t, db.DbStats.Database().SyncFunctionCount.Value(), totalDocCount)
		})
	}
}

// testDBForResyncOptions defines options for setting up a test database for resync tests.
type testDBForResyncOptions struct {
	docsToCreate                 int  // number of documents to create
	updateSyncFuncAfterDocsAdded bool // update the sync function after documents have been added
	distributed                  bool // force distributed or non distributed mode for resync manager
}

// setupTestDBForResyncWithDocs creates a databases and seeds it with documents for setupTestDBForResyncWithDocs
func setupTestDBForResyncWithDocs(t testing.TB, opts testDBForResyncOptions) (*Database, context.Context) {
	db, ctx := setupTestDB(t)
	syncFn := `
function sync(doc, oldDoc){
	channel("channel.ABC");
}
`
	rs, ok := db.ResyncManager.Process.(*ResyncManagerDCP)
	require.True(t, ok)

	if opts.distributed {
		rs.Distributed = true
	} else {
		require.False(t, rs.Distributed, "Expected this database ResyncManager to be in non distributed mode")
	}

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, err := collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	// Create the docs that will be marked and not swept
	body := map[string]any{"foo": "bar"}
	for i := range opts.docsToCreate {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, _, err := collection.Put(ctx, key, body)
		require.NoError(t, err)
	}

	assert.Equal(t, opts.docsToCreate, int(db.DbStats.Database().SyncFunctionCount.Value()))
	db.DbStats.Database().SyncFunctionCount.Set(0)

	if opts.updateSyncFuncAfterDocsAdded {
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
	_, err = collection.dataStore.WriteCas(ctx, "sdkWrite", 0, 0, docBody, 0)
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
	assert.GreaterOrEqual(t, resyncStats.DocsProcessed, int64(2))
	assert.Equal(t, int64(2), resyncStats.DocsChanged)

	assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(2))
	assert.Equal(t, int64(2), db.DbStats.Database().ResyncNumChanged.Value())

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
	assert.GreaterOrEqual(t, resyncStats.DocsProcessed, int64(2))
	assert.Equal(t, int64(2), resyncStats.DocsChanged)

	assert.GreaterOrEqual(t, db.DbStats.Database().ResyncNumProcessed.Value(), int64(4))
	assert.Equal(t, int64(4), db.DbStats.Database().ResyncNumChanged.Value())

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
		"regenerateSequences": false,
		"collections":         base.NewCollectionNames(),
	}

	require.NoError(t, db.ResyncManager.Start(ctx, options))
	return waitForResyncState(t, db, BackgroundProcessStateCompleted)
}

// helper function to Unmarshal BackgroundProcess state into ResyncManagerResponseDCP
func getResyncStats(t testing.TB, db *Database) ResyncManagerResponseDCP {
	var resp ResyncManagerResponseDCP
	rawStatus, err := db.ResyncManager.GetStatus(base.TestCtx(t))
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
		// to stop the document, poll the local status rather than the cluster status which is updated on a timer
		rawStatus, _, err := db.ResyncManager.Process.GetProcessStatus(BackgroundManagerStatus{}, nil)
		require.NoError(c, err)
		var stats ResyncManagerResponseDCP
		assert.NoError(c, base.JSONUnmarshal(rawStatus, &stats))
		assert.Greater(c, stats.DocsProcessed, count)
	}, 1*time.Minute, 10*time.Millisecond)
}

func waitForResyncDocsChanged(t testing.TB, db *Database, count int64) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats := getResyncStats(t, db)
		assert.Equal(c, stats.DocsChanged, count)
		assert.GreaterOrEqual(c, stats.DocsProcessed, count)
	}, 5*time.Minute, 10*time.Millisecond)
}

func TestResyncCheckpointPrefix(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	defaultCollection := bucket.DefaultDataStore(ctx)
	customCollection := bucket.GetSingleDataStore()
	resyncID := "1234"
	testCases := []struct {
		name            string
		collectionNames base.CollectionNameSet
		groupID         string
		distributed     bool
		expected        string
	}{
		{
			name:            "default collection, no group id",
			collectionNames: base.NewCollectionNameSet(defaultCollection),
			groupID:         "",
			distributed:     false,
			expected:        fmt.Sprintf("_sync:dcp_ck::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name:            "default collection, group id=foo",
			collectionNames: base.NewCollectionNameSet(defaultCollection),
			groupID:         "foo",
			distributed:     false,
			expected:        fmt.Sprintf("_sync:dcp_ck:foo::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name: "default collection + collection 1, no group id",
			collectionNames: base.NewCollectionNameSet(
				defaultCollection,
				customCollection,
			),
			groupID:     "",
			distributed: false,
			expected:    fmt.Sprintf("_sync:dcp_ck::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name: "default collection + collection 1, group id=foo",
			collectionNames: base.NewCollectionNameSet(
				defaultCollection,
				customCollection,
			),
			groupID:     "foo",
			distributed: false,
			expected:    fmt.Sprintf("_sync:dcp_ck:foo::sg-%v:resync:1234", base.ProductAPIVersion),
		},
		{
			name:            "distributed, default collection, no group id",
			collectionNames: base.NewCollectionNameSet(defaultCollection),
			groupID:         "",
			distributed:     true,
			expected:        fmt.Sprintf("_sync:dcp_ck::sg-%v:resync-distributed:1234", base.ProductAPIVersion),
		},
		{
			name:            "distributed, default collection, group id=foo",
			collectionNames: base.NewCollectionNameSet(defaultCollection),
			groupID:         "foo",
			distributed:     true,
			expected:        fmt.Sprintf("_sync:dcp_ck::sg-%v:resync-distributed:1234", base.ProductAPIVersion),
		},
		{
			name: "distributed, default collection + collection 1, no group id",
			collectionNames: base.NewCollectionNameSet(
				defaultCollection,
				customCollection,
			),
			groupID:     "",
			distributed: true,
			expected:    fmt.Sprintf("_sync:dcp_ck::sg-%v:resync-distributed:1234", base.ProductAPIVersion),
		},
		{
			name: "distributed, default collection + collection 1, group id=foo",
			collectionNames: base.NewCollectionNameSet(
				defaultCollection,
				customCollection,
			),
			groupID:     "foo",
			distributed: true,
			expected:    fmt.Sprintf("_sync:dcp_ck::sg-%v:resync-distributed:1234", base.ProductAPIVersion),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
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
			clientOptions := db.ResyncManager.Process.(*ResyncManagerDCP).getDCPClientOptions(
				db,
				resyncID,
				test.collectionNames,
				func(sgbucket.FeedEvent) bool {
					// no-op for test, just need to provide a callback to satisfy function signature
					require.Fail(t, "unexpected feed event callback")
					return false
				},
				test.distributed,
			)

			dcpClient, err := base.NewDCPClient(
				ctx,
				bucket,
				clientOptions,
			)
			require.NoError(t, err)
			require.Equal(t, test.expected, dcpClient.GetMetadataKeyPrefix())
		})
	}
}

// TestResyncImportPartitionsPassthrough verifies that ResyncPartitions from UnsupportedOptions
// is passed through to StartShardedDCPFeed by inspecting the persisted CBGT plan pindexes
// in the metadata store after a distributed resync runs.
func TestResyncImportPartitionsPassthrough(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("Distributed resync requires EE")
	}
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Distributed resync not supported for rosmar")
	}

	// Must evenly divide 1024 vbuckets, otherwise CBGT creates an extra pindex for the remainder.
	numPartitions := uint16(4)
	dbcOptions := DatabaseContextOptions{
		UnsupportedOptions: &UnsupportedOptions{
			ResyncPartitions: &numPartitions,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	body := map[string]any{"foo": "bar"}
	// have some work for resync to perform
	for i := range 10 {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, _, err := collection.Put(ctx, key, body)
		require.NoError(t, err)
	}

	syncFn := `function sync(doc, oldDoc) { channel("resyncChannel"); }`
	_, err := collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	rs, ok := db.ResyncManager.Process.(*ResyncManagerDCP)
	require.True(t, ok)
	rs.Distributed = true

	options := map[string]any{
		"regenerateSequences": false,
		"collections":         base.NewCollectionNames(),
	}
	require.NoError(t, db.ResyncManager.Start(ctx, options))
	defer func() {
		require.NoError(t, db.ResyncManager.Stop(ctx))
	}()

	waitForResyncDocsChanged(t, db, int64(10))

	// Read the persisted CBGT plan pindexes from the resync cfg in the metadata store.
	resyncCfgPrefix := db.MetadataKeys.ResyncCfgPrefix()
	cfgKey := resyncCfgPrefix + cbgt.PLAN_PINDEXES_KEY

	var planPIndexesJSON []byte
	_, err = db.MetadataStore.Get(ctx, cfgKey, &planPIndexesJSON)
	require.NoError(t, err)

	var planPIndexes cbgt.PlanPIndexes
	require.NoError(t, base.JSONUnmarshal(planPIndexesJSON, &planPIndexes))
	require.Len(t, planPIndexes.PlanPIndexes, int(numPartitions),
		"expected %d pindexes matching ResyncPartitions", numPartitions)
}

// TestResyncManagerDCPCompletedVBucketsStatus verifies that completed vBuckets are correctly serialized in the
// "meta" section by GetProcessStatus and correctly merged (append-only) by SetProcessStatus.
func TestResyncManagerDCPCompletedVBucketsStatus(t *testing.T) {
	t.Run("GetProcessStatus serializes local completed vBuckets in meta", func(t *testing.T) {
		r := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}
		r.completedvBuckets.m["0"] = struct{}{}
		r.completedvBuckets.m["1"] = struct{}{}

		_, metaBytes, err := r.GetProcessStatus(BackgroundManagerStatus{}, nil)
		require.NoError(t, err)

		var meta ResyncManagerMeta
		require.NoError(t, base.JSONUnmarshal(metaBytes, &meta))
		assert.ElementsMatch(t, []string{"0", "1"}, meta.CompletedVBuckets)
	})

	t.Run("GetProcessStatus with no completed vBuckets omits field from meta", func(t *testing.T) {
		r := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}

		_, metaBytes, err := r.GetProcessStatus(BackgroundManagerStatus{}, nil)
		require.NoError(t, err)

		var meta ResyncManagerMeta
		require.NoError(t, base.JSONUnmarshal(metaBytes, &meta))
		assert.Empty(t, meta.CompletedVBuckets)
	})

	t.Run("GetProcessStatus unions local and previous-doc vBuckets in meta", func(t *testing.T) {
		r := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}
		r.completedvBuckets.m["0"] = struct{}{}
		r.completedvBuckets.m["1"] = struct{}{}

		// Simulate a full bucket doc written by another node with different vBuckets.
		prevDoc := ResyncManagerStatusDocDCP{
			ResyncManagerMeta: ResyncManagerMeta{
				resyncManagerCompletedVBuckets: resyncManagerCompletedVBuckets{
					CompletedVBuckets: []string{"2", "3"},
				},
			},
		}
		prevDocBytes, err := base.JSONMarshal(prevDoc)
		require.NoError(t, err)

		_, metaBytes, err := r.GetProcessStatus(BackgroundManagerStatus{}, prevDocBytes)
		require.NoError(t, err)

		var meta ResyncManagerMeta
		require.NoError(t, base.JSONUnmarshal(metaBytes, &meta))
		assert.ElementsMatch(t, []string{"0", "1", "2", "3"}, meta.CompletedVBuckets)

		// Local state must be unchanged — GetProcessStatus must not mutate completedvBuckets.
		r.completedvBuckets.lock.RLock()
		defer r.completedvBuckets.lock.RUnlock()
		assert.Equal(t, map[string]struct{}{"0": {}, "1": {}}, r.completedvBuckets.m)
	})

	t.Run("SetProcessStatus merges vBuckets from previous doc into local set", func(t *testing.T) {
		ctx := base.TestCtx(t)
		r := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}
		r.completedvBuckets.m["0"] = struct{}{}
		r.completedvBuckets.m["1"] = struct{}{}

		prevDoc := ResyncManagerStatusDocDCP{
			ResyncManagerMeta: ResyncManagerMeta{
				resyncManagerCompletedVBuckets: resyncManagerCompletedVBuckets{
					CompletedVBuckets: []string{"2", "3"},
				},
			},
		}
		prevDocBytes, err := base.JSONMarshal(prevDoc)
		require.NoError(t, err)

		r.SetProcessStatus(ctx, prevDocBytes, nil)

		r.completedvBuckets.lock.RLock()
		defer r.completedvBuckets.lock.RUnlock()
		assert.Equal(t, map[string]struct{}{"0": {}, "1": {}, "2": {}, "3": {}}, r.completedvBuckets.m)
	})

	t.Run("SetProcessStatus never removes existing vBuckets", func(t *testing.T) {
		ctx := base.TestCtx(t)
		r := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}
		r.completedvBuckets.m["0"] = struct{}{}
		r.completedvBuckets.m["1"] = struct{}{}

		// Previous doc has no completed vBuckets (e.g. from a node that hasn't finished any yet).
		prevDoc := ResyncManagerStatusDocDCP{}
		prevDocBytes, err := base.JSONMarshal(prevDoc)
		require.NoError(t, err)

		r.SetProcessStatus(ctx, prevDocBytes, nil)

		r.completedvBuckets.lock.RLock()
		defer r.completedvBuckets.lock.RUnlock()
		assert.Equal(t, map[string]struct{}{"0": {}, "1": {}}, r.completedvBuckets.m)
	})

	t.Run("cross-node accumulation: two simulated nodes converge on full vBucket set", func(t *testing.T) {
		ctx := base.TestCtx(t)
		// Simulate node A completing vBuckets 0-4 and node B completing vBuckets 5-9.
		// Verify that after status propagation each node's meta contains all 10 vBuckets.

		nodeA := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}
		for i := range 5 {
			nodeA.completedvBuckets.m[fmt.Sprintf("%d", i)] = struct{}{}
		}
		nodeB := &ResyncManagerDCP{
			completedvBuckets: newvBucketTracker(),
			db:                &DatabaseContext{},
		}
		for i := 5; i < 10; i++ {
			nodeB.completedvBuckets.m[fmt.Sprintf("%d", i)] = struct{}{}
		}

		// Node A writes status with no prior doc.
		_, metaA, err := nodeA.GetProcessStatus(BackgroundManagerStatus{}, nil)
		require.NoError(t, err)

		// Build a full bucket doc as the background manager would ({"status":..., "meta":...}).
		docFromA, err := base.JSONMarshal(map[string]json.RawMessage{
			"status": json.RawMessage("{}"),
			"meta":   metaA,
		})
		require.NoError(t, err)

		// Node B reads node A's doc: GetProcessStatus should union A's and B's vBuckets.
		_, metaB, err := nodeB.GetProcessStatus(BackgroundManagerStatus{}, docFromA)
		require.NoError(t, err)

		var parsedMetaB ResyncManagerMeta
		require.NoError(t, base.JSONUnmarshal(metaB, &parsedMetaB))
		assert.Len(t, parsedMetaB.CompletedVBuckets, 10, "meta should contain all 10 vBuckets")
		for i := range 10 {
			assert.Contains(t, parsedMetaB.CompletedVBuckets, fmt.Sprintf("%d", i))
		}

		// Node B's SetProcessStatus should also merge A's vBuckets into B's local tracker.
		nodeB.SetProcessStatus(ctx, docFromA, nil)

		nodeB.completedvBuckets.lock.RLock()
		localB := maps.Clone(nodeB.completedvBuckets.m)
		nodeB.completedvBuckets.lock.RUnlock()

		assert.Len(t, localB, 10, "node B local tracker should contain all 10 vBuckets after merge")
	})
}

// TestResyncManagerDCPWritesV1SyncInfoAtCcv41 verifies the end-to-end wiring from
// DatabaseContext.ClusterCompatVersion through ResyncManagerDCP.Run into
// base.SetSyncInfoMetadataID — when ccv>=4.1 the syncInfo doc is written with the V1
// version-byte prefix. regenerateSequences=true is required so the syncInfo update path runs.
func TestResyncManagerDCPWritesV1SyncInfoAtCcv41(t *testing.T) {
	docsToCreate := 10
	opts := testDBForResyncOptions{
		docsToCreate:                 docsToCreate,
		updateSyncFuncAfterDocsAdded: true,
	}
	db, ctx := setupTestDBForResyncWithDocs(t, opts)
	defer db.Close(ctx)

	ccv := base.NewClusterCompatVersion(4, 1)
	db.ClusterCompatVersionFunc = func() *base.ClusterCompatVersion { return &ccv }
	// Resync only writes syncInfo when MetadataID is non-empty (SetSyncInfoMetadataID is a no-op otherwise).
	db.Options.MetadataID = "test_resync_md_id"

	dbc, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	options := map[string]any{
		"regenerateSequences": true,
		"collections":         base.NewCollectionNames(),
	}
	require.NoError(t, db.ResyncManager.Start(ctx, options))
	RequireBackgroundManagerState(t, db.ResyncManager, BackgroundProcessStateCompleted)

	raw, _, err := dbc.dataStore.GetRaw(ctx, base.SGSyncInfo)
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.Equal(t, byte(base.SyncInfoTypeV1), raw[0], "expected V1 prefix byte from resync write at ccv 4.1")
}
