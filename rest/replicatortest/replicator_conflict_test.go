//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package replicatortest

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveReplicatorHLVConflictRemoteAndLocalWins(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges)
	base.RequireNumTestBuckets(t, 2)
	testCases := []struct {
		name            string
		conflictResType db.ConflictResolverType
	}{
		{
			name:            "HLV Conflict Remote Wins",
			conflictResType: db.ConflictResolverRemoteWins,
		},
		{
			name:            "HLV Conflict Local Wins",
			conflictResType: db.ConflictResolverLocalWins,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResType, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			var expectedConflictResBody []byte

			for i := 0; i < 10; i++ {
				version = rt2.UpdateDoc(docID, version, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i))
			}
			rt2.WaitForPendingChanges()
			for i := 0; i < 10; i++ {
				rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
			}
			rt1.WaitForPendingChanges()

			rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()

			var nonWinningRevPreConflict rest.DocVersion
			if testCase.conflictResType == db.ConflictResolverRemoteWins {
				docToPull, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				expectedConflictResBody, err = docToPull.BodyBytes(rt2ctx)
				require.NoError(t, err)

				nonWinningRevPreConflict = rt1Version
			} else {
				localDoc, err := rt1collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				expectedConflictResBody, err = localDoc.BodyBytes(rt2ctx)
				require.NoError(t, err)

				nonWinningRevPreConflict = version
			}

			since, err := rt1collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)

			// grab local doc body and assert it is as expected
			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)
			assert.Equal(t, expectedConflictResBody, actualBody)

			// assert HLV/rev tree is as expected
			if testCase.conflictResType == db.ConflictResolverRemoteWins {
				rest.RequireDocVersionEqual(t, version, rt1Doc.ExtractDocVersion())
				// PV should have the source version pair from the updates on rt1
				require.Len(t, rt1Doc.HLV.PreviousVersions, 1)
				assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.PreviousVersions[nonWinningRevPreConflict.CV.SourceID])

				// remote wins tombstones local rev and takes remote rev tree to write as active
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, version.RevTreeID, rt1Version.RevTreeID)
			} else {
				localWinsVersion := rt1Version
				localWinsVersion.CV.Value = rt1Doc.Cas // will generate a new CV value
				// local wins will generate a new rev ID as child of remote RevID
				remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
				newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, version.RevTreeID, expectedConflictResBody)
				localWinsVersion.RevTreeID = newRevID
				rest.RequireDocVersionEqual(t, localWinsVersion, rt1Doc.ExtractDocVersion())
				require.Len(t, rt1Doc.HLV.MergeVersions, 2)
				assert.Equal(t, rt1Version.CV.Value, rt1Doc.HLV.MergeVersions[rt1Version.CV.SourceID])
				assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.MergeVersions[nonWinningRevPreConflict.CV.SourceID])
				require.Len(t, rt1Doc.HLV.PreviousVersions, 0)

				// check tombstoned leaf previous local active rev and active rev is above generated rev ID
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, localWinsVersion.RevTreeID, rt1Version.RevTreeID)
			}

		})
	}
}

func TestActiveReplicatorLWWDefaultResolver(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges)
	base.RequireNumTestBuckets(t, 2)
	// Passive
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "passivedb",
			},
		},
	})
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "activedb",
			},
		},
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	docID := "doc1_"
	version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
	rt2.WaitForPendingChanges()

	resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:           200,
		ReplicationStatsMap:        dbReplicatorStats(t),
		ConflictResolverFuncForHLV: resolverFunc,
		CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:                 false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	changesResults.RequireDocIDs(t, []string{docID})
	rt1Version, _ := rt1.GetDoc(docID)
	rest.RequireDocVersionEqual(t, version, rt1Version)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	var expectedConflictResBody []byte
	var expectedWinner rest.DocVersion
	var nonWinningRevPreConflict rest.DocVersion

	for i := 0; i < 10; i++ {
		version = rt2.UpdateDoc(docID, version, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i))
	}
	rt2.WaitForPendingChanges()
	for i := 0; i < 10; i++ {
		rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
	}
	rt1.WaitForPendingChanges()

	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()

	// pull out expected winner and expected conflict response body
	var localWins bool
	if rt1Version.CV.Value > version.CV.Value {
		localWinsCaseDoc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)
		expectedWinner = rt1Version
		expectedConflictResBody, err = localWinsCaseDoc.BodyBytes(rt2ctx)
		require.NoError(t, err)
		nonWinningRevPreConflict = version
		localWins = true
	} else {
		localDoc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)
		expectedWinner = version
		expectedConflictResBody, err = localDoc.BodyBytes(rt2ctx)
		require.NoError(t, err)
		nonWinningRevPreConflict = rt1Version
		localWins = false
	}

	since, err := rt1collection.LastSequence(rt1ctx)
	require.NoError(t, err)

	// start again for new revisions
	require.NoError(t, ar.Start(ctx1))

	changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	if localWins {
		expectedWinner.CV.Value = rt1Doc.Cas
		// local wins will gen a new rev ID as child of remote RevID
		remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
		newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, version.RevTreeID, expectedConflictResBody)
		expectedWinner.RevTreeID = newRevID
	}
	rest.RequireDocVersionEqual(t, expectedWinner, rt1Doc.ExtractDocVersion())
	// PV should have the source version pair from the updates on rt1
	if !localWins {
		require.Len(t, rt1Doc.HLV.PreviousVersions, 1)
		assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.PreviousVersions[nonWinningRevPreConflict.CV.SourceID])

		// remote wins:
		//	- tombstones local active revision
		// 	- winning rev is incoming active rev
		docHistoryLeaves := rt1Doc.History.GetLeaves()
		require.Len(t, docHistoryLeaves, 2)
		rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, expectedWinner.RevTreeID, nonWinningRevPreConflict.RevTreeID)
	} else {
		require.Len(t, rt1Doc.HLV.MergeVersions, 2)
		assert.Equal(t, rt1Version.CV.Value, rt1Doc.HLV.MergeVersions[rt1Version.CV.SourceID])
		assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.MergeVersions[nonWinningRevPreConflict.CV.SourceID])
		require.Len(t, rt1Doc.HLV.PreviousVersions, 0)

		// local wins:
		// - tombstones local active revision
		// - winning rev is written as child of remote winning rev
		docHistoryLeaves := rt1Doc.History.GetLeaves()
		require.Len(t, docHistoryLeaves, 2)
		rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, expectedWinner.RevTreeID, rt1Version.RevTreeID)
	}
	// grab local doc body and assert it is as expected
	actualBody, err := rt1Doc.BodyBytes(rt1ctx)
	require.NoError(t, err)
	assert.Equal(t, expectedConflictResBody, actualBody)
}

func TestActiveReplicatorLocalWinsCases(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	const (
		testSource1 = "xyz"
		testSource2 = "abc"
		testSource3 = "def"
		testSource4 = "ghi"
	)

	testCases := []struct {
		name        string
		pvForRemote db.HLVVersions
		pvForLocal  db.HLVVersions
		mvForLocal  db.HLVVersions
		mvForRemote db.HLVVersions
		expectedPV  db.HLVVersions
	}{
		{
			name: "PV on remote doc has common elements that are less than with local PV",
			pvForRemote: db.HLVVersions{
				testSource1: 100,
				testSource2: 200,
			},
			pvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
		},
		{
			name: "PV on remote doc has common elements that are greater than with local PV",
			pvForRemote: db.HLVVersions{
				testSource1: 300,
				testSource2: 400,
			},
			pvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 300,
				testSource2: 400,
			},
		},
		{
			name: "no common elements between two PVs",
			pvForRemote: db.HLVVersions{
				testSource1: 100,
				testSource2: 200,
			},
			pvForLocal: db.HLVVersions{
				testSource3: 100,
				testSource4: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 100,
				testSource2: 200,
				testSource3: 100,
				testSource4: 300,
			},
		},
		{
			name: "local doc has MV defined before conflict res",
			mvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			pvForLocal: db.HLVVersions{
				testSource3: 100,
			},
			expectedPV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
				testSource3: 100,
			},
		},
		{
			name: "remote doc has MV defined before conflict res",
			mvForRemote: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			pvForLocal: db.HLVVersions{
				testSource3: 100,
			},
			expectedPV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
				testSource3: 100,
			},
		},
		{
			name: "both local and remote have MV defined before conflict res",
			mvForRemote: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			mvForLocal: db.HLVVersions{
				testSource3: 200,
				testSource4: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
				testSource3: 200,
				testSource4: 300,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()
			ctx2 := rt2.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverLocalWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// alter remote doc HLV to have MV
			newHLVForRemote := &db.HybridLogicalVector{
				Version:          math.MaxUint64, // will macro expand
				SourceID:         version.CV.SourceID,
				PreviousVersions: testCase.pvForRemote,
				MergeVersions:    testCase.mvForRemote,
			}

			// create local hlv for conflict (simulating an update locally)
			newHLVForLocal := &db.HybridLogicalVector{
				SourceID: rt1.GetDatabase().EncodedSourceID,
				Version:  math.MaxUint64, // will macro expand
				PreviousVersions: map[string]uint64{
					rt1Version.CV.SourceID: rt1Version.CV.Value, // move current cv to pv
				},
				MergeVersions: testCase.mvForLocal,
			}
			for key, value := range testCase.pvForLocal {
				newHLVForLocal.PreviousVersions[key] = value
			}

			docBody := map[string]interface{}{
				"source":   "rt2",
				"channels": []string{"alice"},
				"some":     "data",
			}
			remoteCas := db.AlterHLVForTest(t, ctx2, rt2.GetSingleDataStore(), docID, newHLVForRemote, docBody)
			remoteDocPreConflict, _ := rt2.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			// simplify local doc body to one field so we can easily generate expected revID for local wins case
			docBody = map[string]interface{}{
				"channels": []string{"alice"},
			}
			localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
			localDocPreConflict, localDocPreConflictBody := rt1.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt1.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			expectedHLV := &db.HybridLogicalVector{
				SourceID:         rt1.GetDatabase().EncodedSourceID,
				MergeVersions:    make(db.HLVVersions),
				PreviousVersions: testCase.expectedPV,
			}
			// add current CV's to expected MV
			expectedHLV.MergeVersions[rt1.GetDatabase().EncodedSourceID] = localCas
			expectedHLV.MergeVersions[rt2.GetDatabase().EncodedSourceID] = remoteCas

			rt1.WaitForPendingChanges()
			rt2.WaitForPendingChanges()

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			since, err := rt1collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			// new cv will have been generated for local wins
			assert.Equal(t, rt1Doc.Cas, cvValue)

			// local wins:
			//	- tombstones local active revision
			//  - winning rev is written as child of remote winning rev
			remoteGeneration, _ := db.ParseRevID(ctx1, remoteDocPreConflict.RevTreeID)
			newRevID, err := db.CreateRevID(remoteGeneration+1, remoteDocPreConflict.RevTreeID, localDocPreConflictBody)
			require.NoError(t, err)
			assert.Equal(t, newRevID, rt1Doc.SyncData.GetRevTreeID())
			docHistoryLeaves := rt1Doc.History.GetLeaves()
			require.Len(t, docHistoryLeaves, 2)
			rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, newRevID, localDocPreConflict.RevTreeID)

			// assert on pv
			require.Len(t, rt1Doc.HLV.PreviousVersions, len(testCase.expectedPV))
			for key, val := range testCase.expectedPV {
				assert.Equal(t, val, rt1Doc.HLV.PreviousVersions[key], "Expected key or value is missing in previous versions")
			}
			// assert on mv
			require.Len(t, rt1Doc.HLV.MergeVersions, len(expectedHLV.MergeVersions))
			for key, val := range expectedHLV.MergeVersions {
				assert.Equal(t, val, rt1Doc.HLV.MergeVersions[key], "Expected key or value is missing in merge versions")
			}

			// assert on body
			localDocPreConflictBody = rest.StripInternalProperties(t, localDocPreConflictBody)
			expectedBytes := base.MustJSONMarshal(t, localDocPreConflictBody)
			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedBytes), string(actualBody))
		})
	}
}

func TestActiveReplicatorRemoteWinsCases(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	const (
		testSource1 = "xyz"
		testSource2 = "abc"
		testSource3 = "def"
		testSource4 = "ghi"
	)

	testCases := []struct {
		name        string
		mvForRemote db.HLVVersions
		mvForLocal  db.HLVVersions
		pvForRemote db.HLVVersions
		pvForLocal  db.HLVVersions
		expectedMV  db.HLVVersions
		expectedPV  db.HLVVersions
	}{
		{
			name: "PV on remote doc has common elements that are less than with local PV",
			pvForRemote: db.HLVVersions{
				testSource1: 100,
				testSource2: 200,
			},
			pvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
		},
		{
			name: "PV on remote doc has common elements that are greater than with local PV",
			pvForRemote: db.HLVVersions{
				testSource1: 300,
				testSource2: 400,
			},
			pvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 300,
				testSource2: 400,
			},
		},
		{
			name: "no common elements between two PVs",
			pvForRemote: db.HLVVersions{
				testSource1: 100,
				testSource2: 200,
			},
			pvForLocal: db.HLVVersions{
				testSource3: 100,
				testSource4: 300,
			},
			expectedPV: db.HLVVersions{
				testSource1: 100,
				testSource2: 200,
				testSource3: 100,
				testSource4: 300,
			},
		},
		{
			name: "local doc has MV defined before conflict res",
			mvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			pvForLocal: db.HLVVersions{
				testSource3: 100,
			},
			expectedPV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
				testSource3: 100,
			},
		},
		{
			name: "remote doc has MV defined before conflict res",
			mvForRemote: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			pvForLocal: db.HLVVersions{
				testSource3: 100,
			},
			expectedPV: db.HLVVersions{
				testSource3: 100,
			},
			expectedMV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
		},
		{
			name: "both local and remote have MV defined before conflict res",
			mvForRemote: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			mvForLocal: db.HLVVersions{
				testSource3: 200,
				testSource4: 300,
			},
			expectedMV: db.HLVVersions{
				testSource1: 200,
				testSource2: 300,
			},
			expectedPV: db.HLVVersions{
				testSource3: 200,
				testSource4: 300,
			},
		},
		{
			// invalid case but good to assert on
			name: "both local and remote have MV defined and remote has common element less than local",
			mvForRemote: db.HLVVersions{
				testSource1: 100,
				testSource2: 300,
			},
			mvForLocal: db.HLVVersions{
				testSource1: 200,
				testSource4: 300,
			},
			expectedPV: db.HLVVersions{
				testSource4: 300,
			},
			expectedMV: db.HLVVersions{
				testSource1: 100,
				testSource2: 300,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()
			ctx2 := rt2.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverRemoteWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// alter remote doc HLV to have MV
			newHLVForRemote := &db.HybridLogicalVector{
				Version:          math.MaxUint64, // will macro expand
				SourceID:         version.CV.SourceID,
				MergeVersions:    testCase.mvForRemote,
				PreviousVersions: testCase.pvForRemote,
			}

			// create local hlv for conflict (simulating an update locally)
			newHLVForLocal := &db.HybridLogicalVector{
				SourceID: rt1.GetDatabase().EncodedSourceID,
				Version:  math.MaxUint64, // will macro expand
				PreviousVersions: map[string]uint64{
					rt1Version.CV.SourceID: rt1Version.CV.Value, // move current cv to pv
				},
				MergeVersions: testCase.mvForLocal,
			}
			for key, value := range testCase.pvForLocal {
				newHLVForLocal.PreviousVersions[key] = value
			}

			docBody := map[string]interface{}{
				"source":   "rt2",
				"channels": []string{"alice"},
				"some":     "data",
			}
			remoteCas := db.AlterHLVForTest(t, ctx1, rt2.GetSingleDataStore(), docID, newHLVForRemote, docBody)
			remoteDocVersionPreConflict, remoteDocBodyPreConflict := rt2.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			docBody = map[string]interface{}{
				"source":   "rt1",
				"channels": []string{"alice"},
			}
			localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
			localDocVersionPreConflict, _ := rt1.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt1.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			expectedHLV := &db.HybridLogicalVector{
				SourceID:      rt2.GetDatabase().EncodedSourceID,
				Version:       remoteCas,
				MergeVersions: testCase.expectedMV,
				PreviousVersions: map[string]uint64{
					rt1.GetDatabase().EncodedSourceID: localCas,
				},
			}
			for key, value := range testCase.expectedPV {
				expectedHLV.PreviousVersions[key] = value
			}

			rt1.WaitForPendingChanges()
			rt2.WaitForPendingChanges()

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			since, err := rt1collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			assert.Equal(t, expectedHLV.Version, cvValue)
			// remote wins:
			//	- tombstones local active revision
			// 	- winning rev is incoming active rev
			assert.Equal(t, remoteDocVersionPreConflict.RevTreeID, rt1Doc.GetRevTreeID())
			docHistoryLeaves := rt1Doc.History.GetLeaves()
			require.Len(t, docHistoryLeaves, 2)
			rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, remoteDocVersionPreConflict.RevTreeID, localDocVersionPreConflict.RevTreeID)

			assert.Len(t, rt1Doc.HLV.PreviousVersions, len(expectedHLV.PreviousVersions))
			for key, val := range testCase.expectedPV {
				assert.Equal(t, val, rt1Doc.HLV.PreviousVersions[key], "Expected key or value is missing in previous versions")
			}
			// assert on mv
			assert.Len(t, rt1Doc.HLV.MergeVersions, len(expectedHLV.MergeVersions))
			for key, val := range expectedHLV.MergeVersions {
				assert.Equal(t, val, rt1Doc.HLV.MergeVersions[key], "Expected key or value is missing in merge versions")
			}

			remoteDocBodyPreConflict = rest.StripInternalProperties(t, remoteDocBodyPreConflict)
			expectedJsonBody := base.MustJSONMarshal(t, remoteDocBodyPreConflict)
			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedJsonBody), string(actualBody))
		})
	}
}

func TestActiveReplicatorHLVConflictNoCommonMVPV(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name                 string
		localMV              db.HLVVersions
		remoteMV             db.HLVVersions
		localPV              db.HLVVersions
		remotePV             db.HLVVersions
		expectedMV           db.HLVVersions
		expectedPV           db.HLVVersions
		conflictResolverType db.ConflictResolverType
	}{
		{
			name: "No common MV or PV remote wins",
			localMV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
			},
			remoteMV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			localPV: db.HLVVersions{
				"pv1": 100,
				"pv2": 200,
			},
			remotePV: db.HLVVersions{
				"pv3": 100,
				"pv4": 200,
			},
			expectedPV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
				"pv1": 100,
				"pv2": 200,
				"pv3": 100,
				"pv4": 200,
			},
			expectedMV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			conflictResolverType: db.ConflictResolverRemoteWins,
		},
		{
			name: "No common MV or PV local wins",
			localMV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
			},
			remoteMV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			localPV: db.HLVVersions{
				"pv1": 100,
				"pv2": 200,
			},
			remotePV: db.HLVVersions{
				"pv3": 100,
				"pv4": 200,
			},
			expectedPV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
				"xyz": 100,
				"abc": 200,
				"pv1": 100,
				"pv2": 200,
				"pv3": 100,
				"pv4": 200,
			},
			conflictResolverType: db.ConflictResolverLocalWins,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()
			ctx2 := rt2.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// alter remote doc HLV to have MV
			newHLVForRemote := &db.HybridLogicalVector{
				Version:          math.MaxUint64, // will macro expand
				SourceID:         version.CV.SourceID,
				MergeVersions:    testCase.remoteMV,
				PreviousVersions: testCase.remotePV,
			}

			// create local hlv for conflict (simulating an update locally)
			newHLVForLocal := &db.HybridLogicalVector{
				SourceID: rt1.GetDatabase().EncodedSourceID,
				Version:  math.MaxUint64, // will macro expand
				PreviousVersions: map[string]uint64{
					rt1Version.CV.SourceID: rt1Version.CV.Value, // move current cv to pv
				},
				MergeVersions: testCase.localMV,
			}
			for key, value := range testCase.localPV {
				newHLVForLocal.PreviousVersions[key] = value
			}

			docBody := map[string]interface{}{
				"some":     "data",
				"source":   "rt2",
				"channels": []string{"alice"},
			}
			remoteCas := db.AlterHLVForTest(t, ctx1, rt2.GetSingleDataStore(), docID, newHLVForRemote, docBody)
			remoteDocVersionPreConflict, remoteDocBodyPreConflict := rt2.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			// simplify local doc body to one field so we can easily generate expected revID for local wins case
			docBody = map[string]interface{}{
				"channels": []string{"alice"},
			}
			localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
			localDocVersionPreConflict, localDocBodyPreConflict := rt1.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt1.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			rt1.WaitForPendingChanges()
			rt2.WaitForPendingChanges()

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			since, err := rt1collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			var expectedWinnerPreConflict db.Body
			var expectedHLV *db.HybridLogicalVector
			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				expectedHLV = &db.HybridLogicalVector{
					SourceID:      rt2.GetDatabase().EncodedSourceID,
					Version:       remoteCas,
					MergeVersions: testCase.expectedMV,
					PreviousVersions: map[string]uint64{
						rt1.GetDatabase().EncodedSourceID: localCas,
					},
				}
				expectedWinnerPreConflict = remoteDocBodyPreConflict
			} else {
				expectedHLV = &db.HybridLogicalVector{
					SourceID: rt1.GetDatabase().EncodedSourceID,
					MergeVersions: db.HLVVersions{
						rt1.GetDatabase().EncodedSourceID: localCas,
						rt2.GetDatabase().EncodedSourceID: remoteCas,
					},
					PreviousVersions: make(db.HLVVersions),
				}
				expectedWinnerPreConflict = localDocBodyPreConflict
			}
			// add extra pv expected
			for key, value := range testCase.expectedPV {
				expectedHLV.PreviousVersions[key] = value
			}

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			if testCase.conflictResolverType == db.ConflictResolverLocalWins {
				// writes new cv
				expectedHLV.Version = rt1Doc.Cas
			}
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			assert.Equal(t, expectedHLV.Version, cvValue)

			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// remote wins:
				//	- tombstones local active revision
				// 	- winning rev is incoming active rev
				assert.Equal(t, remoteDocVersionPreConflict.RevTreeID, rt1Doc.GetRevTreeID())
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, remoteDocVersionPreConflict.RevTreeID, localDocVersionPreConflict.RevTreeID)
			} else {
				// local wins:
				//	- tombstones local active revision
				//  - winning rev is written as child of remote winning rev
				remoteGeneration, _ := db.ParseRevID(ctx1, remoteDocVersionPreConflict.RevTreeID)
				newRevID, err := db.CreateRevID(remoteGeneration+1, remoteDocVersionPreConflict.RevTreeID, localDocBodyPreConflict)
				require.NoError(t, err)
				assert.Equal(t, newRevID, rt1Doc.SyncData.GetRevTreeID())
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, newRevID, localDocVersionPreConflict.RevTreeID)
			}

			assert.Len(t, rt1Doc.HLV.PreviousVersions, len(expectedHLV.PreviousVersions))
			for key, value := range expectedHLV.PreviousVersions {
				assert.Equal(t, value, rt1Doc.HLV.PreviousVersions[key], "Expected key %s to have value %d in previous versions", key, value)
			}
			assert.Len(t, rt1Doc.HLV.MergeVersions, len(expectedHLV.MergeVersions))
			for key, value := range expectedHLV.MergeVersions {
				assert.Equal(t, value, rt1Doc.HLV.MergeVersions[key], "Expected key %s to have value %d in merge versions", key, value)
			}

			expectedWinnerPreConflict = rest.StripInternalProperties(t, expectedWinnerPreConflict)
			expectedJsonBody := base.MustJSONMarshal(t, expectedWinnerPreConflict)
			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedJsonBody), string(actualBody))
		})
	}
}

func TestActiveReplicatorAttachmentHandling(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name                 string
		conflictResolverType db.ConflictResolverType
	}{
		{
			name:                 "remote wins",
			conflictResolverType: db.ConflictResolverRemoteWins,
		},
		{
			name:                 "local wins",
			conflictResolverType: db.ConflictResolverLocalWins,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// update remote doc to have two attachments
			version = rt2.UpdateDoc(docID, version, `{"source":"rt2", "channels": ["alice"], "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}, "world.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
			// update local to have one different attachment
			rt1Version = rt1.UpdateDoc(docID, rt1Version, `{"source":"rt1", "channels": ["alice"], "_attachments": {"different.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
			rt2.WaitForPendingChanges()
			rt1.WaitForPendingChanges()

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			since, err := rt1collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			_, localDocBodyPreConflict := rt1.GetDoc(docID) // get body for later comparison + revID assertion
			localDocBodyPreConflict = rest.StripInternalProperties(t, localDocBodyPreConflict)
			_, remoteDocBodyPreConflict := rt2.GetDoc(docID)
			remoteDocBodyPreConflict = rest.StripInternalProperties(t, remoteDocBodyPreConflict)

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)

			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)

			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// remote wins, so we expect the remote attachments to be present
				rest.RequireDocVersionEqual(t, version, rt1Doc.ExtractDocVersion())
				// remote wins:
				//	- tombstones local active revision
				// 	- winning rev is incoming active rev
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, version.RevTreeID, rt1Version.RevTreeID)

				attMeta := rt1Doc.Attachments()
				require.Len(t, attMeta, 2)
				base.RequireKeysEqual(t, []string{"hello.txt", "world.txt"}, attMeta)

				expectedBodyBytes := base.MustJSONMarshal(t, remoteDocBodyPreConflict)
				require.JSONEq(t, string(expectedBodyBytes), string(actualBody), "Doc body does not match expected for remote wins")
			} else {
				// local wins, so we expect the local attachment to be present
				// local wins:
				//	- tombstones local active revision
				//  - winning rev is written as child of remote winning rev
				remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
				newRevID, err := db.CreateRevID(remoteGeneration+1, version.RevTreeID, localDocBodyPreConflict)
				require.NoError(t, err)
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, newRevID, rt1Version.RevTreeID)

				rt1Version.CV.Value = rt1Doc.Cas // local wins writes a new CV
				rt1Version.RevTreeID = newRevID
				rest.RequireDocVersionEqual(t, rt1Version, rt1Doc.ExtractDocVersion())

				attMeta := rt1Doc.Attachments()
				require.Len(t, attMeta, 1)
				base.RequireKeysEqual(t, []string{"different.txt"}, attMeta)

				expectedBodyBytes := base.MustJSONMarshal(t, localDocBodyPreConflict)
				require.JSONEq(t, string(expectedBodyBytes), string(actualBody), "Doc body does not match expected for local wins")
			}
		})
	}
}

func TestActiveReplicatorHLVConflictWinnerIsTombstone(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name                 string
		conflictResolverType db.ConflictResolverType
	}{
		{
			name:                 "remote wins",
			conflictResolverType: db.ConflictResolverRemoteWins,
		},
		{
			name:                 "local wins",
			conflictResolverType: db.ConflictResolverLocalWins,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// update winning rev to be a tombstone
			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// remote wins, so we update the remote doc to be a tombstone
				version = rt2.DeleteDoc(docID, version)

				rt1Version = rt1.UpdateDoc(docID, rt1Version, `{"source":"rt1","channels":["alice"]}`) // create conflicting update
			} else {
				// need to update remote doc to have something to replicate
				version = rt2.UpdateDoc(docID, version, `{"source":"rt2","channels":["alice"]}`)
				// local wins, so we update the local doc to be a tombstone
				rt1Version = rt1.DeleteDoc(docID, rt1Version)
			}
			rt2.WaitForPendingChanges()
			rt1.WaitForPendingChanges()

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			since, err := rt1collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)

			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// here local revIDs gets promoted back to active as the local rev tree is 3-... but remote is 2-...
				version.RevTreeID = "3-83cfc92a9f5b96a5a7df0e5d01c64cfb"
				rest.RequireDocVersionEqual(t, version, rt1Doc.ExtractDocVersion())
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				// both branches should be tombstones
				for _, revID := range docHistoryLeaves {
					revItem, ok := rt1Doc.History[revID]
					require.True(t, ok)
					assert.True(t, revItem.Deleted)
				}
			} else {
				// local wins:
				//	- tombstones local active revision
				//  - winning rev is written as child of remote winning rev
				remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
				newRevID, err := db.CreateRevID(remoteGeneration+1, version.RevTreeID, db.Body{})
				require.NoError(t, err)
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)

				rt1Version.RevTreeID = newRevID
				rt1Version.CV.Value = rt1Doc.Cas // local wins writes a new CV
				rest.RequireDocVersionEqual(t, rt1Version, rt1Doc.ExtractDocVersion())

				// both branches should be tombstones
				for _, revID := range docHistoryLeaves {
					revItem, ok := rt1Doc.History[revID]
					require.True(t, ok)
					assert.True(t, revItem.Deleted)
				}
			}
			assert.True(t, rt1Doc.IsDeleted(), "Expected document to be a tombstone after remote wins conflict resolution")
		})
	}
}

func TestActiveReplicatorInvalidCustomResolver(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	// Passive
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "passivedb",
			},
		},
	})
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{"*"})

	// Active
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "activedb",
			},
		},
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	rt1Collection, rt1Ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	rt2Collection, rt2Ctx := rt2.GetSingleTestDatabaseCollectionWithUser()

	docID := t.Name()
	newDoc := db.CreateTestDocument(docID, "", rest.JsonToMap(t, `{"some": "data"}`), false, 0)
	incomingHLV := &db.HybridLogicalVector{
		SourceID: "abc",
		Version:  1234,
	}
	// add local version
	_, _, _, err := rt1Collection.PutExistingCurrentVersion(rt1Ctx, newDoc, incomingHLV, nil, nil, false, db.ConflictResolvers{}, false)
	require.NoError(t, err)

	newDoc = db.CreateTestDocument(docID, "", rest.JsonToMap(t, `{"some": "data"}`), false, 0)
	incomingHLV = &db.HybridLogicalVector{
		SourceID: "def",
		Version:  1234,
	}
	_, _, _, err = rt2Collection.PutExistingCurrentVersion(rt2Ctx, newDoc, incomingHLV, nil, nil, false, db.ConflictResolvers{}, false)
	require.NoError(t, err)

	resolver := `function(conflict) {var mergedDoc = new Object(); 
										mergedDoc._cv = "@";
										return mergedDoc;}` // invalid - setting cv to something that doesn't match either doc
	customConflictResolver, err := db.NewCustomConflictResolver(ctx1, resolver, rt1.GetDatabase().Options.JavascriptTimeout)
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:           200,
		ReplicationStatsMap:        dbReplicatorStats(t),
		ConflictResolverFunc:       customConflictResolver,
		ConflictResolverFuncForHLV: customConflictResolver,
		CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:                 true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, ar.Stop()) }()

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status := ar.GetStatus(ctx1)
		assert.Equal(c, 1, int(status.PullReplicationStatus.RejectedLocal))
	}, 10*time.Second, 100*time.Millisecond)
}

func TestActiveReplicatorHLVConflictCustom(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	testCases := []struct {
		name                    string
		localBody               string
		remoteBody              string
		expectedBody            string
		conflictResolver        string
		expectedDocPushResolved bool
		mergeVersionsExpected   bool
		newCVGenerated          bool
	}{
		{
			name:                  "remote wins call",
			conflictResolver:      `function(conflict) {return conflict.RemoteDocument;}`,
			localBody:             `{"source": "local"}`,
			expectedBody:          `{"source": "remote"}`,
			remoteBody:            `{"source": "remote"}`,
			mergeVersionsExpected: false,
			newCVGenerated:        false,
		},
		{
			name:                    "local wins call",
			conflictResolver:        `function(conflict) {return conflict.LocalDocument;}`,
			localBody:               `{"source":"local"}`,
			expectedBody:            `{"source":"local"}`,
			remoteBody:              `{"source":"remote"}`,
			expectedDocPushResolved: true,
			mergeVersionsExpected:   true,
			newCVGenerated:          true,
		},
		{
			name: "merge",
			conflictResolver: `function(conflict) {
							var mergedDoc = new Object();
							mergedDoc.source = "merged";
							return mergedDoc;
						}`,
			localBody:               `{"source":"local"}`,
			expectedBody:            `{"source":"merged"}`,
			remoteBody:              `{"source":"remote"}`,
			expectedDocPushResolved: true,
			mergeVersionsExpected:   true,
			newCVGenerated:          true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{"*"})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			rt1Collection, rt1Ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			rt2Collection, rt2Ctx := rt2.GetSingleTestDatabaseCollectionWithUser()

			docID := "doc1_" + testCase.name
			newDoc := db.CreateTestDocument(docID, "", rest.JsonToMap(t, testCase.localBody), false, 0)
			incomingHLV := &db.HybridLogicalVector{
				SourceID: "abc",
				Version:  1234,
			}
			// add local version
			localDoc, _, _, err := rt1Collection.PutExistingCurrentVersion(rt1Ctx, newDoc, incomingHLV, nil, nil, false, db.ConflictResolvers{}, false)
			require.NoError(t, err)

			newDoc = db.CreateTestDocument(docID, "", rest.JsonToMap(t, testCase.remoteBody), false, 0)
			incomingHLV = &db.HybridLogicalVector{
				SourceID: "def",
				Version:  1234,
			}
			remoteDoc, _, _, err := rt2Collection.PutExistingCurrentVersion(rt2Ctx, newDoc, incomingHLV, nil, nil, false, db.ConflictResolvers{}, false)
			require.NoError(t, err)

			customConflictResolver, err := db.NewCustomConflictResolver(ctx1, testCase.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFunc:       customConflictResolver,
				ConflictResolverFuncForHLV: customConflictResolver,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 true,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				status := ar.GetStatus(ctx1)
				assert.Equal(c, 1, int(status.PullReplicationStatus.DocsRead))
				assert.Equal(c, 1, int(status.PushReplicationStatus.DocWriteConflict))
				if testCase.expectedDocPushResolved {
					assert.Equal(c, 1, int(status.PushReplicationStatus.DocsWritten))
				}
			}, 10*time.Second, 100*time.Millisecond)

			changesResults := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", localDoc.Sequence), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			resolvedDoc, err := rt1Collection.GetDocument(rt1Ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			// assert on HLV result
			if testCase.newCVGenerated {
				assert.Equal(t, rt1.GetDatabase().EncodedSourceID, resolvedDoc.HLV.SourceID)
				assert.Equal(t, resolvedDoc.Cas, resolvedDoc.HLV.Version)
				if testCase.mergeVersionsExpected {
					assert.Len(t, resolvedDoc.HLV.MergeVersions, 2)
					assert.Equal(t, uint64(1234), resolvedDoc.HLV.MergeVersions["abc"])
					assert.Equal(t, uint64(1234), resolvedDoc.HLV.MergeVersions["def"])
				} else {
					// pv will be populated
					assert.Equal(t, uint64(1234), resolvedDoc.HLV.MergeVersions["abc"])
					assert.Equal(t, uint64(1234), resolvedDoc.HLV.MergeVersions["def"])
				}
			} else {
				// must be remote wins
				assert.Equal(t, "def", resolvedDoc.HLV.SourceID)
				assert.Equal(t, uint64(1234), resolvedDoc.HLV.Version)
				assert.Len(t, resolvedDoc.HLV.MergeVersions, 0)
				assert.Equal(t, uint64(1234), resolvedDoc.HLV.PreviousVersions["abc"])
			}
			requireBodyEqual(t, testCase.expectedBody, resolvedDoc)

			if testCase.expectedDocPushResolved {
				resolvedDocBodyLocal, err := resolvedDoc.BodyBytes(rt1Ctx)
				require.NoError(t, err)
				resolvedDocRemote, err := rt2Collection.GetDocument(rt2Ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)

				resolvedDocBodyRemote, err := resolvedDocRemote.BodyBytes(rt2Ctx)
				require.NoError(t, err)
				assert.Equal(t, resolvedDocBodyLocal, resolvedDocBodyRemote)
			}

			if testCase.newCVGenerated {
				// merge/local wins will:
				//	- tombstones local active revision
				//  - winning merged rev is written as child of remote winning rev
				remoteGeneration, _ := db.ParseRevID(ctx1, remoteDoc.GetRevTreeID())
				newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, remoteDoc.GetRevTreeID(), []byte(testCase.expectedBody))
				require.NoError(t, err)
				docHistoryLeaves := resolvedDoc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, resolvedDoc, newRevID, localDoc.GetRevTreeID())
			} else {
				// remote wins case
				//	- tombstones local active revision
				// 	- winning rev is incoming active rev
				assert.Equal(t, remoteDoc.GetRevTreeID(), resolvedDoc.GetRevTreeID())
				docHistoryLeaves := resolvedDoc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, resolvedDoc, remoteDoc.GetRevTreeID(), localDoc.GetRevTreeID())
			}
		})
	}
}

func TestActiveReplicatorHLVConflictWhenNonWinningRevHasMoreRevisions(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name                 string
		conflictResolverType db.ConflictResolverType
	}{
		{
			name:                 "remote wins",
			conflictResolverType: db.ConflictResolverRemoteWins,
		},
		{
			name:                 "local wins",
			conflictResolverType: db.ConflictResolverLocalWins,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "passivedb",
					},
				},
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						Name: "activedb",
					},
				},
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			docID := "doc1_"
			version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			rt2.WaitForPendingChanges()

			resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:           200,
				ReplicationStatsMap:        dbReplicatorStats(t),
				ConflictResolverFuncForHLV: resolverFunc,
				CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:                 false,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			rt1Version, _ := rt1.GetDoc(docID)
			rest.RequireDocVersionEqual(t, version, rt1Version)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			var expectedWinnerPreConflict db.Body
			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// update doc on remote to ensure it we have something to replicate
				// update doc locally many times to create more revisions than remote
				version = rt2.UpdateDoc(docID, version, `{"source":"rt2","channels":["alice"]}`)
				for i := 0; i < 10; i++ {
					rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
				}

				_, expectedWinnerPreConflict = rt2.GetDoc(docID) // get body for later comparison + revID assertion
			} else {
				// update doc on remote loads of times to cause large diff in revisions
				for i := 0; i < 10; i++ {
					version = rt2.UpdateDoc(docID, version, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i))
				}
				// update doc locally to conflict
				rt1Version = rt1.UpdateDoc(docID, rt1Version, `{"source":"rt1","channels":["alice"]}`)

				_, expectedWinnerPreConflict = rt1.GetDoc(docID) // get body for later comparison + revID assertion
			}
			rt2.WaitForPendingChanges()
			rt1.WaitForPendingChanges()

			// grab last seq allocated for since param
			rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			since, err := rt1Collection.LastSequence(rt1ctx)
			require.NoError(t, err)

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
			changesResults.RequireDocIDs(t, []string{docID})
			resolvedVersion, _ := rt1.GetDoc(docID)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)

			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				rest.RequireDocVersionEqual(t, version, resolvedVersion)
				// remote wins:
				//	- tombstones local active revision
				// 	- winning rev is incoming active rev
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, version.RevTreeID, rt1Version.RevTreeID)
			} else {
				// local wins:
				//	- tombstones local active revision
				//  - winning rev is written as child of remote winning rev
				remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
				newRevID, err := db.CreateRevID(remoteGeneration+1, version.RevTreeID, expectedWinnerPreConflict)
				require.NoError(t, err)
				docHistoryLeaves := rt1Doc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, newRevID, rt1Version.RevTreeID)

				rt1Version.CV.Value = rt1Doc.Cas // local wins writes a new CV
				rt1Version.RevTreeID = newRevID
				rest.RequireDocVersionEqual(t, rt1Version, resolvedVersion)
			}

			expectedWinnerPreConflict = rest.StripInternalProperties(t, expectedWinnerPreConflict)
			expectedJsonBody := base.MustJSONMarshal(t, expectedWinnerPreConflict)
			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)
			require.JSONEq(t, string(expectedJsonBody), string(actualBody))
		})
	}
}

func TestActiveReplicatorHLVConflictLocalWinsWhenNonWinningRevHasLessRevisionsLocalIsTombstoned(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	base.RequireNumTestBuckets(t, 2)
	// Passive
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "passivedb",
			},
		},
	})
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "activedb",
			},
		},
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	docID := "doc1_"
	rt2Version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
	rt2.WaitForPendingChanges()

	// create conflicting update on rt1 and create more revisions than remote
	rt1Version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)
	rt1.WaitForPendingChanges()
	for i := 0; i < 10; i++ {
		rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
	}
	// delete local version
	rt1Version = rt1.DeleteDoc(docID, rt1Version)
	rt1.WaitForPendingChanges()

	resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverLocalWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:           200,
		ReplicationStatsMap:        dbReplicatorStats(t),
		ConflictResolverFuncForHLV: resolverFunc,
		CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:                 false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// grab last seq allocated for since param
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	since, err := rt1Collection.LastSequence(rt1ctx)
	require.NoError(t, err)

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	// local wins:
	//	- tombstones local active revision
	//  - winning rev is written as child of remote winning rev
	// 	- remote rev is behind in terms of rev tree so local wins will inject revs to catch up and then write new winning rev
	remoteGeneration, _ := db.ParseRevID(ctx1, rt2Version.RevTreeID)
	localGeneration, _ := db.ParseRevID(ctx1, rt1Version.RevTreeID)
	requiredAdditionalRevs := localGeneration - remoteGeneration
	// we will create 10 additional revs to inject into remote  doc's history to catch up with local rev tree
	previousRevID := rt2Version.RevTreeID
	generatedRevs := make([]string, 0, 0)
	for i := 0; i < requiredAdditionalRevs; i++ {
		remoteGeneration++
		previousRevID = db.CreateRevIDWithBytes(remoteGeneration, previousRevID, []byte("{}"))
		generatedRevs = append(generatedRevs, previousRevID)
	}
	newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, previousRevID, []byte("{}"))
	require.NoError(t, err)
	docHistoryLeaves := rt1Doc.History.GetLeaves()
	require.Len(t, docHistoryLeaves, 2)
	for _, revID := range generatedRevs {
		_, ok := rt1Doc.History[revID]
		require.True(t, ok, "expected to find generated rev %s in doc history", revID)
	}
	for _, revID := range docHistoryLeaves {
		_, ok := rt1Doc.History[revID]
		require.True(t, ok, "expected to find leaf rev %s in doc history", revID)
		assert.True(t, rt1Doc.History[revID].Deleted)
	}

	rt1Version.CV.Value = rt1Doc.Cas // local wins writes a new CV
	rt1Version.RevTreeID = newRevID
	rest.RequireDocVersionEqual(t, rt1Version, rt1Doc.ExtractDocVersion())

	actualBody, err := rt1Doc.BodyBytes(rt1ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(actualBody))
}

func TestActiveReplicatorHLVConflictWithBothLocalAndRemoteTombstones(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	base.RequireNumTestBuckets(t, 2)
	// Passive
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "passivedb",
			},
		},
	})
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "activedb",
			},
		},
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	docRevIDVersions := make(map[string]struct{})

	docID := "doc1_"
	rt2Version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
	rt2.WaitForPendingChanges()
	docRevIDVersions[rt2Version.RevTreeID] = struct{}{}

	rt1Version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)
	rt1.WaitForPendingChanges()
	docRevIDVersions[rt1Version.RevTreeID] = struct{}{}

	// delete both docs
	rt1Version = rt1.DeleteDoc(docID, rt1Version)
	rt2Version = rt2.DeleteDoc(docID, rt2Version)
	rt2.WaitForPendingChanges()
	rt1.WaitForPendingChanges()
	docRevIDVersions[rt1Version.RevTreeID] = struct{}{}
	docRevIDVersions[rt2Version.RevTreeID] = struct{}{}

	resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:           200,
		ReplicationStatsMap:        dbReplicatorStats(t),
		ConflictResolverFuncForHLV: resolverFunc,
		CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:                 false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	since, err := rt1Collection.LastSequence(rt1ctx)
	require.NoError(t, err)

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	// assert on local revision rev tree + CV
	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, rt2Version, rt1Doc.ExtractDocVersion())
	assert.True(t, rt1Doc.IsDeleted())

	// assert that both branches are tombstones
	docHistoryLeaves := rt1Doc.History.GetLeaves()
	require.Len(t, docHistoryLeaves, 2)
	for _, revID := range docHistoryLeaves {
		assert.True(t, rt1Doc.History[revID].Deleted)
	}
	// assert that all 4 revisions created above are in history
	require.Len(t, rt1Doc.History, 4)
	for _, revItem := range rt1Doc.History {
		_, ok := docRevIDVersions[revItem.ID]
		assert.True(t, ok)
	}

	actualBody, err := rt1Doc.BodyBytes(rt1ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(actualBody))
}
