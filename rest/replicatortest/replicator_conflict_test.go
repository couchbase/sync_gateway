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
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
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

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// assert HLV and body is as expected
			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			// CBG-4791 - switch to RequireDocVersionEqual
			if testCase.conflictResType == db.ConflictResolverRemoteWins {
				rest.RequireDocumentCV(t, version, rt1Doc.ExtractDocVersion())
				// PV should have the source version pair from the updates on rt1
				require.Len(t, rt1Doc.HLV.PreviousVersions, 1)
				assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.PreviousVersions[nonWinningRevPreConflict.CV.SourceID])
			} else {
				localWinsVersion := rt1Version
				localWinsVersion.CV.Value = rt1Doc.Cas // will generate a new CV value
				rest.RequireDocumentCV(t, localWinsVersion, rt1Doc.ExtractDocVersion())
				require.Len(t, rt1Doc.HLV.MergeVersions, 2)
				assert.Equal(t, rt1Version.CV.Value, rt1Doc.HLV.MergeVersions[rt1Version.CV.SourceID])
				assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.MergeVersions[nonWinningRevPreConflict.CV.SourceID])
				require.Len(t, rt1Doc.HLV.PreviousVersions, 0)
			}
			// grab local doc body and assert it is as expected
			actualBody, err := rt1Doc.BodyBytes(rt1ctx)
			require.NoError(t, err)
			assert.Equal(t, expectedConflictResBody, actualBody)
		})
	}
}

func TestActiveReplicatorLWWDefaultResolver(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges)
	base.RequireNumTestBuckets(t, 2)
	// Passive
	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
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
	}

	// start again for new revisions
	require.NoError(t, ar.Start(ctx1))

	changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	if localWins {
		expectedWinner.CV.Value = rt1Doc.Cas
	}
	// CBG-4791 - switch to RequireDocVersionEqual
	rest.RequireDocumentCV(t, expectedWinner, rt1Doc.ExtractDocVersion())
	// PV should have the source version pair from the updates on rt1
	if !localWins {
		require.Len(t, rt1Doc.HLV.PreviousVersions, 1)
		assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.PreviousVersions[nonWinningRevPreConflict.CV.SourceID])
	} else {
		require.Len(t, rt1Doc.HLV.MergeVersions, 2)
		assert.Equal(t, rt1Version.CV.Value, rt1Doc.HLV.MergeVersions[rt1Version.CV.SourceID])
		assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.MergeVersions[nonWinningRevPreConflict.CV.SourceID])
		require.Len(t, rt1Doc.HLV.PreviousVersions, 0)
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
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
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
			remoteCas := db.AlterHLVForTest(t, ctx1, rt2.GetSingleDataStore(), docID, newHLVForRemote, docBody)
			_, _ = rt2.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			docBody = map[string]interface{}{
				"source":   "rt1",
				"channels": []string{"alice"},
			}
			localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
			_, _ = rt1.GetDoc(docID) // ensure doc is imported
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

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			// CBG-4791 - assert on rev tree here too
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			// new cv will have been generated for local wins
			assert.Equal(t, rt1Doc.Cas, cvValue)

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
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
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
			_, _ = rt2.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			docBody = map[string]interface{}{
				"source":   "rt1",
				"channels": []string{"alice"},
			}
			localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
			_, _ = rt1.GetDoc(docID) // ensure doc is imported
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

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			// CBG-4791 - assert on rev tree here too
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			assert.Equal(t, expectedHLV.Version, cvValue)

			assert.Len(t, rt1Doc.HLV.PreviousVersions, len(expectedHLV.PreviousVersions))
			for key, val := range testCase.expectedPV {
				assert.Equal(t, val, rt1Doc.HLV.PreviousVersions[key], "Expected key or value is missing in previous versions")
			}
			// assert on mv
			assert.Len(t, rt1Doc.HLV.MergeVersions, len(expectedHLV.MergeVersions))
			for key, val := range expectedHLV.MergeVersions {
				assert.Equal(t, val, rt1Doc.HLV.MergeVersions[key], "Expected key or value is missing in merge versions")
			}
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
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
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
				"source":   "rt2",
				"channels": []string{"alice"},
				"some":     "data",
			}
			remoteCas := db.AlterHLVForTest(t, ctx1, rt2.GetSingleDataStore(), docID, newHLVForRemote, docBody)
			_, _ = rt2.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			docBody = map[string]interface{}{
				"source":   "rt1",
				"channels": []string{"alice"},
			}
			localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
			_, _ = rt1.GetDoc(docID) // ensure doc is imported
			base.RequireWaitForStat(t, func() int64 {
				return rt1.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
			}, 1)

			rt1.WaitForPendingChanges()
			rt2.WaitForPendingChanges()

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

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
			} else {
				expectedHLV = &db.HybridLogicalVector{
					SourceID: rt1.GetDatabase().EncodedSourceID,
					MergeVersions: db.HLVVersions{
						rt1.GetDatabase().EncodedSourceID: localCas,
						rt2.GetDatabase().EncodedSourceID: remoteCas,
					},
					PreviousVersions: make(db.HLVVersions),
				}
			}
			// add extra pv expected
			for key, value := range testCase.expectedPV {
				expectedHLV.PreviousVersions[key] = value
			}

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			if testCase.conflictResolverType == db.ConflictResolverLocalWins {
				// writes new cv
				expectedHLV.Version = rt1Doc.Cas
			}
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			// CBG-4791 - assert on rev tree here too
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			assert.Equal(t, expectedHLV.Version, cvValue)

			assert.Len(t, rt1Doc.HLV.PreviousVersions, len(expectedHLV.PreviousVersions))
			for key, value := range expectedHLV.PreviousVersions {
				assert.Equal(t, value, rt1Doc.HLV.PreviousVersions[key], "Expected key %s to have value %d in previous versions", key, value)
			}
			assert.Len(t, rt1Doc.HLV.MergeVersions, len(expectedHLV.MergeVersions))
			for key, value := range expectedHLV.MergeVersions {
				assert.Equal(t, value, rt1Doc.HLV.MergeVersions[key], "Expected key %s to have value %d in merge versions", key, value)
			}
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
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
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

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)

			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// remote wins, so we expect the remote attachments to be present
				// CBG-4791 - switch to RequireDocVersionEqual
				rest.RequireDocumentCV(t, version, rt1Doc.ExtractDocVersion())

				attMeta := rt1Doc.Attachments()
				require.Len(t, attMeta, 2)
				base.RequireKeysEqual(t, []string{"hello.txt", "world.txt"}, attMeta)
			} else {
				// local wins, so we expect the local attachment to be present
				// CBG-4791 - switch to RequireDocVersionEqual
				rt1Version.CV.Value = rt1Doc.Cas // local wins writes a new CV
				rest.RequireDocumentCV(t, rt1Version, rt1Doc.ExtractDocVersion())

				attMeta := rt1Doc.Attachments()
				require.Len(t, attMeta, 1)
				base.RequireKeysEqual(t, []string{"different.txt"}, attMeta)
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
			})
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
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
			} else {
				// need to update remote doc to have something to replicate
				version = rt2.UpdateDoc(docID, version, `{"source":"rt2","channels":["alice"]}`)
				// local wins, so we update the local doc to be a tombstone
				rt1Version = rt1.DeleteDoc(docID, rt1Version)
			}
			rt2.WaitForPendingChanges()
			rt1.WaitForPendingChanges()

			// Start the replicator
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
			changesResults.RequireDocIDs(t, []string{docID})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)

			if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
				// CBG-4791 - switch to RequireDocVersionEqual
				rest.RequireDocumentCV(t, version, rt1Doc.ExtractDocVersion())
			} else {
				// CBG-4791 - switch to RequireDocVersionEqual
				rt1Version.CV.Value = rt1Doc.Cas // local wins writes a new CV
				rest.RequireDocumentCV(t, rt1Version, rt1Doc.ExtractDocVersion())
			}
			assert.True(t, rt1Doc.IsDeleted(), "Expected document to be a tombstone after remote wins conflict resolution")
		})
	}
}
