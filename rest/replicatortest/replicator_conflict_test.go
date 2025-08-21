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

func TestActiveReplicatorHLVConflictRemoteAmndLocalWins(t *testing.T) {
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
			version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
				ChangesBatchSize:     200,
				ReplicationStatsMap:  dbReplicatorStats(t),
				ConflictResolverFunc: resolverFunc,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:           false,
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
				version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i)))
			}
			rt2.WaitForPendingChanges()
			for i := 0; i < 10; i++ {
				rt1Version = rt1.UpdateDocDirectly(docID, rt1Version, rest.JsonToMap(t, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i)))
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
			replicatedVersion, _ := rt1.GetDoc(docID)
			// CBG-4791 - switch to RequireDocVersionEqual
			if testCase.conflictResType == db.ConflictResolverRemoteWins {
				rest.RequireDocumentCV(t, version, replicatedVersion)
			} else {
				rest.RequireDocumentCV(t, rt1Version, replicatedVersion)
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			// assert HLV and body is as expected
			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			if testCase.conflictResType == db.ConflictResolverRemoteWins {
				rest.RequireDocumentCV(t, version, rt1Doc.ExtractDocVersion())
			} else {
				rest.RequireDocumentCV(t, rt1Version, rt1Doc.ExtractDocVersion())
			}
			// PV should have the source version pair from the updates on rt1
			require.Len(t, rt1Doc.HLV.PreviousVersions, 1)
			assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.PreviousVersions[nonWinningRevPreConflict.CV.SourceID])
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
	version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
		ChangesBatchSize:     200,
		ReplicationStatsMap:  dbReplicatorStats(t),
		ConflictResolverFunc: resolverFunc,
		CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:           false,
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
		version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i)))
	}
	rt2.WaitForPendingChanges()
	for i := 0; i < 10; i++ {
		rt1Version = rt1.UpdateDocDirectly(docID, rt1Version, rest.JsonToMap(t, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i)))
	}
	rt1.WaitForPendingChanges()

	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()

	// pull out expected winner and expected conflict response body
	if rt1Version.CV.Value > version.CV.Value {
		docToPull, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)
		expectedWinner = rt1Version
		expectedConflictResBody, err = docToPull.BodyBytes(rt2ctx)
		require.NoError(t, err)
		nonWinningRevPreConflict = version
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
	replicatedVersion, _ := rt1.GetDoc(docID)
	// CBG-4791 - switch to RequireDocVersionEqual
	rest.RequireDocumentCV(t, expectedWinner, replicatedVersion)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocumentCV(t, expectedWinner, rt1Doc.ExtractDocVersion())
	// PV should have the source version pair from the updates on rt1
	require.Len(t, rt1Doc.HLV.PreviousVersions, 1)
	assert.Equal(t, nonWinningRevPreConflict.CV.Value, rt1Doc.HLV.PreviousVersions[nonWinningRevPreConflict.CV.SourceID])
	// grab local doc body and assert it is as expected
	actualBody, err := rt1Doc.BodyBytes(rt1ctx)
	require.NoError(t, err)
	fmt.Println(string(actualBody), "expected:", string(expectedConflictResBody))
	assert.Equal(t, expectedConflictResBody, actualBody)
}

func TestActiveReplicatorLocalWinsWithMV(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	const (
		mergeVersion1 = "xyz"
		mergeVersion2 = "abc"
	)

	testCases := []struct {
		name              string
		olderVersionLocal bool
		mvForRemote       db.HLVVersions
		mvForLocal        db.HLVVersions
		expectedMV        db.HLVVersions
		expectedFlushToPV db.HLVVersions
	}{
		{
			name: "MV on remote doc and no MV on local doc",
			mvForRemote: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedFlushToPV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "MV on remote doc with higher value than MV on local doc",
			mvForRemote: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			mvForLocal: db.HLVVersions{
				mergeVersion1: 100,
				mergeVersion2: 50,
			},
			expectedFlushToPV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "MV on remote doc with lower value than MV on local doc",
			mvForRemote: db.HLVVersions{
				mergeVersion1: 100,
				mergeVersion2: 50,
			},
			mvForLocal: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedMV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "MV on local doc and no MV on remote doc",
			mvForLocal: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedMV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "CV on remote is newer than local CV",
			mvForLocal: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedMV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
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
			version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
				ChangesBatchSize:     200,
				ReplicationStatsMap:  dbReplicatorStats(t),
				ConflictResolverFunc: resolverFunc,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:           false,
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
				Version:       math.MaxUint64, // will macro expand
				SourceID:      version.CV.SourceID,
				MergeVersions: testCase.mvForRemote,
				PreviousVersions: map[string]uint64{
					"foo": 100,
				},
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
			if testCase.olderVersionLocal {
				// alter cv version to be low number to simulate older version than remote
				newHLVForLocal.Version = 50
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
				SourceID:      rt1.GetDatabase().EncodedSourceID,
				Version:       localCas,
				MergeVersions: testCase.expectedMV,
				PreviousVersions: map[string]uint64{
					"foo":                             100,
					rt2.GetDatabase().EncodedSourceID: remoteCas,
				},
			}

			// add expected flush to PV if needed
			if len(testCase.expectedFlushToPV) != 0 {
				for key, value := range testCase.expectedFlushToPV {
					expectedHLV.PreviousVersions[key] = value
				}
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
			if !testCase.olderVersionLocal {
				assert.Equal(t, expectedHLV.Version, cvValue)
			} else {
				// new cv will have been generated as local cv was older than remote
				assert.Equal(t, rt1Doc.Cas, cvValue)
			}
			if len(testCase.expectedFlushToPV) == 0 {
				require.Len(t, rt1Doc.HLV.PreviousVersions, 2)
			} else {
				require.Len(t, rt1Doc.HLV.PreviousVersions, 4)
			}
			assert.Equal(t, expectedHLV.PreviousVersions["foo"], rt1Doc.HLV.PreviousVersions["foo"])
			assert.Equal(t, expectedHLV.PreviousVersions[rt1.GetDatabase().EncodedSourceID], rt1Doc.HLV.PreviousVersions[rt1.GetDatabase().EncodedSourceID])
			if len(testCase.expectedFlushToPV) == 0 {
				require.Len(t, rt1Doc.HLV.MergeVersions, 2)
				assert.Equal(t, expectedHLV.MergeVersions[mergeVersion1], rt1Doc.HLV.MergeVersions[mergeVersion1])
				assert.Equal(t, expectedHLV.MergeVersions[mergeVersion2], rt1Doc.HLV.MergeVersions[mergeVersion2])
			} else {
				require.Len(t, rt1Doc.HLV.MergeVersions, 0)
				for key, value := range testCase.expectedFlushToPV {
					assert.Equal(t, value, rt1Doc.HLV.PreviousVersions[key], "Expected key %s to have value %d in previous versions", key, value)
				}
			}
		})
	}
}

func TestActiveReplicatorRemoteWinsWithMV(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	const (
		mergeVersion1 = "xyz"
		mergeVersion2 = "abc"
	)

	testCases := []struct {
		name              string
		mvForRemote       db.HLVVersions
		mvForLocal        db.HLVVersions
		expectedMV        db.HLVVersions
		expectedFlushToPV db.HLVVersions
	}{
		{
			name: "MV on remote doc and no MV on local doc",
			mvForRemote: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedMV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "MV on remote doc with higher value than MV on local doc",
			mvForRemote: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			mvForLocal: db.HLVVersions{
				mergeVersion1: 100,
				mergeVersion2: 50,
			},
			expectedMV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "MV on remote doc with lower value than MV on local doc",
			mvForRemote: db.HLVVersions{
				mergeVersion1: 100,
				mergeVersion2: 50,
			},
			mvForLocal: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedMV: nil, // mo MV expected should be flushed to pv
			expectedFlushToPV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
		},
		{
			name: "MV on local doc and no MV on remote doc",
			mvForLocal: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
			},
			expectedMV: nil, // no MV expected should be flushed to pv
			expectedFlushToPV: db.HLVVersions{
				mergeVersion1: 200,
				mergeVersion2: 300,
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
			version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
				ChangesBatchSize:     200,
				ReplicationStatsMap:  dbReplicatorStats(t),
				ConflictResolverFunc: resolverFunc,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:           false,
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
				Version:       math.MaxUint64, // will macro expand
				SourceID:      version.CV.SourceID,
				MergeVersions: testCase.mvForRemote,
				PreviousVersions: map[string]uint64{
					"foo": 100,
				},
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
					"foo":                             100,
					rt1.GetDatabase().EncodedSourceID: localCas,
				},
			}
			// add expected flush to PV if needed
			if len(testCase.expectedFlushToPV) != 0 {
				for key, value := range testCase.expectedFlushToPV {
					expectedHLV.PreviousVersions[key] = value
				}
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
			if len(testCase.expectedFlushToPV) == 0 {
				require.Len(t, rt1Doc.HLV.PreviousVersions, 2)
			} else {
				require.Len(t, rt1Doc.HLV.PreviousVersions, 4)
			}
			assert.Equal(t, expectedHLV.PreviousVersions["foo"], rt1Doc.HLV.PreviousVersions["foo"])
			assert.Equal(t, expectedHLV.PreviousVersions[rt1.GetDatabase().EncodedSourceID], rt1Doc.HLV.PreviousVersions[rt1.GetDatabase().EncodedSourceID])
			if len(testCase.expectedFlushToPV) == 0 {
				require.Len(t, rt1Doc.HLV.MergeVersions, 2)
				assert.Equal(t, expectedHLV.MergeVersions[mergeVersion1], rt1Doc.HLV.MergeVersions[mergeVersion1])
				assert.Equal(t, expectedHLV.MergeVersions[mergeVersion2], rt1Doc.HLV.MergeVersions[mergeVersion2])
			} else {
				require.Len(t, rt1Doc.HLV.MergeVersions, 0)
				for key, value := range testCase.expectedFlushToPV {
					assert.Equal(t, value, rt1Doc.HLV.PreviousVersions[key], "Expected key %s to have value %d in previous versions", key, value)
				}
			}
		})
	}
}

func TestActiveReplicatorHLVConflictNoCommonMV(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyVV, base.KeyCRUD, base.KeySync, base.KeyReplicate, base.KeyChanges, base.KeyImport)
	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name                 string
		localMV              db.HLVVersions
		remoteMV             db.HLVVersions
		expectedMV           db.HLVVersions
		expectedPV           db.HLVVersions
		conflictResolverType db.ConflictResolverType
	}{
		{
			name: "No common MV remote wins",
			localMV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
			},
			remoteMV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			expectedPV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
			},
			expectedMV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			conflictResolverType: db.ConflictResolverRemoteWins,
		},
		{
			name: "No common MV local wins",
			localMV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
			},
			remoteMV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			expectedPV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
			},
			expectedMV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
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
			version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
				ChangesBatchSize:     200,
				ReplicationStatsMap:  dbReplicatorStats(t),
				ConflictResolverFunc: resolverFunc,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:           false,
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
				Version:       math.MaxUint64, // will macro expand
				SourceID:      version.CV.SourceID,
				MergeVersions: testCase.remoteMV,
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
					SourceID:      rt1.GetDatabase().EncodedSourceID,
					Version:       localCas,
					MergeVersions: testCase.expectedMV,
					PreviousVersions: map[string]uint64{
						rt2.GetDatabase().EncodedSourceID: remoteCas,
					},
				}
			}
			// add extra pv expected
			for key, value := range testCase.expectedPV {
				expectedHLV.PreviousVersions[key] = value
			}

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

			rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			cvSource, cvValue := rt1Doc.HLV.GetCurrentVersion()
			// CBG-4791 - assert on rev tree here too
			assert.Equal(t, expectedHLV.SourceID, cvSource)
			assert.Equal(t, expectedHLV.Version, cvValue)

			for key, value := range expectedHLV.PreviousVersions {
				assert.Equal(t, value, rt1Doc.HLV.PreviousVersions[key], "Expected key %s to have value %d in previous versions", key, value)
			}
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
			version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
				ChangesBatchSize:     200,
				ReplicationStatsMap:  dbReplicatorStats(t),
				ConflictResolverFunc: resolverFunc,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:           false,
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
			version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt2", "channels": ["alice"], "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}, "world.txt": {"data":"aGVsbG8gd29ybGQ="}}}`))
			// update local to have one different attachment
			rt1Version = rt1.UpdateDocDirectly(docID, rt1Version, rest.JsonToMap(t, `{"source":"rt1", "channels": ["alice"], "_attachments": {"different.txt": {"data":"aGVsbG8gd29ybGQ="}}}`))
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
			version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
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
				ChangesBatchSize:     200,
				ReplicationStatsMap:  dbReplicatorStats(t),
				ConflictResolverFunc: resolverFunc,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
				Continuous:           false,
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
				version = rt2.DeleteDocDirectly(docID, version)
			} else {
				// need to update remote doc to have something to replicate
				version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
				// local wins, so we update the local doc to be a tombstone
				rt1Version = rt1.DeleteDocDirectly(docID, rt1Version)
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
				rest.RequireDocumentCV(t, rt1Version, rt1Doc.ExtractDocVersion())
			}
			assert.True(t, rt1Doc.IsDeleted(), "Expected document to be a tombstone after remote wins conflict resolution")
		})
	}
}
