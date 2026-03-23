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
	"maps"
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveReplicatorHLVConflictRemoteAndLocalWins(t *testing.T) {
	base.LongRunningTest(t)

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
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResType, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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

				for i := range 10 {
					version = rt2.UpdateDoc(docID, version, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i))
				}
				rt2.WaitForPendingChanges()
				for i := range 10 {
					rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
				}
				rt1.WaitForPendingChanges()

				rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
				rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()

				var nonWinningRevPreConflict rest.DocVersion
				var winningRevPreConflict rest.DocVersion
				if testCase.conflictResType == db.ConflictResolverRemoteWins {
					docToPull, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
					require.NoError(t, err)
					expectedConflictResBody, err = docToPull.BodyBytes(rt2ctx)
					require.NoError(t, err)

					nonWinningRevPreConflict = rt1Version
					winningRevPreConflict = version
				} else {
					localDoc, err := rt1collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
					require.NoError(t, err)
					expectedConflictResBody, err = localDoc.BodyBytes(rt2ctx)
					require.NoError(t, err)

					nonWinningRevPreConflict = version
					winningRevPreConflict = rt1Version
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
					// local wins will generate a new rev ID as child of remote RevID
					remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
					newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, version.RevTreeID, expectedConflictResBody)
					localWinsVersion.RevTreeID = newRevID
					require.NotNil(t, rt1Doc.HLV)
					require.Equal(t, db.HybridLogicalVector{
						CurrentVersionCAS: rt1Doc.Cas,
						SourceID:          winningRevPreConflict.CV.SourceID,
						Version:           winningRevPreConflict.CV.Value,
						PreviousVersions: db.HLVVersions{
							nonWinningRevPreConflict.CV.SourceID: nonWinningRevPreConflict.CV.Value,
						},
					}, *rt1Doc.HLV)

					// check tombstoned leaf previous local active rev and active rev is above generated rev ID
					docHistoryLeaves := rt1Doc.History.GetLeaves()
					require.Len(t, docHistoryLeaves, 2)
					rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, localWinsVersion.RevTreeID, rt1Version.RevTreeID)
				}

			})
		}
	})
}

func TestActiveReplicatorLWWDefaultResolver(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
		ctx1 := rt1.Context()

		docID := "doc1_"
		version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		rt2.WaitForPendingChanges()

		resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
		require.NoError(t, err)

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:           200,
			ReplicationStatsMap:        dbReplicatorStats(t),
			ConflictResolverFuncForHLV: resolverFunc,
			CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
			Continuous:                 false,
			SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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

		for i := range 10 {
			version = rt2.UpdateDoc(docID, version, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i))
		}
		rt2.WaitForPendingChanges()
		for i := range 10 {
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
			// local wins will gen a new rev ID as child of remote RevID
			remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
			newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, version.RevTreeID, expectedConflictResBody)
			expectedWinner.RevTreeID = newRevID
		}
		rest.RequireDocVersionEqual(t, expectedWinner, rt1Doc.ExtractDocVersion())
		require.Equal(t, db.HybridLogicalVector{
			CurrentVersionCAS: rt1Doc.Cas,
			SourceID:          expectedWinner.CV.SourceID,
			Version:           expectedWinner.CV.Value,
			PreviousVersions: db.HLVVersions{
				nonWinningRevPreConflict.CV.SourceID: nonWinningRevPreConflict.CV.Value,
			},
		}, *rt1Doc.HLV)

		// PV should have the source version pair from the updates on rt1
		if !localWins {

			// remote wins:
			//	- tombstones local active revision
			// 	- winning rev is incoming active rev
			docHistoryLeaves := rt1Doc.History.GetLeaves()
			require.Len(t, docHistoryLeaves, 2)
			rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, expectedWinner.RevTreeID, nonWinningRevPreConflict.RevTreeID)
		} else {

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
	})
}

func TestActiveReplicatorLocalWinsCases(t *testing.T) {
	base.LongRunningTest(t)

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
			},
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()
				ctx2 := rt2.Context()

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverLocalWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
				maps.Copy(newHLVForLocal.PreviousVersions, testCase.pvForLocal)

				docBody := map[string]any{
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
				docBody = map[string]any{
					"channels": []string{"alice"},
				}
				localCas := db.AlterHLVForTest(t, ctx2, rt1.GetSingleDataStore(), docID, newHLVForLocal, docBody)
				localDocPreConflict, localDocPreConflictBody := rt1.GetDoc(docID) // ensure doc is imported
				base.RequireWaitForStat(t, func() int64 {
					return rt1.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
				}, 1)

				expectedHLV := &db.HybridLogicalVector{
					Version:          localCas,
					SourceID:         rt1.GetDatabase().EncodedSourceID,
					PreviousVersions: testCase.expectedPV,
				}
				if testCase.mvForLocal != nil {
					expectedHLV.MergeVersions = testCase.mvForLocal
				}
				// add current CVs to expected MV
				expectedHLV.PreviousVersions[rt2.GetDatabase().EncodedSourceID] = remoteCas

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

				require.Equal(t,
					db.HybridLogicalVector{
						CurrentVersionCAS: rt1Doc.Cas,
						SourceID:          expectedHLV.SourceID,
						Version:           expectedHLV.Version,
						PreviousVersions:  testCase.expectedPV,
						MergeVersions:     expectedHLV.MergeVersions,
					},
					*rt1Doc.HLV)
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
				localDocPreConflictBody, _ = db.StripInternalProperties(localDocPreConflictBody)
				expectedBytes := base.MustJSONMarshal(t, localDocPreConflictBody)
				actualBody, err := rt1Doc.BodyBytes(rt1ctx)
				require.NoError(t, err)
				require.JSONEq(t, string(expectedBytes), string(actualBody))
			})
		}
	})
}

func TestActiveReplicatorRemoteWinsCases(t *testing.T) {
	base.LongRunningTest(t)

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
				testSource1: 200,
				testSource2: 300,
			},
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()
				ctx2 := rt2.Context()

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverRemoteWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
				maps.Copy(newHLVForLocal.PreviousVersions, testCase.pvForLocal)

				docBody := map[string]any{
					"source":   "rt2",
					"channels": []string{"alice"},
					"some":     "data",
				}
				remoteCas := db.AlterHLVForTest(t, ctx1, rt2.GetSingleDataStore(), docID, newHLVForRemote, docBody)
				remoteDocVersionPreConflict, remoteDocBodyPreConflict := rt2.GetDoc(docID) // ensure doc is imported
				base.RequireWaitForStat(t, func() int64 {
					return rt2.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
				}, 1)

				docBody = map[string]any{
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
				maps.Copy(expectedHLV.PreviousVersions, testCase.expectedPV)

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

				require.Equal(t, db.HybridLogicalVector{
					CurrentVersionCAS: rt1Doc.Cas,
					SourceID:          expectedHLV.SourceID,
					Version:           expectedHLV.Version,
					PreviousVersions:  expectedHLV.PreviousVersions,
					MergeVersions:     expectedHLV.MergeVersions,
				}, *rt1Doc.HLV)

				remoteDocBodyPreConflict, _ = db.StripInternalProperties(remoteDocBodyPreConflict)
				expectedJsonBody := base.MustJSONMarshal(t, remoteDocBodyPreConflict)
				actualBody, err := rt1Doc.BodyBytes(rt1ctx)
				require.NoError(t, err)
				require.JSONEq(t, string(expectedJsonBody), string(actualBody))
			})
		}
	})
}

func TestActiveReplicatorHLVConflictNoCommonMVPV(t *testing.T) {
	base.LongRunningTest(t)

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
			expectedMV: db.HLVVersions{
				"xyz": 100,
				"abc": 200,
			},
			expectedPV: db.HLVVersions{
				"def": 300,
				"ghi": 400,
				"pv1": 100,
				"pv2": 200,
				"pv3": 100,
				"pv4": 200,
			},
			conflictResolverType: db.ConflictResolverLocalWins,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()
				ctx2 := rt2.Context()

				// disable pruning window so we can avoid HLV compaction for the artificially low HLV values in this test
				rt1.GetDatabase().Options.TestVersionPruningWindowOverride = base.Ptr(time.Duration(0))
				rt2.GetDatabase().Options.TestVersionPruningWindowOverride = base.Ptr(time.Duration(0))

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
				maps.Copy(newHLVForLocal.PreviousVersions, testCase.localPV)

				docBody := map[string]any{
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
				docBody = map[string]any{
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

				rt1Doc := rt1.GetDocument(docID)
				var expectedWinnerPreConflict db.Body
				var expectedHLV db.HybridLogicalVector
				if testCase.conflictResolverType == db.ConflictResolverRemoteWins {
					expectedHLV = db.HybridLogicalVector{
						CurrentVersionCAS: rt1Doc.Cas,
						SourceID:          rt2.GetDatabase().EncodedSourceID,
						Version:           remoteCas,
						MergeVersions:     testCase.expectedMV,
						PreviousVersions: map[string]uint64{
							rt1.GetDatabase().EncodedSourceID: localCas,
						},
					}
					expectedWinnerPreConflict = remoteDocBodyPreConflict
				} else {
					expectedHLV = db.HybridLogicalVector{
						CurrentVersionCAS: rt1Doc.Cas,
						SourceID:          rt1.GetDatabase().EncodedSourceID,
						Version:           localCas,
						MergeVersions:     testCase.expectedMV,
						PreviousVersions: db.HLVVersions{
							rt2.GetDatabase().EncodedSourceID: remoteCas,
						},
					}
					expectedWinnerPreConflict = localDocBodyPreConflict
				}
				// add extra pv expected
				maps.Copy(expectedHLV.PreviousVersions, testCase.expectedPV)

				require.Equal(t, expectedHLV, *rt1Doc.HLV)

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

				expectedWinnerPreConflict, _ = db.StripInternalProperties(expectedWinnerPreConflict)
				expectedJsonBody := base.MustJSONMarshal(t, expectedWinnerPreConflict)
				actualBody, err := rt1Doc.BodyBytes(rt1ctx)
				require.NoError(t, err)
				require.JSONEq(t, string(expectedJsonBody), string(actualBody))
			})
		}
	})
}

func TestActiveReplicatorAttachmentHandling(t *testing.T) {
	base.LongRunningTest(t)

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
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
				localDocBodyPreConflict, _ = db.StripInternalProperties(localDocBodyPreConflict)
				_, remoteDocBodyPreConflict := rt2.GetDoc(docID)
				remoteDocBodyPreConflict, _ = db.StripInternalProperties(remoteDocBodyPreConflict)

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

					// remote attachment from body
					delete(remoteDocBodyPreConflict, db.BodyAttachments)
					expectedBodyBytes := base.MustJSONMarshal(t, remoteDocBodyPreConflict)
					require.JSONEq(t, string(expectedBodyBytes), string(actualBody), "Doc body does not match expected for remote wins")
				} else {
					// local wins, so we expect the local attachment to be present
					// local wins:
					//	- tombstones local active revision
					//  - winning rev is written as child of remote winning rev
					remoteGeneration, _ := db.ParseRevID(ctx1, version.RevTreeID)
					// remote attachment from body
					delete(localDocBodyPreConflict, db.BodyAttachments)
					newRevID, err := db.CreateRevID(remoteGeneration+1, version.RevTreeID, localDocBodyPreConflict)
					require.NoError(t, err)
					docHistoryLeaves := rt1Doc.History.GetLeaves()
					require.Len(t, docHistoryLeaves, 2)
					rest.AssertRevTreeAfterHLVConflictResolution(t, rt1Doc, newRevID, rt1Version.RevTreeID)

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
	})
}

func TestActiveReplicatorHLVConflictWinnerIsTombstone(t *testing.T) {
	base.LongRunningTest(t)

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
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
					newRev := db.CreateRevIDWithBytes(3, rt1Version.RevTreeID, []byte(db.DeletedDocument))
					version.RevTreeID = newRev
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

					require.Equal(t, db.HybridLogicalVector{
						CurrentVersionCAS: rt1Doc.Cas,
						Version:           rt1Version.CV.Value,
						SourceID:          rt1Version.CV.SourceID,
						PreviousVersions: db.HLVVersions{
							rt2.GetDatabase().EncodedSourceID: version.CV.Value,
						},
					}, *rt1Doc.HLV)
					rt1Version.RevTreeID = newRevID
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
	})
}

func TestActiveReplicatorInvalidCustomResolver(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
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
		opts := db.PutDocOptions{
			NewDoc:    newDoc,
			NewDocHLV: incomingHLV,
		}
		_, _, _, err = rt1Collection.PutExistingCurrentVersion(rt1Ctx, opts)
		require.NoError(t, err)

		newDoc = db.CreateTestDocument(docID, "", rest.JsonToMap(t, `{"some": "data"}`), false, 0)
		incomingHLV = &db.HybridLogicalVector{
			SourceID: "def",
			Version:  1234,
		}
		opts.NewDocHLV = incomingHLV
		opts.NewDoc = newDoc
		_, _, _, err = rt2Collection.PutExistingCurrentVersion(rt2Ctx, opts)
		require.NoError(t, err)

		resolver := `function(conflict) {var mergedDoc = new Object();
										mergedDoc._cv = "@";
										return mergedDoc;}` // invalid - setting cv to something that doesn't match either doc
		customConflictResolver, err := db.NewCustomConflictResolver(ctx1, resolver, rt1.GetDatabase().Options.JavascriptTimeout)
		require.NoError(t, err)

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:           200,
			ReplicationStatsMap:        dbReplicatorStats(t),
			ConflictResolverFunc:       customConflictResolver,
			ConflictResolverFuncForHLV: customConflictResolver,
			CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
			Continuous:                 true,
			SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status := ar.GetStatus(ctx1)
			assert.Equal(c, 1, int(status.PullReplicationStatus.RejectedLocal))
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func TestActiveReplicatorHLVConflictCustom(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	activeStartingCV := db.Version{SourceID: "activeRT", Value: 12234}
	passiveStartingCV := db.Version{SourceID: "passiveRT", Value: 1234}

	testCases := []struct {
		name                    string
		localBody               string
		remoteBody              string
		expectedBody            string
		conflictResolver        string
		expectedDocPushResolved bool
		hlvResult               db.HybridLogicalVector
		mergeVersionsExpected   bool
		newCVGenerated          bool
		useRemoteRevTreeID      bool
	}{
		{
			name:             "remote wins call",
			conflictResolver: `function(conflict) {return conflict.RemoteDocument;}`,
			localBody:        `{"source": "local"}`,
			expectedBody:     `{"source": "remote"}`,
			remoteBody:       `{"source": "remote"}`,
			hlvResult: db.HybridLogicalVector{
				SourceID: passiveStartingCV.SourceID,
				Version:  passiveStartingCV.Value,
				PreviousVersions: db.HLVVersions{
					activeStartingCV.SourceID: activeStartingCV.Value,
				},
			},
			newCVGenerated:     false,
			useRemoteRevTreeID: true,
		},
		{
			name:                    "local wins call",
			conflictResolver:        `function(conflict) {return conflict.LocalDocument;}`,
			localBody:               `{"source":"local"}`,
			expectedBody:            `{"source":"local"}`,
			remoteBody:              `{"source":"remote"}`,
			expectedDocPushResolved: true,
			hlvResult: db.HybridLogicalVector{
				SourceID: activeStartingCV.SourceID,
				Version:  activeStartingCV.Value,
				PreviousVersions: db.HLVVersions{
					passiveStartingCV.SourceID: passiveStartingCV.Value,
				},
			},
			newCVGenerated:     false,
			useRemoteRevTreeID: false,
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
			hlvResult: db.HybridLogicalVector{
				SourceID: "NEED TO EXPAND IN TEST",
				Version:  math.MaxInt64, // # this needs to get expanded in test code to doc.Cas before comparison
				MergeVersions: db.HLVVersions{
					passiveStartingCV.SourceID: passiveStartingCV.Value,
					activeStartingCV.SourceID:  activeStartingCV.Value,
				},
			},
			mergeVersionsExpected: true,
			newCVGenerated:        true,
			useRemoteRevTreeID:    false,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()

				rt1Collection, rt1Ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
				rt2Collection, rt2Ctx := rt2.GetSingleTestDatabaseCollectionWithUser()

				docID := "doc1_" + testCase.name
				newDoc := db.CreateTestDocument(docID, "", rest.JsonToMap(t, testCase.localBody), false, 0)
				incomingHLV := &db.HybridLogicalVector{
					SourceID: "activeRT",
					Version:  12234,
				}
				// add local version
				opts := db.PutDocOptions{
					NewDoc:    newDoc,
					NewDocHLV: incomingHLV,
				}
				localDoc, _, _, err := rt1Collection.PutExistingCurrentVersion(rt1Ctx, opts)
				require.NoError(t, err)

				newDoc = db.CreateTestDocument(docID, "", rest.JsonToMap(t, testCase.remoteBody), false, 0)
				incomingHLV = &db.HybridLogicalVector{
					SourceID: "passiveRT",
					Version:  1234,
				}
				opts = db.PutDocOptions{
					NewDoc:    newDoc,
					NewDocHLV: incomingHLV,
				}
				remoteDoc, _, _, err := rt2Collection.PutExistingCurrentVersion(rt2Ctx, opts)
				require.NoError(t, err)

				customConflictResolver, err := db.NewCustomConflictResolver(ctx1, testCase.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePushAndPull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFunc:       customConflictResolver,
					ConflictResolverFuncForHLV: customConflictResolver,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 true,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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

				expectedHLV := testCase.hlvResult
				expectedHLV.CurrentVersionCAS = resolvedDoc.Cas
				if testCase.newCVGenerated {
					expectedHLV.SourceID = rt1.GetDatabase().EncodedSourceID
					expectedHLV.Version = resolvedDoc.Cas
				}
				require.Equal(t, expectedHLV, *resolvedDoc.HLV)

				if testCase.expectedDocPushResolved {
					resolvedDocBodyLocal, err := resolvedDoc.BodyBytes(rt1Ctx)
					require.NoError(t, err)
					resolvedDocRemote, err := rt2Collection.GetDocument(rt2Ctx, docID, db.DocUnmarshalAll)
					require.NoError(t, err)

					resolvedDocBodyRemote, err := resolvedDocRemote.BodyBytes(rt2Ctx)
					require.NoError(t, err)
					assert.Equal(t, string(resolvedDocBodyLocal), string(resolvedDocBodyRemote))
				}

				// merge/local wins will:
				//	- tombstones local active revision
				//  - winning merged rev is written as child of remote winning r
				//  remote wins will:
				//  - tombstones local active revision
				//  - winning rev is incoming active rev
				newRevTreeID := remoteDoc.GetRevTreeID()
				if !testCase.useRemoteRevTreeID {
					newRevTreeID = getRevTreeID(t, remoteDoc.GetRevTreeID(), []byte(testCase.expectedBody))
				}
				docHistoryLeaves := resolvedDoc.History.GetLeaves()
				require.Len(t, docHistoryLeaves, 2)
				rest.AssertRevTreeAfterHLVConflictResolution(t, resolvedDoc, newRevTreeID, localDoc.GetRevTreeID())
			})
		}
	})
}

func TestActiveReplicatorHLVConflictWhenNonWinningRevHasMoreRevisions(t *testing.T) {
	base.LongRunningTest(t)

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
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)
				ctx1 := rt1.Context()

				docID := "doc1_"
				version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
				rt2.WaitForPendingChanges()

				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, testCase.conflictResolverType, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ReplicationStatsMap:        dbReplicatorStats(t),
					ConflictResolverFuncForHLV: resolverFunc,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					Continuous:                 false,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
					for i := range 10 {
						rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
					}

					_, expectedWinnerPreConflict = rt2.GetDoc(docID) // get body for later comparison + revID assertion
				} else {
					// update doc on remote loads of times to cause large diff in revisions
					for i := range 10 {
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
					rt1Version.RevTreeID = newRevID
					rest.RequireDocVersionEqual(t, rt1Version, resolvedVersion)
				}

				expectedWinnerPreConflict, _ = db.StripInternalProperties(expectedWinnerPreConflict)
				expectedJsonBody := base.MustJSONMarshal(t, expectedWinnerPreConflict)
				actualBody, err := rt1Doc.BodyBytes(rt1ctx)
				require.NoError(t, err)
				require.JSONEq(t, string(expectedJsonBody), string(actualBody))
			})
		}
	})
}

func TestActiveReplicatorHLVConflictLocalWinsWhenNonWinningRevHasLessRevisionsLocalIsTombstoned(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
		ctx1 := rt1.Context()

		docID := "doc1_"
		rt2Version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		rt2.WaitForPendingChanges()

		// create conflicting update on rt1 and create more revisions than remote
		rt1Version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)
		rt1.WaitForPendingChanges()
		for i := range 10 {
			rt1Version = rt1.UpdateDoc(docID, rt1Version, fmt.Sprintf(`{"source":"rt1","channels":["alice"], "version": "%d"}`, i))
		}
		// delete local version
		rt1Version = rt1.DeleteDoc(docID, rt1Version)
		rt1.WaitForPendingChanges()

		resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverLocalWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
		require.NoError(t, err)

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:           200,
			ReplicationStatsMap:        dbReplicatorStats(t),
			ConflictResolverFuncForHLV: resolverFunc,
			CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
			Continuous:                 false,
			SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
		generatedRevs := make([]string, 0)
		for range requiredAdditionalRevs {
			remoteGeneration++
			previousRevID = db.CreateRevIDWithBytes(remoteGeneration, previousRevID, []byte("{}"))
			generatedRevs = append(generatedRevs, previousRevID)
		}
		newRevID := db.CreateRevIDWithBytes(remoteGeneration+1, previousRevID, []byte("{}"))
		require.NotEmpty(t, newRevID)
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

		rt1Version.RevTreeID = newRevID
		rest.RequireDocVersionEqual(t, rt1Version, rt1Doc.ExtractDocVersion())

		actualBody, err := rt1Doc.BodyBytes(rt1ctx)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(actualBody))
	})
}

func TestActiveReplicatorHLVConflictWithBothLocalAndRemoteTombstones(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
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

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:           200,
			ReplicationStatsMap:        dbReplicatorStats(t),
			ConflictResolverFuncForHLV: resolverFunc,
			CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
			Continuous:                 false,
			SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
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
	})
}

func TestActiveReplicatorConflictRemoveCVFromCache(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
		ctx1 := rt1.Context()

		docID := rest.SafeDocumentName(t, t.Name())
		rt2Version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		rt2.WaitForPendingChanges()
		rt1Version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)
		rt1.WaitForPendingChanges()

		resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverLocalWins, "", rt1.GetDatabase().Options.JavascriptTimeout)
		require.NoError(t, err)

		// add active rt to simulate two nodes on active cluster
		active := addActiveRT(t, rt1.GetDatabase().Name, rt1.TestBucket)
		defer active.Close()

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:           200,
			ReplicationStatsMap:        dbReplicatorStats(t),
			ConflictResolverFuncForHLV: resolverFunc,
			CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
			Continuous:                 true,
			SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		// wait for conflict res and for it to push to passive
		docBodyBytes := []byte(`{"channels":["alice"],"source":"rt1"}`)
		newRev := db.CreateRevIDWithBytes(2, rt2Version.RevTreeID, docBodyBytes) // generate what the new revID will be after conflict resolution
		conflictResVersion := rt1Version
		conflictResVersion.RevTreeID = newRev
		rt2.WaitForVersion(docID, conflictResVersion)

		// assert we cannot keep old cache entry for original wrote (before conflict resolution) on active cluster rest testers
		collectionActive1, ctxActive1 := active.GetSingleTestDatabaseCollectionWithUser()
		_, ok := collectionActive1.GetRevisionCacheForTest().Peek(ctxActive1, docID, rt1Version.RevTreeID)
		require.False(t, ok)
		// no peek for cv so fetch by cv, which in turn loads from bucket so assert item ahs correct history on it
		docRev, err := collectionActive1.GetRevisionCacheForTest().GetWithCV(ctxActive1, docID, &rt1Version.CV, false, false)
		require.NoError(t, err)
		assert.Equal(t, rt2Version.CV.String(), docRev.HlvHistory)

		// same for other active rest tester
		collectionRT1, ctxRT1 := rt1.GetSingleTestDatabaseCollectionWithUser()
		_, ok = collectionRT1.GetRevisionCacheForTest().Peek(ctxRT1, docID, rt1Version.RevTreeID)
		require.False(t, ok)
		docRev, err = collectionRT1.GetRevisionCacheForTest().GetWithCV(ctxRT1, docID, &rt1Version.CV, false, false)
		require.NoError(t, err)
		assert.Equal(t, rt2Version.CV.String(), docRev.HlvHistory)
	})
}

func TestActiveReplicatorV4DefaultResolverWithTombstoneLocal(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		activeRT, passiveRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

		docID := rest.SafeDocumentName(t, t.Name())
		rt1Version := activeRT.PutDoc(docID, `{"source":"activeRT","channels":["alice"]}`)
		// delete local version
		rt1Version = activeRT.DeleteDoc(docID, rt1Version)
		activeRT.WaitForPendingChanges()
		// create conflicting update on passiveRT
		rt2Version := passiveRT.PutDoc(docID, `{"source":"passiveRT","channels":["alice"]}`)
		rt2Version = passiveRT.UpdateDoc(docID, rt2Version, `{"source":"passiveRT-updated","channels":["alice"]}`)
		passiveRT.WaitForPendingChanges()

		replicationID := rest.SafeDocumentName(t, t.Name())
		activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			replicationStats, err := activeRT.GetDatabase().DbStats.DBReplicatorStats(replicationID)
			assert.NoError(c, err)
			assert.Equal(c, int64(1), replicationStats.ConflictResolvedLocalCount.Value())
		}, 10*time.Second, 100*time.Millisecond)

		docBodyBytes := []byte(`{}`)
		newRev := getRevTreeID(t, rt2Version.RevTreeID, docBodyBytes)
		conflictResVersion := rt1Version
		conflictResVersion.RevTreeID = newRev

		// expect local doc to win and still be tombstone with remote docs revision history
		sgrRunner.WaitForTombstone(docID, activeRT, conflictResVersion)

		rt1Doc := activeRT.GetDocument(docID)
		// assert that remote cv is in pv now
		assert.Equal(t, rt2Version.CV.Value, rt1Doc.HLV.PreviousVersions[rt2Version.CV.SourceID])
		// assert doc is still a tombstone
		assert.True(t, rt1Doc.IsDeleted())
		// assert on rev tree structure
		docHistoryLeaves := rt1Doc.History.GetLeaves()
		require.Len(t, docHistoryLeaves, 2)
		for _, revID := range docHistoryLeaves {
			assert.True(t, rt1Doc.History[revID].Deleted)
		}
	})
}

func TestActiveReplicatorV4DefaultResolverWithTombstoneRemote(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	// v4 protocol only test
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		activeRT, passiveRT, remoteURLString := sgrRunner.SetupSGRPeers(t)
		docID := rest.SafeDocumentName(t, t.Name())

		rt2Version := passiveRT.PutDoc(docID, `{"source":"passiveRT","channels":["alice"]}`)
		rt2Version = passiveRT.DeleteDoc(docID, rt2Version)
		passiveRT.WaitForPendingChanges()
		// create conflicting update on activeRT
		rt1Version := activeRT.PutDoc(docID, `{"source":"activeRT","channels":["alice"]}`)
		// delete local version
		rt1Version = activeRT.UpdateDoc(docID, rt1Version, `{"source":"activeRT-updated","channels":["alice"]}`)
		activeRT.WaitForPendingChanges()

		replicationID := rest.SafeDocumentName(t, t.Name())
		activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			replicationStats, err := activeRT.GetDatabase().DbStats.DBReplicatorStats(replicationID)
			assert.NoError(c, err)
			assert.Equal(c, int64(1), replicationStats.ConflictResolvedRemoteCount.Value())
		}, 10*time.Second, 100*time.Millisecond)

		// expect remote doc to win but local doc ends up with longer history and given both local and remote branches
		// are tombstoned then we end up moving local rev 3-xyz to be the current rev
		newRev := getRevTreeID(t, rt1Version.RevTreeID, []byte(db.DeletedDocument))
		conflictResVersion := rt2Version
		conflictResVersion.RevTreeID = newRev
		sgrRunner.WaitForTombstone(docID, activeRT, conflictResVersion)

		rt1Doc := activeRT.GetDocument(docID)
		// assert that remote cv is in pv now
		assert.Equal(t, rt1Version.CV.Value, rt1Doc.HLV.PreviousVersions[rt1Version.CV.SourceID])
		// assert doc is still a tombstone
		assert.True(t, rt1Doc.IsDeleted())
		// assert on rev tree structure
		docHistoryLeaves := rt1Doc.History.GetLeaves()
		require.Len(t, docHistoryLeaves, 2)
		for _, revID := range docHistoryLeaves {
			assert.True(t, rt1Doc.History[revID].Deleted)
		}
	})
}

// getRevTreeID create a revtree ID for a new revision that is a child of the parentRevID for a given body.
func getRevTreeID(t *testing.T, parentRevID string, body []byte) string {
	prevGeneration, _ := db.ParseRevID(t.Context(), parentRevID)
	require.NotEqual(t, -1, prevGeneration, "failed to parse revID %s", parentRevID)
	return db.CreateRevIDWithBytes(prevGeneration+1, parentRevID, body)
}
