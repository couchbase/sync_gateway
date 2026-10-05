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
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestActiveReplicatorPushPullLegacyRev(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		rt1, rt2 := peers.ActiveRT, peers.PassiveRT

		docIDRT2 := rest.SafeDocumentName(t, t.Name()+"rt2doc1")
		rt2InitDoc := rt2.CreateDocNoHLV(docIDRT2, db.Body{"source": "rt2", "channels": []string{username}})
		legacyRevRt2 := rt2InitDoc.GetRevTreeID()
		ctx1 := rt1.Context()

		id := rest.SafeDocumentName(t, t.Name())
		docIDRT1 := id + "rt1doc1"
		rt1InitDoc := rt1.CreateDocNoHLV(docIDRT1, db.Body{"source": "rt1", "channels": []string{username}})
		legacyRevRt1 := rt1InitDoc.GetRevTreeID()

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t, rt1.GetDatabase()),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		sgrRunner.WaitForDocReplicated(docIDRT2, rt2, rt1, rest.DocVersion{RevTreeID: legacyRevRt2})
		sgrRunner.WaitForDocReplicated(docIDRT1, rt1, rt2, rest.DocVersion{RevTreeID: legacyRevRt1})
	})
}

func TestActiveReplicatorBiDirectionalPreUpgradedDocOnPeer(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	const username = "alice"

	testCases := []struct {
		name               string
		newRevOnActivePeer bool
	}{
		{
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// |                 | SGW1        |                                | SGW2        |                                |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// |                 | Rev Tree    | HLV                            | Rev Tree    | HLV                            |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// | Initial State   | 2-abc,1-abc | none                           | 1-abc       | none                           |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// | Expected Result | 2-abc,1-abc | none 							| 2-abc,1-abc | encoded@Revision+Tree+Encoding |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			name:               "new rev on SGW1 to push",
			newRevOnActivePeer: true,
		},
		{
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// |                 | SGW1        |                                | SGW2        |                                |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// |                 | Rev Tree    | HLV                            | Rev Tree    | HLV                            |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// | Initial State   | 1-abc       | none                           | 2-abc,1-abc | none                           |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			// | Expected Result | 2-abc,1-abc | encoded@Revision+Tree+Encoding | 2-abc,1-abc | none                           |  |  |  |  |  |
			// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
			name:               "new rev on SGW2 to pull",
			newRevOnActivePeer: false,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Active is SGW1 in diagram above
				// Passive is SGW2 in diagram above
				peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
				rt1, rt2 := peers.ActiveRT, peers.PassiveRT
				ctx1 := rt1.Context()

				docID := rest.SafeDocumentName(t, t.Name())

				var legacyRevRT1, legacyRevRT2, initLegacyRevRT1, initLegacyRevRT2 string

				if tc.newRevOnActivePeer {
					// create doc on rt1 with two revisions
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "source": "rt1", "channels": []string{username}}
					rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()

					// create doc on rt2 with same body to keep revID generation the same as rev1 of the document above
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2DocInit := rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2DocInit.GetRevTreeID()
				} else {
					// create doc on rt1 with one revision
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()

					// create docs on rt2 with same body for rev 1 to keep revID generation the same as rev1 of the document above
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2DocInit := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2DocInit.GetRevTreeID()
					bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "source": "rt2", "channels": []string{username}}
					rt2DocInit = rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2DocInit.GetRevTreeID()
				}

				id := rest.SafeDocumentName(t, t.Name())
				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          id,
					Direction:   db.ActiveReplicatorTypePushAndPull,
					RemoteDBURL: userDBURL(rt2, username),
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:       200,
					Continuous:             true,
					ReplicationStatsMap:    dbReplicatorStats(t, rt1.GetDatabase()),
					CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
					SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, ar.Stop())
				}()

				// Start the replicator
				require.NoError(t, ar.Start(ctx1))

				if tc.newRevOnActivePeer {
					sgrRunner.WaitForDocReplicated(docID, rt1, rt2, rest.DocVersion{RevTreeID: legacyRevRT1})
					activeDocBeforePullBack := rest.ExpectedISGRDocFromPeer(t, rt1, docID)
					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT1, initLegacyRevRT1})

					// assert that legacy rev 2-abc isn't pulled back to rt1 now rt2 holds it with a legacy revID encoded CV
					rt1Doc := sgrRunner.RequireDocUnchanged(docID, rt2, rt1, ar, db.ActiveReplicatorTypePull, activeDocBeforePullBack)
					assert.Equal(t, legacyRevRT1, rt1Doc.GetRevTreeID())
					assert.Nil(t, rt1Doc.HLV)
					rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1})
				} else {
					sgrRunner.WaitForDocReplicated(docID, rt2, rt1, rest.DocVersion{RevTreeID: legacyRevRT2})
					passiveDocBeforePushBack := rest.ExpectedISGRDocFromPeer(t, rt2, docID)
					rt1Doc := rt1.GetDocument(docID)
					rest.RequireHistoryContains(t, rt1Doc.History, []string{initLegacyRevRT2, legacyRevRT2})

					// assert that legacy rev 2-abc isn't pushed back to rt2 now rt1 holds it with a legacy revID encoded CV
					rt2Doc := sgrRunner.RequireDocUnchanged(docID, rt1, rt2, ar, db.ActiveReplicatorTypePush, passiveDocBeforePushBack)
					assert.Equal(t, legacyRevRT2, rt2Doc.GetRevTreeID())
					assert.Nil(t, rt2Doc.HLV)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, legacyRevRT2})
				}

			})
		}
	})
}

// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// |                 | SGW1        |      | SGW2        |      |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// |                 | Rev Tree    | HLV  | Rev Tree    | HLV  |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// | Initial State   | 2-abc,1-abc | none | 2-abc,1-abc | none |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// | Expected Result | 2-abc,1-abc | none | 2-abc,1-abc | none |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
func TestActiveReplicatorBiDirectionalPreUpgradedDocOnBothSidesAlreadyKnownRev(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"
	// Passive is SGW2 in diagram above
	// Active is SGW1 in diagram above
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		rt1, rt2 := peers.ActiveRT, peers.PassiveRT
		ctx1 := rt1.Context()

		docID := rest.SafeDocumentName(t, t.Name())

		// create doc on rt2 with two revisions
		bodyRT2 := db.Body{"channels": []string{username}}
		rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
		initLegacyRevRT2 := rt2InitDoc.GetRevTreeID()
		bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
		rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
		legacyRevRT2 := rt2InitDoc.GetRevTreeID()

		// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
		bodyRT1 := db.Body{"channels": []string{username}}
		rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
		initLegacyRevRT1 := rt1InitDoc.GetRevTreeID()
		bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
		rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
		legacyRevRT1 := rt1InitDoc.GetRevTreeID()

		// need revIDs to match so the peers know the revs are known to each other
		require.Equal(t, legacyRevRT1, legacyRevRT2)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          t.Name(),
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t, rt1.GetDatabase()),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		// wait for doc checked on each side of replication
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			stats := ar.GetStatus(ctx1)
			assert.Equal(c, int64(1), stats.PushReplicationStatus.DocsCheckedPush)
			assert.Equal(c, int64(1), stats.PullReplicationStatus.DocsCheckedPull)
		}, time.Second*10, time.Millisecond*100)

		rt1Doc := rt1.GetDocument(docID)
		expVersion := rest.DocVersion{
			RevTreeID: legacyRevRT1,
		}
		rest.RequireDocRevTreeEqual(t, expVersion, rest.DocVersion{RevTreeID: rt1Doc.GetRevTreeID()})
		assert.Nil(t, rt1Doc.HLV)
		rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1})

		rt2Doc := rt2.GetDocument(docID)
		expVersion = rest.DocVersion{
			RevTreeID: legacyRevRT2,
		}
		rest.RequireDocRevTreeEqual(t, expVersion, rest.DocVersion{RevTreeID: rt2Doc.GetRevTreeID()})
		assert.Nil(t, rt2Doc.HLV)
		rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, initLegacyRevRT2})
	})
}

func TestActiveReplicatorBiDirectionalPreUpgradedRevInHistory(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"

	testCases := []struct {
		name                     string
		activePeerHasUpgradedRev bool
	}{
		{
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// |                 | SGW1              |          | SGW2              |          |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// |                 | Rev Tree          | HLV      | Rev Tree          | HLV      |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// | Initial State   | 2-abc,1-abc       | none     | 3-abc,2-abc,1-abc | 100@SGW1 |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// | Expected Result | 3-abc,2-abc,1-abc | 100@SGW1 | 3-abc,2-abc,1-abc | 100@SGW1 |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			name:                     "SGW1 has pre-upgraded rev in SGW2 history",
			activePeerHasUpgradedRev: false,
		},
		{
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// |                 | SGW1              |          | SGW2              |          |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// |                 | Rev Tree          | HLV      | Rev Tree          | HLV      |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// | Initial State   | 3-abc,2-abc,1-abc | 100@SGW1 | 2-abc,1-abc       | none     |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			// | Expected Result | 3-abc,2-abc,1-abc | 100@SGW1 | 3-abc,2-abc,1-abc | 100@SGW1 |  |  |  |  |  |
			// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
			name:                     "SGW2 has pre-upgraded rev in SGW1 history",
			activePeerHasUpgradedRev: true,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Active is SGW1 in diagram above
				// Passive is SGW2 in diagram above
				peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
				rt1, rt2 := peers.ActiveRT, peers.PassiveRT
				ctx1 := rt1.Context()

				docID := rest.SafeDocumentName(t, t.Name())

				var initLegacyRevRT2, legacyRevRT2, upgradedRevID, legacyRevRT1, initLegacyRevRT1 string
				var upgradedDocVersion rest.DocVersion
				var expectedBody string

				if !tc.activePeerHasUpgradedRev {
					// create doc on rt2 with two revisions + a third upgraded rev to give it a HLV
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2InitDoc.GetRevTreeID()
					bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
					rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2InitDoc.GetRevTreeID()
					// now give passive a third revision to RT2 (non legacy update to give it a HLV)
					inputBody := fmt.Sprintf(`{"%s": "%s", "source": "rt2", "channels": ["%s"]}`, db.BodyRev, legacyRevRT2, username)
					upgradedDocVersion = rt2.PutDoc(docID, inputBody)
					upgradedRevID = upgradedDocVersion.RevTreeID
					expectedBody = fmt.Sprintf(`{"source": "rt2", "channels": ["%s"]}`, username)

					// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
					rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()
				} else {
					// create doc on rt1 with two revisions + a third upgraded rev to give it a HLV
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
					rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()
					// now give passive a third revision to RT1 (non legacy update to give it a HLV)
					inputBody := fmt.Sprintf(`{"%s": "%s", "source": "rt1", "channels": ["%s"]}`, db.BodyRev, legacyRevRT1, username)
					upgradedDocVersion = rt1.PutDoc(docID, inputBody)
					upgradedRevID = upgradedDocVersion.RevTreeID
					expectedBody = fmt.Sprintf(`{"source": "rt1", "channels": ["%s"]}`, username)

					// create doc on rt2 with same body to keep revID generation the same as rev1 of the document above
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2InitDoc.GetRevTreeID()
					bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
					rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2InitDoc.GetRevTreeID()
				}

				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          t.Name(),
					Direction:   db.ActiveReplicatorTypePushAndPull,
					RemoteDBURL: userDBURL(rt2, username),
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:       200,
					Continuous:             true,
					ReplicationStatsMap:    dbReplicatorStats(t, rt1.GetDatabase()),
					CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
					SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, ar.Stop())
				}()

				// Start the replicator
				require.NoError(t, ar.Start(ctx1))

				if !tc.activePeerHasUpgradedRev {
					sgrRunner.WaitForDocReplicated(docID, rt2, rt1, upgradedDocVersion)

					rt1Doc := rt1.GetDocument(docID)
					rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1, upgradedRevID})
					actualBodyRT1, err := rt1Doc.BodyBytes(rt1.Context())
					require.NoError(t, err)

					// assert passive side doc hasn't changed
					rt2Doc := rt2.GetDocument(docID)
					rest.RequireDocVersionEqual(t, upgradedDocVersion, rt2Doc.ExtractDocVersion())
					rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, initLegacyRevRT2, upgradedRevID})
					actualBodyRT2, err := rt2Doc.BodyBytes(rt2.Context())
					require.NoError(t, err)

					// Assert body matches expected
					require.JSONEq(t, expectedBody, string(actualBodyRT2))
					require.JSONEq(t, expectedBody, string(actualBodyRT1))
				} else {
					sgrRunner.WaitForDocReplicated(docID, rt1, rt2, upgradedDocVersion)

					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, initLegacyRevRT2, upgradedRevID})
					actualBodyRT2, err := rt2Doc.BodyBytes(rt2.Context())
					require.NoError(t, err)

					// assert active side doc hasn't changed
					rt1Doc := rt1.GetDocument(docID)
					rest.RequireDocVersionEqual(t, upgradedDocVersion, rt1Doc.ExtractDocVersion())
					rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1, upgradedRevID})
					actualBodyRT1, err := rt1Doc.BodyBytes(rt1.Context())
					require.NoError(t, err)

					// Assert body matches expected
					require.JSONEq(t, expectedBody, string(actualBodyRT2))
					require.JSONEq(t, expectedBody, string(actualBodyRT1))
				}
			})
		}
	})
}

// Test Case:
// Doc1 :-
//
//	Push new doc not present on passive with rev 1-abc
//	This gets written as encoded@Revision+Tree+Encoding on passive for CV and revID 1-abc
//	Update this doc on active to create 100@activeSource
//	Push this rev and assert that the rev is not conflicting
//
// Doc2 :-
//
//	Create doc on passive with rev 1-abc
//	Pull this doc to active as encoded@Revision+Tree+Encoding for CV and revID 1-abc
//	Update this doc on passive to create 100@passiveSource
//	Pull this rev and assert that the rev is not conflicting
func TestActiveReplicatorPushPullNewDocLegacyRevAndAllowUpdateAfter(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		rt1, rt2 := peers.ActiveRT, peers.PassiveRT
		ctx1 := rt1.Context()

		docIDToPush := rest.SafeDocumentName(t, t.Name()+"_push")
		docIDToPull := rest.SafeDocumentName(t, t.Name()+"_pull")

		// create doc on rt1 with one revision
		bodyRT1 := db.Body{"channels": []string{username}, "source": "rt1"}
		rt1InitDoc := rt1.CreateDocNoHLV(docIDToPush, bodyRT1)
		legacyRevRt1 := rt1InitDoc.GetRevTreeID()

		// create doc on rt2 to pull with one revision
		bodyRT2 := db.Body{"channels": []string{username}, "source": "rt2"}
		rt2InitDoc := rt2.CreateDocNoHLV(docIDToPull, bodyRT2)
		legacyRevRt2 := rt2InitDoc.GetRevTreeID()

		id := rest.SafeDocumentName(t, t.Name())
		replicationStats := rest.DbReplicatorStats(t, rt1.GetDatabase(), id)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    replicationStats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		sgrRunner.WaitForDocReplicated(docIDToPull, rt2, rt1, rest.DocVersion{RevTreeID: legacyRevRt2})
		sgrRunner.WaitForDocReplicated(docIDToPush, rt1, rt2, rest.DocVersion{RevTreeID: legacyRevRt1})

		// now update both docs to create a new revision on each side giving them each hlv based off their nodes source
		cvVersion, err := db.LegacyRevToRevTreeEncodedVersion(legacyRevRt1)
		require.NoError(t, err)
		rt1DocVersion := rest.DocVersion{
			RevTreeID: legacyRevRt1,
		}
		updateVer := rt1.UpdateDoc(docIDToPush, rt1DocVersion, `{"channels": ["alice"], "source": "rt1-updated"}`)
		sgrRunner.WaitForDocReplicated(docIDToPush, rt1, rt2, updateVer)
		// check rev tree encoded version is in pv
		finalRT2Doc := rt2.GetDocument(docIDToPush)
		assert.Equal(t, cvVersion.Value, finalRT2Doc.HLV.PreviousVersions[cvVersion.SourceID])

		cvVersion, err = db.LegacyRevToRevTreeEncodedVersion(legacyRevRt2)
		require.NoError(t, err)
		rt2DocVersion := rest.DocVersion{
			RevTreeID: legacyRevRt2,
		}
		updateVer = rt2.UpdateDoc(docIDToPull, rt2DocVersion, `{"channels": ["alice"], "source": "rt2-updated"}`)
		sgrRunner.WaitForDocReplicated(docIDToPull, rt2, rt1, updateVer)
		finalRT1Doc := rt1.GetDocument(docIDToPull)
		assert.Equal(t, cvVersion.Value, finalRT1Doc.HLV.PreviousVersions[cvVersion.SourceID])

		// assert no conflicts on either side
		assert.Equal(t, int64(0), replicationStats.PushConflictCount.Value())
		// PulledCount is incremented in a defer after the pulled rev is written, so WaitForVersion above (which
		// polls the doc via a separate GET) can observe the write slightly before the stat updates. Poll here too.
		base.RequireWaitForStat(t, replicationStats.PulledCount.Value, 2)
	})
}

/// conflict test cases

// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// |                 | SGW1        |      | SGW2        |      |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// |                 | Rev Tree    | HLV  | Rev Tree    | HLV  |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// | Initial State   | 2-abc,1-abc | none | 2-def,1-abc | none |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
// | Expected Result | 2-abc,1-abc | none | 2-def,1-abc | none |  |  |  |  |  |
// +-----------------+-------------+------+-------------+------+--+--+--+--+--+
func TestActiveReplicatorPushConflictingPreUpgradedVersion(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	const username = "alice"

	// Active is SGW1 in diagram above
	// Passive is SGW2 in diagram above
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		rt1, rt2 := peers.ActiveRT, peers.PassiveRT
		ctx1 := rt1.Context()

		docID := rest.SafeDocumentName(t, t.Name())

		// create doc on rt1 with two revisions
		bodyRT1 := db.Body{"channels": []string{username}}
		rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
		initLegacyRevRT1 := rt1InitDoc.GetRevTreeID()
		bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "source": "rt1", "channels": []string{username}}
		rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
		legacyRevRT1 := rt1InitDoc.GetRevTreeID()

		// create doc on rt2 with same body to keep revID generation the same as rev1 of the document above
		bodyRT2 := db.Body{"channels": []string{username}}
		rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
		initLegacyRevRT2 := rt2InitDoc.GetRevTreeID()
		bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "source": "rt2", "channels": []string{username}}
		rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
		legacyRevRT2 := rt2InitDoc.GetRevTreeID()

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t, rt1.GetDatabase()),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		// wait for doc conflict rejection on push
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			stats := ar.GetStatus(ctx1)
			assert.Equal(c, int64(1), stats.PushReplicationStatus.DocWriteConflict)
		}, time.Second*10, time.Millisecond*100)

		// assert versions each side are not changed (can only assert on rev tree ids given legacy documents on both sides)
		rt1Doc := rt1.GetDocument(docID)
		expVersionRT1 := rest.DocVersion{
			RevTreeID: legacyRevRT1,
		}
		rest.RequireDocRevTreeEqual(t, expVersionRT1, rest.DocVersion{RevTreeID: rt1Doc.GetRevTreeID()})

		rt2Doc := rt2.GetDocument(docID)
		expVersionRT2 := rest.DocVersion{
			RevTreeID: legacyRevRT2,
		}
		rest.RequireDocRevTreeEqual(t, expVersionRT2, rest.DocVersion{RevTreeID: rt2Doc.GetRevTreeID()})
	})
}

func TestActiveReplicatorConflictPreUpgradedVersionEachSide(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"
	// NOTE: below diagrams only show active rev tree branches not tombstones branches from the conflict resolution
	testCases := []struct {
		name       string
		activeWins bool
	}{
		// +-----------------+-------------------+--------------------------------+-------------------+--------------------------------+--+--+--+--+--+
		// |                 | SGW1              |                                | SGW2              |                                |  |  |  |  |  |
		// +-----------------+-------------------+--------------------------------+-------------------+--------------------------------+--+--+--+--+--+
		// |                 | Rev Tree          | HLV                            | Rev Tree          | HLV                            |  |  |  |  |  |
		// +-----------------+-------------------+--------------------------------+-------------------+--------------------------------+--+--+--+--+--+
		// | Initial State   | 2-def,1-abc       | none                           | 2-abc,1-abc       | none                           |  |  |  |  |  |
		// +-----------------+-------------------+--------------------------------+-------------------+--------------------------------+--+--+--+--+--+
		// | Expected Result | 3-def,2-def,1-abc | encoded@Revision+Tree+Encoding | 3-def,2-def,1-abc | encoded@Revision+Tree+Encoding |  |  |  |  |  |
		// +-----------------+-------------------+--------------------------------+-------------------+--------------------------------+--+--+--+--+--+
		{
			name:       "active peer has winning rev",
			activeWins: true,
		},
		// +-----------------+-------------+--------------------------------+-------------+------+--+--+--+--+--+
		// |                 | SGW1        |                                | SGW2        |      |  |  |  |  |  |
		// +-----------------+-------------+--------------------------------+-------------+------+--+--+--+--+--+
		// |                 | Rev Tree    | HLV                            | Rev Tree    | HLV  |  |  |  |  |  |
		// +-----------------+-------------+--------------------------------+-------------+------+--+--+--+--+--+
		// | Initial State   | 2-abc,1-abc | none                           | 2-def,1-abc | none |  |  |  |  |  |
		// +-----------------+-------------+--------------------------------+-------------+------+--+--+--+--+--+
		// | Expected Result | 2-def,1-abc | encoded@Revision+Tree+Encoding | 2-def,1-abc | none |  |  |  |  |  |
		// +-----------------+-------------+--------------------------------+-------------+------+--+--+--+--+--+
		{
			name:       "passive peer has winning rev",
			activeWins: false,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Passive is SGW2 in diagram above
				// Active is SGW1 in diagram above
				peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
				rt1, rt2 := peers.ActiveRT, peers.PassiveRT
				ctx1 := rt1.Context()

				docID := rest.SafeDocumentName(t, t.Name())

				var legacyRevRT1, initLegacyRevRT1, legacyRevRT2, initLegacyRevRT2 string
				if tc.activeWins {
					// create doc on rt1 with two revisions
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}, "source": "rt1"}
					rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()

					// create doc on rt2 with same body to keep revID generation the same as rev1 of the document above
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2InitDoc.GetRevTreeID()
					bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
					rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2InitDoc.GetRevTreeID()
				} else {
					// create doc on rt2 with two revisions
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2InitDoc.GetRevTreeID()
					bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}, "source": "rt2"}
					rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2InitDoc.GetRevTreeID()

					// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
					rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()
				}

				// build conflict resolver functions
				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)
				resolverFuncRevID, err := db.NewConflictResolverFunc(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				id := rest.SafeDocumentName(t, t.Name())
				replicationStats := rest.DbReplicatorStats(t, rt1.GetDatabase(), id)

				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          id,
					Direction:   db.ActiveReplicatorTypePushAndPull,
					RemoteDBURL: userDBURL(rt2, username),
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					Continuous:                 true,
					ReplicationStatsMap:        replicationStats,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					ConflictResolverFuncForHLV: resolverFunc,
					ConflictResolverFunc:       resolverFuncRevID,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, ar.Stop())
				}()

				// Start the replicator
				require.NoError(t, ar.Start(ctx1))

				if tc.activeWins {
					// ConflictResolvedLocalCount is incremented inside the updateAndReturnDoc callback,
					// before the CAS write commits. A plain GetDoc after RequireWaitForStat can still
					// see the pre-resolution rev. Poll both the stat and the version change together so
					// verPostConflictRes is always the committed post-resolution rev when we use it below.
					var verPostConflictRes rest.DocVersion
					require.EventuallyWithT(t, func(c *assert.CollectT) {
						assert.Equal(c, int64(1), replicationStats.ConflictResolvedLocalCount.Value())
						verPostConflictRes, _ = rt1.GetDoc(docID)
						assert.NotEqual(c, legacyRevRT1, verPostConflictRes.RevTreeID)
					}, 10*time.Second, 50*time.Millisecond)
					sgrRunner.WaitForDocReplicated(docID, rt1, rt2, verPostConflictRes)
					activeDocAfterResolution := rest.ExpectedISGRDocFromPeer(t, rt1, docID)
					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, legacyRevRT2, verPostConflictRes.RevTreeID})

					// assert active side doc doesn't change by pulling back the resolution it pushed to rt2, which rt2
					// stored with a legacy CV
					rt1Doc := sgrRunner.RequireDocUnchanged(docID, rt2, rt1, ar, db.ActiveReplicatorTypePull, activeDocAfterResolution)
					rest.RequireDocRevTreeEqual(t, rest.DocVersion{RevTreeID: verPostConflictRes.RevTreeID}, rest.DocVersion{RevTreeID: rt1Doc.GetRevTreeID()})
					tombstonedID := db.CreateRevIDWithBytes(3, legacyRevRT1, []byte(db.DeletedDocument)) // create what would be the tombstone rev id for local branch
					rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1, verPostConflictRes.RevTreeID, legacyRevRT2, tombstonedID})
					// we should have legacy encoded rev tree locally now given the resolved conflict above writes a new
					// revision of the doc locally
					cvVer, err := db.LegacyRevToRevTreeEncodedVersion(rt1Doc.GetRevTreeID())
					require.NoError(t, err)
					assert.Equal(t, cvVer.String(), rt1Doc.HLV.GetCurrentVersionString())
				} else {
					base.RequireWaitForStat(t, func() int64 {
						return replicationStats.ConflictResolvedRemoteCount.Value()
					}, 1)

					sgrRunner.WaitForDocReplicated(docID, rt2, rt1, rest.DocVersion{RevTreeID: legacyRevRT2})
					passiveDocAfterResolution := rest.ExpectedISGRDocFromPeer(t, rt2, docID)
					rt1Doc := rt1.GetDocument(docID)
					tombstonedID := db.CreateRevIDWithBytes(3, legacyRevRT1, []byte(db.DeletedDocument)) // create what would be the tombstone rev id for local branch
					rest.RequireHistoryContains(t, rt1Doc.History, []string{initLegacyRevRT1, legacyRevRT1, legacyRevRT2, tombstonedID})

					// assert passive side doc doesn't change by rt1 pushing back the remote win, which rt1 stored with a
					// legacy CV
					rt2Doc := sgrRunner.RequireDocUnchanged(docID, rt1, rt2, ar, db.ActiveReplicatorTypePush, passiveDocAfterResolution)
					rest.RequireDocRevTreeEqual(t, rest.DocVersion{RevTreeID: legacyRevRT2}, rest.DocVersion{RevTreeID: rt2Doc.GetRevTreeID()})
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, legacyRevRT2})
					// legacy cv written to rt1 will correspond to local rev tree ID thus no HLV should be written yet
					assert.Nil(t, rt2Doc.HLV)
				}
			})
		}
	})
}

func TestActiveReplicatorConflictPreUpgradedVersionOneSide(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"
	// NOTE: below diagrams only show active rev tree branches not tombstones branches from the conflict resolution
	testCases := []struct {
		name                            string
		activePeerHasPostUpgradeVersion bool
	}{
		// +-----------------+-------------------+--------------------------------------+-------------------+
		// |                 | SGW1              |                  | SGW2              |                   |
		// +-----------------+-------------------+------------------+-------------------+-------------------+
		// |                 | Rev Tree          | HLV              | Rev Tree          | HLV               |
		// +-----------------+-------------------+------------------+-------------------+-------------------+
		// | Initial State   | 2-def,1-abc       | 100@SGW1         | 2-abc,1-abc       | none              |
		// +-----------------+-------------------+------------------+-------------------+-------------------+
		// | Expected Result | 3-abc,2-abc,1-abc | 3abc@RTE;100@SGW | 3-abc,2-abc,1-abc | 3abc@RTE;100@SGW1 |
		// +-----------------+-------------------+--------------------------------------+-------------------+
		{
			name:                            "active peer has post upgrade version that wins",
			activePeerHasPostUpgradeVersion: true,
		},
		// The below test cases updates the document again on active peer to ensure we can push back to passive with
		// no conflict after initial conflict resolution. Hence the expected result HLV contains active peer SGW1 as
		// current version.
		// +-----------------+-------------+-------------------------------+-------------+--------------------------------+
		// |                 | SGW1        |                               | SGW2        |                                |
		// +-----------------+-------------+-------------------------------+-------------+--------------------------------+
		// |                 | Rev Tree    | HLV                           | Rev Tree    | HLV                            |
		// +-----------------+-------------+-------------------------------+-------------+--------------------------------+
		// | Initial State   | 2-abc,1-abc | none                          | 2-def,1-abc | 100@SGW2                       |
		// +-----------------+-------------+-------------------------------+-------------+--------------------------------+
		// | Expected Result | 2-def,1-abc | 1100@SGW1;2def@RTE,oldcas@SGW2| 2-def,1-abc | 1100@SGW1;2def@RTE,oldcas@SGW2 |
		// +-----------------+-------------+-------------------------------+-------------+--------------------------------+
		{
			name:                            "passive peer has post upgrade version that wins",
			activePeerHasPostUpgradeVersion: false,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Passive (SGW2 in diagram above)
				peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
				rt1, rt2 := peers.ActiveRT, peers.PassiveRT
				ctx1 := rt1.Context()

				docID := rest.SafeDocumentName(t, t.Name())

				var legacyRevRT1, initLegacyRevRT1, legacyRevRT2, initLegacyRevRT2, expectedBody string
				var upgradedDocVersion rest.DocVersion

				if tc.activePeerHasPostUpgradeVersion {
					// create doc rt1 with two revisions and second revision has HLV
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					upgradedDocVersion = rt1.PutDoc(docID, fmt.Sprintf(`{"%s":"%s", "channels": ["alice"], "source": "rt1"}`, db.BodyRev, initLegacyRevRT1))
					expectedBody = `{"channels": ["alice"], "source": "rt1"}`
					legacyRevRT1 = upgradedDocVersion.RevTreeID

					// create doc on rt2 with same body for rev1 to keep revID generation the same as rev1 of the document above
					// but have both revisions be pre upgraded versions
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2InitDoc.GetRevTreeID()
					bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}, "source": "rt2"}
					rt2InitDoc = rt2.CreateDocNoHLV(docID, bodyRT2)
					legacyRevRT2 = rt2InitDoc.GetRevTreeID()
				} else {
					// create doc rt2 with two revisions and second revision has HLV
					bodyRT2 := db.Body{"channels": []string{username}}
					rt2InitDoc := rt2.CreateDocNoHLV(docID, bodyRT2)
					initLegacyRevRT2 = rt2InitDoc.GetRevTreeID()
					upgradedDocVersion = rt2.PutDoc(docID, fmt.Sprintf(`{"%s":"%s", "channels": ["alice"], "source": "rt2"}`, db.BodyRev, initLegacyRevRT2))
					expectedBody = `{"channels": ["alice"], "source": "rt2"}`

					// create doc on rt1 with same body for rev1 to keep revID generation the same as rev1 of the document above
					// but have both revisions be pre upgraded versions
					bodyRT1 := db.Body{"channels": []string{username}}
					rt1InitDoc := rt1.CreateDocNoHLV(docID, bodyRT1)
					initLegacyRevRT1 = rt1InitDoc.GetRevTreeID()
					bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}, "source": "rt1"}
					rt1InitDoc = rt1.CreateDocNoHLV(docID, bodyRT1)
					legacyRevRT1 = rt1InitDoc.GetRevTreeID()
				}

				// build conflict resolver functions
				resolverFunc, err := db.NewConflictResolverFuncForHLV(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)
				resolverFuncRevID, err := db.NewConflictResolverFunc(ctx1, db.ConflictResolverDefault, "", rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				id := rest.SafeDocumentName(t, t.Name())
				replicationStats := rest.DbReplicatorStats(t, rt1.GetDatabase(), id)

				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          id,
					Direction:   db.ActiveReplicatorTypePushAndPull,
					RemoteDBURL: userDBURL(rt2, username),
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					Continuous:                 true,
					ReplicationStatsMap:        replicationStats,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					ConflictResolverFuncForHLV: resolverFunc,
					ConflictResolverFunc:       resolverFuncRevID,
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, ar.Stop())
				}()

				passiveDocBeforeReplication := rest.ExpectedISGRDocFromPeer(t, rt2, docID)

				// Start the replicator
				require.NoError(t, ar.Start(ctx1))

				if tc.activePeerHasPostUpgradeVersion {
					// ConflictResolvedLocalCount is incremented inside the updateAndReturnDoc callback,
					// before the CAS write commits. A plain GetDocument after RequireWaitForStat can still
					// see the pre-resolution rev. Poll both the stat and the version change together so
					// verPostConflictRes is always the committed post-resolution rev when we use it below.
					var replicatedDoc *db.Document
					var verPostConflictRes rest.DocVersion
					require.EventuallyWithT(t, func(c *assert.CollectT) {
						assert.Equal(c, int64(1), replicationStats.ConflictResolvedLocalCount.Value())
						replicatedDoc = rt1.GetDocument(docID)
						verPostConflictRes = replicatedDoc.ExtractDocVersion()
						assert.NotEqual(c, legacyRevRT1, verPostConflictRes.RevTreeID)
					}, 10*time.Second, 50*time.Millisecond)
					// wait for this resolution to be pushed back to passive peer
					sgrRunner.WaitForDocReplicated(docID, rt1, rt2, verPostConflictRes)
					activeDocAfterResolution := rest.ExpectedISGRDocFromPeer(t, rt1, docID)

					// assert original upgraded version in PV history
					assert.Equal(t, upgradedDocVersion.CV.Value, replicatedDoc.HLV.PreviousVersions[upgradedDocVersion.CV.SourceID])

					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, legacyRevRT2, verPostConflictRes.RevTreeID})
					// assert that original upgraded version is in HLV pv on passive too
					assert.Equal(t, upgradedDocVersion.CV.Value, rt2Doc.HLV.PreviousVersions[upgradedDocVersion.CV.SourceID])

					rt2BodyBytes, err := rt2Doc.BodyBytes(rt2.Context())
					require.NoError(t, err)

					// assert active side doc doesn't change by pulling back the resolution it pushed to rt2
					rt1Doc := sgrRunner.RequireDocUnchanged(docID, rt2, rt1, ar, db.ActiveReplicatorTypePull, activeDocAfterResolution)
					rest.RequireDocVersionEqual(t, verPostConflictRes, rt1Doc.ExtractDocVersion())
					tombstonedID := db.CreateRevIDWithBytes(3, legacyRevRT1, []byte(db.DeletedDocument)) // create what would be the tombstone rev id for local branch
					rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1, verPostConflictRes.RevTreeID, legacyRevRT2, tombstonedID})

					// assert that the body is as expected each side
					rt1BodyBytes, err := rt1Doc.BodyBytes(rt1.Context())
					require.NoError(t, err)
					require.JSONEq(t, expectedBody, string(rt1BodyBytes))
					require.JSONEq(t, expectedBody, string(rt2BodyBytes))
				} else {
					base.RequireWaitForStat(t, func() int64 {
						return replicationStats.ConflictResolvedRemoteCount.Value()
					}, 1)
					// rt1 adopts rt2's CV and keeps its losing legacy branch in pv, so its HLV legitimately differs from rt2's
					sgrRunner.RequireDoc(docID, rt1, rest.ExpectedISGRDoc{
						Version:  upgradedDocVersion,
						Body:     expectedBody,
						Channels: []string{username},
					})

					rt1Doc := rt1.GetDocument(docID)
					tombstonedID := db.CreateRevIDWithBytes(3, legacyRevRT1, []byte(db.DeletedDocument)) // create what would be the tombstone rev id for local branch
					rest.RequireHistoryContains(t, rt1Doc.History, []string{initLegacyRevRT1, legacyRevRT1, upgradedDocVersion.RevTreeID, tombstonedID})

					rt1BodyBytes, err := rt1Doc.BodyBytes(rt1.Context())
					require.NoError(t, err)

					// assert passive side doc hasn't changed - rt1 adopted its CV, so there was nothing to push back
					rt2Doc := sgrRunner.RequireDocUnchanged(docID, rt1, rt2, ar, db.ActiveReplicatorTypePush, passiveDocBeforeReplication)
					rest.RequireDocVersionEqual(t, upgradedDocVersion, rt2Doc.ExtractDocVersion())
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, upgradedDocVersion.RevTreeID})

					// assert that the body is as expected each side
					rt2BodyBytes, err := rt2Doc.BodyBytes(rt2.Context())
					require.NoError(t, err)
					require.JSONEq(t, expectedBody, string(rt1BodyBytes))
					require.JSONEq(t, expectedBody, string(rt2BodyBytes))

					// update doc on active side to ensure we can push with no conflict
					updateVer := rt1.UpdateDoc(docID, upgradedDocVersion, `{"channels": ["alice"], "source": "rt1-updated"}`)
					sgrRunner.WaitForDocReplicated(docID, rt1, rt2, updateVer)
				}
			})
		}
	})
}

func TestActiveReplicatorDeltaSyncWhenBothSidesLegacy(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync only supported in EE")
	}

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
			UseDeltas:         true,
		})
		rt1, rt2 := peers.ActiveRT, peers.PassiveRT
		ctx1 := rt1.Context()

		docIDToPush := rest.SafeDocumentName(t, t.Name()+"_push")

		// create doc on rt1 with one revision
		bodyRT1 := db.Body{"channels": []string{username}, "source": "rt1"}
		rt1InitDoc := rt1.CreateDocNoHLV(docIDToPush, bodyRT1)
		legacyInitRevRt1 := rt1InitDoc.GetRevTreeID()
		// create another rev to ensure we have a rev to delta from
		bodyRT1 = db.Body{db.BodyRev: legacyInitRevRt1, "channels": []string{username}, "source": "rt1"}
		rt1InitDoc = rt1.CreateDocNoHLV(docIDToPush, bodyRT1)
		legacyRevRt1 := rt1InitDoc.GetRevTreeID()

		// create rev on rt2 that will resolve to same revID as rev one above simulating the following:
		// 1. doc created on rt1, pushed to rt2
		// 2. doc updated on rt1 to create rev2, but upgrade happens before being pushed to rt2
		// 3. doc is pushed post upgrade to rt2 and the delta from rev1 to rev2 is sent
		bodyRT2 := db.Body{"channels": []string{username}, "source": "rt1"}
		rt2InitDoc := rt2.CreateDocNoHLV(docIDToPush, bodyRT2)
		legacyRevRt2 := rt2InitDoc.GetRevTreeID()

		require.Equal(t, legacyInitRevRt1, legacyRevRt2)

		replicationStats := dbReplicatorStats(t, rt1.GetDatabase())

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          t.Name(),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    replicationStats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			DeltasEnabled:          true,
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		sgrRunner.WaitForDocReplicated(docIDToPush, rt1, rt2, rest.DocVersion{RevTreeID: legacyRevRt1})

		base.RequireWaitForStat(t, func() int64 {
			return replicationStats.PushDeltaSentCount.Value()
		}, 1)
	})
}

func TestDeltaSyncWhenOneSideHasEncodedCV(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync only supported in EE")
	}

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		peers := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
			UseDeltas:         true,
		})
		rt1, rt2 := peers.ActiveRT, peers.PassiveRT
		ctx1 := rt1.Context()

		docIDToPush := rest.SafeDocumentName(t, t.Name()+"_push")

		// create doc on rt1 with one revision
		bodyRT1 := db.Body{"channels": []string{username}, "source": "rt1"}
		rt1InitDoc := rt1.CreateDocNoHLV(docIDToPush, bodyRT1)
		legacyInitRevRt1 := rt1InitDoc.GetRevTreeID()

		replicationStats := dbReplicatorStats(t, rt1.GetDatabase())

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          t.Name(),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    replicationStats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			DeltasEnabled:          true,
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		sgrRunner.WaitForDocReplicated(docIDToPush, rt1, rt2, rest.DocVersion{RevTreeID: legacyInitRevRt1})

		// flush revision cache to remove old reference to rev 1 in rev cache
		rt1.GetDatabase().FlushRevisionCacheForTest()

		// update doc on rt1 to create a second revision with HLV
		// This should:
		// 1. update doc on rt1 to give HLV based of rt1 sourceID
		// 2. push doc to rt2 with delta from rev1 to rev2
		upgradeVersion := rt1.UpdateDoc(docIDToPush, db.DocVersion{RevTreeID: legacyInitRevRt1}, `{"channels": ["alice"], "source": "rt1-updated"}`)
		sgrRunner.WaitForDocReplicated(docIDToPush, rt1, rt2, upgradeVersion)

		base.RequireWaitForStat(t, func() int64 {
			return replicationStats.PushDeltaSentCount.Value()
		}, 1)
	})
}
