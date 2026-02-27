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
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveReplicatorPushPullLegacyRev(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})

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
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()

		// Start the replicator
		require.NoError(t, ar.Start(ctx1))

		rt1.WaitForLegacyRev(docIDRT2, legacyRevRt2, []byte(`{"source":"rt2","channels":["alice"]}`))
		rt2.WaitForLegacyRev(docIDRT1, legacyRevRt1, []byte(`{"source":"rt1","channels":["alice"]}`))
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
				rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
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
					ReplicationStatsMap:    dbReplicatorStats(t),
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
					rt2.WaitForLegacyRev(docID, legacyRevRT1, []byte(`{"source":"rt1","channels":["alice"]}`))
					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT1, initLegacyRevRT1})

					// add new doc for some replication activity on pull side to allow us to assert that legacy rev 2-abc added
					// isn't pulled back to rt1 now it has a legacy revID encoded CV
					newDocVersion := rt2.PutDoc("newdoc", `{"channels": ["alice"]}`)
					rt1.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt2

					// assert that the document isn't replicated back to rt1 after legacy rev CV is written on rt2
					rt1Doc := rt1.GetDocument(docID)
					assert.Equal(t, legacyRevRT1, rt1Doc.GetRevTreeID())
					assert.Nil(t, rt1Doc.HLV)
					rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1})
				} else {
					rt1.WaitForLegacyRev(docID, legacyRevRT2, []byte(`{"source":"rt2","channels":["alice"]}`))
					rt1Doc := rt1.GetDocument(docID)
					rest.RequireHistoryContains(t, rt1Doc.History, []string{initLegacyRevRT2, legacyRevRT2})

					// add new doc for some replication activity on push side to allow us to assert that legacy rev 2-abc added
					// isn't pushed back to rt2 now it has a legacy revID encoded CV
					newDocVersion := rt1.PutDoc("newdoc", `{"channels": ["alice"]}`)
					rt2.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt2

					// assert that the document isn't replicated back to rt2 after legacy rev CV is written on rt1
					rt2Doc := rt2.GetDocument(docID)
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
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
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
			ReplicationStatsMap:    dbReplicatorStats(t),
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
				rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
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
					ReplicationStatsMap:    dbReplicatorStats(t),
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
					rt1.WaitForVersion(docID, upgradedDocVersion)

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
					rt2.WaitForVersion(docID, upgradedDocVersion)

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
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
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
		stats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
		require.NoError(t, err)
		replicationStats, err := stats.DBReplicatorStats(id)
		require.NoError(t, err)

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

		rt1.WaitForLegacyRev(docIDToPull, legacyRevRt2, []byte(`{"source":"rt2","channels":["alice"]}`))
		rt2.WaitForLegacyRev(docIDToPush, legacyRevRt1, []byte(`{"source":"rt1","channels":["alice"]}`))

		// now update both docs to create a new revision on each side giving them each hlv based off their nodes source
		cvVersion, err := db.LegacyRevToRevTreeEncodedVersion(legacyRevRt1)
		require.NoError(t, err)
		rt1DocVersion := rest.DocVersion{
			RevTreeID: legacyRevRt1,
		}
		updateVer := rt1.UpdateDoc(docIDToPush, rt1DocVersion, `{"channels": ["alice"], "source": "rt1-updated"}`)
		rt2.WaitForVersion(docIDToPush, updateVer)
		// check rev tree encoded version is in pv
		finalRT2Doc := rt2.GetDocument(docIDToPush)
		assert.Equal(t, cvVersion.Value, finalRT2Doc.HLV.PreviousVersions[cvVersion.SourceID])

		cvVersion, err = db.LegacyRevToRevTreeEncodedVersion(legacyRevRt2)
		require.NoError(t, err)
		rt2DocVersion := rest.DocVersion{
			RevTreeID: legacyRevRt2,
		}
		updateVer = rt2.UpdateDoc(docIDToPull, rt2DocVersion, `{"channels": ["alice"], "source": "rt2-updated"}`)
		rt1.WaitForVersion(docIDToPull, updateVer)
		finalRT1Doc := rt1.GetDocument(docIDToPull)
		assert.Equal(t, cvVersion.Value, finalRT1Doc.HLV.PreviousVersions[cvVersion.SourceID])

		// assert no conflicts on either side
		assert.Equal(t, int64(0), replicationStats.PushConflictCount.Value())
		assert.Equal(t, int64(2), replicationStats.PulledCount.Value())
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
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
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
			ReplicationStatsMap:    dbReplicatorStats(t),
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
				rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
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
				stats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
				require.NoError(t, err)
				replicationStats, err := stats.DBReplicatorStats(id)
				require.NoError(t, err)

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
					base.RequireWaitForStat(t, func() int64 {
						return replicationStats.ConflictResolvedLocalCount.Value()
					}, 1)
					verPostConflictRes, _ := rt1.GetDoc(docID)
					rt2.WaitForLegacyRev(docID, verPostConflictRes.RevTreeID, []byte(`{"channels":["alice"],"source":"rt1"}`))
					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, legacyRevRT2, verPostConflictRes.RevTreeID})

					// add doc for some replication activity on pull to assert that local doesn't change after remote is written with legacy CV
					newDocVersion := rt2.PutDoc("newdoc", `{"channels": ["alice"]}`)
					rt1.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt1

					// assert active side doc hasn't changed
					rt1Doc := rt1.GetDocument(docID)
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

					rt1.WaitForLegacyRev(docID, legacyRevRT2, []byte(`{"channels":["alice"],"source":"rt2"}`))
					rt1Doc := rt1.GetDocument(docID)
					tombstonedID := db.CreateRevIDWithBytes(3, legacyRevRT1, []byte(db.DeletedDocument)) // create what would be the tombstone rev id for local branch
					rest.RequireHistoryContains(t, rt1Doc.History, []string{initLegacyRevRT1, legacyRevRT1, legacyRevRT2, tombstonedID})

					// add doc for some replication activity on push to assert that local doesn't change after remote is written with legacy CV
					newDocVersion := rt1.PutDoc("newdoc", `{"channels": ["alice"]}`)
					rt2.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt2

					// assert passive side doc hasn't changed
					rt2Doc := rt2.GetDocument(docID)
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
				rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})
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
					expectedBody = fmt.Sprintf(`{"channels": ["alice"], "source": "rt1"}`)
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
					expectedBody = fmt.Sprintf(`{"channels": ["alice"], "source": "rt2"}`)

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
				stats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
				require.NoError(t, err)
				replicationStats, err := stats.DBReplicatorStats(id)
				require.NoError(t, err)

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

				if tc.activePeerHasPostUpgradeVersion {
					base.RequireWaitForStat(t, func() int64 {
						return replicationStats.ConflictResolvedLocalCount.Value()
					}, 1)
					replicatedDoc := rt1.GetDocument(docID)
					verPostConflictRes := replicatedDoc.ExtractDocVersion()
					// wait for this resolution to be pushed back to passive peer
					rt2.WaitForVersion(docID, verPostConflictRes)

					// assert original upgraded version in PV history
					assert.Equal(t, upgradedDocVersion.CV.Value, replicatedDoc.HLV.PreviousVersions[upgradedDocVersion.CV.SourceID])

					rt2Doc := rt2.GetDocument(docID)
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, legacyRevRT2, verPostConflictRes.RevTreeID})
					// assert that original upgraded version is in HLV pv on passive too
					assert.Equal(t, upgradedDocVersion.CV.Value, rt2Doc.HLV.PreviousVersions[upgradedDocVersion.CV.SourceID])

					rt2BodyBytes, err := rt2Doc.BodyBytes(rt2.Context())
					require.NoError(t, err)

					// add doc for some replication activity on pull to assert that local doesn't change after remote is written with legacy CV
					newDocVersion := rt2.PutDoc("newdoc", `{"channels": ["alice"]}`)
					rt1.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt1

					// assert active side doc hasn't changed
					rt1Doc := rt1.GetDocument(docID)
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
					rt1.WaitForVersion(docID, upgradedDocVersion)

					rt1Doc := rt1.GetDocument(docID)
					tombstonedID := db.CreateRevIDWithBytes(3, legacyRevRT1, []byte(db.DeletedDocument)) // create what would be the tombstone rev id for local branch
					rest.RequireHistoryContains(t, rt1Doc.History, []string{initLegacyRevRT1, legacyRevRT1, upgradedDocVersion.RevTreeID, tombstonedID})

					rt1BodyBytes, err := rt1Doc.BodyBytes(rt1.Context())
					require.NoError(t, err)

					// add doc for some replication activity on push to assert that remote doesn't change after remote wins conflict res is done
					newDocVersion := rt1.PutDoc("newdoc", `{"channels": ["alice"]}`)
					rt2.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt2

					// assert passive side doc hasn't changed
					rt2Doc := rt2.GetDocument(docID)
					rest.RequireDocVersionEqual(t, upgradedDocVersion, rt2Doc.ExtractDocVersion())
					rest.RequireHistoryContains(t, rt2Doc.History, []string{initLegacyRevRT2, upgradedDocVersion.RevTreeID})

					// assert that the body is as expected each side
					rt2BodyBytes, err := rt2Doc.BodyBytes(rt2.Context())
					require.NoError(t, err)
					require.JSONEq(t, expectedBody, string(rt1BodyBytes))
					require.JSONEq(t, expectedBody, string(rt2BodyBytes))

					// update doc on active side to ensure we can push with no conflict
					updateVer := rt1.UpdateDoc(docID, upgradedDocVersion, `{"channels": ["alice"], "source": "rt1-updated"}`)
					rt2.WaitForVersion(docID, updateVer)
				}
			})
		}
	})
}

func TestActiveReplicatorDeltaSyncWhenBothSidesLegacy(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	t.Skip("pending fix from CBG-5106")

	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync only supported in EE")
	}

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.RunSubprotocolV4(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
			ActiveRestTesterConfig: &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Name: "activedb",
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.Ptr(true),
					},
				}},
			},
			PassiveRestTesterConfig: &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Name: "passivedb",
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.Ptr(true),
					},
				}},
			},
		})
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

		stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
		require.NoError(t, err)
		replicationStats, err := stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)

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

		rt2.WaitForLegacyRev(docIDToPush, legacyRevRt1, []byte(`{"source":"rt1","channels":["alice"]}`))

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
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
			ActiveRestTesterConfig: &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Name: "activedb",
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.Ptr(true),
					},
				}},
			},
			PassiveRestTesterConfig: &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Name: "passivedb",
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.Ptr(true),
					},
				}},
			},
		})
		ctx1 := rt1.Context()

		docIDToPush := rest.SafeDocumentName(t, t.Name()+"_push")

		// create doc on rt1 with one revision
		bodyRT1 := db.Body{"channels": []string{username}, "source": "rt1"}
		rt1InitDoc := rt1.CreateDocNoHLV(docIDToPush, bodyRT1)
		legacyInitRevRt1 := rt1InitDoc.GetRevTreeID()

		stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
		require.NoError(t, err)
		replicationStats, err := stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)

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

		rt2.WaitForLegacyRev(docIDToPush, legacyInitRevRt1, []byte(`{"source":"rt1","channels":["alice"]}`))

		// flush revision cache to remove old reference to rev 1 in rev cache
		rt1.GetDatabase().FlushRevisionCacheForTest()

		// update doc on rt1 to create a second revision with HLV
		// This should:
		// 1. update doc on rt1 to give HLV based of rt1 sourceID
		// 2. push doc to rt2 with delta from rev1 to rev2
		upgradeVersion := rt1.UpdateDoc(docIDToPush, db.DocVersion{RevTreeID: legacyInitRevRt1}, `{"channels": ["alice"], "source": "rt1-updated"}`)
		rt1.WaitForVersion(docIDToPush, upgradeVersion)

		base.RequireWaitForStat(t, func() int64 {
			return replicationStats.PushDeltaSentCount.Value()
		}, 1)
	})
}
