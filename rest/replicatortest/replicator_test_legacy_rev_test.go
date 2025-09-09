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
	base.RequireNumTestBuckets(t, 2)

	const username = "alice"

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Name: "passivedb",
			}},
		})
	defer rt2.Close()

	rt2.CreateUser(username, []string{username})

	docIDRT2 := t.Name() + "rt2doc1"
	rt2InitDoc := rt2.CreateDocNoHLV(docIDRT2, db.Body{"source": "rt2", "channels": []string{username}})
	legacyRevRt2 := rt2InitDoc.GetRevTreeID()

	// Active
	rt1 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Name: "activedb",
			}},
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	docIDRT1 := t.Name() + "rt1doc1"
	rt1InitDoc := rt1.CreateDocNoHLV(docIDRT1, db.Body{"source": "rt1", "channels": []string{username}})
	legacyRevRt1 := rt1InitDoc.GetRevTreeID()

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, ar.Stop())
	}()

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	rt1.WaitForLegacyRev(docIDRT2, legacyRevRt2, []byte(`{"source":"rt2","channels":["alice"]}`))
	rt2.WaitForLegacyRev(docIDRT1, legacyRevRt1, []byte(`{"source":"rt1","channels":["alice"]}`))
}

func TestActiveReplicatorBiDirectionalPreUpgradedDocOnPeer(t *testing.T) {
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
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Passive (SGW2 in diagram above)
			rt2 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						Name: "passivedb",
					}},
				})
			defer rt2.Close()

			rt2.CreateUser(username, []string{username})

			// Active (SGW1 in diagram above)
			rt1 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						Name: "activedb",
					}},
				})
			defer rt1.Close()
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

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:    200,
				Continuous:          true,
				ReplicationStatsMap: dbReplicatorStats(t),
				CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
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
	base.RequireNumTestBuckets(t, 2)

	const username = "alice"

	// Passive (SGW2 in diagram above)
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Name: "passivedb",
			}},
		})
	defer rt2.Close()

	rt2.CreateUser(username, []string{username})

	// Active (SGW1 in diagram above)
	rt1 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Name: "activedb",
			}},
		})
	defer rt1.Close()
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
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
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
}

func TestActiveReplicatorBiDirectionalPreUpgradedRevInHistory(t *testing.T) {
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
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Passive (SGW2 in diagram above)
			rt2 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						Name: "passivedb",
					}},
				})
			defer rt2.Close()

			rt2.CreateUser(username, []string{username})

			// Active (SGW1 in diagram above)
			rt1 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						Name: "activedb",
					}},
				})
			defer rt1.Close()
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
				ChangesBatchSize:    200,
				Continuous:          true,
				ReplicationStatsMap: dbReplicatorStats(t),
				CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
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

	// Passive (SGW2 in diagram above)
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Name: "passivedb",
			}},
		})
	defer rt2.Close()

	rt2.CreateUser(username, []string{username})

	// Active (SGW1 in diagram above)
	rt1 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Name: "activedb",
			}},
		})
	defer rt1.Close()
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
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
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
}
