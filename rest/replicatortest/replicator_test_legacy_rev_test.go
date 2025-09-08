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

func TestActiveReplicatorPullLegacyRev(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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

	docID := t.Name() + "rt2doc1"
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	body := db.Body{"source": "rt2", "channels": []string{username}}
	legacyRev, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, body)

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

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
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
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPull)

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
	doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	encodedCV, err := db.LegacyRevToRevTreeEncodedVersion(legacyRev)
	require.NoError(t, err)
	expVersion := rest.DocVersion{
		RevTreeID: legacyRev,
		CV:        encodedCV,
	}
	rest.RequireDocVersionEqual(t, expVersion, doc.ExtractDocVersion())

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])
}

func TestActiveReplicatorPushLegacyRev(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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

	docID := rest.SafeDocumentName(t, t.Name())
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	body := db.Body{"source": "rt1", "channels": []string{username}}
	legacyRev, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, body)

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
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
	doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	encodedCV, err := db.LegacyRevToRevTreeEncodedVersion(legacyRev)
	require.NoError(t, err)
	expVersion := rest.DocVersion{
		RevTreeID: legacyRev,
		CV:        encodedCV,
	}
	rest.RequireDocVersionEqual(t, expVersion, doc.ExtractDocVersion())

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])
}

// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// |                 | SGW1        |                                | SGW2        |                                |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// |                 | Rev Tree    | HLV                            | Rev Tree    | HLV                            |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// | Initial State   | 2-abc,1-abc | none                           | 1-abc       | none                           |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// | Expected Result | 2-abc,1-abc | none 							| 2-abc,1-abc | encoded@Revision+Tree+Encoding |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
func TestActiveReplicatorBiDirectionalPreUpgradedDocOnSG1ToPush(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	bodyRT1 := db.Body{"channels": []string{username}}
	initLegacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)
	bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "source": "rt1", "channels": []string{username}}
	legacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)

	// create doc on rt2 with same body to keep revID generation the same as rev1 of the document above
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	bodyRT2 := db.Body{"channels": []string{username}}
	legacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)

	rt1.WaitForPendingChanges()
	rt2.WaitForPendingChanges()

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

	since, err := rt2Collection.LastSequence(rt2ctx)
	require.NoError(t, err)

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for rev 2 to arrive at rt2
	changesResults := rt2.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	rt2Doc, err := rt2Collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	encodedCV, err := db.LegacyRevToRevTreeEncodedVersion(legacyRevRT1)
	require.NoError(t, err)
	expVersion := rest.DocVersion{
		RevTreeID: legacyRevRT1,
		CV:        encodedCV,
	}
	rest.RequireDocVersionEqual(t, expVersion, rt2Doc.ExtractDocVersion())

	// assert that rev tree is expected
	require.Len(t, rt2Doc.History, 2)
	rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT1, legacyRevRT2})

	// add new do for some replication activity on pull side to allow us to assert that legacy rev 2-abc added
	// isn't pulled back to rt1 now it has a legacy revID encoded CV
	newDocVersion := rt2.PutDoc("newdoc", `{"channels": ["alice"]}`)
	rt1.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt2

	// assert that the document isn't replicated back to rt1 after legacy rev CV is written on rt2
	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, legacyRevRT1, rt1Doc.GetRevTreeID())
	assert.Nil(t, rt1Doc.HLV)

	require.Len(t, rt1Doc.History, 2)
	rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, legacyRevRT2})
}

// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// |                 | SGW1        |                                | SGW2        |                                |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// |                 | Rev Tree    | HLV                            | Rev Tree    | HLV                            |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// | Initial State   | 1-abc       | none                           | 2-abc,1-abc | none                           |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
// | Expected Result | 2-abc,1-abc | encoded@Revision+Tree+Encoding | 2-abc,1-abc | none                           |  |  |  |  |  |
// +-----------------+-------------+--------------------------------+-------------+--------------------------------+--+--+--+--+--+
func TestActiveReplicatorBiDirectionalPreUpgradedDocOnSG2ToPull(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	bodyRT2 := db.Body{"channels": []string{username}}
	initLegacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)
	bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "source": "rt1", "channels": []string{username}}
	legacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)

	// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	bodyRT1 := db.Body{"channels": []string{username}}
	legacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)

	rt1.WaitForPendingChanges()
	rt2.WaitForPendingChanges()

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

	since, err := rt1Collection.LastSequence(rt2ctx)
	require.NoError(t, err)

	// Start the replicator
	require.NoError(t, ar.Start(ctx1))

	// wait for rev 2 to arrive at rt1
	changesResults := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	encodedCV, err := db.LegacyRevToRevTreeEncodedVersion(legacyRevRT2)
	require.NoError(t, err)
	expVersion := rest.DocVersion{
		RevTreeID: legacyRevRT2,
		CV:        encodedCV,
	}
	rest.RequireDocVersionEqual(t, expVersion, rt1Doc.ExtractDocVersion())

	// assert that rev tree is expected
	require.Len(t, rt1Doc.History, 2)
	rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, legacyRevRT2})

	// add new do for some replication activity on push side to allow us to assert that legacy rev 2-abc added
	// isn't pushed back to rt2 now it has a legacy revID encoded CV
	newDocVersion := rt1.PutDoc("newdoc", `{"channels": ["alice"]}`)
	rt2.WaitForVersion("newdoc", newDocVersion) // wait for it to arrive at rt2

	// assert that the document isn't replicated back to rt2 after legacy rev CV is written on rt1
	rt2Doc, err := rt2Collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, legacyRevRT2, rt2Doc.GetRevTreeID())
	assert.Nil(t, rt2Doc.HLV)

	require.Len(t, rt2Doc.History, 2)
	rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, legacyRevRT1})
}

// Test case 3
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	bodyRT2 := db.Body{"channels": []string{username}}
	initLegacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)
	bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
	legacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)

	// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	bodyRT1 := db.Body{"channels": []string{username}}
	initLegacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)
	bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
	legacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)

	rt1.WaitForPendingChanges()
	rt2.WaitForPendingChanges()
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

	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	expVersion := rest.DocVersion{
		RevTreeID: legacyRevRT1,
	}
	rest.RequireDocRevTreeEqual(t, expVersion, rest.DocVersion{RevTreeID: rt1Doc.GetRevTreeID()})
	assert.Nil(t, rt1Doc.HLV)
	require.Len(t, rt1Doc.History, 2)
	rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1})

	rt2Doc, err := rt2Collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	expVersion = rest.DocVersion{
		RevTreeID: legacyRevRT2,
	}
	rest.RequireDocRevTreeEqual(t, expVersion, rest.DocVersion{RevTreeID: rt2Doc.GetRevTreeID()})
	assert.Nil(t, rt2Doc.HLV)
	require.Len(t, rt2Doc.History, 2)
	rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, initLegacyRevRT2})
}

// Test case 4
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// |                 | SGW1              |          | SGW2              |          |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// |                 | Rev Tree          | HLV      | Rev Tree          | HLV      |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// | Initial State   | 2-abc,1-abc       | none     | 3-abc,2-abc,1-abc | 100@SGW1 |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// | Expected Result | 3-abc,2-abc,1-abc | 100@SGW1 | 3-abc,2-abc,1-abc | 100@SGW1 |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
func TestActiveReplicatorBiDirectionalSG1HasPreUpgradedRevInSG2History(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	bodyRT2 := db.Body{"channels": []string{username}}
	initLegacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)
	bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
	legacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)

	// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	bodyRT1 := db.Body{"channels": []string{username}}
	initLegacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)
	bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
	legacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)

	// now give passive a third revision to RT2 (non legacy update to give it a HLV)
	bodyRT2 = db.Body{db.BodyRev: legacyRevRT2, "source": "rt2", "channels": []string{username}}
	upgradedRevID, upgradedRT2MutationDoc, err := rt2Collection.Put(rt2ctx, docID, bodyRT2)
	require.NoError(t, err)
	upgradedRT2Version := upgradedRT2MutationDoc.ExtractDocVersion()
	expectedBody, err := upgradedRT2MutationDoc.BodyBytes(rt2ctx)
	require.NoError(t, err)

	rt1.WaitForPendingChanges()
	rt2.WaitForPendingChanges()

	since, err := rt1Collection.LastSequence(rt1ctx)
	require.NoError(t, err)

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

	// wait for rev 3 to arrive at rt1
	changesResults := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, upgradedRT2Version, rt1Doc.ExtractDocVersion())
	require.Len(t, rt1Doc.History, 3)
	rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1, upgradedRevID})
	actualBodyRT1, err := rt1Doc.BodyBytes(rt2ctx)
	require.NoError(t, err)

	// assert passive side doc hasn't changed
	rt2Doc, err := rt2Collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, upgradedRT2Version, rt2Doc.ExtractDocVersion())
	require.Len(t, rt2Doc.History, 3)
	rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, initLegacyRevRT2, upgradedRevID})
	actualBodyRT2, err := rt2Doc.BodyBytes(rt2ctx)
	require.NoError(t, err)

	// Assert body matches expected
	require.JSONEq(t, string(expectedBody), string(actualBodyRT2))
	require.JSONEq(t, string(expectedBody), string(actualBodyRT1))
}

// Test case 5
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// |                 | SGW1              |          | SGW2              |          |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// |                 | Rev Tree          | HLV      | Rev Tree          | HLV      |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// | Initial State   | 3-abc,2-abc,1-abc | 100@SGW1 | 2-abc,1-abc       | none     |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
// | Expected Result | 3-abc,2-abc,1-abc | 100@SGW1 | 3-abc,2-abc,1-abc | 100@SGW1 |  |  |  |  |  |
// +-----------------+-------------------+----------+-------------------+----------+--+--+--+--+--+
func TestActiveReplicatorBiDirectionalSG2HasPreUpgradedRevInSG1History(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	bodyRT2 := db.Body{"channels": []string{username}}
	initLegacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)
	bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "channels": []string{username}}
	legacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)

	// create doc on rt1 with same body to keep revID generation the same as rev1 of the document above
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	bodyRT1 := db.Body{"channels": []string{username}}
	initLegacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)
	bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "channels": []string{username}}
	legacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)

	// now create a third revision to RT1 (non legacy update to give it a HLV)
	bodyRT2 = db.Body{db.BodyRev: legacyRevRT1, "source": "rt1", "channels": []string{username}}
	upgradedRevID, upgradedRT1MutationDoc, err := rt1Collection.Put(rt1ctx, docID, bodyRT2)
	require.NoError(t, err)
	upgradedRT1Version := upgradedRT1MutationDoc.ExtractDocVersion()
	expectedBody, err := upgradedRT1MutationDoc.BodyBytes(rt1ctx)
	require.NoError(t, err)

	rt1.WaitForPendingChanges()
	rt2.WaitForPendingChanges()

	since, err := rt2Collection.LastSequence(rt2ctx)
	require.NoError(t, err)

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

	// wait for rev 3 to arrive at rt2
	changesResults := rt2.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", since), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	// assert on replicated version on rt2
	rt2Doc, err := rt2Collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, upgradedRT1Version, rt2Doc.ExtractDocVersion())
	require.Len(t, rt2Doc.History, 3)
	rest.RequireHistoryContains(t, rt2Doc.History, []string{legacyRevRT2, initLegacyRevRT2, upgradedRevID})
	actualBodyRT2, err := rt2Doc.BodyBytes(rt2ctx)
	require.NoError(t, err)

	// assert rt1 version hasn't changed
	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, upgradedRT1Version, rt1Doc.ExtractDocVersion())
	require.Len(t, rt1Doc.History, 3)
	rest.RequireHistoryContains(t, rt1Doc.History, []string{legacyRevRT1, initLegacyRevRT1, upgradedRevID})
	actualBodyRT1, err := rt1Doc.BodyBytes(rt1ctx)
	require.NoError(t, err)

	// Assert body matches expected
	require.JSONEq(t, string(expectedBody), string(actualBodyRT2))
	require.JSONEq(t, string(expectedBody), string(actualBodyRT1))
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
	rt1Collection, rt1ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	bodyRT1 := db.Body{"channels": []string{username}}
	initLegacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)
	bodyRT1 = db.Body{db.BodyRev: initLegacyRevRT1, "source": "rt1", "channels": []string{username}}
	legacyRevRT1, _ := rt1Collection.CreateDocNoHLV(t, rt1ctx, docID, bodyRT1)

	// create doc on rt2 with same body to keep revID generation the same as rev1 of the document above
	rt2Collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
	bodyRT2 := db.Body{"channels": []string{username}}
	initLegacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)
	bodyRT2 = db.Body{db.BodyRev: initLegacyRevRT2, "source": "rt2", "channels": []string{username}}
	legacyRevRT2, _ := rt2Collection.CreateDocNoHLV(t, rt2ctx, docID, bodyRT2)

	rt1.WaitForPendingChanges()
	rt2.WaitForPendingChanges()

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
	rt1Doc, err := rt1Collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	expVersionRT1 := rest.DocVersion{
		RevTreeID: legacyRevRT1,
	}
	rest.RequireDocRevTreeEqual(t, expVersionRT1, rest.DocVersion{RevTreeID: rt1Doc.GetRevTreeID()})

	rt2Doc, err := rt2Collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	expVersionRT2 := rest.DocVersion{
		RevTreeID: legacyRevRT2,
	}
	rest.RequireDocRevTreeEqual(t, expVersionRT2, rest.DocVersion{RevTreeID: rt2Doc.GetRevTreeID()})
}
