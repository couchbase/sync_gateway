//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package replicatortest

import (
	"testing"

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

	// Passive
	const username = "alice"

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

	expVersion := rest.DocVersion{
		RevTreeID: legacyRev,
		CV: db.Version{
			SourceID: rt1.GetDatabase().EncodedSourceID,
			Value:    doc.Cas,
		},
	}
	rest.RequireDocVersionEqual(t, expVersion, doc.ExtractDocVersion())

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])
}
