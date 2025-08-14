//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package replicatortest

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKnownRevsForCheckChangeVersion(t *testing.T) {
	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()

	// have three revisions on rt1
	versionRT1 := rt1.PutDoc(t.Name(), `{"some": "data"}`)
	for i := 0; i < 2; i++ {
		versionRT1 = rt1.UpdateDocDirectly(t.Name(), versionRT1, rest.JsonToMap(t, `{"some": "data"}`))
	}

	collectionRT1, rt1Ctx := rt1.GetSingleTestDatabaseCollectionWithUser()

	// call CheckChangeVersion with fake VV and the docID from above
	missing, possible := collectionRT1.CheckChangeVersion(rt1Ctx, t.Name(), "123@src")

	// assert that the missing revision is the CV and known revID is current revIDS of the document
	assert.Len(t, missing, 1)
	assert.Len(t, possible, 2)
	// rt1's CV and revID of the doc should be returned as known revs
	assert.Equal(t, versionRT1.CV.String(), possible[0])
	assert.Equal(t, versionRT1.RevTreeID, possible[1])
}

func TestActiveReplicatorPullRevTreeReconciliation(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

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

	docHistoryList := make([]string, 0, 11)
	docID := t.Name() + "rt2doc1"
	version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
	docHistoryList = append(docHistoryList, version.RevTreeID)

	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
	remoteDocRT2, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version.RevTreeID, remoteDocRT2.CurrentRev)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:          false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	// assert on version
	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
	remoteDocRT1, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, version, remoteDocRT1.ExtractDocVersion())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	for i := 0; i < 10; i++ {
		version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
		docHistoryList = append(docHistoryList, version.RevTreeID)
	}

	// start again for new revisions
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version.RevTreeID, rt1Doc.CurrentRev)
	assert.Len(t, rt1Doc.History.GetLeaves(), 1)
	assert.Len(t, rt1Doc.History, 11) // 1 base + 10 updates

	rest.RequireDocVersionEqual(t, version, rt1Doc.ExtractDocVersion())
	assert.ElementsMatch(t, slices.Collect(maps.Keys(rt1Doc.History)), docHistoryList)
}

func TestActiveReplicatorPushRevTreeReconciliation(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

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

	docHistoryList := make([]string, 0, 11)
	docID := t.Name() + "rt1doc1"
	version := rt1.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
	docHistoryList = append(docHistoryList, version.RevTreeID)

	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
	localDocRT1, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version.RevTreeID, localDocRT1.CurrentRev)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:          false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
	rt2Doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	// assert on version
	rest.RequireDocVersionEqual(t, version, rt2Doc.ExtractDocVersion())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	for i := 0; i < 10; i++ {
		version = rt1.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
		docHistoryList = append(docHistoryList, version.RevTreeID)
	}

	// start again for new revisions
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt2Doc, err = rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, version.RevTreeID, rt2Doc.CurrentRev)
	assert.Len(t, rt2Doc.History.GetLeaves(), 1)
	assert.Len(t, rt2Doc.History, 11) // 1 base + 10 updates

	rest.RequireDocVersionEqual(t, version, rt2Doc.ExtractDocVersion())
	assert.ElementsMatch(t, slices.Collect(maps.Keys(rt2Doc.History)), docHistoryList)
}

func TestActiveReplicatorPullRevtreeLargeDiffInSize(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

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

	docID := t.Name() + "rt2doc1"
	version := rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:          false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()

	localDocRT1, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, version, localDocRT1.ExtractDocVersion())

	// update doc hundreds of times to create a large diff in rev tree versions
	for i := 0; i < 200; i++ {
		version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
	}

	// start replicator again for new revisions
	assert.NoError(t, ar.Start(ctx1))

	changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	localDocRT1, err = rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, version, localDocRT1.ExtractDocVersion())

	remoteDocRT, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, len(localDocRT1.History), len(remoteDocRT.History))
	assert.Equal(t, localDocRT1.HLV.GetCurrentVersionString(), remoteDocRT.HLV.GetCurrentVersionString())
	assert.ElementsMatch(t, slices.Collect(maps.Keys(localDocRT1.History)), slices.Collect(maps.Keys(remoteDocRT.History)))
}

func TestActiveReplicatorPushRevtreeLargeDiffInSize(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

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

	docID := t.Name() + "rt2doc1"
	version := rt1.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: userDBURL(rt2, username),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
		Continuous:          false,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
	rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()

	remoteDocRT2, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	rest.RequireDocVersionEqual(t, version, remoteDocRT2.ExtractDocVersion())

	// update doc hundreds of times to create a large diff in rev tree versions
	for i := 0; i < 200; i++ {
		version = rt1.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
	}

	// start replicator again for new revisions
	assert.NoError(t, ar.Start(ctx1))

	changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
	changesResults.RequireDocIDs(t, []string{docID})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	}, time.Second*20, time.Millisecond*100)

	localDocRT1, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	remoteDocRT, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	// assert remote version of doc is the same as last written local version
	rest.RequireDocVersionEqual(t, version, remoteDocRT.ExtractDocVersion())
	// assert history is in sync
	assert.Equal(t, len(localDocRT1.History), len(remoteDocRT.History))
	assert.Equal(t, localDocRT1.HLV.GetCurrentVersionString(), remoteDocRT.HLV.GetCurrentVersionString())
	assert.ElementsMatch(t, slices.Collect(maps.Keys(localDocRT1.History)), slices.Collect(maps.Keys(remoteDocRT.History)))
}
