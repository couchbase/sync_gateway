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

func TestActiveReplicatorRevTreeReconciliation(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

	testCases := []struct {
		name            string
		replicationType db.ActiveReplicatorDirection
	}{
		{
			name:            "pull",
			replicationType: db.ActiveReplicatorTypePull,
		},
		{
			name:            "push",
			replicationType: db.ActiveReplicatorTypePush,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
			docID := "doc1_" + tc.name
			var version rest.DocVersion
			if tc.replicationType == db.ActiveReplicatorTypePull {
				version = rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
				docHistoryList = append(docHistoryList, version.RevTreeID)
			} else {
				version = rt1.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
				docHistoryList = append(docHistoryList, version.RevTreeID)
			}

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   tc.replicationType,
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
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			var changesResults rest.ChangesResults
			if tc.replicationType == db.ActiveReplicatorTypePull {
				changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
				changesResults.RequireDocIDs(t, []string{docID})

				rt1Version, _ := rt1.GetDoc(docID)
				rest.RequireDocVersionEqual(t, version, rt1Version)
			} else {
				changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
				changesResults.RequireDocIDs(t, []string{docID})

				rt2Version, _ := rt2.GetDoc(docID)
				rest.RequireDocVersionEqual(t, version, rt2Version)
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			if tc.replicationType == db.ActiveReplicatorTypePull {
				for i := 0; i < 10; i++ {
					version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"], "version": "%d"}`))
					docHistoryList = append(docHistoryList, version.RevTreeID)
				}
			} else {
				for i := 0; i < 10; i++ {
					version = rt1.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"], "version": "%d"}`))
					docHistoryList = append(docHistoryList, version.RevTreeID)
				}
			}

			// start again for new revisions
			require.NoError(t, ar.Start(ctx1))

			// wait for the document originally written to rt1 to arrive at rt2
			if tc.replicationType == db.ActiveReplicatorTypePull {
				changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
				changesResults.RequireDocIDs(t, []string{docID})

				rt1Version, _ := rt1.GetDoc(docID)
				rest.RequireDocVersionEqual(t, version, rt1Version)
			} else {
				changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
				changesResults.RequireDocIDs(t, []string{docID})

				rt2Version, _ := rt2.GetDoc(docID)
				rest.RequireDocVersionEqual(t, version, rt2Version)
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			if tc.replicationType == db.ActiveReplicatorTypePull {
				rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
				rt1Doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				assert.Equal(t, version.RevTreeID, rt1Doc.CurrentRev)
				assert.Len(t, rt1Doc.History.GetLeaves(), 1)
				assert.Len(t, rt1Doc.History, 11) // 1 base + 10 updates
				rest.RequireDocVersionEqual(t, version, rt1Doc.ExtractDocVersion())
				base.RequireKeysEqual(t, docHistoryList, rt1Doc.History)
			} else {
				rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
				rt2Doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				assert.Equal(t, version.RevTreeID, rt2Doc.CurrentRev)
				assert.Len(t, rt2Doc.History.GetLeaves(), 1)
				assert.Len(t, rt2Doc.History, 11) // 1 base + 10 updates
				rest.RequireDocVersionEqual(t, version, rt2Doc.ExtractDocVersion())
				base.RequireKeysEqual(t, docHistoryList, rt2Doc.History)
			}
		})
	}
}

func TestActiveReplicatorRevtreeLargeDiffInSize(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

	testCases := []struct {
		name            string
		replicationType db.ActiveReplicatorDirection
	}{
		{
			name:            "pull",
			replicationType: db.ActiveReplicatorTypePull,
		},
		{
			name:            "push",
			replicationType: db.ActiveReplicatorTypePush,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			docID := "doc1"
			var version rest.DocVersion
			if tc.replicationType == db.ActiveReplicatorTypePull {
				version = rt2.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"]}`))
			} else {
				version = rt1.PutDocDirectly(docID, rest.JsonToMap(t, `{"source":"rt2","channels":["alice"]}`))
			}

			ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   tc.replicationType,
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

			require.NoError(t, ar.Start(ctx1))

			// wait for the document to arrive
			var changesResults rest.ChangesResults
			if tc.replicationType == db.ActiveReplicatorTypePull {
				changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
				changesResults.RequireDocIDs(t, []string{docID})

				rt1Doc, _ := rt1.GetDoc(docID)
				rest.RequireDocVersionEqual(t, version, rt1Doc)
			} else {
				changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
				changesResults.RequireDocIDs(t, []string{docID})

				docRt2, _ := rt2.GetDoc(docID)
				rest.RequireDocVersionEqual(t, version, docRt2)
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
			rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()

			// update doc hundreds of times to create a large diff in rev tree versions
			if tc.replicationType == db.ActiveReplicatorTypePull {
				for i := 0; i < 200; i++ {
					version = rt2.UpdateDocDirectly(docID, version, rest.JsonToMap(t, fmt.Sprintf(`{"source":"rt2","channels":["alice"], "version": "%d"}`, i)))
				}
			} else {
				for i := 0; i < 200; i++ {
					version = rt1.UpdateDocDirectly(docID, version, rest.JsonToMap(t, `{"source":"rt1","channels":["alice"], "version": "%d"}`))
				}
			}

			// start replicator again for new revisions
			require.NoError(t, ar.Start(ctx1))

			if tc.replicationType == db.ActiveReplicatorTypePull {
				changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
				changesResults.RequireDocIDs(t, []string{docID})
			} else {
				changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
				changesResults.RequireDocIDs(t, []string{docID})
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
			}, time.Second*20, time.Millisecond*100)

			docRT1, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			docRT2, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			if tc.replicationType == db.ActiveReplicatorTypePull {
				rest.RequireDocVersionEqual(t, version, docRT1.ExtractDocVersion())
			} else {
				rest.RequireDocVersionEqual(t, version, docRT2.ExtractDocVersion())
			}
			assert.Equal(t, len(docRT1.History), len(docRT2.History))
			assert.Equal(t, docRT1.HLV.GetCurrentVersionString(), docRT2.HLV.GetCurrentVersionString())
			assert.ElementsMatch(t, slices.Collect(maps.Keys(docRT1.History)), slices.Collect(maps.Keys(docRT2.History)))
		})
	}
}
