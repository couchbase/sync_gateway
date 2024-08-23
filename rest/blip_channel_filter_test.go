// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func TestChannelFilterRemovalFromChannel(t *testing.T) {
	for _, sendDocWithChannelRemoval := range []bool{true, false} {
		t.Run(fmt.Sprintf("sendDocWithChannelRemoval=%v", sendDocWithChannelRemoval), func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				SyncFn:           channels.DocChannelsSyncFunction,
				PersistentConfig: true,
			})
			defer rt.Close()

			dbConfig := rt.NewDbConfig()
			dbConfig.Unsupported = &db.UnsupportedOptions{
				SendChannelFilterRemovals: sendDocWithChannelRemoval,
			}
			rt.CreateDatabase("db", dbConfig)
			rt.CreateUser("alice", []string{"*"})
			rt.CreateUser("bob", []string{"A"})

			btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
				Username:        "alice",
				Password:        base.StringPtr(RestTesterDefaultUserPassword),
				Channels:        []string{"A"},
				SendRevocations: false,
			})
			require.NoError(t, err)
			defer btc.Close()

			const docID = "doc1"
			revID1 := rt.PutDoc("doc1", `{"channels":["A"]}`).Rev
			require.NoError(t, rt.WaitForPendingChanges())

			response := rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=0&channels=A&include_docs=true", "", "alice")
			RequireStatus(t, response, http.StatusOK)

			log.Printf("response: %s", response.BodyBytes())
			expectedChanges1 := fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/alice", "changes":[]},
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID1, revID1)
			require.JSONEq(t, expectedChanges1, string(response.BodyBytes()))

			continuous := "false"
			since := "0"
			activeOnly := "false"
			channels := "A"
			err = btc.StartFilteredPullSince(continuous, since, activeOnly, channels)
			require.NoError(t, err)

			_, ok := btc.WaitForRev(docID, revID1)
			require.True(t, ok)

			// remove channel A from doc1
			revID2 := rt.UpdateDoc(docID, revID1, `{"channels":["B"]}`).Rev
			require.NoError(t, rt.WaitForPendingChanges())

			// alice will see doc1 rev2 with body
			response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=2&channels=A&include_docs=true", "", "alice")
			RequireStatus(t, response, http.StatusOK)

			aliceExpectedChanges2 := fmt.Sprintf(`
{
	"results": [
		{"seq":4, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["B"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "4"
}`, revID2, revID2)
			require.JSONEq(t, aliceExpectedChanges2, string(response.BodyBytes()))

			err = btc.StartFilteredPullSince(continuous, since, activeOnly, channels)
			require.NoError(t, err)

			if sendDocWithChannelRemoval {
				data, ok := btc.WaitForRev(docID, revID2)
				require.True(t, ok)
				require.Equal(t, `{"channels":["B"]}`, string(data))
			} else {
				btc.RequireRevNotExpected(docID, revID2)
			}

			// bob will not see doc1
			response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=2&channels=A&include_docs=true", "", "bob")
			RequireStatus(t, response, http.StatusOK)

			log.Printf("response: %s", response.BodyBytes())
			bobExpectedChanges2 := fmt.Sprintf(`
{
	"results": [
		{"seq":4, "id": "doc1", "removed":["A"], "doc": {"_id": "doc1", "_rev":"%s", "_removed": true}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "4"
}`, revID2, revID2)
			require.JSONEq(t, bobExpectedChanges2, string(response.BodyBytes()))
		})
	}
}
