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
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func TestChannelFilterRemovalFromChannel(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T, _ []string) {
		for _, sendDocWithChannelRemoval := range []bool{true, false} {
			t.Run(fmt.Sprintf("sendDocWithChannelRemoval=%v", sendDocWithChannelRemoval), func(t *testing.T) {
				rt := NewRestTester(t, &RestTesterConfig{
					SyncFn:           channels.DocChannelsSyncFunction,
					PersistentConfig: true,
				})
				defer rt.Close()

				dbConfig := rt.NewDbConfig()
				dbConfig.Unsupported = &db.UnsupportedOptions{
					BlipSendDocsWithChannelRemoval: sendDocWithChannelRemoval,
				}
				rt.CreateDatabase("db", dbConfig)
				rt.CreateUser("alice", []string{"*"})
				rt.CreateUser("bob", []string{"A"})

				btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
					Username:        "alice",
					Channels:        []string{"A"},
					SendRevocations: false,
				})
				defer btc.Close()

				client := btcRunner.SingleCollection(btc.id)
				const docID = "doc1"
				version1 := rt.PutDoc("doc1", `{"channels":["A"]}`)
				require.NoError(t, rt.WaitForPendingChanges())

				response := rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=0&channels=A&include_docs=true", "", "alice")
				RequireStatus(t, response, http.StatusOK)

				expectedChanges1 := fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/alice", "changes":[]},
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, version1.RevID, version1.RevID)
				require.JSONEq(t, expectedChanges1, string(response.BodyBytes()))

				require.NoError(t, client.StartPullSince(BlipTesterPullOptions{Continuous: false, Since: "0", Channels: "A"}))

				btcRunner.WaitForVersion(btc.id, docID, version1)

				// remove channel A from doc1
				version2 := rt.UpdateDoc(docID, version1, `{"channels":["B"]}`)
				markerDocID := "marker"
				markerDocVersion := rt.PutDoc(markerDocID, `{"channels":["A"]}`)
				require.NoError(t, rt.WaitForPendingChanges())

				// alice will see doc1 rev2 with body
				response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=2&channels=A&include_docs=true", "", "alice")
				RequireStatus(t, response, http.StatusOK)

				aliceExpectedChanges2 := fmt.Sprintf(`
{
	"results": [
		{"seq":4, "id": "%s", "doc": {"_id": "%s", "_rev":"%s", "channels": ["B"]}, "changes": [{"rev":"%s"}]},
		{"seq":5, "id": "%s", "doc": {"_id": "%s", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "5"
}`, docID, docID, version2.RevID, version2.RevID, markerDocID, markerDocID, markerDocVersion.RevID, markerDocVersion.RevID)
				require.JSONEq(t, aliceExpectedChanges2, string(response.BodyBytes()))

				require.NoError(t, client.StartPullSince(BlipTesterPullOptions{Continuous: false, Since: "0", Channels: "A"}))

				if sendDocWithChannelRemoval {
					data := btcRunner.WaitForVersion(btc.id, docID, version2)
					require.Equal(t, `{"channels":["B"]}`, string(data))
				} else {
					client.WaitForVersion(markerDocID, markerDocVersion)
					doc, ok := client.GetDoc(docID)
					require.True(t, ok)
					require.Equal(t, `{"channels":["A"]}`, string(doc))
				}

				// bob will not see doc1
				response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=2&channels=A&include_docs=true", "", "bob")
				RequireStatus(t, response, http.StatusOK)

				bobExpectedChanges2 := fmt.Sprintf(`
{
	"results": [
		{"seq":4, "id": "doc1", "removed":["A"], "doc": {"_id": "doc1", "_rev":"%s", "_removed": true}, "changes": [{"rev":"%s"}]},
		{"seq":5, "id": "%s", "doc": {"_id": "%s", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "5"
}`, version2.RevID, version2.RevID, markerDocID, markerDocID, markerDocVersion.RevID, markerDocVersion.RevID)
				require.JSONEq(t, bobExpectedChanges2, string(response.BodyBytes()))
			})
		}
	})
}
