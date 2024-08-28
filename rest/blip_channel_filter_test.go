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
	const (
		username = "user1"
		body1    = `{"channels":["A"]}`
		body2    = `{"channels":["B"]}`
	)
	tests := []struct {
		name                      string
		channelGrant              []string
		sendDocWithChannelRemoval bool
		sendRevocations           bool
		// changes after adding a single doc
		expectedChanges1 func(revID1 string) string
		expectedChanges2 func(revID2 string) string
		expectedBlipBody *string
	}{
		{
			name:                      "sendDocWithChannelRemoval=true, sendRevocations=true,channelGrant=*",
			channelGrant:              []string{"*"},
			sendDocWithChannelRemoval: true,
			sendRevocations:           true,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["B"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: base.StringPtr(body2),
		},
		{
			name:                      "sendDocWithChannelRemoval=true, sendRevocations=false,channelGrant=*",
			channelGrant:              []string{"*"},
			sendDocWithChannelRemoval: true,
			sendRevocations:           false,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["B"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: base.StringPtr(body2),
		},
		{
			name:                      "sendDocWithChannelRemoval=false, sendRevocations=true,channelGrant=*",
			channelGrant:              []string{"*"},
			sendDocWithChannelRemoval: false,
			sendRevocations:           true,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["B"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: nil, // no body expected
		},
		{
			name:                      "sendDocWithChannelRemoval=false, sendRevocations=false,channelGrant=*",
			channelGrant:              []string{"*"},
			sendDocWithChannelRemoval: false,
			sendRevocations:           false,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["B"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: nil, // no body expected
		},
		// channelGrant=A
		{
			name:                      "sendDocWithChannelRemoval=true, sendRevocations=true,channelGrant=A",
			channelGrant:              []string{"A"},
			sendDocWithChannelRemoval: true,
			sendRevocations:           true,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_removed":true,"_rev":"%s"}, "removed": ["A"], "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: base.StringPtr(`{"_removed":true}`),
		},
		{
			name:                      "sendDocWithChannelRemoval=true, sendRevocations=false,channelGrant=A",
			channelGrant:              []string{"A"},
			sendDocWithChannelRemoval: true,
			sendRevocations:           false,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_removed":true,"_rev":"%s"}, "removed": ["A"], "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: base.StringPtr(`{"_removed":true}`), // TOR: this is the line that seems wrong
		},
		{
			name:                      "sendDocWithChannelRemoval=false, sendRevocations=true,channelGrant=A",
			channelGrant:              []string{"A"},
			sendDocWithChannelRemoval: false,
			sendRevocations:           true,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_removed":true,"_rev":"%s"}, "removed": ["A"], "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: base.StringPtr(`{"_removed":true}`),
		},
		{
			name:                      "sendDocWithChannelRemoval=false, sendRevocations=false,channelGrant=A",
			channelGrant:              []string{"A"},
			sendDocWithChannelRemoval: false,
			sendRevocations:           false,
			expectedChanges1: func(revID1 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":1, "id": "_user/user1", "changes":[]},
		{"seq":2, "id": "doc1", "doc": {"_id": "doc1", "_rev":"%s", "channels": ["A"]}, "changes": [{"rev":"%s"}]}
	],
	"last_seq": "2"
}`, revID1, revID1)
			},
			expectedChanges2: func(revID2 string) string {
				return fmt.Sprintf(`
{
	"results": [
		{"seq":3, "id": "doc1", "doc": {"_id": "doc1", "_removed":true,"_rev":"%s"}, "removed": ["A"], "changes": [{"rev":"%s"}]}
	],
	"last_seq": "3"
}`, revID2, revID2)
			},
			expectedBlipBody: nil, // no body, revocations
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				SyncFn:           channels.DocChannelsSyncFunction,
				PersistentConfig: true,
			})
			defer rt.Close()

			dbConfig := rt.NewDbConfig()
			dbConfig.Unsupported = &db.UnsupportedOptions{
				SendChannelFilterRemovals: test.sendDocWithChannelRemoval,
			}
			rt.CreateDatabase("db", dbConfig)
			const username = "user1"
			rt.CreateUser(username, test.channelGrant)

			btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
				Username:        username,
				Password:        base.StringPtr(RestTesterDefaultUserPassword),
				Channels:        []string{"A"},
				SendRevocations: test.sendRevocations,
			})
			require.NoError(t, err)
			defer btc.Close()

			const docID = "doc1"
			revID1 := rt.PutDoc("doc1", `{"channels":["A"]}`).Rev
			require.NoError(t, rt.WaitForPendingChanges())

			response := rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=0&channels=A&include_docs=true", "", username)
			RequireStatus(t, response, http.StatusOK)

			log.Printf("_changes?since=0&channels=A&include_docs=true: %s", response.BodyBytes())
			require.JSONEq(t, test.expectedChanges1(revID1), string(response.BodyBytes()))

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
			response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes?since=2&channels=A&include_docs=true", "", username)
			RequireStatus(t, response, http.StatusOK)
			log.Printf("_changes?since=2&channels=A&include_docs=true: %s", response.BodyBytes())
			log.Printf("expected: %s", test.expectedChanges2(revID2))

			require.JSONEq(t, test.expectedChanges2(revID2), string(response.BodyBytes()))

			err = btc.StartFilteredPullSince(continuous, since, activeOnly, channels)
			require.NoError(t, err)

			if test.expectedBlipBody == nil {
				btc.RequireRevNotExpected(docID, revID2)
			} else {
				data, ok := btc.WaitForRev(docID, revID2)
				require.True(t, ok)
				require.Equal(t, *test.expectedBlipBody, string(data))
			}
		})
	}
}
