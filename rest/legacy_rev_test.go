// Copyright 2025-Present Couchbase, Inc.
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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

// TestResyncLegacyRev makes sure that running resync on a legacy rev will send future blip messages as a legacy rev and not as an HLV
func TestResyncLegacyRev(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	const (
		alice       = "alice"
		channelName = "A"
	)
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	// default collection will use channel Name and named collection will use collection.Name
	rt.CreateUser(alice, []string{channelName, collection.Name})

	docID := db.SafeDocumentName(t, t.Name())
	doc := rt.CreateDocNoHLV(docID, db.Body{"channels": channelName})

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: alice})
		defer btc.Close()

		btcRunner.StartOneshotPull(btc.id)

		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, DocVersion{RevTreeID: doc.GetRevTreeID()})
		require.Equal(t, msg.Properties[db.RevMessageRev], doc.GetRevTreeID())
	})
	previousChannel := collection.Name
	if base.IsDefaultCollection(collection.ScopeName, collection.Name) {
		previousChannel = "A"
	}

	_, err := collection.UpdateSyncFun(ctx, fmt.Sprintf(`function() {channel("B", "%s")}`, previousChannel))
	require.NoError(t, err)

	// use ResyncDocument and TakeDbOffline/Online instead of /ks/_config/sync && /db/_resync to work under rosmar which
	// doesn't yet support DCP resync or updating config on an existing bucket.
	regenerateSequences := false
	var unusedSequences []uint64
	_, _, err = collection.ResyncDocument(ctx, docID, docID, regenerateSequences, unusedSequences)
	require.NoError(t, err)

	rt.TakeDbOffline()
	rt.TakeDbOnline()

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, "")
	RequireStatus(t, resp, http.StatusOK)

	btcRunner = NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: alice})
		defer btc.Close()

		btcRunner.StartOneshotPull(btc.id)

		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, DocVersion{RevTreeID: doc.GetRevTreeID()})
		// make sure second rev message after resync is still legacy rev format
		require.Equal(t, msg.Properties[db.RevMessageRev], doc.GetRevTreeID())
	})
}
