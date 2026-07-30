// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package changestest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestChangesBackfillContinuationSkippedByCompoundLowSeq(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	pendingMaxWait := uint32(5) // promote pending sequences to the skipped queue quickly
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) {channel(doc.channels);}`,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				CacheConfig: &rest.CacheConfig{
					ChannelCacheConfig: &rest.ChannelCacheConfig{MaxWaitPending: &pendingMaxWait},
				},
			},
		},
	})
	defer rt.Close()
	testDb := rt.GetDatabase()
	ctx := rt.Context()
	collection, _ := rt.GetSingleTestDatabaseCollection()

	rt.CreateUser("sg-user", []string{"ABC"}) // seq 1

	// DEF backfill docs at low seqs 2,3,4. ABC at 5. Gap at 6 (skipped). ABC at 7.
	// The gap makes LowSeq = oldestSkipped-1 = 5, which is > the DEF backfill seqs.
	db.WriteDirect(t, collection, []string{"DEF"}, 2)
	db.WriteDirect(t, collection, []string{"DEF"}, 3)
	db.WriteDirect(t, collection, []string{"DEF"}, 4)
	db.WriteDirect(t, collection, []string{"ABC"}, 5)
	db.WriteDirect(t, collection, []string{"ABC"}, 7) // gap at seq 6
	rt.WaitForSequenceNotSkipped(7)
	rt.WaitForPendingChanges()

	initial := rt.GetChanges("/{{.keyspace}}/_changes", "sg-user")
	require.Equal(t, "5::7", initial.Last_Seq.String())

	// WriteDirect bypasses the sequence allocator, so the next normal SG write could reuse one of the
	// explicitly-assigned WriteDirect sequences. Advance the allocator's _sync:seq counter past the
	// WriteDirect region so the subsequent user update (a normal SG write) lands at a fresh sequence.
	// With sequence batching suspended, each increment reserves a single sequence, so 8 increments
	// moves the counter to 9 and the grant allocates seq 10.
	for range 8 {
		_, err := db.AllocateTestSequence(ctx, testDb)
		require.NoError(t, err)
	}
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/sg-user",
		rest.GetUserPayload(t, "", rest.RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), []string{"ABC", "DEF"}, nil))
	rest.RequireStatus(t, resp, http.StatusOK)
	rt.WaitForPendingChanges()

	// REQ1: limit=2 splits the DEF backfill mid-stream (sends doc-2, doc-3).
	req1 := rt.PostChanges("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":"%s","limit":2}`, initial.Last_Seq.String()), "sg-user")
	compound := req1.Last_Seq
	assert.Equal(t, "10:3", compound.String())
	require.Truef(t, compound.TriggeredBy != 0, "REQ1 last_seq should be mid-backfill (TriggeredBy set); got %q", compound.String())

	// Resolve the skip so the feed's lowSequence changes; the client replays the since it just
	// received.
	db.WriteDirect(t, collection, []string{"ABC"}, 6)
	rt.WaitForSequenceNotSkipped(6)
	rt.WaitForPendingChanges()

	// REQ2: continue the backfill with the since value from REQ1. The pending DEF backfill doc
	// (doc-4) must still be delivered.
	req2 := rt.PostChanges("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":"%s","limit":20}`, compound.String()), "sg-user")

	delivered := changeDocIDSet(req1, req2)
	require.Truef(t, delivered["doc-4"],
		"DEF backfill doc-4 was skipped across the paginated since requests (since=%q; REQ1=%v REQ2=%v)",
		compound.String(), changeDocIDs(req1), changeDocIDs(req2))

	// doc-6 and doc-7 are missing from the feed, doc6 is the skipped sequence
	// whereas the doc-7 is the new stable sequence.
	req2Docs := changeDocIDSet(req2)
	require.Truef(t, req2Docs["doc-6"],
		"doc-6 was missing from REQ2 (since=%q; REQ2=%v)", compound.String(), changeDocIDs(req2))
	require.Truef(t, req2Docs["doc-7"],
		"doc-7 was missing from REQ2 (since=%q; REQ2=%v)", compound.String(), changeDocIDs(req2))
}

func changeDocIDs(cr rest.ChangesResults) []string {
	out := make([]string, 0, len(cr.Results))
	for _, r := range cr.Results {
		out = append(out, r.ID)
	}
	return out
}

func changeDocIDSet(crs ...rest.ChangesResults) map[string]bool {
	set := map[string]bool{}
	for _, cr := range crs {
		for _, r := range cr.Results {
			set[r.ID] = true
		}
	}
	return set
}
