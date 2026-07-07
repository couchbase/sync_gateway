//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

// These two tests are end-to-end (_changes-level) proofs for CBG-5429 review findings #1
// and #2. Both currently FAIL: they assert the correct (no-data-loss) behaviour and
// demonstrate the regression introduced by the new compound "LowSeq:TriggeredBy:Seq"
// serialization in db/sequence_id.go:57-59.
//
// Root cause (shared by both):
//   - The new serialization keeps LowSeq in the string whenever TriggeredBy > 0, even when
//     LowSeq >= Seq (the normal state of a backfill entry, which re-sends OLD low-sequence
//     docs while a newer sequence is skipped). Old code dropped LowSeq in that case.
//   - That LowSeq round-trips back into the client's `since`. If the feed's lowSequence has
//     changed since the string was emitted (the skip resolved), changes.go:851 does NOT zero
//     it, so options.Since.SafeSequence() == LowSeq.
//   - changesFeed starts the per-channel query at SafeSequence() (changes.go:463), so the
//     query start jumps FORWARD to LowSeq, skipping backfill docs with seq in (Seq, LowSeq].
//
// Each test includes a CONTROL request using the exact string the OLD code would have
// emitted for the same feed state; the control delivers the backfill doc, isolating the
// serialization as the cause.

// TestChangesBackfillContinuationSkippedByCompoundLowSeq proves finding #1: a limit-paginated
// backfill that CONTINUES on a follow-up request skips backfill docs because the compound
// since starts the channel query at SafeSequence()=LowSeq.
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

	// WriteDirect bypasses the sequence allocator, so _sync:seq is still 1. Advance it past the
	// WriteDirect region so the DEF grant (a normal SG write, below) doesn't reuse one of those
	// sequences. The grant then reserves a fresh batch and lands after seq 7 (at maxBatchSize=10
	// in Rosmar). The exact grant sequence is allocator-batch dependent — and the count of
	// AllocateTestSequence calls does not change it — so we read the resulting since back rather
	// than hard-coding it. (The unused sequences between seq 7 and the grant are a benign artifact.)
	for range 8 {
		_, err := db.AllocateTestSequence(ctx, testDb)
		require.NoError(t, err)
	}
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/sg-user",
		rest.GetUserPayload(t, "", rest.RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), []string{"ABC", "DEF"}, nil))
	rest.RequireStatus(t, resp, http.StatusOK)
	rt.WaitForPendingChanges()

	// REQ1: limit=2 splits the DEF backfill mid-stream (sends doc-2, doc-3). Its last_seq is a
	// compound {LowSeq, TriggeredBy, Seq} with LowSeq(5) >= Seq — the corner CBG-5429 changed.
	req1 := rt.PostChanges("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":"%s","limit":2}`, initial.Last_Seq.String()), "sg-user")
	compound := req1.Last_Seq
	assert.Equal(t, "5:10:3", compound.String())
	require.Truef(t, compound.LowSeq != 0, "REQ1 last_seq should carry LowSeq while the skip is active; got %q", compound.String())
	require.Truef(t, compound.TriggeredBy != 0, "REQ1 last_seq should be mid-backfill (TriggeredBy set); got %q", compound.String())
	require.Truef(t, compound.LowSeq >= compound.Seq, "the affected corner is LowSeq >= Seq; got %q", compound.String())

	// The string OLD code would have emitted for the same value: LowSeq dropped => "TriggeredBy:Seq".
	oldSince := fmt.Sprintf("%d:%d", compound.TriggeredBy, compound.Seq)

	// Resolve the skip so the feed's lowSequence changes; the client replays the compound since
	// it just received. changes.go:851 will now NOT zero the LowSeq (it no longer matches lowSequence).
	db.WriteDirect(t, collection, []string{"ABC"}, 6)
	rt.WaitForSequenceNotSkipped(6)
	rt.WaitForPendingChanges()

	// REQ2: continue the backfill with the compound since. CONTROL: identical feed state, but with
	// the old-serialization since (LowSeq dropped) — which still delivers the pending backfill doc.
	req2 := rt.PostChanges("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":"%s","limit":20}`, compound.String()), "sg-user")
	fmt.Println("req", req2.Results)
	control := rt.PostChanges("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":"%s","limit":20}`, oldSince), "sg-user")
	fmt.Println("old", control.Results)

	delivered := changeDocIDSet(req1, req2)
	require.Truef(t, changesHaveDoc(control, "doc-4"),
		"control (old since %q) should deliver the pending DEF backfill doc-4; got %v", oldSince, changeDocIDs(control))
	require.Truef(t, delivered["doc-4"],
		"FINDING #1: DEF backfill doc-4 was skipped across the paginated compound-since requests (compound since=%q; REQ1=%v REQ2=%v)",
		compound.String(), changeDocIDs(req1), changeDocIDs(req2))
}

// TestChangesBackfillGrantSuppressedByCompoundLowSeq proves finding #2: a second channel's
// FRESH backfill is suppressed because the compound since flips backfillRequired
// (changes.go:929) to false, so that channel is queried from SafeSequence()=LowSeq.
func TestChangesBackfillGrantSuppressedByCompoundLowSeq(t *testing.T) {
	pendingMaxWait := uint32(5)
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

	// DEF backfill doc @2, GHI backfill doc @3 (both low).
	db.WriteDirect(t, collection, []string{"DEF"}, 2)
	db.WriteDirect(t, collection, []string{"GHI"}, 3)

	// Advance allocator to 3 so the two grants land at seq 4 (DEF) and seq 5 (GHI).
	for range 2 {
		_, err := db.AllocateTestSequence(ctx, testDb)
		require.NoError(t, err)
	}
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/sg-user",
		rest.GetUserPayload(t, "", rest.RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), []string{"ABC", "DEF"}, nil))
	rest.RequireStatus(t, resp, http.StatusOK) // DEF grant -> seqAddedAt 4
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/sg-user",
		rest.GetUserPayload(t, "", rest.RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), []string{"ABC", "DEF", "GHI"}, nil))
	rest.RequireStatus(t, resp, http.StatusOK) // GHI grant -> seqAddedAt 5

	// ABC @6,7,9 with a gap at 8 => oldestSkipped=8, LowSeq=7.
	// This gives LowSeq(7) >= TriggeredBy(4) and TriggeredBy(4) < GHI grant(5) <= LowSeq(7).
	db.WriteDirect(t, collection, []string{"ABC"}, 6)
	db.WriteDirect(t, collection, []string{"ABC"}, 7)
	db.WriteDirect(t, collection, []string{"ABC"}, 9)
	rt.WaitForSequenceNotSkipped(9)

	initChanges := rt.GetChanges("/{{.keyspace}}/_changes?limit=1", "sg-user")
	assert.Equal(t, "7:4:2", initChanges.Last_Seq.String())

	// Resolve the skip so the feed's lowSequence no longer matches the since's LowSeq=7
	// (otherwise changes.go:851 zeroes it and the flip is masked). The client legitimately
	// replays a "7:4:2" since it received while the skip was still active.
	db.WriteDirect(t, collection, []string{"ABC"}, 8)
	rt.WaitForSequenceNotSkipped(8)
	rt.WaitForPendingChanges()

	// "7:4:2" = {LowSeq:7, TriggeredBy:4 (mid DEF backfill), Seq:2} — the compound since the new
	// code emits. "4:2" = what the old code emitted for the same state (LowSeq dropped).
	bug := rt.PostChanges("/{{.keyspace}}/_changes", `{"since":"7:4:2"}`, "sg-user")
	control := rt.PostChanges("/{{.keyspace}}/_changes", `{"since":"4:2"}`, "sg-user")

	fmt.Println("bug", bug.Results)
	fmt.Println("control", control.Results)

	require.Truef(t, changesHaveDoc(control, "doc-3"),
		"control (old since \"4:2\") should deliver the GHI backfill doc-3; got %v", changeDocIDs(control))
	require.Truef(t, changesHaveDoc(bug, "doc-3"),
		"FINDING #2: GHI backfill doc-3 was skipped with the compound since \"7:4:2\" (got %v) — its fresh backfill was suppressed by the backfillRequired flip",
		changeDocIDs(bug))
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

func changesHaveDoc(cr rest.ChangesResults, id string) bool {
	for _, r := range cr.Results {
		if r.ID == id {
			return true
		}
	}
	return false
}
