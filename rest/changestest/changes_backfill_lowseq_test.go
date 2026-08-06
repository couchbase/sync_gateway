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

// TestChangesBackfillContinuationSkippedByCompoundLowSeq covers CBG-5429 finding #1: a
// limit-paginated backfill that continues on a follow-up request must not skip pending backfill
// docs because of a compound since value carrying LowSeq.
//
// The since value produced mid-backfill here has Seq < LowSeq, so intSeqToString drops LowSeq
// from the wire representation (see the String() doc comment in db/sequence_id.go) - the compound
// "LowSeq:TriggeredBy:Seq" form is never actually sent to the client for this data layout. This
// test verifies the resulting since value still resumes the backfill correctly and delivers the
// pending doc.
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
	assert.Equal(t, "5:10:3", compound.String())
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

// TestChangesBackfillGrantSuppressedByCompoundLowSeq covers CBG-5429 finding #2: a fresh backfill
// for a channel granted access while LowSeq mode is active must not be suppressed by the compound
// since.
//
// As with TestChangesBackfillContinuationSkippedByCompoundLowSeq, the since value produced here
// has Seq < LowSeq, so intSeqToString drops LowSeq from the wire representation and the compound
// "LowSeq:TriggeredBy:Seq" form is never sent to the client for this data layout. This test
// verifies the resulting since value still delivers the GHI backfill doc.
func TestChangesBackfillGrantSuppressedByCompoundLowSeq(t *testing.T) {
	defer db.SuspendSequenceBatching()()
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
	assert.Equal(t, "4:2", initChanges.Last_Seq.String())

	// Resolve the skip so the feed's lowSequence no longer matches the since's LowSeq=7
	// (otherwise changes.go:851 zeroes it and the flip is masked). The client legitimately
	// replays the "4:2" since it received while the skip was still active.
	db.WriteDirect(t, collection, []string{"ABC"}, 8)
	rt.WaitForSequenceNotSkipped(8)
	rt.WaitForPendingChanges()

	changes := rt.PostChanges("/{{.keyspace}}/_changes", `{"since":"4:2"}`, "sg-user")
	require.Truef(t, changesHaveDoc(changes, "doc-3"),
		"GHI backfill doc-3 was skipped for since \"4:2\"; got %v", changeDocIDs(changes))
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

// TestMultiChannelChangesWithTriggeredSequence tests that the changes feed correctly handles
// a compound since value containing both LowSeq and TriggeredBy (e.g. "6:10:3"), which is
// produced when a channel access grant (backfill, setting TriggeredBy) occurs while LowSeq
// mode is active (due to skipped sequences).
//
// This exercises the corner case fixed by CBG-5429: before the fix, SequenceID.String() would
// silently drop the LowSeq field when LowSeq > Seq, causing the client to receive an incorrect
// last_seq. On the next request the changes feed would use the wrong since value, potentially
// skipping backfill documents.
//
// Sequence layout written by WriteDirect (seq 1 = user creation via CreateUser):
//
//	Seq  Channel  Notes
//	1    (user)   CreateUser allocates this via the sequence allocator
//	2    ABC
//	3    DEF      not visible until DEF access is granted
//	4    DEF      not visible until DEF access is granted
//	5    ABC
//	6    ABC
//	7    DEF      written late (after 9) to create the skipped-sequence / LowSeq condition
//	8    (gap)    never written; skipped sequence
//	9    ABC
//	10+  (user update granting DEF access, after allocator is advanced)
func TestMultiChannelChangesWithTriggeredSequence(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	// base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCache, base.KeyHTTP)

	// MaxWaitPending: reduce from the 5 s default to 5 ms so the cache promotes pending
	// sequences to the skipped queue almost immediately, keeping test latency low.
	pendingMaxWait := uint32(5)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) {channel(doc.channels);}`,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				CacheConfig: &rest.CacheConfig{
					ChannelCacheConfig: &rest.ChannelCacheConfig{
						MaxWaitPending: &pendingMaxWait,
					},
				},
			},
		},
	})
	defer rt.Close()
	testDb := rt.GetDatabase()
	ctx := rt.Context()

	// ChannelQueryLimit forces the channel cache to paginate when fetching changes, exercising
	// the interaction between the per-channel query limit and the request-level limit parameter.
	testDb.Options.CacheOptions.ChannelQueryLimit = 5

	collection, _ := rt.GetSingleTestDatabaseCollection()

	// CreateUser allocates seq 1 from the sequence allocator (_sync:seq becomes 1).
	// The user starts with access to ABC only; DEF will be granted later.
	rt.CreateUser("sg-user", []string{"ABC"})

	// WriteDirect writes documents at explicit SG sequence numbers, bypassing the sequence
	// allocator entirely (_sync:seq is not updated). This lets us craft a specific sequence
	// layout including a deliberate gap to trigger LowSeq mode.
	//
	// Seq 9 is written before seq 7, creating a gap at seqs 7 and 8. With MaxWaitPending=5ms
	// the cache quickly moves seqs 7 and 8 into the skipped-sequence queue and promotes seq 9,
	// setting LowSeq = 6 (last contiguous sequence before the gap).
	db.WriteDirect(t, collection, []string{"ABC"}, 2)
	db.WriteDirect(t, collection, []string{"DEF"}, 3)
	db.WriteDirect(t, collection, []string{"DEF"}, 4)
	db.WriteDirect(t, collection, []string{"ABC"}, 5)
	db.WriteDirect(t, collection, []string{"ABC"}, 6)
	db.WriteDirect(t, collection, []string{"ABC"}, 9) // gap: seqs 7 and 8 are missing
	// Wait until seq 9 is in the cache and no longer in the skipped queue.
	rt.WaitForSequenceNotSkipped(9)

	// Verify initial changes feed state: user sees 5 entries (seq 1 user doc, seqs 2/5/6/9 ABC).
	// DEF docs at seqs 3 and 4 are not visible since user has no DEF access yet.
	// last_seq = "6::9": LowSeq=6 (seqs 7,8 are in the skipped queue), HighSeq=9.
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "sg-user")
	require.Len(t, changes.Results, 5)
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "6::9", changes.Last_Seq.String())

	// Confirm no spurious results when re-requesting with the compound since value.
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq.String())
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "sg-user")
	require.Len(t, changes.Results, 0)

	// Fill in seq 7 (DEF channel) — one of the two skipped sequences arrives late.
	// This removes seq 7 from the skipped queue; only seq 8 remains skipped.
	// LowSeq advances to 7. The late-sequence feeds for this changes iteration will
	// deliver seq 7 even though it arrived after the initial feed was established.
	db.WriteDirect(t, collection, []string{"DEF"}, 7)
	// WaitForSequenceNotSkipped(7) blocks until seq 7 has been removed from the skipped queue,
	// which happens inside processEntry (change_cache.go) after _addToCache completes. This is
	// the correct gate here: WaitForSequence(7) would return immediately because nextSequence is
	// already 10 (advanced when seq 9 was processed), so it would not wait for seq 7's DCP event
	// to actually be received and cached — causing a race on CI where the subsequent PostChanges
	// request returns 0 results instead of 1.
	rt.WaitForSequenceNotSkipped(7)

	// With seq 7 now in the cache, the changes feed should deliver it via the late-sequence
	// path. The user still has no DEF access, but the LowSeq advancement itself produces a
	// result (last_seq moves forward). The last_seq from this response is used as the since
	// value for the final request below.
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "sg-user")
	require.Len(t, changes.Results, 1)

	// WriteDirect bypasses the sequence allocator, so _sync:seq is still at 1 after all the
	// WriteDirect calls above. If we now do a normal SG write (the user update below) without
	// advancing the allocator, the write would be assigned seq 2 — colliding with the
	// WriteDirect doc already at seq 2, and making TriggeredBy=2 for the DEF grant. With
	// TriggeredBy=2 the backfill would look for DEF docs at seq ≤ 2, missing seqs 3, 4, 7.
	//
	// AllocateTestSequence increments _sync:seq directly (without updating the allocator's
	// local last/max state). After 8 calls _sync:seq = 9. The next normal SG write then
	// triggers a new batch reservation, landing at seq 10.
	for range 8 {
		_, err := db.AllocateTestSequence(ctx, testDb)
		require.NoError(t, err)
	}

	// Grant DEF access to the user. This is a normal SG write and allocates seq 10 from the
	// sequence allocator. Seq 10 becomes the seqAddedAt for the DEF channel grant, and will
	// appear as TriggeredBy in subsequent changes responses during the backfill.
	//
	// Combined with the LowSeq still active (seq 8 remains skipped), the next changes
	// response will contain a since value of the form {LowSeq: L, TriggeredBy: 10, Seq: S}
	// — the exact compound format that CBG-5429 fixes.
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/sg-user", rest.GetUserPayload(t, "", rest.RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), []string{"ABC", "DEF"}, nil))
	rest.RequireStatus(t, resp, http.StatusOK)

	// Use the last_seq from the post-seq7 response as the since value. This since has LowSeq
	// set, and after the DEF grant the changes feed will add TriggeredBy, producing a compound
	// since value {LowSeq: L, TriggeredBy: 10, Seq: S} — the CBG-5429 corner case.
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq.String())

	// Expect 4 results: the user update (seq 10) plus the 3 DEF backfill docs (seqs 3, 4, 7).
	// If Before() or String() are broken for the compound since value, backfill docs will be
	// missed and this assertion will fail.
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "sg-user")
	require.Len(t, changes.Results, 4)

	// The DEF backfill is now complete, but seq 8 is still skipped so last_seq is still
	// LowSeq::Seq (e.g. "7::10"), not a bare sequence. Replay that since as-is: since the
	// backfill already finished and LowSeq hasn't moved, this must not retrigger the backfill -
	// it should come back empty.
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq.String())
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "sg-user")
	require.Len(t, changes.Results, 0)

	// Fill in seq 8, the last remaining skipped sequence. Even though the DEF backfill already
	// completed above, the late-arriving sequence should still reach the client - and we expect
	// the entire DEF backfill to be resent, not just the new doc at seq 8.
	db.WriteDirect(t, collection, []string{"DEF"}, 8)
	rt.WaitForSequenceNotSkipped(8)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "sg-user")
	require.Len(t, changes.Results, 6)

	// All four DEF backfill docs (seqs 3, 4, 7, 8) should now have been delivered.
	for _, docID := range []string{"doc-3", "doc-4", "doc-7", "doc-8"} {
		require.Truef(t, changesHaveDoc(changes, docID), "expected DEF doc %q in changes results; got %v", docID, changeDocIDs(changes))
	}

	// The backfill and the skipped-sequence gap are both fully resolved at this point, so
	// last_seq collapses back to a simple sequence rather than a compound one.
	assert.Equal(t, "10", changes.Last_Seq.String())
}
