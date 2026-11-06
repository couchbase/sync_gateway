// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestWriterGroups(t *testing.T) {
	testCases := []struct {
		name                            string
		numVBuckets, writers, syncSeqVb int
		want                            [][]uint16
	}{
		{name: "one vBucket, no sync-seq vBucket", numVBuckets: 1, writers: 4, syncSeqVb: -1,
			want: [][]uint16{{0}}},
		{name: "more writers than data vBuckets", numVBuckets: 3, writers: 8, syncSeqVb: 2,
			want: [][]uint16{{0}, {1}}},
		{name: "several vBuckets per writer", numVBuckets: 8, writers: 3, syncSeqVb: 7,
			want: [][]uint16{{0, 3, 6}, {1, 4}, {2, 5}}},
		{name: "sync-seq vBucket mid-range", numVBuckets: 6, writers: 2, syncSeqVb: 2,
			want: [][]uint16{{0, 3, 5}, {1, 4}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			groups := writerGroups(tc.numVBuckets, tc.writers, tc.syncSeqVb)
			require.Equal(t, tc.want, groups)

			owners := make(map[uint16]int)
			for _, g := range groups {
				for _, vb := range g {
					owners[vb]++
				}
			}
			for vb := range tc.numVBuckets {
				if vb == tc.syncSeqVb {
					require.Zero(t, owners[uint16(vb)], "sync-seq vBucket %d must not be in a writer group", vb)
				} else {
					require.Equal(t, 1, owners[uint16(vb)], "data vBucket %d must have exactly one writer", vb)
				}
			}
		})
	}
}

// streamRecorder is a mutationSink that checks each vBucket's DCP sequence numbers run 1, 2, 3, ...
// as they would on a KV stream. Called concurrently by the writers.
type streamRecorder struct {
	lock     sync.Mutex
	lastSeq  map[uint16]uint64
	keys     map[string]int // per key, so a duplicate document write shows up
	syncSeqs int            // _sync:seq mutations seen
	errs     []string
}

func newStreamRecorder() *streamRecorder {
	return &streamRecorder{lastSeq: make(map[uint16]uint64), keys: make(map[string]int)}
}

func (r *streamRecorder) Mutation(m gocbcore.DcpMutation) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if want := r.lastSeq[m.VbID] + 1; m.SeqNo != want && len(r.errs) < 10 {
		r.errs = append(r.errs, fmt.Sprintf("vBucket %d: got seqno %d, want %d", m.VbID, m.SeqNo, want))
	}
	r.lastSeq[m.VbID] = m.SeqNo
	if string(m.Key) == "_sync:seq" {
		r.syncSeqs++
		return
	}
	r.keys[string(m.Key)]++
}

func (r *streamRecorder) check(t *testing.T) {
	r.lock.Lock()
	defer r.lock.Unlock()
	require.Empty(t, r.errs)
	for key, n := range r.keys {
		require.Equal(t, 1, n, "document %s written more than once", key)
	}
}

// waitForWritersToExit waits for every generator goroutine to return after ctx is cancelled.
func waitForWritersToExit(t *testing.T) {
	require.Eventually(t, func() bool { return numGoroutines.Load() == 0 }, 10*time.Second, 10*time.Millisecond,
		"%d generator goroutines still running", numGoroutines.Load())
}

// TestGroupedWritersKeepVBucketStreamsInOrder runs the -writerGoroutines writers concurrently, and is
// most useful under -race: each vBucket must still see one ordered stream, every data vBucket must be
// written, and the writers must exit on cancellation.
func TestGroupedWritersKeepVBucketStreamsInOrder(t *testing.T) {
	for _, rapidUpdate := range []bool{false, true} {
		t.Run(fmt.Sprintf("rapidUpdate=%t", rapidUpdate), func(t *testing.T) {
			const numVBuckets = 8
			recorder := newStreamRecorder()
			dcp := &dcpDataGen{
				seqAlloc:          newSequenceAllocator(10, newSyncSeq()),
				client:            recorder,
				numChannelsPerDoc: 2,
				numTotalChannels:  5,
				simRapidUpdate:    rapidUpdate,
				numVBuckets:       numVBuckets,
				writerGoroutines:  3,
			}
			var err error
			dcp.templates, err = newSyncTemplates(dcp)
			require.NoError(t, err)

			ctx, cancel := context.WithCancelCause(t.Context())
			dcp.vBucketCreation(ctx) // returns once every writer has started
			time.Sleep(200 * time.Millisecond)
			cancel(errors.New("test finished"))
			waitForWritersToExit(t)

			recorder.check(t)
			syncSeqVb := uint16(numVBuckets - 1) // syncSeqVBucket clamped into range
			for vb := range uint16(numVBuckets) {
				if vb != syncSeqVb {
					require.Positive(t, recorder.lastSeq[vb], "data vBucket %d was never written", vb)
				}
			}
			require.Positive(t, recorder.syncSeqs, "no _sync:seq updates on vBucket %d", syncSeqVb)
		})
	}
}

// TestSyncSeqVBucketKeepsStreamInOrder runs the sync-seq vBucket writer with a short delay, so its
// document writes and _sync:seq updates interleave on the one stream, while another goroutine
// allocates sequences to raise the _sync:seq events.
func TestSyncSeqVBucketKeepsStreamInOrder(t *testing.T) {
	recorder := newStreamRecorder()
	dcp := &dcpDataGen{
		seqAlloc:          newSequenceAllocator(1, newSyncSeq()),
		client:            recorder,
		numChannelsPerDoc: 1,
		numTotalChannels:  1,
		numVBuckets:       1,
	}
	var err error
	dcp.templates, err = newSyncTemplates(dcp)
	require.NoError(t, err)

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	go dcp.syncSeqVBucketCreation(ctx, 0, time.Millisecond)
	// A batch size of 1 raises a _sync:seq event on every allocation.
	for start := time.Now(); time.Since(start) < 200*time.Millisecond; {
		_ = dcp.seqAlloc.nextSeq()
	}
	cancel(errors.New("test finished"))
	waitForWritersToExit(t)

	recorder.check(t)
	require.Positive(t, recorder.syncSeqs, "no _sync:seq updates were sent")
	require.NotEmpty(t, recorder.keys, "no documents were written")
}
