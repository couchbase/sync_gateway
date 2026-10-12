/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestDCPDestCloseWaitsForActiveCallbacks(t *testing.T) {
	ctx := TestCtx(t)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var callbackCount atomic.Int64
	sgDest, err := NewDCPDest(ctx, DCPDestOptions{
		Callback: func(sgbucket.FeedEvent) bool {
			if callbackCount.Add(1) == 1 {
				close(callbackStarted)
				<-releaseCallback
			}
			return true
		},
		MaxVbNo: 1,
	})
	require.NoError(t, err)
	dest, ok := sgDest.(*DCPDest)
	if !ok {
		loggingDest, ok := sgDest.(*DCPLoggingDest)
		require.True(t, ok, "unexpected SGDest type %T", sgDest)
		dest = loggingDest.dest
	}

	const partition = "0"
	updateDone := make(chan error)
	go func() {
		updateDone <- dest.DataUpdate(partition, []byte("doc1"), 1, []byte(`{}`), 1, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
	}()
	RequireChanClosed(t, callbackStarted)

	closeDone := make(chan error)
	go func() {
		closeDone <- dest.Close(false)
	}()
	RequireChanClosed(t, dest.ctx.Done())

	// Callbacks that start after Close are skipped, while Close still waits for the in-flight one.
	require.NoError(t, dest.DataUpdate(partition, []byte("doc2"), 2, []byte(`{}`), 2, cbgt.DEST_EXTRAS_TYPE_NIL, nil))
	select {
	case <-closeDone:
		require.FailNow(t, "Close returned while a callback was still in progress")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseCallback)
	require.NoError(t, RequireChanRecv(t, updateDone))
	require.NoError(t, RequireChanRecv(t, closeDone))
	assert.Equal(t, int64(1), callbackCount.Load())
	assert.Equal(t, int64(0), dest.activeCallbacks.Load())
}

// TestDCPDestStop makes sure that cbgt's janitor sees a dest as not feedable once Stop is called.
func TestDCPDestStop(t *testing.T) {
	for _, wrapLogging := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrapLogging=%t", wrapLogging), func(t *testing.T) {
			sgDest, err := NewDCPDest(TestCtx(t), DCPDestOptions{
				Callback: func(sgbucket.FeedEvent) bool { return true },
				MaxVbNo:  1,
			})
			require.NoError(t, err)
			dest, ok := sgDest.(*DCPDest)
			if !ok {
				loggingDest, ok := sgDest.(*DCPLoggingDest)
				require.True(t, ok, "unexpected SGDest type %T", sgDest)
				dest = loggingDest.dest
			}
			sgDest = dest
			if wrapLogging {
				sgDest = &DCPLoggingDest{dest: dest}
			}

			pindex := &cbgt.PIndex{Dest: sgDest}
			feedable, err := pindex.IsFeedable()
			require.NoError(t, err)
			require.True(t, feedable)

			sgDest.Stop()
			feedable, err = pindex.IsFeedable()
			require.NoError(t, err)
			require.False(t, feedable)
			require.NoError(t, sgDest.Close(false))
		})
	}
}

// TestDCPDestCheckpointAfterStopAtSnapshotStart makes sure that a checkpoint written after cbgt sends a new snapshot
// marker, but before any of its mutations are processed, resumes the feed before the first mutation of that snapshot.
func TestDCPDestCheckpointAfterStopAtSnapshotStart(t *testing.T) {
	testCases := []struct {
		name               string
		processedSeqs      uint64 // mutations 1..processedSeqs are processed in a snapshot before the one at seq 4
		expectedResumedSeq uint64
	}{
		{name: "after a processed snapshot", processedSeqs: 3, expectedResumedSeq: 3},
		// A collection-filtered stream can send snapshot markers before any mutation reaches the dest.
		{name: "no mutations processed", processedSeqs: 0, expectedResumedSeq: 0},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

			const partition = "0"
			opts := DCPDestOptions{
				Callback:           func(sgbucket.FeedEvent) bool { return true },
				MetadataStore:      bucket.GetSingleDataStore(),
				MaxVbNo:            1,
				PersistCheckpoints: true,
				CheckpointPrefix:   t.Name() + "_",
			}
			dest, err := NewDCPDest(ctx, opts)
			require.NoError(t, err)

			_, lastSeq, err := dest.OpaqueGet(partition)
			require.NoError(t, err)
			require.Equal(t, uint64(0), lastSeq)

			if testCase.processedSeqs > 0 {
				require.NoError(t, dest.OpaqueSet(partition, fmt.Appendf(nil, `{"failOverLog":[[123,0]],"snapStart":1,"snapEnd":%d}`, testCase.processedSeqs)))
				for seq := uint64(1); seq <= testCase.processedSeqs; seq++ {
					require.NoError(t, dest.DataUpdate(partition, fmt.Appendf(nil, "doc%d", seq), seq, []byte(`{}`), seq, cbgt.DEST_EXTRAS_TYPE_NIL, nil))
				}
			}
			require.NoError(t, dest.OpaqueSet(partition, []byte(`{"failOverLog":[[123,0]],"snapStart":4,"snapEnd":6}`)))

			// Mutations that arrive after Stop are skipped, so the checkpoint must not move past them.
			dest.Stop()
			require.NoError(t, dest.DataUpdate(partition, []byte("doc4"), 4, []byte(`{}`), 4, cbgt.DEST_EXTRAS_TYPE_NIL, nil))
			dest.ForceCheckpointWrite()
			require.NoError(t, dest.Close(false))

			resumedDest, err := NewDCPDest(ctx, opts)
			require.NoError(t, err)
			defer func() { assert.NoError(t, resumedDest.Close(false)) }()
			_, lastSeq, err = resumedDest.OpaqueGet(partition)
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedResumedSeq, lastSeq, "cbgt streams mutations after lastSeq, so doc4 at seq 4 must not be skipped")
		})
	}
}
