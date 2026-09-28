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
