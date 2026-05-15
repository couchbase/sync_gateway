//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDatabaseStateUpdate verifies that UpdateState persists the state document and updates the in-memory State and CAS
func TestDatabaseStateUpdate(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()

	t.Run("persists state and updates in-memory CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(true)}))
		require.NotZero(t, mgr.CAS)

		var storedState DatabaseState
		storeCAS, err := metadataStore.Get(ctx, docID, &storedState)
		require.NoError(t, err)
		require.Equal(t, mgr.CAS, storeCAS)
	})

	t.Run("returns no error on stale CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(true)}))
		mgr.CAS = 0 // force stale CAS
		require.NoError(t, mgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(true)}))
	})
}

// TestGetState verifies that GetState reads back the persisted state, and returns a zero-value state
// with a doc-not-found error when no state document exists.
func TestGetState(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()

	t.Run("returns zero-value state when doc not found", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		state, cas, err := mgr.GetState(ctx)
		require.Error(t, err)
		require.True(t, base.IsDocNotFoundError(err))
		require.Zero(t, cas)
		require.Nil(t, state)
	})

	t.Run("returns persisted state and CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(true)}))
		state, cas, err := mgr.GetState(ctx)
		require.NoError(t, err)
		require.NotZero(t, cas)
		require.True(t, *state.ResyncRunning)
	})
}

// TestDatabaseStateMgrPolling verifies SetResyncFunc, StartPolling and StopPolling behaviour:
// the registered callback is invoked with the correct resume value on CAS changes or doc-not-found, skipped
// when the CAS is unchanged, and driven automatically by the polling goroutine.
func TestDatabaseStateMgrPolling(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()

	t.Run("callback invoked with true when doc exists and CAS changed", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond
		require.NoError(t, mgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(true)}))
		mgr.CAS = 0 // simulate stale CAS so the poller sees a change

		var callCount atomic.Int32
		var resumeVal atomic.Bool
		mgr.SetResyncFunc(func(resume bool) {
			callCount.Add(1)
			resumeVal.Store(resume)
		})
		mgr.StartPolling(ctx)
		defer mgr.StopPolling(ctx)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, int32(1), callCount.Load())
			assert.True(c, resumeVal.Load())
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("no callback when CAS unchanged", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond
		require.NoError(t, mgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(true)}))

		var callCount atomic.Int32
		mgr.SetResyncFunc(func(_ bool) { callCount.Add(1) })
		mgr.StartPolling(ctx)

		time.Sleep(50 * time.Millisecond)
		mgr.StopPolling(ctx)
		require.Equal(t, int32(0), callCount.Load())
	})

	t.Run("StopPolling halts the goroutine", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond

		// Register a callback that forwards the resume value to a buffered channel.
		// The select/default prevents blocking if the channel is already full.
		called := make(chan bool, 1)
		var callCount atomic.Int32
		mgr.SetResyncFunc(func(resume bool) {
			callCount.Add(1)
			select {
			case called <- resume:
				return
			default:
			}
		})

		// Start the polling goroutine.
		mgr.StartPolling(ctx)

		// Write a state document directly via the datastore (bypassing mgr.CAS) so that
		// the poller sees a CAS mismatch and invokes the callback.
		_, err := metadataStore.Update(ctx, docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
			bodyBytes, err := base.JSONMarshal(DatabaseState{ResyncRunning: base.Ptr(true)})
			if err != nil {
				return nil, nil, false, err
			}
			return bodyBytes, nil, false, nil
		})
		require.NoError(t, err)

		// Confirm the goroutine is running by waiting for the callback to fire.
		base.RequireChanRecvWithTimeout(t, called, 2*time.Second)
		require.Equal(t, int32(1), callCount.Load(), "expected exactly one callback before stopping")

		// Stop the polling goroutine.
		mgr.StopPolling(ctx)

		// Write a new state change directly to the store. If the goroutine were still
		// running it would detect the CAS mismatch and fire the callback again.
		_, err = metadataStore.Update(ctx, docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
			bodyBytes, err := base.JSONMarshal(DatabaseState{ResyncRunning: base.Ptr(false)})
			if err != nil {
				return nil, nil, false, err
			}
			return bodyBytes, nil, false, nil
		})
		require.NoError(t, err)

		// Wait several polling intervals and assert no callback was received,
		// confirming the goroutine has stopped.
		time.Sleep(50 * time.Millisecond)
		require.Empty(t, called)
		require.Equal(t, int32(1), callCount.Load(), "expected exactly one callback after stopping (no additional calls)")
	})
}
