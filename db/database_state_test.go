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
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		require.NotZero(t, mgr.CAS)

		var storedState DatabaseState
		storeCAS, err := metadataStore.Get(docID, &storedState)
		require.NoError(t, err)
		require.Equal(t, mgr.CAS, storeCAS)
	})

	t.Run("returns no error on stale CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		mgr.CAS = 0 // force stale CAS
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
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
		state, cas, err := mgr.GetState()
		require.Error(t, err)
		require.True(t, base.IsDocNotFoundError(err))
		require.Zero(t, cas)
		require.Nil(t, state)
	})

	t.Run("returns persisted state and CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		state, cas, err := mgr.GetState()
		require.NoError(t, err)
		require.NotZero(t, cas)
		require.True(t, state.ResyncRunning)
	})
}

// TestDeleteState verifies that DeleteState removes the state document, and returns an error when called
// again after the document has already been removed.
func TestDeleteState(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()

	t.Run("removes state doc", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		require.NoError(t, mgr.DeleteState())

		var state DatabaseState
		_, err := metadataStore.Get(docID, &state)
		require.True(t, base.IsDocNotFoundError(err))
	})

	t.Run("resets in-memory CAS and State on success", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		require.NoError(t, mgr.DeleteState())
		require.Zero(t, mgr.CAS)
	})

	t.Run("returns no error when doc already deleted", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		require.NoError(t, mgr.DeleteState())
		require.NoError(t, mgr.DeleteState())
	})
}

// TestDatabaseStateMgrPolling verifies AddResyncFunc, StartPolling and StopPolling behaviour:
// the registered callback is invoked with the correct resume value on CAS changes or doc-not-found, skipped
// when the CAS is unchanged, and driven automatically by the polling goroutine.
func TestDatabaseStateMgrPolling(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()

	t.Run("callback invoked with false when doc not found", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond

		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		_, err := metadataStore.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
			return nil, nil, true, nil
		})
		require.NoError(t, err)
		var called atomic.Bool
		var resumeVal atomic.Bool
		mgr.AddResyncFunc(func(resume bool) {
			called.Store(true)
			resumeVal.Store(resume)
		})
		mgr.StartPolling(ctx)
		defer mgr.StopPolling()

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, called.Load())
			assert.False(c, resumeVal.Load())
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("callback invoked with true when doc exists and CAS changed", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		mgr.CAS = 0 // simulate stale CAS so the poller sees a change

		var called atomic.Bool
		var resumeVal atomic.Bool
		mgr.AddResyncFunc(func(resume bool) {
			called.Store(true)
			resumeVal.Store(resume)
		})
		mgr.StartPolling(ctx)
		defer mgr.StopPolling()

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, called.Load())
			assert.True(c, resumeVal.Load())
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("no callback when CAS unchanged", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))

		var callCount atomic.Int32
		mgr.AddResyncFunc(func(_ bool) { callCount.Add(1) })
		mgr.StartPolling(ctx)

		time.Sleep(50 * time.Millisecond)
		mgr.StopPolling()
		require.Equal(t, int32(0), callCount.Load())
	})

	t.Run("StopPolling halts the goroutine", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond

		// Register a callback that forwards the resume value to a buffered channel.
		// The select/default prevents blocking if the channel is already full.
		called := make(chan bool, 1)
		mgr.AddResyncFunc(func(resume bool) {
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
		_, err := metadataStore.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
			bodyBytes, err := base.JSONMarshal(DatabaseState{ResyncRunning: true})
			if err != nil {
				return nil, nil, false, err
			}
			return bodyBytes, nil, false, nil
		})
		require.NoError(t, err)

		// Confirm the goroutine is running by waiting for the callback to fire.
		base.RequireChanRecvWithTimeout(t, called, 2*time.Second)

		// Stop the polling goroutine.
		mgr.StopPolling()

		// Write a new state change directly to the store. If the goroutine were still
		// running it would detect the CAS mismatch and fire the callback again.
		_, err = metadataStore.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
			bodyBytes, err := base.JSONMarshal(DatabaseState{ResyncRunning: false})
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
	})

	t.Run("Polling without state being created", func(t *testing.T) {
		// Verifies that polling gracefully handles the case where the state document
		// does not exist yet. This is the expected initial state, and should not
		// produce error logs when polled.
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		mgr.pollingInterval = 10 * time.Millisecond

		output := base.CaptureLogOutput(t, func() {
			mgr.StartPolling(ctx)
			time.Sleep(200 * time.Millisecond)
			mgr.StopPolling()
		})

		assert.NotContains(t, output, "error while polling for offline database state")
	})
}
