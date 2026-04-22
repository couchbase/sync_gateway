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

// TestUpdateState verifies that UpdateState persists the state document and updates the in-memory State and
// CAS, and returns an error when the CAS is stale.
func TestUpdateState(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()

	t.Run("persists state and updates in-memory CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		require.True(t, mgr.State.ResyncRunning)
		require.NotZero(t, mgr.CAS)

		var storedState DatabaseState
		storeCAS, err := metadataStore.Get(docID, &storedState)
		require.NoError(t, err)
		require.Equal(t, mgr.CAS, storeCAS)
		require.Equal(t, mgr.State, storedState)
	})

	t.Run("returns error on stale CAS", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		mgr.CAS = 0 // force stale CAS
		require.Error(t, mgr.UpdateState(DatabaseState{ResyncRunning: false}))
	})
}

// TestGetState verifies that GetState reads back the persisted state, and returns a zero-value state
// without error when no state document exists.
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
		require.False(t, state.ResyncRunning)
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
		require.Equal(t, DatabaseState{}, mgr.State)
	})

	t.Run("returns error when doc already deleted", func(t *testing.T) {
		docID := base.NewMetadataKeys(t.Name()).DatabaseStateKey()
		mgr := NewDatabaseStateMgr(metadataStore, docID)
		require.NoError(t, mgr.UpdateState(DatabaseState{ResyncRunning: true}))
		require.NoError(t, mgr.DeleteState())
		require.Error(t, mgr.DeleteState())
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

		called := make(chan bool, 1)
		mgr.AddResyncFunc(func(resume bool) {
			select {
			case called <- resume:
			default:
			}
		})
		mgr.StartPolling(ctx)
		base.RequireChanRecvWithTimeout(t, called, 2*time.Second)
		mgr.StopPolling()
	})
}
