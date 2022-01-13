/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"expvar"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	defaultInitialReconnectInterval = time.Second
	defaultMaxReconnectInterval     = time.Minute * 5
)

// replicatorCommon defines the struct contents shared by ActivePushReplicator
// and ActivePullReplicator
type activeReplicatorCommon struct {
	config                *ActiveReplicatorConfig
	blipSyncContext       *BlipSyncContext
	blipSender            *blip.Sender
	Stats                 expvar.Map
	Checkpointer          *Checkpointer
	checkpointerCtx       context.Context
	checkpointerCtxCancel context.CancelFunc
	CheckpointID          string // Used for checkpoint retrieval when Checkpointer isn't available
	initialStatus         *ReplicationStatus
	state                 string
	lastError             error
	stateErrorLock        sync.RWMutex // state and lastError share their own mutex to support retrieval while holding the main lock
	replicationStats      *BlipSyncStats
	onReplicatorComplete  ReplicatorCompleteFunc
	lock                  sync.RWMutex
	ctx                   context.Context
	ctxCancel             context.CancelFunc
	reconnectActive       base.AtomicBool // Tracks whether reconnect goroutine is active
	replicatorConnectFn   func() error    // the function called inside reconnectLoop.
	activeSendChanges     base.AtomicBool // Tracks whether sendChanges goroutine is active.
}

func newActiveReplicatorCommon(config *ActiveReplicatorConfig, direction ActiveReplicatorDirection) *activeReplicatorCommon {

	var replicationStats *BlipSyncStats
	var checkpointID string
	if direction == ActiveReplicatorTypePush {
		replicationStats = BlipSyncStatsForSGRPush(config.ReplicationStatsMap)
		checkpointID = PushCheckpointID(config.ID)
	} else {
		replicationStats = BlipSyncStatsForSGRPull(config.ReplicationStatsMap)
		checkpointID = PullCheckpointID(config.ID)
	}

	return &activeReplicatorCommon{
		config:           config,
		state:            ReplicationStateStopped,
		replicationStats: replicationStats,
		CheckpointID:     config.checkpointPrefix + checkpointID,
	}
}

// reconnectLoop synchronously calls replicatorConnectFn until successful, or times out trying. Retry loop can be stopped by cancelling ctx
func (a *activeReplicatorCommon) reconnectLoop() {
	base.DebugfCtx(a.ctx, base.KeyReplicate, "starting reconnector")
	defer func() {
		a.reconnectActive.Set(false)
	}()

	initialReconnectInterval := defaultInitialReconnectInterval
	if a.config.InitialReconnectInterval != 0 {
		initialReconnectInterval = a.config.InitialReconnectInterval
	}
	maxReconnectInterval := defaultMaxReconnectInterval
	if a.config.MaxReconnectInterval != 0 {
		maxReconnectInterval = a.config.MaxReconnectInterval
	}

	// ctx causes the retry loop to stop if cancelled
	ctx := a.ctx

	// if a reconnect timeout is set, we'll wrap the existing so both can stop the retry loop
	var deadlineCancel context.CancelFunc
	if a.config.TotalReconnectTimeout != 0 {
		ctx, deadlineCancel = context.WithDeadline(ctx, time.Now().Add(a.config.TotalReconnectTimeout))
	}

	sleeperFunc := base.SleeperFuncCtx(
		base.CreateIndefiniteMaxDoublingSleeperFunc(
			int(initialReconnectInterval.Milliseconds()),
			int(maxReconnectInterval.Milliseconds())),
		ctx)

	retryFunc := func() (shouldRetry bool, err error, _ interface{}) {
		select {
		case <-ctx.Done():
			return false, ctx.Err(), nil
		default:
		}

		a.lock.Lock()

		// preserve lastError from the previous connect attempt
		a.setState(ReplicationStateReconnecting)

		// disconnect no-ops if nothing is active, but will close any checkpointer processes, blip contexts, etc, if active.
		err = a._disconnect()
		if err != nil {
			base.InfofCtx(a.ctx, base.KeyReplicate, "error stopping replicator on reconnect: %v", err)
		}

		// set lastError, but don't set an error state inside the reconnect loop
		err = a.replicatorConnectFn()
		a.setLastError(err)
		a._publishStatus()

		a.lock.Unlock()

		if err != nil {
			base.InfofCtx(a.ctx, base.KeyReplicate, "error starting replicator on reconnect: %v", err)
		}
		return err != nil, err, nil
	}

	err, _ := base.RetryLoopCtx("replicator reconnect", retryFunc, sleeperFunc, ctx)
	// release timer associated with context deadline
	if deadlineCancel != nil {
		deadlineCancel()
	}
	if err != nil {
		a.replicationStats.NumReconnectsAborted.Add(1)
		base.WarnfCtx(ctx, "couldn't reconnect replicator: %v", err)
	}
}

// reconnect will disconnect and stop the replicator, but not set the state - such that it will be reassigned and started again.
func (a *activeReplicatorCommon) reconnect() error {
	a.lock.Lock()
	err := a._disconnect()
	a._publishStatus()
	a.lock.Unlock()
	return err
}

// stopAndDisconnect runs _disconnect and _stop on the replicator, and sets the Stopped replication state.
func (a *activeReplicatorCommon) stopAndDisconnect() error {
	a.lock.Lock()
	a._stop()
	err := a._disconnect()
	a.setState(ReplicationStateStopped)
	a._publishStatus()
	a.lock.Unlock()

	// Wait for up to 10s for reconnect goroutine to exit
	teardownStart := time.Now()
	for a.reconnectActive.IsTrue() && (time.Since(teardownStart) < time.Second*10) {
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

// _disconnect aborts any replicator processes used during a connected/running replication (checkpointing, blip contexts, etc.)
func (a *activeReplicatorCommon) _disconnect() error {
	if a == nil {
		// noop
		return nil
	}

	if a.checkpointerCtx != nil {
		a.checkpointerCtxCancel()
		a.Checkpointer.CheckpointNow()
		a.Checkpointer.closeWg.Wait()
	}
	a.checkpointerCtx = nil

	if a.blipSender != nil {
		a.blipSender.Close()
		a.blipSender = nil
	}

	if a.blipSyncContext != nil {
		a.blipSyncContext.Close()
		a.blipSyncContext = nil
	}

	return nil
}

// _stop aborts any replicator processes that run outside of a running replication (e.g: async reconnect handling)
func (a *activeReplicatorCommon) _stop() {
	if a.ctxCancel != nil {
		a.ctxCancel()
	}
}

type ReplicatorCompleteFunc func()

// _setError updates state and lastError, and
// returns the error provided.  Expects callers to be holding
// a.lock
func (a *activeReplicatorCommon) setError(err error) (passThrough error) {
	base.InfofCtx(a.ctx, base.KeyReplicate, "ActiveReplicator had error state set with err: %v", err)
	a.stateErrorLock.Lock()
	a.state = ReplicationStateError
	a.lastError = err
	a.stateErrorLock.Unlock()
	return err
}

func (a *activeReplicatorCommon) setLastError(err error) {
	a.stateErrorLock.Lock()
	a.lastError = err
	a.stateErrorLock.Unlock()
}

// setState updates replicator state and resets lastError to nil.  Expects callers
// to be holding a.lock
func (a *activeReplicatorCommon) setState(state string) {
	a.stateErrorLock.Lock()
	a.state = state
	if state == ReplicationStateRunning {
		a.lastError = nil
	}
	a.stateErrorLock.Unlock()
}

func (a *activeReplicatorCommon) getState() string {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	return a.state
}

func (a *activeReplicatorCommon) getLastError() error {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	return a.lastError
}

func (a *activeReplicatorCommon) getStateWithErrorMessage() (state string, lastErrorMessage string) {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	if a.lastError == nil {
		return a.state, ""
	} else {
		return a.state, a.lastError.Error()
	}
}

func (a *activeReplicatorCommon) GetStats() *BlipSyncStats {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.replicationStats
}

func (a *activeReplicatorCommon) _publishStatus() {
	status, errorMessage := a.getStateWithErrorMessage()
	if a.Checkpointer != nil {
		a.Checkpointer.setLocalCheckpointStatus(status, errorMessage)
	} else {
		setLocalCheckpointStatus(a.config.ActiveDB, a.CheckpointID, status, errorMessage)
	}

}
