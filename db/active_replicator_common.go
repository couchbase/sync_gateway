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
		CheckpointID:     checkpointID,
	}
}

// reconnect synchronously calls the given _connectFn until successful, or times out trying. Retry loop can be stopped by cancelling a.ctx
func (a *activeReplicatorCommon) reconnect(_connectFn func() error) {
	base.DebugfCtx(a.ctx, base.KeyReplicate, "starting reconnector")

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
		case <-a.ctx.Done():
			return
		default:
		}

		a.lock.Lock()

		// preserve lastError from the previous connect attempt
		a.state = ReplicationStateReconnecting

		// disconnect no-ops if nothing is active, but will close any checkpointer processes, blip contexts, etc, if active.
		err = a._disconnect()
		if err != nil {
			base.InfofCtx(a.ctx, base.KeyReplicate, "error stopping replicator on reconnect: %v", err)
		}

		// set lastError, but don't set an error state inside the reconnect loop
		err = _connectFn()
		a.lastError = err

		a.lock.Unlock()

		if err != nil {
			base.InfofCtx(a.ctx, base.KeyReplicate, "error starting replicator on reconnect: %v", err)
		}
		return err != nil, err, nil
	}

	err, _ := base.RetryLoop("replicator reconnect", retryFunc, sleeperFunc)
	// release timer associated with context deadline
	if deadlineCancel != nil {
		deadlineCancel()
	}
	if err != nil {
		a.replicationStats.NumReconnectsAborted.Add(1)
		base.WarnfCtx(a.ctx, "couldn't reconnect replicator: %v", err)
	}
}

// Stop runs _disconnect and _stop on the replicator, and sets the Stopped replication state.
func (a *activeReplicatorCommon) Stop() error {
	a.lock.Lock()
	err := a._disconnect()
	a._stop()
	a.setState(ReplicationStateStopped)
	a.publishStatus()
	a.lock.Unlock()
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
	defer a.stateErrorLock.Unlock()
	a.state = ReplicationStateError
	a.lastError = err
	return err
}

// setState updates replicator state and resets lastError to nil.  Expects callers
// to be holding a.lock
func (a *activeReplicatorCommon) setState(state string) {
	a.stateErrorLock.Lock()
	defer a.stateErrorLock.Unlock()
	a.state = state
	a.lastError = nil
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

func (a *activeReplicatorCommon) publishStatus() {
	status, errorMessage := a.getStateWithErrorMessage()
	newRev := setLocalCheckpointStatus(a.config.ActiveDB, a.CheckpointID, status, errorMessage)
	if newRev != "" && a.Checkpointer != nil {
		a.Checkpointer.lastLocalCheckpointRevID = newRev
	}
}
