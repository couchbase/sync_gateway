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
	"errors"
	"expvar"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	defaultInitialReconnectInterval = time.Second
	defaultMaxReconnectInterval     = time.Minute * 5
)

var fatalReplicatorConnectError = errors.New("Fatal replication connection")

// replicatorCommon defines the struct contents shared by ActivePushReplicator
// and ActivePullReplicator
type activeReplicatorCommon struct {
	config                *ActiveReplicatorConfig
	blipSyncContext       *BlipSyncContext
	blipSender            *blip.Sender
	Stats                 expvar.Map
	checkpointerCtx       context.Context
	checkpointerCtxCancel context.CancelFunc
	CheckpointID          string // Used for checkpoint retrieval when Checkpointer isn't available
	initialStatus         *ReplicationStatus
	statusKey             string // key used when persisting replication status
	state                 string
	lastError             error
	stateErrorLock        sync.RWMutex // state and lastError share their own mutex to support retrieval while holding the main lock
	replicationStats      *BlipSyncStats
	_getStatusCallback    func() *ReplicationStatus
	onReplicatorComplete  ReplicatorCompleteFunc
	lock                  sync.RWMutex
	ctx                   context.Context
	ctxCancel             context.CancelFunc
	reconnectActive       base.AtomicBool                                             // Tracks whether reconnect goroutine is active
	replicatorConnectFn   func() error                                                // the function called inside reconnectLoop.
	activeSendChanges     base.AtomicBool                                             // Tracks whether sendChanges goroutine is active.
	namedCollections      map[base.ScopeAndCollectionName]*activeReplicatorCollection // set only if the replicator is running with collections - access with forEachCollection
	defaultCollection     *activeReplicatorCollection                                 // set only if the replicator is not running with collections - access with forEachCollection
}

// GetSingleCollection returns the single collection for the replication.
func (apr *activeReplicatorCommon) GetSingleCollection(tb testing.TB) *activeReplicatorCollection {
	if apr.config.CollectionsEnabled {
		if len(apr.namedCollections) != 1 {
			tb.Fatalf("Can only use GetSingleCollection with 1 collection, had %d", len(apr.namedCollections))
		}
		for _, collection := range apr.namedCollections {
			return collection
		}
	}
	return apr.defaultCollection
}

// activeReplicatorCollection stores data about a single collection for the replication.
type activeReplicatorCollection struct {
	metadataStore       base.DataStore // DataStore for metadata outside the collection.
	collectionDataStore base.DataStore // DataStore for this collection
	collectionIdx       *int           // collectionIdx for this collection for this BLIP replication, passed into Get/SetCheckpoint messages
	Checkpointer        *Checkpointer  // Checkpointer for this collection
}

func newActiveReplicatorCommon(ctx context.Context, config *ActiveReplicatorConfig, direction ActiveReplicatorDirection) (*activeReplicatorCommon, error) {

	var replicationStats *BlipSyncStats
	var checkpointID string
	if direction == ActiveReplicatorTypePush {
		replicationStats = BlipSyncStatsForSGRPush(config.ReplicationStatsMap)
		checkpointID = PushCheckpointID(config.ID)
	} else {
		replicationStats = BlipSyncStatsForSGRPull(config.ReplicationStatsMap)
		checkpointID = PullCheckpointID(config.ID)
	}

	if config.CheckpointInterval == 0 {
		config.CheckpointInterval = DefaultCheckpointInterval
	}

	initialStatus, err := LoadReplicationStatus(ctx, config.ActiveDB.DatabaseContext, config.ID)
	if err != nil {
		// Not finding an initialStatus isn't fatal, but we should at least log that we'll reset stats when we do...
		base.InfofCtx(ctx, base.KeyReplicate, "Couldn't load initial replication status for %q: %v - stats will be reset", config.ID, err)
	}

	checkpointID = config.checkpointPrefix + checkpointID

	metakeys := base.DefaultMetadataKeys
	if config.ActiveDB != nil {
		metakeys = config.ActiveDB.MetadataKeys
	}

	apr := activeReplicatorCommon{
		config:           config,
		state:            ReplicationStateStopped,
		replicationStats: replicationStats,
		CheckpointID:     checkpointID,
		initialStatus:    initialStatus,
		statusKey:        metakeys.ReplicationStatusKey(checkpointID),
	}

	if config.CollectionsEnabled {
		apr.namedCollections = make(map[base.ScopeAndCollectionName]*activeReplicatorCollection)
	} else {
		defaultDatabaseCollection, err := config.ActiveDB.GetDefaultDatabaseCollection()
		if err != nil {
			return nil, err
		}
		apr.defaultCollection = &activeReplicatorCollection{
			metadataStore:       config.ActiveDB.MetadataStore,
			collectionDataStore: defaultDatabaseCollection.dataStore,
		}
	}

	return &apr, nil
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
		base.TracefCtx(a.ctx, base.KeyReplicate, "calling disconnect from reconnectLoop")
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
	base.TracefCtx(a.ctx, base.KeyReplicate, "Calling disconnect from reconnect()")
	err := a._disconnect()
	a._publishStatus()
	a.lock.Unlock()
	return err
}

// stopAndDisconnect runs _disconnect and _stop on the replicator, and sets the Stopped replication state.
func (a *activeReplicatorCommon) stopAndDisconnect() error {
	a.lock.Lock()
	a._stop()
	base.TracefCtx(a.ctx, base.KeyReplicate, "Calling _stop and _disconnect from stopAndDisconnect()")
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
		base.TracefCtx(a.ctx, base.KeyReplicate, "cancelling checkpointer context inside _disconnect")
		a.checkpointerCtxCancel()
		_ = a.forEachCollection(func(c *activeReplicatorCollection) error {
			c.Checkpointer.closeWg.Wait()
			c.Checkpointer.CheckpointNow()
			return nil
		})
	}
	a.checkpointerCtx = nil

	if a.blipSender != nil {
		base.TracefCtx(a.ctx, base.KeyReplicate, "closing blip sender")
		a.blipSender.Close()
		a.blipSender = nil
	}

	if a.blipSyncContext != nil {
		base.TracefCtx(a.ctx, base.KeyReplicate, "closing blip sync context")
		a.blipSyncContext.Close()
		a.blipSyncContext = nil
	}

	return nil
}

// _stop aborts any replicator processes that run outside of a running replication (e.g: async reconnect handling)
func (a *activeReplicatorCommon) _stop() {
	if a.ctxCancel != nil {
		base.TracefCtx(a.ctx, base.KeyReplicate, "cancelling context on activeReplicatorCommon in _stop()")
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

// requires a.stateErrorLock
func (a *activeReplicatorCommon) _getStateWithErrorMessage() (state string, lastErrorMessage string) {
	if a.lastError == nil {
		return a.state, ""
	}
	return a.state, a.lastError.Error()
}

func (a *activeReplicatorCommon) getStateWithErrorMessage() (state string, lastErrorMessage string) {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	return a._getStateWithErrorMessage()
}

func (a *activeReplicatorCommon) GetStats() *BlipSyncStats {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.replicationStats
}

// getCheckpointHighSeq returns the highest sequence number that has been processed by the replicator across all collections.
func (a *activeReplicatorCommon) getCheckpointHighSeq() string {
	var highSeq SequenceID
	err := a.forEachCollection(func(c *activeReplicatorCollection) error {
		if c.Checkpointer != nil {
			safeSeq := c.Checkpointer.calculateSafeProcessedSeq()
			if highSeq.Before(safeSeq) {
				highSeq = safeSeq
			}
		}
		return nil
	})
	if err != nil {
		base.WarnfCtx(a.ctx, "Error calculating high sequence: %v", err)
		return ""
	}

	var highSeqStr string
	if highSeq.IsNonZero() {
		highSeqStr = highSeq.String()
	}
	return highSeqStr
}

func (a *activeReplicatorCommon) _publishStatus() {
	status := a._getStatusCallback()
	err := setLocalStatus(a.ctx, a.config.ActiveDB.MetadataStore, a.statusKey, status, int(a.config.ActiveDB.Options.LocalDocExpirySecs))
	if err != nil {
		base.WarnfCtx(a.ctx, "Couldn't set status for replication: %v", err)
	}
}

func (arc *activeReplicatorCommon) startStatusReporter() error {
	go func(ctx context.Context) {
		ticker := time.NewTicker(arc.config.CheckpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				func() {
					arc.lock.RLock()
					defer arc.lock.RUnlock()
					arc._publishStatus()
				}()
			case <-ctx.Done():
				base.DebugfCtx(ctx, base.KeyReplicate, "stats reporter goroutine stopped")
				return
			}
		}
	}(arc.ctx)
	return nil
}

// getLocalStatusDoc retrieves replication status document for a given client ID from the given metadataStore
func getLocalStatusDoc(ctx context.Context, metadataStore base.DataStore, statusKey string) (*ReplicationStatusDoc, error) {
	statusDocBytes, err := getWithTouch(metadataStore, statusKey, 0)
	if err != nil {
		if !base.IsKeyNotFoundError(metadataStore, err) {
			return nil, err
		}
		base.DebugfCtx(ctx, base.KeyReplicate, "couldn't find existing local checkpoint for ID %q", statusKey)
		return nil, nil
	}
	var statusDoc *ReplicationStatusDoc
	err = base.JSONUnmarshal(statusDocBytes, &statusDoc)
	return statusDoc, err
}

// getLocalStatus retrieves replication status for a given client ID from the given metadataStore
func getLocalStatus(ctx context.Context, metadataStore base.DataStore, statusKey string) (*ReplicationStatus, error) {
	localStatusDoc, err := getLocalStatusDoc(ctx, metadataStore, statusKey)
	if err != nil {
		return nil, err
	}
	if localStatusDoc != nil {
		return localStatusDoc.Status, nil
	}
	return nil, nil
}

// setLocalStatus updates replication status.
func setLocalStatus(ctx context.Context, metadataStore base.DataStore, statusKey string, status *ReplicationStatus, localDocExpirySecs int) (err error) {
	base.TracefCtx(ctx, base.KeyReplicate, "setLocalStatus for %q (%v)", statusKey, status)

	// obtain current rev
	currentStatus, err := getLocalStatusDoc(ctx, metadataStore, statusKey)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to retrieve local status doc for %s, status not updated", statusKey)
		return nil
	}

	var revID string
	if currentStatus != nil {
		revID = currentStatus.Rev
	}

	newStatus := &ReplicationStatusDoc{
		Status: status,
	}

	_, err = putDocWithRevision(metadataStore, statusKey, revID, newStatus.AsBody(), localDocExpirySecs)
	return err
}

// removeLocalStatus removes a replication status from the given metadataStore. This is done when a replication is reset.
func removeLocalStatus(ctx context.Context, metadataStore base.DataStore, statusKey string) (err error) {
	base.TracefCtx(ctx, base.KeyReplicate, "removeLocalStatus() for %q", statusKey)

	// obtain current rev
	currentStatus, err := getLocalStatusDoc(ctx, metadataStore, statusKey)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to retrieve local status doc for %s, status not removed", statusKey)
		return nil
	}
	if currentStatus == nil {
		// nothing to do - status already doesn't exist
		return nil
	}

	_, err = putDocWithRevision(metadataStore, statusKey, currentStatus.Rev, nil, 0)
	return err
}
