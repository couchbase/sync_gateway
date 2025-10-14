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
	"fmt"
	"sync"
	"sync/atomic"
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
	config                          *ActiveReplicatorConfig
	blipSyncContext                 *BlipSyncContext
	blipSender                      *blip.Sender
	Stats                           expvar.Map
	checkpointerCtx                 context.Context
	checkpointerCtxCancel           context.CancelFunc
	CheckpointID                    string // Used for checkpoint retrieval when Checkpointer isn't available
	initialStatus                   *ReplicationStatus
	statusKey                       string // key used when persisting replication status
	state                           string
	direction                       ActiveReplicatorDirection
	lastError                       error
	stateErrorLock                  sync.RWMutex // state and lastError share their own mutex to support retrieval while holding the main lock
	replicationStats                *BlipSyncStats
	_getStatusCallback              func(context.Context) *ReplicationStatus
	onReplicatorComplete            ReplicatorCompleteFunc
	lock                            sync.RWMutex
	_ctx                            context.Context
	ctxCancel                       context.CancelFunc
	reconnectActive                 base.AtomicBool                                             // Tracks whether reconnect goroutine is active
	replicatorConnectFn             func(context.Context) error                                 // the function called inside reconnect.
	registerCheckpointerCallbacksFn func(context.Context, *activeReplicatorCollection) error    // function to register checkpointer callbacks
	activeSendChanges               atomic.Int32                                                // Tracks whether sendChanges goroutines are active, there is one per collection.
	namedCollections                map[base.ScopeAndCollectionName]*activeReplicatorCollection // set only if the replicator is running with collections - access with forEachCollection
	defaultCollection               *activeReplicatorCollection                                 // set only if the replicator is not running with collections - access with forEachCollection
}

// GetSingleCollection returns the single collection for the replication.
func (arc *activeReplicatorCommon) GetSingleCollection(tb testing.TB) *activeReplicatorCollection {
	if arc.config.CollectionsEnabled {
		if len(arc.namedCollections) != 1 {
			tb.Fatalf("Can only use GetSingleCollection with 1 collection, had %d", len(arc.namedCollections))
		}
		for _, collection := range arc.namedCollections {
			return collection
		}
	}
	return arc.defaultCollection
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
	switch direction {
	case ActiveReplicatorTypePush:
		replicationStats = BlipSyncStatsForSGRPush(config.ReplicationStatsMap)
		checkpointID = PushCheckpointID(config.ID)
	case ActiveReplicatorTypePull:
		replicationStats = BlipSyncStatsForSGRPull(config.ReplicationStatsMap)
		checkpointID = PullCheckpointID(config.ID)
	default:
		return nil, fmt.Errorf("Invalid replicator direction: %v", direction)
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

	arc := activeReplicatorCommon{
		config:           config,
		state:            ReplicationStateStopped,
		replicationStats: replicationStats,
		CheckpointID:     checkpointID,
		initialStatus:    initialStatus,
		statusKey:        metakeys.ReplicationStatusKey(checkpointID),
		direction:        direction,
	}

	if config.CollectionsEnabled {
		arc.namedCollections = make(map[base.ScopeAndCollectionName]*activeReplicatorCollection)
	} else {
		defaultDatabaseCollection, err := config.ActiveDB.GetDefaultDatabaseCollection()
		if err != nil {
			return nil, err
		}
		arc.defaultCollection = &activeReplicatorCollection{
			metadataStore:       config.ActiveDB.MetadataStore,
			collectionDataStore: defaultDatabaseCollection.dataStore,
		}
	}

	return &arc, nil
}

// Start starts the replicator, setting the state to ReplicationStateStarting and starting the status reporter.
func (arc *activeReplicatorCommon) Start(ctx context.Context) error {
	arc.lock.Lock()
	defer arc.lock.Unlock()

	if arc._ctx != nil && arc._ctx.Err() == nil {
		return fmt.Errorf("Replicator is already running")
	}

	arc.setState(ReplicationStateStarting)
	logCtx := base.CorrelationIDLogCtx(ctx,
		arc.config.ID+"-"+string(arc.direction))
	arc._ctx, arc.ctxCancel = context.WithCancel(logCtx)
	ctx = arc._ctx

	arc.startStatusReporter(ctx)

	err := arc.replicatorConnectFn(ctx)
	if err != nil {
		arc.setError(ctx, err)
		base.WarnfCtx(ctx, "Couldn't connect: %s", err)
		if errors.Is(err, fatalReplicatorConnectError) {
			base.WarnfCtx(ctx, "Stopping replication connection attempt")
			defer arc.ctxCancel()
		} else {
			base.InfofCtx(ctx, base.KeyReplicate, "Attempting to reconnect in background: %v", err)
			arc.reconnect(ctx)
		}
	}
	arc._publishStatus(ctx)
	return err
}

// initCheckpointer starts a checkpointer. The remoteCheckpoints are only for collections and indexed by the blip collectionIdx. If using default collection only, replicationCheckpoints is an empty array.
func (arc *activeReplicatorCommon) _initCheckpointer(ctx context.Context, remoteCheckpoints []replicationCheckpoint) error {
	// wrap the replicator context with a cancelFunc that can be called to abort the checkpointer from _disconnect
	arc.checkpointerCtx, arc.checkpointerCtxCancel = context.WithCancel(ctx)

	err := arc.forEachCollection(func(c *activeReplicatorCollection) error {
		checkpointHash, hashErr := arc.config.CheckpointHash(c.collectionIdx)
		if hashErr != nil {
			return hashErr
		}

		c.Checkpointer = NewCheckpointer(arc.checkpointerCtx, c.metadataStore, c.collectionDataStore, arc.CheckpointID, checkpointHash, arc.blipSender, arc.config, c.collectionIdx)

		if arc.config.CollectionsEnabled {
			err := c.Checkpointer.setLastCheckpointSeq(&remoteCheckpoints[*c.collectionIdx])
			if err != nil {
				return err
			}
		} else {
			err := c.Checkpointer.fetchDefaultCollectionCheckpoints()
			if err != nil {
				return err
			}
		}

		if err := arc.registerCheckpointerCallbacksFn(ctx, c); err != nil {
			return err
		}

		c.Checkpointer.Start()
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// GetStatus is used to retrieve replication status. Combines current running stats with initialStatus.
func (arc *activeReplicatorCommon) GetStatus(ctx context.Context) *ReplicationStatus {
	arc.lock.RLock()
	defer arc.lock.RUnlock()
	return arc._getStatusCallback(ctx)
}

// reset performs a reset on the replication by removing the local checkpoint document.
func (arc *activeReplicatorCommon) reset() error {
	if arc.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", arc.config.ID)
	}

	arc.lock.Lock()
	defer arc.lock.Unlock()
	ctx := arc._ctx

	if err := arc.forEachCollection(func(c *activeReplicatorCollection) error {
		if err := resetLocalCheckpoint(c.collectionDataStore, arc.CheckpointID); err != nil {
			return err
		}
		c.Checkpointer = nil
		return nil
	}); err != nil {
		return err
	}

	return removeLocalStatus(ctx, arc.config.ActiveDB.MetadataStore, arc.statusKey)
}

// reconnect asynchronously calls replicatorConnectFn until successful, or times out trying. Retry loop can be stopped by cancelling ctx
func (arc *activeReplicatorCommon) reconnect(ctx context.Context) {
	arc.reconnectActive.Set(true)
	go func() {
		base.DebugfCtx(ctx, base.KeyReplicate, "starting reconnector")
		defer func() {
			arc.reconnectActive.Set(false)
		}()

		initialReconnectInterval := defaultInitialReconnectInterval
		if arc.config.InitialReconnectInterval != 0 {
			initialReconnectInterval = arc.config.InitialReconnectInterval
		}
		maxReconnectInterval := defaultMaxReconnectInterval
		if arc.config.MaxReconnectInterval != 0 {
			maxReconnectInterval = arc.config.MaxReconnectInterval
		}

		// if a reconnect timeout is set, we'll wrap the existing so both can stop the retry loop
		var deadlineCancel context.CancelFunc
		if arc.config.TotalReconnectTimeout != 0 {
			ctx, deadlineCancel = context.WithDeadline(ctx, time.Now().Add(arc.config.TotalReconnectTimeout))
		}

		sleeperFunc := base.SleeperFuncCtx(
			base.CreateIndefiniteMaxDoublingSleeperFunc(
				int(initialReconnectInterval.Milliseconds()),
				int(maxReconnectInterval.Milliseconds())),
			ctx)

		retryFunc := func() (shouldRetry bool, err error, _ any) {
			// check before and after acquiring lock to make sure to exit early if ActiveReplicatorCommon.Stop() was called.
			if ctx.Err() != nil {
				return false, ctx.Err(), nil
			}

			arc.lock.Lock()
			defer arc.lock.Unlock()

			if ctx.Err() != nil {
				return false, ctx.Err(), nil
			}

			base.DebugfCtx(ctx, base.KeyReplicate, "Attempting to reconnect replicator %s", arc.config.ID)

			// preserve lastError from the previous connect attempt
			arc.setState(ReplicationStateReconnecting)

			// disconnect no-ops if nothing is active, but will close any checkpointer processes, blip contexts, etc, if active.
			base.TracefCtx(ctx, base.KeyReplicate, "calling disconnect from reconnect")
			err = arc._disconnect(ctx)
			if err != nil {
				base.InfofCtx(ctx, base.KeyReplicate, "error stopping replicator on reconnect: %v", err)
			}

			// set lastError, but don't set an error state inside the reconnect loop
			err = arc.replicatorConnectFn(ctx)
			arc.setLastError(err)
			arc._publishStatus(ctx)

			if err != nil {
				base.InfofCtx(ctx, base.KeyReplicate, "error starting replicator %s on reconnect: %v", arc.config.ID, err)
			} else {
				base.DebugfCtx(ctx, base.KeyReplicate, "replicator %s successfully reconnected", arc.config.ID)
			}
			return err != nil, err, nil
		}

		retryErr, _ := base.RetryLoop(ctx, "replicator reconnect", retryFunc, sleeperFunc)
		// release timer associated with context deadline
		if deadlineCancel != nil {
			deadlineCancel()
		}
		// Exit early if no error
		if retryErr == nil {
			return
		}

		// replicator was stopped - appropriate state has already been set
		if errors.Is(ctx.Err(), context.Canceled) {
			base.DebugfCtx(ctx, base.KeyReplicate, "exiting reconnect loop: %v", retryErr)
			return
		}

		base.WarnfCtx(ctx, "aborting reconnect loop: %v", retryErr)
		arc.replicationStats.NumReconnectsAborted.Add(1)
		arc.lock.Lock()
		defer arc.lock.Unlock()
		// use setState to preserve last error from retry loop set by setLastError
		arc.setState(ReplicationStateError)
		arc._publishStatus(ctx)
		arc._stop(ctx)
	}()
}

// disconnect will disconnect and stop the replicator, but not set the state - such that it will be reassigned and started again.
func (arc *activeReplicatorCommon) disconnect(ctx context.Context) error {
	arc.lock.Lock()
	base.TracefCtx(ctx, base.KeyReplicate, "Calling disconnect without stopping the replicator")
	err := arc._disconnect(ctx)
	arc._publishStatus(ctx)
	arc.lock.Unlock()
	return err
}

// stopAndDisconnect runs _disconnect and _stop on the replicator, and sets the Stopped replication state.
func (arc *activeReplicatorCommon) stopAndDisconnect(ctx context.Context) error {
	arc.lock.Lock()
	arc._stop(ctx)
	base.TracefCtx(ctx, base.KeyReplicate, "Calling _stop and _disconnect from stopAndDisconnect()")
	err := arc._disconnect(ctx)
	arc.setState(ReplicationStateStopped)
	arc._publishStatus(ctx)
	arc.lock.Unlock()

	// Wait for up to 10s for reconnect goroutine to exit
	teardownStart := time.Now()
	for arc.reconnectActive.IsTrue() && (time.Since(teardownStart) < time.Second*10) {
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

// _disconnect aborts any replicator processes used during a connected/running replication (checkpointing, blip contexts, etc.)
func (arc *activeReplicatorCommon) _disconnect(ctx context.Context) error {
	if arc == nil {
		// noop
		return nil
	}

	if arc.checkpointerCtx != nil {
		base.TracefCtx(ctx, base.KeyReplicate, "cancelling checkpointer context inside _disconnect")
		arc.checkpointerCtxCancel()
		_ = arc.forEachCollection(func(c *activeReplicatorCollection) error {
			c.Checkpointer.closeWg.Wait()
			c.Checkpointer.CheckpointNow()
			return nil
		})
	}
	arc.checkpointerCtx = nil

	if arc.blipSender != nil {
		base.TracefCtx(ctx, base.KeyReplicate, "closing blip sender")
		arc.blipSender.Close()
		arc.blipSender = nil
	}

	if arc.blipSyncContext != nil {
		base.TracefCtx(ctx, base.KeyReplicate, "closing blip sync context")
		arc.blipSyncContext.Close()
		arc.blipSyncContext = nil
	}

	return nil
}

// _stop aborts any replicator processes that run outside of a running replication connection (e.g: async reconnect handling, statsreporter)
func (arc *activeReplicatorCommon) _stop(ctx context.Context) {
	if arc.ctxCancel != nil {
		base.TracefCtx(ctx, base.KeyReplicate, "cancelling context on activeReplicatorCommon in _stop()")
		arc.ctxCancel()
	}
}

type ReplicatorCompleteFunc func()

// setError updates state to ReplicationStateError and sets the last error.
func (arc *activeReplicatorCommon) setError(ctx context.Context, err error) {
	base.InfofCtx(ctx, base.KeyReplicate, "ActiveReplicator had error state set with err: %v", err)
	arc.stateErrorLock.Lock()
	arc.state = ReplicationStateError
	arc.lastError = err
	arc.stateErrorLock.Unlock()
}

// setLastError updates the lastError field for the replicator without updating the replicator state.
func (arc *activeReplicatorCommon) setLastError(err error) {
	arc.stateErrorLock.Lock()
	arc.lastError = err
	arc.stateErrorLock.Unlock()
}

// setState updates replicator state and resets lastError to nil.  Expects callers
// to be holding activeReplicatorCommon.lock
func (arc *activeReplicatorCommon) setState(state string) {
	arc.stateErrorLock.Lock()
	arc.state = state
	if state == ReplicationStateRunning {
		arc.lastError = nil
	}
	arc.stateErrorLock.Unlock()
}

func (arc *activeReplicatorCommon) getState() string {
	arc.stateErrorLock.RLock()
	defer arc.stateErrorLock.RUnlock()
	return arc.state
}

// getStateWithErrorMessage returns the current state and last error message for the replicator.
func (arc *activeReplicatorCommon) getStateWithErrorMessage() (state string, lastErrorMessage string) {
	arc.stateErrorLock.RLock()
	defer arc.stateErrorLock.RUnlock()
	if arc.lastError == nil {
		return arc.state, ""
	}
	return arc.state, arc.lastError.Error()
}

func (arc *activeReplicatorCommon) GetStats() *BlipSyncStats {
	arc.lock.RLock()
	defer arc.lock.RUnlock()
	return arc.replicationStats
}

// getCheckpointHighSeq returns the highest sequence number that has been processed by the replicator across all collections.
func (arc *activeReplicatorCommon) getCheckpointHighSeq(ctx context.Context) string {
	var highSeq SequenceID
	err := arc.forEachCollection(func(c *activeReplicatorCollection) error {
		if c.Checkpointer != nil {
			safeSeq := c.Checkpointer.calculateSafeProcessedSeq()
			if highSeq.Before(safeSeq) {
				highSeq = safeSeq
			}
		}
		return nil
	})
	if err != nil {
		base.WarnfCtx(ctx, "Error calculating high sequence: %v", err)
		return ""
	}

	var highSeqStr string
	if highSeq.IsNonZero() {
		highSeqStr = highSeq.String()
	}
	return highSeqStr
}

// publishStatus updates the replication status document in the metadata store.
func (arc *activeReplicatorCommon) publishStatus(ctx context.Context) {
	arc.lock.Lock()
	defer arc.lock.Unlock()
	arc._publishStatus(ctx)
}

// _publishStatus updates the replication status document in the metadata store. Requires holding activeReplicatorCommon.lock before calling.
func (arc *activeReplicatorCommon) _publishStatus(ctx context.Context) {
	status := arc._getStatusCallback(ctx)
	err := setLocalStatus(ctx, arc.config.ActiveDB.MetadataStore, arc.statusKey, status, int(arc.config.ActiveDB.Options.LocalDocExpirySecs))
	if err != nil {
		base.WarnfCtx(ctx, "Couldn't set status for replication: %v", err)
	}
}

func (arc *activeReplicatorCommon) startStatusReporter(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(arc.config.CheckpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				func() {
					arc.lock.RLock()
					defer arc.lock.RUnlock()
					arc._publishStatus(ctx)
				}()
			case <-ctx.Done():
				base.DebugfCtx(ctx, base.KeyReplicate, "stats reporter goroutine stopped")
				return
			}
		}
	}()
}

// getLocalStatusDoc retrieves replication status document for a given client ID from the given metadataStore
func getLocalStatusDoc(ctx context.Context, metadataStore base.DataStore, statusKey string) (*ReplicationStatusDoc, error) {
	statusDocBytes, err := getWithTouch(metadataStore, statusKey, 0)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
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
	base.TracefCtx(ctx, base.KeyReplicate, "setLocalStatus for %q (%#+v)", statusKey, status)

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

	_, _, err = putDocWithRevision(metadataStore, statusKey, revID, newStatus.AsBody(), localDocExpirySecs)
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

	_, _, err = putDocWithRevision(metadataStore, statusKey, currentStatus.Rev, nil, 0)
	return err
}

// registerFunctions must be called once after immediately after newActiveReplicatorCommon to set the functions that require a circular definition from parent (ActiveReplicatorPush/ActiveReplicatorPull) and activeReplicatorCommon.
func (arc *activeReplicatorCommon) registerFunctions(getStatusFn func(context.Context) *ReplicationStatus, connectFn func(context.Context) error, registerCheckpointerCallbacksFn func(context.Context, *activeReplicatorCollection) error) {
	arc._getStatusCallback = getStatusFn
	arc.replicatorConnectFn = connectFn
	arc.registerCheckpointerCallbacksFn = registerCheckpointerCallbacksFn
}
