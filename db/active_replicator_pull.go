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
	"fmt"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	*activeReplicatorCommon
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	apr := ActivePullReplicator{
		activeReplicatorCommon: newActiveReplicatorCommon(config, ActiveReplicatorTypePull),
	}
	apr.replicatorConnectFn = apr._connect
	return &apr
}

func (apr *ActivePullReplicator) Start() error {
	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if apr.ctx != nil && apr.ctx.Err() == nil {
		return fmt.Errorf("ActivePullReplicator already running")
	}

	apr.setState(ReplicationStateStarting)
	logCtx := context.WithValue(context.Background(), base.LogContextKey{}, base.LogContext{CorrelationID: apr.config.ID + "-" + string(ActiveReplicatorTypePull)})
	apr.ctx, apr.ctxCancel = context.WithCancel(logCtx)

	err := apr._connect()
	if err != nil {
		_ = apr.setError(err)
		base.WarnfCtx(apr.ctx, "Couldn't connect. Attempting to reconnect in background: %v", err)
		apr.reconnectActive.Set(true)
		go apr.reconnectLoop()
	}
	apr._publishStatus()
	return err
}

func (apr *ActivePullReplicator) _connect() error {
	var err error
	apr.blipSender, apr.blipSyncContext, err = connect(apr.activeReplicatorCommon, "-pull")
	if err != nil {
		return err
	}

	if apr.config.ConflictResolverFunc != nil {
		apr.blipSyncContext.conflictResolver = NewConflictResolver(apr.config.ConflictResolverFunc, apr.config.ReplicationStatsMap)
	}
	apr.blipSyncContext.purgeOnRemoval = apr.config.PurgeOnRemoval

	// wrap the replicator context with a cancelFunc that can be called to abort the checkpointer from _disconnect
	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(apr.ctx)
	if err := apr._initCheckpointer(); err != nil {
		// clean up anything we've opened so far
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	subChangesRequest := SubChangesRequest{
		Continuous:     apr.config.Continuous,
		Batch:          apr.config.ChangesBatchSize,
		Since:          apr.Checkpointer.lastCheckpointSeq,
		Filter:         apr.config.Filter,
		FilterChannels: apr.config.FilterChannels,
		DocIDs:         apr.config.DocIDs,
		ActiveOnly:     apr.config.ActiveOnly,
		clientType:     clientTypeSGR2,
		Revocations:    apr.config.PurgeOnRemoval,
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		// clean up anything we've opened so far
		apr.checkpointerCtxCancel()
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	apr.setState(ReplicationStateRunning)

	if apr.blipSyncContext.blipContext.ActiveSubprotocol() == BlipCBMobileReplicationV2 && apr.config.PurgeOnRemoval {
		base.ErrorfCtx(apr.config.ActiveDB.Ctx, "Pull replicator ID:%s running with revocations enabled but target does not support revocations. Sync Gateway 3.0 required.", apr.config.ID)
	}

	return nil
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePullReplicator) Complete() {
	apr.lock.Lock()
	if apr == nil {
		apr.lock.Unlock()
		return
	}

	err := apr.Checkpointer.waitForExpectedSequences()
	if err != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
	}

	apr._stop()

	stopErr := apr._disconnect()
	if stopErr != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}
	apr.setState(ReplicationStateStopped)

	// unlock the replication before triggering callback, in case callback attempts to access replication information
	// from the replicator
	onCompleteCallback := apr.onReplicatorComplete

	apr._publishStatus()
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

func (apr *ActivePullReplicator) _initCheckpointer() error {

	checkpointHash, hashErr := apr.config.CheckpointHash()
	if hashErr != nil {
		return hashErr
	}

	apr.Checkpointer = NewCheckpointer(apr.checkpointerCtx, apr.CheckpointID, checkpointHash, apr.blipSender, apr.config, apr.getPullStatus)

	var err error
	apr.initialStatus, err = apr.Checkpointer.fetchCheckpoints()
	base.InfofCtx(apr.ctx, base.KeyReplicate, "Initialized pull replication status: %+v", apr.initialStatus)
	if err != nil {
		return err
	}

	apr.registerCheckpointerCallbacks()
	apr.Checkpointer.Start()

	return nil
}

// GetStatus is used externally to retrieve pull replication status.  Combines current running stats with
// initialStatus.
func (apr *ActivePullReplicator) GetStatus() *ReplicationStatus {
	var lastSeqPulled string
	apr.lock.RLock()
	if apr.Checkpointer != nil {
		lastSeqPulled = apr.Checkpointer.calculateSafeProcessedSeq()
	}
	apr.lock.RUnlock()
	status := apr.getPullStatus(lastSeqPulled)
	return status
}

// getPullStatus is used internally, and passed as statusCallback to checkpointer
func (apr *ActivePullReplicator) getPullStatus(lastSeqPulled string) *ReplicationStatus {
	status := &ReplicationStatus{}
	status.Status, status.ErrorMessage = apr.getStateWithErrorMessage()

	pullStats := apr.replicationStats
	status.DocsRead = pullStats.HandleRevCount.Value()
	status.DocsCheckedPull = pullStats.HandleChangesCount.Value()
	status.DocsPurged = pullStats.HandleRevDocsPurgedCount.Value()
	status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
	status.DeltasRecv = pullStats.HandleRevDeltaRecvCount.Value()
	status.DeltasRequested = pullStats.HandleChangesDeltaRequestedCount.Value()
	status.LastSeqPull = lastSeqPulled
	if apr.initialStatus != nil {
		status.PullReplicationStatus.Add(apr.initialStatus.PullReplicationStatus)
	}
	return status
}

func (apr *ActivePullReplicator) reset() error {
	if apr.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", apr.config.ID)
	}
	if err := resetLocalCheckpoint(apr.config.ActiveDB, apr.CheckpointID); err != nil {
		return err
	}

	apr.lock.Lock()
	apr.Checkpointer = nil
	apr.lock.Unlock()
	return nil
}

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePullReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.sgr2PullAlreadyKnownSeqsCallback = apr.Checkpointer.AddAlreadyKnownSeq

	apr.blipSyncContext.sgr2PullAddExpectedSeqsCallback = apr.Checkpointer.AddExpectedSeqIDAndRevs

	apr.blipSyncContext.sgr2PullProcessedSeqCallback = apr.Checkpointer.AddProcessedSeqIDAndRev

	// Trigger complete for non-continuous replications when caught up
	if !apr.config.Continuous {
		apr.blipSyncContext.emptyChangesMessageCallback = func() {
			// Complete blocks waiting for pending rev messages, so needs
			// it's own goroutine
			go apr.Complete()
		}
	}
}

// Stop stops the pull replication and waits for the sub changes goroutine to finish.
func (apr *ActivePullReplicator) Stop() error {
	if err := apr.stopAndDisconnect(); err != nil {
		return err
	}
	teardownStart := time.Now()
	for (apr.blipSyncContext != nil && apr.blipSyncContext.activeSubChanges.IsTrue()) &&
		(time.Since(teardownStart) < time.Second*10) {
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}
