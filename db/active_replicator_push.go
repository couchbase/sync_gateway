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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActivePushReplicator is a unidirectional push active replicator.
type ActivePushReplicator struct {
	*activeReplicatorCommon
}

func NewPushReplicator(config *ActiveReplicatorConfig) *ActivePushReplicator {
	apr := ActivePushReplicator{
		activeReplicatorCommon: newActiveReplicatorCommon(config, ActiveReplicatorTypePush),
	}
	apr.replicatorConnectFn = apr._connect
	return &apr
}

func (apr *ActivePushReplicator) Start() error {
	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePushReplicator, can't start")
	}

	if apr.ctx != nil && apr.ctx.Err() == nil {
		return fmt.Errorf("ActivePushReplicator already running")
	}

	apr.setState(ReplicationStateStarting)
	logCtx := context.WithValue(context.Background(), base.LogContextKey{}, base.LogContext{CorrelationID: apr.config.ID + "-" + string(ActiveReplicatorTypePush)})
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

var PreHydrogenTargetAllowConflictsError = errors.New("cannot run replication to target with allow_conflicts=false. Change to allow_conflicts=true or upgrade to 2.8")

// _connect opens up a connection, and starts replicating.
func (apr *ActivePushReplicator) _connect() error {
	var err error
	apr.blipSender, apr.blipSyncContext, err = connect(apr.activeReplicatorCommon, "-push")
	if err != nil {
		return err
	}

	// TODO: If this were made a config option, and the default conflict resolver not enforced on
	// 	the pull side, it would be feasible to run sgr-2 in 'manual conflict resolution' mode
	apr.blipSyncContext.sendRevNoConflicts = true

	// wrap the replicator context with a cancelFunc that can be called to abort the checkpointer from _disconnect
	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(apr.ctx)
	if err := apr._initCheckpointer(); err != nil {
		// clean up anything we've opened so far
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	bh := blipHandler{
		BlipSyncContext: apr.blipSyncContext,
		db:              apr.config.ActiveDB,
		serialNumber:    apr.blipSyncContext.incrementSerialNumber(),
	}

	seq, err := apr.config.ActiveDB.ParseSequenceID(apr.Checkpointer.lastCheckpointSeq)
	if err != nil {
		base.WarnfCtx(apr.ctx, "couldn't parse checkpointed sequence ID, starting push from seq:0")
	}

	var channels base.Set
	if apr.config.FilterChannels != nil {
		channels = base.SetFromArray(apr.config.FilterChannels)
	}

	apr.blipSyncContext.fatalErrorCallback = func(err error) {
		if strings.Contains(err.Error(), ErrUseProposeChanges.Message) {
			err = ErrUseProposeChanges
			_ = apr.setError(PreHydrogenTargetAllowConflictsError)
			err = apr.stopAndDisconnect()
			if err != nil {
				base.ErrorfCtx(apr.ctx, "Failed to stop and disconnect replication: %v", err)
			}
		} else if strings.Contains(err.Error(), ErrDatabaseWentAway.Message) {
			err = apr.reconnect()
			if err != nil {
				base.ErrorfCtx(apr.ctx, "Failed to reconnect replication: %v", err)
			}
		}
		// No special handling for error
	}

	apr.activeSendChanges.Set(true)
	go func(s *blip.Sender) {
		defer apr.activeSendChanges.Set(false)
		isComplete := bh.sendChanges(s, &sendChangesOptions{
			docIDs:            apr.config.DocIDs,
			since:             seq,
			continuous:        apr.config.Continuous,
			activeOnly:        apr.config.ActiveOnly,
			batchSize:         int(apr.config.ChangesBatchSize),
			revocations:       apr.config.PurgeOnRemoval,
			channels:          channels,
			clientType:        clientTypeSGR2,
			ignoreNoConflicts: true, // force the passive side to accept a "changes" message, even in no conflicts mode.
		})
		// On a normal completion, call complete for the replication
		if isComplete {
			apr.Complete()
		}
	}(apr.blipSender)

	apr.setState(ReplicationStateRunning)
	return nil
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePushReplicator) Complete() {
	apr.lock.Lock()
	if apr == nil {
		apr.lock.Unlock()
		return
	}

	// Wait for any pending changes responses to arrive and be processed
	err := apr._waitForPendingChangesResponse()
	if err != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Timeout waiting for pending changes response for replication %s - stopping: %v", apr.config.ID, err)
	}

	err = apr.Checkpointer.waitForExpectedSequences()
	if err != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
	}

	apr._stop()

	stopErr := apr._disconnect()
	if stopErr != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}
	apr.setState(ReplicationStateStopped)

	// unlock the replication before triggering callback, in case callback attempts to re-acquire the lock
	onCompleteCallback := apr.onReplicatorComplete
	apr._publishStatus()
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

func (apr *ActivePushReplicator) _initCheckpointer() error {

	checkpointHash, hashErr := apr.config.CheckpointHash()
	if hashErr != nil {
		return hashErr
	}
	apr.Checkpointer = NewCheckpointer(apr.checkpointerCtx, apr.CheckpointID, checkpointHash, apr.blipSender, apr.config, apr.getPushStatus)

	var err error
	apr.initialStatus, err = apr.Checkpointer.fetchCheckpoints()
	base.InfofCtx(apr.ctx, base.KeyReplicate, "Initialized push replication status: %+v", apr.initialStatus)
	if err != nil {
		return err
	}

	apr.registerCheckpointerCallbacks()
	apr.Checkpointer.Start()

	return nil
}

// GetStatus is used externally to retrieve pull replication status.  Combines current running stats with
// initialStatus.
func (apr *ActivePushReplicator) GetStatus() *ReplicationStatus {
	var lastSeqPushed string
	apr.lock.RLock()
	if apr.Checkpointer != nil {
		lastSeqPushed = apr.Checkpointer.calculateSafeProcessedSeq()
	}
	apr.lock.RUnlock()
	status := apr.getPushStatus(lastSeqPushed)
	return status
}

// getPullStatus is used internally, and passed as statusCallback to checkpointer
func (apr *ActivePushReplicator) getPushStatus(lastSeqPushed string) *ReplicationStatus {
	status := &ReplicationStatus{}
	status.Status, status.ErrorMessage = apr.getStateWithErrorMessage()

	pushStats := apr.replicationStats
	status.DocsWritten = pushStats.SendRevCount.Value()
	status.DocsCheckedPush = pushStats.SendChangesCount.Value()
	status.DocWriteFailures = pushStats.SendRevErrorTotal.Value()
	status.DocWriteConflict = pushStats.SendRevErrorConflictCount.Value()
	status.RejectedRemote = pushStats.SendRevErrorRejectedCount.Value()
	status.DeltasSent = pushStats.SendRevDeltaSentCount.Value()
	status.LastSeqPush = lastSeqPushed
	if apr.initialStatus != nil {
		status.PushReplicationStatus.Add(apr.initialStatus.PushReplicationStatus)
	}
	return status
}

// reset performs a reset on the replication by removing the local checkpoint document.
func (apr *ActivePushReplicator) reset() error {
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
func (apr *ActivePushReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.sgr2PushAlreadyKnownSeqsCallback = apr.Checkpointer.AddAlreadyKnownSeq

	apr.blipSyncContext.sgr2PushAddExpectedSeqsCallback = apr.Checkpointer.AddExpectedSeqs

	apr.blipSyncContext.sgr2PushProcessedSeqCallback = apr.Checkpointer.AddProcessedSeq
}

// waitForExpectedSequences waits for the pending changes response count
// to drain to zero.  Intended to be used once the replication has been stopped, to wait for
// in-flight changes responses to arrive.
// Waits up to 10s, polling every 100ms.
func (apr *ActivePushReplicator) _waitForPendingChangesResponse() error {
	waitCount := 0
	for waitCount < 100 {
		if apr.blipSyncContext == nil {
			return nil
		}
		pendingCount := atomic.LoadInt64(&apr.blipSyncContext.changesPendingResponseCount)
		if pendingCount <= 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
		waitCount++
	}
	return errors.New("checkpointer _waitForPendingChangesResponse failed to complete after waiting 10s")
}

// Stop stops the push replication and waits for the send changes goroutine to finish.
func (apr *ActivePushReplicator) Stop() error {
	if err := apr.stopAndDisconnect(); err != nil {
		return err
	}
	teardownStart := time.Now()
	for apr.activeSendChanges.IsTrue() && (time.Since(teardownStart) < time.Second*10) {
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}
