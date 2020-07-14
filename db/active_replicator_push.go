package db

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// ActivePushReplicator is a unidirectional push active replicator.
type ActivePushReplicator struct {
	activeReplicatorCommon
}

func NewPushReplicator(config *ActiveReplicatorConfig) *ActivePushReplicator {
	return &ActivePushReplicator{
		activeReplicatorCommon: activeReplicatorCommon{
			config:           config,
			replicationStats: NewBlipSyncStats(),
			state:            ReplicationStateStopped,
		},
	}
}

func (apr *ActivePushReplicator) Start() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePushReplicator, can't start")
	}

	if apr.checkpointerCtx != nil {
		return fmt.Errorf("ActivePushReplicator already running")
	}

	var err error
	apr.blipSender, apr.blipSyncContext, err = connect("-push", apr.config, apr.replicationStats)
	if err != nil {
		return apr._setError(err)
	}

	// TODO: If this were made a config option, and the default conflict resolver not enforced on
	// 	the pull side, it would be feasible to run sgr-2 in 'manual conflict resolution' mode
	apr.blipSyncContext.sendRevNoConflicts = true

	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(context.Background())
	if err := apr._initCheckpointer(); err != nil {
		return apr._setError(err)
	}

	bh := blipHandler{
		BlipSyncContext: apr.blipSyncContext,
		db:              apr.config.ActiveDB,
		serialNumber:    apr.blipSyncContext.incrementSerialNumber(),
	}

	seq, err := apr.config.ActiveDB.ParseSequenceID(apr.Checkpointer.lastCheckpointSeq)
	if err != nil {
		base.Warnf("couldn't parse checkpointed sequence ID, starting push from seq:0")
	}

	var channels base.Set
	if apr.config.FilterChannels != nil {
		channels = base.SetFromArray(apr.config.FilterChannels)
	}

	go func() {
		bh.sendChanges(apr.blipSender, &sendChangesOptions{
			docIDs:     apr.config.DocIDs,
			since:      seq,
			continuous: apr.config.Continuous,
			activeOnly: apr.config.ActiveOnly,
			batchSize:  int(apr.config.ChangesBatchSize),
			channels:   channels,
			clientType: clientTypeSGR2,
		})
		apr.Complete()
	}()

	apr._setState(ReplicationStateRunning)
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
	err := apr.waitForPendingChangesResponse()
	if err != nil {
		base.Infof(base.KeyReplicate, "Timeout waiting for pending changes response for replication %s - stopping: %v", apr.config.ID, err)
	}

	err = apr.Checkpointer.waitForExpectedSequences()
	if err != nil {
		base.Infof(base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
	}

	stopErr := apr._stop()
	if stopErr != nil {
		base.Infof(base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}

	// unlock the replication before triggering callback, in case callback attempts to re-acquire the lock
	onCompleteCallback := apr.onReplicatorComplete
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

func (apr *ActivePushReplicator) Stop() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()
	return apr._stop()
}

func (apr *ActivePushReplicator) _stop() error {
	if apr == nil {
		// noop
		return nil
	}

	if apr.checkpointerCtx != nil {
		apr.checkpointerCtxCancel()
		apr.Checkpointer.CheckpointNow()
	}
	apr.checkpointerCtx = nil

	if apr.blipSender != nil {
		apr.blipSender.Close()
		apr.blipSender = nil
	}

	if apr.blipSyncContext != nil {
		apr.blipSyncContext.Close()
		apr.blipSyncContext = nil
	}
	apr._setState(ReplicationStateStopped)

	return nil
}

func (apr *ActivePushReplicator) _initCheckpointer() error {
	checkpointID, err := apr.CheckpointID()
	if err != nil {
		return err
	}

	apr.Checkpointer = NewCheckpointer(apr.checkpointerCtx, checkpointID, apr.blipSender, apr.config.ActiveDB, apr.config.CheckpointInterval)

	err = apr.Checkpointer.fetchCheckpoints()
	if err != nil {
		return err
	}

	apr.registerCheckpointerCallbacks()
	apr.Checkpointer.Start()

	return nil
}

// CheckpointID returns a unique ID to be used for the checkpoint client (which is used as part of the checkpoint Doc ID on the recipient)
func (apr *ActivePushReplicator) CheckpointID() (string, error) {
	checkpointHash, err := apr.config.CheckpointHash()
	if err != nil {
		return "", err
	}
	return "sgr2cp:push:" + checkpointHash, nil
}

// reset performs a reset on the replication by removing the local checkpoint document.
func (apr *ActivePushReplicator) reset() error {
	if apr.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", apr.config.ID)
	}
	checkpointID, err := apr.CheckpointID()
	if err != nil {
		return err
	}
	return resetLocalCheckpoint(apr.config.ActiveDB, checkpointID)
}

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePushReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.sgr2PushIgnoreSeqCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyPushIgnoredSeqsTotal, 1)
		apr.Checkpointer.AddExpectedSeq(remoteSeq)
		apr.Checkpointer.ProcessedSeq(remoteSeq)
	}
	apr.blipSyncContext.sgr2PushAddExpectedSeqCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyPushExpectedSeqsTotal, 1)
		apr.Checkpointer.AddExpectedSeq(remoteSeq)
	}

	apr.blipSyncContext.sgr2PushProcessedSeqCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyPushProcessedSeqsTotal, 1)
		apr.Checkpointer.ProcessedSeq(remoteSeq)
	}
}

// waitForExpectedSequences waits for the pending changes response count
// to drain to zero.  Intended to be used once the replication has been stopped, to wait for
// in-flight changes responses to arrive.
// Waits up to 10s, polling every 100ms.
func (apr *ActivePushReplicator) waitForPendingChangesResponse() error {
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
	return errors.New("checkpointer waitForPendingChangesResponse failed to complete after waiting 10s")
}
