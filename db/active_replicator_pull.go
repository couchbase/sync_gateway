package db

import (
	"context"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	activeReplicatorCommon
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	return &ActivePullReplicator{
		activeReplicatorCommon: activeReplicatorCommon{
			config:           config,
			replicationStats: NewBlipSyncStats(),
			state:            ReplicationStateStopped,
		},
	}
}

func (apr *ActivePullReplicator) Start() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if apr.checkpointerCtx != nil {
		return fmt.Errorf("ActivePullReplicator already running")
	}

	var err error
	apr.blipSender, apr.blipSyncContext, err = connect("-pull", apr.config, apr.replicationStats)
	if err != nil {
		return apr._setError(err)
	}

	apr.blipSyncContext.conflictResolver = apr.config.ConflictResolver
	apr.blipSyncContext.purgeOnRemoval = apr.config.PurgeOnRemoval

	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(context.Background())
	if err := apr._initCheckpointer(); err != nil {
		apr.checkpointerCtx = nil
		return apr._setError(err)
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
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		apr.checkpointerCtxCancel()
		apr.checkpointerCtx = nil
		return apr._setError(err)
	}

	apr._setState(ReplicationStateRunning)
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
		base.Infof(base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
	}

	stopErr := apr._stop()
	if stopErr != nil {
		base.Infof(base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}

	// unlock the replication before triggering callback, in case callback attempts to access replication information
	// from the replicator
	onCompleteCallback := apr.onReplicatorComplete
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

func (apr *ActivePullReplicator) Stop() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()
	return apr._stop()
}

func (apr *ActivePullReplicator) _stop() error {
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

func (apr *ActivePullReplicator) _initCheckpointer() error {
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
func (apr *ActivePullReplicator) CheckpointID() (string, error) {
	checkpointHash, err := apr.config.CheckpointHash()
	if err != nil {
		return "", err
	}
	return "sgr2cp:pull:" + checkpointHash, nil
}

func (apr *ActivePullReplicator) reset() error {
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
func (apr *ActivePullReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.sgr2PullIgnoreSeqCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyPullIgnoredSeqsTotal, 1)
		apr.Checkpointer.AddExpectedSeq(remoteSeq)
		apr.Checkpointer.ProcessedSeq(remoteSeq)
	}

	apr.blipSyncContext.sgr2PullAddExepectedSeqsCallback = func(changesSeqs []string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyPullExpectedSeqsTotal, int64(len(changesSeqs)))
		apr.Checkpointer.AddExpectedSeq(changesSeqs...)
	}

	// TODO: Check whether we need to add a handleNoRev callback to remove expected sequences.
	apr.blipSyncContext.sgr2PullProcessedSeqCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyPullProcessedSeqsTotal, 1)
		apr.Checkpointer.ProcessedSeq(remoteSeq)
	}

	// Trigger complete for non-continuous replications when caught up
	if !apr.config.Continuous {
		apr.blipSyncContext.emptyChangesMessageCallback = func() {
			// Complete blocks waiting for pending rev messages, so needs
			// it's own goroutine
			go apr.Complete()
		}
	}

}
