package db

import (
	"context"
	"expvar"
	"fmt"

	"github.com/couchbase/go-blip"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	config                *ActiveReplicatorConfig
	blipSyncContext       *BlipSyncContext
	blipSender            *blip.Sender
	Stats                 expvar.Map
	Checkpointer          *Checkpointer
	checkpointerCtx       context.Context
	checkpointerCtxCancel context.CancelFunc
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	return &ActivePullReplicator{
		config:                config,
		checkpointerCtx:       ctx,
		checkpointerCtxCancel: ctxCancelFn,
	}
}

func (apr *ActivePullReplicator) Start() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	var err error

	apr.blipSender, apr.blipSyncContext, err = connect("-pull", apr.config)
	if err != nil {
		return err
	}

	apr.blipSyncContext.conflictResolver = apr.config.ConflictResolver
	apr.blipSyncContext.purgeOnRemoval = apr.config.PurgeOnRemoval

	if err := apr.initCheckpointer(); err != nil {
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
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		return err
	}

	return nil
}

func (apr *ActivePullReplicator) Close() error {
	if apr == nil {
		// noop
		return nil
	}

	apr.checkpointerCtxCancel()
	apr.Checkpointer.CheckpointNow()

	if apr.blipSender != nil {
		apr.blipSender.Close()
		apr.blipSender = nil
	}

	if apr.blipSyncContext != nil {
		apr.blipSyncContext.Close()
		apr.blipSyncContext = nil
	}

	return nil
}

func (apr *ActivePullReplicator) initCheckpointer() error {
	checkpointID, err := apr.CheckpointID()
	if err != nil {
		return err
	}

	apr.Checkpointer = NewCheckpointer(apr.checkpointerCtx, checkpointID, apr.blipSender, apr.config.CheckpointInterval)

	checkpoint := apr.Checkpointer.GetCheckpoint()
	apr.Checkpointer.lastCheckpointRevID = checkpoint.RevID
	apr.Checkpointer.lastCheckpointSeq = checkpoint.Checkpoint.LastSequence

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

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePullReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.postHandleChangesCallback = func(changesSeqs []string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyChangesRevsReceivedTotal, int64(len(changesSeqs)))
		apr.Checkpointer.AddExpectedSeq(changesSeqs...)
	}

	// TODO: Check whether we need to add a handleNoRev callback to remove expected sequences.
	apr.blipSyncContext.postHandleRevCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsReceivedTotal, 1)
		apr.Checkpointer.ProcessedSeq(remoteSeq)
	}
}
