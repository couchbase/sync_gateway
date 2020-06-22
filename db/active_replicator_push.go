package db

import (
	"context"
	"fmt"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActivePushReplicator is a unidirectional push active replicator.
type ActivePushReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
}

func NewPushReplicator(ctx context.Context, config *ActiveReplicatorConfig) *ActivePushReplicator {
	blipContext := blip.NewContextCustomID(config.ID+"-push", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	return &ActivePushReplicator{
		config:          config,
		blipSyncContext: bsc,
	}
}

// CheckpointID returns a unique ID to be used for the checkpoint client (which is used as part of the checkpoint Doc ID on the recipient)
func (apr *ActivePushReplicator) CheckpointID() (string, error) {
	checkpointHash, err := apr.config.CheckpointHash()
	if err != nil {
		return "", err
	}
	return "sgr2cp:push:" + checkpointHash, nil
}

// CheckpointNow forces the checkpointer to send a checkpoint, and blocks until it has finished.
func (apr *ActivePushReplicator) CheckpointNow() {
	// TODO: Implement push checkpointing (CBG-916)
	return
}

func (apr *ActivePushReplicator) Close() error {
	if apr == nil {
		// noop
		return nil
	}

	apr.CheckpointNow()

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

// getCheckpoint tries to fetch a since value for the given replication by requesting a checkpoint.
// If this fails, the function returns a zero value.
func (apr *ActivePushReplicator) getCheckpoint() GetSGR2CheckpointResponse {
	// TODO: Implement push checkpointing (CBG-916)
	return GetSGR2CheckpointResponse{}
}

func (apr *ActivePushReplicator) setCheckpoint(seq string) {
	// TODO: Implement push checkpointing (CBG-916)
	return
}

func (apr *ActivePushReplicator) Start() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePushReplicator, can't start")
	}

	var err error
	apr.blipSender, apr.blipSyncContext, err = connect("-push", apr.config)
	if err != nil {
		return err
	}

	checkpoint := apr.getCheckpoint()

	if err := apr.startCheckpointer(); err != nil {
		return err
	}

	bh := blipHandler{
		BlipSyncContext: apr.blipSyncContext,
		db:              apr.config.ActiveDB,
		serialNumber:    apr.blipSyncContext.incrementSerialNumber(),
	}

	// TODO: set bh.channels and these other options via sendChanges parameters (CBG-915)
	bh.continuous = apr.config.Continuous
	bh.batchSize = int(apr.config.ChangesBatchSize)
	bh.activeOnly = apr.config.ActiveOnly

	seq, err := apr.config.ActiveDB.ParseSequenceID(checkpoint.Checkpoint.LastSequence)
	if err != nil {
		base.Warnf("couldn't parse checkpointed sequence ID, starting push from seq:0")
	}

	go bh.sendChanges(apr.blipSender, &SubChangesParams{
		rq:      nil,
		_since:  seq,
		_docIDs: apr.config.DocIDs,
	})

	return nil
}

// startCheckpointer registers appropriate callback functions for checkpointing, and starts a time-based checkpointer goroutine.
func (apr *ActivePushReplicator) startCheckpointer() error {
	// TODO: Implement push checkpointing (CBG-916)
	return nil
}
