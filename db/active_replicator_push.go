package db

import (
	"context"
	"expvar"
	"fmt"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActivePushReplicator is a unidirectional push active replicator.
type ActivePushReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
	ctx             context.Context // closes checkpointer goroutine on cancel (can be a context passed from a parent DB)
	Checkpointer    *Checkpointer
	Stats           expvar.Map
}

func NewPushReplicator(ctx context.Context, config *ActiveReplicatorConfig) *ActivePushReplicator {
	blipContext := blip.NewContextCustomID(config.ID+"-push", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	return &ActivePushReplicator{
		config:          config,
		blipSyncContext: bsc,
		ctx:             ctx,
	}
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

	if err := apr.initCheckpointer(); err != nil {
		return err
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

	go bh.sendChanges(apr.blipSender, &sendChangesOptions{
		docIDs:     apr.config.DocIDs,
		since:      seq,
		continuous: apr.config.Continuous,
		activeOnly: apr.config.ActiveOnly,
		batchSize:  int(apr.config.ChangesBatchSize),
		channels:   channels,
		clientType: clientTypeSGR2,
	})

	return nil
}

func (apr *ActivePushReplicator) Close() error {
	if apr == nil {
		// noop
		return nil
	}

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

func (apr *ActivePushReplicator) initCheckpointer() error {
	checkpointID, err := apr.CheckpointID()
	if err != nil {
		return err
	}

	apr.Checkpointer = NewCheckpointer(apr.ctx, checkpointID, apr.blipSender, apr.config.CheckpointInterval)

	checkpoint := apr.Checkpointer.GetCheckpoint()
	apr.Checkpointer.lastCheckpointRevID = checkpoint.RevID
	apr.Checkpointer.lastCheckpointSeq = checkpoint.Checkpoint.LastSequence

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

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePushReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.preSendRevisionResponseCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsRequestedTotal, 1)
		apr.Checkpointer.AddExpectedSeq(remoteSeq)
	}

	apr.blipSyncContext.postSendRevisionResponseCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsSentTotal, 1)
		apr.Checkpointer.ProcessedSeq(remoteSeq)
	}
}
