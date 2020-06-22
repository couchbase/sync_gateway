package db

import (
	"context"
	"expvar"
	"fmt"
	"sync"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActivePushReplicator is a unidirectional push active replicator.
type ActivePushReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
	ctx             context.Context // closes checkpointer goroutine on cancel (can be a context passed from a parent DB)
	Checkpointer    *PushCheckpointer
	Stats           expvar.Map
}

type PushCheckpointer struct {
	// lastCheckpointRevID is the last known checkpoint RevID.
	lastCheckpointRevID string
	// lastCheckpointSeq is the last checkpointed sequence
	lastCheckpointSeq string
	// runNow can be sent signals to run a checkpoint (useful for testing)
	runNow chan struct{}
	// wg is used to block close until a checkpoint has finished
	wg sync.WaitGroup
	// lock guards the expectedSeqs slice, and receivedSeqs map
	lock sync.Mutex
	// expectedSeqs is an ordered list of sequence IDs we expect to receive revs for
	expectedSeqs []string
	// receivedSeqs is a map of sequence IDs we've received revs for
	receivedSeqs map[string]struct{}
}

func NewPushCheckpointer() *PushCheckpointer {
	return &PushCheckpointer{
		lock:         sync.Mutex{},
		expectedSeqs: make([]string, 0),
		receivedSeqs: make(map[string]struct{}),
		runNow:       make(chan struct{}),
	}
}

func NewPushReplicator(ctx context.Context, config *ActiveReplicatorConfig) *ActivePushReplicator {
	blipContext := blip.NewContextCustomID(config.ID+"-push", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	return &ActivePushReplicator{
		config:          config,
		blipSyncContext: bsc,
		ctx:             ctx,
		Checkpointer:    NewPushCheckpointer(),
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

func (apr *ActivePushReplicator) connect() (err error) {
	if apr == nil {
		return fmt.Errorf("nil ActivePushReplicator, can't connect")
	}

	if apr.blipSender != nil {
		return fmt.Errorf("replicator already has a blipSender, can't connect twice")
	}

	blipContext := blip.NewContextCustomID(apr.config.ID+"-push", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, apr.config.ActiveDB, blipContext.ID)
	apr.blipSyncContext = bsc

	apr.blipSender, err = blipSync(*apr.config.PassiveDBURL, apr.blipSyncContext.blipContext)
	if err != nil {
		return err
	}

	return nil
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

	if err := apr.connect(); err != nil {
		return err
	}

	checkpoint := apr.getCheckpoint()
	apr.Checkpointer.lastCheckpointRevID = checkpoint.RevID
	apr.Checkpointer.lastCheckpointSeq = checkpoint.Checkpoint.LastSequence

	if err := apr.startCheckpointer(); err != nil {
		return err
	}

	bh := blipHandler{
		BlipSyncContext: apr.blipSyncContext,
		db:              apr.config.ActiveDB,
		serialNumber:    apr.blipSyncContext.incrementSerialNumber(),
	}

	bh.continuous = apr.config.Continuous
	bh.batchSize = int(apr.config.ChangesBatchSize)
	bh.activeOnly = apr.config.ActiveOnly

	// TODO: set bh.channels and other options (CBG-915)

	seq, err := apr.config.ActiveDB.ParseSequenceID(checkpoint.Checkpoint.LastSequence)
	if err != nil {
		base.Warnf("couldn't parse checkpointed sequence ID, starting push from seq:0")
	}

	bh.sendChanges(apr.blipSender, &SubChangesParams{
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
