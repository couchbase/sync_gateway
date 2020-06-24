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

// CheckpointID returns a unique ID to be used for the checkpoint client (which is used as part of the checkpoint Doc ID on the recipient)
func (apr *ActivePushReplicator) CheckpointID() (string, error) {
	checkpointHash, err := apr.config.CheckpointHash()
	if err != nil {
		return "", err
	}
	return "sgr2cp:push:" + checkpointHash, nil
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

// getCheckpoint tries to fetch a since value for the given replication by requesting a checkpoint.
// If this fails, the function returns a zero value.
func (apr *ActivePushReplicator) getCheckpoint() GetSGR2CheckpointResponse {
	client, err := apr.CheckpointID()
	if err != nil {
		base.Warnf("couldn't generate CheckpointID for config, starting from 0: %v", err)
		return GetSGR2CheckpointResponse{}
	}

	rq := GetSGR2CheckpointRequest{
		Client: client,
	}

	if err := rq.Send(apr.blipSender); err != nil {
		base.Warnf("couldn't send GetCheckpoint request, starting from 0: %v", err)
		return GetSGR2CheckpointResponse{}
	}

	resp, err := rq.Response()
	if err != nil {
		base.Warnf("couldn't get response for GetCheckpoint request, starting from 0: %v", err)
		return GetSGR2CheckpointResponse{}
	}

	// checkpoint wasn't found (404)
	if resp == nil {
		base.Debugf(base.KeyReplicate, "couldn't find existing checkpoint for client %q, starting from 0", client)
		apr.Stats.Add(ActiveReplicatorStatsKeyGetCheckpointMissTotal, 1)
		return GetSGR2CheckpointResponse{}
	}

	apr.Stats.Add(ActiveReplicatorStatsKeyGetCheckpointHitTotal, 1)

	return *resp
}

func (apr *ActivePushReplicator) setCheckpoint(seq string) {
	client, err := apr.CheckpointID()
	if err != nil {
		base.Warnf("couldn't generate CheckpointID for config to send checkpoint: %v", err)
		return
	}

	rq := SetSGR2CheckpointRequest{
		Client: client,
		Checkpoint: SGR2Checkpoint{
			LastSequence: seq,
		},
	}
	if apr.Checkpointer.lastCheckpointRevID != "" {
		rq.RevID = &apr.Checkpointer.lastCheckpointRevID
	}

	if err := rq.Send(apr.blipSender); err != nil {
		base.Warnf("couldn't send SetCheckpoint request: %v", err)
		return
	}

	resp, err := rq.Response()
	if err != nil {
		base.Warnf("couldn't get response for SetCheckpoint request: %v", err)
		return
	}

	apr.Stats.Add(ActiveReplicatorStatsKeySetCheckpointTotal, 1)

	apr.Checkpointer.lastCheckpointRevID = resp.RevID
	apr.Checkpointer.lastCheckpointSeq = seq
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

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePushReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.preSendRevisionCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsRequestedTotal, 1)

		select {
		case <-apr.ctx.Done():
			// replicator already closed, bail out of checkpointing work
			return
		default:
		}

		apr.Checkpointer.lock.Lock()
		apr.Checkpointer.processedSeqs[remoteSeq] = struct{}{}
		apr.Checkpointer.lock.Unlock()
	}

	apr.blipSyncContext.postSendRevisionCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsSentTotal, 1)

		select {
		case <-apr.ctx.Done():
			// replicator already closed, bail out of checkpointing work
			return
		default:
		}

		apr.Checkpointer.lock.Lock()
		apr.Checkpointer.expectedSeqs = append(apr.Checkpointer.expectedSeqs, remoteSeq)
		apr.Checkpointer.lock.Unlock()
	}
}
