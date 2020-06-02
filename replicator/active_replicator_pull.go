package replicator

import (
	"fmt"
	"sync/atomic"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
	checkpointRevID string // checkpointRev is the last known checkpoint RevID.
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	blipContext := blip.NewContextCustomID(config.ID+"-pull", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	return &ActivePullReplicator{
		config:          config,
		blipSyncContext: bsc,
	}
}

func (apr *ActivePullReplicator) connect() (err error) {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't connect")
	}

	if apr.blipSender != nil {
		return fmt.Errorf("replicator already has a blipSender, can't connect twice")
	}

	blipContext := blip.NewContextCustomID(apr.config.ID+"-pull", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, apr.config.ActiveDB, blipContext.ID)
	apr.blipSyncContext = bsc

	apr.blipSender, err = blipSync(*apr.config.PassiveDBURL, apr.blipSyncContext.blipContext)
	if err != nil {
		return err
	}

	return nil
}

func (apr *ActivePullReplicator) Close() error {
	if apr == nil {
		// noop
		return nil
	}

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
func (apr *ActivePullReplicator) getCheckpoint() GetSGR2CheckpointResponse {
	// TODO: apr.config.CheckpointHash() ?
	client := apr.blipSyncContext.blipContext.ID

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

	if resp == nil {
		base.Debugf(base.KeyReplicate, "couldn't find existing checkpoint for client %q, starting from 0", client)
		return GetSGR2CheckpointResponse{}
	}

	return *resp
}

func (apr *ActivePullReplicator) setCheckpoint(seq uint64, revID string) {

}

func (apr *ActivePullReplicator) Start() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if err := apr.connect(); err != nil {
		return err
	}

	checkpoint := apr.getCheckpoint()
	apr.checkpointRevID = checkpoint.RevID

	subChangesRequest := SubChangesRequest{
		Continuous:     apr.config.Continuous,
		Batch:          apr.config.ChangesBatchSize,
		Since:          checkpoint.Checkpoint.LastSequence,
		Filter:         apr.config.Filter,
		FilterChannels: apr.config.FilterChannels,
		DocIDs:         apr.config.DocIDs,
		ActiveOnly:     apr.config.ActiveOnly,
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		return err
	}

	if apr.config.CheckpointInterval > 0 {
		// keep track of how many revs we've finished processing to know when to set checkpoints.
		var revsHandled uint64
		apr.blipSyncContext.postHandleRevCallback = func() {
			if val := atomic.AddUint64(&revsHandled, 1); val%uint64(apr.config.CheckpointInterval) != 0 {
				// not at a checkpoint interval, noop
				return
			}

			rq := SetSGR2CheckpointRequest{
				// TODO: apr.config.CheckpointHash() ?
				Client: apr.blipSyncContext.blipContext.ID,
				Checkpoint: SGR2Checkpoint{
					// TODO: use actual value
					LastSequence: db.SequenceID{Seq: 10},
				},
			}
			if apr.checkpointRevID != "" {
				rq.RevID = &apr.checkpointRevID
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
			apr.checkpointRevID = resp.RevID
		}
	}

	return nil
}
