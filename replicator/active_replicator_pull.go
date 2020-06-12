package replicator

import (
	"context"
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
	ctx             context.Context
	checkpointRevID string // checkpointRev is the last known checkpoint RevID.
	checkpointerWg  sync.WaitGroup
	Stats           expvar.Map
}

func NewPullReplicator(ctx context.Context, config *ActiveReplicatorConfig) *ActivePullReplicator {
	blipContext := blip.NewContextCustomID(config.ID+"-pull", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	return &ActivePullReplicator{
		config:          config,
		blipSyncContext: bsc,
		ctx:             ctx,
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

	apr.checkpointerWg.Wait()

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
		apr.Stats.Add(ActiveReplicatorStatsKeyGetCheckpointMissTotal, 1)
		return GetSGR2CheckpointResponse{}
	}

	apr.Stats.Add(ActiveReplicatorStatsKeyGetCheckpointHitTotal, 1)

	return *resp
}

func (apr *ActivePullReplicator) setCheckpoint(seq db.SequenceID) {
	rq := SetSGR2CheckpointRequest{
		// TODO: apr.config.CheckpointHash() ?
		Client: apr.blipSyncContext.blipContext.ID,
		Checkpoint: SGR2Checkpoint{
			LastSequence: seq,
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

	apr.Stats.Add(ActiveReplicatorStatsKeySetCheckpointTotal, 1)

	apr.checkpointRevID = resp.RevID
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

	if err := apr.startCheckpointer(); err != nil {
		return err
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		return err
	}

	return nil
}

// startCheckpointer registers appropriate callback functions for checkpointing, and starts a time-based checkpointer goroutine.
func (apr *ActivePullReplicator) startCheckpointer() error {
	if apr.config.CheckpointMinInterval == 0 {
		apr.config.CheckpointMinInterval = time.Second * 10
	}
	if apr.config.CheckpointMaxInterval == 0 {
		apr.config.CheckpointMaxInterval = time.Second * 30
	}
	if apr.config.CheckpointRevCount == 0 {
		apr.config.CheckpointRevCount = apr.config.ChangesBatchSize
	}

	// checkpointerLock guards the expectedSeqs slice, and receivedSeqs map
	checkpointerLock := sync.Mutex{}

	// An ordered list of sequence IDs we expect to receive revs for.
	expectedSeqs := []db.SequenceID{}
	// A map of sequence IDs we've received revs for.
	receivedSeqs := map[db.SequenceID]struct{}{}

	checkpointNow := make(chan struct{})

	apr.blipSyncContext.postHandleChangesCallback = func(changesSeqs []db.SequenceID) {
		apr.Stats.Add(ActiveReplicatorStatsKeyChangesRevsReceivedTotal, int64(len(changesSeqs)))

		if len(changesSeqs) == 0 {
			// nothing to do
			return
		}

		select {
		case <-apr.ctx.Done():
			// replicator already closed, bail out of checkpointing work
			return
		default:
		}

		checkpointerLock.Lock()
		expectedSeqs = append(expectedSeqs, changesSeqs...)
		checkpointerLock.Unlock()
	}

	revCounter := uint16(0)
	apr.blipSyncContext.postHandleRevCallback = func(remoteSeq db.SequenceID) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsReceivedTotal, 1)

		select {
		case <-apr.ctx.Done():
			// replicator already closed, bail out of checkpointing work
			return
		default:
		}

		checkpointerLock.Lock()
		revCounter++
		receivedSeqs[remoteSeq] = struct{}{}

		shouldCheckpoint := revCounter >= apr.config.CheckpointRevCount
		if !shouldCheckpoint {
			checkpointerLock.Unlock()
			return
		}

		revCounter = uint16(0)
		checkpointerLock.Unlock()

		select {
		case checkpointNow <- struct{}{}:
		case <-time.After(apr.config.CheckpointMinInterval):
			// Blocked trying to send a checkpointNow signal
			base.Tracef(base.KeyReplicate, "checkpointer: skipping sending blocked checkpointNow signal")
		}
	}

	// Start a time-based checkpointer goroutine
	go func() {
		timeout := time.NewTimer(apr.config.CheckpointMaxInterval)
		defer timeout.Stop()
		for {
			select {
			case <-checkpointNow:
				base.Tracef(base.KeyReplicate, "checkpointer: got checkpointNow signal")
				// fall out of select to set the checkpoint
			case <-timeout.C:
				base.Tracef(base.KeyReplicate, "checkpointer: got timeout signal")
				// timed out waiting for number of revs to reach CheckpointRevCount
				// fall out of select to set the checkpoint
			case <-apr.ctx.Done():
				// replicator stopped, set a final checkpoint
			}

			apr.checkpointerWg.Add(1)
			if !timeout.Stop() {
				<-timeout.C
			}

			checkpointerLock.Lock()

			// find the highest contiguous sequence we've received
			if len(expectedSeqs) > 0 {
				var lowSeq db.SequenceID

				// iterates over each (ordered) expected sequence and stops when we find the first sequence we've yet to receive a rev message for
				var maxI int
				for i, seq := range expectedSeqs {
					if _, ok := receivedSeqs[seq]; !ok {
						base.Tracef(base.KeyReplicate, "checkpointer: couldn't find %v in receivedSeqs", seq)
						break
					}

					delete(receivedSeqs, seq)
					maxI = i
				}

				lowSeq = expectedSeqs[maxI]

				if len(expectedSeqs)-1 == maxI {
					// received full set, empty list
					expectedSeqs = expectedSeqs[0:0]
				} else {
					// trim sequence list for partially received set
					expectedSeqs = expectedSeqs[maxI+1:]
				}

				base.Tracef(base.KeyReplicate, "checkpointer got lowSeq: %v", lowSeq)
				apr.setCheckpoint(lowSeq)
			}

			checkpointerLock.Unlock()
			timeout.Reset(apr.config.CheckpointMinInterval)
			apr.checkpointerWg.Done()
		}
	}()

	return nil
}
