package replicator

import (
	"context"
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
	ctx             context.Context // closes checkpointer goroutine on cancel (can be a context passed from a parent DB)
	Checkpointer    *PullCheckpointer
	Stats           expvar.Map
}

type PullCheckpointer struct {
	// lastCheckpointRevID is the last known checkpoint RevID.
	lastCheckpointRevID string
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

func NewPullCheckpointer() *PullCheckpointer {
	return &PullCheckpointer{
		lock:         sync.Mutex{},
		expectedSeqs: make([]string, 0),
		receivedSeqs: make(map[string]struct{}),
		runNow:       make(chan struct{}),
	}
}

func NewPullReplicator(ctx context.Context, config *ActiveReplicatorConfig) *ActivePullReplicator {
	blipContext := blip.NewContextCustomID(config.ID+"-pull", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	return &ActivePullReplicator{
		config:          config,
		blipSyncContext: bsc,
		ctx:             ctx,
		Checkpointer:    NewPullCheckpointer(),
	}
}

// CheckpointID returns a unique ID to be used for the checkpoint client (which is used as part of the checkpoint Doc ID on the recipient)
func (apr *ActivePullReplicator) CheckpointID() (string, error) {
	checkpointHash, err := apr.config.CheckpointHash()
	if err != nil {
		return "", err
	}
	return "sgr2cp:pull:" + checkpointHash, nil
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

// CheckpointNow forces the checkpointer to send a checkpoint, and blocks until it has finished.
func (apr *ActivePullReplicator) CheckpointNow() {
	if apr.Checkpointer == nil {
		return
	}

	apr.Checkpointer.lock.Lock()
	defer apr.Checkpointer.lock.Unlock()

	base.Tracef(base.KeyReplicate, "checkpointer: running")

	// find the highest contiguous sequence we've received
	if len(apr.Checkpointer.expectedSeqs) > 0 {
		var lowSeq string

		// iterates over each (ordered) expected sequence and stops when we find the first sequence we've yet to receive a rev message for
		maxI := -1
		for i, seq := range apr.Checkpointer.expectedSeqs {
			if _, ok := apr.Checkpointer.receivedSeqs[seq]; !ok {
				base.Tracef(base.KeyReplicate, "checkpointer: couldn't find %v in receivedSeqs", seq)
				break
			}

			delete(apr.Checkpointer.receivedSeqs, seq)
			maxI = i
		}

		// the first seq we expected hasn't arrived yet, so can't checkpoint anything
		if maxI < 0 {
			return
		}

		lowSeq = apr.Checkpointer.expectedSeqs[maxI]

		if len(apr.Checkpointer.expectedSeqs)-1 == maxI {
			// received full set, empty list
			apr.Checkpointer.expectedSeqs = apr.Checkpointer.expectedSeqs[0:0]
		} else {
			// trim sequence list for partially received set
			apr.Checkpointer.expectedSeqs = apr.Checkpointer.expectedSeqs[maxI+1:]
		}

		base.Tracef(base.KeyReplicate, "checkpointer: got lowSeq: %v", lowSeq)
		apr.setCheckpoint(lowSeq)
	}

}

func (apr *ActivePullReplicator) Close() error {
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
func (apr *ActivePullReplicator) getCheckpoint() GetSGR2CheckpointResponse {
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

func (apr *ActivePullReplicator) setCheckpoint(seq string) {
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
}

func (apr *ActivePullReplicator) Start() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if err := apr.connect(); err != nil {
		return err
	}

	checkpoint := apr.getCheckpoint()
	apr.Checkpointer.lastCheckpointRevID = checkpoint.RevID

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
	apr.blipSyncContext.postHandleChangesCallback = func(changesSeqs []string) {
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

		apr.Checkpointer.lock.Lock()
		apr.Checkpointer.expectedSeqs = append(apr.Checkpointer.expectedSeqs, changesSeqs...)
		apr.Checkpointer.lock.Unlock()
	}

	// TODO: Check whether we need to add a handleNoRev callback to remove expected sequences.
	apr.blipSyncContext.postHandleRevCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsReceivedTotal, 1)

		select {
		case <-apr.ctx.Done():
			// replicator already closed, bail out of checkpointing work
			return
		default:
		}

		apr.Checkpointer.lock.Lock()
		apr.Checkpointer.receivedSeqs[remoteSeq] = struct{}{}
		apr.Checkpointer.lock.Unlock()
	}

	// Start a time-based checkpointer goroutine
	go func() {
		var exit bool
		ticker := time.NewTicker(apr.config.CheckpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-apr.ctx.Done():
				exit = true
				// parent context stopped stopped, set a final checkpoint before stopping this goroutine
			}

			apr.CheckpointNow()

			if exit {
				base.Debugf(base.KeyReplicate, "checkpointer goroutine stopped")
				return
			}
		}
	}()

	return nil
}
