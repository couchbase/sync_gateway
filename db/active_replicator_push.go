package db

import (
	"context"
	"expvar"
	"fmt"
	"sync"
	"time"

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
	// lock guards the expectedSeqs slice, and sentSeqs map
	lock sync.Mutex
	// expectedSeqs is an ordered list of sequence IDs we expect to receive revs for
	expectedSeqs []string
	// sentSeqs is a map of sequence IDs we've sent revs for
	sentSeqs map[string]struct{}
}

func NewPushCheckpointer() *PushCheckpointer {
	return &PushCheckpointer{
		lock:         sync.Mutex{},
		expectedSeqs: make([]string, 0),
		sentSeqs:     make(map[string]struct{}),
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

// CheckpointNow forces the checkpointer to send a checkpoint, and blocks until it has finished.
func (apr *ActivePushReplicator) CheckpointNow() {
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
			if _, ok := apr.Checkpointer.sentSeqs[seq]; !ok {
				base.Tracef(base.KeyReplicate, "checkpointer: couldn't find %v in receivedSeqs", seq)
				break
			}

			delete(apr.Checkpointer.sentSeqs, seq)
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

	seq, err := apr.config.ActiveDB.ParseSequenceID(checkpoint.Checkpoint.LastSequence)
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

// startCheckpointer registers appropriate callback functions for checkpointing, and starts a time-based checkpointer goroutine.
func (apr *ActivePushReplicator) startCheckpointer() error {
	apr.blipSyncContext.preSendRevisionCallback = func(remoteSeq string) {
		apr.Stats.Add(ActiveReplicatorStatsKeyRevsRequestedTotal, 1)

		select {
		case <-apr.ctx.Done():
			// replicator already closed, bail out of checkpointing work
			return
		default:
		}

		apr.Checkpointer.lock.Lock()
		apr.Checkpointer.sentSeqs[remoteSeq] = struct{}{}
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

	// Start a time-based checkpointer goroutine
	go func() {
		var exit bool
		checkpointInterval := defaultCheckpointInterval
		if apr.config.CheckpointInterval > 0 {
			checkpointInterval = apr.config.CheckpointInterval
		}
		ticker := time.NewTicker(checkpointInterval)
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
