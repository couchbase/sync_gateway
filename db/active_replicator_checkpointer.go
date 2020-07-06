package db

import (
	"context"
	"expvar"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// Checkpointer implements replicator checkpointing, by keeping two lists of sequences. Those which we expect to be processing revs for (either push or pull), and a map for those which we have done so on.
// Periodically (based on a time interval), these two lists are used to calculate the highest sequence number which we've not had a gap for yet, and send a SetCheckpoint message for this sequence.
type Checkpointer struct {
	clientID           string
	blipSender         *blip.Sender
	activeDB           *Database
	checkpointInterval time.Duration
	// lock guards the expectedSeqs slice, and processedSeqs map
	lock sync.Mutex
	// expectedSeqs is an ordered list of sequence IDs we expect to process revs for
	expectedSeqs []string
	// processedSeqs is a map of sequence IDs we've processed revs for
	processedSeqs map[string]struct{}
	// ctx is used to stop the checkpointer goroutine
	ctx context.Context
	// lastRemoteCheckpointRevID is the last known remote checkpoint RevID.
	lastRemoteCheckpointRevID string
	// lastLocalCheckpointRevID is the last known local checkpoint RevID.
	lastLocalCheckpointRevID string
	// lastCheckpointSeq is the last checkpointed sequence
	lastCheckpointSeq string

	StatSetCheckpointTotal     *expvar.Int
	StatGetCheckpointHitTotal  *expvar.Int
	StatGetCheckpointMissTotal *expvar.Int
}

func NewCheckpointer(ctx context.Context, clientID string, blipSender *blip.Sender, activeDB *Database, checkpointInterval time.Duration) *Checkpointer {
	return &Checkpointer{
		clientID:                   clientID,
		blipSender:                 blipSender,
		activeDB:                   activeDB,
		expectedSeqs:               make([]string, 0),
		processedSeqs:              make(map[string]struct{}),
		checkpointInterval:         checkpointInterval,
		ctx:                        ctx,
		StatSetCheckpointTotal:     &expvar.Int{},
		StatGetCheckpointHitTotal:  &expvar.Int{},
		StatGetCheckpointMissTotal: &expvar.Int{},
	}
}

func (c *Checkpointer) ProcessedSeq(seq string) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.processedSeqs[seq] = struct{}{}
	c.lock.Unlock()
}

func (c *Checkpointer) AddExpectedSeq(seq ...string) {
	if len(seq) == 0 {
		// nothing to do
		return
	}

	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.expectedSeqs = append(c.expectedSeqs, seq...)
	c.lock.Unlock()
}

func (c *Checkpointer) Start() {
	// Start a time-based checkpointer goroutine
	go func() {
		checkpointInterval := defaultCheckpointInterval
		if c.checkpointInterval > 0 {
			checkpointInterval = c.checkpointInterval
		}
		ticker := time.NewTicker(checkpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.CheckpointNow()
			case <-c.ctx.Done():
				base.DebugfCtx(c.ctx, base.KeyReplicate, "checkpointer goroutine stopped")
				return
			}
		}
	}()
}

// CheckpointNow forces the checkpointer to send a checkpoint, and blocks until it has finished.
func (c *Checkpointer) CheckpointNow() {
	if c == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: running")

	seq := c._updateCheckpointLists()
	if seq == "" {
		return
	}

	base.InfofCtx(c.ctx, base.KeyReplicate, "checkpointer: calculated seq: %v", seq)
	err := c.setCheckpoints(seq)
	if err != nil {
		base.Warnf("couldn't set checkpoints: %v", err)
	}
}

// _updateCheckpointLists determines the highest checkpointable sequence, and trims the processedSeqs/expectedSeqs lists up to this point.
func (c *Checkpointer) _updateCheckpointLists() (safeSeq string) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: _updateCheckpointLists(expectedSeqs: %v, procssedSeqs: %v)", c.expectedSeqs, c.processedSeqs)

	maxI := c._calculateSafeExpectedSeqsIdx()
	if maxI == -1 {
		// nothing to do
		return ""
	}

	safeSeq = c.expectedSeqs[maxI]

	// removes to-be checkpointed sequences from processedSeqs list
	for i := 0; i < maxI; i++ {
		removeSeq := c.expectedSeqs[i]
		delete(c.processedSeqs, removeSeq)
		base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: _updateCheckpointLists removed seq %v from processedSeqs map %v", removeSeq, c.processedSeqs)
	}

	if len(c.expectedSeqs)-1 == maxI {
		// received full set, empty expectedSeqs list
		c.expectedSeqs = c.expectedSeqs[0:0]
	} else {
		// trim expectedSeqs list for partially received set
		c.expectedSeqs = c.expectedSeqs[maxI+1:]
	}

	return safeSeq
}

// _calculateSafeExpectedSeqsIdx returns an index into expectedSeqs which is safe to checkpoint.
// Returns -1 if no sequence in the list is able to be checkpointed.
func (c *Checkpointer) _calculateSafeExpectedSeqsIdx() int {
	safeIdx := -1

	// iterates over each (ordered) expected sequence and stops when we find the first sequence we've yet to process a rev message for
	for i, seq := range c.expectedSeqs {
		if _, ok := c.processedSeqs[seq]; !ok {
			break
		}
		safeIdx = i
	}

	return safeIdx
}

// calculateSafeProcessedSeq returns the sequence last processed that is able to be checkpointed.
func (c *Checkpointer) calculateSafeProcessedSeq() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	idx := c._calculateSafeExpectedSeqsIdx()
	if idx == -1 {
		return ""
	}

	return c.expectedSeqs[idx]
}

const (
	checkpointDocIDPrefix   = "checkpoint/"
	checkpointDocLastSeqKey = "last_sequence"
)

// fetchCheckpoints tries to fetch since values for the given replication by requesting a checkpoint on the local and remote.
// If we fetch missing, or mismatched checkpoints, we'll pick the lower of the two, and roll back the higher checkpoint.
func (c *Checkpointer) fetchCheckpoints() error {
	base.TracefCtx(c.ctx, base.KeyReplicate, "fetchCheckpoints()")

	localSeq, localRev, err := c.getLocalCheckpoint()
	if err != nil {
		return err
	}
	base.DebugfCtx(c.ctx, base.KeyReplicate, "got local checkpoint: %q %q", localSeq, localRev)
	c.lastLocalCheckpointRevID = localRev

	remoteSeq, remoteRev, err := c.getRemoteCheckpoint()
	if err != nil {
		return err
	}
	base.DebugfCtx(c.ctx, base.KeyReplicate, "got remote checkpoint: %q %q", remoteSeq, remoteRev)
	c.lastRemoteCheckpointRevID = remoteRev

	// If localSeq and remoteSeq match, we'll use this value.
	checkpointSeq := localSeq

	// Determine the lowest sequence if they don't match
	if localSeq != remoteSeq {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "sequences mismatched, finding lowest of %q %q", localSeq, remoteSeq)
		localSeqVal, err := parseIntegerSequenceID(localSeq)
		if err != nil {
			return err
		}

		remoteSeqVal, err := parseIntegerSequenceID(remoteSeq)
		if err != nil {
			return err
		}

		// roll local/remote checkpoint back to lowest of the two
		if remoteSeqVal.Before(localSeqVal) {
			checkpointSeq = remoteSeq
			c.lastLocalCheckpointRevID, err = c.setLocalCheckpoint(checkpointSeq, c.lastLocalCheckpointRevID)
			if err != nil {
				return err
			}
		} else {
			checkpointSeq = localSeq
			c.lastRemoteCheckpointRevID, err = c.setRemoteCheckpoint(checkpointSeq, c.lastRemoteCheckpointRevID)
			if err != nil {
				return err
			}
		}
	}

	if checkpointSeq == "" {
		c.StatGetCheckpointMissTotal.Add(1)
	} else {
		c.StatGetCheckpointHitTotal.Add(1)
	}

	base.DebugfCtx(c.ctx, base.KeyReplicate, "using checkpointed seq: %q", checkpointSeq)
	c.lastCheckpointSeq = checkpointSeq

	return nil
}

func (c *Checkpointer) setCheckpoints(seq string) error {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setCheckpoints(%v)", seq)

	newLocalRev, err := c.setLocalCheckpoint(seq, c.lastLocalCheckpointRevID)
	if err != nil {
		return err
	}
	c.lastLocalCheckpointRevID = newLocalRev

	newRemoteRev, err := c.setRemoteCheckpoint(seq, c.lastRemoteCheckpointRevID)
	if err != nil {
		return err
	}
	c.lastRemoteCheckpointRevID = newRemoteRev

	c.lastCheckpointSeq = seq
	c.StatSetCheckpointTotal.Add(1)

	return nil
}

// getLocalCheckpoint returns the sequence and rev for the local checkpoint.
// if the checkpoint does not exist, returns empty sequence and rev.
func (c *Checkpointer) getLocalCheckpoint() (seq, rev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "getLocalCheckpoint")

	checkpointBody, err := c.activeDB.GetSpecial("local", checkpointDocIDPrefix+c.clientID)
	if err != nil {
		if !base.IsKeyNotFoundError(c.activeDB.Bucket, err) {
			return "", "", err
		}
		base.DebugfCtx(c.ctx, base.KeyReplicate, "couldn't find existing local checkpoint for client %q", c.clientID)
		return "", "", nil
	}

	return checkpointBody[checkpointDocLastSeqKey].(string), checkpointBody[BodyRev].(string), nil
}

func (c *Checkpointer) setLocalCheckpoint(seq, parentRev string) (newRev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setLocalCheckpoint(%v, %v)", seq, parentRev)

	newRev, err = c.activeDB.putSpecial("local", checkpointDocIDPrefix+c.clientID, parentRev, Body{checkpointDocLastSeqKey: seq})
	if err != nil {
		return "", err
	}
	return newRev, nil
}

// getRemoteCheckpoint returns the sequence and rev for the remote checkpoint.
// if the checkpoint does not exist, returns empty sequence and rev.
func (c *Checkpointer) getRemoteCheckpoint() (seq, rev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "getRemoteCheckpoint")

	rq := GetSGR2CheckpointRequest{
		Client: c.clientID,
	}

	if err := rq.Send(c.blipSender); err != nil {
		return "", "", err
	}

	resp, err := rq.Response()
	if err != nil {
		return "", "", err
	}

	if resp == nil {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "couldn't find existing remote checkpoint for client %q", c.clientID)
		return "", "", nil
	}

	return resp.LastSeq, resp.RevID, nil
}

func (c *Checkpointer) setRemoteCheckpoint(seq, parentRev string) (newRev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setRemoteCheckpoint(%v, %v)", seq, parentRev)

	rq := SetSGR2CheckpointRequest{
		Client: c.clientID,
		Checkpoint: SGR2CheckpointDoc{
			LastSequence: seq,
		},
	}

	if parentRev != "" {
		rq.RevID = &parentRev
	}

	if err := rq.Send(c.blipSender); err != nil {
		return "", err
	}

	resp, err := rq.Response()
	if err != nil {
		return "", err
	}

	return resp.RevID, nil
}
