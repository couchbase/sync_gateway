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
		var exit bool
		checkpointInterval := defaultCheckpointInterval
		if c.checkpointInterval > 0 {
			checkpointInterval = c.checkpointInterval
		}
		ticker := time.NewTicker(checkpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-c.ctx.Done():
				exit = true
				// parent context stopped stopped, set a final checkpoint before stopping this goroutine
			}

			c.CheckpointNow()

			if exit {
				base.Debugf(base.KeyReplicate, "checkpointer goroutine stopped")
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

	base.Tracef(base.KeyReplicate, "checkpointer: running")

	seq := calculateCheckpointSeq(c.expectedSeqs, c.processedSeqs)
	if seq == "" {
		return
	}

	base.Tracef(base.KeyReplicate, "checkpointer: calculated seq: %v", seq)
	err := c.setCheckpoints(seq)
	if err != nil {
		base.Warnf("couldn't set checkpoints: %v", err)
	}
}

func calculateCheckpointSeq(expectedSeqs []string, processedSeqs map[string]struct{}) (seq string) {
	if len(expectedSeqs) == 0 {
		// nothing to do
		return ""
	}

	// iterates over each (ordered) expected sequence and stops when we find the first sequence we've yet to process a rev message for
	maxI := -1
	for i, seq := range expectedSeqs {
		if _, ok := processedSeqs[seq]; !ok {
			base.Tracef(base.KeyReplicate, "checkpointer: couldn't find %v in processedSeqs", seq)
			break
		}

		delete(processedSeqs, seq)
		maxI = i
	}

	// the first seq we expected hasn't arrived yet, so can't checkpoint anything
	if maxI < 0 {
		return
	}

	seq = expectedSeqs[maxI]

	if len(expectedSeqs)-1 == maxI {
		// received full set, empty list
		expectedSeqs = expectedSeqs[0:0]
	} else {
		// trim sequence list for partially received set
		expectedSeqs = expectedSeqs[maxI+1:]
	}

	return seq
}

const checkpointDocIDPrefix = "checkpoint/"

// fetchCheckpoints tries to fetch since values for the given replication by requesting a checkpoint on the local and remote.
// If we fail to fetch checkpoints, we remove any existing checkpoints, and start from zero.
// If we fetch mismatched checkpoints, we'll pick the lower of the two, and remove the higher checkpoint.
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
			c.lastLocalCheckpointRevID, err = c.setLocalCheckpoint(checkpointSeq, c.lastRemoteCheckpointRevID)
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
		base.Debugf(base.KeyReplicate, "couldn't find existing local checkpoint for client %q", c.clientID)
		return "", "", nil
	}

	return checkpointBody["last_sequence"].(string), checkpointBody[BodyRev].(string), nil
}

func (c *Checkpointer) setLocalCheckpoint(seq, parentRev string) (newRev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setLocalCheckpoint(%v, %v)", seq, parentRev)

	newRev, err = c.activeDB.putSpecial("local", checkpointDocIDPrefix+c.clientID, parentRev, Body{"last_sequence": seq})
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
		base.Debugf(base.KeyReplicate, "couldn't find existing remote checkpoint for client %q", c.clientID)
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
