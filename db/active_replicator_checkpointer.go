package db

import (
	"context"
	"errors"
	"strings"
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
	// idAndRevLookup is a temporary map of DocID/RevID pair to sequence number,
	// to handle cases where we receive a message that doesn't contain a sequence.
	idAndRevLookup map[IDAndRev]string
	// ctx is used to stop the checkpointer goroutine
	ctx context.Context
	// lastRemoteCheckpointRevID is the last known remote checkpoint RevID.
	lastRemoteCheckpointRevID string
	// lastLocalCheckpointRevID is the last known local checkpoint RevID.
	lastLocalCheckpointRevID string
	// lastCheckpointSeq is the last checkpointed sequence
	lastCheckpointSeq string

	stats CheckpointerStats
}

type CheckpointerStats struct {
	ExpectedSequenceCount     int64
	ProcessedSequenceCount    int64
	AlreadyKnownSequenceCount int64
	SetCheckpointCount        int64
	GetCheckpointHitCount     int64
	GetCheckpointMissCount    int64
}

func NewCheckpointer(ctx context.Context, clientID string, blipSender *blip.Sender, activeDB *Database, checkpointInterval time.Duration) *Checkpointer {
	return &Checkpointer{
		clientID:           clientID,
		blipSender:         blipSender,
		activeDB:           activeDB,
		expectedSeqs:       make([]string, 0),
		processedSeqs:      make(map[string]struct{}),
		idAndRevLookup:     make(map[IDAndRev]string),
		checkpointInterval: checkpointInterval,
		ctx:                ctx,
		stats:              CheckpointerStats{},
	}
}

func (c *Checkpointer) AddAlreadyKnownSeq(seq ...string) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.expectedSeqs = append(c.expectedSeqs, seq...)
	for _, seq := range seq {
		c.processedSeqs[seq] = struct{}{}
	}
	c.stats.AlreadyKnownSequenceCount += int64(len(seq))
	c.lock.Unlock()
}

func (c *Checkpointer) AddProcessedSeq(seq string) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.processedSeqs[seq] = struct{}{}
	c.stats.ProcessedSequenceCount++
	c.lock.Unlock()
}

func (c *Checkpointer) AddProcessedSeqIDAndRev(seq string, idAndRev IDAndRev) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()

	if seq == "" {
		seq, _ = c.idAndRevLookup[idAndRev]
	}
	// should remove entry in the map even if we have a seq available
	delete(c.idAndRevLookup, idAndRev)

	c.processedSeqs[seq] = struct{}{}
	c.stats.ProcessedSequenceCount++

	c.lock.Unlock()
}

func (c *Checkpointer) AddExpectedSeqs(seqs ...string) {
	if len(seqs) == 0 {
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
	c.expectedSeqs = append(c.expectedSeqs, seqs...)
	c.stats.ExpectedSequenceCount += int64(len(seqs))
	c.lock.Unlock()
}

func (c *Checkpointer) AddExpectedSeqIDAndRevs(seqs map[IDAndRev]string) {
	if len(seqs) == 0 {
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
	for idAndRev, seq := range seqs {
		c.idAndRevLookup[idAndRev] = seq
		c.expectedSeqs = append(c.expectedSeqs, seq)
	}
	c.stats.ExpectedSequenceCount += int64(len(seqs))
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
	err := c._setCheckpoints(seq)
	if err != nil {
		base.Warnf("couldn't set checkpoints: %v", err)
	}
}

// Stats returns a copy of the checkpointer stats. Intended for test use - non-test usage may have
// performance implications associated with locking
func (c *Checkpointer) Stats() CheckpointerStats {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stats
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
	for i := 0; i <= maxI; i++ {
		removeSeq := c.expectedSeqs[i]
		delete(c.processedSeqs, removeSeq)
		base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: _updateCheckpointLists removed seq %v from processedSeqs map %v", removeSeq, c.processedSeqs)
	}

	// trim expectedSeqs list for all processed seqs
	c.expectedSeqs = c.expectedSeqs[maxI+1:]

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

// calculateSafeProcessedSeq returns the sequence last processed that is able to be checkpointed, or the last checkpointed sequence.
func (c *Checkpointer) calculateSafeProcessedSeq() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	idx := c._calculateSafeExpectedSeqsIdx()
	if idx == -1 {
		return c.lastCheckpointSeq
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
				base.WarnfCtx(c.ctx, "Unable to roll back local checkpoint: %v", err)
			}
		} else {
			checkpointSeq = localSeq
			c.lastRemoteCheckpointRevID, err = c.setRemoteCheckpoint(checkpointSeq, c.lastRemoteCheckpointRevID)
			if err != nil {
				base.WarnfCtx(c.ctx, "Unable to roll back remote checkpoint: %v", err)
			}
		}
	}

	if checkpointSeq == "" {
		c.stats.GetCheckpointMissCount++
	} else {
		c.stats.GetCheckpointHitCount++
	}

	base.DebugfCtx(c.ctx, base.KeyReplicate, "using checkpointed seq: %q", checkpointSeq)
	c.lastCheckpointSeq = checkpointSeq

	return nil
}

func (c *Checkpointer) _setCheckpoints(seq string) (err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setCheckpoints(%v)", seq)

	c.lastLocalCheckpointRevID, err = c.setLocalCheckpointWithRetry(seq, c.lastLocalCheckpointRevID)
	if err != nil {
		return err
	}

	c.lastRemoteCheckpointRevID, err = c.setRemoteCheckpointWithRetry(seq, c.lastRemoteCheckpointRevID)
	if err != nil {
		return err
	}

	c.lastCheckpointSeq = seq
	c.stats.SetCheckpointCount++

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

// setLocalCheckpointWithRetry attempts to rewrite the checkpoint if the rev ID is mismatched, or the checkpoint has since been deleted.
func (c *Checkpointer) setLocalCheckpointWithRetry(seq, existingRevID string) (newRevID string, err error) {
	return c.setRetry(seq, existingRevID,
		c.setLocalCheckpoint,
		c.getLocalCheckpoint,
	)
}

func resetLocalCheckpoint(activeDB *Database, checkpointID string) error {
	key := activeDB.realSpecialDocID("local", checkpointDocIDPrefix+checkpointID)
	return activeDB.Bucket.Delete(key)
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

// setRemoteCheckpointWithRetry attempts to rewrite the checkpoint if the rev ID is mismatched, or the checkpoint has since been deleted.
func (c *Checkpointer) setRemoteCheckpointWithRetry(seq, existingRevID string) (newRevID string, err error) {
	return c.setRetry(seq, existingRevID,
		c.setRemoteCheckpoint,
		c.getRemoteCheckpoint,
	)
}

func (c *Checkpointer) getCounts() (expectedCount, processedCount int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.expectedSeqs), len(c.processedSeqs)
}

// waitForExpectedSequences waits for the expectedSeqs set to drain to zero.
// Intended to be used once the replication has been stopped, to wait for
// in-flight mutations to complete.
// Triggers immediate checkpointing if expectedCount == processedCount.
// Waits up to 10s, polling every 100ms.
func (c *Checkpointer) waitForExpectedSequences() error {
	waitCount := 0
	for waitCount < 100 {
		expectedCount, processedCount := c.getCounts()
		if expectedCount == 0 {
			return nil
		}
		if expectedCount == processedCount {
			c.CheckpointNow()
			// Doing an additional check here, instead of just 'continue',
			// in case of bugs that result in expectedCount==processedCount, but the
			// sets are not identical.  In that scenario, want to sleep before retrying
			updatedExpectedCount, _ := c.getCounts()
			if updatedExpectedCount == 0 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
		waitCount++
	}
	return errors.New("checkpointer waitForExpectedSequences failed to complete after waiting 10s")
}

type setCheckpointFn func(seq, parentRevID string) (revID string, err error)
type getCheckpointFn func() (seq string, revID string, err error)

// setRetry is a retry loop for a setCheckpointFn, which will fetch a new RevID from a getCheckpointFn in the event of a write conflict.
func (c *Checkpointer) setRetry(seq, existingRevID string, setFn setCheckpointFn, getFn getCheckpointFn) (newRevID string, err error) {
	for numAttempts := 0; numAttempts < 10; numAttempts++ {
		newRevID, err = setFn(seq, existingRevID)
		if err != nil {
			if strings.HasPrefix(err.Error(), "409") {
				base.WarnfCtx(c.ctx, "rev mismatch from setCheckpoint - updating last known rev ID: %v", err)
				_, rev, getErr := getFn()
				if getErr == nil {
					base.InfofCtx(c.ctx, base.KeyReplicate, "using new rev from checkpoint: %v", rev)
					existingRevID = rev
				} else {
					// fall through to retry
				}
			} else if strings.HasPrefix(err.Error(), "404") {
				base.WarnfCtx(c.ctx, "checkpoint did not exist for attempted update - removing last known rev ID: %v", err)
				existingRevID = ""
			} else {
				base.WarnfCtx(c.ctx, "got unexpected error from setCheckpoint: %v", err)
			}
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return newRevID, nil
	}
	return "", errors.New("failed to write checkpoint after 10 attempts")
}
