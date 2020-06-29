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
	checkpointInterval time.Duration
	// lastCheckpointRevID is the last known checkpoint RevID.
	lastCheckpointRevID string
	// lastCheckpointSeq is the last checkpointed sequence
	lastCheckpointSeq string
	// lock guards the expectedSeqs slice, and processedSeqs map
	lock sync.Mutex
	// expectedSeqs is an ordered list of sequence IDs we expect to process revs for
	expectedSeqs []string
	// processedSeqs is a map of sequence IDs we've processed revs for
	processedSeqs map[string]struct{}
	// ctx is used to stop the checkpointer goroutine
	ctx                        context.Context
	StatSetCheckpointTotal     *expvar.Int
	StatGetCheckpointHitTotal  *expvar.Int
	StatGetCheckpointMissTotal *expvar.Int
}

func NewCheckpointer(ctx context.Context, clientID string, blipSender *blip.Sender, checkpointInterval time.Duration) *Checkpointer {
	return &Checkpointer{
		clientID:                   clientID,
		blipSender:                 blipSender,
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
	c.setCheckpoint(seq)
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

// getCheckpoint tries to fetch a since value for the given replication by requesting a checkpoint.
// If this fails, the function returns a zero value and false.
func (c *Checkpointer) getCheckpoint() (r GetSGR2CheckpointResponse) {
	rq := GetSGR2CheckpointRequest{
		Client: c.clientID,
	}

	if err := rq.Send(c.blipSender); err != nil {
		base.Warnf("couldn't send getCheckpoint request, starting from 0: %v", err)
		return GetSGR2CheckpointResponse{}
	}

	resp, err := rq.Response()
	if err != nil {
		base.Warnf("couldn't get response for getCheckpoint request, starting from 0: %v", err)
		return GetSGR2CheckpointResponse{}
	}

	// checkpoint wasn't found (404)
	if resp == nil {
		base.Debugf(base.KeyReplicate, "couldn't find existing checkpoint for client %q, starting from 0", c.clientID)
		c.StatGetCheckpointMissTotal.Add(1)
		return GetSGR2CheckpointResponse{}
	}

	c.StatGetCheckpointHitTotal.Add(1)
	return *resp
}

func (c *Checkpointer) setCheckpoint(seq string) {
	rq := SetSGR2CheckpointRequest{
		Client: c.clientID,
		Checkpoint: SGR2Checkpoint{
			LastSequence: seq,
		},
	}
	if c.lastCheckpointRevID != "" {
		rq.RevID = &c.lastCheckpointRevID
	}

	if err := rq.Send(c.blipSender); err != nil {
		base.Warnf("couldn't send SetCheckpoint request: %v", err)
		return
	}

	resp, err := rq.Response()
	if err != nil {
		base.Warnf("couldn't get response for SetCheckpoint request: %v", err)
		return
	}

	c.lastCheckpointRevID = resp.RevID
	c.lastCheckpointSeq = seq
	c.StatSetCheckpointTotal.Add(1)
}
