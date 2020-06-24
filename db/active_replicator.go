package db

import (
	"context"
	"encoding/base64"
	"expvar"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/net/websocket"
)

const (
	// TODO: Not aligned these to any stats in the PRD yet, these are used for test assertions.
	ActiveReplicatorStatsKeyRevsReceivedTotal        = "revs_received_total"
	ActiveReplicatorStatsKeyChangesRevsReceivedTotal = "changes_revs_received_total"
	ActiveReplicatorStatsKeyRevsSentTotal            = "revs_sent_total"
	ActiveReplicatorStatsKeyRevsRequestedTotal       = "revs_requested_total"
)

const (
	defaultCheckpointInterval = time.Second * 30
)

// ActiveReplicator is a wrapper to encapsulate separate push and pull active replicators.
type ActiveReplicator struct {
	Push *ActivePushReplicator
	Pull *ActivePullReplicator
}

// NewActiveReplicator returns a bidirectional active replicator for the given config.
func NewActiveReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActiveReplicator, error) {
	ar := &ActiveReplicator{}

	if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
		ar.Push = NewPushReplicator(ctx, config)
	}

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		ar.Pull = NewPullReplicator(ctx, config)
	}

	return ar, nil
}

func (ar *ActiveReplicator) Start() error {
	if ar.Push != nil {
		if err := ar.Push.Start(); err != nil {
			return err
		}
	}

	if ar.Pull != nil {
		if err := ar.Pull.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) Close() error {
	if ar.Push != nil {
		if err := ar.Push.Close(); err != nil {
			return err
		}
	}

	if ar.Pull != nil {
		if err := ar.Pull.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) GetStatus(replicationID string) *ReplicationStatus {

	status := &ReplicationStatus{
		ID:     replicationID,
		Status: "running",
	}
	if ar.Pull != nil {
		pullStats := ar.Pull.blipSyncContext.replicationStats
		status.DocsRead = pullStats.HandleRevCount.Value()
		status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
		status.LastSeqPull = ar.Pull.Checkpointer.lastCheckpointSeq
	}

	if ar.Push != nil {
		pushStats := ar.Push.blipSyncContext.replicationStats
		status.DocsWritten = pushStats.SendRevCount.Value()
		status.DocWriteFailures = pushStats.SendRevErrorCount.Value()
		// TODO: This is another scenario where we need to send a rev without noreply set to get the returned error
		// status.RejectedRemote = pushStats.SendRevSyncFunctionErrorCount.Value()
	}

	return status
}

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

// GetCheckpoint tries to fetch a since value for the given replication by requesting a checkpoint.
// If this fails, the function returns a zero value and false.
func (c *Checkpointer) GetCheckpoint() (r GetSGR2CheckpointResponse) {
	rq := GetSGR2CheckpointRequest{
		Client: c.clientID,
	}

	if err := rq.Send(c.blipSender); err != nil {
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
		base.Debugf(base.KeyReplicate, "couldn't find existing checkpoint for client %q, starting from 0", c.clientID)
		c.StatGetCheckpointMissTotal.Add(1)
		return GetSGR2CheckpointResponse{}
	}

	c.StatGetCheckpointHitTotal.Add(1)
	return *resp
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
	c.lock.Lock()
	defer c.lock.Unlock()

	base.Tracef(base.KeyReplicate, "checkpointer: running")

	// find the highest contiguous sequence we've received
	if len(c.expectedSeqs) > 0 {
		var lowSeq string

		// iterates over each (ordered) expected sequence and stops when we find the first sequence we've yet to process a rev message for
		maxI := -1
		for i, seq := range c.expectedSeqs {
			if _, ok := c.processedSeqs[seq]; !ok {
				base.Tracef(base.KeyReplicate, "checkpointer: couldn't find %v in processedSeqs", seq)
				break
			}

			delete(c.processedSeqs, seq)
			maxI = i
		}

		// the first seq we expected hasn't arrived yet, so can't checkpoint anything
		if maxI < 0 {
			return
		}

		lowSeq = c.expectedSeqs[maxI]

		if len(c.expectedSeqs)-1 == maxI {
			// received full set, empty list
			c.expectedSeqs = c.expectedSeqs[0:0]
		} else {
			// trim sequence list for partially received set
			c.expectedSeqs = c.expectedSeqs[maxI+1:]
		}

		base.Tracef(base.KeyReplicate, "checkpointer: got lowSeq: %v", lowSeq)
		c.setCheckpoint(lowSeq)
	}
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

func connect(idSuffix string, config *ActiveReplicatorConfig) (blipSender *blip.Sender, bsc *BlipSyncContext, err error) {

	blipContext := blip.NewContextCustomID(config.ID+idSuffix, blipCBMobileReplication)
	bsc = NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)

	blipSender, err = blipSync(*config.PassiveDBURL, blipContext)
	if err != nil {
		return nil, nil, err
	}

	return blipSender, bsc, nil
}

// blipSync opens a connection to the target, and returns a blip.Sender to send messages over.
func blipSync(target url.URL, blipContext *blip.Context) (*blip.Sender, error) {
	// GET target database endpoint to see if reachable for exit-early/clearer error message
	resp, err := http.Get(target.String())
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from target database", resp.StatusCode)
	}

	// switch to websocket protocol scheme
	if target.Scheme == "http" {
		target.Scheme = "ws"
	} else if target.Scheme == "https" {
		target.Scheme = "wss"
	}

	config, err := websocket.NewConfig(target.String()+"/_blipsync", "http://localhost")
	if err != nil {
		return nil, err
	}

	if target.User != nil {
		config.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(target.User.String())))
	}

	return blipContext.DialConfig(config)
}
