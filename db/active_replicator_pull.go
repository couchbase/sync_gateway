package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	activeReplicatorCommon
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	return &ActivePullReplicator{
		activeReplicatorCommon: activeReplicatorCommon{
			config:           config,
			replicationStats: BlipSyncStatsForSGRPull(config.ReplicationStatsMap),
			state:            ReplicationStateStopped,
		},
	}
}

func (apr *ActivePullReplicator) Start() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if apr.checkpointerCtx != nil {
		return fmt.Errorf("ActivePullReplicator already running")
	}

	var err error
	apr.blipSender, apr.blipSyncContext, err = connect("-pull", apr.config, apr.replicationStats)
	if err != nil {
		return apr._setError(err)
	}

	if apr.config.ConflictResolverFunc != nil {
		apr.blipSyncContext.conflictResolver = NewConflictResolver(apr.config.ConflictResolverFunc, apr.config.ReplicationStatsMap)
	}
	apr.blipSyncContext.purgeOnRemoval = apr.config.PurgeOnRemoval

	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(context.Background())
	if err := apr._initCheckpointer(); err != nil {
		apr.checkpointerCtx = nil
		return apr._setError(err)
	}

	subChangesRequest := SubChangesRequest{
		Continuous:     apr.config.Continuous,
		Batch:          apr.config.ChangesBatchSize,
		Since:          apr.Checkpointer.lastCheckpointSeq,
		Filter:         apr.config.Filter,
		FilterChannels: apr.config.FilterChannels,
		DocIDs:         apr.config.DocIDs,
		ActiveOnly:     apr.config.ActiveOnly,
		clientType:     clientTypeSGR2,
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		apr.checkpointerCtxCancel()
		apr.checkpointerCtx = nil
		return apr._setError(err)
	}

	apr._setState(ReplicationStateRunning)
	return nil
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePullReplicator) Complete() {

	apr.lock.Lock()
	if apr == nil {
		apr.lock.Unlock()
		return
	}

	err := apr.Checkpointer.waitForExpectedSequences()
	if err != nil {
		base.Infof(base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
	}

	stopErr := apr._stop()
	if stopErr != nil {
		base.Infof(base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}

	// unlock the replication before triggering callback, in case callback attempts to access replication information
	// from the replicator
	onCompleteCallback := apr.onReplicatorComplete
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

func (apr *ActivePullReplicator) Stop() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()
	return apr._stop()
}

func (apr *ActivePullReplicator) _stop() error {
	if apr == nil {
		// noop
		return nil
	}

	if apr.checkpointerCtx != nil {
		apr.checkpointerCtxCancel()
		apr.Checkpointer.CheckpointNow()
	}
	apr.checkpointerCtx = nil

	if apr.blipSender != nil {
		apr.blipSender.Close()
		apr.blipSender = nil
	}

	if apr.blipSyncContext != nil {
		apr.blipSyncContext.Close()
		apr.blipSyncContext = nil
	}
	apr._setState(ReplicationStateStopped)

	return nil
}

func (apr *ActivePullReplicator) _initCheckpointer() error {
	checkpointID, err := apr.CheckpointID()
	if err != nil {
		return err
	}

	apr.Checkpointer = NewCheckpointer(apr.checkpointerCtx, checkpointID, apr.blipSender, apr.config.ActiveDB, apr.config.CheckpointInterval)

	err = apr.Checkpointer.fetchCheckpoints()
	if err != nil {
		return err
	}

	apr.registerCheckpointerCallbacks()
	apr.Checkpointer.Start()

	return nil
}

// CheckpointID returns a unique ID to be used for the checkpoint client (which is used as part of the checkpoint Doc ID on the recipient)
func (apr *ActivePullReplicator) CheckpointID() (string, error) {
	checkpointHash, err := apr.config.CheckpointHash()
	if err != nil {
		return "", err
	}
	return "sgr2cp:pull:" + checkpointHash, nil
}

func (apr *ActivePullReplicator) reset() error {
	if apr.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", apr.config.ID)
	}
	checkpointID, err := apr.CheckpointID()
	if err != nil {
		return err
	}
	return resetLocalCheckpoint(apr.config.ActiveDB, checkpointID)
}

type pullCheckpointerLookup struct {
	m map[IDAndRev]string
	l sync.Mutex
}

func newPullCheckpointerLookup() *pullCheckpointerLookup {
	return &pullCheckpointerLookup{
		m: make(map[IDAndRev]string, 0),
	}
}

func (p *pullCheckpointerLookup) AddSeqs(idRevAndSeq map[IDAndRev]string) (addedSeqs []string) {
	addedSeqs = make([]string, 0, len(idRevAndSeq))
	p.l.Lock()
	for k, v := range idRevAndSeq {
		p.m[k] = v
		addedSeqs = append(addedSeqs, v)
	}
	p.l.Unlock()
	return addedSeqs
}

// Pop a sequence by DocID+RevID
func (p *pullCheckpointerLookup) PopSeq(idAndRev IDAndRev) (seq string) {
	p.l.Lock()
	seq, ok := p.m[idAndRev]
	if ok {
		delete(p.m, idAndRev)
	}
	p.l.Unlock()
	return seq
}

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePullReplicator) registerCheckpointerCallbacks() {
	// noRevSeqLookup is required for sgr2PullNorevCallback because sequence isn't sent in norev messages like it is for everything else.
	// For hydrogen-to-hydrogen replications, we do send seq in norev, but still need this handling for pre-hydrogen targets.
	noRevSeqLookup := newPullCheckpointerLookup()

	apr.blipSyncContext.sgr2PullAlreadyKnownSeqsCallback = func(alreadyKnownSeqs []string) {
		apr.Checkpointer.AddAlreadyKnownSeq(alreadyKnownSeqs...)
	}

	apr.blipSyncContext.sgr2PullAddExpectedSeqsCallback = func(changesSeqs map[IDAndRev]string) {
		seqs := noRevSeqLookup.AddSeqs(changesSeqs)
		apr.Checkpointer.AddExpectedSeq(seqs...)
	}

	apr.blipSyncContext.sgr2PullProcessedSeqCallback = func(idAndRev IDAndRev, remoteSeq string) {
		_ = noRevSeqLookup.PopSeq(idAndRev)
		apr.Checkpointer.AddProcessedSeq(remoteSeq)
	}

	apr.blipSyncContext.sgr2PullNorevCallback = func(docID, revID string) {
		seq := noRevSeqLookup.PopSeq(IDAndRev{DocID: docID, RevID: revID})
		apr.Checkpointer.AddProcessedSeq(seq)
	}

	// Trigger complete for non-continuous replications when caught up
	if !apr.config.Continuous {
		apr.blipSyncContext.emptyChangesMessageCallback = func() {
			// Complete blocks waiting for pending rev messages, so needs
			// it's own goroutine
			go apr.Complete()
		}
	}

}
