// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// DCP Worker manages checkpoint persistence and adds a configurable level of concurrency when
// processing a feed.  gocb's DCPAgent opens one pipeline per server node (by default, can be increased
// via kvPoolSize).  Increasing the number of workers allows SG to increase the level of concurrent processing without
// adding additional load on the server.
// DCPWorker are also single-threaded and guarantee ordered processing of DCP events within a given vbucket.
//
// DCPWorker queues incoming mutations in a buffered channel (eventFeed).  The main worker goroutine
// works this channel and synchronously invokes the mutationCallback for mutations or deletions.
type DCPWorker struct {
	ID                    int
	endSeqNos             []uint64
	eventFeed             chan streamEvent
	terminator            chan bool
	checkpointPrefixBytes []byte
	mutationCallback      sgbucket.FeedEventCallbackFunc
	endStreamCallback     endStreamCallbackFunc
	ignoreDeletes         bool
	metadata              DCPMetadataStore
	pendingSnapshot       map[uint16]snapshotEvent
	lastMetaPersistTime   time.Time
	pendingPersist        bool // a requested checkpoint was deferred by metaPersistFrequency
	metaPersistFrequency  time.Duration
	assignedVbs           []uint16
	idleCheckpointOps     bool // Whether a checkpoint write made only for _sync: doc events is counted as an idle KV op
	sawNonSyncDoc         bool // Whether an event that was not for a _sync: doc has been seen since the last persist
}

const defaultQueueLength = 10
const defaultMetadataPersistFrequency = 1 * time.Minute

type DCPWorkerOptions struct {
	eventQueueLength     int
	ignoreDeletes        bool
	metaPersistFrequency *time.Duration
	idleCheckpointOps    bool
}

func NewDCPWorker(workerID int, metadata DCPMetadataStore, mutationCallback sgbucket.FeedEventCallbackFunc,
	endCallback endStreamCallbackFunc, terminator chan bool, endSeqNos []uint64, checkpointPrefix string,
	assignedVbs []uint16, options *DCPWorkerOptions) *DCPWorker {

	// Create a buffered channel for queueing incoming DCP events
	queueLength := defaultQueueLength
	if options != nil && options.eventQueueLength > 0 {
		queueLength = options.eventQueueLength
	}

	metadataPersistFrequency := defaultMetadataPersistFrequency
	if options != nil && options.metaPersistFrequency != nil {
		metadataPersistFrequency = *options.metaPersistFrequency
	}

	eventQueue := make(chan streamEvent, queueLength)

	return &DCPWorker{
		ID:                    workerID,
		eventFeed:             eventQueue,
		terminator:            terminator,
		endSeqNos:             endSeqNos,
		checkpointPrefixBytes: []byte(checkpointPrefix),
		mutationCallback:      mutationCallback,
		endStreamCallback:     endCallback,
		ignoreDeletes:         options != nil && options.ignoreDeletes,
		metadata:              metadata,
		pendingSnapshot:       make(map[uint16]snapshotEvent),
		metaPersistFrequency:  metadataPersistFrequency,
		assignedVbs:           assignedVbs,
		idleCheckpointOps:     options != nil && options.idleCheckpointOps,
	}
}

// Send accepts incoming events from the DCP client and adds to the worker's buffered feed, to be processed by the main worker goroutine
func (w *DCPWorker) Send(ctx context.Context, event streamEvent) {
	// Ignore mutations if they come in after the client has started closing (CBG-2173)
	// This needs to be a separate select because if w.eventFeed has capacity at the same time as the terminator is closed,
	// the outcome is non-deterministic (https://go.dev/ref/spec#Select_statements)
	select {
	case <-w.terminator:
		TracefCtx(ctx, KeyDCP, "Ignoring stream event (vb:%d) as the client is closing", event.VbID())
		return
	default:
	}
	select {
	case w.eventFeed <- event:
	case <-w.terminator:
		InfofCtx(ctx, KeyDCP, "Closing DCP worker, DCP Client was closed")
	}
}

func (w *DCPWorker) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case event := <-w.eventFeed:
				w.handleEvent(ctx, event)
			case <-w.terminator:
				w.Close(ctx)
				return
			}
		}
	}()
}

// handleEvent processes a single stream event. Events for a given vbucket must be handled in order.
func (w *DCPWorker) handleEvent(ctx context.Context, event streamEvent) {
	vbID := event.VbID()
	switch e := event.(type) {
	case snapshotEvent:
		// Set pending snapshot - don't persist to meta until we receive first sequence in the snapshot,
		// to avoid attempting to restart with a new snapshot and old sequence value
		w.pendingSnapshot[vbID] = e
	case mutationEvent:
		shouldPersist := true
		if w.mutationCallback != nil {
			shouldPersist = w.mutationCallback(e.asFeedEvent())
		}
		w.updateSeq(ctx, e.key, vbID, e.seq, shouldPersist)
	case deletionEvent:
		shouldPersist := true
		if w.mutationCallback != nil && !w.ignoreDeletes {
			shouldPersist = w.mutationCallback(e.asFeedEvent())
		}
		w.updateSeq(ctx, e.key, vbID, e.seq, shouldPersist)
	case seqnoAdvancedEvent:
		// no callback requested a checkpoint, so the seq is persisted with the next requested checkpoint
		w.updateSeq(ctx, nil, vbID, e.seq, false)
	case endStreamEvent:
		w.endStreamCallback(e)
	}
}

func (w *DCPWorker) checkPendingSnapshot(vbID uint16) {
	if snapshot, ok := w.pendingSnapshot[vbID]; ok {
		w.metadata.SetSnapshot(snapshot)
		delete(w.pendingSnapshot, vbID)
	}
}

// updateSeq records seq as processed. If shouldPersist is set, it persists a checkpoint, or defers it to the end of the
// persist interval or to Close.
func (w *DCPWorker) updateSeq(ctx context.Context, key []byte, vbID uint16, seq uint64, shouldPersist bool) {
	// Ignore the echo of this feed's own checkpoint documents to avoid a persistence feedback loop
	if len(w.checkpointPrefixBytes) > 0 && bytes.HasPrefix(key, w.checkpointPrefixBytes) {
		return
	}

	// TODO: update snapshot and seq in a single atomic update
	w.checkPendingSnapshot(vbID)
	w.metadata.UpdateSeq(vbID, seq)
	if !bytes.HasPrefix(key, []byte(SyncDocPrefix)) {
		w.sawNonSyncDoc = true
	}

	if !shouldPersist {
		return
	}
	if time.Since(w.lastMetaPersistTime) <= w.metaPersistFrequency {
		w.pendingPersist = true
		return
	}
	w.persist(ctx)
}

func (w *DCPWorker) persist(ctx context.Context) {
	w.metadata.Persist(ctx, w.ID, w.assignedVbs)
	// Background work such as heartbeats writes _sync: docs even on a node with no clients. When only those were
	// seen, the checkpoint write is idle work too.
	if w.idleCheckpointOps && !w.sawNonSyncDoc {
		SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
	}
	w.sawNonSyncDoc = false
	w.lastMetaPersistTime = time.Now()
	w.pendingPersist = false
}

// Close persists any checkpoint that was deferred by metaPersistFrequency.
func (w *DCPWorker) Close(ctx context.Context) {
	if w.pendingPersist {
		w.persist(ctx)
	}
}
