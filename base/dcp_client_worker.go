package base

import (
	"bytes"

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
	endSeqNos             []uint64
	eventFeed             chan streamEvent
	terminator            chan bool
	checkpointPrefixBytes []byte
	mutationCallback      sgbucket.FeedEventCallbackFunc
	endStreamCallback     endStreamCallbackFunc
	ignoreDeletes         bool
	metadata              DCPMetadataStore
	pendingSnapshot       map[uint16]snapshotEvent
}

const defaultQueueLength = 10

type DCPWorkerOptions struct {
	eventQueueLength int
	ignoreDeletes    bool
}

func NewDCPWorker(metadata DCPMetadataStore, mutationCallback sgbucket.FeedEventCallbackFunc, endCallback endStreamCallbackFunc, terminator chan bool, endSeqNos []uint64, checkpointPrefix string, options *DCPWorkerOptions) *DCPWorker {

	// Create a buffered channel for queueing incoming DCP events
	queueLength := defaultQueueLength
	if options != nil && options.eventQueueLength > 0 {
		queueLength = options.eventQueueLength
	}
	eventQueue := make(chan streamEvent, queueLength)

	return &DCPWorker{
		eventFeed:             eventQueue,
		terminator:            terminator,
		endSeqNos:             endSeqNos,
		checkpointPrefixBytes: []byte(checkpointPrefix),
		mutationCallback:      mutationCallback,
		endStreamCallback:     endCallback,
		ignoreDeletes:         options != nil && options.ignoreDeletes,
		metadata:              metadata,
		pendingSnapshot:       make(map[uint16]snapshotEvent),
	}
}

// Send accepts incoming events from the DCP client and adds to the worker's buffered feed, to be processed by the main worker goroutine
func (w *DCPWorker) Send(event streamEvent) {
	select {
	case w.eventFeed <- event:
	case <-w.terminator:
		Infof(KeyDCP, "Closing DCP worker, DCP Client was closed")
	}
}

func (w *DCPWorker) Start() {

	go func() {
		for {
			select {
			case event := <-w.eventFeed:
				vbID := event.VbID()
				switch e := event.(type) {
				case snapshotEvent:
					// Set pending snapshot - don't persist to meta until we receive first sequence in the snapshot,
					// to avoid attempting to restart with a new snapshot and old sequence value
					w.pendingSnapshot[vbID] = e
				case mutationEvent:
					if w.mutationCallback != nil {
						w.mutationCallback(e.asFeedEvent())
					}
					w.updateSeq(e.key, vbID, e.seq)
				case deletionEvent:
					if w.mutationCallback != nil && !w.ignoreDeletes {
						w.mutationCallback(e.asFeedEvent())
					}
					w.updateSeq(e.key, vbID, e.seq)
				case endStreamEvent:
					w.endStreamCallback(e)
				}
			case <-w.terminator:
				w.Close()
				return
			}
		}
	}()
}

func (w *DCPWorker) checkPendingSnapshot(vbID uint16) {
	if snapshot, ok := w.pendingSnapshot[vbID]; ok {
		w.metadata.SetSnapshot(snapshot)
		delete(w.pendingSnapshot, vbID)
	}
}

func (w *DCPWorker) updateSeq(key []byte, vbID uint16, seq uint64) {
	// Ignore DCP checkpoint documents
	if bytes.HasPrefix(key, w.checkpointPrefixBytes) {
		return
	}

	// TODO: update snapshot and seq in a single atomic update
	w.checkPendingSnapshot(vbID)
	w.metadata.UpdateSeq(vbID, seq)

}

func (w *DCPWorker) Close() {
	// cleanup persistence
}
