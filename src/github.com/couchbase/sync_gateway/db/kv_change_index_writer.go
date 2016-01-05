//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

type kvChangeIndexWriter struct {
	context                 *DatabaseContext           // Database context
	indexWriteBucket        base.Bucket                // Index bucket
	channelIndexWriters     map[string]*kvChannelIndex // Manages writes to channel. Map indexed by channel name.
	channelIndexWriterLock  sync.RWMutex               // Coordinates access to channel index writer map.
	pending                 chan *LogEntry             // Incoming changes, pending indexing
	unmarshalWorkers        []*unmarshalWorker         // Workers to unmarshal documents in parallel, while preserving vbucket ordering
	unmarshalWorkQueue      chan *unmarshalEntry       // Queue for concurrent processing of incoming docs
	writerStableSequence    *base.ShardedClock         // Stable sequence used during index writes.  Initialized on first write
	logsDisabled            bool                       // If true, ignore incoming tap changes
	indexPartitionsCallback IndexPartitionsFunc        // callback to retrieve the index partition map
	terminator              chan struct{}              // Terminates unmarshal processes
}

func (k *kvChangeIndexWriter) Init(context *DatabaseContext, options *CacheOptions, indexOptions *ChangeIndexOptions, indexPartitionsCallback IndexPartitionsFunc) (err error) {

	k.context = context
	k.pending = make(chan *LogEntry, maxCacheUpdate)

	k.indexPartitionsCallback = indexPartitionsCallback

	// start process to work pending sequences
	go k.indexPending()

	k.channelIndexWriters = make(map[string]*kvChannelIndex)
	k.indexWriteBucket, err = base.GetBucket(indexOptions.Spec, nil)
	if err != nil {
		base.Logf("Error opening index bucket %q, pool %q, server <%s>",
			indexOptions.Spec.BucketName, indexOptions.Spec.PoolName, indexOptions.Spec.Server)
		// TODO: revert to local index?
		return err
	}
	cbBucket, ok := k.indexWriteBucket.(base.CouchbaseBucket)
	var maxVbNo uint16
	if ok {
		maxVbNo, _ = cbBucket.GetMaxVbno()
	} else {
		// walrus, for unit testing
		maxVbNo = 1024
	}

	// Set of worker goroutines used to process incoming entries
	k.unmarshalWorkQueue = make(chan *unmarshalEntry, 500)

	// Start fixed set of goroutines to work the unmarshal work queue
	for i := 0; i < maxUnmarshalProcesses; i++ {
		go func() {
			for {
				select {
				case unmarshalEntry := <-k.unmarshalWorkQueue:
					unmarshalEntry.process()
				case <-k.terminator:
					return
				}
			}
		}()
	}

	// Initialize unmarshalWorkers
	k.unmarshalWorkers = make([]*unmarshalWorker, maxVbNo)

	return nil
}

func (k *kvChangeIndexWriter) Clear() {
	k.channelIndexWriters = make(map[string]*kvChannelIndex)
}

func (k *kvChangeIndexWriter) Stop() {
	if k.indexWriteBucket != nil {
		k.indexWriteBucket.Close()
	}
}

// If set to false, DocChanged() becomes a no-op.
func (k *kvChangeIndexWriter) EnableChannelIndexing(enable bool) {
	k.logsDisabled = !enable
}

// Initializes an empty stable sequence, with no partitions defined, for use by index writer.
// The current value for each partition is loaded lazily on partition usage/initialization.
func (k *kvChangeIndexWriter) initWriterStableSequence() (*base.ShardedClock, error) {
	partitions, err := k.indexPartitionsCallback()
	if err != nil {
		return nil, err
	}
	stableSequence := base.NewShardedClock(base.KStableSequenceKey, partitions, k.indexWriteBucket)
	stableSequence.Load()
	return stableSequence, nil
}

// Returns the stable sequence used for index writes.  If it hasn't been initialized, loads from
// bucket.
func (k *kvChangeIndexWriter) getWriterStableSequence() *base.ShardedClock {
	var err error
	if k.writerStableSequence == nil {
		k.writerStableSequence, err = k.initWriterStableSequence()
		if err != nil {
			base.LogFatal("Unable to initialize writer stable sequence")
		}
	}
	return k.writerStableSequence

}

func (k *kvChangeIndexWriter) addToCache(change *LogEntry) {
	// queue for cache addition
	base.LogTo("DIndex+", "Change Index: Adding Entry with Key [%s], VbNo [%d], Seq [%d]", change.DocID, change.VbNo, change.Sequence)
	k.pending <- change
	return
}

// Given a newly changed document (received from the feed), adds to the pending entries.
// The JSON must be the raw document from the bucket, with the metadata and all.
// NOTE: DocChanged is not thread-safe (due to k.unmarshalWorkers usage).  It should only
// be getting called from a single process per change index (i.e. DCP feed processing.)
func (k *kvChangeIndexWriter) DocChanged(docID string, docJSON []byte, seq uint64, vbNo uint16) {

	// Incoming docs are assigned to the appropriate unmarshalWorker for the vbucket, in order
	// to ensure docs are processed in sequence for a given vbucket.
	unmarshalWorker := k.unmarshalWorkers[vbNo]
	if unmarshalWorker == nil {
		// Initialize new worker that sends results to k.pending for processing by the indexPending loop
		unmarshalWorker = NewUnmarshalWorker(k.pending, k.unmarshalWorkQueue, k.terminator)
		k.unmarshalWorkers[vbNo] = unmarshalWorker
	}

	// Is this a user/role doc?
	if strings.HasPrefix(docID, auth.UserKeyPrefix) || strings.HasPrefix(docID, auth.RoleKeyPrefix) {
		unmarshalWorker.add(docID, docJSON, vbNo, seq, k.processPrincipalDoc)
		return
	}

	// Adds to the worker queue for processing, then returns.  Doesn't block unless
	// worker queue is full.
	unmarshalWorker.add(docID, docJSON, vbNo, seq, k.processDoc)
}

func (k *kvChangeIndexWriter) indexPending() {

	// Read entries from the pending list into array
	readPendingTime := time.Now()
	entries := k.readFromPending()

	// Initialize partition map (lazy init)
	indexPartitions, err := k.indexPartitionsCallback()
	if err != nil {
		base.LogFatal("Unable to load index partition map - cannot write incoming entry to index")
	}

	// Generic channelStorage for log entry storage (if needed)
	channelStorage := NewChannelStorage(k.indexWriteBucket, "", indexPartitions)

	// Continual processing of arriving entries from the feed.
	for {
		latestWriteBatch.Set(int64(len(entries)))
		indexTimingExpvars.Add("batch_readPending", time.Since(readPendingTime).Nanoseconds())
		indexTimingExpvars.Add("entryCount", int64(len(entries)))
		k.indexEntries(entries, indexPartitions.VbMap, channelStorage)
		// Read next entries
		readPendingTime = time.Now()
		entries = k.readFromPending()
	}
}

func (k *kvChangeIndexWriter) readFromPending() []*LogEntry {

	entries := make([]*LogEntry, 0, maxCacheUpdate)

	// TODO - needs cancellation handling?
	// Blocks until reading at least one from pending
	for {
		select {
		case entry := <-k.pending:
			entries = append(entries, entry)
			// read additional from cache if present, up to maxCacheUpdate
			for {
				select {
				case additionalEntry := <-k.pending:
					entries = append(entries, additionalEntry)
					if len(entries) > maxCacheUpdate {
						return entries
					}
				default:
					return entries
				}
			}

		}
	}
}

func (k *kvChangeIndexWriter) getOrCreateWriter(channelName string) (*kvChannelIndex, error) {
	var err error
	index := k.getChannelWriter(channelName)
	if index == nil {
		index, err = k.newChannelWriter(channelName)
	}
	return index, err
}

func (k *kvChangeIndexWriter) getChannelWriter(channelName string) *kvChannelIndex {

	k.channelIndexWriterLock.RLock()
	defer k.channelIndexWriterLock.RUnlock()
	return k.channelIndexWriters[channelName]
}

func (k *kvChangeIndexWriter) newChannelWriter(channelName string) (*kvChannelIndex, error) {

	k.channelIndexWriterLock.Lock()
	defer k.channelIndexWriterLock.Unlock()
	// make sure someone else hasn't created while we waited for the lock
	if _, ok := k.channelIndexWriters[channelName]; ok {
		return k.channelIndexWriters[channelName], nil
	}
	indexPartitions, err := k.indexPartitionsCallback()
	if err != nil {
		return nil, err
	}
	k.channelIndexWriters[channelName] = NewKvChannelIndex(channelName, k.indexWriteBucket, indexPartitions, nil)
	k.channelIndexWriters[channelName].setType("writer")
	return k.channelIndexWriters[channelName], nil
}

// Index a group of entries.  Iterates over the entry set to build updates per channel, then
// updates using channel index.
func (k *kvChangeIndexWriter) indexEntries(entries []*LogEntry, indexPartitions base.IndexPartitionMap, channelStorage ChannelStorage) {

	batchStart := time.Now()
	channelSets := make(map[string][]*LogEntry)
	updatedSequences := base.NewSequenceClockImpl()

	// Record a histogram of the batch sizes
	var batchSizeWindow int
	if len(entries) < 50 {
		batchSizeWindow = int(len(entries)/5) * 5
	} else {
		batchSizeWindow = int(len(entries)/50) * 50
	}
	indexTimingExpvars.Add(fmt.Sprintf("indexEntries-batchsize-%04d", batchSizeWindow), 1)

	// Wait group tracks when the current buffer has been completely processed
	var entryWg sync.WaitGroup
	// Iterate over entries to write index entry docs, and group entries for subsequent channel index updates
	for _, logEntry := range entries {
		// If principal, update the stable sequence and continue
		if logEntry.IsPrincipal {
			updatedSequences.SetSequence(logEntry.VbNo, logEntry.Sequence)
			continue
		}

		// Remove channels from entry to save space in memory, index entries
		ch := logEntry.Channels
		logEntry.Channels = nil

		// Add index log entry if needed
		if channelStorage.StoresLogEntries() {
			entryWg.Add(1)
			go func(logEntry *LogEntry) {
				defer entryWg.Done()
				channelStorage.WriteLogEntry(logEntry)
			}(logEntry)
		}
		// Collect entries by channel
		for channelName, removal := range ch {
			if removal == nil || removal.RevID == logEntry.RevID {
				// Store by channel and partition, to avoid having to iterate over results again in the channel index to group by partition
				_, found := channelSets[channelName]
				if !found {
					// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
					channelSets[channelName] = make([]*LogEntry, 0, maxCacheUpdate)
				}
				if removal != nil {
					removalEntry := *logEntry
					removalEntry.Flags |= channels.Removed
					channelSets[channelName] = append(channelSets[channelName], &removalEntry)
				} else {
					channelSets[channelName] = append(channelSets[channelName], logEntry)
				}
			}
		}
		if EnableStarChannelLog {
			_, found := channelSets[channels.UserStarChannel]
			if !found {
				// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
				channelSets[channels.UserStarChannel] = make([]*LogEntry, 0, maxCacheUpdate)
			}
			channelSets[channels.UserStarChannel] = append(channelSets[channels.UserStarChannel], logEntry)
		}

		// Track vbucket sequences for clock update
		updatedSequences.SetSequence(logEntry.VbNo, logEntry.Sequence)

	}

	// Wait group tracks when the current buffer has been completely processed
	var channelWg sync.WaitGroup
	channelStart := time.Now()
	// Iterate over channel sets to update channel index
	for channelName, entrySet := range channelSets {
		channelWg.Add(1)
		go func(channelName string, entrySet []*LogEntry) {
			defer channelWg.Done()
			k.addSetToChannelIndex(channelName, entrySet)

		}(channelName, entrySet)
	}

	// Wait for entry and channel processing to complete
	entryWg.Wait()
	channelWg.Wait()

	indexTimingExpvars.Add("batch_writeEntriesAndChannelBlocks", time.Since(batchStart).Nanoseconds())
	indexTimingExpvars.Add("batch_writeChannelBlocks", time.Since(channelStart).Nanoseconds())

	writeHistogram(indexTimingExpvars, batchStart, "indexEntries-entryAndChannelTime")

	stableStart := time.Now()
	// Update stable sequence
	err := k.getWriterStableSequence().UpdateAndWrite(updatedSequences)
	if err != nil {
		base.LogPanic("Error updating stable sequence", err)
	}

	indexTimingExpvars.Add("batch_updateStableSequence", time.Since(stableStart).Nanoseconds())
	writeHistogram(indexTimingExpvars, stableStart, "indexEntries-stableSeqTime")

	// TODO: remove - iterate once more for perf logging
	for _, logEntry := range entries {
		writeHistogram(indexTimingExpvars, logEntry.TimeReceived, "lag-indexing")
		writeHistogram(indexTimingExpvars, logEntry.TimeSaved, "lag-totalWrite")
	}

	writeHistogram(indexTimingExpvars, batchStart, "indexEntries-batchTime")
	indexTimingExpvars.Add("batch_totalBatchTime", time.Since(batchStart).Nanoseconds())
}

func (k *kvChangeIndexWriter) addSetToChannelIndex(channelName string, entries []*LogEntry) {
	writer, err := k.getOrCreateWriter(channelName)
	if err != nil {
		base.LogFatal("Unable to obtain channel writer - partition map not defined?")
	}
	err = writer.AddSet(entries)
	if err != nil {
		base.Warn("Error writing %d entries for channel [%s]: %+v", len(entries), channelName, err)
	}
}

type PrincipalIndex struct {
	VbNo             uint16            `json:"vbucket"`        // vbucket number for user doc - for convenience
	ExplicitChannels channels.TimedSet `json:"admin_channels"` // Timed Set of channel names to vbucket seq no of first user version that granted access
	ExplicitRoles    channels.TimedSet `json:"admin_roles"`    // Timed Set of role names to vbucket seq no of first user version that granted access
}

type processDocFunc func(docID string, docJSON []byte, vbNo uint16, seq uint64) (*LogEntry, error)

func (k *kvChangeIndexWriter) processDoc(docID string, docJSON []byte, vbNo uint16, seq uint64) (*LogEntry, error) {

	entryTime := time.Now()

	// First unmarshal the doc (just its metadata, to save time/memory):
	doc, err := unmarshalDocumentSyncData(docJSON, false)

	// Record histogram of time spent unmarshalling sync metadata
	writeHistogram(changeCacheExpvars, entryTime, "lag-unmarshal")

	if err != nil || !doc.hasValidSyncData(false) {
		base.Warn("ChangeCache: Error unmarshaling doc %q: %v", docID, err)
		return nil, err
	}

	// Record a histogram of the  feed's lag:
	feedLag := time.Since(doc.TimeSaved) - time.Since(entryTime)
	writeHistogramForDuration(changeCacheExpvars, feedLag, "lag-feed")

	// Now add the entry for the new doc revision:
	logEntry := &LogEntry{
		Sequence:     seq,
		DocID:        docID,
		RevID:        doc.CurrentRev,
		Flags:        doc.Flags,
		TimeReceived: time.Now(),
		TimeSaved:    doc.TimeSaved,
		Channels:     doc.Channels,
		VbNo:         uint16(vbNo),
	}
	base.LogTo("DIndex+", "Received #%d after %3dms (%q / %q)", logEntry.Sequence, int(feedLag/time.Millisecond), logEntry.DocID, logEntry.RevID)

	if logEntry.DocID == "" {
		base.Warn("Unexpected change with empty DocID for sequence %d, vbno:%d", doc.Sequence, vbNo)
		changeCacheExpvars.Add("changes_without_id", 1)
		return nil, errors.New(fmt.Sprintf("Unexpected change with empty DocID for sequence %d, vbno:%d", doc.Sequence, vbNo))
	}

	writeHistogram(changeCacheExpvars, entryTime, "lag-processDoc")

	return logEntry, nil
}

func (k *kvChangeIndexWriter) processPrincipalDoc(docID string, docJSON []byte, vbNo uint16, sequence uint64) (*LogEntry, error) {

	entryTime := time.Now()

	isUser := strings.HasPrefix(docID, auth.UserKeyPrefix)

	var err error
	if isUser {
		err = k.context.Authenticator().UpdateUserVbucketSequences(docID, sequence)
	} else {
		err = k.context.Authenticator().UpdateRoleVbucketSequences(docID, sequence)
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf("kvChangeIndex: Error updating principal doc %q: %v", docID, err))
	}

	// Add to cache so that the stable sequence gets updated
	logEntry := &LogEntry{
		Sequence:     sequence,
		DocID:        docID,
		TimeReceived: time.Now(),
		VbNo:         uint16(vbNo),
		IsPrincipal:  true,
	}

	writeHistogram(changeCacheExpvars, entryTime, "lag-processPrincipalDoc")
	return logEntry, nil
}

type unmarshalWorker struct {
	output             chan<- *LogEntry
	processing         chan *unmarshalEntry
	unmarshalWorkQueue chan<- *unmarshalEntry
}

// UnmarshalWorker is used to unmarshal incoming entries from the DCP feed in parallel, while maintaining ordering of
// sequences per vbucket.  The main DocChanged loop creates one UnmarshalWorker per vbucket.
func NewUnmarshalWorker(output chan<- *LogEntry, unmarshalWorkQueue chan<- *unmarshalEntry, terminator chan struct{}) *unmarshalWorker {

	// goroutine to work the processing channel and return completed entries
	// to the output channel
	maxProcessing := 50

	worker := &unmarshalWorker{
		output:             output,
		processing:         make(chan *unmarshalEntry, maxProcessing),
		unmarshalWorkQueue: unmarshalWorkQueue,
	}

	// Start goroutine to work the worker's processing channel.  When an entry arrives on the
	// processing channel, blocks waiting for success/fail for that document.
	go func(worker *unmarshalWorker, terminator chan struct{}) {
		for {
			select {
			case unmarshalEntry := <-worker.processing:
				// Wait for entry processing to be done
				entryTime := time.Now()
				select {
				case ok := <-unmarshalEntry.success:
					if ok {
						base.LogTo("DIndex+", "Change Index: Adding Entry with Key [%s], VbNo [%d], Seq [%d]", unmarshalEntry.logEntry.DocID, unmarshalEntry.logEntry.VbNo, unmarshalEntry.logEntry.Sequence)
						writeHistogram(indexTimingExpvars, entryTime, "lag-waitForSuccess")
						writeHistogram(indexTimingExpvars, unmarshalEntry.logEntry.TimeReceived, "lag-readyForPending")
						outputStart := time.Now()
						output <- unmarshalEntry.logEntry
						writeHistogram(indexTimingExpvars, outputStart, "lag-processedToOutput")
						writeHistogram(indexTimingExpvars, unmarshalEntry.logEntry.TimeReceived, "lag-incomingToPending")

					} else {
						changeCacheExpvars.Add("unmarshalEntry_success_false", 1)
						// error already logged - just ignore the entry
					}
				case <-terminator:
					return

				}
			case <-terminator:
				return
			}
		}
	}(worker, terminator)

	return worker
}

// Add document to the ordered processing results channel, and start goroutine to do processing work.
func (uw *unmarshalWorker) add(docID string, docJSON []byte, vbNo uint16, seq uint64, callback processDocFunc) {
	// create new unmarshal entry
	unmarshalEntry := NewUnmarshalEntry(docID, docJSON, vbNo, seq, callback)

	// Add it to the ordering queue.  Will block if we're at limit for concurrent processing for this vbucket (maxProcessing).
	// TODO: This means that one full vbucket queue will block additions to all others (as the main DocChanged blocks on
	// call to this method).  Leaving as-is to avoid complexity, and as it's probably reasonable for hotspot vbs to get priority.
	uw.processing <- unmarshalEntry

	// Send the entry to the entryWorker channel to get processed by the kvChangeIndex entry processors
	uw.unmarshalWorkQueue <- unmarshalEntry
}

// An incoming document being unmarshalled by unmarshalWorker
type unmarshalEntry struct {
	entryTime time.Time
	logEntry  *LogEntry
	docID     string
	docJSON   []byte
	vbNo      uint16
	seq       uint64
	callback  processDocFunc
	success   chan bool
	err       error
}

// UnmarshalEntry represents a document being processed by an UnmarshalWorker.
func NewUnmarshalEntry(docID string, docJSON []byte, vbNo uint16, seq uint64, callback processDocFunc) *unmarshalEntry {

	return &unmarshalEntry{
		entryTime: time.Now(),
		success:   make(chan bool),
		docID:     docID,
		docJSON:   docJSON,
		vbNo:      vbNo,
		seq:       seq,
		callback:  callback,
	}
}

// UnmarshalEntry.process executes the processing (via callback), then updates the entry's success channel on completion
func (e *unmarshalEntry) process() {

	// Do the processing work
	e.logEntry, e.err = e.callback(e.docID, e.docJSON, e.vbNo, e.seq)

	// Return completion status on the success channel
	if e.err != nil || e.logEntry == nil {
		e.success <- false
	} else {
		e.success <- true
	}
}
