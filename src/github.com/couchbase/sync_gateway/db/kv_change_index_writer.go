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
	"sync/atomic"
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
	go func() {
		err := k.indexPending()
		if err != nil {
			base.LogFatal("Indexer failed with unrecoverable error:%v", err)
		}

	}()

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

func (k *kvChangeIndexWriter) indexPending() error {

	// Read entries from the pending list into array
	entries := k.readFromPending()

	// Initialize partition map (lazy init)
	indexPartitions, err := k.indexPartitionsCallback()
	if err != nil {
		base.LogFatal("Unable to load index partition map - cannot write incoming entry to index")
	}

	// Generic channelStorage for log entry storage (if needed)
	channelStorage := NewChannelStorage(k.indexWriteBucket, "", indexPartitions)

	indexRetryCount := 0
	maxRetries := 15

	// Continual processing of arriving entries from the feed.
	var sleeper base.RetrySleeper
	for {
		latestWriteBatch.Set(int64(len(entries)))
		err := k.indexEntries(entries, indexPartitions.VbMap, channelStorage)
		if err != nil {
			if indexRetryCount == 0 {
				sleeper = base.CreateDoublingSleeperFunc(maxRetries, 5)
			}
			indexRetryCount++
			shouldContinue, sleepMs := sleeper(indexRetryCount)
			if !shouldContinue {
				return errors.New(fmt.Sprintf("Unable to successfully write to index after %d attempts", maxRetries))
			}
			<-time.After(time.Millisecond * time.Duration(sleepMs))
		} else {
			// Successful indexing, read next entries
			indexRetryCount = 0
			entries = k.readFromPending()
		}
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
func (k *kvChangeIndexWriter) indexEntries(entries []*LogEntry, indexPartitions base.IndexPartitionMap, channelStorage ChannelStorage) error {

	channelSets := make(map[string][]*LogEntry)
	updatedSequences := base.NewSequenceClockImpl()

	// Wait group tracks when the current buffer has been completely processed
	var entryWg sync.WaitGroup
	entryErrorCount := uint32(0)
	// Iterate over entries to write index entry docs, and group entries for subsequent channel index updates
	for _, logEntry := range entries {
		// If principal, update the stable sequence and continue
		if logEntry.IsPrincipal {
			updatedSequences.SetSequence(logEntry.VbNo, logEntry.Sequence)
			continue
		}

		// Remove channels from entry to save space in memory, index entries
		ch := logEntry.Channels

		// Add index log entry if needed
		if channelStorage.StoresLogEntries() {
			entryWg.Add(1)
			go func(logEntry *LogEntry, errorCount uint32) {
				defer entryWg.Done()
				err := channelStorage.WriteLogEntry(logEntry)
				if err != nil {
					// Unrecoverable error writing entry.  Increments error count for handling in main
					// indexEntries process, below.
					base.Warn("Error writing entry - entry set will be retried: %v", err)
					atomic.AddUint32(&errorCount, 1)
				}
			}(logEntry, entryErrorCount)
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
	entryWg.Wait()

	// Wait group tracks when the current buffer has been completely processed
	var channelWg sync.WaitGroup

	channelErrorCount := uint32(0)
	// Iterate over channel sets to update channel index
	for channelName, entrySet := range channelSets {
		channelWg.Add(1)
		go func(channelName string, entrySet []*LogEntry, errorCount uint32) {
			defer channelWg.Done()
			err := k.addSetToChannelIndex(channelName, entrySet)
			if err != nil {
				atomic.AddUint32(&errorCount, 1)
				// Unrecoverable error writing channel set.  Increments error count for handling in main
				// indexEntries process, below.
				base.Warn("Error writing channel set - entry set will be retried: %v", err)
			}
		}(channelName, entrySet, channelErrorCount)
	}

	// Wait for entry and channel processing to complete
	channelWg.Wait()
	if atomic.LoadUint32(&entryErrorCount) > 0 || atomic.LoadUint32(&channelErrorCount) > 0 {
		// Returns error.  Triggers retry handling in the caller.
		return errors.New("Unrecoverable error indexing entry or channel")
	}

	// Update stable sequence
	err := k.getWriterStableSequence().UpdateAndWrite(updatedSequences)
	return err
}

func (k *kvChangeIndexWriter) addSetToChannelIndex(channelName string, entries []*LogEntry) error {
	writer, err := k.getOrCreateWriter(channelName)
	if err != nil {
		base.Warn("Unable to obtain channel writer - partition map not defined?")
		return err
	}
	err = writer.AddSet(entries)
	if err != nil {
		base.Warn("Error writing %d entries for channel [%s]: %+v", len(entries), channelName, err)
		return err
	}
	return nil
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
		base.Warn("Error updating principal doc %s: %v", docID, err)
		return nil, errors.New(fmt.Sprintf("kvChangeIndex: Error updating principal doc %q: %v", docID, err))
	}

	// Increment the principal count, used for changes notification
	k.updatePrincipalCount(docID)

	// Add to cache so that the stable sequence gets updated
	logEntry := &LogEntry{
		Sequence:     sequence,
		DocID:        docID,
		TimeReceived: time.Now(),
		VbNo:         vbNo,
		IsPrincipal:  true,
	}

	writeHistogram(changeCacheExpvars, entryTime, "lag-processPrincipalDoc")
	return logEntry, nil
}

// updatePrincipalCount increments the counter for a principal, as well as the overall principal
// counter.  Called when a principal doc is processed over the DCP feed.
// Counters are used to trigger changes notifications on the reader side.
func (k *kvChangeIndexWriter) updatePrincipalCount(docID string) error {
	// Update principal count for this principal
	principalCountKey := fmt.Sprintf(base.KPrincipalCountKeyFormat, docID)
	_, err := k.indexWriteBucket.Incr(principalCountKey, 1, 1, 0)
	if err != nil {
		return err
	}
	// Update global principal change count
	_, err = k.indexWriteBucket.Incr(base.KTotalPrincipalCountKey, 1, 1, 0)
	return err
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
				select {
				case ok := <-unmarshalEntry.success:
					if ok {
						output <- unmarshalEntry.logEntry
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
