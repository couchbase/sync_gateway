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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbgt"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	kIndexPrefix       = "_idx"
	maxCacheUpdate     = 2000
	minCacheUpdate     = 1
	kPollFrequency     = 500
	kIndexPartitionKey = "_idxPartitionMap"
)

type kvChangeIndex struct {
	context                  *DatabaseContext           // Database context
	indexReadBucket              base.Bucket                // Index bucket
	indexWriteBucket              base.Bucket                // Index bucket
	maxVbNo                  uint16                     // Number of vbuckets
	indexPartitions          IndexPartitionMap          // Partitioning of vbuckets in the index
	channelIndexWriters      map[string]*kvChannelIndex // Manages writes to channel. Map indexed by channel name.
	channelIndexWriterLock   sync.RWMutex               // Coordinates access to channel index writer map.
	channelIndexReaders      map[string]*kvChannelIndex // Manages read access to channel.  Map indexed by channel name.
	channelIndexReaderLock   sync.RWMutex               // Coordinates read access to channel index reader map
	onChange                 func(base.Set)             // Client callback that notifies of channel changes
	pending                  chan *LogEntry             // Incoming changes, pending indexing
	unmarshalWorkers         []*unmarshalWorker         // Workers to unmarshal documents in parallel, while preserving vbucket ordering
	stableSequence           *base.SyncSequenceClock    // Stable sequence in index
	logsDisabled             bool                       // If true, ignore incoming tap changes
	lastPolledStableSequence base.SequenceClock         // Used to identify changes
	terminator               chan struct{}              // Ends polling
}

type IndexPartitionMap map[uint16]uint16 // Maps vbuckets to index partition value

type partitionStorage struct {
	Uuid  string   `json:"uuid"`
	Index uint16   `json:"index"`
	VbNos []uint16 `json:"vbNos"`
}

var indexExpvars *expvar.Map

var indexTimingExpvars *expvar.Map
var latestWriteBatch expvar.Int

func init() {
	indexExpvars = expvar.NewMap("syncGateway_index")
	indexExpvars.Set("latest_write_batch", &latestWriteBatch)
	indexTimingExpvars = expvar.NewMap("syncGateway_indexTiming")
}
////// Cache writer API

func (k *kvChangeIndex) Init(context *DatabaseContext, lastSequence SequenceID, onChange func(base.Set), options *CacheOptions, indexOptions *ChangeIndexOptions) (err error){

	// not sure yet whether we'll need initial sequence
	k.channelIndexWriters = make(map[string]*kvChannelIndex)
	k.channelIndexReaders = make(map[string]*kvChannelIndex)
	k.pending = make(chan *LogEntry, maxCacheUpdate)

	k.context = context

	k.indexReadBucket, err = base.GetBucket(indexOptions.Spec)
	if err != nil {
		base.Logf("Error opening index bucket %q, pool %q, server <%s>",
			indexOptions.Spec.BucketName, indexOptions.Spec.PoolName, indexOptions.Spec.Server)
		// TODO: revert to local index?
		return err
	}

	if indexOptions.Writer {
		k.indexWriteBucket, err = base.GetBucket(indexOptions.Spec)
		if err != nil {
			base.Logf("Error opening index bucket %q, pool %q, server <%s>",
				indexOptions.Spec.BucketName, indexOptions.Spec.PoolName, indexOptions.Spec.Server)
			// TODO: revert to local index?
			return err
		}
	}


	cbBucket, ok := k.indexReadBucket.(base.CouchbaseBucket)
	if ok {
		k.maxVbNo, _ = cbBucket.GetMaxVbno()
	} else {
		// walrus, for unit testing
		k.maxVbNo = 1024
	}

	// Initialize stable sequence
	k.stableSequence = k.loadStableClock()

	// Initialize last polled stable sequence
	k.lastPolledStableSequence = base.NewSequenceClockImpl()
	k.lastPolledStableSequence.SetTo(k.stableSequence)

	// Initialize notification Callback
	k.onChange = onChange

	// start process to work pending sequences
	go k.indexPending()

	// Start background task to poll for changes
	k.terminator = make(chan struct{})
	go func(k *kvChangeIndex) {
		for {
			select {
			case <-time.After(kPollFrequency * time.Millisecond):
				// TODO: Doesn't trigger the reader removal processing (in pollReaders) during long
				//       periods without changes to stableSequence.  In that scenario we'll continue
				//       stable sequence polling each poll interval, even if we *actually* don't have any
				//       active readers.
				if k.hasActiveReaders() && k.stableSequenceChanged() {
					k.pollReaders()
				}
			case <-k.terminator:
				return
			}
		}
	}(k)

	// Initialize unmarshalWorkers
	k.unmarshalWorkers = make([]*unmarshalWorker, k.maxVbNo)

	return nil
}

func (k *kvChangeIndex) Prune() {
	// TODO: currently no pruning of channel indexes
}

func (k *kvChangeIndex) Clear() {
	// TODO: Currently no clear for distributed cache
	// temp handling until implemented, to pass unit tests
	k.channelIndexWriters = make(map[string]*kvChannelIndex)
	k.channelIndexReaders = make(map[string]*kvChannelIndex)
}

func (k *kvChangeIndex) Stop() {
	close(k.terminator)
	if k.indexReadBucket != nil {
		k.indexReadBucket.Close()
	}
	if k.indexWriteBucket != nil {
		k.indexWriteBucket.Close()
	}
}

// Returns the stable sequence for a document from the stable clock, based on the vbucket for the document.
// Used during document write for handling deduplicated sequences on the DCP feed.
func (k *kvChangeIndex) GetStableSequence(docID string) SequenceID {

	vbNo := k.indexReadBucket.VBHash(docID)
	return SequenceID{Seq: k.stableSequence.GetSequence(uint16(vbNo))}

}

// Returns the cached stable sequence clock for the index
func (k *kvChangeIndex) getStableClock() base.SequenceClock {
	return k.stableSequence.Clock
}

func (k *kvChangeIndex) GetStableClock() (clock base.SequenceClock, err error) {
	return k.loadStableClock(), nil
}

func (k *kvChangeIndex) loadStableClock() *base.SyncSequenceClock {
	clock := base.NewSyncSequenceClock()
	value, cas, err := k.indexReadBucket.GetRaw(base.KStableSequenceKey)
	indexExpvars.Add("get_loadStableClock", 1)
	if err != nil {
		base.Warn("Stable sequence not found in index - treating as 0")
	}
	clock.Unmarshal(value)
	clock.SetCas(cas)
	base.LogTo("Feed", "loadStableClock:%s", base.PrintClock(clock))
	return clock
}

func (k *kvChangeIndex) addToCache(change *LogEntry) {
	// queue for cache addition
	base.LogTo("DCache+", "Change Index: Adding Entry with Key [%s], VbNo [%d], Seq [%d]", change.DocID, change.VbNo, change.Sequence)
	k.pending <- change
	return
}

func (k *kvChangeIndex) getIndexPartitionMap() (IndexPartitionMap, error) {

	if k.indexPartitions == nil || len(k.indexPartitions) == 0 {
		var partitionDef []partitionStorage
		// First attempt to load from the bucket
		value, _, err := k.indexReadBucket.GetRaw(kIndexPartitionKey)
		indexExpvars.Add("get_indexPartitionMap", 1)
		if err == nil {
			if err = json.Unmarshal(value, &partitionDef); err != nil {
				return nil, err
			}
		}

		// If unable to load from index bucket - attempt to initialize based on cbgt partitions
		if partitionDef == nil {
			var manager *cbgt.Manager
			if k.context != nil {
				manager = k.context.BucketSpec.CbgtContext.Manager
			} else {
				base.Warn("No database context - using generic partition map")
				return k.testIndexPartitionMap(), nil
			}

			if manager == nil {
				if _, ok := k.context.Bucket.(base.CouchbaseBucket); ok {
					return nil, errors.New("No map definition in index bucket, and no cbgt manager available")
				} else {
					base.Warn("Non-couchbase bucket - using generic partition map")
					return k.testIndexPartitionMap(), nil
				}
			}

			_, planPIndexesByName, _ := manager.GetPlanPIndexes(true)
			indexName := k.context.GetCBGTIndexNameForBucket(k.context.Bucket)
			pindexes := planPIndexesByName[indexName]

			for index, pIndex := range pindexes {
				vbStrings := strings.Split(pIndex.SourcePartitions, ",")
				// convert string vbNos to uint16
				vbNos := make([]uint16, len(vbStrings))
				for i := 0; i < len(vbStrings); i++ {
					vbNumber, err := strconv.ParseUint(vbStrings[i], 10, 16)
					if err != nil {
						base.LogFatal("Error creating index partition definition - unable to parse vbucket number %s as integer:%v", vbStrings[i], err)
					}
					vbNos[i] = uint16(vbNumber)
				}
				entry := partitionStorage{
					Index: uint16(index),
					Uuid:  pIndex.UUID,
					VbNos: vbNos,
				}
				partitionDef = append(partitionDef, entry)
			}

			// Persist to bucket
			value, err = json.Marshal(partitionDef)
			if err != nil {
				return nil, err
			}
			k.indexReadBucket.SetRaw(kIndexPartitionKey, 0, value)
		}

		// Create k.indexPartitions based on partitionDef
		k.indexPartitions = make(map[uint16]uint16)
		for _, partition := range partitionDef {
			for _, vbNo := range partition.VbNos {
				k.indexPartitions[vbNo] = partition.Index
			}
		}
	}
	return k.indexPartitions, nil
}

// Index partition map for testing
func (k *kvChangeIndex) testIndexPartitionMap() IndexPartitionMap {
	maxVbNo := uint16(1024)
	partitions := make(IndexPartitionMap, maxVbNo)

	numPartitions := uint16(32)
	vbPerPartition := maxVbNo / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			partitions[vb] = partition
		}
	}
	return partitions
}

// Primarily for use by unit tests to avoid CBGT dependency for partition map
func (k *kvChangeIndex) setIndexPartitionMap(partitionMap IndexPartitionMap) {
	k.indexPartitions = partitionMap
}

// No-ops - pending refactoring of change_cache.go to remove usage (or deprecation of
// change_cache altogether)
func (k *kvChangeIndex) getOldestSkippedSequence() uint64 {
	return uint64(0)
}
func (k *kvChangeIndex) getChannelCache(channelName string) *channelCache {
	return nil
}

// TODO: refactor waitForSequence to accept either vbNo or clock
func (k *kvChangeIndex) waitForSequenceID(sequence SequenceID) {
	k.waitForSequence(sequence.Seq)
}
func (k *kvChangeIndex) waitForSequence(sequence uint64) {
	return
}
func (k *kvChangeIndex) waitForSequenceWithMissing(sequence uint64) {
	k.waitForSequence(sequence)
}

// If set to false, DocChanged() becomes a no-op.
func (k *kvChangeIndex) EnableChannelIndexing(enable bool) {
	k.logsDisabled = !enable
}

// Given a newly changed document (received from the feed), adds to the pending entries.
// The JSON must be the raw document from the bucket, with the metadata and all.
// NOTE: DocChanged is not thread-safe (due to k.unmarshalWorkers usage).  It should only
// be getting called from a single process per change index (i.e. DCP feed processing.)
func (k *kvChangeIndex) DocChanged(docID string, docJSON []byte, seq uint64, vbNo uint16) {

	// Incoming docs are assigned to the appropriate unmarshalWorker for the vbucket, in order
	// to ensure docs are processed in sequence for a given vbucket.
	unmarshalWorker := k.unmarshalWorkers[vbNo]
	if unmarshalWorker == nil {
		// Initialize new worker that sends results to k.pending for processing by the indexPending loop
		unmarshalWorker = NewUnmarshalWorker(k.pending)
		k.unmarshalWorkers[vbNo] = unmarshalWorker
	}

	// Is this a user/role doc?
	if strings.HasPrefix(docID, auth.UserKeyPrefix) || strings.HasPrefix(docID, auth.RoleKeyPrefix){
		unmarshalWorker.add(docID, docJSON, vbNo, seq, k.processPrincipalDoc)
		return
	}

	// Adds to the worker queue for processing, then returns.  Doesn't block unless
	// worker queue is full.
	unmarshalWorker.add(docID, docJSON, vbNo, seq, k.processDoc)

}

func (k *kvChangeIndex) getChannelWriter(channelName string) *kvChannelIndex {

	k.channelIndexWriterLock.RLock()
	defer k.channelIndexWriterLock.RUnlock()
	return k.channelIndexWriters[channelName]
}

func (k *kvChangeIndex) getChannelReader(channelName string) *kvChannelIndex {

	k.channelIndexReaderLock.RLock()
	defer k.channelIndexReaderLock.RUnlock()
	return k.channelIndexReaders[channelName]
}

func (k *kvChangeIndex) newChannelReader(channelName string) (*kvChannelIndex, error) {

	k.channelIndexReaderLock.Lock()
	defer k.channelIndexReaderLock.Unlock()
	// make sure someone else hasn't created while we waited for the lock
	if _, ok := k.channelIndexReaders[channelName]; ok {
		return k.channelIndexReaders[channelName], nil
	}
	indexPartitions, err := k.getIndexPartitionMap()
	if err != nil {
		return nil, err
	}
	k.channelIndexReaders[channelName] = NewKvChannelIndex(channelName, k.indexReadBucket, indexPartitions, k.getStableClock, k.onChange)
	k.channelIndexReaders[channelName].setType("reader")
	indexExpvars.Add("polling_readers_added", 1)
	return k.channelIndexReaders[channelName], nil
}

func (k *kvChangeIndex) newChannelWriter(channelName string) (*kvChannelIndex, error) {

	k.channelIndexWriterLock.Lock()
	defer k.channelIndexWriterLock.Unlock()
	// make sure someone else hasn't created while we waited for the lock
	if _, ok := k.channelIndexWriters[channelName]; ok {
		return k.channelIndexWriters[channelName], nil
	}
	indexPartitions, err := k.getIndexPartitionMap()
	if err != nil {
		return nil, err
	}
	k.channelIndexWriters[channelName] = NewKvChannelIndex(channelName, k.indexWriteBucket, indexPartitions, k.getStableClock, nil)
	k.channelIndexWriters[channelName].setType("writer")
	return k.channelIndexWriters[channelName], nil
}

func (k *kvChangeIndex) getOrCreateReader(channelName string, options ChangesOptions) (*kvChannelIndex, error) {

	// For continuous or longpoll processing, use the shared reader from the channelindexReaders map to coordinate
	// polling.
	if options.Wait {
		var err error
		index := k.getChannelReader(channelName)
		if index == nil {
			index, err = k.newChannelReader(channelName)
			indexExpvars.Add("getOrCreateReader_create", 1)
			base.LogTo("DIndex+", "getOrCreateReader: Created new reader for channel %s", channelName)
		} else {
			indexExpvars.Add("getOrCreateReader_get", 1)
			base.LogTo("DIndex+", "getOrCreateReader: Using existing reader for channel %s", channelName)
		}
		return index, err
	} else {
		// For non-continuous/non-longpoll, use a one-off reader, no onChange handling.
		indexPartitions, err := k.getIndexPartitionMap()
		if err != nil {
			return nil, err
		}
		return NewKvChannelIndex(channelName, k.indexReadBucket, indexPartitions, k.getStableClock, nil), nil

	}
}

func (k *kvChangeIndex) getOrCreateWriter(channelName string) (*kvChannelIndex, error) {
	var err error
	index := k.getChannelWriter(channelName)
	if index == nil {
		index, err = k.newChannelWriter(channelName)
	}
	return index, err
}

func (k *kvChangeIndex) readFromPending() []*LogEntry {

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

func (k *kvChangeIndex) indexPending() {

	// Read entries from the pending list into array
	readPendingTime := time.Now()
	entries := k.readFromPending()

	// Initialize partition map (lazy init)
	indexPartitions, err := k.getIndexPartitionMap()
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
		k.indexEntries(entries, indexPartitions, channelStorage)
		// Read next entries
		readPendingTime = time.Now()
		entries = k.readFromPending()
	}
}

// Index a group of entries.  Iterates over the entry set to build updates per channel, then
// updates using channel index.
func (k *kvChangeIndex) indexEntries(entries []*LogEntry, indexPartitions IndexPartitionMap, channelStorage ChannelStorage) {


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

		// Add index log entry if needed
		if channelStorage.StoresLogEntries() {
			entryWg.Add(1)
			go func(logEntry *LogEntry) {
				defer entryWg.Done()
				channelStorage.WriteLogEntry(logEntry)
			}(logEntry)
		}
		// Collect entries by channel
		// Remove channels from entry to save space in memory, index entries
		ch := logEntry.Channels
		logEntry.Channels = nil
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
	err := k.updateStableSequence(updatedSequences)
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

// TODO: If mutex read lock is too much overhead every time we poll, could manage numReaders using
// atomic uint64
func (k *kvChangeIndex) hasActiveReaders() bool {
	k.channelIndexReaderLock.RLock()
	defer k.channelIndexReaderLock.RUnlock()
	return len(k.channelIndexReaders) > 0
}

func (k *kvChangeIndex) stableSequenceChanged() bool {

	value, cas, err := k.indexReadBucket.GetRaw(base.KStableSequenceKey)
	indexExpvars.Add("get_stableSequenceChanged", 1)
	if err != nil {
		base.Warn("Error loading stable sequence - skipping polling. Error:%v", err)
		return false
	}

	if cas == k.lastPolledStableSequence.Cas() {
		return false
	}

	// Stable sequence has changed.  Update lastPolled and return
	k.lastPolledStableSequence.Unmarshal(value)
	k.lastPolledStableSequence.SetCas(cas)
	return true

}

func (k *kvChangeIndex) pollReaders() bool {
	k.channelIndexReaderLock.Lock()
	defer k.channelIndexReaderLock.Unlock()

	if len(k.channelIndexReaders) == 0 {
		return true
	}

	// Build the set of clock keys to retrieve.  Stable sequence, plus one per channel reader
	keySet := make([]string, len(k.channelIndexReaders))
	index := 0
	for _, reader := range k.channelIndexReaders {
		keySet[index] = getChannelClockKey(reader.channelName)
		index++
	}
	bulkGetResults, err := k.indexReadBucket.GetBulkRaw(keySet)

	if err != nil {
		base.Warn("Error retrieving channel clocks: %v", err)
	}
	indexExpvars.Add("bulkGet_channelClocks", 1)
	indexExpvars.Add("bulkGet_channelClocks_keyCount", int64(len(keySet)))

	changedChannels := make(chan string, len(k.channelIndexReaders))
	cancelledChannels := make(chan string, len(k.channelIndexReaders))
	var wg sync.WaitGroup
	for _, reader := range k.channelIndexReaders {
		// For each channel, unmarshal new channel clock, then check with reader whether this represents changes
		wg.Add(1)
		go func(reader *kvChannelIndex, wg *sync.WaitGroup) {
			defer func() {
				wg.Done()
			}()

			// Unmarshal channel clock.  If not present in the bulk get results, use empty clock to support
			// channels that don't have any indexed data yet.  If clock was previously found successfully (i.e. empty clock is
			// due to temporary error from server), empty clock treated safely as a non-update by pollForChanges.
			clockKey := getChannelClockKey(reader.channelName)
			var newChannelClock *base.SequenceClockImpl
			clockBytes, found := bulkGetResults[clockKey]
			if !found {
				newChannelClock = base.NewSequenceClockImpl()
			} else {
				var err error
				newChannelClock, err = base.NewSequenceClockForBytes(clockBytes)
				if err != nil {
					base.Warn("Error unmarshalling channel clock - skipping polling for channel %s: %v", reader.channelName, err)
					return
				}
			}
			// Poll for changes
			hasChanges, cancelPolling := reader.pollForChanges(k.lastPolledStableSequence, newChannelClock)
			if hasChanges {
				changedChannels <- reader.channelName
			}
			if cancelPolling {
				cancelledChannels <- reader.channelName
			}

		}(reader, &wg)
	}

	wg.Wait()
	close(changedChannels)
	close(cancelledChannels)

	// Build channel set from the changed channels
	var channels []string
	for channelName := range changedChannels {
		channels = append(channels, channelName)
	}

	if len(channels) > 0 && k.onChange != nil {
		k.onChange(base.SetFromArray(channels))
	}

	// Remove cancelled channels from channel readers
	for channelName := range cancelledChannels {
		indexExpvars.Add("polling_readers_removed", 1)
		delete(k.channelIndexReaders, channelName)
	}

	return true
}

func (k *kvChangeIndex) SetNotifier(onChange func(base.Set)) {
	k.onChange = onChange
}

func (k *kvChangeIndex) GetChanges(channelName string, options ChangesOptions) (entries []*LogEntry, err error) {

	_, resultFromCache := k.GetCachedChanges(channelName, options)
	return resultFromCache, nil
}

func (k *kvChangeIndex) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {

	if options.Since.Clock == nil {
		options.Since.Clock = base.NewSequenceClockImpl()
	}

	reader, err := k.getOrCreateReader(channelName, options)
	if err != nil {
		base.Warn("Error obtaining channel reader (need partition index?) for channel %s", channelName)
		return uint64(0), nil
	}
	changes, err := reader.getChanges(options.Since.Clock)
	if err != nil {
		base.Warn("Error retrieving cached changes for channel %s", channelName)
		return uint64(0), nil
	}

	// Limit handling
	if options.Limit > 0 && len(changes) > options.Limit {
		limitResult := make([]*LogEntry, options.Limit)
		copy(limitResult[0:], changes[0:])
		return uint64(0), limitResult
	}

	// todo: Set validFrom when we enable pruning/compacting cache
	return uint64(0), changes
}

func (k *kvChangeIndex) InitLateSequenceClient(channelName string) uint64 {
	// no-op when not buffering sequences
	return 0
}

func (k *kvChangeIndex) GetLateSequencesSince(channelName string, sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {
	// no-op when not buffering sequences
	return entries, lastSequence, nil
}

func (k *kvChangeIndex) ReleaseLateSequenceClient(channelName string, sequence uint64) error {
	// no-op when not buffering sequences
	return nil
}

func (k *kvChangeIndex) addSetToChannelIndex(channelName string, entries []*LogEntry) {
	writer, err := k.getOrCreateWriter(channelName)
	if err != nil {
		base.LogFatal("Unable to obtain channel writer - partition map not defined?")
	}
	err = writer.AddSet(entries)
	if err != nil {
		base.Warn("Error writing %d entries for channel [%s]: %+v", len(entries), channelName, err)
	}
}

// Add late sequence information to channel cache
func (k *kvChangeIndex) addLateSequence(channelName string, change *LogEntry) error {
	// TODO: no-op for now
	return nil
}

func (k *kvChangeIndex) updateStableSequence(updates base.SequenceClock) error {

	// Initial set, for the first cas update attempt
	indexTimingExpvars.Add("updateStableSequence-count", 1)
	k.stableSequence.UpdateWithClock(updates)
	value, err := k.stableSequence.Marshal()
	if err != nil {
		return err
	}
	casOut, err := writeCasRaw(k.indexWriteBucket, base.KStableSequenceKey, value, k.stableSequence.Cas(), 0, func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times

		indexTimingExpvars.Add("updateStableSequence-casRetryCount", 1)
		err = k.stableSequence.Unmarshal(value)
		if err != nil {
			return nil, err
		}
		k.stableSequence.UpdateWithClock(updates)
		return k.stableSequence.Marshal()
	})
	k.stableSequence.SetCas(casOut)
	return nil
}

//////////  FakeBucket for testing KV flows:
type FakeBucket struct {
	docs map[string][]byte
}

func (fb *FakeBucket) Init() {
	fb.docs = make(map[string][]byte)
}

func (fb *FakeBucket) Put(key string, value []byte) error {
	fb.docs[key] = value
	return nil
}

func (fb *FakeBucket) Get(key string) ([]byte, error) {
	result, found := fb.docs[key]
	if !found {
		return result, errors.New("not found")
	}
	return result, nil
}

func (fb *FakeBucket) Incr(key string, amount int) error {

	var value uint64
	byteValue, found := fb.docs[key]
	if !found {
		value = 0
	}
	value = byteToUint64(byteValue)
	value += uint64(amount)

	return fb.Put(key, uint64ToByte(value))
}

func (fb *FakeBucket) SetIfGreater(key string, newValue uint64) {

	byteValue, found := fb.docs[key]
	if !found {
		fb.Put(key, uint64ToByte(newValue))
		return
	}
	value := byteToUint64(byteValue)
	if value < newValue {
		fb.Put(key, uint64ToByte(newValue))
	}
}

// utils for int/byte

func byteToUint64(input []byte) uint64 {
	readBuffer := bytes.NewReader(input)
	var result uint64
	err := binary.Read(readBuffer, binary.LittleEndian, &result)
	if err != nil {
		base.LogTo("DCache", "byteToUint64 error:%v", err)
	}
	return result
}

func uint64ToByte(input uint64) []byte {
	result := new(bytes.Buffer)
	binary.Write(result, binary.LittleEndian, input)
	return result.Bytes()
}

// debug API

type AllChannelStats struct {
	Channels []ChannelStats `json:"channels"`
}
type ChannelStats struct {
	Name         string              `json:"channel_name"`
	IndexStats   ChannelIndexStats   `json:"index,omitempty"`
	PollingStats ChannelPollingStats `json:"poll,omitempty"`
}

type ChannelIndexStats struct {
	Clock     string `json:"index_clock,omitempty"`
	ClockHash uint64 `json:"index_clock_hash,omitempty"`
}
type ChannelPollingStats struct {
	Clock     string `json:"poll_clock,omitempty"`
	ClockHash uint64 `json:"poll_clock_hash,omitempty"`
}

func (db *DatabaseContext) IndexChannelStats(channelName string) (*ChannelStats, error) {

	kvIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return nil, errors.New("No channel index in use")
	}

	return db.singleChannelStats(kvIndex, channelName)

}

func (db *DatabaseContext) IndexAllChannelStats() ([]*ChannelStats, error) {

	kvIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return nil, errors.New("No channel index in use")
	}

	/*	results := &AllChannelStats{
		Channels: make([]*ChannelStats, len(kvIndex.channelIndexReaders)),
	}*/
	results := make([]*ChannelStats, 0)

	for channelName, _ := range kvIndex.channelIndexReaders {
		channelStats, err := db.singleChannelStats(kvIndex, channelName)
		if err == nil {
			results = append(results, channelStats)
		}
	}
	return results, nil

}

func (db *DatabaseContext) singleChannelStats(kvIndex *kvChangeIndex, channelName string) (*ChannelStats, error) {

	channelStats := &ChannelStats{
		Name: channelName,
	}

	// Create a clean channel reader to retrieve bucket index stats
	indexPartitions, err := kvIndex.getIndexPartitionMap()
	if err != nil {
		return nil, err
	}

	// Retrieve index stats from bucket
	channelIndex := NewKvChannelIndex(channelName, kvIndex.indexReadBucket, indexPartitions, kvIndex.getStableClock, nil)
	indexClock, err := channelIndex.loadChannelClock()
	if err == nil {
		channelStats.IndexStats = ChannelIndexStats{}
		channelStats.IndexStats.Clock = base.PrintClock(indexClock)
		channelStats.IndexStats.ClockHash = db.SequenceHasher.calculateHash(indexClock)
	}

	// Retrieve polling stats from kvIndex
	pollingChannelIndex := kvIndex.getChannelReader(channelName)
	if pollingChannelIndex != nil {
		lastPolledClock := pollingChannelIndex.lastPolledChannelClock
		if lastPolledClock != nil {
			channelStats.PollingStats = ChannelPollingStats{}
			channelStats.PollingStats.Clock = base.PrintClock(lastPolledClock)
			channelStats.PollingStats.ClockHash = db.SequenceHasher.calculateHash(lastPolledClock)
		}
	}
	return channelStats, nil
}

func IsNotFoundError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

type unmarshalWorker struct {
	output  chan<- *LogEntry
	processing chan *unmarshalEntry
}


// UnmarshalWorker is used to unmarshal incoming entries from the DCP feed in parallel, while maintaining ordering of
// sequences per vbucket.  The main DocChanged loop creates one UnmarshalWorker per vbucket.
func NewUnmarshalWorker(output chan<- *LogEntry) *unmarshalWorker {

	// goroutine to work the processing channel and return completed entries
	// to the output channel
	maxProcessing := 500

	worker := &unmarshalWorker{
		output: output,
		processing: make(chan *unmarshalEntry, maxProcessing),
	}

	// Start goroutine to work the worker's processing channel.  When an entry arrives on the
	// processing channel, blocks waiting for success/fail for that document.
	go func() {
		for {
			select {
				case unmarshalEntry:= <-worker.processing:
					// Wait for entry processing to be done
					entryTime := time.Now()
					select {
						case ok:= <-unmarshalEntry.success:
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

					}
					/*
					case <- time.After(10 * time.Minute) :
						terminate the worker if it's idle for some time window?
						*/
			}
		}
	}()

	return worker
}

// Add document to the ordered processing results channel, and start goroutine to do processing work.
func (uw *unmarshalWorker) add(docID string, docJSON []byte, vbNo uint16, seq uint64, callback processDocFunc) {
	// create new unmarshal entry
	unmarshalEntry := NewUnmarshalEntry()

	// Add it to the ordering queue.  Will block if we're at limit for concurrent processing for this vbucket (maxProcessing).
	// TODO: This means that one full vbucket queue will block additions to all others (as the main DocChanged blocks on
	// call to this method).  Leaving as-is to avoid complexity, and as it's probably reasonable for hotspot vbs to get priority.
	uw.processing <- unmarshalEntry

	// start goroutine to do the work on the document
	go unmarshalEntry.process(docID, docJSON, vbNo, seq, callback)

}

// An incoming document being unmarshalled by unmarshalWorker
type unmarshalEntry struct {
	entryTime time.Time
	logEntry *LogEntry
	success chan bool
	err error
}

// UnmarshalEntry represents a document being processed by an UnmarshalWorker.
func NewUnmarshalEntry() *unmarshalEntry {

	return &unmarshalEntry{
		entryTime: time.Now(),
		success: make(chan bool),
	}
}

// UnmarshalEntry.process executes the processing (via callback), then updates the entry's success channel on completion
func (e *unmarshalEntry) process(docID string, docJSON []byte, vbNo uint16, seq uint64, callback processDocFunc) {

	// Do the processing work
	e.logEntry, e.err = callback(docID, docJSON, vbNo, seq)

	// Return completion status on the success channel
	if e.err != nil || e.logEntry == nil {
		e.success <-false
	} else {
		e.success <-true
	}
}


type PrincipalIndex struct {
	VbNo             uint16            `json:"vbucket"`        // vbucket number for user doc - for convenience
	ExplicitChannels channels.TimedSet `json:"admin_channels"` // Timed Set of channel names to vbucket seq no of first user version that granted access
	ExplicitRoles    channels.TimedSet `json:"admin_roles"`    // Timed Set of role names to vbucket seq no of first user version that granted access
}

type processDocFunc func(docID string, docJSON []byte, vbNo uint16, seq uint64) (*LogEntry, error)

func (k *kvChangeIndex) processDoc(docID string, docJSON []byte, vbNo uint16, seq uint64) (*LogEntry, error){

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


func (k *kvChangeIndex) processPrincipalDoc(docID string, docJSON []byte, vbNo uint16, sequence uint64) (*LogEntry, error) {

	entryTime := time.Now()

	isUser :=  strings.HasPrefix(docID, auth.UserKeyPrefix)
	// We need to track vbucket/sequence numbers for admin-granted roles and channels in the index,
	// since we can no longer store the sequence in the user doc body itself

	base.LogTo("DIndex+", "Processing principal doc %s", docID)
	// Index doc for a user stores the timed channel set of admin-granted channels.  These are merged with
	// the document-granted channels during calculateChannels.
	princ, err := k.context.Authenticator().UnmarshalPrincipal(docJSON, "", 0, isUser)
	if princ == nil {
		return nil, errors.New(fmt.Sprintf("kvChangeIndex: Error unmarshaling principal doc %q: %v", docID, err))
	}

	var indexDocID string
	if isUser {
		indexDocID = kIndexPrefix + "_user:" + princ.Name()
	} else {
		indexDocID = kIndexPrefix + "_role:" + princ.Name()
	}

	// Update the user doc in the index based on the principal
	var principalIndex *PrincipalIndex
	err = k.indexWriteBucket.Update(indexDocID, 0, func(currentValue []byte) ([]byte, error) {

		// Be careful: this block can be invoked multiple times if there are races!
		var principalRoleSet channels.TimedSet
		principalChannelSet := princ.ExplicitChannels()
		user, isUserPrincipal := princ.(auth.User)
		if isUserPrincipal {
			principalRoleSet = user.ExplicitRoles()
		}

		if currentValue == nil {
			// User index entry doesn't yet exist - create as new if the principal has channels
			// or roles
			if len(principalChannelSet) == 0 && len(principalRoleSet) == 0 {
				return nil, errors.New("No update required")
			}
			principalIndex = &PrincipalIndex{
				VbNo:             vbNo,
				ExplicitChannels: make(channels.TimedSet),
				ExplicitRoles:    make(channels.TimedSet),
			}

		} else {
			if err := json.Unmarshal(currentValue, &principalIndex); err != nil {
				return nil, err
			}
		}
		var rolesChanged, channelsChanged bool
		channelsChanged = principalIndex.ExplicitChannels.UpdateAtSequence(principalChannelSet.AsSet(), sequence)

		if isUserPrincipal {
			rolesChanged = principalIndex.ExplicitRoles.UpdateAtSequence(principalRoleSet.AsSet(), sequence)
		}

		if channelsChanged || rolesChanged {
			return json.Marshal(principalIndex)
		} else {
			return nil, errors.New("No update required")
		}
	})

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


