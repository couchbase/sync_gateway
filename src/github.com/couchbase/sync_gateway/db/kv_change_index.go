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
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	kCachePrefix       = "_cache"
	kStableSequenceKey = "_cache_stableSeq"
	maxCacheUpdate     = 500
	minCacheUpdate     = 1
)

type kvChangeIndex struct {
	context          *DatabaseContext           // Database context
	indexBucket      base.Bucket                // Index bucket
	maxVbNo          uint16                     // Number of vbuckets
	indexPartitions  IndexPartitionMap          // Partitioning of vbuckets in the index
	channelIndexes   map[string]*kvChannelIndex // Channel indexes, map indexed by channel name
	channelIndexLock sync.RWMutex               // Coordinates access to channel index map
	onChange         func(base.Set)             // Client callback that notifies of channel changes
	pending          chan *LogEntry             // Incoming changes, pending indexing
	stableSequence   *SyncSequenceClock         // Stabfle sequence in index
	logsDisabled     bool                       // If true, ignore incoming tap changes
}

type IndexPartitionMap map[uint16]uint16 // Maps vbuckets to index partition value

type ChannelPartition struct {
	channelName string
	partition   uint16
}

//
type ChannelPartitionMap map[ChannelPartition][]*LogEntry

func (cpm ChannelPartitionMap) add(cp ChannelPartition, entry *LogEntry) {
	_, found := cpm[cp]
	if !found {
		// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
		cpm[cp] = make([]*LogEntry, 0, maxCacheUpdate)
	}
	cpm[cp] = append(cpm[cp], entry)
}

////// Cache writer API

func (k *kvChangeIndex) Init(context *DatabaseContext, lastSequence SequenceID, onChange func(base.Set), options *CacheOptions, indexOptions *ChangeIndexOptions) {

	// not sure yet whether we'll need initial sequence
	k.channelIndexes = make(map[string]*kvChannelIndex)
	k.pending = make(chan *LogEntry, maxCacheUpdate)

	// Load index partitions
	k.indexPartitions = k.loadIndexPartitionMap()

	k.context = context
	k.indexBucket = indexOptions.Bucket

	// Initialize stable sequence

	k.initStableClock()

	// start process to work pending sequences
	go k.indexPending()
}

func (k *kvChangeIndex) Prune() {
	// TODO: currently no pruning of channel indexes
}

func (k *kvChangeIndex) Clear() {
	// TODO: Currently no clear for distributed cache
	// temp handling until implemented, to pass unit tests
	k.channelIndexes = make(map[string]*kvChannelIndex)
}

func (k *kvChangeIndex) Stop() {
	// TBD
}

// Returns the stable sequence for a document from the stable clock, based on the vbucket for the document.
// Used during document write for handling deduplicated sequences on the DCP feed.
func (k *kvChangeIndex) GetStableSequence(docID string) SequenceID {

	vbNo := k.indexBucket.VBHash(docID)
	return SequenceID{Seq: k.stableSequence.GetSequence(uint16(vbNo))}

}

// Returns the stable sequence clock for the index
func (k *kvChangeIndex) getStableClock() SequenceClock {
	return k.stableSequence.clock
}

func (k *kvChangeIndex) initStableClock() {

	k.stableSequence = NewSyncSequenceClock()
	value, cas, err := k.indexBucket.GetRaw(kStableSequenceKey)
	if err != nil {
		base.Warn("Stable sequence not found in index - resetting to 0")
		return
	}
	k.stableSequence.Unmarshal(value)
	k.stableSequence.SetCas(cas)

}

func (k *kvChangeIndex) AddToCache(change *LogEntry) base.Set {

	log.Println("CHANNEL ENTRY AGAIN:", change.VbNo)
	// queue for cache addition
	base.LogTo("DCache+", "Change Index: Adding Entry with Key [%s], VbNo [%d]", change.DocID, change.VbNo)
	k.pending <- change
	return base.Set{}

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
func (k *kvChangeIndex) DocChanged(docID string, docJSON []byte, vbNo uint64) {
	entryTime := time.Now()
	// ** This method does not directly access any state of c, so it doesn't lock.
	go func() {
		// Is this a user/role doc?
		if strings.HasPrefix(docID, auth.UserKeyPrefix) {
			k.processPrincipalDoc(docID, docJSON, true)
			return
		} else if strings.HasPrefix(docID, auth.RoleKeyPrefix) {
			k.processPrincipalDoc(docID, docJSON, false)
			return
		}

		// First unmarshal the doc (just its metadata, to save time/memory):
		doc, err := unmarshalDocumentSyncData(docJSON, false)
		if err != nil || !doc.hasValidSyncData() {
			base.Warn("changeCache: Error unmarshaling doc %q: %v", docID, err)
			return
		}

		// TODO: disabled - we don't care about unused sequences since we don't need contiguous seqs
		// If the doc update wasted any sequences due to conflicts, add empty entries for them:

		/*
			for _, seq := range doc.UnusedSequences {
				base.LogTo("Cache", "Received unused #%d for (%q / %q)", seq, docID, doc.CurrentRev)
				change := &LogEntry{
					Sequence:     seq,
					TimeReceived: time.Now(),
				}
				c.AddToCache(change)
			}
		*/

		// TODO: Handling for feed de-duplication by Couchbase Server
		// Ignoring this for now, since we don't have a requirement for contiguous sequences in this model
		/*
			currentSequence := doc.Sequence
			if len(doc.UnusedSequences) > 0 {
				currentSequence = doc.UnusedSequences[0]
			}
			if len(doc.RecentSequences) > 0 {
				//TODO: this has the possibility to send the same sequence to be buffered multiple times - depends
				//      on the subsequent processing ignoring duplicates.  Probably a better question is how much
				//      we care about this since we're not dependent on contiguous sequences.
				nextSeq := c.getNextSequence(vbNo)
				for _, seq := range doc.RecentSequences {
					if seq >= nextSeq && seq < currentSequence {
						base.LogTo("Cache", "Received deduplicated #%d for (%q / %q)", seq, docID, doc.CurrentRev)
						change := &LogEntry{
							Sequence:     seq,
							TimeReceived: time.Now(),
						}
						c.processEntry(change)
					}
				}
			}
		*/

		// Record a histogram of the  feed's lag:
		feedLag := time.Since(doc.TimeSaved) - time.Since(entryTime)
		lagMs := int(feedLag/(100*time.Millisecond)) * 100
		changeCacheExpvars.Add(fmt.Sprintf("lag-feed-%04dms", lagMs), 1)

		// Now add the entry for the new doc revision:
		change := &LogEntry{
			Sequence:     doc.Sequence,
			DocID:        docID,
			RevID:        doc.CurrentRev,
			Flags:        doc.Flags,
			TimeReceived: time.Now(),
			TimeSaved:    doc.TimeSaved,
			Channels:     doc.Channels,
			VbNo:         uint16(vbNo),
		}
		base.LogTo("Cache", "Received #%d after %3dms (%q / %q)", change.Sequence, int(feedLag/time.Millisecond), change.DocID, change.RevID)

		if change.DocID == "" {
			base.Warn("Unexpected change with empty DocID for sequence %d, vbno:%d", doc.Sequence, vbNo)
			changeCacheExpvars.Add("changes_without_id", 1)
			return
		}

		// TODO: not doing local notification - it's all done via the bucket.  Could revisit as optimization on small SG clusters
		k.AddToCache(change)
	}()
}

func (k *kvChangeIndex) processPrincipalDoc(docID string, docJSON []byte, isUser bool) {
	// TODO
}

func (b *kvChangeIndex) getChannelIndex(channelName string) *kvChannelIndex {

	b.channelIndexLock.RLock()
	defer b.channelIndexLock.RUnlock()
	return b.channelIndexes[channelName]
}

func (k *kvChangeIndex) newChannelIndex(channelName string) *kvChannelIndex {

	k.channelIndexLock.Lock()
	defer k.channelIndexLock.Unlock()
	k.channelIndexes[channelName] = NewKvChannelIndex(channelName, k.indexBucket, k.indexPartitions, k.getStableClock, k.onChange)
	return k.channelIndexes[channelName]
}

func (b *kvChangeIndex) getOrCreateChannelIndex(channelName string) *kvChannelIndex {
	index := b.getChannelIndex(channelName)
	if index == nil {
		index = b.newChannelIndex(channelName)
	}
	return index
}

func (c *kvChangeIndex) readFromPending() []*LogEntry {

	entries := make([]*LogEntry, 0, maxCacheUpdate)

	// TODO - needs cancellation handling?
	// Blocks until reading at least one from pending
	for {
		select {
		case entry := <-c.pending:
			entries = append(entries, entry)
			// read additional from cache if present, up to maxCacheUpdate
			for {
				select {
				case additionalEntry := <-c.pending:
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

func (c *kvChangeIndex) indexPending() {

	// TODO: cancellation handling

	// Continual processing of arriving entries from the feed.
	for {
		// Read entries from the pending list into array
		entries := c.readFromPending()

		// Wait group tracks when the current buffer has been completely processed
		var wg sync.WaitGroup
		channelSets := make(map[ChannelPartition][]*LogEntry)
		updatedSequences := NewSequenceClockImpl()

		// Generic channelStorage for log entry storage
		channelStorage := NewChannelStorage(c.indexBucket, "", c.indexPartitions)
		// Iterate over entries to write index entry docs, and group entries for subsequent channel index updates
		for _, logEntry := range entries {
			// Add index log entry if needed
			if channelStorage.StoresLogEntries() {
				wg.Add(1)
				go func(logEntry *LogEntry) {
					defer wg.Done()
					channelStorage.WriteLogEntry(logEntry)
				}(logEntry)
			}
			// Collect entries by channel
			// Remove channels from entry to save space in memory, index entries
			ch := logEntry.Channels
			logEntry.Channels = nil
			for channelName, removal := range ch {
				if removal == nil || removal.Seq == logEntry.Sequence {
					// Store by channel and partition, to avoid having to iterate over results again in the channel index to group by partition
					chanPartition := ChannelPartition{channelName: channelName, partition: c.indexPartitions[logEntry.VbNo]}
					_, found := channelSets[chanPartition]
					if !found {
						// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
						channelSets[chanPartition] = make([]*LogEntry, 0, maxCacheUpdate)
					}
					if removal != nil {
						removalEntry := *logEntry
						removalEntry.Flags |= channels.Removed
						channelSets[chanPartition] = append(channelSets[chanPartition], &removalEntry)
					} else {
						channelSets[chanPartition] = append(channelSets[chanPartition], logEntry)
					}
				}
			}
			if EnableStarChannelLog {
				chanPartition := ChannelPartition{channelName: "*", partition: c.indexPartitions[logEntry.VbNo]}
				_, found := channelSets[chanPartition]
				if !found {
					// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
					channelSets[chanPartition] = make([]*LogEntry, 0, maxCacheUpdate)
				}
				channelSets[chanPartition] = append(channelSets[chanPartition], logEntry)
			}

			// Track vbucket sequences for clock update
			updatedSequences.SetSequence(logEntry.VbNo, logEntry.Sequence)

		}

		// Iterate over channel sets to update channel index
		for chanPartition, entrySet := range channelSets {
			wg.Add(1)
			go func(chanPartition ChannelPartition, entrySet []*LogEntry) {
				defer wg.Done()
				c.addSetToChannelIndex(chanPartition.channelName, entrySet)

			}(chanPartition, entrySet)
		}

		wg.Wait()

		// Update stable sequence
		err := c.updateStableSequence(updatedSequences)
		if err != nil {
			base.LogPanic("Error updating stable sequence", err)
		}
	}
}

func addEntryToMap(setMap map[ChannelPartition][]*LogEntry, channelName string, partition uint16, entry *LogEntry) {
	chanPartition := ChannelPartition{channelName: channelName, partition: partition}
	_, found := setMap[chanPartition]
	if !found {
		// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
		setMap[chanPartition] = make([]*LogEntry, 0, maxCacheUpdate)
	}
	setMap[chanPartition] = append(setMap[chanPartition], entry)
}

func (b *kvChangeIndex) SetNotifier(onChange func(base.Set)) {
	b.onChange = onChange
}

func (b *kvChangeIndex) GetChanges(channelName string, options ChangesOptions) (entries []*LogEntry, err error) {

	// TODO: add backfill from view?  Currently expects infinite cache
	_, resultFromCache := b.GetCachedChanges(channelName, options)
	b.enableNotifications(channelName)
	return resultFromCache, nil
}

func (b *kvChangeIndex) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {

	//TODO: calculate the since clock based on the incoming since sequence
	sinceSequence := NewSequenceClockFromHash(options.Since.String())

	// TODO: Compare with stable clock (hash only?) first for a potential short-circuit

	changes, err := b.getChannelIndex(channelName).getChanges(sinceSequence)
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

// TODO: Implement late sequence handling if needed
func (b *kvChangeIndex) InitLateSequenceClient(channelName string) uint64 {
	// no-op
	return 0
}

func (b *kvChangeIndex) GetLateSequencesSince(channelName string, sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {
	// no-op
	return entries, lastSequence, nil
}

func (b *kvChangeIndex) ReleaseLateSequenceClient(channelName string, sequence uint64) error {
	// no-op
	return nil
}

func (b *kvChangeIndex) addToChannelCache(channelName string, change *LogEntry) {

	b.getOrCreateChannelIndex(channelName).Add(change)
	base.LogTo("DCache", "    #%d ==> channel %q", change.Sequence, channelName)
}

func (b *kvChangeIndex) addSetToChannelIndex(channelName string, entries []*LogEntry) {
	b.getOrCreateChannelIndex(channelName).AddSet(entries)
}

// Add late sequence information to channel cache
func (b *kvChangeIndex) addLateSequence(channelName string, change *LogEntry) error {
	// TODO: no-op for now
	return nil
}

func (k *kvChangeIndex) updateStableSequence(updates SequenceClock) error {

	log.Println("Updating stable sequence:", k.stableSequence)

	// Initial set, for the first cas update attempt
	k.stableSequence.UpdateWithClock(updates)
	value, err := k.stableSequence.Marshal()
	if err != nil {
		return err
	}
	casOut, err := writeCasRaw(k.indexBucket, kStableSequenceKey, value, k.stableSequence.Cas(), func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times
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

// Load the index partition definitions
func (b *kvChangeIndex) loadIndexPartitionMap() IndexPartitionMap {

	// TODO: load from the profile definition.  Hardcode for now to 32 sequential partitions
	partitions := make(IndexPartitionMap, b.maxVbNo)

	numPartitions := uint16(32)
	vbPerPartition := b.maxVbNo / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			partitions[vb] = partition
		}
	}
	return partitions
}

// Notification handling
func (b *kvChangeIndex) enableNotifications(channelName string) {
	b.getChannelIndex(channelName).startNotify()
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
