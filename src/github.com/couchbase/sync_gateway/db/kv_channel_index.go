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
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var ByteCachePollingTime = 1000 // initial polling time for notify, ms

const kSequenceOffsetLength = 0 // disabled until we actually need it
const kMaxVbNo = 1024           // TODO: load from cluster config

type kvChannelIndex struct {
	indexBucket         base.Bucket       // Database connection (used for connection queries)
	partitionMap        IndexPartitionMap // Index partition map
	channelName         string
	lastSequence        uint64
	lastCounter         uint64
	notifyRunning       bool
	notifyLock          sync.Mutex
	lastNotifiedChanges []*LogEntry
	lastNotifiedSince   SequenceClock
	stableSequence      SequenceClock
	stableSequenceCb    func() SequenceClock
	onChange            func(base.Set)
	indexBlockCache     map[string]IndexBlock // Recently used index blocks, by key
	indexBlockCacheLock sync.Mutex
	clock               *SequenceClockImpl
}

/*
	Add(entry kvIndexEntry) error
	AddSet(entries []kvIndexEntry) error
	GetClock() (uint64, error)
	SetClock() (uint64, error)
	GetCachedChanges(options ChangesOptions, stableSequence uint64)
*/

type kvIndexEntry struct {
	vbNo     uint16
	sequence uint64
	removal  bool
}

func newKvIndexEntry(logEntry *LogEntry, isRemoval bool) kvIndexEntry {
	return kvIndexEntry{sequence: logEntry.Sequence, removal: isRemoval, vbNo: logEntry.VbNo}
}

func NewKvChannelIndex(channelName string, bucket base.Bucket, partitions IndexPartitionMap, stableClockCallback func() SequenceClock, onChangeCallback func(base.Set)) *kvChannelIndex {

	channelIndex := &kvChannelIndex{
		channelName:      channelName,
		indexBucket:      bucket,
		partitionMap:     partitions,
		stableSequenceCb: stableClockCallback,
		onChange:         onChangeCallback,
	}

	channelIndex.indexBlockCache = make(map[string]IndexBlock)

	// Init stable sequence
	channelIndex.stableSequence = channelIndex.stableSequenceCb()

	// Initialize and load channel clock
	channelIndex.loadClock()

	return channelIndex

}

//
// Index Writing
//

func (k *kvChannelIndex) Add(entry kvIndexEntry) error {
	// Update the sequence in the appropriate cache block
	base.LogTo("DCache+", "Add to channel index %s, vbNo, isRemoval:%v", k.channelName, entry.vbNo, entry.removal)
	entries := make([]kvIndexEntry, 0)
	entries = append(entries, entry)
	k.AddSet(entries)
	return nil
}

// Adds a set
func (k *kvChannelIndex) AddSet(entries []kvIndexEntry) error {

	base.LogTo("DCache", "enter AddSet for channel %s, %+v", k.channelName, entries)
	// Update the sequences in the appropriate cache block
	if len(entries) == 0 {
		return nil
	}

	// The set of updates may be distributed over multiple partitions and blocks.
	// To support this, iterate over the set, and define groups of sequences by block
	// TODO: this approach feels like it's generating a lot of GC work.  Considered an iterative
	//       approach where a set update returned a list of entries that weren't targeted at the
	//       same block as the first entry in the list, but this would force sequential
	//       processing of the blocks.  Might be worth revisiting if we see high GC overhead.
	blockSets := make(BlockSet)
	clockUpdates := NewSequenceClockImpl()
	for _, entry := range entries {
		// Update the sequence in the appropriate cache block
		base.LogTo("DCache+", "Add to channel index [%s], vbNo=%d, isRemoval:%v", k.channelName, entry.vbNo, entry.removal)
		blockKey := GenerateBlockKey(k.channelName, entry.sequence, k.partitionMap[entry.vbNo])
		if _, ok := blockSets[blockKey]; !ok {
			blockSets[blockKey] = make([]kvIndexEntry, 0)
		}
		blockSets[blockKey] = append(blockSets[blockKey], entry)
		clockUpdates.SetSequence(entry.vbNo, entry.sequence)
	}

	for _, blockSet := range blockSets {
		// update the block
		err := k.writeSingleBlockWithCas(blockSet)
		if err != nil {
			return err
		}
	}

	// Update the clock.  Doing once per AddSet (instead of after each block update) to minimize the
	// round trips.
	err := k.writeClockCas(clockUpdates)

	return err
}

// Expects all incoming entries to target the same block
func (k *kvChannelIndex) writeSingleBlockWithCas(entries []kvIndexEntry) error {

	if len(entries) == 0 {
		return nil
	}
	// Get block based on first entry
	block := k.getIndexBlockForEntry(entries[0])
	// Apply updates to the in-memory block
	log.Println("Updating in-memory:", len(entries))
	for _, entry := range entries {
		err := block.AddEntry(entry)
		if err != nil { // Wrong block for this entry
			return err
		}
	}
	localValue, err := block.Marshal()
	if err != nil {
		base.Warn("Unable to marshal channel block - cancelling block update")
		return errors.New("Error marshalling channel block")
	}

	log.Println("Starting cas write for key:", block.Key(), len(localValue))
	casOut, err := writeCasRaw(k.indexBucket, block.Key(), localValue, block.Cas(), func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - kmay be called multiple times
		err = block.Unmarshal(value)

		log.Println("callback, unmarshalled block", len(value), nil)
		log.Println("callback, entry count", len(entries))
		for _, entry := range entries {
			err := block.AddEntry(entry)
			if err != nil { // Wrong block for this entry
				return nil, err
			}
		}
		return block.Marshal()
	})

	if err != nil {
		return err
	}
	block.SetCas(casOut)
	return nil
}

func (k *kvChannelIndex) Compact() {
	// TODO: for each index block being cached, check whether expired
}

func (k *kvChannelIndex) startNotify() {
	if !k.notifyRunning {
		k.notifyLock.Lock()
		defer k.notifyLock.Unlock()
		// ensure someone else didn't already start while we were waiting
		if k.notifyRunning {
			return
		}
		k.notifyRunning = true
		go func() {
			for k.pollForChanges() {
				// TODO: geometrically increase sleep time to better manage rarely updated channels? Would result in increased
				// latency for those channels for connected clients, but reduce churn until we have DCP-based
				// notification instead of polling
				time.Sleep(time.Duration(ByteCachePollingTime) * time.Millisecond)
			}

		}()

	} else {
		base.LogTo("DCache", "Notify already running for channel %s", k.channelName)
	}
}

func (k *kvChannelIndex) updateIndexCount() error {

	// increment index count
	key := getIndexCountKey(k.channelName)

	newValue, err := k.indexBucket.Incr(key, 1, 1, 0)
	if err != nil {
		base.Warn("Error from Incr in updateCacheClock(%s): %v", key, err)
		return err
	}
	base.LogTo("DCache", "Updated clock for %s (%s) to %d", k.channelName, key, newValue)

	return nil

}

func (k *kvChannelIndex) pollForChanges() bool {

	// check stable sequence first
	// TODO: move this up out of the channel cache so that we're only doing it once (not once per channel), and have that process call the registered
	// channel caches

	changeCacheExpvars.Add(fmt.Sprintf("pollCount-%s", k.channelName), 1)
	stableSequence := k.stableSequenceCb()

	k.stableSequence = stableSequence

	base.LogTo("DCacheDebug", "Updating stable sequence to: %d (%s)", k.stableSequence, k.channelName)

	currentCounter, err := k.getIndexCounter()
	if err != nil {
		return true
	}

	// If there's an update, cache the recent changes in memory, as we'll expect all
	// all connected clients to request these changes
	base.LogTo("DCache", "poll for changes on channel %s: (current, last) (%d, %d)", k.channelName, currentCounter, k.lastCounter)
	if currentCounter > k.lastCounter {
		//k.UpdateRecentCache(currentCounter)
	}

	return true
	// if count != previous count

	//   get changes since previous, save to lastChanges
	//   update lastSince
	//   call onChange
	//   return true

	// else
	//   return false
	//
}

/*
func (k *kvChannelIndex) UpdateRecentCache(currentCounter uint64) {
	k.notifyLock.Lock()
	defer k.notifyLock.Unlock()

	// Compare counter again, in case someone has already updated cache while we waited for the lock
	if currentCounter > k.lastCounter {
		options := ChangesOptions{Since: SequenceID{Seq: k.lastSequence}}
		_, recentChanges := k.getChanges(options)
		if len(recentChanges) > 0 {
			k.lastNotifiedChanges = recentChanges
			k.lastNotifiedSince = k.lastSequence
			k.lastSequence = k.lastNotifiedChanges[len(k.lastNotifiedChanges)-1].Sequence
			k.lastCounter = currentCounter
		} else {
			base.Warn("pollForChanges: channel [%s] clock changed to %d (from %d), but no changes found in cache since #%d", k.channelName, currentCounter, k.lastCounter, k.lastSequence)
			return
		}
		if k.onChange != nil {
			k.onChange(base.SetOf(k.channelName))
		}
	}
}
*/

// Returns the set of index entries for the channel more recent than the
// specified since SequenceClock.  Index entries with sequence values greater than
// the index stable sequence are not returned.
func (k *kvChannelIndex) getChanges(since SequenceClock) []*LogEntry {

	var results []*LogEntry

	// Get the set of vbuckets that have changed for this channel since 'since', where
	// the since value is greater than the stable sequence.
	stableClock := k.stableSequence
	sinceBlockSet := make(BlockSet)
	for vbNo, sequence := range k.clock.value {
		sinceSeq := since.GetSequence(vbNo)
		if sequence > sinceSeq && sinceSeq > stableClock.GetSequence(vbNo) {
			sinceEntry := kvIndexEntry{vbNo: vbNo, sequence: sinceSeq}
			blockKey := GenerateBlockKey(k.channelName, sinceSeq, k.partitionMap[vbNo])
			sinceBlockSet[blockKey] = append(sinceBlockSet[blockKey], sinceEntry)
		}
	}

	if len(sinceBlockSet) == 0 {
		return results
	}

	// If there are changes, compare with last notified to see if it's a match
	if since.Equals(k.lastNotifiedSince) {
		return k.lastNotifiedChanges
	}

	// If not, retrieve from cache.  Don't return anything later than stableClock.

	// loop while more blocks (use channel clock to figure out when you're done)
	// for {

	// Only retrieves one block per partition per iteration, to avoid a huge bulk get when the
	// since value is much older than the channel clock.  Could consider instead using a target
	// number of documents as a future performance optimization.
	//blocks, err := k.indexBucket.GetBulkRaw(sinceBlockSet.keySet())

	// 		Do a bulk get to retrieve all changed blocks
	//      Process blocks in parallel
	//      For each block, get the set of vb since values from sinceBlockSet
	//      Add results
	// }

	// start goroutines to process each block, and write to some channel
	// use waitgroup to coordinate when done
	// start another goroutine to close the channel when the waitgroups are done.  that will allow range below to close
	// do a range on the channel, writing entries to limit

	// Byte channel cache is separated into docs of (byteCacheBlockCapacity) sequences.
	/*
		blockIndex := k.getBlockIndex(since)
		block := k.readIndexBlock(blockIndex)
		// Get index of sequence within block
		offset := uint64(blockIndex) * byteCacheBlockCapacity
		startIndex := (since + 1) - offset

		// Iterate over blocks
		moreChanges := true

		for moreChanges {
			if block != nil {
				for i := startIndex; i < byteCacheBlockCapacity; i++ {
					sequence := i + offset
					if sequence > stableSequence {
						moreChanges = false
						break
					}
					if block.value[i] > 0 {
						entry, err := readCacheEntry(sequence, k.context.Bucket)
						if err != nil {
							// error reading cache entry - return changes to this point
							moreChanges = false
							break
						}
						if block.value[i] == 2 {
							// deleted
							entry.Flags |= channels.Removed
						}
						cacheContents = append(cacheContents, entry)
					}
				}
			}
			// Init for next block
			blockIndex++
			if uint64(blockIndex)*byteCacheBlockCapacity > stableSequence {
				moreChanges = false
			}
			block = k.readIndexBlock(blockIndex)
			startIndex = 0
			offset = uint64(blockIndex) * byteCacheBlockCapacity
		}

		base.LogTo("DCacheDebug", "getCachedChanges: found %d changes for channel %s, since %d", len(cacheContents), k.channelName, options.Since.Seq)
		// TODO: is there a way to deduplicate doc IDs without a second iteration and the slice copying?
		// Possibly work the cache in reverse, starting from the high sequence of the channel (if that's available
		// in metadata)?  We've got two competing goals here:
		//   - deduplicate Doc IDs, keeping the most recent
		//   - only read to limit, from oldest
		// - Reading oldest to the limit first means that DocID deduplication
		// could result in smaller resultset than limit
		// - Deduplicating first requires loading the entire set (could be much larger than limit)
		result := make([]*LogEntry, 0, len(cacheContents))
		count := len(cacheContents)
		base.LogTo("DCache", "found contents with length %d", count)
		docIDs := make(map[string]struct{}, options.Limit)
		for i := count - 1; i >= 0; i-- {
			entry := cacheContents[i]
			docID := entry.DocID
			if _, found := docIDs[docID]; !found {
				// safe insert at 0
				result = append(result, nil)
				copy(result[1:], result[0:])
				result[0] = entry
				docIDs[docID] = struct{}{}
			}

		}

		base.LogTo("DCacheDebug", " getCachedChanges returning %d changes", len(result))

		//TODO: correct validFrom
	*/
	return []*LogEntry{}
}

// Returns the block keys needed to retrieve all changes between fromClock and toClock.  When
// stableOnly is true, restricts to only those needed to retrieve changes earlier than the stable sequence
/*
func (k *kvChannelIndex) calculateBlocks(fromClock SequenceClock, toClock SequenceClock, stableOnly bool) BlockSet {

	stableClock := k.stableSequence

	sinceBlockSet := make(BlockSet)
	for vb, sequence := range k.clock.value {
		sinceSeq := since.GetSequence(vb)
		if sequence > sinceSeq && (sinceSeq > toClock.GetSequence(vb) || stableOnly == false) {
			sinceEntry := kvIndexEntry{vbNo: vb, sequence: sinceSeq}
			blockKey := GenerateBlockKey(k.channelName, sinceSeq, k.partitionMap[vbNo])
			sinceBlockSet[blockKey] = append(sinceBlockSet[blockKey], sinceEntry)
		}
	}

}
*/

func (k *kvChannelIndex) getIndexCounter() (uint64, error) {
	key := getIndexCountKey(k.channelName)
	var intValue uint64
	_, err := k.indexBucket.Get(key, &intValue)
	if err != nil {
		// return nil here - cache clock may not have been initialized yet
		return 0, nil
	}
	return intValue, nil
}

// Determine the cache block index for a sequence
func (k *kvChannelIndex) getBlockIndex(sequence uint64) uint16 {
	base.LogTo("DCache", "block index for sequence %d is %d", sequence, uint16(sequence/byteCacheBlockCapacity))
	return uint16(sequence / byteCacheBlockCapacity)
}

// Safely retrieves index block from the channel's block cache
func (k *kvChannelIndex) getIndexBlockFromCache(key string) IndexBlock {
	k.indexBlockCacheLock.Lock()
	defer k.indexBlockCacheLock.Unlock()
	return k.indexBlockCache[key]
}

// Safely inserts index block from the channel's block cache
func (k *kvChannelIndex) putIndexBlockToCache(key string, block IndexBlock) {
	k.indexBlockCacheLock.Lock()
	defer k.indexBlockCacheLock.Unlock()
	k.indexBlockCache[key] = block
}

// Retrieves the appropriate index block for an entry. If not found in the channel's block cache,
// will initialize a new block and add to the map.
func (k *kvChannelIndex) getIndexBlockForEntry(entry kvIndexEntry) IndexBlock {

	partition := k.partitionMap[entry.vbNo]
	key := GenerateBlockKey(k.channelName, entry.sequence, partition)

	log.Println("Retrieving for key:", key)
	// First attempt to retrieve from the cache of recently accessed blocks
	block := k.getIndexBlockFromCache(key)
	if block != nil {
		log.Println("Returning from cache")
		return block
	}

	// If not found in cache, attempt to retrieve from the index bucket
	block = NewIndexBlock(k.channelName, entry.sequence, partition)
	err := k.loadBlock(block)
	if err == nil {
		log.Println("Returning from disk")
		return block
	}

	// If still not found, initialize a new empty index block and add to cache
	block = NewIndexBlock(k.channelName, entry.sequence, partition)
	k.putIndexBlockToCache(key, block)

	log.Println("Returning new")
	return block
}

func (k *kvChannelIndex) loadBlock(block IndexBlock) error {

	data, cas, err := k.indexBucket.GetRaw(block.Key())
	if err != nil {
		return err
	}

	err = block.Unmarshal(data)
	block.SetCas(cas)

	if err != nil {
		return err
	}
	// If found, add to the cache
	k.putIndexBlockToCache(block.Key(), block)
	return nil
}

func (k *kvChannelIndex) loadClock() {

	if k.clock == nil {
		k.clock = NewSequenceClockImpl()
	}
	data, cas, err := k.indexBucket.GetRaw(getChannelClockKey(k.channelName))
	if err != nil {
		base.LogTo("DCache+", "Unable to find existing channel clock for channel %s - treating as new", k.channelName)
	}
	k.clock.Unmarshal(data)
	k.clock.SetCas(cas)
}

func (k *kvChannelIndex) writeClockCas(updateClock SequenceClock) error {
	// Initial set, for the first cas update attempt
	k.clock.UpdateWithClock(updateClock)
	value, err := k.clock.Marshal()
	if err != nil {
		return err
	}
	casOut, err := writeCasRaw(k.indexBucket, getChannelClockKey(k.channelName), value, k.clock.Cas(), func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times
		err = k.clock.Unmarshal(value)
		if err != nil {
			return nil, err
		}
		k.clock.UpdateWithClock(updateClock)
		return k.clock.Marshal()
	})
	k.clock.SetCas(casOut)
	return nil
}

type kvChannelCache struct {
}

// vbCache caches a set of LogEntry values, representing the set of entries in a channel for a
// particular vbucket.

type vbCache struct {
	validFrom uint64            // Sequence the cache is valid from
	validTo   uint64            // Sequence the cache is valid to
	entries   SortedEntrySet    // Deduplicated entries for the vbucket for sequences between validFrom and validTo, ordered by sequence
	docIDs    map[string]uint64 // Map of doc ids present in entries, mapped to that doc's sequence id - used to optimize dedup processing
	cacheLock sync.RWMutex      // Lock used for updating cache from index
}

func newVbCache() *vbCache {
	return &vbCache{
		validFrom: math.MaxUint64,
		validTo:   0,
		docIDs:    make(map[string]uint64),
	}

}

// Returns all cached entries between fromSequence and toSequence.  When the cache doesn't have the full range, returns
// non-zero validFrom and validTo values
func (v *vbCache) getEntries(fromSequence, toSequence uint64) (validFrom uint64, validTo uint64, entries []*LogEntry) {

	// Returning a new copy (instead of subslice of entries) to protect against future deduplication within entries
	// TODO: use binary search to find starting point in entry list?
	if fromSequence > toSequence {
		return 0, 0, entries
	}
	v.cacheLock.RLock()
	defer v.cacheLock.RUnlock()

	for _, entry := range v.entries {
		if entry.Sequence >= fromSequence {
			if entry.Sequence <= toSequence {
				entries = append(entries, entry)
			} else {
				break
			}
		}
	}
	validFrom = maxUint64(fromSequence, v.validFrom)
	validTo = minUint64(toSequence, v.validTo)
	return validFrom, validTo, entries
}

// Adds the provided set of entries to the cache.  Incoming entries must be ordered by vbucket sequence, and the first
// entry must be greater than the current cache's validTo.
// Deduplicates by doc id.
func (v *vbCache) appendEntries(entries SortedEntrySet, validFrom uint64, validTo uint64) error {
	if len(entries) == 0 {
		return nil
	}
	if entries[0].Sequence < v.validTo {
		return errors.New("Cache conflict - attempt to append entry earlier than validTo")
	}

	entries, duplicateDocIDs, error := v.validateCacheUpdate(entries, false)
	if error != nil {
		return error
	}

	v.cacheLock.Lock()
	defer v.cacheLock.Unlock()

	// Remove duplicates from cache
	for _, docID := range duplicateDocIDs {
		v.entries.Remove(v.docIDs[docID])
	}

	// Append entries
	v.entries = append(v.entries, entries...)

	// Update docID map
	for _, entry := range entries {
		v.docIDs[entry.DocID] = entry.Sequence
	}

	// If this is the first set appended, may need to set validFrom as well as validTo
	if validFrom < v.validFrom {
		v.validFrom = validFrom
	}
	v.validTo = validTo
	return nil
}

// Adds the provided set of entries to the cache.  Incoming entries must be ordered by vbucket sequence, and the first
// entry must be greater than the current cache's validTo.
// Deduplicates by doc id.
func (v *vbCache) prependEntries(entries SortedEntrySet, validFrom uint64) error {
	if len(entries) == 0 {
		return nil
	}
	if entries[len(entries)-1].Sequence > v.validFrom {
		return errors.New("Cache conflict - attempt to prepend entry later than validFrom")
	}

	entries, _, error := v.validateCacheUpdate(entries, true)

	if error != nil {
		return error
	}

	v.cacheLock.Lock()
	defer v.cacheLock.Unlock()

	// Prepend all remaining entries
	v.entries = append(entries, v.entries...)

	// Append new doc ids
	for _, entry := range entries {
		v.docIDs[entry.DocID] = entry.Sequence
	}

	v.validFrom = validFrom

	return nil
}

// Pre-processing for an incoming set of cache updates.  Returns the set of already cached doc IDs that need to get
// deduplicated, the set of new DocIDs, removes any doc ID duplicates in the set
func (v *vbCache) validateCacheUpdate(entries SortedEntrySet, deleteDuplicates bool) (validEntries SortedEntrySet, duplicateEntries []string, err error) {
	v.cacheLock.RLock()
	defer v.cacheLock.RUnlock()
	var prevSeq uint64
	prevSeq = math.MaxUint64

	// Iterate over entries, doing the following:
	//  - deduplicating by DocID within the set of new entries (*not* v.entries)
	//  - build list of docIDs that are already known to the cache
	entriesDocIDs := make(map[string]interface{})
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		// validate that the sequences are ordered as expected
		if entry.Sequence > prevSeq {
			return entries, duplicateEntries, errors.New("Entries must be ordered by sequencewhen updating cache")
		}

		// If we've already seen this doc ID in the new entry set, remove it
		if _, ok := entriesDocIDs[entry.DocID]; ok {
			entries = append(entries[:i], entries[i+1:]...)
		} else { // Otherwise, add to appropriate DocId list
			if _, ok := v.docIDs[entry.DocID]; ok {
				duplicateEntries = append(duplicateEntries, entry.DocID)
				if deleteDuplicates {
					entries = append(entries[:i], entries[i+1:]...)
				}
			}
		}
	}

	return entries, duplicateEntries, nil

}

// Removes an entry from the cache.
func (v *vbCache) removeEntry(docID string, sequence uint64) {
	v.cacheLock.Lock()
	defer v.cacheLock.Unlock()
	v._removeEntry(docID, sequence)
}

// Removes an entry from the cache.  Assumes caller has write lock on v.cacheLock
func (v *vbCache) _removeEntry(docID string, sequence uint64) {

}

// SortedEntrySet
// Optimizes removal of entries from a sorted array.
type SortedEntrySet []*LogEntry

func (h *SortedEntrySet) Remove(x uint64) error {

	log.Printf("removeEntry: %d", x)
	i := SearchSortedEntrySet(*h, x)
	log.Printf("search results: i=%d, len=%d", i, len(*h))
	if i < len(*h) && (*h)[i].Sequence == x {
		*h = append((*h)[:i], (*h)[i+1:]...)
		return nil
	} else {
		return errors.New("Value not found")
	}
}

// Skipped Sequence version of sort.SearchInts - based on http://golang.org/src/sort/search.go?s=2959:2994#L73
func SearchSortedEntrySet(a SortedEntrySet, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i].Sequence >= x })
}

////////  Utility functions for key generation

// Get the key for the cache count doc
func getIndexCountKey(channelName string) string {
	return fmt.Sprintf("%s_count:%s", kCachePrefix, channelName)
}

// Get the key for the cache block, based on the block index
func getIndexBlockKey(channelName string, partition uint16, blockIndex uint16) string {
	return fmt.Sprintf("%s:%s:%d:block%d", kCachePrefix, channelName, partition, blockIndex)
}

// Get the key for the cache block, based on the block index
func getChannelClockKey(channelName string) string {
	return fmt.Sprintf("%s_SequenceClock:%s", kCachePrefix, channelName)
}

// Generate the key for a single sequence/entry
func getEntryKey(sequence uint64) string {
	return fmt.Sprintf("%s:seq:%d", kCachePrefix, sequence)
}

func minUint64(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// BlockSet - used to organize collections of vbuckets by partition for block-based removal

type BlockSet map[string][]kvIndexEntry // Collection of index entries, indexed by block id.

func (b BlockSet) keySet() []string {
	keys := make([]string, len(b))
	i := 0
	for k := range b {
		keys[i] = k
		i += 1
	}
	return keys
}
