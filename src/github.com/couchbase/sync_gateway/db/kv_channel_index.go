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
	indexBucket       base.Bucket       // Database connection (used for connection queries)
	partitionMap      IndexPartitionMap // Index partition map
	channelName       string
	lastSequence      uint64
	lastCounter       uint64
	notifyRunning     bool
	notifyLock        sync.Mutex
	lastPolledChanges []*LogEntry
	lastPolledClock   SequenceClock
	lastPolledLock    sync.RWMutex
	stableSequence    SequenceClock
	stableSequenceCb  func() SequenceClock
	onChange          func(base.Set)
	clock             *SequenceClockImpl
	vbucketCache      map[uint64]*vbCache // Cached entries by vbucket
	channelStorage    ChannelStorage
}

func NewKvChannelIndex(channelName string, bucket base.Bucket, partitions IndexPartitionMap, stableClockCallback func() SequenceClock, onChangeCallback func(base.Set)) *kvChannelIndex {

	channelIndex := &kvChannelIndex{
		channelName:      channelName,
		indexBucket:      bucket,
		partitionMap:     partitions,
		stableSequenceCb: stableClockCallback,
		onChange:         onChangeCallback,
		channelStorage:   NewChannelStorage(bucket, channelName, partitions),
	}

	// TODO: The optimal initial capacity for the vbucketCache will vary depending on how many documents
	// are assigned to the channel.  Currently starting with kMaxVbNo/4 as a ballpark, but might want
	// to tune this based on performance analysis
	channelIndex.vbucketCache = make(map[uint64]*vbCache, kMaxVbNo/4)

	// Init stable sequence
	channelIndex.stableSequence = channelIndex.stableSequenceCb()

	// Initialize and load channel clock
	channelIndex.loadClock()

	return channelIndex

}

//
// Index Writing
//

func (k *kvChannelIndex) Add(entry *LogEntry) error {
	// Update the sequence in the appropriate cache block
	entries := make([]*LogEntry, 1)
	entries[0] = entry
	return k.AddSet(entries)
}

// Adds a set
func (k *kvChannelIndex) AddSet(entries []*LogEntry) error {
	base.LogTo("DCache", "enter AddSet for channel %s, %+v", k.channelName, entries)
	clockUpdates, err := k.channelStorage.AddEntrySet(entries)
	if err != nil {
		return err
	}
	// Update the clock.  Doing once per AddSet (instead of after each block update) to minimize the
	// round trips.
	return k.writeClockCas(clockUpdates)
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

type vbEntryRange struct {
	vbNo    uint64
	fromSeq uint64
	toSeq   uint64
}

func (k *kvChannelIndex) checkLastPolled(since SequenceClock) (results []*LogEntry) {
	if k.lastPolledClock == nil {
		return results
	}
	k.lastPolledLock.RLock()
	defer k.lastPolledLock.RUnlock()
	if k.lastPolledClock.Before(since) {
		copy(results, k.lastPolledChanges)
	}
	return results
}

// Returns the set of index entries for the channel more recent than the
// specified since SequenceClock.  Index entries with sequence values greater than
// the index stable sequence are not returned.
func (k *kvChannelIndex) getChanges(since SequenceClock) ([]*LogEntry, error) {

	var results []*LogEntry

	// If requested clock is later than the channel clock, return empty
	if since.After(k.clock) {
		base.LogTo("DIndex+", "requested clock is later than channel clock - no new changes to report")
		return results, nil
	}

	// If the since value is more recent than the last polled clock, return the results from the
	// last polling.  Has the potential to return values earlier than since and later than
	// lastPolledClock, but these duplicates will be ignored by replication.  Could validate
	// greater than since inside this if clause, but leaving out as a performance optimization for
	// now
	if lastPolledResults := k.checkLastPolled(since); len(lastPolledResults) > 0 {
		return lastPolledResults, nil
	}

	return k.channelStorage.GetChanges(since, k.stableSequence, k.clock)
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

// A vbCache caches a set of LogEntry values, representing the set of entries for the channel for a
// particular vbucket.  Intended to cache recently accessed channel block data, to avoid repeated
// block retrieval, parsing, and deduplication.
// The validFrom and validTo specify the range for which the cache is valid.
// Additional entries can be added to the cache using appendEntries (for entries after validTo), or
// prependEntries (backfilling values prior to validFrom).

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

	if v.validTo > 0 && validFrom > v.validTo+1 {
		return errors.New("Cache conflict - attempt to append entries with gap in valid range")
	}

	entries, duplicateDocIDs, err := v.validateCacheUpdate(entries, false)
	if err != nil {
		return err
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
func (v *vbCache) prependEntries(entries SortedEntrySet, validFrom uint64, validTo uint64) error {
	if len(entries) == 0 {
		return nil
	}

	if v.validFrom < math.MaxUint64 && validTo < v.validFrom-1 {
		return errors.New("Cache conflict - attempt to prepend entries with gap in valid range")
	}

	entries, _, err := v.validateCacheUpdate(entries, true)

	if err != nil {
		return err
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
			return entries, duplicateEntries, errors.New("Entries must be ordered by sequence when updating cache")
		}
		prevSeq = entry.Sequence

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
	i := SearchSortedEntrySet(*h, x)
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
func getIndexBlockKey(channelName string, blockIndex uint16, partition uint16) string {
	return fmt.Sprintf("%s:%s:%d:block%d", kCachePrefix, channelName, partition, blockIndex)
}

// Get the key for the cache block, based on the block index
func getChannelClockKey(channelName string) string {
	return fmt.Sprintf("%s_SequenceClock:%s", kCachePrefix, channelName)
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
