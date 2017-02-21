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
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

var ByteCachePollingTime = 1000 // initial polling time for notify, ms

const (
	kMaxUnreadPollCount = 10  // If the channel polls for (kMaxUnusedPollCount) times after publishing a notification without anyone calling getChanges(), terminates polling
	kMaxEmptyPollCount  = 100 // If a channel is polled and finds no changes (kMaxEmptyPollCount) times, it triggers a null update in order to trigger unused poll handling (i.e. see if anyone is still listening)

)

// KvChannelIndex manages read requests from Sync Gateway to the bucket-based channel index, for a given channel.  Supports
// one-shot requests, as well as polling to support notification for continuous/longpoll requests.  KvChangeIndex maintains a collection
// of KvChannelIndex instances for active channels.
type KvChannelIndex struct {
	indexBucket            base.Bucket             // Database connection (used for connection queries)
	channelName            string                  // Channel name
	lastPolledChanges      []*LogEntry             // Set of changes found in most recent polling.  Optimization for scenario where multiple continuous changes listeners are awakened at once
	lastPolledSince        base.SequenceClock      // Since value used for most recent polling
	lastPolledValidTo      base.SequenceClock      // Stable sequence at time of last polling that found changes
	lastPolledChannelClock base.SequenceClock      // Channel clock value that triggered the most recent polling
	lastPolledLock         sync.RWMutex            // Synchronization for lastPolled data
	lastPolledPostStable   bool                    // Whether the channel clock was later than the stable sequence during the most recent polling.  (used to trigger update next polling window even if channel clock is unchanged)
	unreadPollCount        uint32                  // Number of times the channel has polled for data since the last non-empty poll, without a getChanges call
	pollCount              uint32                  // Number of times the channel has polled for data and not found changes
	onChange               func(base.Set)          // Notification callback
	clock                  *base.SequenceClockImpl // Channel clock
	channelStorage         ChannelStorageReader    // Channel storage - manages interaction with the index format
	partitions             *base.IndexPartitions   // Partition map
}

func NewKvChannelIndex(channelName string, bucket base.Bucket, partitions *base.IndexPartitions, onChangeCallback func(base.Set)) *KvChannelIndex {

	channelIndex := &KvChannelIndex{
		channelName:    channelName,
		indexBucket:    bucket,
		onChange:       onChangeCallback,
		channelStorage: NewDenseStorageReader(bucket, channelName, partitions),
		partitions:     partitions,
	}

	// Initialize and load channel clock
	channelIndex.loadClock()

	return channelIndex
}

func (k *KvChannelIndex) IndexBucket() base.Bucket {
	return k.indexBucket
}

func (k *KvChannelIndex) pollForChanges(stableClock base.SequenceClock, newChannelClock base.SequenceClock) (hasChanges bool, cancelPolling bool) {

	changeCacheExpvars.Add(fmt.Sprintf("pollCount-%s", k.channelName), 1)
	// Increment the overall poll count since a changes request (regardless of whether there have been polled changes)
	totalPollCount := atomic.AddUint32(&k.pollCount, 1)
	unreadPollCount := atomic.LoadUint32(&k.unreadPollCount)
	if unreadPollCount > kMaxUnreadPollCount {
		// We've sent a notify, but had (kMaxUnreadPollCount) polls without anyone calling getChanges.
		// Assume nobody is listening for updates - cancel polling for this channel
		base.LogTo("DIndex+", "Cancelling polling for channel %s", k.channelName)
		return false, true
	}

	if unreadPollCount > 0 {
		// Give listeners more time to call getChanges, but increment
		atomic.AddUint32(&k.unreadPollCount, 1)
	}

	k.lastPolledLock.Lock()
	defer k.lastPolledLock.Unlock()

	// First poll handling
	if k.lastPolledChannelClock == nil {
		k.lastPolledChannelClock = k.clock.Copy()
		k.lastPolledSince = k.clock.Copy()
		k.lastPolledValidTo = k.clock.Copy()
		k.lastPolledPostStable = true // We can't guarantee this wasn't the case when we loaded k.clock, so force an update
	}

	// Minimum clock is minimum of stableClock, new channel clock.
	isChanged, changedPartitions, hasPostStableChanges := k.calculateChanges(k.lastPolledChannelClock, stableClock, newChannelClock)

	if !isChanged && !k.lastPolledPostStable {
		// No changes to channel clock - update validTo based on the new stable sequence
		k.lastPolledValidTo.SetTo(stableClock)
		// If we've exceeded empty poll count, return hasChanges=true to trigger the "is
		// anyone listening" check
		if totalPollCount > kMaxEmptyPollCount {
			return true, false
		} else {
			return false, false
		}
	}

	if hasPostStableChanges {
		base.LogTo("DIndex+", "Channel %s has changes later than the stable sequence - will be updated in next polling cycle.", k.channelName)
	}
	k.lastPolledPostStable = hasPostStableChanges

	// The clock has changed - load the changes and store in last polled
	if err := k.updateLastPolled(stableClock, newChannelClock, changedPartitions); err != nil {
		base.Warn("Error updating last polled for channel %s: %v", k.channelName, err)
		return false, false
	}

	// We have changes - increment unread counter if we haven't already
	if unreadPollCount == 0 {
		atomic.AddUint32(&k.unreadPollCount, 1)
	}
	return true, false
}

func (k *KvChannelIndex) calculateChanges(lastPolledClock base.SequenceClock, stableClock base.SequenceClock, newChannelClock base.SequenceClock) (isChanged bool, changedPartitions []*base.PartitionRange, changesPostStable bool) {

	// One iteration through the new channel clock, to:
	//   1. Check whether it's later than the previous channel clock
	//   2. If it's later than the stable clock, roll back to the stable clock
	//   3. If it still represents a change, add to set of changed vbs and partition ranges
	changesPostStable = false
	changedPartitions = make([]*base.PartitionRange, k.partitions.PartitionCount())
	for vbNoInt, newChannelClockSeq := range newChannelClock.Value() {
		vbNo := uint16(vbNoInt)
		lastPolledClockSeq := lastPolledClock.GetSequence(vbNo)
		if lastPolledClockSeq < newChannelClockSeq {
			// Handle rollback to stable sequence if needed
			stableSeq := stableClock.GetSequence(vbNo)
			if stableSeq < newChannelClockSeq {
				changesPostStable = true
				newChannelClock.SetSequence(vbNo, stableSeq)
				// We've rolled back the new channel clock to the stable sequence.  Ensure that we still have a change for this vb.
				if !(lastPolledClockSeq < stableSeq) {
					continue
				}
				newChannelClockSeq = stableSeq
			}
			isChanged = true
			partitionNo := k.partitions.PartitionForVb(vbNo)
			if changedPartitions[partitionNo] == nil {
				partitionRange := base.NewPartitionRange()
				changedPartitions[partitionNo] = &partitionRange
			}
			changedPartitions[partitionNo].SetRange(vbNo, lastPolledClockSeq, newChannelClockSeq)
		}
	}
	return isChanged, changedPartitions, changesPostStable

}

func (k *KvChannelIndex) updateLastPolled(stableSequence base.SequenceClock, newChannelClock base.SequenceClock, changedPartitions []*base.PartitionRange) error {

	// Update the storage cache, if present
	err := k.channelStorage.UpdateCache(k.lastPolledChannelClock, newChannelClock, changedPartitions)
	if err != nil {
		return err
	}

	IndexExpvars.Add("updateChannelPolled", 1)
	k.lastPolledSince.SetTo(k.lastPolledChannelClock)
	k.lastPolledChannelClock.SetTo(newChannelClock)
	k.lastPolledValidTo.SetTo(stableSequence)

	return nil
}

// Returns the set of index entries for the channel more recent than the
// specified since SequenceClock.  Index entries with sequence values greater than
// the index stable sequence are not returned.
func (k *KvChannelIndex) GetChanges(sinceClock base.SequenceClock, toClock base.SequenceClock, limit int) ([]*LogEntry, error) {

	var results []*LogEntry

	// Someone is still interested in this channel - reset poll counts
	atomic.StoreUint32(&k.pollCount, 0)
	atomic.StoreUint32(&k.unreadPollCount, 0)

	chanClock, err := k.getChannelClock()
	if err != nil {
		// Note: gocb returns "Key not found.", go-couchbase returns "MCResponse status=KEY_ENOENT, opcode=GET, opaque=0, msg: Not found"
		// Using string matching to identify key not found for now - really need a better API in go-couchbase/gocb for gets that allows us to distinguish
		// between errors and key not found with something more robust than string matching.
		if IsNotFoundError(err) {
			// initialize chanClock as empty clock
			chanClock = base.NewSequenceClockImpl()
		} else {
			return results, err
		}
	}

	// If requested clock is later than the channel clock, return empty
	if sinceClock.AllAfter(chanClock) {
		base.LogTo("ChannelIndex+", "requested clock is later than channel clock - no new changes to report")
		return results, nil
	}

	// Don't request values later than channel clock
	if toClock != nil {
		toClock = toClock.LimitTo(chanClock)
	} else {
		toClock = chanClock
	}

	return k.channelStorage.GetChanges(sinceClock, toClock, limit)
}

func (k *KvChannelIndex) getIndexCounter() (uint64, error) {
	key := getIndexCountKey(k.channelName)
	var intValue uint64
	_, err := k.indexBucket.Get(key, &intValue)
	if err != nil {
		// return nil here - cache clock may not have been initialized yet
		return 0, nil
	}
	return intValue, nil
}

func (k *KvChannelIndex) getChannelClock() (base.SequenceClock, error) {

	var channelClock base.SequenceClock
	var err error
	// If we're polling, return a copy
	k.lastPolledLock.RLock()
	defer k.lastPolledLock.RUnlock()
	if k.lastPolledChannelClock != nil {
		channelClock = base.NewSequenceClockImpl()
		channelClock.SetTo(k.lastPolledChannelClock)
	} else {
		channelClock, err = k.loadChannelClock()
		if err != nil {
			return nil, err
		}
	}
	return channelClock, nil

}

func (k *KvChannelIndex) loadChannelClock() (base.SequenceClock, error) {

	chanClock := base.NewSyncSequenceClock()
	key := GetChannelClockKey(k.channelName)
	value, _, err := k.indexBucket.GetRaw(key)
	if err != nil {
		base.LogTo("DIndex+", "No existing channel clock for key %s:%v.  Using empty channel clock", key, err)
		return chanClock, err
	}
	err = chanClock.Unmarshal(value)
	if err != nil {
		base.Warn("Error unmarshalling channel clock for channel %s, clock value %v", k.channelName, value)
	}
	return chanClock, err
}

// Determine the cache block index for a sequence
func (k *KvChannelIndex) getBlockIndex(sequence uint64) uint16 {
	return uint16(sequence / byteIndexBlockCapacity)
}

func (k *KvChannelIndex) loadClock() {

	if k.clock == nil {
		k.clock = base.NewSequenceClockImpl()
	}
	data, cas, err := k.indexBucket.GetRaw(GetChannelClockKey(k.channelName))
	if err != nil {
		base.LogTo("DIndex+", "Unable to find existing channel clock for channel %s - treating as new", k.channelName)
	}
	k.clock.Unmarshal(data)
	k.clock.SetCas(cas)

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
	return fmt.Sprintf("%s_count:%s", base.KIndexPrefix, channelName)
}

// Get the key for the cache block, based on the block index
func GetIndexBlockKey(channelName string, blockIndex uint16, partition uint16) string {
	return fmt.Sprintf("%s:%s:block%d:%s", base.KIndexPrefix, channelName, blockIndex, vbSuffixMap[partition])
}

// Get the key for the cache block, based on the block index
func GetChannelClockKey(channelName string) string {
	return fmt.Sprintf("%s:chClock:%s", base.KIndexPrefix, channelName)
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
