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
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

var ByteCachePollingTime = 1000 // initial polling time for notify, ms

const kSequenceOffsetLength = 0 // disabled until we actually need it
const kMaxVbNo = 1024           // TODO: load from cluster config

type kvChannelIndex struct {
	context             *DatabaseContext  // Database connection (used for connection queries)
	partitionMap        IndexPartitionMap // Index partition map
	channelName         string
	lastSequence        uint64
	lastCounter         uint64
	notifyRunning       bool
	notifyLock          sync.Mutex
	lastNotifiedChanges []*LogEntry
	lastNotifiedSince   uint64
	stableSequence      uint64
	stableSequenceCb    func() (uint64, error)
	onChange            func(base.Set)
	indexBlockCache     map[string]IndexBlock // Recently used index blocks, by key
	indexBlockCacheLock sync.Mutex
	clock               *channelClock
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
	return kvIndexEntry{sequence: logEntry.Sequence, removal: isRemoval}
}

func NewKvChannelIndex(channelName string, context *DatabaseContext, partitions IndexPartitionMap, stableSequenceCallback func() (uint64, error), onChangeCallback func(base.Set)) *kvChannelIndex {

	channelIndex := &kvChannelIndex{
		channelName:      channelName,
		context:          context,
		partitionMap:     partitions,
		stableSequenceCb: stableSequenceCallback,
		onChange:         onChangeCallback,
	}

	channelIndex.indexBlockCache = make(map[string]IndexBlock)

	// Init stable sequence
	stableSequence, err := channelIndex.stableSequenceCb()
	if err != nil {
		stableSequence = 0
	}

	// Initialize clock
	channelIndex.clock = NewChannelClock(context.Bucket, channelName)

	channelIndex.stableSequence = stableSequence
	return channelIndex

}

//
// Index Writing
//

func (k *kvChannelIndex) Add(entry kvIndexEntry) error {
	// Update the sequence in the appropriate cache block
	base.LogTo("DCache+", "Add to channel index %s, isRemoval:%v", k.channelName, entry.removal)
	entries := make([]kvIndexEntry, 0)
	entries = append(entries, entry)
	k.writeSingleBlockWithCas(entries)
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
	blockSets := make(map[string][]kvIndexEntry)
	for _, entry := range entries {
		blockKey := GenerateBlockKey(k.channelName, entry.sequence, k.partitionMap[entry.vbNo])
		if _, ok := blockSets[blockKey]; !ok {
			blockSets[blockKey] = make([]kvIndexEntry, 0)
		}
		blockSets[blockKey] = append(blockSets[blockKey], entry)
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
	err := k.writeClockCas(entries)

	return err
}

// Expects all incoming entries to target the same block
func (k *kvChannelIndex) writeSingleBlockWithCas(entries []kvIndexEntry) error {

	casSuccess := false
	if len(entries) == 0 {
		return nil
	}
	// Get block based on first entry
	block := k.getIndexBlockForEntry(entries[0])
	for casSuccess == false {
		// Apply updates to the in-memory block
		for _, entry := range entries {
			err := block.AddEntry(entry)
			// Wrong block for this entry
			if err != nil {
				return err
			}
		}

		value, err := block.Marshal()
		if err != nil {
			base.Warn("Unable to marshal channel block - cancelling block update")
			return errors.New("Error marshalling channel block")
		}
		casOut, err := k.context.Bucket.WriteCas(block.Key(), 0, 0, block.Cas(), value, sgbucket.Raw)
		if err != nil {
			// CAS failure - reload block for another try
			log.Println("CAS error - retrying")
			err = k.loadBlock(block)
			if err != nil {
				return err
			}
		} else {
			// Success - update the local cas value for next time
			block.SetCas(casOut)
			casSuccess = true
		}
	}
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

	newValue, err := k.context.Bucket.Incr(key, 1, 1, 0)
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
	stableSequence, err := k.stableSequenceCb()
	if err != nil {
		stableSequence = 0
	}

	if stableSequence <= k.stableSequence {
		return true
	}

	k.stableSequence = stableSequence

	base.LogTo("DCacheDebug", "Updating stable sequence to: %d (%s)", k.stableSequence, k.channelName)

	sequenceAsVar := &base.IntMax{}
	sequenceAsVar.SetIfMax(int64(k.stableSequence))
	changeCacheExpvars.Set(fmt.Sprintf("stableSequence-%s", k.channelName), sequenceAsVar)

	currentCounter, err := k.getIndexCounter()
	if err != nil {
		return true
	}

	// If there's an update, cache the recent changes in memory, as we'll expect all
	// all connected clients to request these changes
	base.LogTo("DCache", "poll for changes on channel %s: (current, last) (%d, %d)", k.channelName, currentCounter, k.lastCounter)
	if currentCounter > k.lastCounter {
		k.UpdateRecentCache(currentCounter)
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

func (k *kvChannelIndex) UpdateRecentCache(currentCounter uint64) {
	k.notifyLock.Lock()
	defer k.notifyLock.Unlock()

	// Compare counter again, in case someone has already updated cache while we waited for the lock
	if currentCounter > k.lastCounter {
		options := ChangesOptions{Since: SequenceID{Seq: k.lastSequence}}
		_, recentChanges := k.getCachedChanges(options)
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

func (k *kvChannelIndex) getCachedChanges(options ChangesOptions) (uint64, []*LogEntry) {

	/*
		// Check whether we can use the cached results from the latest poll
		base.LogTo("DCacheDebug", "Comparing since=%d with k.lastSince=%d", options.Since.SafeSequence(), k.lastNotifiedSince)
		if k.lastNotifiedSince > 0 && options.Since.SafeSequence() == k.lastNotifiedSince {
			return uint64(0), k.lastNotifiedChanges
		}

		// If not, retrieve from cache
		stableSequence := k.stableSequence
		base.LogTo("DCacheDebug", "getCachedChanges - since=%d, stable sequence: %d", options.Since.Seq, k.stableSequence)

		var cacheContents []*LogEntry
		since := options.Since.SafeSequence()

		// Byte channel cache is separated into docs of (byteCacheBlockCapacity) sequences.
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
	return uint64(0), []*LogEntry{}
}

func (k *kvChannelIndex) getIndexCounter() (uint64, error) {
	key := getIndexCountKey(k.channelName)
	var intValue uint64
	_, err := k.context.Bucket.Get(key, &intValue)
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

	data, cas, err := k.context.Bucket.GetRaw(block.Key())
	if err != nil {
		return err
	}

	err = block.Unmarshal(data, cas)

	if err != nil {
		return err
	}
	// If found, add to the cache
	k.putIndexBlockToCache(block.Key(), block)
	return nil
}

func (k *kvChannelIndex) writeClockCas(entries []kvIndexEntry) error {

	casSuccess := false
	if len(entries) == 0 {
		return nil
	}
	// Make a copy of the channel clock to use during update
	updatedClock := k.clock.clone()
	for casSuccess == false {
		// Apply updates to the
		for _, entry := range entries {
			// assumes always ascending
			updatedClock.value[entry.vbNo] = entry.sequence
		}

		value, err := updatedClock.Marshal()
		if err != nil {
			base.Warn("Unable to marshal channel clock - cancelling clock update")
			return errors.New("Error marshalling channel clock")
		}
		casOut, err := k.context.Bucket.WriteCas(getChannelClockKey(k.channelName), 0, 0, updatedClock.cas, value, sgbucket.Raw)
		if err != nil {
			// CAS failure - reload clock for another try
			log.Println("CAS error on clock update - retrying")
			updatedClock.load(k.context.Bucket, k.channelName)
			if err != nil {
				return err
			}
		} else {
			// Success - update the local cas value for next time
			updatedClock.cas = casOut
			// TODO: copying into k.clock might be less GC overhead, but at cost of CPU processing.
			k.clock = updatedClock
			casSuccess = true
		}
	}
	return nil
}

///////// Channel clock
type channelClock struct {
	value map[uint16]uint64
	cas   uint64
	lock  sync.RWMutex
}

func NewChannelClock(bucket base.Bucket, channelName string) *channelClock {
	// Initialize empty clock
	clock := channelClock{
		value: make(map[uint16]uint64, kMaxVbNo),
	}
	clock.load(bucket, channelName)
	return &clock
}

// Copies a channel clock
func (c *channelClock) clone() *channelClock {
	c.lock.RLock()
	defer c.lock.RUnlock()
	copy := channelClock{
		value: make(map[uint16]uint64, len(c.value)),
		cas:   c.cas,
	}

	for k, v := range c.value {
		copy.value[k] = v
	}
	return &copy
}

func (c *channelClock) load(bucket base.Bucket, channelName string) error {
	data, cas, err := bucket.GetRaw(getChannelClockKey(channelName))
	if err != nil {
		// return empty clock
		base.LogTo("DCache+", "Unable to find existing channel clock for channel %s - initializing as new", channelName)
		return errors.New("Channel clock not found")
	}
	c.Unmarshal(data)
	c.cas = cas
	return nil
}

// TODO: replace with something more intelligent than gob encode, to take advantage of known
//       clock structure?
func (c *channelClock) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(c.value)
	if err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

func (c *channelClock) Unmarshal(value []byte) error {

	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&c.value)
	if err != nil {
		return err
	}
	return nil
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
	return fmt.Sprintf("%s_channelClock:%s", kCachePrefix, channelName)
}

// Generate the key for a single sequence/entry
func getEntryKey(sequence uint64) string {
	return fmt.Sprintf("%s:seq:%d", kCachePrefix, sequence)
}
