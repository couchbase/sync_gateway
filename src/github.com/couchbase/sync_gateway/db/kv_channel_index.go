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
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

var byteCacheBlockCapacity = uint64(10000)
var ByteCachePollingTime = 1000 // initial polling time for notify, ms

const kSequenceOffsetLength = 0 // disabled until we actually need it

type kvChannelIndex struct {
	context             *DatabaseContext // Database connection (used for connection queries)
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

func NewKvChannelIndex(channelName string, stableSequenceCallback func() (uint64, error), onChangeCallback func(base.Set)) *kvChannelIndex {

	channelCache := &kvChannelIndex{
		channelName:      channelName,
		stableSequenceCb: stableSequenceCallback,
		onChange:         onChangeCallback,
	}
	// Init stable sequence
	stableSequence, err := channelCache.stableSequenceCb()
	if err != nil {
		stableSequence = 0
	}

	channelCache.stableSequence = stableSequence
	return channelCache

}

//
// Index Writing
//

func (k *kvChannelIndex) Add(entry kvIndexEntry) error {
	// Update the sequence in the appropriate cache block
	base.LogTo("DCache+", "Add to channel cache %s, isRemoval:%v", k.channelName, entry.removal)
	return k.addIndexEntry(entry.sequence, entry.removal)
}

func (k *kvChannelIndex) AddSet(entries []kvIndexEntry) error {

	base.LogTo("DCache", "enter AddSet for channel %s, %+v", k.channelName, entries)
	// Update the sequences in the appropriate cache block
	if len(entries) == 0 {
		return nil
	}

	// get cache block for first sequence
	cacheBlock := k.readOrCreateIndexBlockForSequence(entries[0].sequence)
	lowSequence := uint64(entries[0].sequence/byteCacheBlockCapacity) * byteCacheBlockCapacity
	highSequence := lowSequence + byteCacheBlockCapacity

	base.LogTo("DCache", "highSequence: %d", highSequence)
	var nextBlock *IndexBlock
	for _, entry := range entries {
		var addError error
		if entry.sequence >= highSequence {
			base.LogTo("DCache", "next block handling for sequence : %d", entry.sequence)
			if nextBlock == nil {
				nextBlock = k.readOrCreateIndexBlockForSequence(entry.sequence)
			}
			addError = nextBlock.addSequence(entry.sequence, entry.removal)
		} else {
			addError = cacheBlock.addSequence(entry.sequence, entry.removal)
		}

		if addError != nil {
			base.Warn("Error adding sequence to block - adding individually")
			k.addIndexEntry(entry.sequence, entry.removal)
		}

	}

	err := k.writeIndexBlock(cacheBlock)
	if err != nil {
		return err
	}

	if nextBlock != nil {
		err = k.writeIndexBlock(nextBlock)
		if err != nil {
			return err
		}
	}

	return nil
}

// Writes the index block
func (k *kvChannelIndex) writeIndexBlock(block *IndexBlock) error {
	return k.context.Bucket.SetRaw(block.key, 0, block.value)
}

func (k *kvChannelIndex) addIndexEntry(sequence uint64, isRemoval bool) error {
	cacheBlock := k.readOrCreateIndexBlockForSequence(sequence)
	cacheBlock.addSequence(sequence, isRemoval)
	err := k.writeIndexBlock(cacheBlock)
	if err != nil {
		return err
	}
	return nil
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
	return uint64(0), result
}

func (k *kvChannelIndex) getIndexCounter() (uint64, error) {
	key := getIndexCountKey(k.channelName)
	var intValue uint64
	err := k.context.Bucket.Get(key, &intValue)
	if err != nil {
		// return nil here - cache clock may not have been initialized yet
		return 0, nil
	}
	return intValue, nil
}

// Index block management

///////// DenseCacheBlock - management of a single channel cache block (one doc)
// Cache block is stored as binary, as a collection of [vbNo][sequence flags]
type IndexBlock struct {
	key         string            // DocID for the cache block doc
	value       []byte            // Raw document value
	values      map[uint16][]byte // Contents of the cache block doc
	minSequence uint64            // Starting sequence
}

func NewIndexBlock(channelName string, sequence uint64) (*IndexBlock, error) {

	// Calculate the key
	blockIndex := uint16(sequence / byteCacheBlockCapacity)
	key := getIndexBlockKey(channelName, blockIndex)

	// Initialize the byte[] bucket
	cacheValue := make([]byte, byteCacheBlockCapacity+kSequenceOffsetLength)
	sequenceOffset := uint64(sequence/byteCacheBlockCapacity) * byteCacheBlockCapacity
	// Write the sequence Offset first (8 bytes for uint64)
	/* disabling until we actually need the offset
	offSet := intToBytes(sequenceOffset)
	copy(cacheValue[0:8], offset)
	*/
	// Grow the buffer by kvCacheCapacity

	cacheBlock := &IndexBlock{
		key:         key,
		value:       cacheValue,
		minSequence: sequenceOffset,
	}
	return cacheBlock, nil

}

// Determine the cache block index for a sequence
func (k *kvChannelIndex) getBlockIndex(sequence uint64) uint16 {
	base.LogTo("DCache", "block index for sequence %d is %d", sequence, uint16(sequence/byteCacheBlockCapacity))
	return uint16(sequence / byteCacheBlockCapacity)
}

func (k *kvChannelIndex) readOrCreateIndexBlockForSequence(sequence uint64) *IndexBlock {

	cacheBlock := k.readIndexBlockForSequence(sequence)
	if cacheBlock == nil {
		cacheBlock, _ = NewIndexBlock(k.channelName, sequence)
	}
	return cacheBlock
}

// Loads the cache block associated with the sequence
func (k *kvChannelIndex) readIndexBlockForSequence(sequence uint64) *IndexBlock {
	// Determine which cache block should be used for the sequence, and read that block
	return k.readIndexBlock(k.getBlockIndex(sequence))
}

// Loads the cache block
func (k *kvChannelIndex) readIndexBlock(blockIndex uint16) *IndexBlock {

	docID := getIndexBlockKey(k.channelName, blockIndex)
	cacheBlockDoc, err := k.context.Bucket.GetRaw(docID)
	// Not found
	if err != nil {
		return nil
	}
	startSequence := uint64(blockIndex) * byteCacheBlockCapacity
	cacheBlock := &IndexBlock{
		key:         docID,
		minSequence: startSequence,
	}
	cacheBlock.load(cacheBlockDoc)
	return cacheBlock
}

func (b *IndexBlock) addSequence(sequence uint64, isRemoval bool) error {
	// Byte for sequence:
	//   0 : sequence is not in channel
	//   1 : sequence is in channel
	//   2 : sequence triggers removal from channel
	index := b.getIndexForSequence(sequence)
	if index < 0 || index >= uint64(len(b.value)) {
		base.LogTo("DCache", "CACHE ERROR CACHE ERROR KLAXON KLAXON")
		return errors.New("Sequence out of range of block")
	}
	if isRemoval {
		b.value[index] = 2
	} else {
		b.value[index] = 1
	}
	return nil
}

func (b *IndexBlock) hasSequence(sequence uint64) bool {
	base.LogTo("DCache", "hasSequence? %d ", sequence)
	return b.value[b.getIndexForSequence(sequence)] > 0
}

func (b *IndexBlock) getIndexForSequence(sequence uint64) uint64 {
	base.LogTo("DCache", "GetIndexForSequence %d: %d", sequence, sequence-b.minSequence+kSequenceOffsetLength)
	return sequence - b.minSequence + kSequenceOffsetLength
}

func (b *IndexBlock) load(contents []byte) {
	b.value = contents
}

////////  Utility functions for key generation

// Get the key for the cache count doc
func getIndexCountKey(channelName string) string {
	return fmt.Sprintf("%s_count:%s", kCachePrefix, channelName)
}

// Get the key for the cache block, based on the block index
func getIndexBlockKey(channelName string, blockIndex uint16) string {
	return fmt.Sprintf("%s:%s:block%d", kCachePrefix, channelName, blockIndex)
}

// Generate the key for a single sequence/entry
func getEntryKey(sequence uint64) string {
	return fmt.Sprintf("%s:seq:%d", kCachePrefix, sequence)
}
