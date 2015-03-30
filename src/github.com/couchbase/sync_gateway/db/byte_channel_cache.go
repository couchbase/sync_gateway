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
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"sync"
	"time"
)

var byteCacheBlockCapacity = uint64(10000)
var ByteCachePollingTime = 1000 // initial polling time for notify, ms

const kSequenceOffsetLength = 0 // disabled until we actually need it

type distributedChannelCache struct {
	channelName         string
	lastSequence        uint64
	lastCounter         uint64
	notifyRunning       bool
	notifyLock          sync.Mutex
	lastNotifiedChanges []*LogEntry
	lastNotifiedSince   uint64
	cache               *kvCache
}

func NewDistributedChannelCache(channelName string, cache *kvCache) *distributedChannelCache {
	return &distributedChannelCache{
		channelName: channelName,
		cache:       cache,
	}
}

func (dcc *distributedChannelCache) startNotify() {
	if !dcc.notifyRunning {
		base.LogTo("DCache", "Starting notify for channel %s", dcc.channelName)
		dcc.notifyLock.Lock()
		defer dcc.notifyLock.Unlock()
		// ensure someone else didn't already start while we were waiting
		if dcc.notifyRunning {
			return
		}
		dcc.notifyRunning = true
		go func() {
			for dcc.pollForChanges() {
				// TODO: geometrically increase sleep time to better manage rarely updated channels? Would result in increased
				// latency for those channels for connected clients, but reduce churn until we have DCP-based
				// notification instead of polling
				time.Sleep(time.Duration(ByteCachePollingTime) * time.Millisecond)
			}

		}()

	} else {
		base.LogTo("DCache", "Notify already running for channel %s", dcc.channelName)
	}
}

func (dcc *distributedChannelCache) pollForChanges() bool {

	cacheHelper := NewByteCacheHelper(dcc.channelName, dcc.cache.storage)
	currentCounter, err := cacheHelper.getCacheClock()
	if err != nil {
		return false
	}

	// If there's an update, cache the recent changes in memory, as we'll expect all
	// all connected clients to request these changes
	if currentCounter > dcc.lastCounter {
		dcc.UpdateRecentCache(cacheHelper, currentCounter)
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

func (dcc *distributedChannelCache) UpdateRecentCache(cacheHelper byteCacheHelper, currentCounter uint64) {
	dcc.notifyLock.Lock()
	defer dcc.notifyLock.Unlock()

	// Compare counter again, in case someone has already updated cache while we waited for the lock
	if currentCounter > dcc.lastCounter {
		options := ChangesOptions{Since: SequenceID{Seq: dcc.lastSequence}}
		_, dcc.lastNotifiedChanges = cacheHelper.getCachedChanges(options)
		if len(dcc.lastNotifiedChanges) > 0 {
			dcc.lastNotifiedSince = dcc.lastSequence
			dcc.lastSequence = dcc.lastNotifiedChanges[len(dcc.lastNotifiedChanges)-1].Sequence
			dcc.lastCounter = currentCounter
		} else {
			base.Warn("pollForChanges: channel [%s] clock changed to %d (from %d), but no changes found in cache", dcc.channelName, currentCounter, dcc.lastCounter)
		}
		base.LogTo("DCache", "Calling notify for channel %s", dcc.channelName)
		if dcc.cache.onChange != nil {
			dcc.cache.onChange(base.SetOf(dcc.channelName))
		}
	}
}

func (dcc *distributedChannelCache) getCachedChanges(options ChangesOptions) (uint64, []*LogEntry) {

	// Check whether we can use the cached results from the latest poll

	cacheHelper := NewByteCacheHelper(dcc.channelName, dcc.cache.storage)
	base.LogTo("DCache", "Comparing with dcc.lastSince=%d", dcc.lastNotifiedSince)
	if dcc.lastNotifiedSince > 0 && options.Since.SafeSequence() == dcc.lastNotifiedSince {
		base.LogTo("DCache", "Returning cached poll results")
		return uint64(0), dcc.lastNotifiedChanges
	}

	// If not, retrieve from cache
	return cacheHelper.getCachedChanges(options)

}

type byteCacheHelper struct {
	channelName string
	storage     base.Bucket
	lastChanges []*LogEntry
	lastSince   uint64
}

func NewByteCacheHelper(channel string, storage base.Bucket) byteCacheHelper {
	return byteCacheHelper{
		channelName: channel,
		storage:     storage,
	}
}

func (b *byteCacheHelper) Add(change *LogEntry, isRemoval bool) error {
	// Update the sequence in the appropriate cache block

	base.LogTo("DCache", "Add to channel cache %s, isRemoval:%v", b.channelName, isRemoval)

	cacheBlock := b.readCacheBlockForSequence(change.Sequence)
	if cacheBlock == nil {
		base.LogTo("DCache", "creating new cache block for sequence %d", change.Sequence)
		cacheBlock, _ = b.newCacheBlock(change.Sequence)
	}
	cacheBlock.addSequence(change.Sequence, isRemoval)
	b.writeCacheBlock(cacheBlock)

	return nil
}

func (b *byteCacheHelper) getCachedChanges(options ChangesOptions) (uint64, []*LogEntry) {

	var cacheContents []*LogEntry
	since := options.Since.SafeSequence()

	base.LogTo("DCache", "Starting getCachedChanges for channel %s, Since=%d", b.channelName, options.Since.Seq)

	// Byte channel cache is separated into docs of (byteCacheBlockCapacity) sequences.
	blockIndex := b.getCacheBlockIndex(since)
	block := b.readCacheBlock(blockIndex)
	// Get index of sequence within block
	offset := uint64(blockIndex) * byteCacheBlockCapacity
	startIndex := (since + 1) - offset
	// Iterate over blocks
	for block != nil {
		for i := startIndex; i < byteCacheBlockCapacity; i++ {
			if block.value[i] > 0 {
				sequence := i + offset
				entry := readCacheEntry(sequence, b.storage)
				if block.value[i] == 2 {
					// deleted
					entry.Flags |= channels.Removed
				}

				base.LogTo("DCache", "appending entry for sequence %d: %+v", sequence, entry)
				cacheContents = append(cacheContents, entry)
			}
		}

		// Init for next block
		blockIndex++
		block = b.readCacheBlock(blockIndex)
		startIndex = 0
		offset = uint64(blockIndex) * byteCacheBlockCapacity
	}

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

	base.LogTo("DCache", " getCachedChanges returning %d changes", len(result))

	//TODO: correct validFrom
	return uint64(0), result
}

func (b *byteCacheHelper) getCacheClock() (uint64, error) {
	key := getCacheCountKey(b.channelName)
	byteValue, err := b.storage.GetRaw(key)
	if err != nil {
		return 0, err
	}

	return byteToUint64(byteValue), nil

}

func (b *byteCacheHelper) newCacheBlock(sequence uint64) (*ByteCacheBlock, error) {

	// Calculate the key
	blockIndex := b.getCacheBlockIndex(sequence)
	key := getCacheBlockKey(b.channelName, blockIndex)

	// Initialize the byte[] storage
	cacheValue := make([]byte, byteCacheBlockCapacity+kSequenceOffsetLength)
	sequenceOffset := uint64(sequence/byteCacheBlockCapacity) * byteCacheBlockCapacity
	// Write the sequence Offset first (8 bytes for uint64)
	/* disabling until we actually need the offset
	offSet := intToBytes(sequenceOffset)
	copy(cacheValue[0:8], offset)
	*/
	// Grow the buffer by kvCacheCapacity

	cacheBlock := &ByteCacheBlock{
		key:         key,
		value:       cacheValue,
		minSequence: sequenceOffset,
	}
	return cacheBlock, nil

}

// Loads the cache block associated with the sequence
func (b *byteCacheHelper) readCacheBlockForSequence(sequence uint64) *ByteCacheBlock {
	// Determine which cache block should be used for the sequence, and read that block
	return b.readCacheBlock(b.getCacheBlockIndex(sequence))
}

// Loads the cache block
func (b *byteCacheHelper) readCacheBlock(blockIndex uint16) *ByteCacheBlock {

	docID := getCacheBlockKey(b.channelName, blockIndex)
	cacheBlockDoc, err := b.storage.GetRaw(docID)
	// Not found
	if err != nil {
		return nil
	}
	startSequence := uint64(blockIndex) * byteCacheBlockCapacity
	cacheBlock := &ByteCacheBlock{
		key:         docID,
		minSequence: startSequence,
	}
	cacheBlock.load(cacheBlockDoc)
	return cacheBlock
}

// Determine the cache block index for a sequence
func (b *byteCacheHelper) getCacheBlockIndex(sequence uint64) uint16 {
	return uint16(sequence / byteCacheBlockCapacity)
}

// Writes the cache block associated with the sequence
func (b *byteCacheHelper) writeCacheBlock(block *ByteCacheBlock) error {
	return b.storage.SetRaw(block.key, 0, block.value)
}

///////// ByteCacheBlock - management of a single channel cache block (one doc)
type ByteCacheBlock struct {
	key         string // DocID for the cache block doc
	value       []byte // Contents of the cache block doc
	minSequence uint64 // index for block
}

// Loads the cache block from []byte
func (cb *ByteCacheBlock) load(contents []byte) {
	// do we care about sequence offset?  skip for now until we actually need it
	/*
		readBuffer := bytes.NewReader(contents)
		err = binary.Read(readBuffer, binary.LittleEndian, &block.sequenceOffset)
	*/
	cb.value = contents
}

/* Not using offset.  Leaving here for reference for now
func (c *ByteCacheBlock) getOffset() uint64 {
	buf := bytes.NewReader(c.value)
	offset, err := binary.ReadUvarint(buf)
	if err != nil {
		return offset
	} else {
		base.LogTo("DCache", "Error reading offset for cache block %v", c.value)
		return 0
	}

}

func (c *ByteCacheBlock) setOffset(value uint64) {
	return binary.PutUvarint(c.value, offset)
}
*/

func (c *ByteCacheBlock) addSequence(sequence uint64, isRemoval bool) error {
	// Byte for sequence:
	//   0 : sequence is not in channel
	//   1 : sequence is in channel
	//   2 : sequence triggers removal from channel
	index := c.getIndexForSequence(sequence)
	if isRemoval {
		c.value[index] = 2
	} else {
		c.value[index] = 1
	}
	return nil
}

func (c *ByteCacheBlock) hasSequence(sequence uint64) bool {
	return c.value[c.getIndexForSequence(sequence)] > 0
}

func (c *ByteCacheBlock) getIndexForSequence(sequence uint64) uint64 {
	return sequence - c.minSequence + kSequenceOffsetLength
}
