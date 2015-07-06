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
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

const (
	kCachePrefix       = "_cache"
	kStableSequenceKey = "_cache_stableSeq"
	maxCacheUpdate     = 500
	minCacheUpdate     = 1
)

type kvChangeIndex struct {
	context          *DatabaseContext           // Database connection (used for connection queries)
	maxVbNo          uint16                     // Number of vbuckets
	indexPartitions  map[uint16]uint16          // Partitioning of vbuckets in the index
	channelIndexes   map[string]*kvChannelIndex // Channel indexes, map indexed by channel name
	channelIndexLock sync.RWMutex               // Coordinates access to channel index map
	onChange         func(base.Set)             // Client callback that notifies of channel changes
	pending          chan *LogEntry             // Incoming changes, pending indexing
}

////// Cache writer API

func (c *kvChangeIndex) Init(initialSequence uint64) {

	// not sure yet whether we'll need initial sequence
	c.channelIndexes = make(map[string]*kvChannelIndex)
	c.pending = make(chan *LogEntry, maxCacheUpdate)

	// Load index partitions
	c.indexPartitions = c.loadIndexPartitions()

	// start process to work pending sequences
	go c.indexPending()
}

func (c *kvChangeIndex) Prune() {
	// TODO: currently no pruning of channel indexes
}

func (c *kvChangeIndex) Clear(initialSequence uint64) {
	// TODO: Currently no clear for distributed cache
	// temp handling until implemented, to pass unit tests
	c.channelIndexes = make(map[string]*kvChannelIndex)
}

func (c *kvChangeIndex) AddToCache(change *LogEntry) base.Set {

	// queue for cache addition
	c.pending <- change
	return base.Set{}

}

func (b *kvChangeIndex) getChannelIndex(channelName string) *kvChannelIndex {

	b.channelIndexLock.RLock()
	defer b.channelIndexLock.RUnlock()
	return b.channelIndexes[channelName]
}

func (b *kvChangeIndex) newChannelIndex(channelName string) *kvChannelIndex {

	b.channelIndexLock.Lock()
	defer b.channelIndexLock.Unlock()
	b.channelIndexes[channelName] = NewKvChannelIndex(channelName, b.getStableSequence, b.onChange)
	return b.channelIndexes[channelName]
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

	// Continual processing of pending
	for {
		// Read entries from the pending list into array
		entries := c.readFromPending()

		// Wait group to track when the buffer has been completely processed
		var wg sync.WaitGroup
		channelSets := make(map[string][]kvIndexEntry)
		if EnableStarChannelLog {
			channelSets["*"] = make([]kvIndexEntry, 0, maxCacheUpdate)
		}

		// Iterate over entries to do cache entry writes, and group channel updates
		for _, logEntry := range entries {
			// Add cache entry
			wg.Add(1)
			go func(logEntry *LogEntry) {
				defer wg.Done()
				c.writeEntry(logEntry)
			}(logEntry)

			// Collect entries by channel for channel cache updates below
			for channelName, removal := range logEntry.Channels {
				if removal == nil || removal.Seq == logEntry.Sequence {
					_, found := channelSets[channelName]
					if !found {
						// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
						channelSets[channelName] = make([]kvIndexEntry, 0, maxCacheUpdate)
					}
					channelSets[channelName] = append(channelSets[channelName], kvIndexEntry{sequence: logEntry.Sequence, vbNo: logEntry.VbNo, removal: removal != nil})
				}
			}
			if EnableStarChannelLog {
				channelSets["*"] = append(channelSets["*"], kvIndexEntry{sequence: logEntry.Sequence, removal: false})
			}
		}

		// Iterate over channel collections, and update cache
		for channelName, entrySet := range channelSets {
			wg.Add(1)
			go func(channelName string, entrySet []kvIndexEntry) {
				defer wg.Done()
				c.addSetToChannelCache(channelName, entrySet)

			}(channelName, entrySet)
		}

		wg.Wait()
		// Update stable sequence
		lastSequence := entries[len(entries)-1].Sequence
		c.setStableSequence(lastSequence)
	}
}

func (b *kvChangeIndex) SetNotifier(onChange func(base.Set)) {
	b.onChange = onChange
}

func (b *kvChangeIndex) GetChanges(channelName string, options ChangesOptions) (entries []*LogEntry, err error) {

	// TODO: add backfill from view.  Currently expects infinite cache
	_, resultFromCache := b.GetCachedChanges(channelName, options)
	b.enableNotifications(channelName)
	return resultFromCache, nil
}

func (b *kvChangeIndex) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {

	validFrom, changes := b.getChannelIndex(channelName).getCachedChanges(options)

	// Limit handling
	if options.Limit > 0 && len(changes) > options.Limit {
		limitResult := make([]*LogEntry, options.Limit)
		copy(limitResult[0:], changes[0:])
		return uint64(0), limitResult
	}

	// todo: Set validFrom when we enable pruning/compacting cache
	return validFrom, changes
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

func (b *kvChangeIndex) addToChannelCache(channelName string, change kvIndexEntry) {

	b.getChannelIndex(channelName).Add(change)
	base.LogTo("DCache", "    #%d ==> channel %q", change.sequence, channelName)
}

func (b *kvChangeIndex) addSetToChannelCache(channelName string, entries []kvIndexEntry) {
	b.getChannelIndex(channelName).AddSet(entries)
}

// Add late sequence information to channel cache
func (b *kvChangeIndex) addLateSequence(channelName string, change *LogEntry) error {
	// TODO: no-op for now
	return nil
}

func (b *kvChangeIndex) getStableSequence() (uint64, error) {

	var intValue uint64
	err := b.context.Bucket.Get(kStableSequenceKey, &intValue)
	if err != nil {
		// return nil here - cache clock may not have been initialized yet
		return 0, nil
	}
	base.LogTo("StableSeq", "getStableSequence - returning %d", intValue)
	return intValue, nil
}

func (b *kvChangeIndex) setStableSequence(sequence uint64) error {

	err := b.context.Bucket.Set(kStableSequenceKey, 0, sequence)
	if err != nil {
		base.Warn("Error Setting stable sequence in setStableSequence(%s): %v", sequence, err)
		return err
	}

	return nil
}

// Writes a single doc to the cache for the entry.
func (b *kvChangeIndex) writeEntry(change *LogEntry) {

	cacheEntry := &kvChannelIndexEntry{
		Sequence: change.Sequence,
		DocID:    change.DocID,
		RevID:    change.RevID,
		Flags:    change.Flags,
	}
	key := getEntryKey(change.Sequence)
	value, _ := json.Marshal(cacheEntry)
	b.context.Bucket.SetRaw(key, 0, value)

}

// Load the index partition definitions
func (b *kvChangeIndex) loadIndexPartitions() map[uint16]uint16 {

	// TODO: load from the profile definition.  Hardcode for now to 32 sequential partitions
	partitions := make(map[uint16]uint16, b.maxVbNo)

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

// Reads a single doc from the cache for the entry.
func readCacheEntry(sequence uint64, bucket base.Bucket) (*LogEntry, error) {

	var cacheEntry kvChannelIndexEntry

	key := getEntryKey(sequence)
	value, err := bucket.GetRaw(key)
	if err != nil {
		base.Warn("DCache", "Error retrieving entry from bucket for sequence %d, key %s", sequence, key)
		return nil, err
	}

	err = json.Unmarshal(value, &cacheEntry)
	if err != nil {
		base.Warn("DCache", "Error unmarshalling entry for sequence %d, key %s", sequence, key)
		return nil, err
	}
	// TODO: the cache write/read loses information from the original LogEntry (timing).  re-confirm it's not needed by readers
	// TODO: Is there a way to optimize entry unmarshalling when being used by multiple readers?  Recent Sequence cache
	// per notify?

	logEntry := &LogEntry{
		Sequence: cacheEntry.Sequence,
		DocID:    cacheEntry.DocID,
		RevID:    cacheEntry.RevID,
		Flags:    cacheEntry.Flags,
	}
	return logEntry, nil
}

// Notes:
// Functional logic that happens in channel_cache, not in change cache:
//  - removed handling, since it varies per channel

type kvChannelIndexEntry struct {
	Sequence uint64 `json:"seq"`
	DocID    string `json:"doc"`
	RevID    string `json:"rev"`
	Flags    uint8  `json:"flags"`
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
