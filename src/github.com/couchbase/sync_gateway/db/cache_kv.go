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
	"fmt"
	"sync"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	kCachePrefix = "_cache"
)

type kvCache struct {
	channelCaches    map[string]*distributedChannelCache
	channelCacheLock sync.RWMutex // Coordinates access to channel map
	storage          base.Bucket
	onChange         func(base.Set) // Client callback that notifies of channel changes
}

////// Cache writer API

func (b *kvCache) Init(initialSequence uint64) {
	// not sure yet whether we'll need initial sequence
	b.channelCaches = make(map[string]*distributedChannelCache)
}

func (b *kvCache) Prune() {
	// TODO: currently no pruning of channel caches
}

func (b *kvCache) Clear(initialSequence uint64) {
	// TODO: Currently no clear for distributed cache
	// temp handling until implemented, to pass unit tests
	b.channelCaches = make(map[string]*distributedChannelCache)
}

func (b *kvCache) AddToCache(change *LogEntry) base.Set {
	addedTo := make([]string, 0, len(change.Channels))
	ch := change.Channels

	// TODO: Late sequence handling

	// Add the cache entry
	b.writeEntry(change)

	// Add to channel caches
	for channelName, removal := range ch {
		// TODO: cache writers are only using a transient instance of channel cache, as there's no locking/persistence
		//       needed.  Should we avoid allocation of the struct and just move the relevant methods up to
		//       the kvCache with a channel name parameter?
		if removal == nil || removal.Seq == change.Sequence {
			b.addToChannelCache(channelName, change, removal != nil)
			addedTo = append(addedTo, channelName)
			if change.Skipped {
				b.addLateSequence(channelName, change)
			}
		}
	}

	if EnableStarChannelLog {
		b.addToChannelCache(channels.UserStarChannel, change, false)
		addedTo = append(addedTo, channels.UserStarChannel)
	}

	return base.SetFromArray(addedTo)
}

func (b *kvCache) getCacheHelper(channelName string) byteCacheHelper {
	return NewByteCacheHelper(channelName, b.storage)
}

func (b *kvCache) getChannelCache(channelName string) *distributedChannelCache {
	b.channelCacheLock.RLock()
	channelCache, found := b.channelCaches[channelName]
	b.channelCacheLock.RUnlock()
	if !found {
		b.channelCacheLock.Lock()
		channelCache = NewDistributedChannelCache(channelName, b)
		b.channelCaches[channelName] = channelCache
		b.channelCacheLock.Unlock()
	}
	return channelCache

}

func (b *kvCache) SetNotifier(onChange func(base.Set)) {
	b.onChange = onChange
}

/////// Cache reader API

func (b *kvCache) GetChanges(channelName string, options ChangesOptions) (entries []*LogEntry, err error) {

	// TODO: add backfill from view.  Currently expects infinite cache
	_, resultFromCache := b.GetCachedChanges(channelName, options)
	b.enableNotifications(channelName)
	return resultFromCache, nil
}

func (b *kvCache) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {

	validFrom, changes := b.getChannelCache(channelName).getCachedChanges(options)

	// Limit handling
	if options.Limit > 0 && len(changes) > options.Limit {
		limitResult := make([]*LogEntry, options.Limit)
		copy(limitResult[0:], changes[0:])
		return uint64(0), limitResult
	}

	// todo: Set validFrom when we enable pruning/compacting cache
	return validFrom, changes
}

// TODO: Implement late sequence handling
func (b *kvCache) InitLateSequenceClient(channelName string) uint64 {
	// no-op
	return 0
}

func (b *kvCache) GetLateSequencesSince(channelName string, sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {
	// no-op
	return entries, lastSequence, nil
}

func (b *kvCache) ReleaseLateSequenceClient(channelName string, sequence uint64) error {
	// no-op
	return nil
}

func (b *kvCache) addToChannelCache(channelName string, change *LogEntry, isRemoval bool) {

	channelCacheHelper := b.getCacheHelper(channelName)
	channelCacheHelper.Add(change, isRemoval)

	// Update the channel cache metadata
	b.updateCacheClock(channelName, change)
	base.LogTo("DCache", "    #%d ==> channel %q", change.Sequence, channelName)
}

// Add late sequence information to channel cache
func (b *kvCache) addLateSequence(channelName string, change *LogEntry) error {
	// TODO: no-op for now
	return nil
}

func (b *kvCache) updateCacheClock(channelName string, change *LogEntry) {

	key := getCacheCountKey(channelName)

	newValue := change.Sequence
	// increment cache clock if new sequence is higher than clock
	// TODO: review whether we should use
	byteValue, err := b.storage.GetRaw(key)
	if err != nil {
		b.storage.SetRaw(key, 0, uint64ToByte(newValue))
		return
	}
	value := byteToUint64(byteValue)
	if value < newValue {
		b.storage.SetRaw(key, 0, uint64ToByte(newValue))
	}
}

// Writes a single doc to the cache for the entry.
func (b *kvCache) writeEntry(change *LogEntry) {

	cacheEntry := &distributedChannelCacheEntry{
		Sequence: change.Sequence,
		DocID:    change.DocID,
		RevID:    change.RevID,
		Flags:    change.Flags,
	}
	key := getEntryKey(change.Sequence)
	value, _ := json.Marshal(cacheEntry)
	b.storage.SetRaw(key, 0, value)

}

// Notification handling
func (b *kvCache) enableNotifications(channelName string) {
	b.getChannelCache(channelName).startNotify()
}

// Reads a single doc from the cache for the entry.
func readCacheEntry(sequence uint64, storage base.Bucket) *LogEntry {

	var cacheEntry distributedChannelCacheEntry

	key := getEntryKey(sequence)
	value, err := storage.GetRaw(key)
	if err != nil {
		base.LogTo("DCache", "Error retrieving entry from storage for sequence %d", sequence)
		return nil
	}

	err = json.Unmarshal(value, &cacheEntry)
	if err != nil {
		base.LogTo("DCache", "Error unmarshalling entry for sequence %d", sequence)
		return nil
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
	return logEntry
}

// Notes:
// Functional logic that happens in channel_cache, not in change cache:
//  - removed handling, since it varies per channel

type distributedChannelCacheEntry struct {
	Sequence uint64 `json:"seq"`
	DocID    string `json:"doc"`
	RevID    string `json:"rev"`
	Flags    uint8  `json:"flags"`
}

////////  Utility functions for key generation:

// Get the key for the cache count doc
func getCacheCountKey(channelName string) string {
	return fmt.Sprintf("%s_count:%s", kCachePrefix, channelName)
}

// Get the key for the cache block, based on the block index
func getCacheBlockKey(channelName string, blockIndex uint16) string {
	return fmt.Sprintf("%s:%s:block%d", kCachePrefix, channelName, blockIndex)
}

// Generate the key for a single sequence/entry
func getEntryKey(sequence uint64) string {
	return fmt.Sprintf("%s:seq:%d", kCachePrefix, sequence)
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
	binary.Read(readBuffer, binary.LittleEndian, &result)
	return result
}

func uint64ToByte(input uint64) []byte {
	result := new(bytes.Buffer)
	binary.Write(result, binary.LittleEndian, input)
	return result.Bytes()
}
