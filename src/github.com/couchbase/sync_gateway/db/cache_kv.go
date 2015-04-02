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
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	kCachePrefix       = "_cache"
	kStableSequenceKey = "_cache_stableSeq"
	maxCacheUpdate     = 500
	minCacheUpdate     = 1
)

type kvCache struct {
	channelCaches    map[string]*distributedChannelCache
	channelCacheLock sync.RWMutex // Coordinates access to channel map
	bucket           base.Bucket
	onChange         func(base.Set) // Client callback that notifies of channel changes

	pending chan *LogEntry
}

type kvCacheEntry struct {
	sequence uint64
	removal  bool
}

////// Cache writer API

func (b *kvCache) Init(initialSequence uint64) {

	// not sure yet whether we'll need initial sequence
	b.channelCaches = make(map[string]*distributedChannelCache)
	b.pending = make(chan *LogEntry, maxCacheUpdate)

	// start process to work pending sequences
	go b.cachePending()
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

	// queue for cache addition
	b.pending <- change
	return base.Set{}

}

func (b *kvCache) readFromPending() []*LogEntry {

	entries := make([]*LogEntry, 0, maxCacheUpdate)

	// TODO - needs cancellation handling?
	// Blocks until reading at least one from pending
	for {
		select {
		case entry := <-b.pending:
			entries = append(entries, entry)
			// read additional from cache if present, up to maxCacheUpdate
			for {
				select {
				case additionalEntry := <-b.pending:
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

func (b *kvCache) cachePending() {

	// TODO: cancellation handling

	// Continual processing of pending
	for {
		// Read entries from the pending list into array
		entries := b.readFromPending()

		// Wait group to determine when the buffer has been completely processed
		var wg sync.WaitGroup
		channelSets := make(map[string][]kvCacheEntry)
		if EnableStarChannelLog {
			channelSets["*"] = make([]kvCacheEntry, 0, maxCacheUpdate)
		}

		// Iterate over entries to do cache entry writes, and group channel updates
		for _, logEntry := range entries {
			// Add cache entry
			wg.Add(1)
			go func(logEntry *LogEntry) {
				defer wg.Done()
				b.writeEntry(logEntry)
			}(logEntry)

			// Collect entries by channel for channel cache updates below
			for channelName, removal := range logEntry.Channels {
				if removal == nil || removal.Seq == logEntry.Sequence {
					_, found := channelSets[channelName]
					if !found {
						// TODO: maxCacheUpdate may be unnecessarily large memory allocation here
						channelSets[channelName] = make([]kvCacheEntry, 0, maxCacheUpdate)
					}
					channelSets[channelName] = append(channelSets[channelName], kvCacheEntry{sequence: logEntry.Sequence, removal: removal != nil})
				}
			}
			if EnableStarChannelLog {
				channelSets["*"] = append(channelSets["*"], kvCacheEntry{sequence: logEntry.Sequence, removal: false})
			}
		}

		// Iterate over channel collections, and update cache
		for channelName, entrySet := range channelSets {
			wg.Add(1)
			go func(channelName string, entrySet []kvCacheEntry) {
				defer wg.Done()
				b.addSetToChannelCache(channelName, entrySet)

			}(channelName, entrySet)

			// Update cache clock
			wg.Add(1)
			go func(channelName string) {
				defer wg.Done()
				b.updateCacheClock(channelName)
			}(channelName)
		}

		wg.Wait()
		// Update stable sequence
		lastSequence := entries[len(entries)-1].Sequence
		b.setStableSequence(lastSequence)
	}
}

func (b *kvCache) _addToCache(change *LogEntry) base.Set {

	addedTo := make([]string, 0, len(change.Channels))
	ch := change.Channels
	// Add the cache entry first, to ensure it's present before cache clocks are updated
	writeTime := time.Now()
	b.writeEntry(change)

	changeCacheExpvars.Add("kv-cache-writeEntry", time.Since(writeTime).Nanoseconds())

	allChannelTime := time.Now()
	var wg sync.WaitGroup
	// Add to channel caches
	for channelName, removal := range ch {
		// TODO: cache writers are only using a transient instance of channel cache, as there's no locking/persistence
		//       needed.  Should we avoid allocation of the struct and just move the relevant methods up to
		//       the kvCache with a channel name parameter?
		if removal == nil || removal.Seq == change.Sequence {
			wg.Add(1)
			go func(channelName string, removal *channels.ChannelRemoval) {
				defer wg.Done()
				channelWrite := time.Now()
				b.addToChannelCache(channelName, change, removal != nil)
				addedTo = append(addedTo, channelName)
				if change.Skipped {
					b.addLateSequence(channelName, change)
				}
				changeCacheExpvars.Add("kv-cache-channelWrite", time.Since(channelWrite).Nanoseconds())
			}(channelName, removal)
		}
	}

	if EnableStarChannelLog {
		wg.Add(1)
		go func() {
			defer wg.Done()
			starWrite := time.Now()
			b.addToChannelCache(channels.UserStarChannel, change, false)
			changeCacheExpvars.Add("kv-cache-starChannel", time.Since(starWrite).Nanoseconds())
			addedTo = append(addedTo, channels.UserStarChannel)
		}()
	}

	wg.Wait()
	changeCacheExpvars.Add("allChannelTime", time.Since(allChannelTime).Nanoseconds())
	return base.SetFromArray(addedTo)
}

func (b *kvCache) getCacheHelper(channelName string) byteCacheHelper {
	return NewByteCacheHelper(channelName, b.bucket)
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
	b.updateCacheClock(channelName)
	base.LogTo("DCache", "    #%d ==> channel %q", change.Sequence, channelName)
}

func (b *kvCache) addSetToChannelCache(channelName string, entries []kvCacheEntry) {
	channelCacheHelper := b.getCacheHelper(channelName)
	channelCacheHelper.AddSet(entries)
}

// Add late sequence information to channel cache
func (b *kvCache) addLateSequence(channelName string, change *LogEntry) error {
	// TODO: no-op for now
	return nil
}

func (b *kvCache) updateCacheClock(channelName string) error {

	key := getCacheCountKey(channelName)

	//newValue := change.Sequence
	// increment cache clock if new sequence is higher than clock
	// TODO: review whether we should use
	/*
		byteValue, err := b.bucket.GetRaw(key)
		if err != nil {
			b.bucket.SetRaw(key, 0, uint64ToByte(newValue))
			return
		}
		value := byteToUint64(byteValue)
		if value < newValue {
			b.bucket.SetRaw(key, 0, uint64ToByte(newValue))
		}
	*/

	newValue, err := b.bucket.Incr(key, 1, 1, 0)
	if err != nil {
		base.Warn("Error from Incr in updateCacheClock(%s): %v", key, err)
		return err
	}
	base.LogTo("DCache", "Updated clock for %s (%s) to %d", channelName, key, newValue)

	return nil

}

func (b *kvCache) getStableSequence() (uint64, error) {

	var intValue uint64
	err := b.bucket.Get(kStableSequenceKey, &intValue)
	if err != nil {
		// return nil here - cache clock may not have been initialized yet
		return 0, nil
	}
	base.LogTo("StableSeq", "getStableSequence - returning %d", intValue)
	return intValue, nil
}

func (b *kvCache) setStableSequence(sequence uint64) error {

	err := b.bucket.Set(kStableSequenceKey, 0, sequence)
	if err != nil {
		base.Warn("Error Setting stable sequence in setStableSequence(%s): %v", sequence, err)
		return err
	}

	return nil
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
	b.bucket.SetRaw(key, 0, value)

}

// Notification handling
func (b *kvCache) enableNotifications(channelName string) {
	b.getChannelCache(channelName).startNotify()
}

// Reads a single doc from the cache for the entry.
func readCacheEntry(sequence uint64, bucket base.Bucket) (*LogEntry, error) {

	var cacheEntry distributedChannelCacheEntry

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
