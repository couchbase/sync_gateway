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
	"sync"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

type localCache struct {
	context         *DatabaseContext
	initialSequence uint64
	channelCaches   map[string]*channelCache // A cache of changes for each channel
	lateSeqLock     sync.RWMutex             // Coordinates access to late sequence caches
	lock            sync.RWMutex             // Coordinates access to struct fields
}

func (lc *localCache) Init(initialSequence uint64) {

	lc.initialSequence = initialSequence
	lc.channelCaches = make(map[string]*channelCache, 10)
}

// Adds an entry to the appropriate channels' caches, returning the affected channels.  lateSequence
// flag indicates whether it was a change arriving out of sequence
func (lc *localCache) AddToCache(change *LogEntry) base.Set {

	addedTo := make([]string, 0, 4)
	ch := change.Channels
	change.Channels = nil // not needed anymore, so free some memory

	// If it's a late sequence, we want to add to all channel late queues within a single write lock,
	// to avoid a changes feed seeing the same late sequence in different iteration loops (and sending
	// twice)
	func() {
		if change.Skipped {
			lc.lateSeqLock.Lock()
			defer lc.lateSeqLock.Unlock()
		}

		for channelName, removal := range ch {
			if removal == nil || removal.Seq == change.Sequence {
				channelCache := lc._getChannelCache(channelName)
				channelCache.addToCache(change, removal != nil)
				addedTo = append(addedTo, channelName)
				if change.Skipped {
					channelCache.AddLateSequence(change)
				}
			}
		}

	}()

	if EnableStarChannelLog {
		lc._getChannelCache(channels.UserStarChannel).addToCache(change, false)
		addedTo = append(addedTo, channels.UserStarChannel)
	}

	return base.SetFromArray(addedTo)
}

func (lc *localCache) Clear(initialSequence uint64) {
	lc.channelCaches = make(map[string]*channelCache, 10)
	lc.initialSequence = initialSequence

}

func (lc *localCache) SetNotifier(onChange func(base.Set)) {
	// no-op, local cache does notification on the TAP read side.
}

func (lc *localCache) getChannelCache(channelName string) *channelCache {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	return lc._getChannelCache(channelName)
}

func (lc *localCache) _getChannelCache(channelName string) *channelCache {
	cache := lc.channelCaches[channelName]
	if cache == nil {
		cache = newChannelCache(lc.context, channelName, lc.initialSequence+1)
		lc.channelCaches[channelName] = cache
	}
	return cache
}

func (lc *localCache) GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error) {
	return lc.getChannelCache(channelName).GetChanges(options)
}

func (lc *localCache) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {
	return lc.getChannelCache(channelName).getCachedChanges(options)
}

func (lc *localCache) InitLateSequenceClient(channelName string) uint64 {
	return lc.getChannelCache(channelName).InitLateSequenceClient()
}

func (lc *localCache) GetLateSequencesSince(channelName string, sequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {
	return lc.getChannelCache(channelName).GetLateSequencesSince(sequence)
}

func (lc *localCache) ReleaseLateSequenceClient(channelName string, sequence uint64) error {
	return lc.getChannelCache(channelName).ReleaseLateSequenceClient(sequence)
}

func (lc *localCache) Prune() {

	// Remove old cache entries:
	for channelName, _ := range lc.channelCaches {
		lc._getChannelCache(channelName).pruneCache()
	}
}

type distributedCache struct {
}
