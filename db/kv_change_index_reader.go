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
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type kvChangeIndexReader struct {
	indexReadBucket           base.Bucket                // Index bucket
	readerStableSequence      *base.ShardedClock         // Initialized on first polling, updated on subsequent polls
	readerStableSequenceLock  sync.RWMutex               // Coordinates read access to channel index reader map
	channelIndexReaders       map[string]*kvChannelIndex // Manages read access to channel.  Map indexed by channel name.
	channelIndexReaderLock    sync.RWMutex               // Coordinates read access to channel index reader map
	onChange                  func(base.Set)             // Client callback that notifies of channel changes
	terminator                chan struct{}              // Ends polling	indexPartitions *base.IndexPartitions
	pollingActive             chan struct{}              // Detects polling closed
	maxVbNo                   uint16                     // Number of vbuckets
	indexPartitionsCallback   IndexPartitionsFunc        // callback to retrieve the index partition map
	overallPrincipalCount     uint64                     // Counter for all principals
	activePrincipalCounts     map[string]uint64          // Counters for principals with active changes feeds
	activePrincipalCountsLock sync.RWMutex               // Coordinates access to active principals map

}

func (k *kvChangeIndexReader) Init(options *CacheOptions, indexOptions *ChangeIndexOptions, onChange func(base.Set), indexPartitionsCallback IndexPartitionsFunc) (err error) {

	k.channelIndexReaders = make(map[string]*kvChannelIndex)
	k.indexPartitionsCallback = indexPartitionsCallback
	k.activePrincipalCounts = make(map[string]uint64)

	// Initialize notification Callback
	k.onChange = onChange

	k.indexReadBucket, err = base.GetBucket(indexOptions.Spec, nil)
	if err != nil {
		base.Logf("Error opening index bucket %q, pool %q, server <%s>",
			indexOptions.Spec.BucketName, indexOptions.Spec.PoolName, indexOptions.Spec.Server)
		// TODO: revert to local index?
		return err
	}

	cbBucket, ok := k.indexReadBucket.(base.CouchbaseBucket)
	if ok {
		k.maxVbNo, _ = cbBucket.GetMaxVbno()
	} else {
		// walrus, for unit testing
		k.maxVbNo = 1024
	}

	// Start background task to poll for changes
	k.terminator = make(chan struct{})
	k.pollingActive = make(chan struct{})
	go func(k *kvChangeIndexReader) {
		defer close(k.pollingActive)
		pollStart := time.Now()
		for {
			timeSinceLastPoll := time.Since(pollStart)
			waitTime := (kPollFrequency * time.Millisecond) - timeSinceLastPoll
			if waitTime < 0 {
				waitTime = 0 * time.Millisecond
			}
			select {
			case <-k.terminator:
				return
			case <-time.After(waitTime):
				// TODO: Doesn't trigger the reader removal processing (in pollReaders) during long
				//       periods without changes to stableSequence.  In that scenario we'll continue
				//       stable sequence polling each poll interval, even if we *actually* don't have any
				//       active readers.
				pollStart = time.Now()
				if k.hasActiveReaders() && k.stableSequenceChanged() {
					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						k.pollReaders()
					}()
					go func() {
						defer wg.Done()
						k.pollPrincipals()
					}()
					wg.Wait()
				}
			}

		}
	}(k)

	return nil
}

func (k *kvChangeIndexReader) Clear() {
	k.channelIndexReaders = make(map[string]*kvChannelIndex)
}

func (k *kvChangeIndexReader) Stop() {
	close(k.terminator)
	// Closing the terminator tells polling loop to stop, but may not be immediate.  Wait for polling to actually stop before closing the bucket
	<-k.pollingActive
	if k.indexReadBucket != nil {
		k.indexReadBucket.Close()
	}
}

// Returns the stable sequence for a document's vbucket from the stable clock.
// Used during document write for handling deduplicated sequences on the DCP feed,
// to determine when recent sequences can be pruned.
// Uses reader's stable sequence to avoid db lookups, since document write doesn't
// need the absolute latest version for pruning.
func (k *kvChangeIndexReader) GetStableSequence(docID string) (seq SequenceID) {

	k.readerStableSequenceLock.Lock()
	defer k.readerStableSequenceLock.Unlock()
	// TODO: needs to be db bucket, since gocbbucket doesn't implement VBHash.  Currently not used.
	vbNo := k.indexReadBucket.VBHash(docID)
	if k.readerStableSequence == nil {
		var err error
		k.readerStableSequence, err = k.loadStableSequence()
		if err != nil {
			base.Warn("Error initializing reader stable sequence")
			return SequenceID{}
		}
	}

	return SequenceID{Seq: k.readerStableSequence.AsClock().GetSequence(uint16(vbNo))}
}

// Loads the full current stable sequence from the index bucket
func (k *kvChangeIndexReader) loadStableSequence() (*base.ShardedClock, error) {
	partitions, err := k.indexPartitionsCallback()
	if err != nil {
		return nil, err
	}
	stableSequence := base.NewShardedClockWithPartitions(base.KStableSequenceKey, partitions, k.indexReadBucket)
	_, err = stableSequence.Load()
	return stableSequence, err
}

func (k *kvChangeIndexReader) stableSequenceChanged() bool {

	k.readerStableSequenceLock.Lock()
	defer k.readerStableSequenceLock.Unlock()
	if k.readerStableSequence == nil {
		var err error
		k.readerStableSequence, err = k.loadStableSequence()
		if err != nil {
			base.Warn("Error initializing reader stable sequence:%v", err)
			return false
		}
		return true
	}

	isChanged, err := k.readerStableSequence.Load()

	if err != nil {
		base.Warn("Error loading reader stable sequence")
		return false
	}

	return isChanged
}

func (k *kvChangeIndexReader) getReaderStableSequence() base.SequenceClock {
	k.readerStableSequenceLock.RLock()
	defer k.readerStableSequenceLock.RUnlock()
	if k.readerStableSequence != nil {
		return k.readerStableSequence.AsClock()
	} else {
		return nil
	}
}

func (k *kvChangeIndexReader) GetStableClock() (clock base.SequenceClock, err error) {

	// Validation partition map is available.
	_, err = k.indexPartitionsCallback()
	if err != nil {
		// Unable to load partitions.  Check whether the index has data (stable counter is non-zero)
		count, err := base.LoadClockCounter(base.KStableSequenceKey, k.indexReadBucket)
		// Index has data, but we can't get partition map.  Return error
		if err == nil && count > 0 {
			return nil, errors.New("Error: Unable to retrieve index partition map, but index counter exists")
		} else {
			// Index doesn't have data.  Return zero clock as stable clock
			return base.NewSequenceClockImpl(), nil
		}
	}

	clock = base.NewSequenceClockImpl()
	stableShardedClock, err := k.loadStableSequence()
	if err != nil {
		base.Warn("Stable sequence and clock not found in index - returning err")
		return nil, err
	} else {
		clock = stableShardedClock.AsClock()
	}

	return clock, nil
}

func (k *kvChangeIndexReader) SetNotifier(onChange func(base.Set)) {
	k.onChange = onChange
}

func (k *kvChangeIndexReader) GetChanges(channelName string, options ChangesOptions) ([]*LogEntry, error) {

	var sinceClock base.SequenceClock
	if options.Since.Clock == nil {
		// If there's no since clock, we may be in backfill for another channel - revert to the triggered by clock.
		if options.Since.TriggeredByClock != nil {
			sinceClock = options.Since.TriggeredByClock
		} else {
			sinceClock = base.NewSequenceClockImpl()
		}
	} else {
		sinceClock = options.Since.Clock
	}

	reader, err := k.getOrCreateReader(channelName, options)
	if err != nil {
		base.Warn("Error obtaining channel reader (need partition index?) for channel %s", channelName)
		return nil, err
	}
	changes, err := reader.getChanges(sinceClock)
	if err != nil {
		base.LogTo("DIndex+", "No clock found for channel %d, assuming no entries in index", channelName)
		return nil, nil
	}

	// Limit handling
	if options.Limit > 0 && len(changes) > options.Limit {
		limitResult := make([]*LogEntry, options.Limit)
		copy(limitResult[0:], changes[0:])
		return limitResult, nil
	}

	return changes, nil
}

func (k *kvChangeIndexReader) getOrCreateReader(channelName string, options ChangesOptions) (*kvChannelIndex, error) {

	// For continuous or longpoll processing, use the shared reader from the channelindexReaders map to coordinate
	// polling.
	if options.Wait {
		var err error
		index := k.getChannelReader(channelName)
		if index == nil {
			index, err = k.newChannelReader(channelName)
			indexExpvars.Add("getOrCreateReader_create", 1)
			base.LogTo("DIndex+", "getOrCreateReader: Created new reader for channel %s", channelName)
		} else {
			indexExpvars.Add("getOrCreateReader_get", 1)
			base.LogTo("DIndex+", "getOrCreateReader: Using existing reader for channel %s", channelName)
		}
		return index, err
	} else {
		// For non-continuous/non-longpoll, use a one-off reader, no onChange handling.
		indexPartitions, err := k.indexPartitionsCallback()
		if err != nil {
			return nil, err
		}
		return NewKvChannelIndex(channelName, k.indexReadBucket, indexPartitions, nil), nil

	}
}

func (k *kvChangeIndexReader) getChannelReader(channelName string) *kvChannelIndex {

	k.channelIndexReaderLock.RLock()
	defer k.channelIndexReaderLock.RUnlock()
	return k.channelIndexReaders[channelName]
}

func (k *kvChangeIndexReader) newChannelReader(channelName string) (*kvChannelIndex, error) {

	k.channelIndexReaderLock.Lock()
	defer k.channelIndexReaderLock.Unlock()
	// make sure someone else hasn't created while we waited for the lock
	if _, ok := k.channelIndexReaders[channelName]; ok {
		return k.channelIndexReaders[channelName], nil
	}
	indexPartitions, err := k.indexPartitionsCallback()
	if err != nil {
		return nil, err
	}
	k.channelIndexReaders[channelName] = NewKvChannelIndex(channelName, k.indexReadBucket, indexPartitions, k.onChange)
	k.channelIndexReaders[channelName].setType("reader")
	indexExpvars.Add("pollingChannels_active", 1)
	return k.channelIndexReaders[channelName], nil
}

// TODO: If mutex read lock is too much overhead every time we poll, could manage numReaders using
// atomic uint64
func (k *kvChangeIndexReader) hasActiveReaders() bool {
	k.channelIndexReaderLock.RLock()
	defer k.channelIndexReaderLock.RUnlock()
	return len(k.channelIndexReaders) > 0
}

func (k *kvChangeIndexReader) pollReaders() bool {
	k.channelIndexReaderLock.Lock()
	defer k.channelIndexReaderLock.Unlock()

	if len(k.channelIndexReaders) == 0 {
		return true
	}

	// Build the set of clock keys to retrieve.  Stable sequence, plus one per channel reader
	keySet := make([]string, len(k.channelIndexReaders))
	index := 0
	for _, reader := range k.channelIndexReaders {
		keySet[index] = getChannelClockKey(reader.channelName)
		index++
	}
	bulkGetResults, err := k.indexReadBucket.GetBulkRaw(keySet)

	if err != nil {
		base.Warn("Error retrieving channel clocks: %v", err)
	}
	indexExpvars.Add("bulkGet_channelClocks", 1)
	indexExpvars.Add("bulkGet_channelClocks_keyCount", int64(len(keySet)))
	changedChannels := make(chan string, len(k.channelIndexReaders))
	cancelledChannels := make(chan string, len(k.channelIndexReaders))

	var wg sync.WaitGroup
	for _, reader := range k.channelIndexReaders {
		// For each channel, unmarshal new channel clock, then check with reader whether this represents changes
		wg.Add(1)
		go func(reader *kvChannelIndex, wg *sync.WaitGroup) {
			defer func() {
				wg.Done()
			}()
			// Unmarshal channel clock.  If not present in the bulk get results, use empty clock to support
			// channels that don't have any indexed data yet.  If clock was previously found successfully (i.e. empty clock is
			// due to temporary error from server), empty clock treated safely as a non-update by pollForChanges.
			clockKey := getChannelClockKey(reader.channelName)
			var newChannelClock *base.SequenceClockImpl
			clockBytes, found := bulkGetResults[clockKey]
			if !found {
				newChannelClock = base.NewSequenceClockImpl()
			} else {
				var err error
				newChannelClock, err = base.NewSequenceClockForBytes(clockBytes)
				if err != nil {
					base.Warn("Error unmarshalling channel clock - skipping polling for channel %s: %v", reader.channelName, err)
					return
				}
			}

			// Poll for changes
			hasChanges, cancelPolling := reader.pollForChanges(k.readerStableSequence.AsClock(), newChannelClock)
			if hasChanges {
				changedChannels <- reader.channelName
			}
			if cancelPolling {
				cancelledChannels <- reader.channelName
			}

		}(reader, &wg)
	}

	wg.Wait()
	close(changedChannels)
	close(cancelledChannels)

	// Build channel set from the changed channels
	var channels []string
	for channelName := range changedChannels {
		channels = append(channels, channelName)
	}

	if len(channels) > 0 && k.onChange != nil {
		k.onChange(base.SetFromArray(channels))
	}

	// Remove cancelled channels from channel readers
	for channelName := range cancelledChannels {
		indexExpvars.Add("pollingChannels_active", -1)
		delete(k.channelIndexReaders, channelName)
	}

	return true
}

// PollPrincipals checks the principal counts, stored in the index, to determine whether there's been
// a change to a user or role that should trigger a notification for that principal.
func (k *kvChangeIndexReader) pollPrincipals() {

	// Principal polling is strictly for notification handling, so skip if no notify function is defined
	if k.onChange == nil {
		return
	}

	k.activePrincipalCountsLock.Lock()
	defer k.activePrincipalCountsLock.Unlock()

	// Check whether ANY principals have been updated since last poll, before doing the work of retrieving individual keys
	overallCount, err := k.indexReadBucket.Incr(base.KTotalPrincipalCountKey, 0, 0, 0)
	if err != nil {
		base.Warn("Principal polling encountered error getting overall count:%v", err)
		return
	}
	if overallCount == k.overallPrincipalCount {
		return
	}
	k.overallPrincipalCount = overallCount

	// There's been a change - check whether any of our active principals have changed
	var changedWaitKeys []string
	for principalID, currentCount := range k.activePrincipalCounts {
		key := fmt.Sprintf(base.KPrincipalCountKeyFormat, principalID)
		newCount, err := k.indexReadBucket.Incr(key, 0, 0, 0)
		if err != nil {
			base.Warn("Principal polling encountered error getting overall count for key %s:%v", key, err)
			continue
		}
		if newCount != currentCount {
			k.activePrincipalCounts[principalID] = newCount
			waitKey := strings.TrimPrefix(key, base.KPrincipalCountKeyPrefix)
			changedWaitKeys = append(changedWaitKeys, waitKey)
		}
	}
	if len(changedWaitKeys) > 0 {
		k.onChange(base.SetFromArray(changedWaitKeys))
	}
}

// AddActivePrincipal - adds one or more principal keys to the set being polled.
// Key format is the same used to store the principal in the data bucket.
func (k *kvChangeIndexReader) addActivePrincipals(keys []string) {

	k.activePrincipalCountsLock.Lock()
	defer k.activePrincipalCountsLock.Unlock()
	for _, key := range keys {
		_, ok := k.activePrincipalCounts[key]
		if !ok {
			// Get the count
			countKey := fmt.Sprintf(base.KPrincipalCountKeyFormat, key)
			currentCount, err := k.indexReadBucket.Incr(countKey, 0, 0, 0)
			if err != nil {
				currentCount = 0
			}
			k.activePrincipalCounts[key] = currentCount
		}
	}
}
