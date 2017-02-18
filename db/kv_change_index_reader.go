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
	"expvar"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var indexReaderOneShotCount expvar.Int
var indexReaderPersistentCount expvar.Int
var indexReaderPollReadersCount = base.NewIntRollingMeanVar(100)
var indexReaderPollReadersTime = base.NewIntRollingMeanVar(100)
var indexReaderPollPrincipalsCount = base.NewIntRollingMeanVar(100)
var indexReaderPollPrincipalsTime = base.NewIntRollingMeanVar(100)
var indexReaderGetChangesTime = base.NewIntRollingMeanVar(100)
var indexReaderGetChangesCount expvar.Int
var indexReaderGetChangesUseCached expvar.Int
var indexReaderGetChangesUseIndexed expvar.Int

type kvChangeIndexReader struct {
	indexReadBucket           base.Bucket                // Index bucket
	readerStableSequence      *base.ShardedClock         // Initialized on first polling, updated on subsequent polls
	readerStableSequenceLock  sync.RWMutex               // Coordinates read access to channel index reader map
	channelIndexReaders       map[string]*KvChannelIndex // Manages read access to channel.  Map indexed by channel name.
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

func init() {
	base.StatsExpvars.Set("indexReader.numReaders.OneShot", &indexReaderOneShotCount)
	base.StatsExpvars.Set("indexReader.numReaders.Persistent", &indexReaderPersistentCount)
	base.StatsExpvars.Set("indexReader.pollReaders.Time", &indexReaderPollReadersTime)
	base.StatsExpvars.Set("indexReader.pollReaders.Count", &indexReaderPollReadersCount)
	base.StatsExpvars.Set("indexReader.pollPrincipals.Time", &indexReaderPollPrincipalsTime)
	base.StatsExpvars.Set("indexReader.pollPrincipals.Count", &indexReaderPollPrincipalsCount)
	base.StatsExpvars.Set("indexReader.getChanges.Time", &indexReaderGetChangesTime)
	base.StatsExpvars.Set("indexReader.getChanges.Count", &indexReaderGetChangesCount)
	base.StatsExpvars.Set("indexReader.getChanges.UseCached", &indexReaderGetChangesUseCached)
	base.StatsExpvars.Set("indexReader.getChanges.UseIndexed", &indexReaderGetChangesUseIndexed)
}

func (k *kvChangeIndexReader) Init(options *CacheOptions, indexOptions *ChangeIndexOptions, onChange func(base.Set), indexPartitionsCallback IndexPartitionsFunc, context *DatabaseContext) (err error) {

	k.channelIndexReaders = make(map[string]*KvChannelIndex)
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

	// Make sure that the index bucket and data bucket have correct sequence parity
	if err := k.verifyBucketSequenceParity(context); err != nil {
		base.Warn("Unable to verify bucket sequence index parity [%v]. "+
			"May indicate that Couchbase Server experienced a rollback,"+
			" which Sync Gateway will attempt to handle gracefully.", err)
	}

	// Start background task to poll for changes
	k.terminator = make(chan struct{})
	k.pollingActive = make(chan struct{})

	// Skip polling initialization if writer
	if indexOptions != nil && indexOptions.Writer {
		close(k.pollingActive)
		return nil
	}

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
				if k.stableSequenceChanged() {
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

// Make sure that the index bucket and data bucket have correct sequence parity
// https://github.com/couchbase/sync_gateway/issues/1133
func (k *kvChangeIndexReader) verifyBucketSequenceParity(context *DatabaseContext) error {

	// Verify that the index bucket stable sequence is equal to or later to the
	// data bucket stable sequence
	indexBucketStableClock, err := k.GetStableClock()
	if err != nil {
		return err
	}

	return base.VerifyBucketSequenceParity(indexBucketStableClock, context.Bucket)

}

func (k *kvChangeIndexReader) Clear() {
	k.channelIndexReaders = make(map[string]*KvChannelIndex)
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

	var prevTimingSeq uint64
	if base.TimingExpvarsEnabled {
		prevTimingSeq = k.readerStableSequence.GetSequence(base.KTimingExpvarVbNo)
	}

	isChanged, err := k.readerStableSequence.Load()

	if err != nil {
		base.Warn("Error loading reader stable sequence")
		return false
	}

	if base.TimingExpvarsEnabled && isChanged {
		base.TimingExpvars.UpdateBySequenceRange("StableSequence", base.KTimingExpvarVbNo, prevTimingSeq, k.readerStableSequence.GetSequence(base.KTimingExpvarVbNo))
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

	return k.GetChangesForRange(channelName, sinceClock, nil, options.Limit)
}

func (k *kvChangeIndexReader) GetChangesForRange(channelName string, sinceClock base.SequenceClock, toClock base.SequenceClock, limit int) ([]*LogEntry, error) {

	defer indexReaderGetChangesTime.AddSince(time.Now())
	defer indexReaderGetChangesCount.Add(1)

	reader, err := k.getOrCreateReader(channelName)
	if err != nil {
		base.Warn("Error obtaining channel reader (need partition index?) for channel %s", channelName)
		return nil, err
	}
	changes, err := reader.GetChanges(sinceClock, toClock, limit)
	if err != nil {
		base.LogTo("DIndex+", "No clock found for channel %s, assuming no entries in index", channelName)
		return nil, nil
	}

	return changes, nil
}

func (k *kvChangeIndexReader) getOrCreateReader(channelName string) (*KvChannelIndex, error) {

	var err error
	index := k.getChannelReader(channelName)
	if index == nil {
		index, err = k.newChannelReader(channelName)
		IndexExpvars.Add("getOrCreateReader_create", 1)
		base.LogTo("DIndex+", "getOrCreateReader: Created new reader for channel %s", channelName)
	} else {
		IndexExpvars.Add("getOrCreateReader_get", 1)
		base.LogTo("DIndex+", "getOrCreateReader: Using existing reader for channel %s", channelName)
	}
	return index, err
}

func (k *kvChangeIndexReader) getChannelReader(channelName string) *KvChannelIndex {

	k.channelIndexReaderLock.RLock()
	defer k.channelIndexReaderLock.RUnlock()
	return k.channelIndexReaders[channelName]
}

func (k *kvChangeIndexReader) newChannelReader(channelName string) (*KvChannelIndex, error) {

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
	indexReaderPersistentCount.Add(1)
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

	defer indexReaderPollReadersTime.AddSince(time.Now())
	defer indexReaderPollReadersCount.AddValue(int64(len(k.channelIndexReaders)))

	// Build the set of clock keys to retrieve.  Stable sequence, plus one per channel reader
	keySet := make([]string, len(k.channelIndexReaders))
	index := 0
	for _, reader := range k.channelIndexReaders {
		keySet[index] = GetChannelClockKey(reader.channelName)
		index++
	}
	bulkGetResults, err := k.indexReadBucket.GetBulkRaw(keySet)

	if err != nil {
		base.Warn("Error retrieving channel clocks: %v", err)
	}
	IndexExpvars.Add("bulkGet_channelClocks", 1)
	IndexExpvars.Add("bulkGet_channelClocks_keyCount", int64(len(keySet)))
	changedChannels := make(chan string, len(k.channelIndexReaders))
	cancelledChannels := make(chan string, len(k.channelIndexReaders))

	var wg sync.WaitGroup
	for _, reader := range k.channelIndexReaders {
		// For each channel, unmarshal new channel clock, then check with reader whether this represents changes
		wg.Add(1)
		go func(reader *KvChannelIndex, wg *sync.WaitGroup) {
			defer func() {
				wg.Done()
			}()
			// Unmarshal channel clock.  If not present in the bulk get results, use empty clock to support
			// channels that don't have any indexed data yet.  If clock was previously found successfully (i.e. empty clock is
			// due to temporary error from server), empty clock treated safely as a non-update by pollForChanges.
			clockKey := GetChannelClockKey(reader.channelName)
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
		indexReaderPersistentCount.Add(-1)
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

	if len(k.activePrincipalCounts) == 0 {
		return
	}

	defer indexReaderPollPrincipalsTime.AddSince(time.Now())
	defer indexReaderPollPrincipalsCount.AddValue(int64(len(k.activePrincipalCounts)))

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

	// There's been a change - check if our active principals have changed
	var changedWaitKeys []string

	// When using a gocb bucket, use a single bulk operation to retrieve counters.
	if gocbIndexBucket, ok := k.indexReadBucket.(base.CouchbaseBucketGoCB); ok {
		principalKeySet := make([]string, len(k.activePrincipalCounts))
		i := 0
		for principalID, _ := range k.activePrincipalCounts {
			key := fmt.Sprintf(base.KPrincipalCountKeyFormat, principalID)
			principalKeySet[i] = key
			i++
		}

		bulkGetResults, err := gocbIndexBucket.GetBulkCounters(principalKeySet)
		if err != nil {
			base.Warn("Error during GetBulkRaw while polling principals: %v", err)
		}

		for principalID, currentCount := range k.activePrincipalCounts {
			key := fmt.Sprintf(base.KPrincipalCountKeyFormat, principalID)
			newCount, ok := bulkGetResults[key]
			if !ok {
				base.Warn("Expected key not found in results when checking for principal updates, key:[%s]", key)
				continue
			}
			if newCount != currentCount {
				k.activePrincipalCounts[principalID] = newCount
				waitKey := strings.TrimPrefix(key, base.KPrincipalCountKeyPrefix)
				changedWaitKeys = append(changedWaitKeys, waitKey)
			}
		}
	} else {
		// TODO: Add bulk counter retrieval support to walrus, go-couchbase.  Until then, doing sequential gets
		// There's been a change - check whether any of our active principals have changed
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
