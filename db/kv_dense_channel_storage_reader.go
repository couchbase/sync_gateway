//  Copyright (c) 2016 Couchbase, Inc.
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
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// Implementation of ChannelStorage that stores entries as an append-based list of
// full log entries
type DenseStorageReader struct {
	indexBucket          base.Bucket                             // Index bucket
	channelName          string                                  // Channel name
	partitions           *base.IndexPartitions                   // Partition assignment map
	partitionStorage     map[uint16]*DensePartitionStorageReader // PartitionStorage for this channel
	partitionStorageLock sync.RWMutex                            // Coordinates read access to partition storage map
}

func NewDenseStorageReader(bucket base.Bucket, channelName string, partitions *base.IndexPartitions) *DenseStorageReader {

	storage := &DenseStorageReader{
		indexBucket:      bucket,
		channelName:      channelName,
		partitions:       partitions,
		partitionStorage: make(map[uint16]*DensePartitionStorageReader, partitions.PartitionCount()),
	}
	return storage
}

// Number of blocks to store in channel cache, per partition
const kCachedBlocksPerShard = 2

func (ds *DenseStorageReader) UpdateCache(sinceClock base.SequenceClock, toClock base.SequenceClock, changedPartitions []*base.PartitionRange) error {

	var wg sync.WaitGroup
	errCh := make(chan error, len(changedPartitions))
	for partitionNo, partitionRange := range changedPartitions {
		if partitionRange == nil {
			continue
		}
		wg.Add(1)
		go func(partitionNo uint16, partitionRange *base.PartitionRange) {
			defer wg.Done()
			reader := ds.getPartitionStorageReader(partitionNo)
			if reader == nil {
				base.Warn("Expected to get reader for channel %s partition %d, based on changed range %v", ds.channelName, partitionNo, partitionRange)
				return
			}
			err := reader.UpdateCache(kCachedBlocksPerShard)
			if err != nil {
				base.Warn("Unable to update cache for channel:[%s] partition:[%d] : %v", ds.channelName, partitionNo, err)
				errCh <- err
			}
		}(uint16(partitionNo), partitionRange)
	}
	wg.Wait()
	if len(errCh) > 0 {
		return <-errCh
	}
	return nil

}

// Returns all changes for the channel with sequences greater than sinceClock, and less than or equal to
// toClock.  Changes need to be ordered by vbNo in order to support interleaving of results from multiple channels by
// caller.  Changes are retrieved in vbucket order, to allow a limit check after each vbucket (to avoid retrieval).  Since
// a given partition includes results for more than one vbucket

func (ds *DenseStorageReader) GetChanges(sinceClock base.SequenceClock, toClock base.SequenceClock, limit int) (changes []*LogEntry, err error) {

	changes = make([]*LogEntry, 0)

	// Identify what's changed:
	//  changedVbuckets: ordered list of vbuckets that have changes, based on the clock comparison
	//  partitionRanges: array of PartitionRange, indexed by partitionNo

	changedVbuckets, partitionRanges := ds.calculateChanged(sinceClock, toClock)

	// changed partitions is a cache of changes for a partition, for reuse by multiple vbs
	changedPartitions := make(map[uint16]*PartitionChanges, len(partitionRanges))

	for _, vbNo := range changedVbuckets {
		partitionNo := ds.partitions.PartitionForVb(vbNo)
		partitionChanges, ok := changedPartitions[partitionNo]
		if !ok {
			partitionChanges, err = ds.getPartitionStorageReader(partitionNo).GetChanges(*partitionRanges[partitionNo])
			if err != nil {
				return changes, err
			}
			changedPartitions[partitionNo] = partitionChanges
		}
		changes = append(changes, partitionChanges.GetVbChanges(vbNo)...)
		if limit > 0 && len(changes) > limit {
			break
		}
	}

	return changes, nil
}

// Returns PartitionStorageReader for this channel storage reader.  Initializes if needed.
func (ds *DenseStorageReader) getPartitionStorageReader(partitionNo uint16) (partitionStorage *DensePartitionStorageReader) {

	partitionStorage = ds._getPartitionStorageReader(partitionNo)
	if partitionStorage == nil {
		partitionStorage = ds._newPartitionStorageReader(partitionNo)
	}
	return partitionStorage

}

func (ds *DenseStorageReader) _getPartitionStorageReader(partitionNo uint16) (partitionStorage *DensePartitionStorageReader) {

	ds.partitionStorageLock.RLock()
	defer ds.partitionStorageLock.RUnlock()
	return ds.partitionStorage[partitionNo]
}

func (ds *DenseStorageReader) _newPartitionStorageReader(partitionNo uint16) (partitionStorage *DensePartitionStorageReader) {

	ds.partitionStorageLock.Lock()
	defer ds.partitionStorageLock.Unlock()
	// make sure someone else hasn't created while we waited for the lock
	if _, ok := ds.partitionStorage[partitionNo]; ok {
		return ds.partitionStorage[partitionNo]
	}
	ds.partitionStorage[partitionNo] = NewDensePartitionStorageReader(ds.channelName, partitionNo, ds.indexBucket)
	return ds.partitionStorage[partitionNo]
}

// calculateChangedPartitions identifies which vbuckets have changed after sinceClock until toClock, groups these
// by partition, and returns as a array of PartitionRanges, indexed by partition number.
func (ds *DenseStorageReader) calculateChanged(sinceClock, toClock base.SequenceClock) (changedVbs []uint16, changedPartitions []*base.PartitionRange) {

	changedVbs = make([]uint16, 0)
	changedPartitions = make([]*base.PartitionRange, ds.partitions.PartitionCount())
	for vbNoInt, toSeq := range toClock.Value() {
		vbNo := uint16(vbNoInt)
		sinceSeq := sinceClock.GetSequence(vbNo)
		if sinceSeq < toSeq {
			changedVbs = append(changedVbs, vbNo)
			partitionNo := ds.partitions.PartitionForVb(vbNo)
			if changedPartitions[partitionNo] == nil {
				partitionRange := base.NewPartitionRange()
				changedPartitions[partitionNo] = &partitionRange
			}
			changedPartitions[partitionNo].SetRange(vbNo, sinceSeq, toSeq)
		}
	}
	return changedVbs, changedPartitions
}

type PartitionChanges struct {
	changes map[uint16][]*LogEntry
}

func NewPartitionChanges() *PartitionChanges {
	return &PartitionChanges{
		changes: make(map[uint16][]*LogEntry),
	}
}
func (p *PartitionChanges) GetVbChanges(vbNo uint16) []*LogEntry {
	if vbchanges, ok := p.changes[vbNo]; ok {
		return vbchanges
	} else {
		return nil
	}
}

func (p *PartitionChanges) AddEntry(entry *LogEntry) {
	_, ok := p.changes[entry.VbNo]
	if !ok {
		p.changes[entry.VbNo] = make([]*LogEntry, 0)
	}
	p.changes[entry.VbNo] = append(p.changes[entry.VbNo], entry)
}

func (p *PartitionChanges) Count() int {
	count := 0
	for _, vbSet := range p.changes {
		count += len(vbSet)
	}
	return count
}

func (p *PartitionChanges) PrependChanges(other *PartitionChanges) {
	for vbNo, otherChanges := range other.changes {
		_, ok := p.changes[vbNo]
		if !ok {
			p.changes[vbNo] = make([]*LogEntry, 0)
		}
		p.changes[vbNo] = append(otherChanges, p.changes[vbNo]...)
	}
}

// DensePartitionStorageReaderNonCaching is a non-caching reader - every read request retrieves the latest from the bucket.
type DensePartitionStorageReaderNonCaching struct {
	channelName string      // Channel name
	partitionNo uint16      // Partition number
	indexBucket base.Bucket // Index bucket
}

func NewDensePartitionStorageReaderNonCaching(channelName string, partitionNo uint16, indexBucket base.Bucket) *DensePartitionStorageReaderNonCaching {
	storage := &DensePartitionStorageReaderNonCaching{
		channelName: channelName,
		partitionNo: partitionNo,
		indexBucket: indexBucket,
	}
	return storage
}

func (r *DensePartitionStorageReaderNonCaching) GetChanges(partitionRange base.PartitionRange) (*PartitionChanges, error) {

	changes := NewPartitionChanges()

	// Initialize the block list to the starting range, then find the starting block for the partition range
	blockList := r.GetBlockListForRange(partitionRange)
	if blockList == nil {
		base.LogTo("ChannelStorage+", "No block found for requested partition range.  channel:[%s] partition:[%d]", r.channelName, r.partitionNo)
		return changes, nil
	}
	startIndex := 0
	for startIndex < len(blockList.blocks) {
		if partitionRange.SinceAfter(blockList.blocks[startIndex].StartClock) {
			startIndex++
		} else {
			// We've gone too far - want to start with the previous block
			break
		}
	}
	startIndex--

	// Iterate over the blocks, collecting entries by vbucket
	for i := startIndex; i <= len(blockList.blocks)-1; i++ {
		blockIter := NewDenseBlockIterator(blockList.LoadBlock(blockList.blocks[i]))
		for {
			blockEntry := blockIter.next()
			if blockEntry == nil {
				break
			}
			switch compare := partitionRange.Compare(blockEntry.getVbNo(), blockEntry.getSequence()); compare {
			case base.PartitionRangeAfter:
				break // We've exceeded the range - return
			case base.PartitionRangeWithin:
				changes.AddEntry(blockEntry.MakeLogEntry())
			}
		}
	}
	return changes, nil
}

func (r *DensePartitionStorageReaderNonCaching) GetBlockListForRange(partitionRange base.PartitionRange) *DenseBlockList {

	// Initialize the block list, by loading all block list docs until we get one with
	// a starting clock earlier than the partitionRange start.
	blockList := NewDenseBlockListReader(r.channelName, r.partitionNo, r.indexBucket)
	if blockList == nil {
		return nil
	}
	validFromClock := blockList.ValidFrom()
	if partitionRange.SinceBefore(validFromClock) {
		err := blockList.LoadPrevious()
		if err != nil {
			base.Warn("Error loading previous block list - will not be included in set. channel:[%s] partition:[%d]", r.channelName, r.partitionNo)
		}
		validFromClock = blockList.ValidFrom()
	}
	return blockList
}

func (l *DenseBlockList) LoadBlock(listEntry DenseBlockListEntry) *DenseBlock {
	block := NewDenseBlock(l.generateBlockKey(listEntry.BlockIndex), listEntry.StartClock)
	err := block.loadBlock(l.indexBucket)
	if err != nil {
		// error loading block - leave as new block, and let any conflicts get resolved on next write
	}
	return block
}

type DensePartitionStorageReader struct {
	channelName          string                 // Channel name
	partitionNo          uint16                 // Partition number
	indexBucket          base.Bucket            // Index bucket
	blockList            *DenseBlockList        // Cached block list
	blockCache           map[string]*DenseBlock // Cached blocks
	activeCachedBlockKey string                 // Latest block cached
	lock                 sync.RWMutex           // Storage reader lock
	validFrom            base.PartitionClock    // Reader cache is valid from this clock
	pendingReload        map[string]bool        // Set of blocks to be reloaded in next poll
	pendingReloadLock    sync.Mutex             // Allow holders of lock.RLock to update pendingReload
}

func NewDensePartitionStorageReader(channelName string, partitionNo uint16, indexBucket base.Bucket) *DensePartitionStorageReader {
	storage := &DensePartitionStorageReader{
		channelName:   channelName,
		partitionNo:   partitionNo,
		indexBucket:   indexBucket,
		blockCache:    make(map[string]*DenseBlock),
		pendingReload: make(map[string]bool),
	}
	return storage
}

// GetChanges attempts to return results from the cached changes.  If the cache doesn't satisfy the specified range,
// retrieves from the index.  Note: currently no writeback of indexed retrieval into the cache - cache is only updated
// during UpdateCache()
func (pr *DensePartitionStorageReader) GetChanges(partitionRange base.PartitionRange) (*PartitionChanges, error) {

	changes, cacheOk, err := pr.getCachedChanges(partitionRange)
	if err != nil {
		return nil, err
	}
	if cacheOk {
		base.LogTo("Cache+", "Returning cached changes for channel:[%s], partition:[%d]", pr.channelName, pr.partitionNo)
		indexReaderGetChangesUseCached.Add(1)
		return changes, nil
	}

	// Cache didn't cover the partition range - retrieve from the index
	base.LogTo("Cache+", "Returning indexed changes for channel:[%s], partition:[%d]", pr.channelName, pr.partitionNo)
	indexReaderGetChangesUseIndexed.Add(1)
	return pr.getIndexedChanges(partitionRange)

}

func (pr *DensePartitionStorageReader) UpdateCache(numBlocks int) error {
	pr.lock.Lock()
	defer pr.lock.Unlock()

	// Reload the block list if changed
	if pr.blockList == nil {
		pr.blockList = NewDenseBlockListReader(pr.channelName, pr.partitionNo, pr.indexBucket)
		if pr.blockList == nil {
			return errors.New("Unable to initialize block list")
		}
	} else {
		pr.blockList.loadDenseBlockList()
		for _, block := range pr.blockList.blocks {
			base.LogTo("Cache+", "block valid from: %v", block.StartClock)
		}
	}

	// Block lists can span multiple documents.  If pr.blockList include numBlocks,
	// of the requested partitionRange, attempt to load previous block list doc(s).
	blockCount := len(pr.blockList.blocks)
	for blockCount < numBlocks {
		err := pr.blockList.LoadPrevious()
		if err != nil {
			// No previous blocks - we're done
			break
		}
		blockCount = len(pr.blockList.blocks)
	}

	if blockCount == 0 {
		base.Warn("Attempted to update reader cache for partition with no blocks. channel:[%s] partition:[%d]", pr.channelName, pr.partitionNo)
		return errors.New("No blocks found when updating partition cache")
	}
	// cacheKeySet tracks the blocks that should be in the cache - used for cache expiry, below
	cacheKeySet := make(map[string]bool)

	// Reload the active block first
	blockListEntry := pr.blockList.ActiveListEntry()
	base.LogTo("Cache+", "Reloading active block: %s", blockListEntry.Key(pr.blockList))
	activeBlockKey := blockListEntry.Key(pr.blockList)
	_, err := pr._loadAndCacheBlock(blockListEntry)
	cacheKeySet[blockListEntry.Key(pr.blockList)] = true
	if err != nil {
		return err
	}
	pr.validFrom = blockListEntry.StartClock

	// Update cache with older blocks, up to numBlocks, when present.
	blocksCached := 1
	for blocksCached < numBlocks {
		blockListEntry, err = pr.blockList.PreviousBlock(blockListEntry.BlockIndex)
		if blockListEntry == nil {
			// We've hit the end of the block list before reaching num blocks - we're done.
			break
		}
		blockKey := blockListEntry.Key(pr.blockList)

		_, ok := pr.blockCache[blockKey]
		if ok {
			// If it's already in the cache, reload if it was the previous active block, or it's been flagged for reload
			if blockKey == pr.activeCachedBlockKey || pr.pendingReload[blockKey] {
				base.LogTo("Cache+", "Reloading older block: %s", blockListEntry.Key(pr.blockList))
				pr._loadAndCacheBlock(blockListEntry)
				delete(pr.pendingReload, blockKey)
			}
		} else {
			// Not in the cache - add it
			base.LogTo("Cache+", "Adding older block to the cache: %s", blockListEntry.Key(pr.blockList))
			pr._loadAndCacheBlock(blockListEntry)
		}
		cacheKeySet[blockKey] = true
		pr.validFrom = blockListEntry.StartClock
		blocksCached++
	}

	// Remove expired blocks from the cache
	for key := range pr.blockCache {
		if _, ok := cacheKeySet[key]; !ok {
			delete(pr.blockCache, key)
		}
	}

	// Update the active block key
	pr.activeCachedBlockKey = activeBlockKey
	return nil
}

// GetCachedChanges attempts to retrieve changes for the specified range using cached data.  If cache isn't
// sufficient for the range, returns isCached=false and callers should call getIndexChanges
func (pr *DensePartitionStorageReader) getCachedChanges(partitionRange base.PartitionRange) (changes *PartitionChanges, isCached bool, err error) {
	pr.lock.RLock()
	defer pr.lock.RUnlock()

	// If blocklist isn't loaded, no cache
	if pr.blockList == nil {
		return nil, false, nil
	}

	// If the since value is earlier than the cache's validFrom, can't use cache
	// Note: We're not comparing partitionRange.To with pr.validTo, because it's valid for the partition reader to
	// be behind the requested range.
	if partitionRange.SinceBefore(pr.validFrom) {
		return nil, false, nil
	}

	// Iterate over the blocks, collecting entries.  Iterating over the blocks in reverse to support simple
	// duplicate detection.
	// KeySet is used to identify duplicates across blocks. Writers guarantee no duplicates within a
	// given block, but we may see duplicates when spanning multiple blocks, in this scenario:
	//         1. Writer adds foo rev2 to block 2
	//         2. We read block 2, see foo rev2
	//         3. We read block 1, see foo rev1
	//         4. Writer removes foo rev1 from block 1
	changes = NewPartitionChanges()
	keySet := make(map[string]bool, 0)
	for i := len(pr.blockList.blocks) - 1; i >= 0; i-- {

		blockListEntry := pr.blockList.blocks[i]
		blockKey := blockListEntry.Key(pr.blockList)
		currBlock, ok := pr._getCachedBlock(blockKey)
		if !ok {
			base.Warn("Unexpected missing block from partition cache. blockKey:[%s] block startClock:[%s] cache validFrom:[%s]",
				blockKey, blockListEntry.StartClock.String(), pr.validFrom.String())
			return nil, false, nil
		}
		blockIter := NewDenseBlockIterator(currBlock)
		blockChanges := NewPartitionChanges()
		for {
			blockEntry := blockIter.next()
			if blockEntry == nil {
				// End of block, continue to next block
				break
			}
			switch compare := partitionRange.Compare(blockEntry.getVbNo(), blockEntry.getSequence()); compare {
			case base.PartitionRangeAfter:
			// Possible when processing the most recent block in the range, when block is ahead of stable seq
			case base.PartitionRangeWithin:
				// Deduplication check
				docIdString := string(blockEntry.getDocId())
				if keySet[docIdString] {
					pr.notifyDuplicateFound(currBlock.Key)
				} else {
					blockChanges.AddEntry(blockEntry.MakeLogEntryWithDocId(docIdString))
					keySet[docIdString] = true
				}
			case base.PartitionRangeBefore:
				// Expected when processing the oldest block in the range.  Don't include in set
			}
		}
		// Prepend partition changes with the results for this block
		changes.PrependChanges(blockChanges)

		// If we've reached a block with a startclock earlier than our since value, we're done
		if partitionRange.SinceAfter(blockListEntry.StartClock) {
			break
		}
	}

	return changes, true, err
}

// If a reader hits a duplicate during load from cache, it means that one of the older blocks in the cache is stale (i.e. has been
// updated by an index writer to remove an older revision).  Flag it for reload during next cache update
func (pr *DensePartitionStorageReader) notifyDuplicateFound(blockKey string) {

	pr.pendingReloadLock.Lock()
	defer pr.pendingReloadLock.Unlock()
	pr.pendingReload[blockKey] = true

}

// GetIndexedChanges retrieves changes directly from the index.
func (pr *DensePartitionStorageReader) getIndexedChanges(partitionRange base.PartitionRange) (changes *PartitionChanges, err error) {

	// Initialize block list
	blockList := NewDenseBlockListReader(pr.channelName, pr.partitionNo, pr.indexBucket)
	if blockList == nil {
		return nil, errors.New("Unable to initialize block list")
	}
	err = blockList.populateForRange(partitionRange)
	if err != nil {
		return nil, err
	}
	changes = NewPartitionChanges()
	keySet := make(map[string]bool, 0)
	for i := len(blockList.blocks) - 1; i >= 0; i-- {
		blockListEntry := blockList.blocks[i]
		blockKey := blockListEntry.Key(blockList)

		currBlock, err := pr.loadBlock(blockKey, blockListEntry.StartClock)
		if err != nil {
			base.Warn("Unexpected missing block from index. blockKey:[%s] err:[%v]",
				blockKey, err)
			return nil, err
		}
		blockIter := NewDenseBlockIterator(currBlock)
		blockChanges := NewPartitionChanges()
		for {
			blockEntry := blockIter.next()
			if blockEntry == nil {
				// End of block, break out of inner for loop over block entries and continue to next block in outer for loop
				break
			}
			switch compare := partitionRange.Compare(blockEntry.getVbNo(), blockEntry.getSequence()); compare {
			case base.PartitionRangeAfter:
				// Possible when processing the most recent block in the range
			case base.PartitionRangeWithin:
				// Deduplication check
				docIdString := string(blockEntry.getDocId())
				if keySet[docIdString] {
					// Ignore duplicates.
				} else {
					blockChanges.AddEntry(blockEntry.MakeLogEntryWithDocId(docIdString))
					keySet[docIdString] = true
				}
			case base.PartitionRangeBefore:
				// Expected when processing the oldest block in the range.  Don't include in set
			}
		}
		// Prepend partition changes with the results for this block
		changes.PrependChanges(blockChanges)

		// If we've reached a block with a startclock earlier than our since value, we're done
		if partitionRange.SinceAfter(blockListEntry.StartClock) {
			break
		}
	}
	return changes, nil
}

// Loads a block and adds to cache.  Callers must hold pr.lock.Lock()
func (pr *DensePartitionStorageReader) _loadAndCacheBlock(listEntry *DenseBlockListEntry) (*DenseBlock, error) {
	blockKey := listEntry.Key(pr.blockList)
	block, err := pr.loadBlock(blockKey, listEntry.StartClock)
	if err != nil {
		return nil, err
	}
	pr.blockCache[blockKey] = block
	IndexExpvars.Add("indexReader.blocksCached", 1)
	return block, nil
}

// Check whether the block for the specified block list entry is in the cached block map.
//  Callers must hold pr.lock.RLock()
func (pr *DensePartitionStorageReader) _getCachedBlock(key string) (*DenseBlock, bool) {
	block, ok := pr.blockCache[key]
	if !ok {
		return nil, false
	}
	return block, true
}

// Loads a block from the index bucket
func (pr *DensePartitionStorageReader) loadBlock(key string, startClock base.PartitionClock) (*DenseBlock, error) {

	block := NewDenseBlock(key, startClock)
	err := block.loadBlock(pr.indexBucket)
	if err != nil {
		return nil, err
	}
	IndexExpvars.Add("indexReader.blocksLoaded", 1)
	return block, nil
}
