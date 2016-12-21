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
	"github.com/couchbase/sync_gateway/base"
)

// Implementation of ChannelStorage that stores entries as an append-based list of
// full log entries
type DenseStorageReader struct {
	indexBucket      base.Bucket                             // Index bucket
	channelName      string                                  // Channel name
	partitions       *base.IndexPartitions                   // Partition assignment map
	partitionStorage map[uint16]*DensePartitionStorageReader // PartitionStorage for this channel
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

// Returns all changes for the channel with sequences greater than sinceClock, and less than or equal to
// toClock.  Changes need to be ordered by vbNo in order to support interleaving of results from multiple channels by
// caller.  Changes are retrieved in vbucket order, to allow a limit check after each vbucket (to avoid retrieval).  Since
// a given partition includes results for more than one vbucket

func (ds *DenseStorageReader) GetChanges(sinceClock base.SequenceClock, toClock base.SequenceClock, limit int) (changes []*LogEntry, err error) {

	changes = make([]*LogEntry, 0)

	// Identify what's changed:
	//  changedVbuckets: ordered list of vbuckets that have changes, based on the clock comparison
	//  partitionRanges: map from partitionNo to PartitionRange for each partition that's changed
	changedVbuckets, partitionRanges := ds.calculateChanged(sinceClock, toClock)
	base.LogTo("ChannelStorage+", "DenseStorageReader.GetChanges.  #changed partitions:[%d]", len(partitionRanges))

	changedPartitions := make(map[uint16]*PartitionChanges, len(partitionRanges))

	for _, vbNo := range changedVbuckets {
		partitionNo := ds.partitions.PartitionForVb(vbNo)
		partitionChanges, ok := changedPartitions[partitionNo]
		if !ok {
			partitionChanges, err = ds.getPartitionStorageReader(partitionNo).GetChanges(*partitionRanges[partitionNo])
			if err != nil {
				return changes, err
			}
		}
		// Append the changes for this vbucket
		changes = append(changes, partitionChanges.GetVbChanges(vbNo)...)
		if limit > 0 && len(changes) > limit {
			break
		}
	}
	return changes, nil
}

// Returns PartitionStorageReader for this channel storage reader.  Initializes if needed.
func (ds *DenseStorageReader) getPartitionStorageReader(partitionNo uint16) (partitionStorage *DensePartitionStorageReader) {
	var ok bool
	if partitionStorage, ok = ds.partitionStorage[partitionNo]; !ok {
		partitionStorage = NewDensePartitionStorageReader(ds.channelName, partitionNo, ds.indexBucket)
		ds.partitionStorage[partitionNo] = partitionStorage
	}
	return partitionStorage

}

// calculateChangedPartitions identifies which vbuckets have changed after sinceClock until toClock, groups these
// by partition, and returns as a array of PartitionRanges, indexed by partition number.
func (ds *DenseStorageReader) calculateChanged(sinceClock, toClock base.SequenceClock) (changedVbs []uint16, changedPartitions []*PartitionRange) {

	changedVbs = make([]uint16, 0)
	changedPartitions = make([]*PartitionRange, ds.partitions.PartitionCount())
	for vbNoInt, toSeq := range toClock.Value() {
		vbNo := uint16(vbNoInt)
		sinceSeq := sinceClock.GetSequence(vbNo)
		if sinceSeq < toSeq {
			changedVbs = append(changedVbs, vbNo)
			partitionNo := ds.partitions.PartitionForVb(vbNo)
			if changedPartitions[partitionNo] == nil {
				partitionRange := NewPartitionRange()
				changedPartitions[partitionNo] = &partitionRange
			}
			changedPartitions[partitionNo].SetRange(vbNo, sinceSeq, toSeq)
		}
	}
	return changedVbs, changedPartitions
}

// DensePartitionStorageReader is a non-caching reader - every read request retrieves the latest from the bucket.
type DensePartitionStorageReader struct {
	channelName string      // Channel name
	partitionNo uint16      // Partition number
	indexBucket base.Bucket // Index bucket
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

func NewDensePartitionStorageReader(channelName string, partitionNo uint16, indexBucket base.Bucket) *DensePartitionStorageReader {
	storage := &DensePartitionStorageReader{
		channelName: channelName,
		partitionNo: partitionNo,
		indexBucket: indexBucket,
	}
	return storage
}

func (r *DensePartitionStorageReader) GetChanges(partitionRange PartitionRange) (*PartitionChanges, error) {

	changes := NewPartitionChanges()

	// Initialize the block list to the starting range, then find the starting block for the partition range
	blockList := r.GetBlockListForRange(partitionRange)
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
			logEntry := blockIter.next()
			if logEntry == nil {
				break
			}
			switch compare := partitionRange.Compare(logEntry.VbNo, logEntry.Sequence); compare {
			case PartitionRangeAfter:
				break // We've exceeded the range - return
			case PartitionRangeWithin:
				changes.AddEntry(logEntry)
			}
		}
	}
	return changes, nil
}

func (r *DensePartitionStorageReader) GetBlockListForRange(partitionRange PartitionRange) *DenseBlockList {

	// Initialize the block list, by loading all block list docs until we get one with
	// a starting clock earlier than the partitionRange start.
	blockList := NewDenseBlockList(r.channelName, r.partitionNo, r.indexBucket)
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
	block.loadBlock(l.indexBucket)
	return block
}
