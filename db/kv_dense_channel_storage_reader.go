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
	"log"

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

// TODO: For channel backfill, need to guarantee that []*LogEntry is ordered identically for repeated requests

// Returns all changes for the channel with sequences greater than sinceClock, and less than or equal to
// toClock
func (ds *DenseStorageReader) GetChanges(sinceClock base.SequenceClock, toClock base.SequenceClock) ([]*LogEntry, error) {
	// Identify which partitions have changed
	changes := make([]*LogEntry, 0)
	partitionRanges := ds.calculateChangedPartitions(sinceClock, toClock)
	base.LogTo("ChannelStorage+", "DenseStorageReader.GetChanges.  #changed partitions:[%d]", len(partitionRanges))

	for partitionNo, partitionRange := range partitionRanges {
		partitionChanges, err := ds.getPartitionStorageReader(partitionNo).GetChanges(partitionRange, 0)
		base.LogTo("ChannelStorage+", "DenseStorageReader.GetChanges.  partitionNo:[%d] #changes:[%d]", partitionNo, len(partitionChanges))

		if err != nil {
			return nil, err
		}
		changes = append(changes, partitionChanges...)
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
// by partition, and returns as a map of PartitionRanges, indexed by partition number.
func (ds *DenseStorageReader) calculateChangedPartitions(sinceClock, toClock base.SequenceClock) map[uint16]PartitionRange {

	results := make(map[uint16]PartitionRange, 0)
	for vbNoInt, toSeq := range toClock.Value() {
		vbNo := uint16(vbNoInt)
		sinceSeq := sinceClock.GetSequence(vbNo)
		if sinceSeq < toSeq {
			partitionNo := ds.partitions.PartitionForVb(vbNo)
			partitionRange, ok := results[partitionNo]
			if !ok {
				partitionRange = NewPartitionRange()
				results[partitionNo] = partitionRange
			}
			partitionRange.SetRange(vbNo, sinceSeq, toSeq)
		}
	}
	return results
}

// DensePartitionStorageReader is a non-caching reader - every read request retrieves the latest from the bucket.
type DensePartitionStorageReader struct {
	channelName string      // Channel name
	partitionNo uint16      // Partition number
	indexBucket base.Bucket // Index bucket
}

func NewDensePartitionStorageReader(channelName string, partitionNo uint16, indexBucket base.Bucket) *DensePartitionStorageReader {
	storage := &DensePartitionStorageReader{
		channelName: channelName,
		partitionNo: partitionNo,
		indexBucket: indexBucket,
	}
	return storage
}

func (r *DensePartitionStorageReader) GetChanges(partitionRange PartitionRange, limit int) ([]*LogEntry, error) {

	entries := make([]*LogEntry, 0)
	// Load the block list to the starting range
	blockList := r.GetBlockListForRange(partitionRange)

	// Find the starting block for the partitionRange
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

	// Iterate over the blocks, returning changes
	for i := startIndex; i <= len(blockList.blocks)-1; i++ {
		blockIter := NewDenseBlockIterator(blockList.LoadBlock(blockList.blocks[i]))
		for {
			logEntry := blockIter.next()
			if logEntry == nil {
				break
			}
			if partitionRange.Includes(logEntry.VbNo, logEntry.Sequence) {
				entries = append(entries, logEntry)
				if limit > 0 && len(entries) >= limit {
					break
				}
			} else {
				break
			}
		}
	}
	return entries, nil
}

func (r *DensePartitionStorageReader) GetBlockListForRange(partitionRange PartitionRange) *DenseBlockList {

	// Initialize the block list, by loading all block list docs until we get one with
	// a starting clock earlier than the partitionRange start.
	blockList := NewDenseBlockList(r.channelName, r.partitionNo, r.indexBucket)
	validFromClock := blockList.ValidFrom()
	if partitionRange.SinceBefore(validFromClock) {
		err := blockList.LoadPrevious()
		if err != nil {
			log.Println("loadPrevious error: %v", err)
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
