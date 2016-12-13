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
type DenseStorageWriter struct {
	indexBucket      base.Bucket                       // Index bucket
	channelName      string                            // Channel name
	partitions       *base.IndexPartitions             // Partition assignment map
	partitionStorage map[uint16]*DensePartitionStorage // PartitionStorage for this channel
}

func NewDenseStorageWriter(bucket base.Bucket, channelName string, partitions *base.IndexPartitions) *DenseStorageWriter {

	storage := &DenseStorageWriter{
		indexBucket:      bucket,
		channelName:      channelName,
		partitions:       partitions,
		partitionStorage: make(map[uint16]*DensePartitionStorage, partitions.PartitionCount()),
	}
	return storage
}

// Adds the provided set of entries to storage.  Batches by partition, then invokes each partition update
func (ds *DenseStorageWriter) AddEntrySet(entries []*LogEntry) (clockUpdates base.SequenceClock, err error) {

	partitionSets := make(map[uint16][]*LogEntry)
	for _, entry := range entries {
		partitionNo := ds.partitions.VbMap[entry.VbNo]
		if _, ok := partitionSets[partitionNo]; !ok {
			partitionSets[partitionNo] = make([]*LogEntry, 0)
		}
		partitionSets[partitionNo] = append(partitionSets[partitionNo], entry)
	}

	// Potential optimization: change clockUpdates to SyncSequenceClock, run partition updates in parallel.
	// With parallel channel updates already happening, this may not be worth the goroutine overhead.  Worth
	// revisiting if we need to address performance for scenarios with few channels and many updates per
	// channel per batch
	clockUpdates = base.NewSequenceClockImpl()

	for partitionNo, entries := range partitionSets {
		partitionStorage, ok := ds.partitionStorage[partitionNo]
		if !ok {
			partitionStorage = NewDensePartitionStorage(ds.channelName, partitionNo, ds.indexBucket)
			ds.partitionStorage[partitionNo] = partitionStorage
		}
		partitionUpdateClock, err := partitionStorage.AddEntrySet(entries)
		if err != nil {
			base.LogTo("ChannelIndex", "Unable to add entry set to partition.  entries count:[%d] channel:[%s]", len(entries), ds.channelName)
			return nil, err
		}
		partitionUpdateClock.AddToClock(clockUpdates)
	}

	return clockUpdates, nil
}

func (ds *DenseStorageWriter) StoresLogEntries() bool {
	return false
}

func (ds *DenseStorageWriter) WriteLogEntry(entry *LogEntry) error {
	// no-op
	return nil
}

func (dps *DensePartitionStorage) AddEntrySet(entries []*LogEntry) (PartitionClock, error) {

	base.LogTo("ChannelStorage+", "Adding entry set to partition storage.  channel:[%s] partition:[%d] #entries:[%d]", dps.channelName, dps.partitionNo, len(entries))
	overflow, pendingRemoval, updateClock, err := dps.getActiveBlock().AddEntrySet(entries, dps.indexBucket)

	if err != nil {
		return nil, err
	}

	// Process any entries with previous index entries in older blocks
	if len(pendingRemoval) > 0 {
		dps.removeEntries(pendingRemoval)
	}
	if updateClock == nil {
		updateClock = make(PartitionClock)
	}
	for len(overflow) > 0 {
		base.LogTo("ChannelStorage+",
			"Block overflow, adding new block. channel:[%s] partition:[%d] block:[%s] count:[%d] #overflow:[%d]",
			dps.channelName, dps.partitionNo, dps.getActiveBlock().key, dps.getActiveBlock().getEntryCount(), len(overflow))

		_, err = dps.blockList.AddBlock()
		if err != nil {
			return nil, err
		}
		var overflowClock PartitionClock
		overflow, pendingRemoval, overflowClock, err = dps.getActiveBlock().AddEntrySet(overflow, dps.indexBucket)
		updateClock.Set(overflowClock)
		dps.removeEntries(pendingRemoval)
	}

	// TODO: return updated clock (high sequence number for entries per vbno)
	return updateClock, nil
}
