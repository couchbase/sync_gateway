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
	"encoding/json"
	"fmt"
	"log"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

const (
	KeyFormat_DenseBlockList       = "%s:blist%d:p%d:%s" //  base.KIndexPrefix, list index, partition, channelname
	KeyFormat_DenseBlockListActive = "%s:blist:p%d:%s"   //  base.KIndexPrefix, partition, channelname
	KeyFormat_DenseBlock           = "%s:block%d:p%d:%s" //  base.KIndexPrefix, block index, partition, channelname
)

// Implementation of ChannelStorage that stores entries as an append-based list of
// full log entries
type DenseStorage struct {
	indexBucket      base.Bucket                       // Index bucket
	channelName      string                            // Channel name
	partitions       *base.IndexPartitions             // Partition assignment map
	partitionStorage map[uint16]*DensePartitionStorage // PartitionStorage for this channel
}

func NewDenseStorage(bucket base.Bucket, channelName string, partitions *base.IndexPartitions) *DenseStorage {

	storage := &DenseStorage{
		indexBucket:      bucket,
		channelName:      channelName,
		partitions:       partitions,
		partitionStorage: make(map[uint16]*DensePartitionStorage, partitions.PartitionCount()),
	}
	return storage
}

// Adds the provided set of entries to storage.  Batches by partition, then invokes each partition update
func (ds *DenseStorage) AddEntrySet(entries []*LogEntry) (clockUpdates base.SequenceClock, err error) {

	partitionSets := make(map[uint16][]*LogEntry)
	for _, entry := range entries {
		partitionNo := ds.partitions.VbMap[entry.VbNo]
		if _, ok := partitionSets[partitionNo]; !ok {
			partitionSets[partitionNo] = make([]*LogEntry, len(entries))
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
		clockUpdates.UpdateWithClock(partitionUpdateClock)
	}
	return clockUpdates, nil
}

// TODO: For channel backfill, need to guarantee that []*LogEntry is ordered identically for repeated requests

func (ds *DenseStorage) GetChanges(fromSeq base.SequenceClock, channelClock base.SequenceClock) ([]*LogEntry, error) {
	// Identify changed partitions based on clock
	// call partitionStorage.GetChanges(from partition clock, to partition clock) for each partition to get entries for that partition
	//
	return nil, nil
}

func (ds *DenseStorage) StoresLogEntries() bool {
	return false
}

func (ds *DenseStorage) WriteLogEntry(entry *LogEntry) error {
	// no-op
	return nil
}

// DensePartitionStorage manages storage for a partition.  Index entries within a partition (across multiple vbuckets)
// are stored in the same DenseBlocks
type DensePartitionStorage struct {
	blockList   *DenseBlockList // List of blocks associated with the partition and channel
	channelName string          // Channel name
	partitionNo uint16          // Partition number
	indexBucket base.Bucket     // Index bucket
}

func NewDensePartitionStorage(channelName string, partitionNo uint16, indexBucket base.Bucket) *DensePartitionStorage {
	storage := &DensePartitionStorage{
		channelName: channelName,
		partitionNo: partitionNo,
		indexBucket: indexBucket,
	}

	storage.blockList = NewDenseBlockList(channelName, partitionNo, storage.indexBucket)
	return storage
}
func (dps *DensePartitionStorage) init() error {
	return nil
}

func (dps *DensePartitionStorage) getActiveBlock() *DenseBlock {
	return dps.blockList.activeBlock
}

func (dps *DensePartitionStorage) AddEntrySet(entries []*LogEntry) (base.SequenceClock, error) {

	overflow, pendingRemoval, err := dps.getActiveBlock().addEntries(entries)
	if err != nil {
		return nil, err
	}

	// Process any entries with previous index entries in older blocks
	if len(pendingRemoval) > 0 {
		dps.removeEntries(pendingRemoval)
	}

	for len(overflow) > 0 {
		_, err = dps.blockList.AddBlock()
		if err != nil {
			return nil, err
		}
		overflow, pendingRemoval, err = dps.getActiveBlock().addEntries(entries)
		dps.removeEntries(pendingRemoval)
	}

	// TODO: return updated clock (high sequence number for entries per vbno)
	return nil, nil
}

// TODO: look up old entries in non-active blocks and remove them
func (dps *DensePartitionStorage) removeEntries(pendingRemoval []*LogEntry) error {
	return nil
}

// PartitionClock is simplified version of SequenceClock
type PartitionClock map[uint16]uint64

func (clock PartitionClock) Add(other PartitionClock) {
	for key, otherValue := range other {
		value, _ := clock[key]
		clock[key] = value + otherValue
	}
}

func (clock PartitionClock) Copy() PartitionClock {
	newClock := make(PartitionClock, len(clock))
	for key, value := range clock {
		newClock[key] = value
	}
	return newClock
}

const (
	MaxListBlockCount = 1000 // When the number of blocks in the active list exceeds MaxListBlockCount, it's rotated
)

// DenseBlockList is an ordered list of DenseBlockListEntries keys.  Each key is associated with the starting
// clock for that DenseBlock.  The list is persisted into one or more documents (DenseBlockListStorage) in the index.
// The active list has key activeKey - older lists are rotated into activeKey_n
type DenseBlockList struct {
	indexBucket      base.Bucket           // Index Bucket
	blocks           []DenseBlockListEntry // Dense Block keys
	activeKey        string                // Key for active list doc
	activeCas        uint64                // Cas for active list doc
	activeStartIndex int                   // Position of the start of the active doc in the entries
	activeCount      int                   // Counter for the active doc
	channelName      string                // Channel Name
	partition        uint16                // Partition number
	activeBlock      *DenseBlock           // Active block for the list
}

type DenseBlockListStorage struct {
	ListIndex uint32                `json:"list_index"`
	Blocks    []DenseBlockListEntry `json:"blocks"`
}

type DenseBlockListEntry struct {
	BlockIndex int            `json:"index"` // Dense Block index
	StartClock PartitionClock `json:"clock"` // Starting clock for Dense Block
}

func NewDenseBlockList(channelName string, partition uint16, indexBucket base.Bucket) *DenseBlockList {

	list := &DenseBlockList{
		channelName: channelName,
		partition:   partition,
		indexBucket: indexBucket,
	}
	list.activeKey = list.generateActiveListKey()
	list.init()
	return list
}

// Creates a new block, and adds to the block list
func (l *DenseBlockList) AddBlock() (*DenseBlock, error) {

	// Mark previous block inactive
	if l.activeBlock != nil {
		l.activeBlock.MarkInactive()
	}

	nextIndex := l.generateNextBlockIndex()
	var nextStartClock PartitionClock
	if l.activeBlock == nil {
		// No previous active block - new block list
		nextStartClock = make(PartitionClock)
	} else {
		// Determine index and startclock from previous active block
		nextStartClock = l.activeBlock.startClock.Copy()
		nextStartClock.Add(l.activeBlock.clock)
	}

	nextBlockKey := l.generateBlockKey(nextIndex)
	block := NewDenseBlock(nextBlockKey, nextStartClock)

	// Add the new block to the list
	listEntry := DenseBlockListEntry{
		BlockIndex: nextIndex,
		StartClock: nextStartClock,
	}
	l.blocks = append(l.blocks, listEntry)
	// Do a CAS-safe write of the active list
	value, err := l.marshalActive()
	if err != nil {
		return nil, err
	}

	log.Printf("%s", value)

	casOut, err := l.indexBucket.WriteCas(l.activeKey, 0, 0, l.activeCas, value, sgbucket.Raw)
	if err != nil {
		// CAS error.  If there's a concurrent writer for this partition, assume they have created the new block.
		//  Re-initialize the current block list, and get the active block key from there.
		l.init()
		if len(l.blocks) == 0 {
			return nil, fmt.Errorf("Unable to determine active block after DenseBlockList cas write failure")
		}
		latestEntry := l.blocks[len(l.blocks)-1]
		return NewDenseBlock(l.generateBlockKey(latestEntry.BlockIndex), latestEntry.StartClock), nil
	}
	l.activeCas = casOut
	l.activeBlock = block
	return block, nil
}

func (l *DenseBlockList) loadActiveBlock() *DenseBlock {
	if len(l.blocks) == 0 {
		return nil
	} else {
		latestEntry := l.blocks[len(l.blocks)-1]
		return NewDenseBlock(l.generateBlockKey(latestEntry.BlockIndex), latestEntry.StartClock)
	}

}

// Rotate out the active block list document from the active key to a rotated key (adds activeCount to the key), and
// start a new empty block.
func (l *DenseBlockList) rotate() error {
	rotatedKey := l.generateRotatedListKey()

	rotatedValue, err := l.marshalActive()
	if err != nil {
		return err
	}
	_, err = l.indexBucket.WriteCas(rotatedKey, 0, 0, 0, rotatedValue, sgbucket.Raw)
	if err != nil {
		// TODO: confirm cas error and not retry error
		// CAS error - someone else has already rotated out for this count.  Continue to initialize empty
	}

	// Empty the active list
	l.activeCount++
	l.activeStartIndex = len(l.blocks)
	activeValue, err := l.marshalActive()
	if err != nil {
		return err
	}
	l.activeCas, err = l.indexBucket.WriteCas(l.activeKey, 0, 0, l.activeCas, activeValue, sgbucket.Raw)
	if err != nil {
		// CAS error.  Assume concurrent writer has already updated the active block list.
		//  Re-initialize the current block list.
		l.init()
	}

	return nil
}

// Only loads the active block list doc during init.  Older entries are lazy loaded when a search
// requests a clock earlier than the first entry's clock
func (l *DenseBlockList) init() error {

	var activeBlockList DenseBlockListStorage
	var err error
	log.Printf("attempting to retrieve key: %s", l.activeKey)
	l.activeCas, err = l.indexBucket.Get(l.activeKey, &activeBlockList)
	if err != nil {
		if base.GoCBErrorType(err) == base.GoCBErr_MemdStatusKeyNotFound {
			l.blocks = make([]DenseBlockListEntry, 0)
			l.activeCount = 0
		} else {
			return err
		}
	}
	l.blocks = activeBlockList.Blocks
	l.activeStartIndex = 0

	l.activeBlock = l.loadActiveBlock()
	return nil
}

func (l *DenseBlockList) unmarshalActive(value []byte) error {
	var activeBlock DenseBlockListStorage
	if err := json.Unmarshal(value, &activeBlock); err != nil {
		return err
	}
	return nil
}

// Marshals the active block list
func (l *DenseBlockList) marshalActive() ([]byte, error) {

	activeBlock := &DenseBlockListStorage{
		ListIndex: uint32(l.activeCount),
	}
	// When initializing an empty active block list (no blocks), activeStartIndex > len(l.blocks). Only
	// include blocks in the output when this isn't the case.
	log.Printf("startindex [%d], length [%d]", l.activeStartIndex, len(l.blocks))
	if l.activeStartIndex <= len(l.blocks) {
		log.Println("adding blocks")
		activeBlock.Blocks = l.blocks[l.activeStartIndex:]
	}
	return json.Marshal(activeBlock)
}

func (l *DenseBlockList) getActiveBlockListEntry() (entry DenseBlockListEntry, ok bool) {
	if len(l.blocks) > 0 {
		return l.blocks[len(l.blocks)-1], true
	} else {
		return entry, false
	}
}

func (l *DenseBlockList) generateNextBlockIndex() int {
	lastBlockIndex, ok := l.getActiveBlockListEntry()
	if ok {
		return lastBlockIndex.BlockIndex + 1
	} else {
		return 0
	}
}

func (l *DenseBlockList) generateRotatedListKey() string {
	return fmt.Sprintf(KeyFormat_DenseBlockList, base.KIndexPrefix, l.activeCount, l.partition, l.channelName)
}

func (l *DenseBlockList) generateActiveListKey() string {
	return fmt.Sprintf(KeyFormat_DenseBlockListActive, base.KIndexPrefix, l.partition, l.channelName)
}

func (l *DenseBlockList) generateBlockKey(blockIndex int) string {
	return fmt.Sprintf(KeyFormat_DenseBlock, base.KIndexPrefix, blockIndex, l.partition, l.channelName)
}
