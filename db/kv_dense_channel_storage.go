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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

const (
	KeyFormat_DenseBlockList       = "%s:blist%d:p%d:%s" //  base.KIndexPrefix, list index, partition, channelname
	KeyFormat_DenseBlockListActive = "%s:blist:p%d:%s"   //  base.KIndexPrefix, partition, channelname
	KeyFormat_DenseBlock           = "%s:block%d:p%d:%s" //  base.KIndexPrefix, block index, partition, channelname
)

// PartitionClock is simplified version of SequenceClock
type PartitionClock map[uint16]uint64

// Adds the values from another clock to the current clock
func (clock PartitionClock) Add(other PartitionClock) {
	for key, otherValue := range other {
		value, _ := clock[key]
		clock[key] = value + otherValue
	}
}

// Set the values in the current clock to the values in other clock
func (clock PartitionClock) Set(other PartitionClock) {
	for key, otherValue := range other {
		clock[key] = otherValue
	}
}

func (clock PartitionClock) Copy() PartitionClock {
	newClock := make(PartitionClock, len(clock))
	for key, value := range clock {
		newClock[key] = value
	}
	return newClock
}

func (clock PartitionClock) SetSequence(vbNo uint16, seq uint64) {
	clock[vbNo] = seq
}

func (clock PartitionClock) GetSequence(vbNo uint16) uint64 {
	if seq, ok := clock[vbNo]; ok {
		return seq
	} else {
		return 0
	}
}

func (clock PartitionClock) AddToClock(seqClock base.SequenceClock) {
	for vbNo, seq := range clock {
		seqClock.SetSequence(vbNo, seq)
	}
}

// PartitionRange is a pair of clocks defining a range of sequences with a partition.
// Defines helper functions for range comparison
type PartitionRange struct {
	Since PartitionClock
	To    PartitionClock
}

func NewPartitionRange() PartitionRange {
	return PartitionRange{
		Since: make(PartitionClock),
		To:    make(PartitionClock),
	}
}

func (p PartitionRange) SetRange(vbNo uint16, sinceSeq, toSeq uint64) {
	p.Since[vbNo] = sinceSeq
	p.To[vbNo] = toSeq
}

// StartsBefore returns true if any non-nil since sequences in the partition range
// are earlier than the partition clock
func (p PartitionRange) SinceBefore(clock PartitionClock) bool {
	for vbNo, sinceSeq := range p.Since {
		if sinceSeq < clock.GetSequence(vbNo) {
			return true
		}
	}
	return false
}

// StartsAfter returns true if all since sequences in the partition range are
// equal to or later than the partition clock
func (p PartitionRange) SinceAfter(clock PartitionClock) bool {
	for vbNo, sinceSeq := range p.Since {
		if sinceSeq < clock.GetSequence(vbNo) {
			return false
		}
	}
	return true
}

// Identifies whether the specified vbNo, sequence is within the PartitionRange.
// If the vbNo isn't included in the partition range, returns false.
func (p PartitionRange) Includes(vbNo uint16, sequence uint64) bool {
	var sinceSeq, toSeq uint64
	var ok bool
	sinceSeq, ok = p.Since[vbNo]
	if !ok {
		return false
	}
	toSeq, ok = p.To[vbNo]
	if !ok {
		return false
	}

	if sinceSeq < sequence && toSeq >= sequence {
		return true
	}
	return false
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
	activeStartIndex int                   // Index of the start of the active list doc in DenseBlockList.blocks
	activeCounter    uint32                // Counter for the active doc
	channelName      string                // Channel Name
	partition        uint16                // Partition number
	activeBlock      *DenseBlock           // Active block for the list
	validFromCounter uint32                // Count of the oldest list doc loaded
}

type DenseBlockListStorage struct {
	Counter uint32                `json:"counter"`
	Blocks  []DenseBlockListEntry `json:"blocks"`
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
	err := list.initDenseBlockList()
	if err != nil {
		base.Warn("Error initializing dense block list:%v", err)
		return nil
	}
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
		nextStartClock = l.activeBlock.getCumulativeClock()
	}

	base.LogTo("ChannelStorage+", "Adding block to list. channel:[%s] partition:[%d] index:[%d]", l.channelName, l.partition, nextIndex)

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

	casOut, err := l.indexBucket.WriteCas(l.activeKey, 0, 0, l.activeCas, value, sgbucket.Raw)
	if err != nil {
		// CAS error.  If there's a concurrent writer for this partition, assume they have created the new block.
		//  Re-initialize the current block list, and get the active block key from there.
		l.initDenseBlockList()
		if len(l.blocks) == 0 {
			return nil, fmt.Errorf("Unable to determine active block after DenseBlockList cas write failure")
		}
		latestEntry := l.blocks[len(l.blocks)-1]
		return NewDenseBlock(l.generateBlockKey(latestEntry.BlockIndex), latestEntry.StartClock), nil
	}
	l.activeCas = casOut
	l.activeBlock = block
	base.LogTo("ChannelStorage+", "Successfully added block to list. channel:[%s] partition:[%d] index:[%d]", l.channelName, l.partition, nextIndex)

	return block, nil
}

func (l *DenseBlockList) loadActiveBlock() *DenseBlock {
	if len(l.blocks) == 0 {
		return NewDenseBlock(l.generateBlockKey(0), PartitionClock{})
	} else {
		latestEntry := l.blocks[len(l.blocks)-1]
		return NewDenseBlock(l.generateBlockKey(latestEntry.BlockIndex), latestEntry.StartClock)
	}
}

// Rotate out the active block list document from the active key to a rotated key (adds activeCounter to the key), and
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
	l.activeCounter++
	l.activeStartIndex = len(l.blocks)
	activeValue, err := l.marshalActive()
	if err != nil {
		return err
	}
	l.activeCas, err = l.indexBucket.WriteCas(l.activeKey, 0, 0, l.activeCas, activeValue, sgbucket.Raw)
	if err != nil {
		// CAS error.  Assume concurrent writer has already updated the active block list.
		//  Re-initialize the current block list.
		l.initDenseBlockList()
	}

	return nil
}

// Only loads the active block list doc during init.  Older entries are lazy loaded when a search
// requests a clock earlier than the first entry's clock
func (l *DenseBlockList) initDenseBlockList() error {
	var activeBlockList DenseBlockListStorage
	var err error
	l.activeCas, err = l.indexBucket.Get(l.activeKey, &activeBlockList)
	if err != nil {
		// TODO: figure out how to make this work with walrus
		//if base.GoCBErrorType(err) == base.GoCBErr_MemdStatusKeyNotFound {

		// New block list - add first empty block
		base.LogTo("ChannelStorage+", "Creating new block list. channel:[%s] partition:[%d]", l.channelName, l.partition)
		l.blocks = make([]DenseBlockListEntry, 0)
		l.activeBlock, _ = l.AddBlock()
	} else {
		l.blocks = activeBlockList.Blocks
		l.activeCounter = activeBlockList.Counter
		l.activeBlock = l.loadActiveBlock()
	}
	l.activeStartIndex = 0
	l.validFromCounter = l.activeCounter
	return nil
}

func (l *DenseBlockList) GetActiveBlock() *DenseBlock {
	return l.activeBlock
}

// LoadPrevious loads the previous DenseBlockList storage document, and:
//  - prepends the blocks in that DenseBlockList to the block set (l.blocks)
//  - shifts the activeStartIndex based on the modified list
//  - updates validFromCounter
func (l *DenseBlockList) LoadPrevious() error {
	if l.validFromCounter == 0 {
		return fmt.Errorf("Attempted to load previous block list when count=0")
	}
	previousCount := l.validFromCounter - 1
	previousBlockKey := l.generateNumberedListKey(previousCount)
	var previousBlockList DenseBlockListStorage
	cas, err := l.indexBucket.Get(previousBlockKey, &previousBlockList)
	if err != nil {
		return fmt.Errorf("Unable to find block list with key [%s]:%v", previousBlockKey, err)
	}
	l.blocks = append(previousBlockList.Blocks, l.blocks...)
	l.activeStartIndex += len(previousBlockList.Blocks)
	l.validFromCounter = previousCount
	l.activeCas = cas

	return nil
}

// ValidFrom returns the starting clock of the first block in the list.
func (l *DenseBlockList) ValidFrom() PartitionClock {
	if len(l.blocks) == 0 {
		return make(PartitionClock)
	}
	return l.blocks[0].StartClock
}

// Marshals the active block list
func (l *DenseBlockList) marshalActive() ([]byte, error) {

	activeBlock := &DenseBlockListStorage{
		Counter: uint32(l.activeCounter),
	}
	// When initializing an empty active block list (no blocks), activeStartIndex > len(l.blocks). Only
	// include blocks in the output when this isn't the case.
	if l.activeStartIndex <= len(l.blocks) {
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

func (l *DenseBlockList) generatePreviousListKey() string {
	return l.generateNumberedListKey(l.activeCounter - 1)
}

func (l *DenseBlockList) generateRotatedListKey() string {
	return l.generateNumberedListKey(l.activeCounter)
}

func (l *DenseBlockList) generateNumberedListKey(count uint32) string {
	return fmt.Sprintf(KeyFormat_DenseBlockList, base.KIndexPrefix, count, l.partition, l.channelName)
}

func (l *DenseBlockList) generateActiveListKey() string {
	return fmt.Sprintf(KeyFormat_DenseBlockListActive, base.KIndexPrefix, l.partition, l.channelName)
}

func (l *DenseBlockList) generateBlockKey(blockIndex int) string {
	return fmt.Sprintf(KeyFormat_DenseBlock, base.KIndexPrefix, blockIndex, l.partition, l.channelName)
}
