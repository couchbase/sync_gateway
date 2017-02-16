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
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

const (
	KeyFormat_DenseBlockList       = "%s:blist%d:p%d:%s" //  base.KIndexPrefix, list index, partition, channelname
	KeyFormat_DenseBlockListActive = "%s:blist:p%d:%s"   //  base.KIndexPrefix, partition, channelname
	KeyFormat_DenseBlock           = "%s:block%d:p%d:%s" //  base.KIndexPrefix, block index, partition, channelname
)

// Using var instead of const to simplify testing
var MaxListBlockCount = 1000 // When the number of blocks in the active list exceeds MaxListBlockCount, it's rotated

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
	BlockIndex int                 `json:"index"` // Dense Block index
	StartClock base.PartitionClock `json:"clock"` // Starting clock for Dense Block
	key        string              // Used for key helper function
}

func (e *DenseBlockListEntry) Key(parentList *DenseBlockList) string {
	if parentList == nil {
		base.Warn("Attempted to generate key without parent list")
		return ""
	}
	if e.key == "" {
		e.key = parentList.generateBlockKey(e.BlockIndex)
	}
	return e.key
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

func NewDenseBlockListReader(channelName string, partition uint16, indexBucket base.Bucket) *DenseBlockList {

	list := &DenseBlockList{
		channelName: channelName,
		partition:   partition,
		indexBucket: indexBucket,
	}
	list.activeKey = list.generateActiveListKey()
	found, err := list.loadDenseBlockList()
	if !found || err != nil {
		base.Warn("Error initializing dense block list:  found:[%v] err:[%v]", found, err)
		return nil
	}
	return list
}

func (l *DenseBlockList) ActiveListEntry() *DenseBlockListEntry {
	if len(l.blocks) == 0 {
		return nil
	}
	return &l.blocks[len(l.blocks)-1]
}

// Returns the block preceding the specified block index in the list.  Loads earlier block lists if needed.
// Returns error if currentBlockIndex not found in list.  Returns nil if currentBlockIndex is the first
// block in the list.
func (l *DenseBlockList) PreviousBlock(currentBlockIndex int) (*DenseBlockListEntry, error) {
	// Find the current block in the list
	var currentPosition int
	currentFound := false
	for position, entry := range l.blocks {
		if entry.BlockIndex == currentBlockIndex {
			currentPosition = position
			currentFound = true
			break
		}
	}
	if !currentFound {
		return nil, fmt.Errorf("Requested previous for unknown current index: [%d]", currentBlockIndex)
	}
	if currentPosition == 0 {
		// Not found in the current list, load the previous block list and run again
		err := l.LoadPrevious()
		if err != nil {
			// Current is the first block on the list - return nil
			return nil, nil
		}
		return l.PreviousBlock(currentBlockIndex)
	} else {
		return &l.blocks[currentPosition-1], nil
	}
}

// Creates a new block, and adds to the block list
func (l *DenseBlockList) AddBlock() (*DenseBlock, error) {

	// Mark previous block inactive
	if l.activeBlock != nil {
		l.activeBlock.MarkInactive()
	}

	nextIndex := l.generateNextBlockIndex()
	var nextStartClock base.PartitionClock
	if l.activeBlock == nil {
		// No previous active block - new block list
		nextStartClock = make(base.PartitionClock)
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

	if len(l.blocks) > l.activeStartIndex+MaxListBlockCount {
		err := l.rotate()
		if err != nil {
			return nil, err
		}
	}

	l.blocks = append(l.blocks, listEntry)
	// Do a CAS-safe write of the active list
	storageValue, err := l.marshalAsStorage()
	if err != nil {
		return nil, err
	}
	casOut, err := l.indexBucket.WriteCas(l.activeKey, 0, 0, l.activeCas, storageValue, 0)
	if err != nil {
		// CAS error.  If there's a concurrent writer for this partition, assume they have created the new block.
		//  Re-initialize the current block list, and get the active block key from there.
		found, err := l.loadDenseBlockList()
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("CAS error during block list write during AddBlock, but list not found on reload.")
		}
		return l.GetActiveBlock(), nil
	}
	l.activeCas = casOut
	l.activeBlock = block
	base.LogTo("ChannelStorage+", "Successfully added block to list. channel:[%s] partition:[%d] index:[%d] activeBlocks:[%d]", l.channelName, l.partition, nextIndex, len(l.blocks))

	return block, nil
}

func (l *DenseBlockList) loadActiveBlock() *DenseBlock {
	if len(l.blocks) == 0 {
		return NewDenseBlock(l.generateBlockKey(0), base.PartitionClock{})
	} else {
		latestEntry := l.blocks[len(l.blocks)-1]
		return l.LoadBlock(latestEntry)
	}
}

// Rotate out the active block list document from the active key to a rotated key (adds activeCounter to the key).  Rotate just modifies the underlying storage - the full set
// of blocks is retained in the in-memory DenseBlockList.  Marshal/unmarshal ensures persistence of only the active blocks
func (l *DenseBlockList) rotate() error {
	rotatedKey := l.generateRotatedListKey()

	rotatedStorageValue, err := l.marshalAsStorage()
	if err != nil {
		return err
	}
	_, err = l.indexBucket.WriteCas(rotatedKey, 0, 0, 0, rotatedStorageValue, 0)

	// For CAS error - someone else has already rotated out for this count.  Continue to initialize empty.  For all other errors,
	// return error
	if err != nil && !base.IsCasMismatch(l.indexBucket, err) {
		return err
	}

	// Empty the active list
	l.activeCounter++
	l.activeStartIndex = len(l.blocks)

	activeStorageValue, err := l.marshalAsStorage()
	if err != nil {
		return err
	}
	var casOut uint64
	casOut, err = l.indexBucket.WriteCas(l.activeKey, 0, 0, l.activeCas, activeStorageValue, 0)
	if err != nil {
		if base.IsCasMismatch(l.indexBucket, err) {
			// CAS error.  Assume concurrent writer has already updated the active block list.
			//  Re-initialize the current block list and return
			found, err := l.loadDenseBlockList()
			if !found {
				return fmt.Errorf("CAS fail on block list write, but no list found on reload.")
			}
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		l.activeCas = casOut
	}

	return nil
}

// Loads the dense block list.  Initializes an empty block list if not found.
func (l *DenseBlockList) initDenseBlockList() error {
	// Load the existing block list
	found, err := l.loadDenseBlockList()
	if err != nil {
		base.Warn("Dense block list load failed: %v", err)
		return err
	}
	// If block list doesn't exist, add a block (which will initialize)
	if !found {
		l.activeCas = 0
		base.LogTo("ChannelStorage+", "Creating new block list. channel:[%s] partition:[%d] cas:[%d]", l.channelName, l.partition, l.activeCas)
		l.blocks = make([]DenseBlockListEntry, 0)
		_, err = l.AddBlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// Only loads the active block list doc during init.  Older entries are lazy loaded when a search
// requests a clock earlier than the first entry's clock
func (l *DenseBlockList) loadDenseBlockList() (found bool, err error) {

	activeBlockListStorage, casOut, readError := l.loadStorage(l.activeKey)
	if readError != nil {
		if base.IsKeyNotFoundError(l.indexBucket, readError) {
			return false, nil
		} else {
			base.LogTo("ChannelStorage+", "Unexpected error attempting to retrieve active block list.  key:[%s] err:[%v]", l.activeKey, readError)
			return false, readError
		}
	}
	l.activeCas = casOut
	l.blocks = activeBlockListStorage.Blocks
	l.activeCounter = activeBlockListStorage.Counter
	l.activeBlock = l.loadActiveBlock()
	l.validFromCounter = l.activeCounter
	return true, nil
}

func (l *DenseBlockList) ReloadDenseBlockList() (found bool, err error) {
	return l.loadDenseBlockList()
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
	previousBlockListKey := l.generateNumberedListKey(previousCount)
	previousBlockListStorage, cas, readError := l.loadStorage(previousBlockListKey)
	if readError != nil {
		return fmt.Errorf("Unable to find block list with key [%s]:%v", previousBlockListKey, readError)
	}
	l.blocks = append(previousBlockListStorage.Blocks, l.blocks...)
	l.activeStartIndex += len(previousBlockListStorage.Blocks)
	l.validFromCounter = previousCount
	l.activeCas = cas

	return nil
}

func (l *DenseBlockList) loadStorage(key string) (storage DenseBlockListStorage, cas uint64, err error) {

	storage = DenseBlockListStorage{}
	casOut, err := l.indexBucket.Get(key, &storage)
	if err != nil {
		return storage, 0, err
	}
	return storage, casOut, nil

}

// ValidFrom returns the starting clock of the first block in the list.
func (l *DenseBlockList) ValidFrom() base.PartitionClock {
	if len(l.blocks) == 0 {
		return make(base.PartitionClock)
	}
	return l.blocks[0].StartClock
}

// Marshals the active block list
func (l *DenseBlockList) marshalAsStorage() (*DenseBlockListStorage, error) {

	activeBlock := &DenseBlockListStorage{
		Counter: uint32(l.activeCounter),
	}
	// When initializing an empty active block list (no blocks), activeStartIndex > len(l.blocks). Only
	// include blocks in the output when this isn't the case.
	if l.activeStartIndex <= len(l.blocks) {
		activeBlock.Blocks = l.blocks[l.activeStartIndex:]
	}
	return activeBlock, nil
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

func (l *DenseBlockList) populateForRange(partitionRange base.PartitionRange) error {
	// Block lists can span multiple documents.  If current blockList doesn't extend back to the start
	// of the requested partitionRange, load previous block list doc(s).
	for partitionRange.SinceBefore(l.ValidFrom()) {
		err := l.LoadPrevious()
		if err != nil {
			return fmt.Errorf("Unable to load previous block list: %v", err)
		}
	}
	return nil
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
