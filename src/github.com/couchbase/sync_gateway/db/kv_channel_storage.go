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
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var byteCacheBlockCapacity = uint64(10000)

// ChannelStorage implemented as an interface, to support swapping to different underlying storage model
// without significant refactoring.
type ChannelStorage interface {

	// AddEntrySet adds a set of entries to the channel index
	AddEntrySet(entries []*LogEntry) (clockUpdates base.SequenceClock, err error)

	// GetAllEntries returns all entries for the channel in the specified range, for all vbuckets
	GetChanges(fromSeq base.SequenceClock, channelClock base.SequenceClock) ([]*LogEntry, error)

	// If channel storage implementation uses separate storage for log entries and channel presence,
	// WriteLogEntry and ReadLogEntry can be used to read/write.  Useful when changeIndex wants to
	// manage these document outside the scope of a channel.  StoresLogEntries() allows callers to
	// check whether this is available.
	StoresLogEntries() bool
	ReadLogEntry(vbNo uint16, sequence uint64) (*LogEntry, error)
	WriteLogEntry(entry *LogEntry) error

	// For unit testing only
	getIndexBlockForEntry(entry *LogEntry) IndexBlock
}

func NewChannelStorage(bucket base.Bucket, channelName string, partitions IndexPartitionMap) ChannelStorage {
	return NewBitFlagStorage(bucket, channelName, partitions)

}

type BitFlagStorage struct {
	bucket              base.Bucket // Index bucket
	channelName         string      // Channel name
	partitionMap        IndexPartitionMap
	indexBlockCache     map[string]IndexBlock // Recently used index blocks, by key
	indexBlockCacheLock sync.Mutex
}

func NewBitFlagStorage(bucket base.Bucket, channelName string, partitions IndexPartitionMap) *BitFlagStorage {

	storage := &BitFlagStorage{
		bucket:       bucket,
		channelName:  channelName,
		partitionMap: partitions,
	}
	storage.indexBlockCache = make(map[string]IndexBlock)
	return storage
}

func (b *BitFlagStorage) StoresLogEntries() bool {
	return true
}

func (b *BitFlagStorage) WriteLogEntry(entry *LogEntry) error {
	key := getEntryKey(entry.VbNo, entry.Sequence)
	value, _ := json.Marshal(entry)
	return b.bucket.SetRaw(key, 0, value)
}

// Reads a single entry from the index
func (b *BitFlagStorage) ReadLogEntry(vbNo uint16, sequence uint64) (*LogEntry, error) {
	entry := &LogEntry{}
	err := b.readIndexEntryInto(vbNo, sequence, entry)
	return entry, err
}

// Reads a single entry from the index
func (b *BitFlagStorage) readIndexEntryInto(vbNo uint16, sequence uint64, entry *LogEntry) error {
	key := getEntryKey(vbNo, sequence)
	value, _, err := b.bucket.GetRaw(key)
	if err != nil {
		base.Warn("DCache", "Error retrieving entry from bucket for sequence %d, key %s", sequence, key)
		return err
	}
	err = json.Unmarshal(value, entry)
	if err != nil {
		base.Warn("DCache", "Error unmarshalling entry for sequence %d, key %s", sequence, key)
		return err
	}
	return nil
}

// Adds a set
func (b *BitFlagStorage) AddEntrySet(entries []*LogEntry) (clockUpdates base.SequenceClock, err error) {

	// Update the sequences in the appropriate cache block
	if len(entries) == 0 {
		return clockUpdates, nil
	}

	// The set of updates may be distributed over multiple partitions and blocks.
	// To support this, iterate over the set, and define groups of sequences by block
	// TODO: this approach feels like it's generating a lot of GC work.  Considered an iterative
	//       approach where a set update returned a list of entries that weren't targeted at the
	//       same block as the first entry in the list, but this would force sequential
	//       processing of the blocks.  Might be worth revisiting if we see high GC overhead.
	blockSets := make(BlockSet)
	clockUpdates = base.NewSequenceClockImpl()
	for _, entry := range entries {
		// Update the sequence in the appropriate cache block
		base.LogTo("DCache+", "Add to channel index [%s], vbNo=%d, isRemoval:%v", b.channelName, entry.VbNo, entry.isRemoved())
		blockKey := GenerateBlockKey(b.channelName, entry.Sequence, b.partitionMap[entry.VbNo])
		if _, ok := blockSets[blockKey]; !ok {
			blockSets[blockKey] = make([]*LogEntry, 0)
		}
		blockSets[blockKey] = append(blockSets[blockKey], entry)
		clockUpdates.SetMaxSequence(entry.VbNo, entry.Sequence)
	}

	for _, blockSet := range blockSets {
		// update the block
		err := b.writeSingleBlockWithCas(blockSet)
		if err != nil {
			return nil, err
		}
	}

	return clockUpdates, err
}

// Expects all incoming entries to target the same block
func (b *BitFlagStorage) writeSingleBlockWithCas(entries []*LogEntry) error {

	if len(entries) == 0 {
		return nil
	}
	// Get block based on first entry
	block := b.getIndexBlockForEntry(entries[0])
	// Apply updates to the in-memory block
	for _, entry := range entries {
		err := block.AddEntry(entry)
		if err != nil { // Wrong block for this entry
			return err
		}
	}
	localValue, err := block.Marshal()
	if err != nil {
		base.Warn("Unable to marshal channel block - cancelling block update")
		return errors.New("Error marshalling channel block")
	}

	casOut, err := writeCasRaw(b.bucket, block.Key(), localValue, block.Cas(), 0, func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - kmay be called multiple times
		err = block.Unmarshal(value)
		for _, entry := range entries {
			err := block.AddEntry(entry)
			if err != nil { // Wrong block for this entry
				return nil, err
			}
		}
		return block.Marshal()
	})

	if err != nil {
		return err
	}
	block.SetCas(casOut)
	return nil
}

func (b *BitFlagStorage) loadBlock(block IndexBlock) error {

	data, cas, err := b.bucket.GetRaw(block.Key())
	if err != nil {
		return err
	}

	err = block.Unmarshal(data)
	block.SetCas(cas)

	if err != nil {
		return err
	}
	// If found, add to the cache
	b.putIndexBlockToCache(block.Key(), block)
	return nil
}

func (b *BitFlagStorage) GetChanges(fromSeq base.SequenceClock, toSeq base.SequenceClock) ([]*LogEntry, error) {

	// Determine which blocks have changed, and load those blocks

	base.LogTo("DIndex+", "[channelStorage.GetChanges] From clock, to clock: %s: %s", base.PrintClock(fromSeq), base.PrintClock(toSeq))
	blocksByVb, blocksByKey, err := b.calculateChangedBlocks(fromSeq, toSeq)
	if err != nil {
		return nil, err
	}
	b.bulkLoadBlocks(blocksByKey)

	// For each vbucket, create the entries from the blocks.  Create in reverse sequence order, for
	// deduplication once we've retrieved the full entry
	entries := make([]*LogEntry, 0)
	entryKeys := make([]string, 0)
	for vbNo, blocks := range blocksByVb {
		fromVbSeq := fromSeq.GetSequence(vbNo)
		toVbSeq := toSeq.GetSequence(vbNo)

		for blockIndex := len(blocks) - 1; blockIndex >= 0; blockIndex-- {
			blockEntries, keys := blocks[blockIndex].GetEntries(vbNo, fromVbSeq, toVbSeq, true)
			entries = append(entries, blockEntries...)
			entryKeys = append(entryKeys, keys...)
		}
	}

	// Bulk retrieval of individual entries
	results := b.bulkLoadEntries(entryKeys, entries)

	base.LogTo("DIndex+", "[channelStorage.GetChanges] Returning %d entries...", len(results))
	return results, nil

}

// Calculate the set of index blocks that need to be loaded.
//   blocksByVb stores which blocks need to be processed for each vbucket.  Multiple vb map
//   values can point to the same IndexBlock (i.e. when those vbs share a partition).
//   blocksByKey stores all IndexBlocks to be retrieved, indexed by block key - no duplicates
func (b *BitFlagStorage) calculateChangedBlocks(fromSeq base.SequenceClock, channelClock base.SequenceClock) (blocksByVb map[uint16][]IndexBlock, blocksByKey map[string]IndexBlock, err error) {

	blocksByVb = make(map[uint16][]IndexBlock)
	blocksByKey = make(map[string]IndexBlock, 1)

	for vbNo, clockVbSeq := range channelClock.Value() {
		log.Printf("Channel clock has value for vbNo (%d):%d", vbNo, clockVbSeq)
		fromVbSeq := fromSeq.GetSequence(vbNo)

		// Verify that the requested from value is less than the channel clock sequence (there are
		// new entries for this vbucket in the channel)
		if fromVbSeq >= clockVbSeq {
			continue
		}

		for _, blockKey := range GenerateBlockKeys(b.channelName, fromVbSeq, clockVbSeq, b.partitionMap[vbNo]) {
			block, found := blocksByKey[blockKey]
			if !found {
				block = newBitFlagBlockForKey(blockKey)
				blocksByKey[blockKey] = block
			}
			blocksByVb[vbNo] = append(blocksByVb[vbNo], block)
		}
	}
	return blocksByVb, blocksByKey, nil
}

// Bulk get the blocks from the index bucket, and unmarshal into the provided map.
func (b *BitFlagStorage) bulkLoadBlocks(loadedBlocks map[string]IndexBlock) {
	// Do bulk retrieval of blocks, and unmarshal into loadedBlocks
	var keySet []string
	for key, _ := range loadedBlocks {
		base.LogTo("DIndex+", "Loading blocks, adding key:%s", key)
		keySet = append(keySet, key)
	}
	blocks, err := b.bucket.GetBulkRaw(keySet)
	if err != nil {
		// TODO FIX: if there's an error on a single block retrieval, differentiate between that
		//  and an empty/non-existent block.  Requires identification of expected blocks by the cache.
		base.Warn("Error doing bulk get:", err)
	}

	// Unmarshal concurrently
	var wg sync.WaitGroup
	for key, blockBytes := range blocks {
		if len(blockBytes) > 0 {
			wg.Add(1)
			go func(key string, blockBytes []byte) {
				defer wg.Done()
				if err := loadedBlocks[key].Unmarshal(blockBytes); err != nil {
					base.Warn("Error unmarshalling block into map")
				}
			}(key, blockBytes)
		}
	}
	wg.Wait()
}

// Bulk get the blocks from the index bucket, and unmarshal into the provided map.
func (b *BitFlagStorage) bulkLoadEntries(keySet []string, blockEntries []*LogEntry) (results []*LogEntry) {
	// Do bulk retrieval of entries
	// TODO: do in batches if keySet is very large?

	entries, err := b.bucket.GetBulkRaw(keySet)
	if err != nil {
		base.Warn("Error doing bulk get:", err)
	}

	docIDs := make(map[string]struct{}, len(blockEntries))
	results = make([]*LogEntry, 0, len(blockEntries))
	// Unmarshal and deduplicate
	// TODO: unmarshalls sequentially on a single thread - consider unmarshalling in parallel, with a separate
	// iteration for deduplication - based on performance
	for _, entry := range blockEntries {
		entryKey := getEntryKey(entry.VbNo, entry.Sequence)
		entryBytes := entries[entryKey]
		removed := entry.isRemoved()
		if err := json.Unmarshal(entryBytes, entry); err != nil {
			base.Warn("Error unmarshalling entry for key", entryKey)
		}
		if _, exists := docIDs[entry.DocID]; !exists {
			docIDs[entry.DocID] = struct{}{}
			if removed {
				entry.setRemoved()
			}
			results = append(results, entry)
		}
	}
	return results
}

// Retrieves the appropriate index block for an entry. If not found in the channel's block cache,
// will initialize a new block and add to the map.
func (b *BitFlagStorage) getIndexBlockForEntry(entry *LogEntry) IndexBlock {

	partition := b.partitionMap[entry.VbNo]
	key := GenerateBlockKey(b.channelName, entry.Sequence, partition)

	// First attempt to retrieve from the cache of recently accessed blocks
	block := b.getIndexBlockFromCache(key)
	if block != nil {
		return block
	}

	// If not found in cache, attempt to retrieve from the index bucket
	block = NewIndexBlock(b.channelName, entry.Sequence, partition)
	err := b.loadBlock(block)
	if err == nil {
		return block
	}

	// If still not found, initialize a new empty index block and add to cache
	block = NewIndexBlock(b.channelName, entry.Sequence, partition)
	b.putIndexBlockToCache(key, block)

	return block
}

// Safely retrieves index block from the channel's block cache
func (b *BitFlagStorage) getIndexBlockFromCache(key string) IndexBlock {
	b.indexBlockCacheLock.Lock()
	defer b.indexBlockCacheLock.Unlock()
	return b.indexBlockCache[key]
}

// Safely inserts index block from the channel's block cache
func (b *BitFlagStorage) putIndexBlockToCache(key string, block IndexBlock) {
	b.indexBlockCacheLock.Lock()
	defer b.indexBlockCacheLock.Unlock()
	b.indexBlockCache[key] = block
}

// IndexBlock interface - defines interactions with a block
type IndexBlock interface {
	AddEntry(entry *LogEntry) error
	Key() string
	Marshal() ([]byte, error)
	Unmarshal(value []byte) error
	Cas() uint64
	SetCas(cas uint64)
	GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string)
	GetAllEntries() []*LogEntry
}

func NewIndexBlock(channelName string, sequence uint64, partition uint16) IndexBlock {
	return newBitFlagBlock(channelName, sequence, partition)
}

func NewIndexBlockForKey(key string) IndexBlock {
	return newBitFlagBlockForKey(key)
}

func GenerateBlockKey(channelName string, sequence uint64, partition uint16) string {
	return generateBitFlagBlockKey(channelName, sequence, partition)
}

func GenerateBlockKeys(channelName string, minSeq uint64, maxSeq uint64, partition uint16) []string {
	return generateBitFlagBlockKeys(channelName, minSeq, maxSeq, partition)
}

// Determine the cache block key for a sequence
func generateBitFlagBlockKey(channelName string, minSequence uint64, partition uint16) string {
	base.LogTo("DCache", "block index for minSequence %d is %d", minSequence, uint16(minSequence/byteCacheBlockCapacity))
	index := uint16(minSequence / byteCacheBlockCapacity)
	return getIndexBlockKey(channelName, index, partition)
}

// Returns an ordered list of blocks needed to return the range minSequence to maxSequence
func generateBitFlagBlockKeys(channelName string, minSequence uint64, maxSequence uint64, partition uint16) []string {
	var keys []string
	firstIndex := GenerateBitFlagIndex(minSequence)
	lastIndex := GenerateBitFlagIndex(maxSequence)
	for index := firstIndex; index <= lastIndex; index++ {
		keys = append(keys, getIndexBlockKey(channelName, index, partition))
	}
	return keys
}

func GenerateBitFlagIndex(sequence uint64) uint16 {
	return uint16(sequence / byteCacheBlockCapacity)
}

type BitFlagBlock struct {
	key    string           // DocID for the cache block doc
	value  BitFlagBlockData // Raw document value
	cas    uint64           // cas value of block in database
	expiry time.Time        // Expiry - used for compact
}

type BitFlagBlockData struct {
	MinSequence uint64            // Starting sequence
	Entries     map[uint16][]byte // Contents of the cache block doc
}

func (b *BitFlagBlockData) MaxSequence() uint64 {
	return b.MinSequence + byteCacheBlockCapacity - 1
}

/*
Note: Considered trying to store 4 sequences (presence/removed flags) per byte to optimize space, but don't feel it's worth
      the effort at this stage.  Could revisit as an optimization
type BitFlags byte

func (bf *BitFlags) Set(n uint8) error {
	if n > 7 {
		return errors.New("out of range")
	}
	*bf = (*bf) | (1 << n)
	return nil
}

func (bf *BitFlags) Unset(n uint8) error {
	if n > 7 {
		return errors.New("out of range")
	}
	*bf = (*bf) &^ (1 << n)
	return nil
}

func (d *BitFlagBlockData) addEntry(vbNo uint16, sequence uint64, removal bool) {
	localIndex := sequence - d.MinSequence
	byteIndex := uint64(localIndex / 4)
	bytePosition := localIndex%4 - 1
	bitFlags := d.Entries[vbNo][byteIndex]
	// presence is at flag 2 * bytePosition + 1
	presencePos := uint8(2*bytePosition + 1)
	// removal is at flag position 2 * bytePosition
	removalPos := uint8(2 * bytePosition)
	bitFlags.Set(presencePos)
	if removal {
		bitFlags.Set(removalPos)
	} else {
		bitFlags.Unset(removalPos)
	}
	return
*/

func (d *BitFlagBlockData) hasEntry(vbNo uint16, sequence uint64) (found, isRemoval bool) {
	return false, false
}

func newBitFlagBlock(channelName string, forSequence uint64, partition uint16) *BitFlagBlock {

	minSequence := uint64(forSequence/byteCacheBlockCapacity) * byteCacheBlockCapacity

	key := generateBitFlagBlockKey(channelName, minSequence, partition)

	cacheBlock := newBitFlagBlockForKey(key)

	// Initialize the entry map
	cacheBlock.value = BitFlagBlockData{
		MinSequence: minSequence,
		Entries:     make(map[uint16][]byte),
	}

	return cacheBlock
}

func newBitFlagBlockForKey(key string) *BitFlagBlock {

	cacheBlock := &BitFlagBlock{
		key: key,
	}

	return cacheBlock
}

func (b *BitFlagBlock) Key() string {
	return b.key
}

func (b *BitFlagBlock) Cas() uint64 {
	return b.cas
}

func (b *BitFlagBlock) SetCas(cas uint64) {
	b.cas = cas
}

func (b *BitFlagBlock) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(b.value)
	if err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

func (b *BitFlagBlock) Unmarshal(value []byte) error {
	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&b.value)
	if err != nil {
		return err
	}
	return nil
}

func (b *BitFlagBlock) AddEntry(entry *LogEntry) error {
	if _, ok := b.value.Entries[entry.VbNo]; !ok {
		b.value.Entries[entry.VbNo] = make([]byte, byteCacheBlockCapacity+kSequenceOffsetLength)
	}

	index := b.getIndexForSequence(entry.Sequence)
	if index < 0 || index >= uint64(len(b.value.Entries[entry.VbNo])) {
		return errors.New("Sequence out of range of block")
	}
	if entry.isRemoved() {
		b.value.Entries[entry.VbNo][index] = byte(2)
	} else {
		b.value.Entries[entry.VbNo][index] = byte(1)
	}
	return nil
}

func (b *BitFlagBlock) AddEntrySet(entries []*LogEntry) error {
	return nil
}

func (b *BitFlagBlock) GetAllEntries() []*LogEntry {
	results := make([]*LogEntry, 0)
	// Iterate over all vbuckets, returning entries for each.
	for vbNo, sequences := range b.value.Entries {
		for index, entry := range sequences {
			if entry != byte(0) {
				removed := entry == byte(2)
				newEntry := &LogEntry{VbNo: vbNo,
					Sequence: b.value.MinSequence + uint64(index),
				}
				if removed {
					newEntry.setRemoved()
				}
				results = append(results, newEntry)
			}

		}
	}
	return results
}

// Block entry retrieval - used by GetEntries and GetEntriesAndKeys.
func (b *BitFlagBlock) GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string) {

	// To avoid GC overhead when we append entries one-by-one into the results below, define the capacity as the
	// max possible (toSeq - fromSeq).  This is going to be unnecessarily large in most cases - could consider writing
	// a custom append
	entries = make([]*LogEntry, 0, toSeq-fromSeq+1)
	keySet = make([]string, 0, toSeq-fromSeq+1)
	vbEntries, ok := b.value.Entries[vbNo]
	if !ok { // nothing for this vbucket
		base.LogTo("DIndex+", "No entries found for vbucket %d in block %s", vbNo, b.Key())
		return entries, keySet
	}

	// Validate range against block bounds
	if fromSeq > b.value.MaxSequence() || toSeq < b.value.MinSequence {
		base.LogTo("DIndex+", "Invalid Range for block (from, to): (%d, %d).  MinSeq:%d", fromSeq, toSeq, b.value.MinSequence)
		return entries, keySet
	}

	// Determine the range to iterate within this block, based on fromSeq and toSeq
	var startIndex, endIndex uint64
	if fromSeq < b.value.MinSequence {
		startIndex = 0
	} else {
		startIndex = fromSeq - b.value.MinSequence
	}
	if toSeq > b.value.MaxSequence() {
		endIndex = b.value.MaxSequence() - b.value.MinSequence // max index value within block
	} else {
		endIndex = toSeq - b.value.MinSequence
	}

	base.LogTo("DIndex+", "Block.GetEntries for block.  endIndex:%d, startIndex:%d", endIndex, startIndex)
	for index := int(endIndex); index >= int(startIndex); index-- {
		entry := vbEntries[index]
		if entry != byte(0) {
			removed := entry == byte(2)
			newEntry := &LogEntry{VbNo: vbNo,
				Sequence: b.value.MinSequence + uint64(index),
			}
			if removed {
				newEntry.setRemoved()
			}
			entries = append(entries, newEntry)
			if includeKeys {
				keySet = append(keySet, getEntryKey(vbNo, newEntry.Sequence))
			}
		}
	}

	base.LogTo("DIndex+", "Block.GetEntries for block %s - returning %d entries...", b.Key(), len(entries))

	return entries, keySet
}

func (b *BitFlagBlock) getIndexForSequence(sequence uint64) uint64 {
	return sequence - b.value.MinSequence + kSequenceOffsetLength
}

// Determine the cache block index for a sequence
func (b *BitFlagBlock) getBlockIndex(sequence uint64) uint16 {
	return uint16(sequence / byteCacheBlockCapacity)
}

// BlockSet - used to organize collections of vbuckets by partition for block-based removal

type BlockSet map[string][]*LogEntry // Collection of index entries, indexed by block id.

func (b BlockSet) keySet() []string {
	keys := make([]string, len(b))
	i := 0
	for k := range b {
		keys[i] = k
		i += 1
	}
	return keys
}

// IndexEntry handling
// The BitFlagBlock implementation only stores presence and removal information in the block - the
// rest of the LogEntry details (DocID, RevID) are stored in a separate "entry" document.  The functions
// below manage interaction with the entry documents

// Bulk retrieval of index entries
/*
func readIndexEntriesInto(bucket base.Bucket, keys []string, entries map[string]*LogEntry) error {
	results, err := bucket.GetBulkRaw(keys)
	if err != nil {
		base.Warn("error doing bulk get:", err)
		return err
	}
	// TODO: unmarshal in concurrent goroutines
	for key, entryBytes := range results {
		if len(entryBytes) > 0 {
			entry := entries[key]
			removed := entry.Removed
			if err := entries[key].Unmarshal(entry); err != nil {
				base.Warn("Error unmarshalling entry")
			}
			if removed {
				entry.setRemoved()
			}
		}
	}
	return nil
}
*/

// Generate the key for a single sequence/entry
func getEntryKey(vbNo uint16, sequence uint64) string {
	return fmt.Sprintf("%s_entry:%d:%d", kCachePrefix, vbNo, sequence)
}
