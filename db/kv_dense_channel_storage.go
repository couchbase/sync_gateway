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
	"encoding/binary"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Implementation of ChannelStorage that stores entries as an append-based list of
// full log entries
type DenseStorage struct {
	bucket           base.Bucket                       // Index bucket
	channelName      string                            // Channel name
	partitions       *base.IndexPartitions             // Partition assignment map
	partitionStorage map[uint16]*DensePartitionStorage // PartitionStorage for this channel
}

func NewDenseStorage(bucket base.Bucket, channelName string, partitions *base.IndexPartitions) *DenseStorage {

	storage := &DenseStorage{
		bucket:      bucket,
		channelName: channelName,
		partitions:  partitions,
	}

	// Maximum theoretical block cache capacity is 1024 - if this index writer were indexing every vbucket,
	// and each vbucket sequence was in a different block.  More common case would be this index writer
	// having at most 512 vbuckets, and most of those vbuckets working the same block index per partition (16 vbs per
	// partition) == 32 blocks.  Setting default to 50 to handle any temporary spikes.
	/* var err error
	storage.indexBlockCache, err = base.NewLRUCache(50)
	if err != nil {
		base.LogFatal("Error creating LRU cache for index blocks: %v", err)
	}
	*/
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
		partitionUpdateClock, err := ds.partitionStorage[partitionNo].AddEntrySet(entries)
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

type DensePartitionStorage struct {
	partitionIndex      PartitionIndex // Mapping from clock to index block key
	partitionBlockCache *base.LRUCache // Cache of recently used index blocks
}

func (dps *DensePartitionStorage) AddEntrySet(entries []*LogEntry) (base.SequenceClock, error) {
	// attempt to add to active block
	// returns overflow, items requiring previous revision cleanup
	// do previous revision cleanup
	// handle the case where previous revision cleanup has made room on the active block
	// else if returns overflow, initialize new block
	// attempt to add to active block
	// handle the case where len(entries) is greater than multiple blocks
	// return updated clock (high sequence number for entries per vbno)
	return nil, nil
}

type PartitionIndex []PartitionIndexEntry
type PartitionClock map[uint16]uint64

type PartitionIndexEntry struct {
	partitionStart PartitionClock
	blockId        string
}

// DenseBlock has the following binary-encoded format
//  | Name               | Size                  | Description                                     |
//  |--------------------|-----------------------|-------------------------------------------------|
//  | blockIndexCount    | 2 bytes               | Number of entries in block                      |
//  | []BlockIndexEntry  | 12 bytes/entry        | List of vb, seq and length for entries in block |
//  | []BlockEntry       | variable length/entry | Key, rev id and flags for each entry            |
//  ------------------------------------------------------------------------------------------------

type DenseBlock struct {
	key            string // Key of block document in the index bucket
	value          []byte // Binary storage of block data, in the above format
	entryCount     uint16 // Number of log entries stored in the block
	indexEntryData []byte // Index portion of the block
	entryData      []byte // Entries portion of the block
	cas            uint64 // Document cas
}

func NewDenseBlock(key string, bucket *base.Bucket) *DenseBlock {

	// Set initial capacity of value to handle ~5 docs (depending on key length) - avoids a lot of
	// alloc overhead when the first few entries in the channel are appended (since append only
	// doubles capacity as needed).
	return &DenseBlock{
		key:            key,
		value:          make([]byte, 400),
		entryCount:     0,
		indexEntryData: make([]byte, 0),
		entryData:      make([]byte, 0),
	}
}

func (d *DenseBlock) getEntryCount() uint16 {
	return binary.BigEndian.Uint16(d.value[0:])
}

func (d *DenseBlock) setEntryCount(count uint16) {
	binary.BigEndian.PutUint16(d.value[0:2], count+amount)
}

func (d *DenseBlock) getBlockIndex() DenseBlockIndex {
	return value[2:2+INDEX_ENTRY_LEN*d.getEntryCount()]
}


func (d *DenseBlock) loadBlock(bucket base.Bucket) (err error) {
	d.value, d.cas, err = bucket.GetRaw(d.key)
	return err
}



// Adds entries to block and writes block to the bucket
func (d *DenseBlock) AddEntrySet(entries []*LogEntry, bucket base.Bucket) error {

	addError := d.addEntries(entries)
	if addError != nil {
		// Error adding entries - reset the block and return error
		d.loadBlock(bucket)
		return addError
	}

	casOut, writeErr := base.WriteCasRaw(bucket, d.key, d.value, d.cas, 0, func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times
		d.value = value
		// Reapply update
		addError := d.addEntries(entries)
		if addError != nil {
			d.loadBlock(bucket)
			return nil, addError
		}
		return d.value, nil
	})
	if writeErr != nil {
		return writeErr
	}
	d.cas = casOut
	return nil
}

// Adds a set of log entries to the block
func (d *DenseBlock) addEntries(entries []*LogEntry) error {

	for _, entry := range entries {
		if err := d.addEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

// Adds a LogEntry
func (d *DenseBlock) addEntry(logEntry *LogEntry) error {

	// Encode log entry as index and entry portions
	entryBytes := NewDenseBlockEntry(logEntry.DocID, logEntry.RevID, logEntry.Flags)
	indexBytes := NewDenseBlockIndexEntry(logEntry.VbNo, logEntry.Sequence, uint16(len(entryBytes)))

	// If this is a new addition to the channel, don't need to remove previous entry
	if logEntry.Flags&channels.Added != 0 {
		err := d.appendEntry(indexBytes, entryBytes)
		if err != nil {
			return err
		}
	} else {
		// TODO: Entry already exists in the channel - handle potential removal from this block
		indexPos, entryPos, entryLen :=

	}
	return nil
}

func (d *DenseBlock) findEntryBySequence(vbNo uint16, sequence uint64) {

	// Iterate over the index, gett
}

// AppendEntry
//  Increments the entry count of the block,
//  appends the provided indexBytes to the index portion of the block, and
//  appends the provided entryBytes to the entries portion of the block.
func (d *DenseBlock) appendEntry(indexBytes, entryBytes []byte) error {
	newCount, err := d.incrEntryCount(1)
	if err != nil {
		return err
	}

	// Resize the block by appending entry AND index on the end (ensures we have capacity
	// in d.value for indexBytes, entryBytes but avoids an additional alloc during append)
	// See https://play.golang.org/p/pUNq2sUN6h
	//   |n|oldIndex|oldEntries| -> |n|oldIndex|oldEntries|newEntry|newIndex|
	d.value = append(d.value, entryBytes...)
	d.value = append(d.value, indexBytes...)

	// EndOfIndex is
	endOfIndex := 2 + (newCount-1)*INDEX_ENTRY_LEN

	//  Shift all entries:
	// |n|oldIndex|oldEntries|newEntry|newIndexEntry| -> |n|oldIndex|oldEntrieoldEntries|newEntry|
	copy(d.value[endOfIndex+INDEX_ENTRY_LEN:], d.value[endOfIndex:])

	//  Insert index entry:
	// |n|oldIndex|oldEntrieoldEntries|newEntry| -> |n|oldIndex|newIndex|oldEntries|newEntry|
	copy(d.value[endOfIndex:endOfIndex+INDEX_ENTRY_LEN], indexBytes)

	return nil
}

// Increments the entry count
func (d *DenseBlock) incrEntryCount(amount uint16) (uint16, error) {
	count := d.getEntryCount()

	// Check for overflow
	if count+amount < count {
		return 0, fmt.Errorf("Maximum block entry count exceeded")
	}
	d.setEntryCount(count+amount)
	return count + amount, nil
}

func (d *DenseBlock) GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string) {
	return nil, nil
}

func (d *DenseBlock) GetAllEntries() []*LogEntry {
	return nil
}

type DenseBlockIndex []byte

// Attempts to find the specified [vb, seq] in the block index.  Returns index position, entry position and
// entry length when found.  Returns zeroes when not found
func (bi *DenseBlockIndex) findEntry(vbNo uint16, sequence uint64) (indexPosition, entryPosition uint32, entryLength uint16) {

	indexPosition = 0
	entryPosition = 2 + len(bi)
	for indexPosition < len(bi) {
		var entry DenseBlockIndexEntry
		entry = DenseBlockIndex[indexPosition : indexPosition+12]
		if entry.getVbNo() == vbNo {
			if entry.getSequence() == sequence {
				// Found, return location information
				return indexPosition, entryPosition, e.getEntryLen()
			} else if entry.getSequence() > sequence {
				// Not found (reached sequence greater than the targeted value, for the vbucket)
				return 0, 0, 0
			}
		}
		pos += INDEX_ENTRY_LEN // Move to next index entry
		entryPos += entry.getEntryLen()
	}

	// Not found, return 0
	return 0, 0, 0
}

// DenseBlockIndexEntry format
//  | Name      | Size     | Description                      |
//  |-----------|----------|----------------------------------|
//  | vbno      | 2 bytes  | Vbucket number                   |
//  | sequence  | 8 bytes  | Vbucket seq                      |
//  | entryLen  | 2 bytes  | Length of associated block entry |
//  -----------------------------------------------------------
type DenseBlockIndexEntry []byte

func (e *DenseBlockIndexEntry) getVbNo() uint16 {
	return binary.BigEndian.Uint16(e[0:2])
}

func (e *DenseBlockIndexEntry) setVbNo(vbno uint16) {
	binary.BigEndian.PutUint16(e[0:2], vbNo)
}

func (e *DenseBlockIndexEntry) getSequence() uint64 {
	return binary.BigEndian.Uint64(e[2:10])
}

func (e *DenseBlockIndexEntry) setSequence(vbno uint64) {
	binary.BigEndian.PutUint64(e[2:10], vbNo)
}

func (e *DenseBlockIndexEntry) getEntryLen() uint16 {
	return binary.BigEndian.Uint16(e[10:12])
}

func (e *DenseBlockIndexEntry) setEntryLen(entryLen uint16) {
	binary.BigEndian.PutUint16(e[10:12], entryLen)
}

const INDEX_ENTRY_LEN = 12

func NewDenseBlockIndexEntry(vbno uint16, sequence uint64, entryLen uint16) DenseBlockIndexEntry {
	indexEntry := make(DenseBlockIndexEntry, INDEX_ENTRY_LEN)
	indexEntry.SetVbNo(vbno)
	indexEntry.SetSequence(sequence)
	indexEntry.SetEntryLen(entryLen)
	return indexEntry
}

// DenseBlockEntry format
//  | Name      | Size     | Description                      |
//  |-----------|----------|----------------------------------|
//  | flags     | 1 byte   | Flags (deleted, removed, etc)    |
//  | keylen    | 2 bytes  | Length of key                    |
//  | key       | n bytes  | Key                              |
//  | revid     | n bytes  | Revision id                      |
//  -----------------------------------------------------------
type DenseBlockEntry []byte

func NewDenseBlockEntry(docID, revID string, flags uint8) DenseBlockEntry {
	keyBytes := []byte(docID)
	revBytes := []byte(revID)
	entryLen := 3 + len(keyBytes) + len(revBytes)
	entry := make(DenseBlockEntry, entryLen)
	entry[0] = flags                                              // Flags
	binary.BigEndian.PutUint16(entry[1:3], uint16(len(keyBytes))) // KeyLen
	copy(entry[3:3+len(keyBytes)], keyBytes)                      // Key
	copy(entry[3+len(keyBytes):], revBytes)                       // Rev
	return entry
}

/*
func (e *DenseBlockEntry) encode() []byte {

	data := make([]byte, e.Size())
	pos := 0
	// Write fixed length data
	binary.BigEndian.PutUint16(data[0:2], e.vbno)
	binary.BigEndian.PutUint64(data[2:10], e.sequence)
	data[11] = byte(e.flags)
	binary.BigEndian.PutUint16(data[11:13], len(e.docId))
	binary.BigEndian.PutUint16(data[13:15], len(e.revId))

	// Write variable length data
	docIdEnd := 15 + len(e.docId)
	copy(data[15:docIdEnd], e.docId)
	copy(data[docIdEnd:docIdEnd+len(e.revId)], e.revId)
	return data
}

func UnmarshalDenseBlockEntry(value []byte) (*DenseBlockEntry, error) {

}
*/
