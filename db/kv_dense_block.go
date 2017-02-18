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
	"bytes"
	"encoding/binary"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

var MaxBlockSize = 10000 // Maximum size of index block, in bytes

// DenseBlock stores a collection of LogEntries for a channel.  Entries which are added to a DenseBlock are
// appended to existing entries.  A DenseBlock is considered 'full' when the size of the block exceeds
// MaxBlockSize.
// Underlying storage for DenseBlock is the raw []byte, composed of three high-level sections:
//  | Name               | Size                  | Description                                     |
//  |--------------------|-----------------------|-------------------------------------------------|
//  | entry count        | 2 bytes               | Number of entries in block                      |
//  | index              | 12 bytes/entry        | List of vb, seq and length for entries in block |
//  | entries            | variable length/entry | Key, rev id and flags for each entry            |
//  ------------------------------------------------------------------------------------------------
// When an entry is added to the block, a new index entry is added to the index to store the vb/seq,
// a new entry is added to entries to store key/revId/flags, and entry count is incremented.

const DB_HEADER_LEN = 2 // Length of block header.  Currently only contains entry count

type DenseBlock struct {
	Key        string              // Key of block document in the index bucket
	value      []byte              // Binary storage of block data, in the above format
	cas        uint64              // Document cas
	clock      base.PartitionClock // Highest seq per vbucket written to the block
	startClock base.PartitionClock // Starting clock for the block (partition clock for all previous blocks)
}

func NewDenseBlock(key string, startClock base.PartitionClock) *DenseBlock {

	// Set initial capacity of value to handle ~5 docs (depending on key length) - avoids a lot of
	// alloc overhead when the first few entries in the channel are appended (since append only
	// doubles capacity as needed).
	// Initial length of value is set to 2, to initialize the entry count to zero.
	return &DenseBlock{
		Key:        key,
		value:      make([]byte, DB_HEADER_LEN, 400),
		startClock: startClock,
	}
}

func (d *DenseBlock) Count() uint16 {
	return d.getEntryCount()
}

func (d *DenseBlock) getEntryCount() uint16 {
	return binary.BigEndian.Uint16(d.value[0:2])
}

func (d *DenseBlock) setEntryCount(count uint16) {
	binary.BigEndian.PutUint16(d.value[0:2], count)
}

func (d *DenseBlock) getClock() base.PartitionClock {
	if d.clock == nil {
		d.initClock()
	}
	return d.clock
}

// Get CumulativeClock returns the full clock for the partition:
// the starting clock for this block, plus any changes made in this block
func (d *DenseBlock) getCumulativeClock() base.PartitionClock {
	cumulativeClock := d.startClock.Copy()
	cumulativeClock.Set(d.clock)
	return cumulativeClock
}

func (d *DenseBlock) loadBlock(bucket base.Bucket) error {
	value, cas, err := bucket.GetRaw(d.Key)
	if err != nil {
		return err
	}
	d.value = value
	d.cas = cas
	d.clock = nil
	IndexExpvars.Add("indexReader.blocksLoaded", 1)
	return nil
}

// Initializes PartitionClock - called on first use of block clock.
func (d *DenseBlock) initClock() {
	// Initialize clock
	d.clock = d.startClock.Copy()

	var indexEntry DenseBlockIndexEntry
	numEntries := d.getEntryCount()
	for i := 0; i < int(numEntries); i++ {
		indexEntry = d.value[DB_HEADER_LEN+i*INDEX_ENTRY_LEN : DB_HEADER_LEN+(i+1)*INDEX_ENTRY_LEN]
		d.clock[indexEntry.getVbNo()] = indexEntry.getSequence()
	}
}

// Adds entries to block and writes block to the bucket
func (d *DenseBlock) AddEntrySet(entries []*LogEntry, bucket base.Bucket) (overflow []*LogEntry, pendingRemoval []*LogEntry, updateClock base.PartitionClock, casFailure bool, err error) {

	casFailure = false
	// Check if block is already full.  If so, return all entries as overflow.
	if len(d.value) > MaxBlockSize {
		base.LogTo("ChannelStorage+", "Block full - returning entries as overflow.  #entries:[%d]", len(entries))
		return entries, pendingRemoval, nil, casFailure, nil
	}

	overflow, pendingRemoval, updateClock, addError := d.addEntries(entries)
	if addError != nil {
		// Error adding entries - reset the block and return error
		base.LogTo("ChannelStorage+", "Error adding entries to block. %v", err)
		d.loadBlock(bucket)
		return nil, nil, nil, casFailure, addError
	}

	casOut, err := bucket.WriteCas(d.Key, 0, 0, d.cas, d.value, sgbucket.Raw)
	if err != nil {
		casFailure = true
		base.LogTo("ChannelStorage+", "CAS error writing block to database. %v", err)
		return entries, []*LogEntry{}, nil, casFailure, nil
	}

	d.cas = casOut
	base.LogTo("ChannelStorage+", "Successfully added set to block. key:[%s] #added:[%d] #overflow:[%d] #pendingRemoval:[%d]",
		d.Key, len(entries)-len(overflow), len(overflow), len(pendingRemoval))
	return overflow, pendingRemoval, updateClock, casFailure, nil
}

// Adds a set of log entries to a block.  Returns:
//  overflow        Entries that didn't fit in the block
//  pendingRemoval  Entries with a parent that needs to be removed from the index,
//                  but the parent isn't in this block
func (d *DenseBlock) addEntries(entries []*LogEntry) (overflow []*LogEntry, pendingRemoval []*LogEntry, updateClock base.PartitionClock, err error) {

	blockFull := false
	addCount := 0
	partitionClock := make(base.PartitionClock)
	for i, entry := range entries {
		if !blockFull {
			added, removalRequired, err := d.addEntry(entry)
			if err != nil {
				base.LogTo("ChannelStorage+", "Error adding entry to block.  key:[%s] error:%v", entry.DocID, err)
				return nil, nil, nil, err
			}
			if added {
				addCount++
			}
			partitionClock.SetSequence(entry.VbNo, entry.Sequence)
			if removalRequired {
				if pendingRemoval == nil {
					pendingRemoval = make([]*LogEntry, 0)
				}
				pendingRemoval = append(pendingRemoval, entry)
			}
			if len(d.value) > MaxBlockSize {
				blockFull = true
			}
		} else {
			overflow = entries[i:]
			break
		}

	}
	return overflow, pendingRemoval, partitionClock, nil
}

// Adds a LogEntry to the block.  If the entry already exists in the block (new rev of existing doc),
// handles removal
func (d *DenseBlock) addEntry(logEntry *LogEntry) (added bool, removalRequired bool, err error) {

	if logEntry == nil {
		return false, false, fmt.Errorf("LogEntry must be non-nil")
	}

	// Ensure this entry hasn't already been written by another writer
	clockSequence := d.getClock()[logEntry.VbNo]
	if logEntry.Sequence <= clockSequence {
		base.LogTo("ChannelStorage+", "Index already has entries later than or matching sequence - skipping.  key:[%s] seq:[%d] index_seq[%d] blockKey:[%s]",
			logEntry.DocID, logEntry.Sequence, clockSequence, d.Key)
		return false, false, nil
	}

	// Encode log entry as index and entry portions
	entryBytes := NewDenseBlockDataEntry(logEntry.DocID, logEntry.RevID, logEntry.Flags)
	indexBytes := NewDenseBlockIndexEntry(logEntry.VbNo, logEntry.Sequence, uint16(len(entryBytes)))

	// If this is a new addition to the channel, don't need to remove previous entry
	if logEntry.Flags&channels.Added != 0 {
		err := d.appendEntry(indexBytes, entryBytes)
		if err != nil {
			return false, removalRequired, err
		}
	} else {
		// Entry already exists in the channel - remove previous entry if present in this block.  Sequence-based
		// removal when available, otherwise search by key
		var oldIndexPos, oldEntryPos uint32
		var oldEntryLen uint16
		if logEntry.PrevSequence != 0 {
			oldIndexPos, oldEntryPos, oldEntryLen = d.findEntry(logEntry.VbNo, logEntry.PrevSequence)
		} else {
			var oldSeq uint64
			oldIndexPos, oldEntryPos, oldEntryLen, oldSeq = d.findEntryByKey(logEntry.VbNo, []byte(logEntry.DocID))
			if oldSeq == logEntry.Sequence {
				oldIndexPos = 0
			}
		}
		// If found, replace entry in this block.  Otherwise append new entry and
		// return flag for removal from older block.
		if oldIndexPos > 0 {
			d.replaceEntry(oldIndexPos, oldEntryPos, oldEntryLen, indexBytes, entryBytes)
		} else {
			if err := d.appendEntry(indexBytes, entryBytes); err != nil {
				return false, removalRequired, err
			}
			removalRequired = true
		}
	}
	d.getClock()[logEntry.VbNo] = logEntry.Sequence
	return true, removalRequired, err
}

// Attempts to remove entries from the block
func (d *DenseBlock) RemoveEntrySet(entries []*LogEntry, bucket base.Bucket) (pendingRemoval []*LogEntry, err error) {

	pendingRemoval = d.removeEntries(entries)
	// If nothing was removed, don't update the block
	if len(pendingRemoval) == len(entries) {
		return entries, nil
	}

	casOut, writeErr := base.WriteCasRaw(bucket, d.Key, d.value, d.cas, 0, func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times
		d.value = value
		d.clock = nil
		pendingRemoval = d.removeEntries(entries)

		// If nothing was removed, cancel the write
		if len(pendingRemoval) == len(entries) {
			return nil, nil
		}
		return d.value, nil
	})
	if writeErr != nil {
		base.LogTo("ChannelStorage+", "Error writing block to database. %v", err)
		return entries, writeErr
	}
	d.cas = casOut
	if len(pendingRemoval) != len(entries) {
		base.LogTo("ChannelStorage+", "Successfully removed set from block. key:[%s] #removed:[%d] #pending:[%d]",
			d.Key, len(entries)-len(pendingRemoval), len(pendingRemoval))
	}
	return pendingRemoval, nil
}

// Removes all entries greater than vb, seq from the block.  Returns rollbackComplete=true if it finds a seq
// in the block for the vbucket where seq <= rollbackSeq
func (d *DenseBlock) RollbackTo(rollbackVbNo uint16, rollbackSeq uint64, bucket base.Bucket) (rollbackComplete bool, err error) {

	numRemoved := 0
	numRemoved, rollbackComplete = d.rollbackEntries(rollbackVbNo, rollbackSeq)

	// If nothing was removed, don't update the block
	if numRemoved == 0 {
		return rollbackComplete, nil
	}

	casOut, writeErr := base.WriteCasRaw(bucket, d.Key, d.value, d.cas, 0, func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times
		d.value = value
		d.clock = nil
		numRemoved, rollbackComplete = d.rollbackEntries(rollbackVbNo, rollbackSeq)

		// If nothing was removed, cancel the write
		if numRemoved == 0 {
			return nil, nil
		}
		return d.value, nil
	})
	if writeErr != nil {
		base.LogTo("ChannelStorage+", "Error writing block to database. %v", err)
		return false, writeErr
	}
	d.cas = casOut
	if numRemoved > 0 {
		base.LogTo("ChannelStorage+", "Successfully removed entries from block during rollback. key:[%s] #removed:[%d] complete?:[%v]",
			d.Key, numRemoved, rollbackComplete)
	}
	return rollbackComplete, nil
}

// MarkInactive - apply any changes required when block stops being the active block
func (d *DenseBlock) MarkInactive() error {
	// TODO: set a flag on the block to indicate it's inactive, for concurrency purposes?
	return nil

}

// Attempt to remove entries from the block.  Return any entries not found in the block.
func (d *DenseBlock) removeEntries(entries []*LogEntry) []*LogEntry {
	// Note: need to store 'notRemoved' as a separate slice, instead of modifying entries, since we
	// may need to replay removal on a cas retry
	notRemoved := make([]*LogEntry, 0)
	for _, entry := range entries {
		var oldIndexPos, oldEntryPos uint32
		var oldEntryLen uint16
		if entry.PrevSequence != 0 {
			oldIndexPos, oldEntryPos, oldEntryLen = d.findEntry(entry.VbNo, entry.PrevSequence)
		} else {
			var oldSeq uint64
			oldIndexPos, oldEntryPos, oldEntryLen, oldSeq = d.findEntryByKey(entry.VbNo, []byte(entry.DocID))
			if oldSeq == entry.Sequence {
				// Safety check - don't remove if the returned sequence equals the sequence we're adding
				oldIndexPos = 0
			}
		}
		if oldIndexPos > 0 {
			d.removeEntry(oldIndexPos, oldEntryPos, oldEntryLen)
		} else {
			notRemoved = append(notRemoved, entry)
		}
	}
	return notRemoved

}

// Rollback removes any entries in the block for the vbucket greater than the specified sequence
func (d *DenseBlock) rollbackEntries(vbNo uint16, seq uint64) (numRemoved int, rollbackComplete bool) {

	// Work backwards through the block, removing entries greater than vbNo, seq
	indexPos := DB_HEADER_LEN + uint32(d.getEntryCount()-1)*INDEX_ENTRY_LEN
	entryPos := uint32(len(d.value))

	for {
		indexEntry := d.GetIndexEntry(int64(indexPos))
		entryPos -= uint32(indexEntry.getEntryLen())
		if indexEntry.getVbNo() == vbNo {
			if indexEntry.getSequence() > seq {
				d.removeEntry(indexPos, entryPos, indexEntry.getEntryLen())
				entryPos -= INDEX_ENTRY_LEN // Move entryPos to account for the removed index entry
				numRemoved++
			} else {
				rollbackComplete = true
				break
			}
		}

		// Move to previous
		if indexPos <= DB_HEADER_LEN {
			// Reached the beginning of the block, need to continue to the next block
			rollbackComplete = false
			break
		}
		indexPos -= INDEX_ENTRY_LEN
	}

	return numRemoved, rollbackComplete
}

// removeEntry
//  Cuts an entry from the block,
func (d *DenseBlock) removeEntry(oldIndexPos, oldEntryPos uint32, oldEntryLen uint16) {

	if len(d.value) < int(oldEntryPos)+int(oldEntryLen) {
		base.Warn(fmt.Sprintf("Attempted to remove entry in invalid block, len=%d, old entry pos=%d, old entry len=%d", len(d.value), oldEntryPos, oldEntryLen))
		return
	}

	// Cut entry
	d.value = append(d.value[:oldEntryPos], d.value[oldEntryPos+uint32(oldEntryLen):]...)
	// Cut index entry
	d.value = append(d.value[:oldIndexPos], d.value[oldIndexPos+INDEX_ENTRY_LEN:]...)

	d.decrEntryCount(1)
}

// AppendEntry
//  Increments the entry count of the block,
//  appends the provided indexBytes to the index portion of the block, and
//  appends the provided entryBytes to the entries portion of the block.
func (d *DenseBlock) appendEntry(indexBytes, entryBytes []byte) error {

	if len(d.value) < DB_HEADER_LEN {
		return fmt.Errorf("Attempted to append entry to invalid block, len=%d", len(d.value))
	}

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

	endOfIndex := DB_HEADER_LEN + (newCount-1)*INDEX_ENTRY_LEN

	//  Shift all entries:
	// |n|oldIndex|oldEntries|newEntry|newIndexEntry| -> |n|oldIndex|oldEntrieoldEntries|newEntry|
	copy(d.value[endOfIndex+INDEX_ENTRY_LEN:], d.value[endOfIndex:])

	//  Insert index entry:
	// |n|oldIndex|oldEntrieoldEntries|newEntry| -> |n|oldIndex|newIndex|oldEntries|newEntry|
	copy(d.value[endOfIndex:endOfIndex+INDEX_ENTRY_LEN], indexBytes)

	return nil
}

// Attempts to find the specified [vb, seq] in the block index.  Returns index position, entry position and
// entry length when found.  Returns zeroes when not found
func (d *DenseBlock) findEntry(vbNo uint16, seq uint64) (indexPos, entryPos uint32, entryLength uint16) {

	// Skip search if the targeted sequence is earlier than the start clock value for the vb
	if d.startClock != nil {
		startSeq, ok := d.startClock[vbNo]
		if ok && startSeq >= seq {
			return 0, 0, 0
		}
	}

	// Iterate through the index, looking for the entry
	indexEnd := DB_HEADER_LEN + INDEX_ENTRY_LEN*uint32(d.getEntryCount())
	indexPos = DB_HEADER_LEN

	if len(d.value) < int(indexEnd) {
		base.Warn(fmt.Sprintf("Attempted to find entry to invalid block, len=%d", len(d.value)))
		return 0, 0, 0
	}
	entryPos = indexEnd
	var indexEntry DenseBlockIndexEntry
	for indexPos < indexEnd {
		indexEntry = d.value[indexPos : indexPos+INDEX_ENTRY_LEN]
		if indexEntry.getVbNo() == vbNo {
			if indexEntry.getSequence() == seq {
				return indexPos, entryPos, indexEntry.getEntryLen()
			} else if indexEntry.getSequence() > seq {
				// Not found (reached sequence greater than the targeted value, for the vbucket)
				return 0, 0, 0
			}
		}
		indexPos += INDEX_ENTRY_LEN // Move to next index entry
		entryPos += uint32(indexEntry.getEntryLen())
	}

	// Not found
	return 0, 0, 0
}

// Attempts to find the specified [vb, key] in the block.  Iterates through the index, looking up key for
// any vb matches
func (d *DenseBlock) findEntryByKey(vbNo uint16, key []byte) (indexPos, entryPos uint32, entryLength uint16, seq uint64) {
	indexEnd := DB_HEADER_LEN + INDEX_ENTRY_LEN*uint32(d.getEntryCount())
	if len(d.value) < int(indexEnd) {
		base.Warn(fmt.Sprintf("Attempted to find entry by key in invalid block, len=%d", len(d.value)))
		return 0, 0, 0, 0
	}
	indexPos = DB_HEADER_LEN
	entryPos = indexEnd
	var indexEntry DenseBlockIndexEntry
	var currentEntry DenseBlockDataEntry
	for indexPos < indexEnd {
		indexEntry = d.value[indexPos : indexPos+INDEX_ENTRY_LEN]
		if indexEntry.getVbNo() == vbNo {
			currentEntry = d.value[entryPos : entryPos+uint32(indexEntry.getEntryLen())]
			if bytes.Equal(currentEntry.getDocId(), key) {
				// Found, return location information
				return indexPos, entryPos, indexEntry.getEntryLen(), indexEntry.getSequence()
			}
		}
		indexPos += INDEX_ENTRY_LEN // Move to next index entry
		entryPos += uint32(indexEntry.getEntryLen())
	}

	// Not found
	return 0, 0, 0, 0
}

// ReplaceEntry.  Replaces the existing entry with the specified index and entry positions/length with the new
// entry described by indexBytes, entryBytes.  Used to replace a previous revision of a document in the cache with a minimum of slice
// manipulation.
func (d *DenseBlock) replaceEntry(oldIndexPos, oldEntryPos uint32, oldEntryLen uint16, indexBytes, entryBytes []byte) {

	// Shift and insert index entry
	endOfIndex := DB_HEADER_LEN + uint32(INDEX_ENTRY_LEN)*uint32(d.getEntryCount())

	if len(d.value) < int(endOfIndex) {
		base.Warn(fmt.Sprintf("Attempted to replace entry in invalid block, len=%d", len(d.value)))
		return
	}
	// Replace index.
	//  1. Unless oldIndexPos is the last entry in the index, shift index entries:
	if oldIndexPos+uint32(INDEX_ENTRY_LEN) < endOfIndex {
		copy(d.value[oldIndexPos:], d.value[oldIndexPos+INDEX_ENTRY_LEN:endOfIndex])
	}
	// 2. Replace last index entry
	copy(d.value[endOfIndex-INDEX_ENTRY_LEN:endOfIndex], indexBytes)

	// Replace entry
	//  1. Cut the previous entry
	d.value = append(d.value[:oldEntryPos], d.value[oldEntryPos+uint32(oldEntryLen):]...)
	//  2. Append the new entry
	d.value = append(d.value, entryBytes...)
}

// Increments the entry count
func (d *DenseBlock) incrEntryCount(amount uint16) (uint16, error) {
	count := d.getEntryCount()

	// Check for uint16 overflow
	if count+amount < count {
		base.Warn("Overflow incrementing entry count (%s): %d, %d", d.Key, count, amount)
		return 0, fmt.Errorf("Maximum block entry count exceeded")
	}
	d.setEntryCount(count + amount)
	return count + amount, nil
}

// Decrements the entry count
func (d *DenseBlock) decrEntryCount(amount uint16) (uint16, error) {
	count := d.getEntryCount()
	if amount > count {
		base.Warn("Cannot decrement entry count below zero (%s): %d, %d", d.Key, count, amount)
		return 0, fmt.Errorf("Can't decrement block entry count below zero")
	}
	d.setEntryCount(count - amount)
	return count - amount, nil
}

func (d *DenseBlock) GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string) {
	return nil, nil
}

func (d *DenseBlock) GetAllEntries() []*LogEntry {
	count := d.getEntryCount()
	entries := make([]*LogEntry, count)
	entryPos := uint32(DB_HEADER_LEN + count*INDEX_ENTRY_LEN)
	var indexEntry DenseBlockIndexEntry
	var entry DenseBlockDataEntry
	for i := uint16(0); i < count; i++ {
		indexEntry = d.GetIndexEntry(int64(DB_HEADER_LEN + i*INDEX_ENTRY_LEN))
		entry = d.GetEntry(int64(entryPos), indexEntry.getEntryLen())
		entries[i] = d.MakeLogEntry(indexEntry, entry)
		entryPos += uint32(indexEntry.getEntryLen())
	}

	return entries
}

func (d *DenseBlock) MakeLogEntry(indexEntry DenseBlockIndexEntry, entry DenseBlockDataEntry) *LogEntry {
	return &LogEntry{
		VbNo:     indexEntry.getVbNo(),
		Sequence: indexEntry.getSequence(),
		DocID:    string(entry.getDocId()),
		RevID:    string(entry.getRevId()),
		Flags:    entry.getFlags(),
	}
}

func (d *DenseBlock) GetIndexEntry(position int64) (indexEntry DenseBlockIndexEntry) {
	indexEntry = d.value[position : position+INDEX_ENTRY_LEN]
	return indexEntry
}

func (d *DenseBlock) GetEntry(position int64, length uint16) (entry DenseBlockDataEntry) {
	entry = d.value[position : position+int64(length)]
	return entry
}

type DenseBlockEntry struct {
	DenseBlockIndexEntry
	DenseBlockDataEntry
}

func (d *DenseBlockEntry) MakeLogEntry() *LogEntry {
	if d.DenseBlockIndexEntry == nil || d.DenseBlockDataEntry == nil {
		return nil
	}
	return &LogEntry{
		VbNo:     d.getVbNo(),
		Sequence: d.getSequence(),
		DocID:    string(d.getDocId()),
		RevID:    string(d.getRevId()),
		Flags:    d.getFlags(),
	}
}

// Used when the doc id has already been converted to string, to avoid performance overhead
func (d *DenseBlockEntry) MakeLogEntryWithDocId(docId string) *LogEntry {
	if d.DenseBlockIndexEntry == nil || d.DenseBlockDataEntry == nil {
		return nil
	}
	return &LogEntry{
		VbNo:     d.getVbNo(),
		Sequence: d.getSequence(),
		DocID:    docId,
		RevID:    string(d.getRevId()),
		Flags:    d.getFlags(),
	}
}

// DenseBlockIndexEntry is a helper class for interacting with entries in the index portion of a DenseBlock.
// NewDenseBlockIndexEntry(...) should be used for creating new entries; work with existing entries by targeting
// a slice withing the DenseBlock.
//  | Name      | Size     | Description                      |
//  |-----------|----------|----------------------------------|
//  | vbno      | 2 bytes  | Vbucket number                   |
//  | sequence  | 8 bytes  | Vbucket seq                      |
//  | entryLen  | 2 bytes  | Length of associated block entry |
//  -----------------------------------------------------------
type DenseBlockIndexEntry []byte

func NewDenseBlockIndexEntry(vbno uint16, sequence uint64, entryLen uint16) DenseBlockIndexEntry {
	indexEntry := make(DenseBlockIndexEntry, INDEX_ENTRY_LEN)
	indexEntry.setVbNo(vbno)
	indexEntry.setSequence(sequence)
	indexEntry.setEntryLen(entryLen)
	return indexEntry
}

func (e DenseBlockIndexEntry) getVbNo() uint16 {
	return binary.BigEndian.Uint16(e[0:2])
}

func (e DenseBlockIndexEntry) setVbNo(vbNo uint16) {
	binary.BigEndian.PutUint16(e[0:2], vbNo)
}

func (e DenseBlockIndexEntry) getSequence() uint64 {
	return binary.BigEndian.Uint64(e[2:10])
}

func (e DenseBlockIndexEntry) setSequence(vbNo uint64) {
	binary.BigEndian.PutUint64(e[2:10], vbNo)
}

func (e DenseBlockIndexEntry) getEntryLen() uint16 {
	return binary.BigEndian.Uint16(e[10:12])
}

func (e DenseBlockIndexEntry) setEntryLen(entryLen uint16) {
	binary.BigEndian.PutUint16(e[10:12], entryLen)
}

const INDEX_ENTRY_LEN = 12

// DenseBlockDataEntry - a single doc entry within a block.  Stores key, revId, and flags.
// Storage format:
//  |-----------|----------|----------------------------------|
//  | flags     | 1 byte   | Flags (deleted, removed, etc)    |
//  | keylen    | 2 bytes  | Length of key                    |
//  | key       | n bytes  | Key                              |
//  | revid     | n bytes  | Revision id                      |
//  -----------------------------------------------------------
//  We don't store rev id length - it's derived from the entryLen stored in the DenseBlockEntryIndex.
type DenseBlockDataEntry []byte

const DENSE_BLOCK_ENTRY_FIXED_LEN = 3 // Length of fixed length components (flags, keylen)

func NewDenseBlockDataEntry(docId, revId string, flags uint8) DenseBlockDataEntry {
	keyBytes := []byte(docId)
	revBytes := []byte(revId)
	entryLen := DENSE_BLOCK_ENTRY_FIXED_LEN + len(keyBytes) + len(revBytes)
	entry := make(DenseBlockDataEntry, entryLen)
	entry[0] = flags                                                                             // Flags
	binary.BigEndian.PutUint16(entry[1:3], uint16(len(keyBytes)))                                // KeyLen
	copy(entry[DENSE_BLOCK_ENTRY_FIXED_LEN:DENSE_BLOCK_ENTRY_FIXED_LEN+len(keyBytes)], keyBytes) // Key
	copy(entry[DENSE_BLOCK_ENTRY_FIXED_LEN+len(keyBytes):], revBytes)                            // Rev
	return entry
}

func (e DenseBlockDataEntry) getDocId() []byte {
	return e[DENSE_BLOCK_ENTRY_FIXED_LEN : DENSE_BLOCK_ENTRY_FIXED_LEN+e.getKeyLen()]
}
func (e DenseBlockDataEntry) getRevId() []byte {
	return e[DENSE_BLOCK_ENTRY_FIXED_LEN+e.getKeyLen():]
}
func (e DenseBlockDataEntry) getFlags() uint8 {
	return e[0]
}
func (e DenseBlockDataEntry) getKeyLen() uint16 {
	return binary.BigEndian.Uint16(e[1:3])
}

// DenseBlockIterator - manages iteration over the contents of a block by storing
// pointer to index and entry locations
type DenseBlockIterator struct {
	block    *DenseBlock
	indexPtr int64 // Current position in block index
	entryPtr int64 // Current position in block entries
}

func NewDenseBlockIterator(block *DenseBlock) *DenseBlockIterator {
	reader := DenseBlockIterator{
		block: block,
	}
	reader.indexPtr = DB_HEADER_LEN
	reader.entryPtr = DB_HEADER_LEN + int64(block.getEntryCount())*INDEX_ENTRY_LEN
	return &reader
}

// Returns current entry in the block, and moves pointers to the next entry.
// Returns nil when at the end of the block
func (r *DenseBlockIterator) next() *DenseBlockEntry {
	if r.indexPtr >= DB_HEADER_LEN+int64(r.block.getEntryCount())*INDEX_ENTRY_LEN {
		return nil
	}
	indexEntry := r.block.GetIndexEntry(r.indexPtr)
	entry := r.block.GetEntry(r.entryPtr, indexEntry.getEntryLen())
	r.indexPtr += INDEX_ENTRY_LEN
	r.entryPtr += int64(indexEntry.getEntryLen())
	return &DenseBlockEntry{
		DenseBlockDataEntry:  entry,
		DenseBlockIndexEntry: indexEntry,
	}
}

// Sets pointers to the last entry in the block
func (r *DenseBlockIterator) end() {
	r.indexPtr = DB_HEADER_LEN + int64(r.block.getEntryCount())*INDEX_ENTRY_LEN
	r.entryPtr = int64(len(r.block.value))
}

// Returns entry preceding the pointers, and moves pointers back.
// Returns nil when at the start of the block
func (r *DenseBlockIterator) previous() *DenseBlockEntry {
	if r.indexPtr <= DB_HEADER_LEN {
		return nil
	}
	// Move pointers
	r.indexPtr -= INDEX_ENTRY_LEN
	indexEntry := r.block.GetIndexEntry(r.indexPtr)
	r.entryPtr -= int64(indexEntry.getEntryLen())
	entry := r.block.GetEntry(r.entryPtr, indexEntry.getEntryLen())
	return &DenseBlockEntry{
		DenseBlockDataEntry:  entry,
		DenseBlockIndexEntry: indexEntry,
	}
}
