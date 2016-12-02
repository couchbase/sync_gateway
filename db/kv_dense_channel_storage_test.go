package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbaselabs/go.assert"
)

const (
	IsAdded      = true
	IsNotAdded   = false
	IsRemoval    = true
	IsNotRemoval = false
)

func makeBlockEntry(docId string, revId string, vbNo int, sequence int, removal bool, added bool) *LogEntry {
	entry := makeEntryForDoc(docId, revId, vbNo, sequence, removal)
	if added {
		entry.Flags |= channels.Added
	}
	return entry
}

func assertLogEntry(t *testing.T, entry *LogEntry, docId string, revId string, vbNo int, sequence int) {
	assertTrue(t, entry.DocID == docId, fmt.Sprintf("Doc ID mismatch.  Expected [%s] Actual [%s]", docId, entry.DocID))
	assertTrue(t, entry.RevID == revId, fmt.Sprintf("Rev ID mismatch.  Expected [%s] Actual [%s]", revId, entry.RevID))
	assertTrue(t, entry.VbNo == uint16(vbNo), fmt.Sprintf("VbNo mismatch.  Expected [%d] Actual [%d]", vbNo, entry.VbNo))
	assertTrue(t, entry.Sequence == uint64(sequence), fmt.Sprintf("VbNo mismatch.  Expected [%d] Actual [%d]", sequence, entry.Sequence))
}

func assertLogEntriesEqual(t *testing.T, actualEntry *LogEntry, expectedEntry *LogEntry) {
	assertLogEntry(t, actualEntry, expectedEntry.DocID, expectedEntry.RevID, int(expectedEntry.VbNo), int(expectedEntry.Sequence))
}

// -----------------
// Dense Block Tests
// -----------------
func TestDenseBlockSingleDoc(t *testing.T) {

	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Simple insert
	entries := make([]*LogEntry, 1)
	entries[0] = makeBlockEntry("doc1", "1-abc", 50, 1, IsNotRemoval, IsAdded)

	overflow, pendingRemoval, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "1-abc", 50, 1)

	// Update within the same partition block, deduplicate by id
	entries[0] = makeBlockEntry("doc1", "2-abc", 50, 3, IsNotRemoval, IsNotAdded)

	overflow, pendingRemoval, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "2-abc", 50, 3)

	// Update within the same partition block, deduplicate by sequence
	entries[0] = makeBlockEntry("doc1", "3-abc", 50, 5, IsNotRemoval, IsNotAdded)
	entries[0].PrevSequence = uint64(3)

	overflow, pendingRemoval, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "3-abc", 50, 5)
}

func TestDenseBlockMultipleInserts(t *testing.T) {
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Inserts
	entries := make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "1-abc", i*10, i+1, IsNotRemoval, IsAdded)
	}
	overflow, pendingRemoval, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, block.getEntryCount(), uint16(10))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "1-abc", i*10, i+1)
	}

}

func TestDenseBlockMultipleUpdates(t *testing.T) {
	base.EnableLogKey("ChannelStorage")
	base.EnableLogKey("ChannelStorage+")
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Inserts
	entries := make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		vbno := 10*i + 1
		sequence := i + 1
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "1-abc", vbno, sequence, IsNotRemoval, IsAdded)
	}
	overflow, pendingRemoval, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, block.getEntryCount(), uint16(10))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		vbno := 10*i + 1
		sequence := i + 1
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "1-abc", vbno, sequence)
	}

	// Updates
	entries = make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		vbno := 10*i + 1
		sequence := i + 21
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "2-abc", vbno, sequence, IsNotRemoval, IsNotAdded)
		entries[i].PrevSequence = uint64(i + 1)
	}
	overflow, pendingRemoval, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 10)

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "2-abc", 10*i+1, 21+i)
	}

}

func TestDenseBlockOverflow(t *testing.T) {
	base.EnableLogKey("ChannelStorage")
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Insert 100 entries, no overflow
	entries := make([]*LogEntry, 100)
	for i := 0; i < 100; i++ {
		vbno := 100
		sequence := i + 1
		entries[i] = makeBlockEntry(fmt.Sprintf("longerDocumentID-%d", sequence), "1-abcdef01234567890", vbno, sequence, IsNotRemoval, IsAdded)
	}
	overflow, pendingRemoval, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 100)

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 100)
	for i := 0; i < 100; i++ {
		assertLogEntriesEqual(t, foundEntries[i], entries[i])
	}

	// Insert 100 more entries, expect overflow.  Based on this test's doc/rev ids, expect to fit 188 entries in
	// the default block size.
	entries = make([]*LogEntry, 100)
	for i := 0; i < 100; i++ {
		vbno := 100
		sequence := i + 101
		entries[i] = makeBlockEntry(fmt.Sprintf("longerDocumentID-%d", sequence), "1-abcdef01234567890", vbno, sequence, IsNotRemoval, IsAdded)
	}
	overflow, pendingRemoval, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 12)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 188)
	assert.Equals(t, len(block.value), 10046)

	// Validate overflow contents (last 12 entries)
	for i := 0; i < 12; i++ {
		assertLogEntriesEqual(t, overflow[i], entries[i+88])
	}

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 188)
	for i := 0; i < 188; i++ {
		vbno := 100
		sequence := i + 1
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("longerDocumentID-%d", sequence), "1-abcdef01234567890", vbno, sequence)
	}

	// Retry the 12 entries, all should overflow
	var newOverflow []*LogEntry
	newOverflow, pendingRemoval, err = block.AddEntrySet(overflow, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(newOverflow), 12)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 188)
	assert.Equals(t, len(block.value), 10046)

}

// CAS handling test
func TestDenseBlockConcurrentUpdates(t *testing.T) {

	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Simple insert
	entries := make([]*LogEntry, 1)
	entries[0] = makeBlockEntry("doc1", "1-abc", 50, 1, IsNotRemoval, IsAdded)

	overflow, pendingRemoval, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "1-abc", 50, 1)
	log.Println("Wrote doc as block1")

	// Initialize a second instance of the block (simulates multiple writers), write a doc.
	// Expects cas failure on first write, success on subsequent.
	block2 := NewDenseBlock("block1", nil)
	entries2 := make([]*LogEntry, 1)
	entries2[0] = makeBlockEntry("doc2", "1-abc", 50, 3, IsNotRemoval, IsAdded)

	overflow2, pendingRemoval2, err := block2.AddEntrySet(entries2, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow2), 0)
	assert.Equals(t, len(pendingRemoval2), 0)

	log.Println("Wrote doc as block2")
	foundEntries2 := block2.GetAllEntries()
	assert.Equals(t, len(foundEntries2), 2)
	assertLogEntry(t, foundEntries2[0], "doc1", "1-abc", 50, 1)
	assertLogEntry(t, foundEntries2[1], "doc2", "1-abc", 50, 3)

	// Attempt to write the same entry with the first block/writer
	overflow, pendingRemoval, err = block.AddEntrySet(entries2, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	log.Println("Wrote doc as block1")

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 2)
	assertLogEntry(t, foundEntries[0], "doc1", "1-abc", 50, 1)
	assertLogEntry(t, foundEntries[1], "doc2", "1-abc", 50, 3)
	assert.Equals(t, int(block.getEntryCount()), 2)
}

// --------------------
// DenseBlockList Tests
// --------------------
func TestDenseBlockList(t *testing.T) {

	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	list := NewDenseBlockList("ABC", 1, indexBucket)

	// Simple insert
	partitionClock := makePartitionClock(
		[]uint16{1, 3, 6, 11},
		[]uint64{0, 0, 0, 0},
	)
	_, err := list.AddBlock()
	assertNoError(t, err, "Error adding block to blocklist")

	indexBucket.Dump()

	// Create a new instance of the block list, validate contents
	newList := NewDenseBlockList("ABC", 1, indexBucket)
	assert.Equals(t, len(newList.blocks), 1)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)

	// Add a few more blocks to the new list

	partitionClock.incrementPartitionClock(1)
	_, err = newList.AddBlock()
	assertNoError(t, err, "Error adding block2 to blocklist")
	assert.Equals(t, len(newList.blocks), 2)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)
	assert.Equals(t, newList.blocks[1].BlockIndex, 1)

	// Attempt to add a block via original list.  Should be cancelled due to cas
	// mismatch, and reload the current state (i.e. newList)
	partitionClock.incrementPartitionClock(1)
	list.AddBlock()
	assert.Equals(t, len(list.blocks), 2)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)
	assert.Equals(t, newList.blocks[1].BlockIndex, 1)

}

func makePartitionClock(vbNos []uint16, sequences []uint64) PartitionClock {
	clock := make(PartitionClock, len(vbNos))
	for i := 0; i < len(vbNos); i++ {
		clock[vbNos[i]] = sequences[i]
	}
	return clock
}

func (c PartitionClock) incrementPartitionClock(count uint64) {
	for key, value := range c {
		c[key] = value + count
	}
}
