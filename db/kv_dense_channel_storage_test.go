package db

import (
	"fmt"
	"log"
	"sync"
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
	assertTrue(t, entry.Sequence == uint64(sequence), fmt.Sprintf("Sequence mismatch.  Expected [%d] Actual [%d]", sequence, entry.Sequence))
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

	overflow, pendingRemoval, updateClock, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, updateClock.GetSequence(50), uint64(1))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "1-abc", 50, 1)

	// Update within the same partition block, deduplicate by id
	entries[0] = makeBlockEntry("doc1", "2-abc", 50, 3, IsNotRemoval, IsNotAdded)

	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, updateClock.GetSequence(50), uint64(3))

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "2-abc", 50, 3)

	// Update within the same partition block, deduplicate by sequence
	entries[0] = makeBlockEntry("doc1", "3-abc", 50, 5, IsNotRemoval, IsNotAdded)
	entries[0].PrevSequence = uint64(3)

	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, updateClock.GetSequence(50), uint64(5))

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
	overflow, pendingRemoval, updateClock, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, block.getEntryCount(), uint16(10))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "1-abc", i*10, i+1)
		assert.Equals(t, updateClock.GetSequence(uint16(i*10)), uint64(i+1))
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
	overflow, pendingRemoval, updateClock, _, err := block.AddEntrySet(entries, indexBucket)
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
		assert.Equals(t, updateClock.GetSequence(uint16(i*10+1)), uint64(i+1))

	}

	// Updates
	entries = make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		vbno := 10*i + 1
		sequence := i + 21
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "2-abc", vbno, sequence, IsNotRemoval, IsNotAdded)
		entries[i].PrevSequence = uint64(i + 1)
	}
	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 10)

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "2-abc", 10*i+1, 21+i)
		assert.Equals(t, updateClock.GetSequence(uint16(i*10+1)), uint64(i+21))
	}

	// Validate pending removal by adding an entry where the previous revision isn't in the block
	entries = make([]*LogEntry, 1)
	entries[0] = makeBlockEntry("doc_not_in_block", "2-abc", 11, 65, IsNotRemoval, IsNotAdded)
	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 1)
	assert.Equals(t, int(block.getEntryCount()), 11)

}

func TestDenseBlockRemovalByKey(t *testing.T) {
	base.EnableLogKey("ChannelStorage")
	base.EnableLogKey("ChannelStorage+")
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	vbno := 50
	// Inserts
	entries := make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		sequence := i + 1
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "1-abc", vbno, sequence, IsNotRemoval, IsAdded)
	}
	overflow, pendingRemoval, updateClock, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, block.getEntryCount(), uint16(10))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		sequence := i + 1
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "1-abc", vbno, sequence)
	}
	assert.Equals(t, updateClock.GetSequence(uint16(50)), uint64(10))

	// Updates with removal by key
	entries = make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		vbno := 50
		sequence := i + 21
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "2-abc", vbno, sequence, IsNotRemoval, IsNotAdded)
	}
	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 10)

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	for i := 0; i < 10; i++ {
		assertLogEntry(t, foundEntries[i], fmt.Sprintf("doc%d", i), "2-abc", 50, 21+i)
	}
	assert.Equals(t, updateClock.GetSequence(uint16(50)), uint64(30))

	// Validate pending removal by adding an entry where the previous revision isn't in the block
	entries = make([]*LogEntry, 1)
	entries[0] = makeBlockEntry("doc_not_in_block", "2-abc", 50, 65, IsNotRemoval, IsNotAdded)
	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 1)
	assert.Equals(t, int(block.getEntryCount()), 11)

}

func TestDenseBlockRollbackTo(t *testing.T) {
	base.EnableLogKey("ChannelStorage")
	base.EnableLogKey("ChannelStorage+")
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Inserts the following entries:
	// [0,1] [1,2] [2,3] [0,4] [1,5] [2,6] [0,7] [1,8] [2,9] [0,10]
	entries := make([]*LogEntry, 10)
	for i := 0; i < 10; i++ {
		sequence := i + 1
		vbNo := i % 3 // mix up the vbuckets
		entries[i] = makeBlockEntry(fmt.Sprintf("doc%d", i), "1-abc", vbNo, sequence, IsNotRemoval, IsAdded)
	}
	overflow, pendingRemoval, _, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, block.getEntryCount(), uint16(10))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 10)
	assertLogEntry(t, foundEntries[0], "doc0", "1-abc", 0, 1)
	assertLogEntry(t, foundEntries[1], "doc1", "1-abc", 1, 2)
	assertLogEntry(t, foundEntries[2], "doc2", "1-abc", 2, 3)
	assertLogEntry(t, foundEntries[3], "doc3", "1-abc", 0, 4)
	assertLogEntry(t, foundEntries[4], "doc4", "1-abc", 1, 5)
	assertLogEntry(t, foundEntries[5], "doc5", "1-abc", 2, 6)
	assertLogEntry(t, foundEntries[6], "doc6", "1-abc", 0, 7)
	assertLogEntry(t, foundEntries[7], "doc7", "1-abc", 1, 8)
	assertLogEntry(t, foundEntries[8], "doc8", "1-abc", 2, 9)
	assertLogEntry(t, foundEntries[9], "doc9", "1-abc", 0, 10)

	// Rollback should complete in this block
	rollbackComplete, err := block.RollbackTo(2, 5, indexBucket)
	assertNoError(t, err, "Error rolling back")
	assert.Equals(t, rollbackComplete, true)
	assert.Equals(t, block.getEntryCount(), uint16(8))

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 8)
	assertLogEntry(t, foundEntries[0], "doc0", "1-abc", 0, 1)
	assertLogEntry(t, foundEntries[1], "doc1", "1-abc", 1, 2)
	assertLogEntry(t, foundEntries[2], "doc2", "1-abc", 2, 3)
	assertLogEntry(t, foundEntries[3], "doc3", "1-abc", 0, 4)
	assertLogEntry(t, foundEntries[4], "doc4", "1-abc", 1, 5)
	assertLogEntry(t, foundEntries[5], "doc6", "1-abc", 0, 7)
	assertLogEntry(t, foundEntries[6], "doc7", "1-abc", 1, 8)
	assertLogEntry(t, foundEntries[7], "doc9", "1-abc", 0, 10)

	// Rollback should NOT complete in this block, because we don't see a sequence value earlier than
	// rollback value in this block (haven't seen a sequence earlier than 1 in vb 1)
	rollbackComplete, err = block.RollbackTo(1, 1, indexBucket)
	assertNoError(t, err, "Error rolling back")
	assert.Equals(t, rollbackComplete, false)
	assert.Equals(t, block.getEntryCount(), uint16(5))

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 5)
	assertLogEntry(t, foundEntries[0], "doc0", "1-abc", 0, 1)
	assertLogEntry(t, foundEntries[1], "doc2", "1-abc", 2, 3)
	assertLogEntry(t, foundEntries[2], "doc3", "1-abc", 0, 4)
	assertLogEntry(t, foundEntries[3], "doc6", "1-abc", 0, 7)
	assertLogEntry(t, foundEntries[4], "doc9", "1-abc", 0, 10)

	// Remove the first entry, make sure nothing breaks
	rollbackComplete, err = block.RollbackTo(0, 0, indexBucket)
	assertNoError(t, err, "Error rolling back")
	assert.Equals(t, rollbackComplete, false)
	assert.Equals(t, block.getEntryCount(), uint16(1))

	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc2", "1-abc", 2, 3)

}

func DisableTestDenseBlockOverflow(t *testing.T) {
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
	overflow, pendingRemoval, updateClock, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 100)
	assert.Equals(t, updateClock.GetSequence(100), uint64(100))

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
	overflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 12)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 188)
	assert.Equals(t, len(block.value), 10046)
	assert.Equals(t, updateClock.GetSequence(100), uint64(188))

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
	newOverflow, pendingRemoval, updateClock, _, err = block.AddEntrySet(overflow, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(newOverflow), 12)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, int(block.getEntryCount()), 188)
	assert.Equals(t, len(block.value), 10046)
	assert.Equals(t, len(updateClock), 0)

}

// CAS handling test
func TestDenseBlockConcurrentUpdates(t *testing.T) {
	base.EnableLogKey("ChannelStorage")

	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1", nil)

	// Simple insert
	entries := make([]*LogEntry, 1)
	entries[0] = makeBlockEntry("doc1", "1-abc", 50, 1, IsNotRemoval, IsAdded)

	overflow, pendingRemoval, updateClock, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, updateClock.GetSequence(50), uint64(1))

	foundEntries := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 1)
	assertLogEntry(t, foundEntries[0], "doc1", "1-abc", 50, 1)
	log.Println("Wrote doc as block1")

	// Initialize a second instance of the block (simulates multiple writers), write a doc.
	// Expects cas failure on first write, success on subsequent.
	block2 := NewDenseBlock("block1", nil)
	entries2 := make([]*LogEntry, 1)
	entries2[0] = makeBlockEntry("doc2", "1-abc", 50, 3, IsNotRemoval, IsAdded)
	overflow2, pendingRemoval2, updateClock2, casFail, err := block2.AddEntrySet(entries2, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, casFail, true)
	assert.Equals(t, len(overflow2), 1)
	assert.Equals(t, len(pendingRemoval2), 0)
	assert.Equals(t, updateClock2.GetSequence(50), uint64(0))

	block2.loadBlock(indexBucket)
	overflow2, pendingRemoval2, updateClock2, casFail, err = block2.AddEntrySet(entries2, indexBucket)
	assert.Equals(t, casFail, false)
	assert.Equals(t, len(overflow2), 0)
	assert.Equals(t, len(pendingRemoval2), 0)
	assert.Equals(t, updateClock2.GetSequence(50), uint64(3))

	log.Println("Wrote doc as block2")
	foundEntries2 := block2.GetAllEntries()
	assert.Equals(t, len(foundEntries2), 2)
	assertLogEntry(t, foundEntries2[0], "doc1", "1-abc", 50, 1)
	assertLogEntry(t, foundEntries2[1], "doc2", "1-abc", 50, 3)

	// Attempt to write the same entry with the first block/writer
	overflow, pendingRemoval, updateClock, casFail, err = block.AddEntrySet(entries2, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 1)
	assert.Equals(t, casFail, true)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, updateClock.GetSequence(50), uint64(0))
	log.Println("Wrote doc as block1")

	block.loadBlock(indexBucket)
	foundEntries = block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 2)
	assertLogEntry(t, foundEntries[0], "doc1", "1-abc", 50, 1)
	assertLogEntry(t, foundEntries[1], "doc2", "1-abc", 50, 3)
	assert.Equals(t, int(block.getEntryCount()), 2)
}

// ------------------------
// DenseBlockIterator Tests
// ------------------------
func TestDenseBlockIterator(t *testing.T) {

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
	overflow, pendingRemoval, _, _, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(overflow), 0)
	assert.Equals(t, len(pendingRemoval), 0)
	assert.Equals(t, block.getEntryCount(), uint16(10))

	reader := NewDenseBlockIterator(block)
	i := 0
	logEntry := reader.next()
	for logEntry != nil {
		assertLogEntry(t, logEntry.MakeLogEntry(), fmt.Sprintf("doc%d", i), "1-abc", 10*i+1, i+1)
		i++
		logEntry = reader.next()
	}
	assert.Equals(t, i, 10)

	reverseReader := NewDenseBlockIterator(block)
	reverseReader.end()
	i = 9
	logEntry = reader.previous()
	for logEntry != nil {
		assertLogEntry(t, logEntry.MakeLogEntry(), fmt.Sprintf("doc%d", i), "1-abc", 10*i+1, i+1)
		i--
		logEntry = reader.previous()
	}
	assert.Equals(t, i, -1)

	bidiReader := NewDenseBlockIterator(block)
	logEntry = bidiReader.next()
	assertLogEntry(t, logEntry.MakeLogEntry(), fmt.Sprintf("doc0"), "1-abc", 1, 1)
	logEntry = bidiReader.previous()
	assertLogEntry(t, logEntry.MakeLogEntry(), fmt.Sprintf("doc0"), "1-abc", 1, 1)
	logEntry = bidiReader.previous()
	assert.Equals(t, logEntry == nil, true)
	logEntry = bidiReader.next()
	assertLogEntry(t, logEntry.MakeLogEntry(), fmt.Sprintf("doc0"), "1-abc", 1, 1)
	bidiReader.end()
	logEntry = bidiReader.next()
	assert.Equals(t, logEntry == nil, true)
	logEntry = bidiReader.previous()
	assertLogEntry(t, logEntry.MakeLogEntry(), fmt.Sprintf("doc9"), "1-abc", 91, 10)

}

// --------------------
// DenseBlockList Tests
// --------------------
func TestDenseBlockList(t *testing.T) {

	base.EnableLogKey("ChannelStorage+")

	log.Printf("Calling testIndexBucket() to bucket on server: %v", kTestURL)
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	// Initialize a new block list.  Will initialize with first block
	list := NewDenseBlockList("ABC", 1, indexBucket)

	// Simple insert
	_, err := list.AddBlock()
	assertNoError(t, err, "Error adding block to blocklist")

	indexBucket.Dump()

	// Create a new instance of the same block list, validate contents
	newList := NewDenseBlockList("ABC", 1, indexBucket)
	assert.Equals(t, len(newList.blocks), 2)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)

	// Add a few more blocks to the new list
	_, err = newList.AddBlock()
	assertNoError(t, err, "Error adding block2 to blocklist")
	assert.Equals(t, len(newList.blocks), 3)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)
	assert.Equals(t, newList.blocks[1].BlockIndex, 1)

	// Attempt to add a block via original list.  Should be cancelled due to cas
	// mismatch, and reload the current state (i.e. newList)
	list.AddBlock()
	assert.Equals(t, len(list.blocks), 3)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)
	assert.Equals(t, newList.blocks[1].BlockIndex, 1)

}

// Artificially set the CAS to an invalid value, to ensure write processing recovers from CAS mismatch
func TestDenseBlockListBadCas(t *testing.T) {

	base.EnableLogKey("ChannelStorage+")

	log.Printf("Calling testIndexBucket() to bucket on server: %v", kTestURL)
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	// Initialize a new block list manually to set an unexpected cas value.
	list := &DenseBlockList{
		channelName: "ABC",
		partition:   1,
		indexBucket: indexBucket,
	}
	list.activeCas = 50
	list.activeKey = list.generateActiveListKey()
	list.initDenseBlockList()

	// Simple insert
	_, err := list.AddBlock()
	assertNoError(t, err, "Error adding block to blocklist")

	indexBucket.Dump()

	// Create a new instance of the same block list, validate contents
	newList := NewDenseBlockList("ABC", 1, indexBucket)
	assert.Equals(t, len(newList.blocks), 2)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)

	// Add a few more blocks to the new list
	_, err = newList.AddBlock()
	assertNoError(t, err, "Error adding block2 to blocklist")
	assert.Equals(t, len(newList.blocks), 3)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)
	assert.Equals(t, newList.blocks[1].BlockIndex, 1)

	// Attempt to add a block via original list.  Should be cancelled due to cas
	// mismatch, and reload the current state (i.e. newList)
	list.AddBlock()
	assert.Equals(t, len(list.blocks), 3)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)
	assert.Equals(t, newList.blocks[1].BlockIndex, 1)

}

// Test multiple writers attempting to concurrently initialize a block
func TestDenseBlockListConcurrentInit(t *testing.T) {

	base.EnableLogKey("ChannelStorage+")
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	// Concurrent initialization
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			list := NewDenseBlockList("ABC", 1, indexBucket)
			assertTrue(t, list != nil, "Error creating block list")
		}()
	}
	wg.Wait()

	// Create a new instance of the same block list, validate contents
	newList := NewDenseBlockList("ABC", 1, indexBucket)
	assert.Equals(t, len(newList.blocks), 1)
	assert.Equals(t, newList.blocks[0].BlockIndex, 0)

}

func TestDenseBlockListRotate(t *testing.T) {

	initCount := MaxListBlockCount
	MaxListBlockCount = 10
	defer func() {
		MaxListBlockCount = initCount
	}()

	base.EnableLogKey("ChannelStorage+")

	log.Printf("Calling testIndexBucket() to bucket on server: %v", kTestURL)
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	// Initialize a new block list.  Will initialize with first block
	list := NewDenseBlockList("ABC", 1, indexBucket)

	// Add more than max blocks to block list
	for i := 1; i <= MaxListBlockCount+10; i++ {
		_, err := list.AddBlock()
		assertNoError(t, err, "Error adding block to blocklist")
	}

	indexBucket.Dump()

	// Create a new instance of the same block list, validate contents
	newList := NewDenseBlockList("ABC", 1, indexBucket)
	assert.Equals(t, len(newList.blocks), 10)

	err := newList.LoadPrevious()
	assertNoError(t, err, "Error loading previous")
	assert.Equals(t, len(newList.blocks), 21)

}

// ---------------------------------------------------------------------------------------------
// Dense Storage Reader Tests
//   The majority of reader tests are in sg_accel, leveraging the writer to populate the index.
//   There are a few utility-type tests here.
//----------------------------------------------------------------------------------------------

func TestCalculateChangedPartitions(t *testing.T) {
	base.EnableLogKey("ChannelStorage+")
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	reader := NewDenseStorageReader(indexBucket, "ABC", testPartitionMap())

	startClock := getClockForMap(map[uint16]uint64{
		0:   0,
		100: 0,
		200: 0,
	})
	endClock := getClockForMap(map[uint16]uint64{
		0:   5,
		100: 10,
		200: 15,
	})

	changedVbs, changedPartitions := reader.calculateChanged(startClock, endClock)
	assert.Equals(t, len(changedVbs), 3)
	assert.Equals(t, changedVbs[0], uint16(0))   // Partition 0
	assert.Equals(t, changedVbs[1], uint16(100)) // Partition 6
	assert.Equals(t, changedVbs[2], uint16(200)) // Partition 12

	changedPartitionCount := 0
	for partition, partitionRange := range changedPartitions {
		if partitionRange != nil {
			changedPartitionCount++
			assertTrue(t, partition == 0 || partition == 6 || partition == 12, "Unexpected changed partition")
		}
	}
	assert.Equals(t, changedPartitions[0].GetSequenceRange(0).Since, uint64(0))
	assert.Equals(t, changedPartitions[6].GetSequenceRange(100).Since, uint64(0))
	assert.Equals(t, changedPartitions[12].GetSequenceRange(200).Since, uint64(0))
	assert.Equals(t, changedPartitions[0].GetSequenceRange(0).To, uint64(5))
	assert.Equals(t, changedPartitions[6].GetSequenceRange(100).To, uint64(10))
	assert.Equals(t, changedPartitions[12].GetSequenceRange(200).To, uint64(15))
	assert.Equals(t, changedPartitionCount, 3)

}

func getClockForMap(values map[uint16]uint64) base.SequenceClock {
	clock := base.NewSequenceClockImpl()
	for vb, seq := range values {
		clock.SetSequence(vb, seq)
	}
	return clock
}

func makePartitionClock(vbNos []uint16, sequences []uint64) base.PartitionClock {
	clock := make(base.PartitionClock, len(vbNos))
	for i := 0; i < len(vbNos); i++ {
		clock[vbNos[i]] = sequences[i]
	}
	return clock
}
