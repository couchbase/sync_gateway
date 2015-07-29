package db

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func testPartitionMap() IndexPartitionMap {
	// Simplified partition map of 64 sequential partitions, 16 vbuckets each
	partitions := make(IndexPartitionMap, 64)

	numPartitions := uint16(64)
	vbPerPartition := 1024 / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			partitions[vb] = partition
		}
	}
	return partitions
}

func testContextAndChannelIndex(channelName string) (*DatabaseContext, *kvChannelIndex) {
	context, _ := NewDatabaseContext("db", testBucket(), false, DatabaseContextOptions{})
	// TODO: don't use the base bucket as the index bucket in tests
	channelIndex := NewKvChannelIndex(channelName, context.Bucket, testPartitionMap(), testStableClock, testOnChange)
	return context, channelIndex
}

func testStableSequence() (uint64, error) {
	return 0, nil
}

func testStableClock() SequenceClock {
	return NewSequenceClockImpl()
}

func testOnChange(keys base.Set) {
	for key, _ := range keys {
		log.Println("on change:", key)
	}
}

func makeEntry(vbNo int, sequence int, removal bool) kvIndexEntry {
	return kvIndexEntry{
		vbNo:     uint16(vbNo),
		sequence: uint64(sequence),
		removal:  removal,
	}
}

func makeLogEntry(seq uint64, docid string) *LogEntry {
	return e(seq, docid, "1-abc")
}

func TestIndexBlockCreation(t *testing.T) {

	context, channelIndex := testContextAndChannelIndex("ABC")
	defer context.Close()

	entry := kvIndexEntry{
		vbNo:     1,
		sequence: 1,
		removal:  false,
	}
	block := channelIndex.getIndexBlockForEntry(entry)
	assert.True(t, len(channelIndex.indexBlockCache) == 1)
	blockEntries := block.GetAllEntries()
	assert.Equals(t, len(blockEntries), 0)

}

func TestIndexBlockStorage(t *testing.T) {

	context, channelIndex := testContextAndChannelIndex("ABC")
	defer context.Close()

	// Add entries
	block := channelIndex.getIndexBlockForEntry(makeEntry(5, 100, false))

	assertNoError(t, block.AddEntry(makeEntry(5, 100, false)), "Add entry 5_100")
	assertNoError(t, block.AddEntry(makeEntry(5, 105, true)), "Add entry 5_105")
	assertNoError(t, block.AddEntry(makeEntry(7, 100, true)), "Add entry 7_100")
	assertNoError(t, block.AddEntry(makeEntry(9, 100, true)), "Add entry 9_100")
	assertNoError(t, block.AddEntry(makeEntry(9, 1001, true)), "Add entry 9_1001")

	// validate in-memory storage
	storedEntries := block.GetAllEntries()
	assert.Equals(t, 5, len(storedEntries))
	log.Printf("Stored: %+v", storedEntries)

	marshalledBlock, err := block.Marshal()
	assertNoError(t, err, "Marshal block")
	log.Printf("Marshalled size: %d", len(marshalledBlock))

	newBlock := BitFlagBlock{}
	assertNoError(t, newBlock.Unmarshal(marshalledBlock), "Unmarshal block")
	loadedEntries := newBlock.GetAllEntries()
	assert.Equals(t, 5, len(loadedEntries))
	log.Printf("Unmarshalled: %+v", loadedEntries)

}

func TestChannelIndexWrite(t *testing.T) {

	context, channelIndex := testContextAndChannelIndex("ABC")
	defer context.Close()

	// Init block for sequence 100, partition 1
	entry_5_100 := kvIndexEntry{
		vbNo:     5,
		sequence: 100,
	}

	// Add entries
	assertNoError(t, channelIndex.Add(entry_5_100), "Add entry 5_100")

	log.Println("ADD COMPLETE")

	// Reset the channel index to verify loading the block from the DB and validate contents
	channelIndex = NewKvChannelIndex("ABC", context.Bucket, testPartitionMap(), testStableClock, testOnChange)
	block := channelIndex.getIndexBlockForEntry(entry_5_100)
	assert.Equals(t, len(block.GetAllEntries()), 1)

	// Test CAS handling.  AnotherChannelIndex is another writer updating the same block in the DB.
	anotherChannelIndex := NewKvChannelIndex("ABC", context.Bucket, testPartitionMap(), testStableClock, testOnChange)
	// Init block for sequence 100, partition 1
	entry_5_101 := kvIndexEntry{
		vbNo:     5,
		sequence: 101,
	}
	assertNoError(t, anotherChannelIndex.Add(entry_5_101), "Add entry 5_100")

	// Now send another update to the original index (which now has a stale cas/cache).  Ensure all three entries
	// end up in the block.
	entry_5_102 := kvIndexEntry{
		vbNo:     5,
		sequence: 102,
	}
	channelIndex.Add(entry_5_102)
	block = channelIndex.getIndexBlockForEntry(entry_5_102)
	assert.Equals(t, len(block.GetAllEntries()), 3)

}

/*  obsolete
func TestAddPartitionSet(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

	// Entries for a single partition, single block
	entrySet := []kvIndexEntry{
		makeEntry(5, 100, false),
		makeEntry(5, 105, false),
		makeEntry(7, 100, false),
		makeEntry(9, 100, false),
		makeEntry(9, 1001, false),
	}
	// Add entries
	assertNoError(t, channelIndex.addPartitionSet(channelIndex.partitionMap[5], entrySet), "Add partition set")

	block := channelIndex.getIndexBlockForEntry(makeEntry(5, 100, false))
	assert.Equals(t, len(block.GetAllEntries()), 5)

	// Validate error when sending updates for multiple partitions

	entrySet = []kvIndexEntry{
		makeEntry(25, 100, false),
		makeEntry(35, 100, false),
	}
	err := channelIndex.addPartitionSet(channelIndex.partitionMap[25], entrySet)
	log.Printf("error adding set? %v", err)
	assertTrue(t, err != nil, "Adding mixed-partition set should fail.")
}

func TestAddPartitionSetMultiBlock(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

	// Init entries for a single partition, across multiple blocks
	entrySet := []kvIndexEntry{
		makeEntry(5, 100, false),
		makeEntry(5, 15000, false),
		makeEntry(7, 100, false),
		makeEntry(9, 100, false),
		makeEntry(9, 25000, false),
	}
	// Add entries
	assertNoError(t, channelIndex.addPartitionSet(channelIndex.partitionMap[5], entrySet), "Add partition set")

	block := channelIndex.getIndexBlockForEntry(makeEntry(5, 100, false))
	assert.Equals(t, len(block.GetAllEntries()), 3) // 5_100, 7_100, 9_100
	block = channelIndex.getIndexBlockForEntry(makeEntry(5, 15000, false))
	assert.Equals(t, len(block.GetAllEntries()), 1) // 5_15000
	block = channelIndex.getIndexBlockForEntry(makeEntry(9, 25000, false))
	assert.Equals(t, len(block.GetAllEntries()), 1) // 9_25000

}
*/

func TestAddSet(t *testing.T) {

	context, channelIndex := testContextAndChannelIndex("ABC")
	defer context.Close()

	// Init entries across multiple partitions
	entrySet := []kvIndexEntry{
		makeEntry(5, 100, false),
		makeEntry(5, 105, false),
		makeEntry(7, 100, false),
		makeEntry(50, 100, false),
		makeEntry(75, 1001, false),
	}
	// Add entries
	assertNoError(t, channelIndex.AddSet(entrySet), "Add entry set")

	block := channelIndex.getIndexBlockForEntry(makeEntry(5, 100, false))
	assert.Equals(t, len(block.GetAllEntries()), 3)
	block = channelIndex.getIndexBlockForEntry(makeEntry(50, 100, false))
	assert.Equals(t, len(block.GetAllEntries()), 1)
	block = channelIndex.getIndexBlockForEntry(makeEntry(75, 1001, false))
	assert.Equals(t, len(block.GetAllEntries()), 1)

	// check non-existent entry
	block = channelIndex.getIndexBlockForEntry(makeEntry(100, 1, false))
	assert.Equals(t, len(block.GetAllEntries()), 0)

}

func TestAddSetMultiBlock(t *testing.T) {

	context, channelIndex := testContextAndChannelIndex("ABC")
	defer context.Close()

	// Init entries across multiple partitions
	entrySet := []kvIndexEntry{
		makeEntry(5, 100, false),
		makeEntry(5, 15000, false),
		makeEntry(7, 100, false),
		makeEntry(9, 100, false),
		makeEntry(9, 25000, false),
	}
	// Add entries
	assertNoError(t, channelIndex.AddSet(entrySet), "Add set")

	block := channelIndex.getIndexBlockForEntry(makeEntry(5, 100, false))
	assert.Equals(t, len(block.GetAllEntries()), 3) // 5_100, 7_100, 9_100
	block = channelIndex.getIndexBlockForEntry(makeEntry(5, 15000, false))
	assert.Equals(t, len(block.GetAllEntries()), 1) // 5_15000
	block = channelIndex.getIndexBlockForEntry(makeEntry(9, 25000, false))
	assert.Equals(t, len(block.GetAllEntries()), 1) // 9_25000

}

func TestSequenceClockWrite(t *testing.T) {

	context, channelIndex := testContextAndChannelIndex("ABC")
	defer context.Close()

	// Init entries across multiple partitions
	entrySet := []kvIndexEntry{
		makeEntry(5, 100, false),
		makeEntry(5, 15000, false),
		makeEntry(7, 100, false),
		makeEntry(9, 100, false),
		makeEntry(9, 25000, false),
	}
	// Add entries
	assertNoError(t, channelIndex.AddSet(entrySet), "Add set")

	assert.Equals(t, channelIndex.clock.value[9], uint64(25000))
	assert.Equals(t, channelIndex.clock.value[5], uint64(15000))
	assert.Equals(t, channelIndex.clock.value[7], uint64(100))

	// Load clock from db and reverify
	channelIndex.loadClock()
	assert.Equals(t, channelIndex.clock.value[9], uint64(25000))
	assert.Equals(t, channelIndex.clock.value[5], uint64(15000))
	assert.Equals(t, channelIndex.clock.value[7], uint64(100))

}

// vbCache tests

func TestVbCache(t *testing.T) {
	vbCache := newVbCache()

	// Add initial entries
	entries := []*LogEntry{
		makeLogEntry(15, "doc1"),
		makeLogEntry(17, "doc2"),
		makeLogEntry(23, "doc3"),
	}
	assertNoError(t, vbCache.appendEntries(entries, uint64(5), uint64(25)), "Error appending entries")

	from, to, results := vbCache.getEntries(uint64(10), uint64(20))
	assert.Equals(t, from, uint64(10))
	assert.Equals(t, to, uint64(20))
	assert.Equals(t, len(results), 2)
	assert.Equals(t, results[0].DocID, "doc1")
	assert.Equals(t, results[0].Sequence, uint64(15))
	assert.Equals(t, results[1].DocID, "doc2")
	assert.Equals(t, results[1].Sequence, uint64(17))

	// Request for a range earlier than the cache is valid
	from, to, results = vbCache.getEntries(uint64(0), uint64(15))
	assert.Equals(t, from, uint64(5))
	assert.Equals(t, to, uint64(15))
	assert.Equals(t, len(results), 1)
	assert.Equals(t, results[0].DocID, "doc1")
	assert.Equals(t, results[0].Sequence, uint64(15))

	// Request for a range later than the cache is valid
	from, to, results = vbCache.getEntries(uint64(20), uint64(30))
	assert.Equals(t, from, uint64(20))
	assert.Equals(t, to, uint64(25))
	assert.Equals(t, len(results), 1)
	assert.Equals(t, results[0].DocID, "doc3")
	assert.Equals(t, results[0].Sequence, uint64(23))

	// Prepend older entries, including one duplicate doc id
	olderEntries := []*LogEntry{
		makeLogEntry(3, "doc1"),
		makeLogEntry(4, "doc4"),
	}
	assertNoError(t, vbCache.prependEntries(olderEntries, uint64(0)), "Error prepending entries")

	from, to, results = vbCache.getEntries(uint64(0), uint64(50))
	assert.Equals(t, from, uint64(0))
	assert.Equals(t, to, uint64(25))
	assert.Equals(t, len(results), 4)
	assert.Equals(t, results[0].DocID, "doc4")
	assert.Equals(t, results[1].DocID, "doc1")
	assert.Equals(t, results[2].DocID, "doc2")
	assert.Equals(t, results[3].DocID, "doc3")

	// Append newer entries, including two duplicate doc ids
	newerEntries := []*LogEntry{
		makeLogEntry(28, "doc1"),
		makeLogEntry(31, "doc5"),
		makeLogEntry(35, "doc3"),
	}
	assertNoError(t, vbCache.appendEntries(newerEntries, uint64(25), uint64(35)), "Error appending entries")

	from, to, results = vbCache.getEntries(uint64(0), uint64(50))
	assert.Equals(t, from, uint64(0))
	assert.Equals(t, to, uint64(35))
	assert.Equals(t, len(results), 5)
	assert.Equals(t, results[0].DocID, "doc4")
	assert.Equals(t, results[1].DocID, "doc2")
	assert.Equals(t, results[2].DocID, "doc1")
	assert.Equals(t, results[3].DocID, "doc5")
	assert.Equals(t, results[4].DocID, "doc3")

	// Attempt to add out-of-order entries
	newerEntries = []*LogEntry{
		makeLogEntry(40, "doc1"),
		makeLogEntry(37, "doc5"),
		makeLogEntry(43, "doc3"),
	}
	err := vbCache.appendEntries(newerEntries, uint64(35), uint64(43))
	log.Println("got back error:", err)
	//assertTrue(t, err != nil, "Adding out-of-sequence entries should return error")

}
