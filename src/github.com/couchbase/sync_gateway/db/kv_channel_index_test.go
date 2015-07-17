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

func testStableSequence() (uint64, error) {
	return 0, nil
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

func TestIndexBlockCreation(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

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

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

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
	assertNoError(t, newBlock.Unmarshal(marshalledBlock, 0), "Unmarshal block")
	loadedEntries := newBlock.GetAllEntries()
	assert.Equals(t, 5, len(loadedEntries))
	log.Printf("Unmarshalled: %+v", loadedEntries)

}

func TestChannelIndexWrite(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

	// Init block for sequence 100, partition 1
	entry_5_100 := kvIndexEntry{
		vbNo:     5,
		sequence: 100,
	}

	// Add entries
	assertNoError(t, channelIndex.Add(entry_5_100), "Add entry 5_100")

	// Reset the channel index to verify loading the block from the DB and validate contents
	channelIndex = NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)
	block := channelIndex.getIndexBlockForEntry(entry_5_100)
	assert.Equals(t, len(block.GetAllEntries()), 1)

	// Test CAS handling.  AnotherChannelIndex is another writer updating the same block in the DB.
	anotherChannelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)
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

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

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

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

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

func TestChannelClockWrite(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	defer context.Close()
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

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
	channelIndex.clock.load(context.Bucket, "ABC")
	assert.Equals(t, channelIndex.clock.value[9], uint64(25000))
	assert.Equals(t, channelIndex.clock.value[5], uint64(15000))
	assert.Equals(t, channelIndex.clock.value[7], uint64(100))

}
