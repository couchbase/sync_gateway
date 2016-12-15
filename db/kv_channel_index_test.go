package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func testPartitionMap() *base.IndexPartitions {

	return testPartitionMapWithShards(64)
}

func testContextAndChannelIndex(channelName string) (*DatabaseContext, *KvChannelIndex) {
	context, _ := NewDatabaseContext("db", testBucket(), false, DatabaseContextOptions{})
	// TODO: don't use the base bucket as the index bucket in tests
	channelIndex := NewKvChannelIndex(channelName, context.Bucket, testPartitionMap(), testOnChange)
	return context, channelIndex
}

func testIndexBucket() base.Bucket {
	bucket, err := ConnectToBucket(base.BucketSpec{
		Server:     kTestURL,
		BucketName: "index_tests"}, nil)
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	return bucket
}

func testPartitionMapWithShards(numShards int) *base.IndexPartitions {

	partitions := make(base.PartitionStorageSet, numShards)

	numPartitions := uint16(numShards)
	vbPerPartition := 1024 / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		pStorage := base.PartitionStorage{
			Index: partition,
			Uuid:  fmt.Sprintf("partition_%d", partition),
			VbNos: make([]uint16, vbPerPartition),
		}
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			pStorage.VbNos[index] = vb
		}
		partitions[partition] = pStorage
	}

	indexPartitions := base.NewIndexPartitions(partitions)
	return indexPartitions
}

func testBitFlagStorage(channelName string) *BitFlagStorage {
	return NewBitFlagStorage(testIndexBucket(), channelName, testPartitionMap())
}

func testStableSequence() (uint64, error) {
	return 0, nil
}

func testOnChange(keys base.Set) {
	for key := range keys {
		log.Println("on change:", key)
	}
}

func makeEntry(vbNo int, sequence int, removal bool) *LogEntry {
	docId := fmt.Sprintf("doc_%d_%d", vbNo, sequence)
	revId := "1-abcdef01234567890123456789"
	return makeEntryForDoc(docId, revId, vbNo, sequence, removal)

}

func makeEntryForDoc(docId string, revId string, vbNo int, sequence int, removal bool) *LogEntry {
	entry := LogEntry{
		DocID:    docId,
		RevID:    revId,
		VbNo:     uint16(vbNo),
		Sequence: uint64(sequence),
	}
	if removal {
		entry.SetRemoved()
	}
	return &entry
}

func makeLogEntry(seq uint64, docid string) *LogEntry {
	return e(seq, docid, "1-abc")
}

func TestIndexBlockCreation(t *testing.T) {

	testStorage := testBitFlagStorage("ABC")
	defer testStorage.bucket.Close()
	entry := makeEntry(1, 1, false)
	block := testStorage.getIndexBlockForEntry(entry)
	assert.Equals(t, testStorage.indexBlockCache.Count(), 1)
	blockEntries := block.GetAllEntries()
	assert.Equals(t, len(blockEntries), 0)

}

func TestIndexBlockStorage(t *testing.T) {

	testStorage := testBitFlagStorage("ABC")
	defer testStorage.bucket.Close()

	// Add entries
	block := testStorage.getIndexBlockForEntry(makeEntry(5, 100, false))

	assertNoError(t, block.AddEntry(makeEntry(5, 100, false)), "Add entry 5_100")
	assertNoError(t, block.AddEntry(makeEntry(5, 105, true)), "Add entry 5_105")
	assertNoError(t, block.AddEntry(makeEntry(7, 100, true)), "Add entry 7_100")
	assertNoError(t, block.AddEntry(makeEntry(9, 100, true)), "Add entry 9_100")
	assertNoError(t, block.AddEntry(makeEntry(9, 101, true)), "Add entry 9_101")

	// validate in-memory storage
	storedEntries := block.GetAllEntries()
	assert.Equals(t, 5, len(storedEntries))
	log.Printf("Stored: %+v", storedEntries)

	marshalledBlock, err := block.Marshal()
	assertNoError(t, err, "Marshal block")
	log.Printf("Marshalled size: %d", len(marshalledBlock))

	newBlock := newBitFlagBufferBlock("ABC", 0, 0, testStorage.partitions.VbPositionMaps[0])
	assertNoError(t, newBlock.Unmarshal(marshalledBlock), "Unmarshal block")
	loadedEntries := newBlock.GetAllEntries()
	assert.Equals(t, 5, len(loadedEntries))
	log.Printf("Unmarshalled: %+v", loadedEntries)

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

// vbCache tests
/*
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
	assertNoError(t, vbCache.prependEntries(olderEntries, uint64(3), uint64(4)), "Error prepending entries")

	from, to, results = vbCache.getEntries(uint64(0), uint64(50))
	assert.Equals(t, from, uint64(3))
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
	assert.Equals(t, from, uint64(3))
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
	assertTrue(t, err != nil, "Adding out-of-sequence entries should return error")
	from, to, results = vbCache.getEntries(uint64(0), uint64(50))
	assert.Equals(t, len(results), 5)

	// Attempt to append entries with gaps
	newerEntries = []*LogEntry{
		makeLogEntry(40, "doc1"),
	}
	err = vbCache.appendEntries(newerEntries, uint64(40), uint64(45))
	assertTrue(t, err != nil, "Appending with gap should return error")

	// Attempt to prepend entries with gaps
	newerEntries = []*LogEntry{
		makeLogEntry(1, "doc1"),
	}
	err = vbCache.prependEntries(newerEntries, uint64(0), uint64(1))
	assertTrue(t, err != nil, "Prepending with gap should return error")

}
*/
