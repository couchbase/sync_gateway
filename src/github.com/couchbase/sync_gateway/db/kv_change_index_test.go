package db

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbaselabs/go.assert"
)

func testKvChangeIndex(bucketname string) (*kvChangeIndex, base.Bucket) {
	cacheBucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "walrus:",
		BucketName: bucketname})
	if err != nil {
		log.Fatal("Couldn't connect to cache bucket")
	}
	index := &kvChangeIndex{}

	changeIndexOptions := &ChangeIndexOptions{
		Bucket: cacheBucket,
	}
	index.Init(nil, SequenceID{}, nil, &CacheOptions{}, changeIndexOptions)

	return index, cacheBucket
}

func channelEntry(vbNo uint16, seq uint64, docid string, revid string, channelNames []string) *LogEntry {

	channelMap := make(channels.ChannelMap, len(channelNames))
	for _, channel := range channelNames {
		channelMap[channel] = nil
	}
	entry := &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
		Channels:     channelMap,
		VbNo:         vbNo,
	}
	log.Println("CHANNEL ENTRY:", entry.VbNo)
	return entry
}

func TestChannelPartitionMap(t *testing.T) {
	cpMap := make(ChannelPartitionMap, 0)
	channels := []string{"A", "B", "C"}
	partitions := []uint16{0, 1, 2, 3}
	for _, ch := range channels {
		for _, par := range partitions {
			entry := &LogEntry{Sequence: 1, VbNo: 1}
			chanPar := ChannelPartition{channelName: ch, partition: par}
			cpMap.add(chanPar, entry, false)
		}
	}
	log.Printf("map: %v", cpMap)
	assert.True(t, len(cpMap) == 12)

}

func TestChangeIndexAddEntry(t *testing.T) {

	base.LogKeys["DCache+"] = true
	changeIndex, bucket := testKvChangeIndex("indexBucket")
	changeIndex.AddToCache(channelEntry(1, 1, "foo1", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(50 * time.Millisecond)

	// Verify entry
	var entry kvChannelIndexEntry
	entryBytes, _, err := bucket.GetRaw("_cache:seq:1")
	json.Unmarshal(entryBytes, &entry)

	assert.Equals(t, entry.DocID, "foo1")
	assert.Equals(t, entry.Sequence, uint64(1))
	assert.Equals(t, entry.RevID, "1-a")
	assert.True(t, err == nil)

	// Verify Channel Index Block
	block := NewIndexBlock("ABC", 1, 1)
	blockBytes, _, err := bucket.GetRaw(getIndexBlockKey("ABC", 0, 0))
	bucket.Dump()
	err = block.Unmarshal(blockBytes)
	assertNoError(t, err, "Unmarshal block")
	allEntries := block.GetAllEntries()
	assert.Equals(t, len(allEntries), 1)

	// Verify stable sequence
	stableClock := SequenceClockImpl{}
	stableSeqBytes, _, err := bucket.GetRaw(kStableSequenceKey)
	err = stableClock.Unmarshal(stableSeqBytes)
	log.Println("bytes:", stableSeqBytes)
	assertNoError(t, err, "Unmarshal stable sequence")
	assert.Equals(t, stableClock.GetSequence(1), uint64(1))
	assert.Equals(t, stableClock.GetSequence(2), uint64(0))

	// Verify channel sequences
	channelClock := SequenceClockImpl{}
	chanClockBytes, _, err := bucket.GetRaw(getChannelClockKey("ABC"))
	log.Println("key:", getChannelClockKey("ABC"))
	log.Println("bytes:", chanClockBytes)
	err = channelClock.Unmarshal(chanClockBytes)
	log.Println("chan ABC", channelClock.GetSequence(1))
	assertNoError(t, err, "Unmarshal channel clock sequence")
	assert.Equals(t, channelClock.GetSequence(1), uint64(1))
	assert.Equals(t, channelClock.GetSequence(2), uint64(0))

	channelClock = SequenceClockImpl{}
	chanClockBytes, _, err = bucket.GetRaw(getChannelClockKey("CBS"))
	err = channelClock.Unmarshal(chanClockBytes)
	assertNoError(t, err, "Unmarshal channel clock sequence")
	assert.Equals(t, channelClock.GetSequence(1), uint64(1))
	assert.Equals(t, channelClock.GetSequence(2), uint64(0))
}

func TestChangeIndexGetChanges(t *testing.T) {

	base.LogKeys["DCache+"] = true
	changeIndex, _ := testKvChangeIndex("indexBucket")
	// Add entries across multiple partitions
	changeIndex.AddToCache(channelEntry(100, 1, "foo1", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(300, 5, "foo3", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(500, 1, "foo5", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(10 * time.Millisecond)

	// Verify entries
	//entries, err := changeIndex.GetChanges("ABC", ChangesOptions{Since: SequenceID{Seq: 0}})

	//assert.Equals(t, len(entries), 3)
	//assert.True(t, err == nil)

}
