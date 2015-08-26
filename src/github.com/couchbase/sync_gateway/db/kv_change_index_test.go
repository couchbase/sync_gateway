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

func setupTestDBForChangeIndex(t *testing.T) *Database {

	leakyBucketConfig := base.LeakyBucketConfig{
		TapFeedVbuckets: true,
	}
	vbEnabledBucket := testLeakyBucket(leakyBucketConfig)

	indexBucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "walrus:",
		BucketName: "indexBucket"})

	if err != nil {
		log.Fatal("Couldn't connect to index bucket")
	}
	dbcOptions := DatabaseContextOptions{
		IndexOptions: &ChangeIndexOptions{
			Bucket: indexBucket,
		},
		SequenceHashOptions: &SequenceHashOptions{
			Bucket: indexBucket,
		},
	}
	context, err := NewDatabaseContext("db", vbEnabledBucket, false, dbcOptions)
	assertNoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")
	return db
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
	return entry
}

// Returns a clock-base SequenceID with all vb values set to seq
func simpleClockSequence(seq uint64) SequenceID {
	result := SequenceID{
		SeqType: ClockSequenceType,
		Clock:   base.NewSequenceClockImpl(),
	}
	for i := 0; i < 1024; i++ {
		result.Clock.SetSequence(uint16(i), seq)
	}
	return result
}

func TestChannelPartitionMap(t *testing.T) {
	cpMap := make(ChannelPartitionMap, 0)
	channels := []string{"A", "B", "C"}
	partitions := []uint16{0, 1, 2, 3}
	for _, ch := range channels {
		for _, par := range partitions {
			entry := &LogEntry{Sequence: 1, VbNo: 1}
			chanPar := ChannelPartition{channelName: ch, partition: par}
			cpMap.add(chanPar, entry)
		}
	}
	log.Printf("map: %v", cpMap)
	assert.True(t, len(cpMap) == 12)

}

func TestChangeIndexAddEntry(t *testing.T) {

	base.LogKeys["DCache+"] = true
	changeIndex, bucket := testKvChangeIndex("indexBucket")
	defer bucket.Close()
	changeIndex.AddToCache(channelEntry(1, 1, "foo1", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(50 * time.Millisecond)

	// Verify entry
	var entry LogEntry
	entryBytes, _, err := bucket.GetRaw("_cache_entry:1:1")
	assert.True(t, err == nil)
	json.Unmarshal(entryBytes, &entry)
	assert.Equals(t, entry.DocID, "foo1")
	assert.Equals(t, entry.Sequence, uint64(1))
	assert.Equals(t, entry.RevID, "1-a")

	// Verify Channel Index Block
	block := NewIndexBlock("ABC", 1, 1)
	blockBytes, _, err := bucket.GetRaw(getIndexBlockKey("ABC", 0, 0))
	bucket.Dump()
	err = block.Unmarshal(blockBytes)
	assertNoError(t, err, "Unmarshal block")
	allEntries := block.GetAllEntries()
	assert.Equals(t, len(allEntries), 1)

	// Verify stable sequence
	stableClock := base.SequenceClockImpl{}
	stableSeqBytes, _, err := bucket.GetRaw(base.KStableSequenceKey)
	err = stableClock.Unmarshal(stableSeqBytes)
	log.Println("bytes:", stableSeqBytes)
	assertNoError(t, err, "Unmarshal stable sequence")
	assert.Equals(t, stableClock.GetSequence(1), uint64(1))
	assert.Equals(t, stableClock.GetSequence(2), uint64(0))

	// Verify channel sequences
	channelClock := base.SequenceClockImpl{}
	chanClockBytes, _, err := bucket.GetRaw(getChannelClockKey("ABC"))
	log.Println("key:", getChannelClockKey("ABC"))
	log.Println("bytes:", chanClockBytes)
	err = channelClock.Unmarshal(chanClockBytes)
	log.Println("chan ABC", channelClock.GetSequence(1))
	assertNoError(t, err, "Unmarshal channel clock sequence")
	assert.Equals(t, channelClock.GetSequence(1), uint64(1))
	assert.Equals(t, channelClock.GetSequence(2), uint64(0))

	channelClock = base.SequenceClockImpl{}
	chanClockBytes, _, err = bucket.GetRaw(getChannelClockKey("CBS"))
	err = channelClock.Unmarshal(chanClockBytes)
	assertNoError(t, err, "Unmarshal channel clock sequence")
	assert.Equals(t, channelClock.GetSequence(1), uint64(1))
	assert.Equals(t, channelClock.GetSequence(2), uint64(0))
}

func TestChangeIndexGetChanges(t *testing.T) {

	base.LogKeys["DIndex+"] = true
	changeIndex, bucket := testKvChangeIndex("indexBucket")
	defer bucket.Close()
	// Add entries across multiple partitions
	changeIndex.AddToCache(channelEntry(100, 1, "foo1", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(300, 5, "foo3", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(500, 1, "foo5", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(10 * time.Millisecond)

	// Verify entries
	entries, err := changeIndex.GetChanges("ABC", ChangesOptions{Since: simpleClockSequence(0)})
	assert.Equals(t, len(entries), 3)
	assert.True(t, err == nil)

	// Add entries across multiple partitions in the same block
	changeIndex.AddToCache(channelEntry(101, 1, "foo101-1", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(100, 8, "foo100-8", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(498, 3, "foo498-3", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(10 * time.Millisecond)
	bucket.Dump()
	// Verify entries
	entries, err = changeIndex.GetChanges("ABC", ChangesOptions{Since: simpleClockSequence(0)})
	assert.Equals(t, len(entries), 6)
	assert.True(t, err == nil)

	// Add entries across multiple partitions, multiple blocks
	changeIndex.AddToCache(channelEntry(101, 10001, "foo101-10001", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(100, 10100, "foo100-10100", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(498, 20003, "foo498-20003", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(10 * time.Millisecond)
	// Verify entries
	entries, err = changeIndex.GetChanges("ABC", ChangesOptions{Since: simpleClockSequence(0)})
	assert.Equals(t, len(entries), 9)
	assert.True(t, err == nil)

	// Retrieval for a more restricted range
	entries, err = changeIndex.GetChanges("ABC", ChangesOptions{Since: simpleClockSequence(100)})
	assert.Equals(t, len(entries), 3)
	assert.True(t, err == nil)

	// Retrieval for a more restricted range where the since matches a valid sequence number (since border case)
	entries, err = changeIndex.GetChanges("ABC", ChangesOptions{Since: simpleClockSequence(10100)})
	assert.Equals(t, len(entries), 1)
	assert.True(t, err == nil)

	// Add entries that skip a block in a partition
	changeIndex.AddToCache(channelEntry(800, 100, "foo800-100", "1-a", []string{"ABC", "CBS"}))
	changeIndex.AddToCache(channelEntry(800, 20100, "foo800-20100", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(10 * time.Millisecond)
	// Verify entries
	entries, err = changeIndex.GetChanges("ABC", ChangesOptions{Since: simpleClockSequence(0)})
	assert.Equals(t, len(entries), 11)
	assert.True(t, err == nil)

	// Test deduplication by doc id, including across empty blocks
	changeIndex.AddToCache(channelEntry(700, 100, "foo700", "1-a", []string{"DUP"}))
	changeIndex.AddToCache(channelEntry(700, 200, "foo700", "1-b", []string{"DUP"}))
	changeIndex.AddToCache(channelEntry(700, 300, "foo700", "1-c", []string{"DUP"}))
	changeIndex.AddToCache(channelEntry(700, 10100, "foo700", "1-d", []string{"DUP"}))
	changeIndex.AddToCache(channelEntry(700, 30100, "foo700", "1-e", []string{"DUP"}))
	// wait for add
	time.Sleep(10 * time.Millisecond)
	// Verify entries
	entries, err = changeIndex.GetChanges("DUP", ChangesOptions{Since: simpleClockSequence(0)})
	assert.Equals(t, len(entries), 1)
	assert.True(t, err == nil)

	bucket.Dump()
}

func TestChangeIndexChanges(t *testing.T) {
	base.LogKeys["DIndex+"] = true
	base.LogKeys["DIndex"] = true
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "PBS", "NBC", "TBS"))
	authenticator.Save(user)

	// Write an entry to the bucket
	WriteDirectWithKey(db, "1c856b5724dcf4273c3993619900ce7f", []string{}, 1)

	time.Sleep(2000 * time.Millisecond)
	changes, err := db.GetChanges(base.SetOf("*"), ChangesOptions{Since: simpleClockSequence(0)})
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 1)

	time.Sleep(2000 * time.Millisecond)
	// Write a few more entries to the bucket
	WriteDirectWithKey(db, "12389b182ababd12fff662848edeb908", []string{}, 1)
	time.Sleep(2000 * time.Millisecond)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: simpleClockSequence(0)})
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 2)
}

func TestLoadStableSequence(t *testing.T) {
	changeIndex, bucket := testKvChangeIndex("indexBucket")
	defer bucket.Close()
	// Add entries across multiple partitions
	changeIndex.AddToCache(channelEntry(100, 1, "foo1", "1-a", []string{}))
	changeIndex.AddToCache(channelEntry(300, 5, "foo3", "1-a", []string{}))
	changeIndex.AddToCache(channelEntry(500, 1, "foo5", "1-a", []string{}))
	time.Sleep(10 * time.Millisecond)

	stableSequence := base.LoadStableSequence(bucket)
	assert.Equals(t, stableSequence.GetSequence(100), uint64(1))
	assert.Equals(t, stableSequence.GetSequence(300), uint64(5))
	assert.Equals(t, stableSequence.GetSequence(500), uint64(1))

}

func TestStableSequenceCallback(t *testing.T) {
	changeIndex, bucket := testKvChangeIndex("indexBucket")
	defer bucket.Close()

	// Add entries across multiple partitions
	changeIndex.AddToCache(channelEntry(100, 1, "foo1", "1-a", []string{}))
	changeIndex.AddToCache(channelEntry(300, 5, "foo3", "1-a", []string{}))
	changeIndex.AddToCache(channelEntry(500, 1, "foo5", "1-a", []string{}))
	time.Sleep(10 * time.Millisecond)

	stableSequence, err := base.StableCallbackTest(changeIndex.GetStableClock)
	assertNoError(t, err, "Got error on callback")
	assert.Equals(t, stableSequence.GetSequence(100), uint64(1))
	assert.Equals(t, stableSequence.GetSequence(300), uint64(5))
	assert.Equals(t, stableSequence.GetSequence(500), uint64(1))
}
