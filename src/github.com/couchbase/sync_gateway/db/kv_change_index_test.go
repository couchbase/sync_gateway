package db

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
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
	defer changeIndex.Stop()
	changeIndex.AddToCache(channelEntry(1, 1, "foo1", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(50 * time.Millisecond)

	// Verify entry
	var entry LogEntry
	entryBytes, _, err := bucket.GetRaw("_idx_entry:1:1")
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
	defer changeIndex.Stop()
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

	time.Sleep(20 * time.Millisecond)
	changes, err := db.GetChanges(base.SetOf("*"), ChangesOptions{Since: simpleClockSequence(0)})
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 1)

	time.Sleep(20 * time.Millisecond)
	// Write a few more entries to the bucket
	WriteDirectWithKey(db, "12389b182ababd12fff662848edeb908", []string{}, 1)
	time.Sleep(20 * time.Millisecond)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: simpleClockSequence(0)})
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 2)
}

func TestPollingChangesFeed(t *testing.T) {
	//base.LogKeys["DIndex+"] = true
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Start a longpoll changes
	go func() {
		abcHboChanges, err := db.GetChanges(base.SetOf("ABC", "HBO"), ChangesOptions{Since: simpleClockSequence(0), Wait: true})
		assertTrue(t, err == nil, "Error getting changes")
		// Expects two changes - the nil that's sent on initial wait, and then docABC_1
		assert.Equals(t, len(abcHboChanges), 2)
	}()

	time.Sleep(100 * time.Millisecond)
	// Write an entry to channel ABC to notify the waiting longpoll
	WriteDirectWithKey(db, "docABC_1", []string{"ABC"}, 1)

	// Start a continuous changes on a different channel (CBS).  Waitgroup keeps test open until continuous is terminated
	var wg sync.WaitGroup
	continuousTerminator := make(chan bool)
	go func() {
		defer wg.Done()
		wg.Add(1)
		cbsChanges, err := db.GetChanges(base.SetOf("CBS"), ChangesOptions{Since: simpleClockSequence(0), Wait: true, Continuous: true, Terminator: continuousTerminator})
		assertTrue(t, err == nil, "Error getting changes")
		// Expect 15 entries + 16 nil entries (one per wait)
		assert.Equals(t, len(cbsChanges), 25)
		log.Println("Continuous completed")

	}()

	// Write another entry to channel ABC to start the clock for unread polls
	time.Sleep(1000 * time.Millisecond)
	WriteDirectWithKey(db, "docABC_2", []string{"ABC"}, 1)

	// Verify that the channel is initially in the polled set
	changeIndex, _ := db.changeCache.(*kvChangeIndex)
	assertTrue(t, changeIndex.getChannelReader("ABC") != nil, "Channel reader should not be nil")
	log.Printf("changeIndex readers: %+v", changeIndex.channelIndexReaders)

	// Send multiple docs to channels HBO, PBS, CBS.  Expected results:
	//   ABC - Longpoll has ended - should trigger "nobody listening" expiry of channel reader
	//   CBS - Active continuous feed - channel reader shouldn't expire
	//   PBS - No changes feeds have requested this channel - no channel reader should be created
	//   HBO - New longpoll started mid-way before poll limit reached, channel reader shouldn't expire
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 12; i++ {
		log.Printf("writing multiDoc_%d", i)
		WriteDirectWithKey(db, fmt.Sprintf("multiDoc_%d", i), []string{"PBS", "HBO", "CBS"}, 1)
		// Midway through, read from HBO
		if i == 9 {
			time.Sleep(20 * time.Millisecond)
			hboChanges, err := db.GetChanges(base.SetOf("HBO"), ChangesOptions{Since: simpleClockSequence(0), Wait: true})
			assertTrue(t, err == nil, "Error getting changes")
			assert.Equals(t, len(hboChanges), 10)
		}
		time.Sleep(kPollFrequency * time.Millisecond)
	}

	close(continuousTerminator)
	log.Println("closed terminator")

	// Send another entry to continuous CBS feed in order to trigger the terminator check
	time.Sleep(100 * time.Millisecond)
	WriteDirectWithKey(db, "terminatorCheck", []string{"CBS"}, 1)

	time.Sleep(100 * time.Millisecond)
	// Validate that the ABC reader was deleted due to inactivity
	log.Printf("channel reader ABC:%+v", changeIndex.getChannelReader("ABC"))
	assertTrue(t, changeIndex.getChannelReader("ABC") == nil, "Channel reader should be nil")

	// Validate that the HBO reader is still present
	assertTrue(t, changeIndex.getChannelReader("HBO") != nil, "Channel reader should be exist")

	// Start another longpoll changes for ABC, ensure it's successful (will return the existing 2 records, no wait)
	changes, err := db.GetChanges(base.SetOf("ABC"), ChangesOptions{Since: simpleClockSequence(0), Wait: true})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 2)

	// Repeat, verify use of existing channel reader
	changes, err = db.GetChanges(base.SetOf("ABC"), ChangesOptions{Since: simpleClockSequence(0), Wait: true})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 2)

	wg.Wait()

}

func TestLoadStableSequence(t *testing.T) {
	changeIndex, bucket := testKvChangeIndex("indexBucket")
	defer changeIndex.Stop()
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
	changeIndex, _ := testKvChangeIndex("indexBucket")
	defer changeIndex.Stop()

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

func TestChangeIndexPrincipal(t *testing.T) {

	base.LogKeys["DIndex+"] = true
	db := setupTestDBForChangeIndex(t)
	db.ChannelMapper = channels.NewDefaultChannelMapper()
	kvChangeIndex, _ := db.changeCache.(*kvChangeIndex)

	defer tearDownTestDB(t, db)
	kvChangeIndex.DocChanged("_sync:user:bob", []byte(`{"name": "bob", "admin_channels":["ABC", "PBS"]}`), uint64(1), uint16(1))
	time.Sleep(100 * time.Millisecond) // wait for indexing

	// Verify user entry in index
	var principal PrincipalIndex
	principalBytes, _, err := kvChangeIndex.indexBucket.GetRaw("_idx_user:bob")
	assert.True(t, err == nil)
	json.Unmarshal(principalBytes, &principal)
	assert.Equals(t, principal.VbNo, uint16(1))
	assert.DeepEquals(t, principal.ExplicitChannels, channels.TimedSet{"ABC": 1, "PBS": 1})

	// Verify stable sequence was incremented
	stableClock := base.NewSequenceClockImpl()
	stableClockBytes, _, err := kvChangeIndex.indexBucket.GetRaw("_idx_stableSeq")
	assertNoError(t, err, "Unable to retrieve stable sequence")
	stableClock.Unmarshal(stableClockBytes)
	log.Printf("got clock:%s", base.PrintClock(stableClock))
	assert.Equals(t, stableClock.GetSequence(1), uint64(1))

	// Update the user to add a channel, remove a channel
	kvChangeIndex.DocChanged("_sync:user:bob", []byte(`{"name": "bob", "admin_channels":["PBS", "NBC"]}`), uint64(2), uint16(1))
	time.Sleep(100 * time.Millisecond) // wait for indexing

	principalBytes, _, err = kvChangeIndex.indexBucket.GetRaw("_idx_user:bob")
	assert.True(t, err == nil)
	json.Unmarshal(principalBytes, &principal)
	assert.Equals(t, principal.VbNo, uint16(1))
	assert.DeepEquals(t, principal.ExplicitChannels, channels.TimedSet{"PBS": 1, "NBC": 2})

}
