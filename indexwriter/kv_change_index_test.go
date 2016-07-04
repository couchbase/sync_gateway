package indexwriter

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
)

const kTestURL = "walrus:"

func testBucket() base.Bucket {
	bucket, err := db.ConnectToBucket(base.BucketSpec{
		Server:     kTestURL,
		BucketName: "sg_accel_tests"}, nil)
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	return bucket
}

func testLeakyBucket(config base.LeakyBucketConfig) base.Bucket {
	testBucket := testBucket()
	leakyBucket := base.NewLeakyBucket(testBucket, config)
	return leakyBucket
}

/*

func testDatabaseContext(bucketName string, options db.DatabaseContextOptions) (*db.DatabaseContext, error) {
	return db.NewDatabaseContext("db", testBucket(bucketName), false, options)
}
*/

func testKvIndexWriter(t *testing.T) (*kvChangeIndexWriter, base.Bucket, base.Bucket) {

	database := setupTestDBForChangeIndex(t)

	indexWriter := &kvChangeIndexWriter{}
	indexWriter.Init(database.DatabaseContext, nil)

	return indexWriter, database.Bucket, indexWriter.indexWriteBucket
}

func setupTestDBForChangeIndex(t *testing.T) *db.Database {

	context := setupTestDBContextForChangeIndex(t)
	db, err := db.CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")

	return db
}

func setupTestDBContextForChangeIndex(t *testing.T) *db.DatabaseContext {

	var vbEnabledBucket base.Bucket
	if kTestURL == "walrus:" {
		leakyBucketConfig := base.LeakyBucketConfig{
			TapFeedVbuckets: true,
		}
		vbEnabledBucket = testLeakyBucket(leakyBucketConfig)
	} else {
		vbEnabledBucket = testBucket()
	}

	indexBucketSpec := base.BucketSpec{
		Server:     kTestURL,
		BucketName: "test_indexBucket"}
	indexBucket, err := db.ConnectToBucket(indexBucketSpec, nil)

	if err != nil {
		log.Fatal("Couldn't connect to index bucket")
	}
	dbcOptions := db.DatabaseContextOptions{
		IndexOptions: &db.ChangeIndexOptions{
			Spec:   indexBucketSpec,
			Writer: true,
		},
		SequenceHashOptions: &db.SequenceHashOptions{
			Bucket: indexBucket,
		},
	}
	context, err := db.NewDatabaseContext("db", vbEnabledBucket, false, dbcOptions)
	assertNoError(t, err, "Couldn't create context for database 'db'")

	base.SeedTestPartitionMap(context.GetIndexBucket(), 64)
	return context
}

func channelEntry(vbNo uint16, seq uint64, docid string, revid string, channelNames []string) *db.LogEntry {

	channelMap := make(channels.ChannelMap, len(channelNames))
	for _, channel := range channelNames {
		channelMap[channel] = nil
	}
	entry := &db.LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
		Channels:     channelMap,
		VbNo:         vbNo,
	}
	return entry
}

func TestChangeIndexAddEntry(t *testing.T) {

	base.EnableLogKey("DIndex+")
	writer, _, indexBucket := testKvIndexWriter(t)
	dbContext := writer.context
	defer tearDownIndexTest(dbContext, writer)
	writer.addToCache(channelEntry(1, 1, "foo1", "1-a", []string{"ABC", "CBS"}))

	// wait for add
	time.Sleep(500 * time.Millisecond)

	// Verify entry
	var entry db.LogEntry
	entryBytes, _, err := indexBucket.GetRaw("_idx_entry:1:1")
	assert.True(t, err == nil)
	json.Unmarshal(entryBytes, &entry)
	assert.Equals(t, entry.DocID, "foo1")
	assert.Equals(t, entry.Sequence, uint64(1))
	assert.Equals(t, entry.RevID, "1-a")

	// Verify Channel Index Block
	partitions, err := writer.getIndexPartitions()
	assertNoError(t, err, "Get index partitions")
	block := db.NewIndexBlock("ABC", 1, 1, partitions)
	blockBytes, _, err := indexBucket.GetRaw(db.GetIndexBlockKey("ABC", 0, 0))
	//indexBucket.Dump()
	err = block.Unmarshal(blockBytes)
	assertNoError(t, err, "Unmarshal block")
	allEntries := block.GetAllEntries()
	assert.Equals(t, len(allEntries), 1)

	// Verify stable sequence.  Use GetStableClock here to load from the bucket, not use
	// the writer's in-memory version
	stableClock, err := writer.context.GetStableClock()
	assertNoError(t, err, "Get stable clock")
	log.Printf("Stable clock:%s", base.PrintClock(stableClock))
	assert.Equals(t, stableClock.GetSequence(1), uint64(1))
	assert.Equals(t, stableClock.GetSequence(2), uint64(0))

	// Verify channel sequences
	channelClock := base.SequenceClockImpl{}
	chanClockBytes, _, err := indexBucket.GetRaw(db.GetChannelClockKey("ABC"))
	err = channelClock.Unmarshal(chanClockBytes)
	assertNoError(t, err, "Unmarshal channel clock sequence")
	assert.Equals(t, channelClock.GetSequence(1), uint64(1))
	assert.Equals(t, channelClock.GetSequence(2), uint64(0))

	channelClock = base.SequenceClockImpl{}
	chanClockBytes, _, err = indexBucket.GetRaw(db.GetChannelClockKey("CBS"))
	err = channelClock.Unmarshal(chanClockBytes)
	assertNoError(t, err, "Unmarshal channel clock sequence")
	assert.Equals(t, channelClock.GetSequence(1), uint64(1))
	assert.Equals(t, channelClock.GetSequence(2), uint64(0))
}

func TestChangeIndexGetChanges(t *testing.T) {

	base.EnableLogKey("DIndex+")
	writer, _, _ := testKvIndexWriter(t)
	changeIndex := writer.context.GetChangeIndex()
	dbContext := writer.context
	defer tearDownIndexTest(dbContext, writer)
	// Add entries across multiple partitions
	writer.addToCache(channelEntry(100, 1, "foo1", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(300, 5, "foo3", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(500, 1, "foo5", "1-a", []string{"ABC", "CBS"}))

	// Verify entries
	_, err := getExpectedChangesWithRetry(changeIndex, "ABC", 0, 3)
	assert.True(t, err == nil)

	// Add entries across multiple partitions in the same block
	writer.addToCache(channelEntry(101, 1, "foo101-1", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(100, 8, "foo100-8", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(498, 3, "foo498-3", "1-a", []string{"ABC", "CBS"}))

	// Verify entries
	_, err = getExpectedChangesWithRetry(changeIndex, "ABC", 0, 6)
	assert.True(t, err == nil)

	// Add entries across multiple partitions, multiple blocks
	writer.addToCache(channelEntry(101, 10001, "foo101-10001", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(100, 10100, "foo100-10100", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(498, 20003, "foo498-20003", "1-a", []string{"ABC", "CBS"}))

	// Verify entries
	_, err = getExpectedChangesWithRetry(changeIndex, "ABC", 0, 9)
	assert.True(t, err == nil)

	// Retrieval for a more restricted range
	_, err = getExpectedChangesWithRetry(changeIndex, "ABC", 100, 3)
	assert.True(t, err == nil)

	// Retrieval for a more restricted range where the since matches a valid sequence number (since border case)
	_, err = getExpectedChangesWithRetry(changeIndex, "ABC", 10100, 1)
	assert.True(t, err == nil)

	// Add entries that skip a block in a partition
	writer.addToCache(channelEntry(800, 100, "foo800-100", "1-a", []string{"ABC", "CBS"}))
	writer.addToCache(channelEntry(800, 20100, "foo800-20100", "1-a", []string{"ABC", "CBS"}))

	// Verify entries
	_, err = getExpectedChangesWithRetry(changeIndex, "ABC", 0, 11)
	assert.True(t, err == nil)

	// Test deduplication by doc id, including across empty blocks
	writer.addToCache(channelEntry(700, 100, "foo700", "1-a", []string{"DUP"}))
	writer.addToCache(channelEntry(700, 200, "foo700", "1-b", []string{"DUP"}))
	writer.addToCache(channelEntry(700, 300, "foo700", "1-c", []string{"DUP"}))
	writer.addToCache(channelEntry(700, 10100, "foo700", "1-d", []string{"DUP"}))
	writer.addToCache(channelEntry(700, 30100, "foo700", "1-e", []string{"DUP"}))

	// Verify entries
	_, err = getExpectedChangesWithRetry(changeIndex, "DUP", 0, 1)
	assert.True(t, err == nil)
}

func getExpectedChangesWithRetry(changeIndex db.ChangeIndex, channelName string, simpleSince uint64, expectedChanges int) ([]*db.LogEntry, error) {

	// Retry with backoff, up to ~5s total
	waitTime := time.Millisecond * 10
	for i := 0; i < 8; i++ {
		entries, err := changeIndex.GetChanges(channelName, db.ChangesOptions{Since: SimpleClockSequence(simpleSince)})
		if err != nil {
			return nil, err
		}
		if len(entries) == expectedChanges {
			return entries, nil
		}
		time.Sleep(waitTime)
		waitTime = waitTime * 2
	}
	return nil, fmt.Errorf("Changes never returned expected change count of %v", expectedChanges)

}

// Currently disabled, due to test race conditions between the continuous changes start (in its own goroutine),
// and the send of the continuous terminator.  We can't ensure that the changes request has been
// started before all other test operations have been sent (so that we never break out of the changes loop)
/*
func RaceTestPollingChangesFeed(t *testing.T) {
	//base.LogKeys["DIndex+"] = true
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)

	dbExpvars.Init()
	db.ChannelMapper = channels.NewDefaultChannelMapper()
	// Start a longpoll changes
	go func() {
		abcHboChanges, err := db.GetChanges(base.SetOf("ABC", "HBO"), db.ChangesOptions{Since: SimpleClockSequence(0), Wait: true})
		assertTrue(t, err == nil, "Error getting changes")
		// Expects two changes - the nil that's sent on initial wait, and then docABC_1
		for _, change := range abcHboChanges {
			log.Printf("abcHboChange:%v", change)
		}
		assert.Equals(t, len(abcHboChanges), 2)
	}()

	time.Sleep(100 * time.Millisecond)
	// Write an entry to channel ABC to notify the waiting longpoll
	WriteDirectWithKey(db, "docABC_1", []string{"ABC"}, 1)

	// Start a continuous changes on a different channel (CBS).  Waitgroup keeps test open until continuous is terminated
	var wg sync.WaitGroup
	continuousTerminator := make(chan bool)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cbsChanges, err := db.GetChanges(base.SetOf("CBS"), db.ChangesOptions{Since: SimpleClockSequence(0), Wait: true, Continuous: true, Terminator: continuousTerminator})
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
	assertTrue(t, changeIndex.reader.getChannelReader("ABC") != nil, "Channel reader should not be nil")
	log.Printf("changeIndex readers: %+v", changeIndex.reader.channelIndexReaders)

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
			// wait for polling cycle
			time.Sleep(600 * time.Millisecond)
			hboChanges, err := db.GetChanges(base.SetOf("HBO"), db.ChangesOptions{Since: SimpleClockSequence(0), Wait: true})
			assertTrue(t, err == nil, "Error getting changes")
			assert.Equals(t, len(hboChanges), 10)
		}
		time.Sleep(kPollFrequency * time.Millisecond)
	}

	// Verify that the changes feed has been started (avoids test race conditions where we close the terminator before
	// starting the changes feed
	for i := 0; i <= 40; i++ {
		channelChangesCount := getExpvarAsString(dbExpvars, "channelChangesFeeds")
		log.Printf("channelChangesCount:%s", channelChangesCount)
		if channelChangesCount != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	close(continuousTerminator)
	log.Println("closed terminator")

	// Send another entry to continuous CBS feed in order to trigger the terminator check
	time.Sleep(100 * time.Millisecond)
	WriteDirectWithKey(db, "terminatorCheck", []string{"CBS"}, 1)

	time.Sleep(100 * time.Millisecond)
	// Validate that the ABC reader was deleted due to inactivity
	log.Printf("channel reader ABC:%+v", changeIndex.reader.getChannelReader("ABC"))
	assertTrue(t, changeIndex.reader.getChannelReader("ABC") == nil, "Channel reader should be nil")

	// Validate that the HBO reader is still present
	assertTrue(t, changeIndex.reader.getChannelReader("HBO") != nil, "Channel reader should be exist")

	// Start another longpoll changes for ABC, ensure it's successful (will return the existing 2 records, no wait)
	changes, err := db.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: SimpleClockSequence(0), Wait: true})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 2)

	// Repeat, verify use of existing channel reader
	changes, err = db.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: SimpleClockSequence(0), Wait: true})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 2)

	wg.Wait()

}
*/

// This test is unreliable - timing under race conditions is unpredictable.  Need to revisit
/*
func RacePollResultReuseLongpoll(t *testing.T) {
	// Reset the index expvars
	db.IndexExpvars.Init()
	base.EnableLogKey("IndexPoll")
	base.EnableLogKey("DIndex+")
	base.EnableLogKey("Changes+")

	indexWriter, _, _ := testKvIndexWriter(t)
	dbContext := indexWriter.context
	defer tearDownIndexTest(dbContext, indexWriter)
	dbContext.ChannelMapper = channels.NewDefaultChannelMapper()
	adminDB, _ := db.GetDatabase(dbContext, nil)

	adminDB.Put("docABC_1", db.Body{"channels": []string{"ABC"}})
	time.Sleep(200 * time.Millisecond)
	// Do a basic changes to trigger start of polling for channel
	changes, err := adminDB.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: SimpleClockSequence(0)})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 1)
	log.Printf("Changes:%+v", changes[0])

	// Start a longpoll changes, use waitgroup to delay the test until it returns.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		since, err := (*adminDB.DatabaseContext).ParseSequenceID("2-0")
		assertTrue(t, err == nil, "Error parsing sequence ID")
		abcHboChanges, err := adminDB.GetChanges(base.SetOf("ABC", "HBO"), db.ChangesOptions{Since: since, Wait: true})
		assertTrue(t, err == nil, "Error getting changes")
		// Expects two changes - the nil that's sent on initial wait, and then docABC_2
		assert.Equals(t, len(abcHboChanges), 2)
	}()

	time.Sleep(200 * time.Millisecond)
	// Write an entry to channel ABC to notify the waiting longpoll
	adminDB.Put("docABC_2", db.Body{"channels": []string{"ABC"}})

	wg.Wait()

	// Use expvars to confirm poll hits/misses (can't tell from changes response whether it used poll results,
	// or reloaded from index).  Expect one poll hit (the longpoll request), and one miss (the basic changes request)
	assert.Equals(t, getExpvarAsString(db.IndexExpvars, "getChanges_lastPolled_hit"), "1")
	assert.Equals(t, getExpvarAsString(db.IndexExpvars, "getChanges_lastPolled_miss"), "1")

}
*/

// Currently disabled, due to test race conditions between the continuous changes start (in its own goroutine),
// and the send of the continuous terminator.  We can't ensure that the changes request has been
// started before all other test operations have been sent (so that we never break out of the changes loop)
/*
func RaceTestPollResultReuseContinuous(t *testing.T) {
	// Reset the index expvars
	db.IndexExpvars.Init()
	base.EnableLogKey("IndexPoll")
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	WriteDirectWithKey(db, "docABC_1", []string{"ABC"}, 1)
	time.Sleep(100 * time.Millisecond)
	// Do a basic changes to trigger start of polling for channel
	changes, err := db.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: SimpleClockSequence(0)})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 1)
	log.Printf("Changes:%+v", changes[0])

	// Start a continuous changes on a different channel (CBS).  Waitgroup keeps test open until continuous is terminated
	var wg sync.WaitGroup
	continuousTerminator := make(chan bool)
	wg.Add(1)
	go func() {
		defer wg.Done()
		since, err := db.ParseSequenceID("2-0")
		abcHboChanges, err := db.GetChanges(base.SetOf("ABC", "HBO"), db.ChangesOptions{Since: since, Wait: true, Continuous: true, Terminator: continuousTerminator})
		assertTrue(t, err == nil, "Error getting changes")
		// Expect 2 entries + 3 nil entries (one per wait)
		assert.Equals(t, len(abcHboChanges), 5)
		for i := 0; i < len(abcHboChanges); i++ {
			log.Printf("Got change:%+v", abcHboChanges[i])
		}
		log.Println("Continuous completed")
	}()

	time.Sleep(100 * time.Millisecond)
	// Write an entry to channel HBO to shift the continuous since value ahead
	WriteDirectWithKey(db, "docHBO_1", []string{"HBO"}, 3)

	time.Sleep(1000 * time.Millisecond) // wait for indexing, polling, and changes processing
	// Write an entry to channel ABC - last polled should be used
	WriteDirectWithKey(db, "docABC_2", []string{"ABC"}, 4)

	time.Sleep(1000 * time.Millisecond) // wait for indexing, polling, and changes processing
	close(continuousTerminator)
	log.Println("closed terminator")

	time.Sleep(100 * time.Millisecond)
	WriteDirectWithKey(db, "terminatorCheck", []string{"HBO"}, 1)

	wg.Wait()

	// Use expvars to confirm poll hits/misses (can't tell from changes response whether it used poll results,
	// or reloaded from index).  Expect two poll hits (docHBO_1, docABC_2), and one miss (the initial changes request)

	assert.Equals(t, getExpvarAsString(IndexExpvars, "getChanges_lastPolled_hit"), "2")
	assert.Equals(t, getExpvarAsString(IndexExpvars, "getChanges_lastPolled_miss"), "1")

	time.Sleep(100 * time.Millisecond)

	// Make a changes request prior to the last polled range, ensure it doesn't reuse polled results
	changes, err = db.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: SimpleClockSequence(0)})

	assert.Equals(t, getExpvarAsString(IndexExpvars, "getChanges_lastPolled_hit"), "2")
	assert.Equals(t, getExpvarAsString(IndexExpvars, "getChanges_lastPolled_miss"), "2")

}

// Currently disabled, due to test race conditions between the continuous changes start (in its own goroutine),
// and the send of the continuous terminator.  We can't ensure that the changes request has been
// started before all other test operations have been sent (so that we never break out of the changes loop)
func RaceTestPollResultLongRunningContinuous(t *testing.T) {
	// Reset the index expvars
	IndexExpvars.Init()
	base.EnableLogKey("IndexPoll")
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	WriteDirectWithKey(db, "docABC_1", []string{"ABC"}, 1)
	time.Sleep(100 * time.Millisecond)
	// Do a basic changes to trigger start of polling for channel
	changes, err := db.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: SimpleClockSequence(0)})
	assertTrue(t, err == nil, "Error getting changes")
	assert.Equals(t, len(changes), 1)
	log.Printf("Changes:%+v", changes[0])

	// Start a continuous changes on channel (ABC).  Waitgroup keeps test open until continuous is terminated
	var wg sync.WaitGroup
	continuousTerminator := make(chan bool)
	wg.Add(1)
	go func() {
		defer wg.Done()
		since, err := db.ParseSequenceID("2-0")
		abcChanges, err := db.GetChanges(base.SetOf("ABC"), db.ChangesOptions{Since: since, Wait: true, Continuous: true, Terminator: continuousTerminator})
		assertTrue(t, err == nil, "Error getting changes")
		log.Printf("Got %d changes", len(abcChanges))
		log.Println("Continuous completed")

	}()

	for i := 0; i < 10000; i++ {
		WriteDirectWithKey(db, fmt.Sprintf("docABC_%d", i), []string{"ABC"}, 3)
		time.Sleep(1 * time.Millisecond)
	}

	time.Sleep(1000 * time.Millisecond) // wait for indexing, polling, and changes processing
	close(continuousTerminator)
	log.Println("closed terminator")

	time.Sleep(100 * time.Millisecond)
	WriteDirectWithKey(db, "terminatorCheck", []string{"ABC"}, 1)

	wg.Wait()

}
*/

func TestChangeIndexAddSet(t *testing.T) {

	base.EnableLogKey("DIndex+")
	writer, _, indexBucket := testKvIndexWriter(t)
	dbContext := writer.context
	defer tearDownIndexTest(dbContext, writer)

	entries := make([]*db.LogEntry, 1000)
	for vb := 0; vb < 1000; vb++ {
		entries[vb] = channelEntry(uint16(vb), 1, fmt.Sprintf("foo%d", vb), "1-a", []string{"ABC"})
	}

	indexPartitions, err := writer.getIndexPartitions()
	assertNoError(t, err, "Error retrieving index partitions")
	channelStorage := db.NewChannelStorage(indexBucket, "", indexPartitions)
	writer.indexEntries(entries, indexPartitions.VbMap, channelStorage)

	// wait for add to complete
	time.Sleep(100 * time.Millisecond)

	// Verify channel clocks
	channelClock := base.SequenceClockImpl{}
	chanClockBytes, _, err := indexBucket.GetRaw(db.GetChannelClockKey("ABC"))
	err = channelClock.Unmarshal(chanClockBytes)
	assertNoError(t, err, "Unmarshal channel clock sequence")

	starChannelClock := base.SequenceClockImpl{}
	chanClockBytes, _, err = indexBucket.GetRaw(db.GetChannelClockKey("*"))
	err = starChannelClock.Unmarshal(chanClockBytes)

	for vb := uint16(0); vb < 1000; vb++ {
		assert.Equals(t, channelClock.GetSequence(vb), uint64(1))
		assert.Equals(t, starChannelClock.GetSequence(vb), uint64(1))
	}
}

// Index partitionsfor testing
func SeedPartitionMap(bucket base.Bucket, numPartitions uint16) error {
	maxVbNo := uint16(1024)
	//maxVbNo := uint16(64)
	partitionDefs := make(base.PartitionStorageSet, numPartitions)
	vbPerPartition := maxVbNo / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		storage := base.PartitionStorage{
			Index: partition,
			VbNos: make([]uint16, vbPerPartition),
		}
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			storage.VbNos[index] = vb
		}
		partitionDefs[partition] = storage
	}

	// Persist to bucket
	value, err := json.Marshal(partitionDefs)
	if err != nil {
		return err
	}
	bucket.SetRaw(base.KIndexPartitionKey, 0, value)
	return nil
}

func getExpvarAsString(expvar *expvar.Map, name string) string {
	value := expvar.Get(name)
	if value != nil {
		return value.String()
	} else {
		return ""
	}
}

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertStatus(t *testing.T, response *testResponse, expectedStatus int) {
	if response.Code != expectedStatus {
		t.Fatalf("Response status %d (expected %d) for %s <%s> : %s",
			response.Code, expectedStatus, response.rq.Method, response.rq.URL, response.Body)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}

func assertFailed(t *testing.T, message string) {
	_, file, line, ok := runtime.Caller(2) // assertFailed + assertNoError + public function.
	if ok {
		// Truncate file name at last file name separator.
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		} else if index = strings.LastIndex(file, "\\"); index >= 0 {
			file = file[index+1:]
		}
	} else {
		file = "???"
		line = 1
	}
	t.Fatalf("%s:%d: %s", file, line, message)
}
