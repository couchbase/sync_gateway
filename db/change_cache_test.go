//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"

	"github.com/couchbaselabs/go.assert"
)

func e(seq uint64, docid string, revid string) *LogEntry {
	return &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
	}
}

func testBucketContext() *DatabaseContext {
	context, _ := NewDatabaseContext("db", testBucket(), false, DatabaseContextOptions{})
	return context
}

func TestSkippedSequenceQueue(t *testing.T) {

	var skipQueue SkippedSequenceQueue
	//Push values
	skipQueue.Push(&SkippedSequence{4, time.Now()})
	skipQueue.Push(&SkippedSequence{7, time.Now()})
	skipQueue.Push(&SkippedSequence{8, time.Now()})
	skipQueue.Push(&SkippedSequence{12, time.Now()})
	skipQueue.Push(&SkippedSequence{18, time.Now()})
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{4, 7, 8, 12, 18}))

	// Retrieval of low value
	lowValue := skipQueue[0].seq
	assert.Equals(t, lowValue, uint64(4))

	// Removal of first value
	err := skipQueue.Remove(4)
	assert.True(t, err == nil)
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{7, 8, 12, 18}))

	// Removal of middle values
	err = skipQueue.Remove(8)
	assert.True(t, err == nil)
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{7, 12, 18}))

	err = skipQueue.Remove(12)
	assert.True(t, err == nil)
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{7, 18}))

	// Removal of last value
	err = skipQueue.Remove(18)
	assert.True(t, err == nil)
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{7}))

	// Removal of non-existent returns error
	err = skipQueue.Remove(25)
	assert.True(t, err != nil)
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{7}))

	// Add an out-of-sequence entry (make sure bad sequencing doesn't throw us into an infinite loop)
	err = skipQueue.Push(&SkippedSequence{6, time.Now()})
	assert.True(t, err != nil)
	skipQueue.Push(&SkippedSequence{9, time.Now()})
	assert.True(t, err != nil)
	assert.True(t, verifySkippedSequences(skipQueue, []uint64{7, 9}))
}

func TestLateSequenceHandling(t *testing.T) {

	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Test1", 0)
	assert.True(t, cache != nil)

	// Empty late sequence cache should return empty set
	startSequence := cache.InitLateSequenceClient()
	entries, lastSeq, err := cache.GetLateSequencesSince(startSequence)
	assert.Equals(t, len(entries), 0)
	assert.Equals(t, lastSeq, uint64(0))
	assert.True(t, err == nil)

	cache.AddLateSequence(e(5, "foo", "1-a"))
	cache.AddLateSequence(e(8, "foo2", "1-a"))

	// Retrieve since 0
	entries, lastSeq, err = cache.GetLateSequencesSince(0)
	log.Println("entries:", entries)
	assert.Equals(t, len(entries), 2)
	assert.Equals(t, lastSeq, uint64(8))
	assert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))
	assert.True(t, err == nil)

	// Add Sequences.  Will trigger purge on old sequences without listeners
	cache.AddLateSequence(e(2, "foo3", "1-a"))
	cache.AddLateSequence(e(7, "foo4", "1-a"))
	assert.Equals(t, len(cache.lateLogs), 3)
	assert.Equals(t, cache.lateLogs[0].logEntry.Sequence, uint64(8))
	assert.Equals(t, cache.lateLogs[1].logEntry.Sequence, uint64(2))
	assert.Equals(t, cache.lateLogs[2].logEntry.Sequence, uint64(7))
	assert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))

	// Retrieve since previous
	entries, lastSeq, err = cache.GetLateSequencesSince(lastSeq)
	log.Println("entries:", entries)
	assert.Equals(t, len(entries), 2)
	assert.Equals(t, lastSeq, uint64(7))
	assert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(0))
	assert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))
	log.Println("cache.lateLogs:", cache.lateLogs)
	assert.True(t, err == nil)

	// Purge.  We have a listener sitting at seq=7, so purge should only clear previous
	cache.AddLateSequence(e(15, "foo5", "1-a"))
	cache.AddLateSequence(e(11, "foo6", "1-a"))
	log.Println("cache.lateLogs:", cache.lateLogs)
	cache.purgeLateLogEntries()
	assert.Equals(t, len(cache.lateLogs), 3)
	assert.Equals(t, cache.lateLogs[0].logEntry.Sequence, uint64(7))
	assert.Equals(t, cache.lateLogs[1].logEntry.Sequence, uint64(15))
	assert.Equals(t, cache.lateLogs[2].logEntry.Sequence, uint64(11))
	log.Println("cache.lateLogs:", cache.lateLogs)
	assert.True(t, err == nil)

	// Release the listener, and purge again
	cache.ReleaseLateSequenceClient(uint64(7))
	cache.purgeLateLogEntries()
	assert.Equals(t, len(cache.lateLogs), 1)
	assert.True(t, err == nil)

}

func TestLateSequenceHandlingWithMultipleListeners(t *testing.T) {

	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Test1", 0)
	assert.True(t, cache != nil)

	// Add Listener before late entries arrive
	startSequence := cache.InitLateSequenceClient()
	entries, lastSeq1, err := cache.GetLateSequencesSince(startSequence)
	assert.Equals(t, len(entries), 0)
	assert.Equals(t, lastSeq1, uint64(0))
	assert.True(t, err == nil)

	// Add two entries
	cache.AddLateSequence(e(5, "foo", "1-a"))
	cache.AddLateSequence(e(8, "foo2", "1-a"))

	// Add a second client.  Expect the first listener at [0], and the new one at [2]
	startSequence = cache.InitLateSequenceClient()
	entries, lastSeq2, err := cache.GetLateSequencesSince(startSequence)
	assert.Equals(t, startSequence, uint64(8))
	assert.Equals(t, lastSeq2, uint64(8))

	assert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))
	assert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))

	cache.AddLateSequence(e(3, "foo3", "1-a"))
	// First client requests again.  Expect first client at latest (3), second still at (8).
	entries, lastSeq1, err = cache.GetLateSequencesSince(lastSeq1)
	assert.Equals(t, lastSeq1, uint64(3))
	assert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))
	assert.Equals(t, cache.lateLogs[3].getListenerCount(), uint64(1))

	// Add another sequence, which triggers a purge.  Ensure we don't lose our listeners
	cache.AddLateSequence(e(12, "foo4", "1-a"))
	assert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))
	assert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(1))

	// Release the first listener - ensure we maintain the second
	cache.ReleaseLateSequenceClient(lastSeq1)
	assert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))
	assert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(0))

	// Release the second listener
	cache.ReleaseLateSequenceClient(lastSeq2)
	assert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(0))
	assert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(0))

}

// Create a document directly to the bucket with specific _sync metadata - used for
// simulating out-of-order arrivals on the tap feed using walrus.

func WriteDirect(db *Database, channelArray []string, sequence uint64) {
	docId := fmt.Sprintf("doc-%v", sequence)
	WriteDirectWithKey(db, docId, channelArray, sequence)
}

func WriteUserDirect(db *Database, username string, sequence uint64) {
	docId := fmt.Sprintf("_sync:user:%v", username)
	db.Bucket.Add(docId, 0, Body{"sequence": sequence, "name": username})
}

func WriteDirectWithKey(db *Database, key string, channelArray []string, sequence uint64) {

	rev := "1-a"
	chanMap := make(map[string]*channels.ChannelRemoval, 10)

	for _, channel := range channelArray {
		chanMap[channel] = nil
	}

	syncData := &syncData{
		CurrentRev: rev,
		Sequence:   sequence,
		Channels:   chanMap,
		TimeSaved:  time.Now(),
	}
	db.Bucket.Add(key, 0, Body{"_sync": syncData, "key": key})
}

// Create a document directly to the bucket with specific _sync metadata - used for
// simulating out-of-order arrivals on the tap feed using walrus.

func WriteDirectWithChannelGrant(db *Database, channelArray []string, sequence uint64, username string, channelGrantArray []string) {

	docId := fmt.Sprintf("doc-%v", sequence)
	rev := "1-a"
	chanMap := make(map[string]*channels.ChannelRemoval, 10)

	for _, channel := range channelArray {
		chanMap[channel] = nil
	}

	accessMap := make(map[string]channels.TimedSet)
	channelTimedSet := channels.AtSequence(base.SetFromArray(channelGrantArray), sequence)
	accessMap[username] = channelTimedSet

	syncData := &syncData{
		CurrentRev: rev,
		Sequence:   sequence,
		Channels:   chanMap,
		Access:     accessMap,
	}
	db.Bucket.Add(docId, 0, Body{"_sync": syncData, "key": docId})
}

// Test notification when buffered entries are processed after a user doc arrives.
func TestChannelCacheBufferingWithUserDoc(t *testing.T) {

	base.EnableLogKey("Cache")
	base.EnableLogKey("Cache+")
	base.EnableLogKey("Changes")
	base.EnableLogKey("Changes+")
	db := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Simulate seq 1 (user doc) being delayed - write 2 first
	WriteDirect(db, []string{"ABC"}, 2)

	// Start wait for doc in ABC
	waiter := db.tapListener.NewWaiterWithChannels(channels.SetOf("ABC"), nil)

	successChan := make(chan bool)
	go func() {
		waiter.Wait()
		close(successChan)
	}()

	// Simulate a user doc update
	WriteUserDirect(db, "bernard", 1)

	// Wait 3 seconds for notification, else fail the test.
	select {
	case <-successChan:
		log.Println("notification successful")
	case <-time.After(time.Second * 3):
		assertFailed(t, "No notification after 3 seconds")
	}

}

// Test backfill of late arriving sequences to the channel caches
func TestChannelCacheBackfill(t *testing.T) {

	base.EnableLogKey("Cache")
	base.EnableLogKey("Changes+")
	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "PBS", "NBC", "TBS"))
	authenticator.Save(user)

	// Simulate seq 3 being delayed - write 1,2,4,5
	WriteDirect(db, []string{"ABC", "NBC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"ABC", "PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	// Test that retrieval isn't blocked by skipped sequences
	db.changeCache.waitForSequenceID(SequenceID{Seq: 6})
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), ChangesOptions{Since: SequenceID{Seq: 0}})
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 4)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	lastSeq := changes[len(changes)-1].Seq

	// Validate insert to various cache states
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(db, []string{"CBS"}, 7)
	db.changeCache.waitForSequenceID(SequenceID{Seq: 7})
	// verify insert at start (PBS)
	pbsCache := db.changeCache.getChannelCache("PBS")
	assert.True(t, verifyCacheSequences(pbsCache, []uint64{3, 5, 6}))
	// verify insert at middle (ABC)
	abcCache := db.changeCache.getChannelCache("ABC")
	assert.True(t, verifyCacheSequences(abcCache, []uint64{1, 2, 3, 5, 6}))
	// verify insert at end (NBC)
	nbcCache := db.changeCache.getChannelCache("NBC")
	assert.True(t, verifyCacheSequences(nbcCache, []uint64{1, 3}))
	// verify insert to empty cache (TBS)
	tbsCache := db.changeCache.getChannelCache("TBS")
	assert.True(t, verifyCacheSequences(tbsCache, []uint64{3}))

	// verify changes has three entries (needs to resend all since previous LowSeq, which
	// will be the late arriver (3) along with 5, 6)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	assert.Equals(t, len(changes), 3)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 3, LowSeq: 3},
		ID:      "doc-3",
		Changes: []ChangeRev{{"rev": "1-a"}}})

}

// Test backfill of late arriving sequences to a continuous changes feed
func TestContinuousChangesBackfill(t *testing.T) {

	var logKeys = map[string]bool{
		"Sequences": true,
		"Cache":     true,
		"Changes+":  true,
	}

	base.UpdateLogKeys(logKeys, true)

	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "PBS", "NBC", "CBS"))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC", "NBC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"PBS"}, 5)
	WriteDirect(db, []string{"CBS"}, 6)

	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed
	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true

	defer close(options.Terminator)
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	assert.True(t, err == nil)

	time.Sleep(50 * time.Millisecond)

	// Write some more docs
	WriteDirect(db, []string{"CBS"}, 3)
	WriteDirect(db, []string{"PBS"}, 12)
	db.changeCache.waitForSequenceID(SequenceID{Seq: 12})

	// Test multiple backfill in single changes loop iteration
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "CBS"}, 4)
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "CBS"}, 7)
	WriteDirect(db, []string{"ABC", "PBS"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 13)
	db.changeCache.waitForSequenceID(SequenceID{Seq: 13})
	time.Sleep(50 * time.Millisecond)

	// We can't guarantee how compound sequences will be generated in a multi-core test - will
	// depend on timing of arrival in late sequence logs.  e.g. could come through as any one of
	// the following (where all are valid), depending on timing:
	//  ..."4","7","8","8::13"
	//  ..."4", "6::7", "6::8", "6::13"
	//  ..."3::4", "3::7", "3::8", "3::13"
	// For this reason, we're just verifying the number of sequences is correct

	// Validate that all docs eventually arrive
	expectedDocs := map[string]bool{
		"doc-1":  true,
		"doc-2":  true,
		"doc-3":  true,
		"doc-4":  true,
		"doc-5":  true,
		"doc-6":  true,
		"doc-7":  true,
		"doc-8":  true,
		"doc-12": true,
		"doc-13": true,
	}
	for {
		nextEntry, err := readNextFromFeed(feed, 5*time.Second)
		log.Printf("Read changes entry: %v", nextEntry)
		assertNoError(t, err, "Error reading next change entry from feed")
		if err != nil {
			break
		}
		if nextEntry == nil {
			continue
		}
		_, ok := expectedDocs[nextEntry.ID]
		if ok {
			delete(expectedDocs, nextEntry.ID)
		}
		if len(expectedDocs) == 0 {
			break
		}
	}
	if len(expectedDocs) > 0 {
		log.Printf("Did not receive expected docs: %v")
	}
	assert.Equals(t, len(expectedDocs), 0)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed
func TestLowSequenceHandling(t *testing.T) {

	var logKeys = map[string]bool{
		"Cache":    true,
		"Changes":  true,
		"Changes+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "PBS", "NBC", "TBS"))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC", "NBC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"ABC", "PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	db.changeCache.waitForSequenceID(SequenceID{Seq: 6})
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	defer close(options.Terminator)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	assert.True(t, err == nil)

	// Array to read changes from feed to support assertions
	var changes = make([]*ChangeEntry, 0, 50)

	time.Sleep(50 * time.Millisecond)
	err = appendFromFeed(&changes, feed, 4)

	// Validate the initial sequences arrive as expected
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 4)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	// Test backfill clear - sequence numbers go back to standard handling
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(db, []string{"ABC", "PBS"}, 4)

	db.changeCache.waitForSequenceWithMissing(4)

	time.Sleep(50 * time.Millisecond)
	err = appendFromFeed(&changes, feed, 2)
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 6)
	assert.True(t, verifyChangesSequencesIgnoreOrder(changes, []uint64{1, 2, 5, 6, 3, 4}))

	WriteDirect(db, []string{"ABC"}, 7)
	WriteDirect(db, []string{"ABC", "NBC"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 9)
	db.changeCache.waitForSequence(9)
	appendFromFeed(&changes, feed, 5)
	assert.True(t, verifyChangesSequencesIgnoreOrder(changes, []uint64{1, 2, 5, 6, 3, 4, 7, 8, 9}))

}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user doesn't have visibility to some of the late arriving sequences
func TestLowSequenceHandlingAcrossChannels(t *testing.T) {

	/*
		var logKeys = map[string]bool {
			"Cache": true,
			"Changes": true,
			"Changes+": true,
		}

		base.UpdateLogKeys(logKeys, true)
	*/

	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	db.changeCache.waitForSequence(6)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	assert.True(t, err == nil)

	// Go-routine to work the feed channel and write to an array for use by assertions
	var changes = make([]*ChangeEntry, 0, 50)

	time.Sleep(50 * time.Millisecond)
	err = appendFromFeed(&changes, feed, 3)

	// Validate the initial sequences arrive as expected
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 3)
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6"}))

	// Test backfill of sequence the user doesn't have visibility to
	WriteDirect(db, []string{"PBS"}, 3)
	WriteDirect(db, []string{"ABC"}, 9)

	db.changeCache.waitForSequenceWithMissing(9)

	time.Sleep(50 * time.Millisecond)
	err = appendFromFeed(&changes, feed, 1)
	assert.Equals(t, len(changes), 4)
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6", "3::9"}))

	close(options.Terminator)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user gets added to a new channel with existing entries (and existing backfill)
func TestLowSequenceHandlingWithAccessGrant(t *testing.T) {

	var logKeys = map[string]bool{
		"Sequence": true,
	}

	base.UpdateLogKeys(logKeys, true)

	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	db.changeCache.waitForSequence(6)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	assert.True(t, err == nil)

	// Go-routine to work the feed channel and write to an array for use by assertions
	var changes = make([]*ChangeEntry, 0, 50)

	time.Sleep(500 * time.Millisecond)

	// Validate the initial sequences arrive as expected
	err = appendFromFeed(&changes, feed, 3)
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 3)
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6"}))

	db.Bucket.Incr("_sync:seq", 7, 0, 0)
	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")

	WriteDirect(db, []string{"PBS"}, 9)

	db.changeCache.waitForSequence(9)

	time.Sleep(500 * time.Millisecond)
	err = appendFromFeed(&changes, feed, 4)
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 7)
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6", "2:8:5", "2:8:6", "2::8", "2::9"}))
	// Notes:
	// 1. 2::8 is the user sequence
	// 2. The duplicate send of sequence '6' is the standard behaviour when a channel is added - we don't know
	// whether the user has already seen the documents on the channel previously, so it gets resent

	close(options.Terminator)
}

// Test race condition causing skipped sequences in changes feed.  Channel feeds are processed sequentially
// in the main changes.go iteration loop, without a lock on the underlying channel caches.  The following
// sequence is possible while running a changes feed for channels "A", "B":
//    1. Sequence 100, Channel A arrives, and triggers changes loop iteration
//    2. Changes loop calls changes.changesFeed(A) - gets 100
//    3. Sequence 101, Channel A arrives
//    4. Sequence 102, Channel B arrives
//    5. Changes loop calls changes.changesFeed(B) - gets 102
//    6. Changes sends 100, 102, and sets since=102
// Here 101 is skipped, and never gets sent.  There are a number of ways a sufficient delay between #2 and #5
// could be introduced in a real-world scenario
//     - there are channels C,D,E,F,G with large caches that get processed between A and B
// To test, uncomment the following
// lines at the start of changesFeed() in changes.go to simulate slow processing:
//	    base.LogTo("Sequences", "Simulate slow processing time for channel %s - sleeping for 100 ms", channel)
//	    time.Sleep(100 * time.Millisecond)

// Test current fails intermittently on concurrent access to var changes.  Disabling for now - should be refactored.
func FailingTestChannelRace(t *testing.T) {

	var logKeys = map[string]bool{
		"Sequences": true,
	}

	base.UpdateLogKeys(logKeys, true)

	db := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channels "Odd", "Even"
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("Even", "Odd"))
	authenticator.Save(user)

	// Write initial sequences
	WriteDirect(db, []string{"Odd"}, 1)
	WriteDirect(db, []string{"Even"}, 2)
	WriteDirect(db, []string{"Odd"}, 3)

	db.changeCache.waitForSequence(3)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("Even", "Odd"), options)
	assert.True(t, err == nil)
	feedClosed := false

	// Go-routine to work the feed channel and write to an array for use by assertions
	var changes = make([]*ChangeEntry, 0, 50)
	go func() {
		for feedClosed == false {
			select {
			case entry, ok := <-feed:
				if ok {
					// feed sends nil after each continuous iteration
					if entry != nil {
						log.Println("Changes entry:", entry.Seq)
						changes = append(changes, entry)
					}
				} else {
					log.Println("Closing feed")
					feedClosed = true
				}
			}
		}
	}()

	// Wait for processing of two channels (100 ms each)
	time.Sleep(250 * time.Millisecond)
	// Validate the initial sequences arrive as expected
	assert.Equals(t, len(changes), 3)

	// Send update to trigger the start of the next changes iteration
	WriteDirect(db, []string{"Even"}, 4)
	time.Sleep(150 * time.Millisecond)
	// After read of "Even" channel, but before read of "Odd" channel, send three new entries
	WriteDirect(db, []string{"Odd"}, 5)
	WriteDirect(db, []string{"Even"}, 6)
	WriteDirect(db, []string{"Odd"}, 7)

	time.Sleep(100 * time.Millisecond)

	// At this point we've haven't sent sequence 6, but the continuous changes feed has since=7

	// Write a few more to validate that we're not catching up on the missing '6' later
	WriteDirect(db, []string{"Even"}, 8)
	WriteDirect(db, []string{"Odd"}, 9)
	time.Sleep(750 * time.Millisecond)
	assert.Equals(t, len(changes), 9)
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}))
	changesString := ""
	for _, change := range changes {
		changesString = fmt.Sprintf("%s%d, ", changesString, change.Seq.Seq)
	}
	fmt.Println("changes: ", changesString)

	close(options.Terminator)
}

// Test retrieval of skipped sequence using view.  Unit test catches panic, but we don't currently have a way
// to simulate an entry that makes it to the bucket (and so is accessible to the view), but doesn't show up on the TAP feed.
// Cache logging in this test validates that view retrieval is working because of TAP latency (item is in the bucket, but hasn't
// been seen on the TAP feed yet).  Longer term could consider enhancing leaky bucket to 'miss' the entry on the tap feed.
func TestSkippedViewRetrieval(t *testing.T) {

	var logKeys = map[string]bool{
		"Cache":  true,
		"Cache+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	// Use leaky bucket to have the tap feed 'lose' document 3
	leakyConfig := base.LeakyBucketConfig{
		TapFeedMissingDocs: []string{"doc-3"},
	}
	db := setupTestLeakyDBWithCacheOptions(t, shortWaitCache(), leakyConfig)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Allow db to initialize and run initial CleanSkippedSequenceQueue
	time.Sleep(10 * time.Millisecond)

	// Write sequences direct
	WriteDirect(db, []string{"ABC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"ABC"}, 3)

	changeCache, ok := db.changeCache.(*changeCache)
	assertTrue(t, ok, "Testing skipped sequences without a change cache")

	// Artificially add 3 skipped, and back date skipped entry by 2 hours to trigger attempted view retrieval during Clean call
	changeCache.skippedSeqs.Push(&SkippedSequence{3, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{5, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.CleanSkippedSequenceQueue()

	// Validate that 3 is in the channel cache, 5 isn't
	entries, err := db.changeCache.GetChanges("ABC", ChangesOptions{Since: SequenceID{Seq: 2}})
	assertNoError(t, err, "Get Changes returned error")
	assertTrue(t, len(entries) == 1, "Incorrect number of entries returned")
	assert.Equals(t, entries[0].DocID, "doc-3")

}

// Test that housekeeping goroutines get terminated when change cache is stopped
func TestStopChangeCache(t *testing.T) {
	// Setup short-wait cache to ensure cleanup goroutines fire often
	cacheOptions := CacheOptions{
		CachePendingSeqMaxWait: 10 * time.Millisecond,
		CachePendingSeqMaxNum:  50,
		CacheSkippedSeqMaxWait: 1 * time.Second}
	// Use leaky bucket to have the tap feed 'lose' document 3
	leakyConfig := base.LeakyBucketConfig{
		TapFeedMissingDocs: []string{"doc-3"},
	}
	db := setupTestLeakyDBWithCacheOptions(t, cacheOptions, leakyConfig)

	// Write sequences direct
	WriteDirect(db, []string{"ABC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"ABC"}, 3)

	changeCache, ok := db.changeCache.(*changeCache)
	assertTrue(t, ok, "Testing skipped sequences without a change cache")

	// Artificially add 3 skipped, and back date skipped entry by 2 hours to trigger attempted view retrieval during Clean call
	changeCache.skippedSeqLock.Lock()
	changeCache.skippedSeqs.Push(&SkippedSequence{3, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqLock.Unlock()

	// tear down the DB.  Should stop the cache before view retrieval of the skipped sequence is attempted.
	tearDownTestDB(t, db)

	// Hang around a while to see if the housekeeping tasks fire and panic
	time.Sleep(2 * time.Second)
}

// Test size config
func TestChannelCacheSize(t *testing.T) {

	base.EnableLogKey("Cache")
	channelOptions := ChannelCacheOptions{
		ChannelCacheMinLength: 600,
		ChannelCacheMaxLength: 600,
	}
	options := CacheOptions{
		ChannelCacheOptions: channelOptions,
	}

	log.Printf("Options in test:%+v", options)
	db := setupTestDBWithCacheOptions(t, options)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Write 750 docs to channel ABC
	for i := 1; i <= 750; i++ {
		WriteDirect(db, []string{"ABC"}, uint64(i))
	}

	// Validate that retrieval returns expected sequences
	db.changeCache.waitForSequence(750)
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("ABC"), ChangesOptions{Since: SequenceID{Seq: 0}})
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 750)

	// Validate that cache stores the expected number of values
	changeCache, ok := db.changeCache.(*changeCache)
	assertTrue(t, ok, "Testing skipped sequences without a change cache")
	abcCache := changeCache.channelCaches["ABC"]
	assert.Equals(t, len(abcCache.logs), 600)
}

func shortWaitCache() CacheOptions {

	return CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CachePendingSeqMaxNum:  50,
		CacheSkippedSeqMaxWait: 2 * time.Minute}
}

func verifySkippedSequences(queue SkippedSequenceQueue, sequences []uint64) bool {
	if len(queue) != len(sequences) {
		log.Printf("verifySkippedSequences: queue size (%v) not equals to sequences size (%v)",
			len(queue), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if queue[index].seq != seq {
			log.Printf("verifySkippedSequences: sequence mismatch at index %v, queue=%v, sequences=%v",
				index, queue[index].seq, seq)
			return false
		}
	}
	return true
}

func verifyCacheSequences(cache *channelCache, sequences []uint64) bool {
	if len(cache.logs) != len(sequences) {

		log.Printf("verifyCacheSequences: cache size (%v) not equals to sequences size (%v)",
			len(cache.logs), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if cache.logs[index].Sequence != seq {
			log.Printf("verifyCacheSequences: sequence mismatch at index %v, cache=%v, sequences=%v",
				index, cache.logs[index].Sequence, seq)
			return false
		}
	}
	return true
}

// verifyChangesFullSequences compares for a full match on sequence, including compound elements)
func verifyChangesFullSequences(changes []*ChangeEntry, sequences []string) bool {
	if len(changes) != len(sequences) {
		log.Printf("verifyChangesFullSequences: changes size (%v) not equals to sequences size (%v)",
			len(changes), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if changes[index].Seq.String() != seq {
			log.Printf("verifyChangesFullSequences: sequence mismatch at index %v, changes=%s, sequences=%s",
				index, changes[index].Seq.String(), seq)
			return false
		}
	}
	return true
}

// verifyChangesSequences compares for a match on sequence number only
func verifyChangesSequences(changes []*ChangeEntry, sequences []uint64) bool {
	if len(changes) != len(sequences) {
		log.Printf("verifyChangesSequences: changes size (%v) not equals to sequences size (%v)",
			len(changes), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if changes[index].Seq.Seq != seq {
			log.Printf("verifyChangesSequences: sequence mismatch at index %v, changes=%d, sequences=%d",
				index, changes[index].Seq.Seq, seq)
			return false
		}
	}
	return true
}

// verifyChangesSequencesIgnoreOrder compares for a match on sequence number only and ignores sequenceID order
func verifyChangesSequencesIgnoreOrder(changes []*ChangeEntry, sequences []uint64) bool {

	for _, seq := range sequences {
		matchingSequence := false
		for _, change := range changes {
			log.Printf("Change entry sequenceID = %d:%d:%d", change.Seq.LowSeq, change.Seq.TriggeredBy, change.Seq.Seq)
			if change.Seq.Seq == seq {
				matchingSequence = true
				break
			}
		}
		if !matchingSequence {
			log.Printf("verifyChangesSequencesIgnorerder: sequenceID %d missing in changes", seq)
			return false
		}
	}
	return true
}

func appendFromFeed(changes *[]*ChangeEntry, feed <-chan (*ChangeEntry), numEntries int) error {

	log.Println("Feed retrieving ", feed, numEntries)
	count := 0
	timeout := false
	for !timeout {
		select {
		case entry, ok := <-feed:
			if ok {
				if entry != nil {
					log.Println("Changes entry:", entry)
					*changes = append(*changes, entry)
					count++
				}
			} else {
				log.Println("Non-entry error")
				return errors.New("Non-entry returned on feed.")
			}
			if count == numEntries {
				log.Println("returned numEntries - returning")
				return nil
			}
		case <-time.After(time.Millisecond * 100):
			timeout = true
		}
	}
	if count != numEntries {
		log.Println("Miscount")
		return errors.New("Unable to return the requested number of entries")
	}
	log.Println("standard completion")
	return nil

}

func readNextFromFeed(feed <-chan (*ChangeEntry), timeout time.Duration) (*ChangeEntry, error) {

	select {
	case entry, ok := <-feed:
		if ok {
			return entry, nil
		} else {
			log.Printf("Non-entry error (%v): %v", ok, entry)
			return nil, errors.New("Non-entry returned on feed.")
		}
	case <-time.After(timeout):
		return nil, errors.New("Timeout waiting for entry")
	}

}
