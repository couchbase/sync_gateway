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
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func e(seq uint64, docid string, revid string) *LogEntry {
	return &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
	}
}

// Tombstoned entry
func et(seq uint64, docid string, revid string) *LogEntry {
	entry := e(seq, docid, revid)
	entry.SetDeleted()
	return entry
}

func testBucketContext() *DatabaseContext {

	context, _ := NewDatabaseContext("db", testBucket().Bucket, false, DatabaseContextOptions{})
	return context
}

func TestSkippedSequenceList(t *testing.T) {

	skipList := NewSkippedSequenceList()
	//Push values
	skipList.Push(&SkippedSequence{4, time.Now()})
	skipList.Push(&SkippedSequence{7, time.Now()})
	skipList.Push(&SkippedSequence{8, time.Now()})
	skipList.Push(&SkippedSequence{12, time.Now()})
	skipList.Push(&SkippedSequence{18, time.Now()})
	goassert.True(t, verifySkippedSequences(skipList, []uint64{4, 7, 8, 12, 18}))

	// Retrieval of low value
	goassert.Equals(t, skipList.getOldest(), uint64(4))

	// Removal of first value
	err := skipList.Remove(4)
	goassert.True(t, err == nil)
	goassert.True(t, verifySkippedSequences(skipList, []uint64{7, 8, 12, 18}))

	// Removal of middle values
	err = skipList.Remove(8)
	goassert.True(t, err == nil)
	goassert.True(t, verifySkippedSequences(skipList, []uint64{7, 12, 18}))

	err = skipList.Remove(12)
	goassert.True(t, err == nil)
	goassert.True(t, verifySkippedSequences(skipList, []uint64{7, 18}))

	// Removal of last value
	err = skipList.Remove(18)
	goassert.True(t, err == nil)
	goassert.True(t, verifySkippedSequences(skipList, []uint64{7}))

	// Removal of non-existent returns error
	err = skipList.Remove(25)
	goassert.True(t, err != nil)
	goassert.True(t, verifySkippedSequences(skipList, []uint64{7}))

	// Add an out-of-sequence entry (make sure bad sequencing doesn't throw us into an infinite loop)
	err = skipList.Push(&SkippedSequence{6, time.Now()})
	goassert.True(t, err != nil)
	skipList.Push(&SkippedSequence{9, time.Now()})
	goassert.True(t, err != nil)
	goassert.True(t, verifySkippedSequences(skipList, []uint64{7, 9}))
}

func TestLateSequenceHandling(t *testing.T) {

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)
	goassert.True(t, cache != nil)

	// Empty late sequence cache should return empty set
	startSequence := cache.InitLateSequenceClient()
	entries, lastSeq, err := cache.GetLateSequencesSince(startSequence)
	goassert.Equals(t, len(entries), 0)
	goassert.Equals(t, lastSeq, uint64(0))
	goassert.True(t, err == nil)

	cache.AddLateSequence(e(5, "foo", "1-a"))
	cache.AddLateSequence(e(8, "foo2", "1-a"))

	// Retrieve since 0
	entries, lastSeq, err = cache.GetLateSequencesSince(0)
	log.Println("entries:", entries)
	goassert.Equals(t, len(entries), 2)
	goassert.Equals(t, lastSeq, uint64(8))
	goassert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))
	goassert.True(t, err == nil)

	// Add Sequences.  Will trigger purge on old sequences without listeners
	cache.AddLateSequence(e(2, "foo3", "1-a"))
	cache.AddLateSequence(e(7, "foo4", "1-a"))
	goassert.Equals(t, len(cache.lateLogs), 3)
	goassert.Equals(t, cache.lateLogs[0].logEntry.Sequence, uint64(8))
	goassert.Equals(t, cache.lateLogs[1].logEntry.Sequence, uint64(2))
	goassert.Equals(t, cache.lateLogs[2].logEntry.Sequence, uint64(7))
	goassert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))

	// Retrieve since previous
	entries, lastSeq, err = cache.GetLateSequencesSince(lastSeq)
	log.Println("entries:", entries)
	goassert.Equals(t, len(entries), 2)
	goassert.Equals(t, lastSeq, uint64(7))
	goassert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(0))
	goassert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))
	log.Println("cache.lateLogs:", cache.lateLogs)
	goassert.True(t, err == nil)

	// Purge.  We have a listener sitting at seq=7, so purge should only clear previous
	cache.AddLateSequence(e(15, "foo5", "1-a"))
	cache.AddLateSequence(e(11, "foo6", "1-a"))
	log.Println("cache.lateLogs:", cache.lateLogs)
	cache.purgeLateLogEntries()
	goassert.Equals(t, len(cache.lateLogs), 3)
	goassert.Equals(t, cache.lateLogs[0].logEntry.Sequence, uint64(7))
	goassert.Equals(t, cache.lateLogs[1].logEntry.Sequence, uint64(15))
	goassert.Equals(t, cache.lateLogs[2].logEntry.Sequence, uint64(11))
	log.Println("cache.lateLogs:", cache.lateLogs)
	goassert.True(t, err == nil)

	// Release the listener, and purge again
	cache.ReleaseLateSequenceClient(uint64(7))
	cache.purgeLateLogEntries()
	goassert.Equals(t, len(cache.lateLogs), 1)
	goassert.True(t, err == nil)

}

func TestLateSequenceHandlingWithMultipleListeners(t *testing.T) {

	context := testBucketContext()
	defer context.Close()
	defer base.DecrNumOpenBuckets(context.Bucket.GetName())

	cache := newChannelCache(context, "Test1", 0)
	goassert.True(t, cache != nil)

	// Add Listener before late entries arrive
	startSequence := cache.InitLateSequenceClient()
	entries, lastSeq1, err := cache.GetLateSequencesSince(startSequence)
	goassert.Equals(t, len(entries), 0)
	goassert.Equals(t, lastSeq1, uint64(0))
	goassert.True(t, err == nil)

	// Add two entries
	cache.AddLateSequence(e(5, "foo", "1-a"))
	cache.AddLateSequence(e(8, "foo2", "1-a"))

	// Add a second client.  Expect the first listener at [0], and the new one at [2]
	startSequence = cache.InitLateSequenceClient()
	entries, lastSeq2, err := cache.GetLateSequencesSince(startSequence)
	goassert.Equals(t, startSequence, uint64(8))
	goassert.Equals(t, lastSeq2, uint64(8))

	goassert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))
	goassert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))

	cache.AddLateSequence(e(3, "foo3", "1-a"))
	// First client requests again.  Expect first client at latest (3), second still at (8).
	entries, lastSeq1, err = cache.GetLateSequencesSince(lastSeq1)
	goassert.Equals(t, lastSeq1, uint64(3))
	goassert.Equals(t, cache.lateLogs[2].getListenerCount(), uint64(1))
	goassert.Equals(t, cache.lateLogs[3].getListenerCount(), uint64(1))

	// Add another sequence, which triggers a purge.  Ensure we don't lose our listeners
	cache.AddLateSequence(e(12, "foo4", "1-a"))
	goassert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))
	goassert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(1))

	// Release the first listener - ensure we maintain the second
	cache.ReleaseLateSequenceClient(lastSeq1)
	goassert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(1))
	goassert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(0))

	// Release the second listener
	cache.ReleaseLateSequenceClient(lastSeq2)
	goassert.Equals(t, cache.lateLogs[0].getListenerCount(), uint64(0))
	goassert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(0))

}

// Create a document directly to the bucket with specific _sync metadata - used for
// simulating out-of-order arrivals on the tap feed using walrus.

func WriteDirect(db *Database, channelArray []string, sequence uint64) {
	docId := fmt.Sprintf("doc-%v", sequence)
	WriteDirectWithKey(db, docId, channelArray, sequence)
}

func WriteUserDirect(db *Database, username string, sequence uint64) {
	docId := base.UserPrefix + username
	db.Bucket.Add(docId, 0, Body{"sequence": sequence, "name": username})
}

func WriteDirectWithKey(db *Database, key string, channelArray []string, sequence uint64) {

	if base.TestUseXattrs() {
		panic(fmt.Sprintf("WriteDirectWithKey() cannot be used in tests that are xattr enabled"))
	}

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
	db.Bucket.Add(key, 0, Body{base.SyncXattrName: syncData, "key": key})
}

// Create a document directly to the bucket with specific _sync metadata - used for
// simulating out-of-order arrivals on the tap feed using walrus.

func WriteDirectWithChannelGrant(db *Database, channelArray []string, sequence uint64, username string, channelGrantArray []string) {

	if base.TestUseXattrs() {
		panic(fmt.Sprintf("WriteDirectWithKey() cannot be used in tests that are xattr enabled"))
	}

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
	db.Bucket.Add(docId, 0, Body{base.SyncXattrName: syncData, "key": docId})
}

// Test notification when buffered entries are processed after a user doc arrives.
func TestChannelCacheBufferingWithUserDoc(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges|base.KeyDCP)()

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer tearDownTestDB(t, db)
	defer testBucket.Close()
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Simulate seq 1 (user doc) being delayed - write 2 first
	WriteDirect(db, []string{"ABC"}, 2)

	// Start wait for doc in ABC
	waiter := db.mutationListener.NewWaiterWithChannels(channels.SetOf("ABC"), nil)

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
		t.Fatal("No notification after 3 seconds")
	}

}

// Test backfill of late arriving sequences to the channel caches
func TestChannelCacheBackfill(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()
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
	db.changeCache.waitForSequenceID(SequenceID{Seq: 6}, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.NoError(t, err, "Couldn't GetChanges")
	goassert.Equals(t, len(changes), 4)
	goassert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	lastSeq := changes[len(changes)-1].Seq

	// Validate insert to various cache states
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(db, []string{"CBS"}, 7)
	db.changeCache.waitForSequenceID(SequenceID{Seq: 7}, base.DefaultWaitForSequenceTesting)
	// verify insert at start (PBS)
	pbsCache := db.changeCache.getChannelCache("PBS")
	goassert.True(t, verifyCacheSequences(pbsCache, []uint64{3, 5, 6}))
	// verify insert at middle (ABC)
	abcCache := db.changeCache.getChannelCache("ABC")
	goassert.True(t, verifyCacheSequences(abcCache, []uint64{1, 2, 3, 5, 6}))
	// verify insert at end (NBC)
	nbcCache := db.changeCache.getChannelCache("NBC")
	goassert.True(t, verifyCacheSequences(nbcCache, []uint64{1, 3}))
	// verify insert to empty cache (TBS)
	tbsCache := db.changeCache.getChannelCache("TBS")
	goassert.True(t, verifyCacheSequences(tbsCache, []uint64{3}))

	// verify changes has three entries (needs to resend all since previous LowSeq, which
	// will be the late arriver (3) along with 5, 6)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	goassert.Equals(t, len(changes), 3)
	goassert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 3, LowSeq: 3},
		ID:      "doc-3",
		Changes: []ChangeRev{{"rev": "1-a"}}})

}

// Test backfill of late arriving sequences to a continuous changes feed
func TestContinuousChangesBackfill(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache|base.KeyChanges|base.KeyDCP)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

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
	goassert.True(t, err == nil)

	time.Sleep(50 * time.Millisecond)

	// Write some more docs
	WriteDirect(db, []string{"CBS"}, 3)
	WriteDirect(db, []string{"PBS"}, 12)
	db.changeCache.waitForSequenceID(SequenceID{Seq: 12}, base.DefaultWaitForSequenceTesting)

	// Test multiple backfill in single changes loop iteration
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "CBS"}, 4)
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "CBS"}, 7)
	WriteDirect(db, []string{"ABC", "PBS"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 13)
	db.changeCache.waitForSequenceID(SequenceID{Seq: 13}, base.DefaultWaitForSequenceTesting)
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
		assert.NoError(t, err, "Error reading next change entry from feed")
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
		log.Printf("Received %d unexpected docs", len(expectedDocs))
	}

	goassert.Equals(t, len(expectedDocs), 0)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed
func TestLowSequenceHandling(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges|base.KeyQuery)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	assert.True(t, authenticator != nil, "db.Authenticator() returned nil")
	user, err := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "PBS", "NBC", "TBS"))
	assert.NoError(t, err, fmt.Sprintf("Error creating new user: %v", err))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC", "NBC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"ABC", "PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	db.changeCache.waitForSequenceID(SequenceID{Seq: 6}, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	defer close(options.Terminator)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	goassert.True(t, err == nil)

	changes, err := verifySequencesInFeed(feed, []uint64{1, 2, 5, 6})
	goassert.True(t, err == nil)
	goassert.Equals(t, len(changes), 4)
	goassert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	// Test backfill clear - sequence numbers go back to standard handling
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(db, []string{"ABC", "PBS"}, 4)

	_, err = verifySequencesInFeed(feed, []uint64{3, 4})
	goassert.True(t, err == nil)

	WriteDirect(db, []string{"ABC"}, 7)
	WriteDirect(db, []string{"ABC", "NBC"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 9)
	_, err = verifySequencesInFeed(feed, []uint64{7, 8, 9})
	goassert.True(t, err == nil)

}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user doesn't have visibility to some of the late arriving sequences
func TestLowSequenceHandlingAcrossChannels(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges|base.KeyQuery)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, err := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	assert.NoError(t, err, fmt.Sprintf("db.Authenticator() returned err: %v", err))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	db.changeCache.waitForSequence(6, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	goassert.True(t, err == nil)

	_, err = verifySequencesInFeed(feed, []uint64{1, 2, 6})
	goassert.True(t, err == nil)

	// Test backfill of sequence the user doesn't have visibility to
	WriteDirect(db, []string{"PBS"}, 3)
	WriteDirect(db, []string{"ABC"}, 9)

	_, err = verifySequencesInFeed(feed, []uint64{9})
	goassert.True(t, err == nil)

	close(options.Terminator)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user gets added to a new channel with existing entries (and existing backfill)
func TestLowSequenceHandlingWithAccessGrant(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyChanges|base.KeyQuery)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

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

	db.changeCache.waitForSequence(6, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	goassert.True(t, err == nil)

	// Go-routine to work the feed channel and write to an array for use by assertions
	var changes = make([]*ChangeEntry, 0, 50)

	time.Sleep(500 * time.Millisecond)

	// Validate the initial sequences arrive as expected
	err = appendFromFeed(&changes, feed, 3, base.DefaultWaitForSequenceTesting)
	goassert.True(t, err == nil)
	goassert.Equals(t, len(changes), 3)
	goassert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6"}))

	_, incrErr := db.Bucket.Incr(base.SyncSeqKey, 7, 7, 0)
	goassert.True(t, incrErr == nil)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipal("naomi", true)
	goassert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assert.NoError(t, err, "UpdatePrincipal failed")

	WriteDirect(db, []string{"PBS"}, 9)

	db.changeCache.waitForSequence(9, base.DefaultWaitForSequenceTesting)

	time.Sleep(500 * time.Millisecond)
	err = appendFromFeed(&changes, feed, 4, base.DefaultWaitForSequenceTesting)
	assert.NoError(t, err, "Expected more changes to be sent on feed, but never received")
	goassert.Equals(t, len(changes), 7)
	goassert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6", "2:8:5", "2:8:6", "2::8", "2::9"}))
	// Notes:
	// 1. 2::8 is the user sequence
	// 2. The duplicate send of sequence '6' is the standard behaviour when a channel is added - we don't know
	// whether the user has already seen the documents on the channel previously, so it gets resent

	close(options.Terminator)
}

// Tests channel cache backfill with slow query, validates that a request that is terminated while
// waiting for the view lock doesn't trigger a view query.  Runs multiple goroutines, using a channel
// and two waitgroups to ensure expected ordering of events, as follows
//    1. Define a PostQueryCallback (via leaky bucket) that does two things:
//        - closes a notification channel (queryBlocked) to indicate that the initial query is blocking
//        - blocks via a WaitGroup (queryWg)
//    2. Start a goroutine to issue a changes request
//        - when view executes, will trigger callback handling above
//    3. Start a second goroutine to issue another changes request.
//        - will block waiting for viewLock, which increments StatKeyChannelCachePendingQueries stat
//    4. Wait until StatKeyChannelCachePendingQueries is incremented
//    5. Terminate second changes request
//    6. Unblock the initial view query
//       - releases the view lock, second changes request is unblocked
//       - since it's been terminated, should return error before executing a second view query
func TestChannelQueryCancellation(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency test when running in integration")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	// Set up PostQueryCallback on bucket - will be invoked when changes triggers the cache backfill view query

	// Use queryWg to pause the query
	var queryWg sync.WaitGroup
	queryWg.Add(1)

	// Use queryBlocked to detect when the first called has reached queryWg.Wait
	queryBlocked := make(chan struct{})

	// Use changesWg to block until test goroutines are both complete
	var changesWg sync.WaitGroup

	postQueryCallback := func(ddoc, viewName string, params map[string]interface{}) {
		close(queryBlocked) // Notifies that a query is blocked, trigger to initiate second changes request
		queryWg.Wait()      // Waits until second changes request attempts to make a view query
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		PostQueryCallback: postQueryCallback,
	}

	db := setupTestLeakyDBWithCacheOptions(t, CacheOptions{}, queryCallbackConfig)
	db.ChannelMapper = channels.NewDefaultChannelMapper()
	defer tearDownTestDB(t, db)

	// Write a handful of docs/sequences to the bucket
	_, err := db.Put("key1", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	_, err = db.Put("key2", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	_, err = db.Put("key3", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	_, err = db.Put("key4", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	db.changeCache.waitForSequence(4, base.DefaultWaitForSequenceTesting)

	// Flush the cache, to ensure view query on subsequent changes requests
	db.FlushChannelCache()

	// Issue two one-shot since=0 changes request.  Both will attempt a view query.  The first will block based on queryWg,
	// the second will block waiting for the view lock
	initialQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	changesWg.Add(1)
	go func() {
		defer changesWg.Done()
		var options ChangesOptions
		options.Since = SequenceID{Seq: 0}
		options.Continuous = false
		options.Wait = false
		options.Limit = 2 // Avoid prepending results in cache, as we don't want second changes to serve results from cache
		_, err := db.GetChanges(base.SetOf("ABC"), options)
		assert.NoError(t, err, "Expect no error for first changes request")
	}()

	// Wait for queryBlocked=true - ensures the first goroutine has acquired view lock
	select {
	case <-queryBlocked:
		// continue
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Changes goroutine failed to initiate view query in 10 seconds.")
	}

	initialPendingQueries := base.ExpvarVar2Int(db.DbStats.StatsCache().Get(base.StatKeyChannelCachePendingQueries))

	// Start a second goroutine that should block waiting for the view lock
	changesTerminator := make(chan bool)
	changesWg.Add(1)
	go func() {
		defer changesWg.Done()
		var options ChangesOptions
		options.Since = SequenceID{Seq: 0}
		options.Terminator = changesTerminator
		options.Continuous = false
		options.Limit = 2
		options.Wait = false
		_, err := db.GetChanges(base.SetOf("ABC"), options)
		assert.Error(t, err, "Expected error for second changes")
	}()

	// wait for second goroutine to be queued for the view lock (based on expvar)
	var pendingQueries int64
	for i := 0; i < 1000; i++ {
		pendingQueries = base.ExpvarVar2Int(db.DbStats.StatsCache().Get(base.StatKeyChannelCachePendingQueries))
		if pendingQueries > initialPendingQueries {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, pendingQueries > initialPendingQueries, "Pending queries (%d) didn't exceed initialPendingQueries (%d) after 10s", pendingQueries, initialPendingQueries)

	// Terminate the second changes request
	close(changesTerminator)

	// Unblock the first goroutine
	queryWg.Done()

	// Wait for both goroutines to complete and evaluate their asserts
	changesWg.Wait()

	// Validate only a single query was executed
	finalQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, initialQueryCount+1, finalQueryCount)
}

func TestLowSequenceHandlingNoDuplicates(t *testing.T) {
	// TODO: Disabled until https://github.com/couchbase/sync_gateway/issues/3056 is fixed.
	t.Skip("WARNING: TEST DISABLED")

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyChanges|base.KeyCache)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	assert.True(t, authenticator != nil, "db.Authenticator() returned nil")
	user, err := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC", "PBS", "NBC", "TBS"))
	assert.NoError(t, err, fmt.Sprintf("Error creating new user: %v", err))
	authenticator.Save(user)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(db, []string{"ABC", "NBC"}, 1)
	WriteDirect(db, []string{"ABC"}, 2)
	WriteDirect(db, []string{"ABC", "PBS"}, 5)
	WriteDirect(db, []string{"ABC", "PBS"}, 6)

	db.changeCache.waitForSequenceID(SequenceID{Seq: 6}, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	defer close(options.Terminator)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
	goassert.True(t, err == nil)

	// Array to read changes from feed to support assertions
	var changes = make([]*ChangeEntry, 0, 50)

	err = appendFromFeed(&changes, feed, 4, base.DefaultWaitForSequenceTesting)

	// Validate the initial sequences arrive as expected
	goassert.True(t, err == nil)
	goassert.Equals(t, len(changes), 4)
	goassert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	// Test backfill clear - sequence numbers go back to standard handling
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(db, []string{"ABC", "PBS"}, 4)

	db.changeCache.waitForSequenceWithMissing(4, base.DefaultWaitForSequenceTesting)

	err = appendFromFeed(&changes, feed, 2, base.DefaultWaitForSequenceTesting)
	goassert.True(t, err == nil)
	goassert.Equals(t, len(changes), 6)
	goassert.True(t, verifyChangesSequencesIgnoreOrder(changes, []uint64{1, 2, 5, 6, 3, 4}))

	WriteDirect(db, []string{"ABC"}, 7)
	WriteDirect(db, []string{"ABC", "NBC"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 9)
	db.changeCache.waitForSequence(9, base.DefaultWaitForSequenceTesting)
	appendFromFeed(&changes, feed, 5, base.DefaultWaitForSequenceTesting)
	goassert.True(t, verifyChangesSequencesIgnoreOrder(changes, []uint64{1, 2, 5, 6, 3, 4, 7, 8, 9}))

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
//	    base.Infof(base.KeyChanges, "Simulate slow processing time for channel %s - sleeping for 100 ms", channel)
//	    time.Sleep(100 * time.Millisecond)
func TestChannelRace(t *testing.T) {
	// TODO: Test current fails intermittently on concurrent access to var changes.
	// Disabling for now - should be refactored.
	t.Skip("WARNING: TEST DISABLED")

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges)()

	db, testBucket := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channels "Odd", "Even"
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("Even", "Odd"))
	authenticator.Save(user)

	// Write initial sequences
	WriteDirect(db, []string{"Odd"}, 1)
	WriteDirect(db, []string{"Even"}, 2)
	WriteDirect(db, []string{"Odd"}, 3)

	db.changeCache.waitForSequence(3, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	options.Terminator = make(chan bool)
	options.Continuous = true
	options.Wait = true
	feed, err := db.MultiChangesFeed(base.SetOf("Even", "Odd"), options)
	goassert.True(t, err == nil)
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
	goassert.Equals(t, len(changes), 3)

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
	goassert.Equals(t, len(changes), 9)
	goassert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}))
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

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache)()

	originalBatchSize := SkippedSeqCleanViewBatch
	SkippedSeqCleanViewBatch = 4
	defer func() {
		SkippedSeqCleanViewBatch = originalBatchSize
	}()

	// Use leaky bucket to have the tap feed 'lose' document 3
	leakyConfig := base.LeakyBucketConfig{
		TapFeedMissingDocs: []string{"doc-3", "doc-7", "doc-10", "doc-13", "doc-14"},
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
	WriteDirect(db, []string{"ABC"}, 7)
	WriteDirect(db, []string{"ABC"}, 10)
	WriteDirect(db, []string{"ABC"}, 13)
	WriteDirect(db, []string{"ABC"}, 14)
	WriteDirect(db, []string{"ABC"}, 15)

	//db.Options.UnsupportedOptions.DisableCleanSkippedQuery = true
	changeCache, ok := db.changeCache.(*changeCache)
	assert.True(t, ok, "Testing skipped sequences without a change cache")

	// Artificially add skipped sequences to queue, and back date skipped entry by 2 hours to trigger attempted view retrieval during Clean call
	// Sequences '3', '7', '10', '13' and '14' exist, should be found.
	changeCache.skippedSeqs.Push(&SkippedSequence{3, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{5, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{6, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{7, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{10, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{11, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{12, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{13, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.skippedSeqs.Push(&SkippedSequence{14, time.Now().Add(time.Duration(time.Hour * -2))})
	changeCache.CleanSkippedSequenceQueue()

	// Validate expected entries
	db.changeCache.waitForSequenceID(SequenceID{Seq: 15}, base.DefaultWaitForSequenceTesting)
	entries, err := db.changeCache.GetChanges("ABC", ChangesOptions{Since: SequenceID{Seq: 2}})
	assert.NoError(t, err, "Get Changes returned error")
	goassert.Equals(t, len(entries), 6)
	log.Printf("entries: %v", entries)
	if len(entries) == 6 {
		goassert.Equals(t, entries[0].DocID, "doc-3")
		goassert.Equals(t, entries[1].DocID, "doc-7")
		goassert.Equals(t, entries[2].DocID, "doc-10")
		goassert.Equals(t, entries[3].DocID, "doc-13")
		goassert.Equals(t, entries[4].DocID, "doc-14")
		goassert.Equals(t, entries[5].DocID, "doc-15")
	}

}

// Test that housekeeping goroutines get terminated when change cache is stopped
func TestStopChangeCache(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyChanges|base.KeyDCP)()

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

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
	assert.True(t, ok, "Testing skipped sequences without a change cache")

	// Artificially add 3 skipped, and back date skipped entry by 2 hours to trigger attempted view retrieval during Clean call
	changeCache.skippedSeqs.Push(&SkippedSequence{3, time.Now().Add(time.Duration(time.Hour * -2))})

	// tear down the DB.  Should stop the cache before view retrieval of the skipped sequence is attempted.
	tearDownTestDB(t, db)

	// Hang around a while to see if the housekeeping tasks fire and panic
	time.Sleep(2 * time.Second)

}

// Test size config
func TestChannelCacheSize(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to calling WriteDirect().  Skipping.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache)()

	channelOptions := ChannelCacheOptions{
		ChannelCacheMinLength: 600,
		ChannelCacheMaxLength: 600,
	}
	options := CacheOptions{
		ChannelCacheOptions: channelOptions,
	}

	log.Printf("Options in test:%+v", options)
	db, testBucket := setupTestDBWithCacheOptions(t, options)
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

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
	db.changeCache.waitForSequence(750, base.DefaultWaitForSequenceTesting)
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("ABC"), ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.NoError(t, err, "Couldn't GetChanges")
	goassert.Equals(t, len(changes), 750)

	// Validate that cache stores the expected number of values
	changeCache, ok := db.changeCache.(*changeCache)
	assert.True(t, ok, "Testing skipped sequences without a change cache")
	abcCache := changeCache.channelCaches["ABC"]
	goassert.Equals(t, len(abcCache.logs), 600)
}

func shortWaitCache() CacheOptions {

	return CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CachePendingSeqMaxNum:  50,
		CacheSkippedSeqMaxWait: 2 * time.Minute}
}

func verifySkippedSequences(list *SkippedSequenceList, sequences []uint64) bool {
	if len(list.skippedMap) != len(sequences) {
		log.Printf("verifySkippedSequences: skippedMap size (%v) not equals to sequences size (%v)",
			len(list.skippedMap), len(sequences))
		return false
	}

	i := -1
	for e := list.skippedList.Front(); e != nil; e = e.Next() {
		i++
		skippedSeq := e.Value.(*SkippedSequence)
		if skippedSeq.seq != sequences[i] {
			log.Printf("verifySkippedSequences: sequence mismatch at index %v, queue=%v, sequences=%v",
				i, skippedSeq.seq, sequences[i])
			return false
		}
	}
	if i != len(sequences)-1 {
		log.Printf("verifySkippedSequences: skippedList size (%d) not equals to sequences size (%d)",
			i+1, len(sequences))
		return false
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

// Reads changes from the feed, one at a time, until the expected sequences are received.  If
// appendFromFeed times out before the full set of expected sequences are found, returns error.
func verifySequencesInFeed(feed <-chan (*ChangeEntry), sequences []uint64) ([]*ChangeEntry, error) {
	log.Printf("Attempting to verify sequences %v in changes feed", sequences)
	var changes = make([]*ChangeEntry, 0, 50)
	for {
		// Attempt to read at one entry from feed
		err := appendFromFeed(&changes, feed, 1, base.DefaultWaitForSequenceTesting)
		if err != nil {
			return nil, err
		}
		// Check if we've received the required sequences
		if verifyChangesSequencesIgnoreOrder(changes, sequences) {
			return changes, nil
		}
	}
}

func appendFromFeed(changes *[]*ChangeEntry, feed <-chan (*ChangeEntry), numEntries int, maxWaitTime time.Duration) error {

	count := 0
	timeout := false
	for !timeout {
		select {
		case entry, ok := <-feed:
			if ok {
				if entry != nil {
					*changes = append(*changes, entry)
					count++
				}
			} else {
				return errors.New("Non-entry returned on feed.")
			}
			if count == numEntries {
				return nil
			}
		case <-time.After(maxWaitTime):
			timeout = true
		}
	}
	if count != numEntries {
		return fmt.Errorf("appendFromFeed expected %d entries but only received %d.  Timeout: %v", numEntries, count, timeout)
	}
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

// Repro SG #2633
//
// Create doc1 w/ unused sequences 1, actual sequence 3.
// Create doc2 w/ sequence 2, channel ABC
// Send feed event for doc2. This won't trigger notifyChange, as buffering is waiting for seq 1
// Send feed event for doc1. This should trigger caching for doc2, and trigger notifyChange for channel ABC.
//
// Verify that notifyChange for channel ABC was sent.
func TestLateArrivingSequenceTriggersOnChange(t *testing.T) {

	// -------- Test setup ----------------

	if base.TestUseXattrs() {
		t.Skip("This test only works in channel cache mode")
	}

	// Enable relevant logging
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache|base.KeyChanges)()

	// Create a test db that uses channel cache
	channelOptions := ChannelCacheOptions{
		ChannelCacheMinLength: 600,
		ChannelCacheMaxLength: 600,
	}
	options := CacheOptions{
		ChannelCacheOptions: channelOptions,
	}
	db, testBucket := setupTestDBWithCacheOptions(t, options)
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

	// -------- Setup notifyChange callback ----------------

	// type assert this from ChangeIndex interface -> concrete changeCache implementation
	changeCacheImpl := db.changeCache.(*changeCache)

	//  Detect whether the 2nd was ignored using an notifyChange listener callback and make sure it was not added to the ABC channel
	waitForOnChangeCallback := sync.WaitGroup{}
	waitForOnChangeCallback.Add(1)
	changeCacheImpl.notifyChange = func(channels base.Set) {
		// defer waitForOnChangeCallback.Done()
		log.Printf("channelsChanged: %v", channels)
		// goassert.True(t, channels.Contains("ABC"))
		if channels.Contains("ABC") {
			waitForOnChangeCallback.Done()
		}

	}

	// -------- Perform actions ----------------

	// Create doc1 w/ unused sequences 1, actual sequence 3.
	doc1Id := "doc1Id"
	doc1 := document{
		ID: doc1Id,
	}
	doc1.syncData = syncData{
		UnusedSequences: []uint64{
			1,
		},
		CurrentRev: "1-abc",
		Sequence:   3,
	}
	doc1Bytes, err := doc1.MarshalJSON()
	assert.NoError(t, err, "Unexpected error")

	// Create doc2 w/ sequence 2, channel ABC
	doc2Id := "doc2Id"
	doc2 := document{
		ID: doc2Id,
	}
	channelMap := channels.ChannelMap{
		"ABC": nil,
	}
	doc2.syncData = syncData{
		CurrentRev: "1-cde",
		Sequence:   2,
		Channels:   channelMap,
	}
	doc2Bytes, err := doc2.MarshalJSON()
	assert.NoError(t, err, "Unexpected error")

	// Send feed event for doc2. This won't trigger notifyChange, as buffering is waiting for seq 1
	feedEventDoc2 := sgbucket.FeedEvent{
		Synchronous: true,
		Key:         []byte(doc2Id),
		Value:       doc2Bytes,
	}
	db.changeCache.DocChanged(feedEventDoc2)

	// Send feed event for doc1. This should trigger caching for doc2, and trigger notifyChange for channel ABC.
	feedEventDoc1 := sgbucket.FeedEvent{
		Synchronous: true,
		Key:         []byte(doc1Id),
		Value:       doc1Bytes,
	}
	db.changeCache.DocChanged(feedEventDoc1)

	// -------- Wait for waitgroup ----------------

	// Block until the notifyChange callback was invoked with the expected channels.
	// If the callback is never called back with expected, will block forever.
	waitForOnChangeCallback.Wait()

}
