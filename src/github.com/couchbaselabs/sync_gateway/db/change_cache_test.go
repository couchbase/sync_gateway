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
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"

	"github.com/couchbaselabs/go.assert"
)

func e(seq uint64, docid string, revid string) *LogEntry {
	return &LogEntry{
		Sequence: seq,
		DocID:    docid,
		RevID:    revid,
	}
}

func TestSkippedSequenceQueue(t *testing.T) {

	var skipQueue SkippedSequenceQueue
	//Push values
	skipQueue.Push(SkippedSequence{4, time.Now()})
	skipQueue.Push(SkippedSequence{7, time.Now()})
	skipQueue.Push(SkippedSequence{8, time.Now()})
	skipQueue.Push(SkippedSequence{12, time.Now()})
	skipQueue.Push(SkippedSequence{18, time.Now()})

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
}

func TestLateSequenceHandling(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{})
	cache := &channelCache{context: context, channelName: "Test1", validFrom: 0}
	assert.True(t, cache != nil)

	// Empty late sequence cache should return empty set
	entries, lastSeq, err := cache.GetLateSequencesSince(0)
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
	assert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(1))
	assert.True(t, err == nil)

	// Retrieve since previous
	cache.AddLateSequence(e(2, "foo3", "1-a"))
	cache.AddLateSequence(e(7, "foo4", "1-a"))
	entries, lastSeq, err = cache.GetLateSequencesSince(lastSeq)
	log.Println("entries:", entries)
	assert.Equals(t, len(entries), 2)
	assert.Equals(t, lastSeq, uint64(7))
	assert.Equals(t, cache.lateLogs[1].getListenerCount(), uint64(0))
	assert.Equals(t, cache.lateLogs[3].getListenerCount(), uint64(1))
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
	cache.ReleaseLateSequence(uint64(7))
	cache.purgeLateLogEntries()
	assert.Equals(t, len(cache.lateLogs), 0)
	log.Println("cache.lateLogs:", cache.lateLogs)
	assert.True(t, err == nil)

}

// Create a document directly to the bucket with specific _sync metadata - used for
// simulating out-of-order arrivals on the tap feed using walrus.

func WriteDirect(db *Database, channelArray []string, sequence uint64) {

	docId := fmt.Sprintf("doc-%v", sequence)
	rev := "1-a"
	chanMap := make(map[string]*channels.ChannelRemoval, 10)

	for _, channel := range channelArray {
		chanMap[channel] = nil
	}

	syncData := &syncData{
		CurrentRev: rev,
		Sequence:   sequence,
		Channels:   chanMap,
	}
	db.Bucket.Add(docId, 0, Body{"_sync": syncData, "key": docId})
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

// Test backfill of late arriving sequences to the channel caches
func TestChannelCacheBackfill(t *testing.T) {

	base.LogKeys["Cache"] = true
	base.LogKeys["Changes"] = true
	base.LogKeys["Changes+"] = true
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
	db.changeCache.waitForSequence(6)
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
	db.changeCache.waitForSequence(7)
	// verify insert at start (PBS)
	pbsCache := db.changeCache.channelCaches["PBS"]
	assert.True(t, verifyCacheSequences(pbsCache, []uint64{3, 5, 6}))
	// verify insert at middle (ABC)
	abcCache := db.changeCache.channelCaches["ABC"]
	assert.True(t, verifyCacheSequences(abcCache, []uint64{1, 2, 3, 5, 6}))
	// verify insert at end (NBC)
	nbcCache := db.changeCache.channelCaches["NBC"]
	assert.True(t, verifyCacheSequences(nbcCache, []uint64{1, 3}))
	// verify insert to empty cache (TBS)
	tbsCache := db.changeCache.channelCaches["TBS"]
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

	base.LogKeys["Sequences"] = true
	//base.LogKeys["Cache"] = true
	//base.LogKeys["Changes"] = true
	//base.LogKeys["Changes+"] = true
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

	time.Sleep(50 * time.Millisecond)
	// Validate the initial sequences arrive as expected
	assert.Equals(t, len(changes), 4)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:      "doc-1",
		Changes: []ChangeRev{{"rev": "1-a"}}})

	WriteDirect(db, []string{"CBS"}, 3)
	WriteDirect(db, []string{"PBS"}, 12)

	db.changeCache.waitForSequence(12)

	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, len(changes), 6)
	assert.True(t, verifyChangesSequences(changes, []string{
		"1", "2", "2::5", "2::6", "3", "3::12"}))
	// Test multiple backfill in single changes loop iteration
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "CBS"}, 4)
	WriteDirect(db, []string{"ABC", "NBC", "PBS", "CBS"}, 7)
	WriteDirect(db, []string{"ABC", "PBS"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 13)
	db.changeCache.waitForSequence(13)

	assert.Equals(t, len(changes), 10)
	assert.True(t, verifyChangesSequences(changes, []string{
		"1", "2", "2::5", "2::6", "3", "3::12", "4", "7", "8", "8::13"}))

	close(options.Terminator)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed
func TestLowSequenceHandling(t *testing.T) {

	//base.LogKeys["Cache"] = true
	//base.LogKeys["Changes"] = true
	//base.LogKeys["Changes+"] = true
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
					log.Println("Closing Feed")
					feedClosed = true
				}
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	// Validate the initial sequences arrive as expected
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
	assert.Equals(t, len(changes), 6)
	assert.True(t, verifyChangesSequences(changes, []string{"1", "2", "2::5", "2::6", "3", "4"}))

	WriteDirect(db, []string{"ABC"}, 7)
	WriteDirect(db, []string{"ABC", "NBC"}, 8)
	WriteDirect(db, []string{"ABC", "PBS"}, 9)
	db.changeCache.waitForSequence(9)
	assert.Equals(t, len(changes), 9)
	assert.True(t, verifyChangesSequences(changes, []string{"1", "2", "2::5", "2::6", "3", "4", "7", "8", "9"}))

	close(options.Terminator)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user doesn't have visibility to some of the late arriving sequences
func TestLowSequenceHandlingAcrossChannels(t *testing.T) {

	//base.LogKeys["Cache"] = true
	//base.LogKeys["Changes"] = true
	//base.LogKeys["Changes+"] = true
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
					log.Println("Closing Feed")
					feedClosed = true
				}
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	// Validate the initial sequences arrive as expected
	assert.Equals(t, len(changes), 3)
	assert.True(t, verifyChangesSequences(changes, []string{"1", "2", "2::6"}))

	// Test backfill of sequence the user doesn't have visibility to
	WriteDirect(db, []string{"PBS"}, 3)
	WriteDirect(db, []string{"ABC"}, 9)

	db.changeCache.waitForSequenceWithMissing(9)

	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, len(changes), 4)
	assert.True(t, verifyChangesSequences(changes, []string{"1", "2", "2::6", "3::9"}))

	close(options.Terminator)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user gets added to a new channel with existing entries (and existing backfill)
func TestLowSequenceHandlingWithAccessGrant(t *testing.T) {

	base.LogKeys["Sequence"] = true
	//base.LogKeys["Changes"] = true
	//base.LogKeys["Changes+"] = true
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
					log.Println("Closing Feed")
					feedClosed = true
				}
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	// Validate the initial sequences arrive as expected
	assert.Equals(t, len(changes), 3)
	assert.True(t, verifyChangesSequences(changes, []string{"1", "2", "2::6"}))

	db.Bucket.Incr("_sync:seq", 7, 0, 0)
	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")

	WriteDirect(db, []string{"PBS"}, 9)

	db.changeCache.waitForSequence(9)

	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, len(changes), 7)
	assert.True(t, verifyChangesSequences(changes, []string{"1", "2", "2::6", "2:8:5", "2:8:6", "2::8", "2::9"}))
	// Notes:
	// 1. 2::8 is the user sequence
	// 2. The duplicate send of sequence '6' is the standard behaviour when a channel is added - we don't know
	// whether the user has already seen the documents on the channel previously, so it gets resent

	close(options.Terminator)
}

func shortWaitCache() CacheOptions {

	return CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CachePendingSeqMaxNum:  50,
		CacheSkippedSeqMaxWait: 60 * time.Minute}
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

func verifyChangesSequences(changes []*ChangeEntry, sequences []string) bool {
	if len(changes) != len(sequences) {
		log.Printf("verifyChangesSequences: changes size (%v) not equals to sequences size (%v)",
			len(changes), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if changes[index].Seq.String() != seq {
			log.Printf("verifyChangesSequences: sequence mismatch at index %v, changes=%s, sequences=%s",
				index, changes[index].Seq.String(), seq)
			return false
		}
	}
	return true
}
