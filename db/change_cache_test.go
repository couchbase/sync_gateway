//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogEntry(seq uint64, docid string, revid string) *LogEntry {
	return &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
	}
}

// Creates a log entry with key "doc_[sequence]", rev="1-abc" with the specified channels
func testLogEntryForChannels(seq int, channelNames []string) *LogEntry {
	channelMap := make(channels.ChannelMap)
	for _, channelName := range channelNames {
		channelMap[channelName] = nil
	}

	return &LogEntry{
		Sequence:     uint64(seq),
		DocID:        fmt.Sprintf("doc_%d", seq),
		RevID:        "1-abc",
		TimeReceived: time.Now(),
		Channels:     channelMap,
	}
}

// Tombstoned entry
func et(seq uint64, docid string, revid string) *LogEntry {
	entry := testLogEntry(seq, docid, revid)
	entry.SetDeleted()
	return entry
}

func logEntry(seq uint64, docid string, revid string, channelNames []string, collectionID uint32) *LogEntry {
	entry := &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
		CollectionID: collectionID,
	}
	channelMap := make(channels.ChannelMap)
	for _, channelName := range channelNames {
		channelMap[channelName] = nil
	}
	entry.Channels = channelMap
	return entry
}

func testLogEntryWithCV(seq uint64, docid string, revid string, channelNames []string, collectionID uint32, sourceID string, version uint64) *LogEntry {
	entry := &LogEntry{
		Sequence:     seq,
		DocID:        docid,
		RevID:        revid,
		TimeReceived: time.Now(),
		CollectionID: collectionID,
		SourceID:     sourceID,
		Version:      version,
	}
	channelMap := make(channels.ChannelMap)
	for _, channelName := range channelNames {
		channelMap[channelName] = nil
	}
	entry.Channels = channelMap
	return entry
}

func TestLateSequenceHandling(t *testing.T) {

	context, ctx := setupTestDBWithCacheOptions(t, DefaultCacheOptions())
	defer context.Close(ctx)

	collection := GetSingleDatabaseCollection(t, context.DatabaseContext)
	collectionID := collection.GetCollectionID()

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)

	cache := newSingleChannelCache(collection, channels.NewID("Test1", collectionID), 0, dbstats.CacheStats)
	assert.True(t, cache != nil)

	// Empty late sequence cache should return empty set
	startSequence := cache.RegisterLateSequenceClient()
	entries, lastSeq, err := cache.GetLateSequencesSince(startSequence)
	assert.Len(t, entries, 0)
	assert.Equal(t, uint64(0), lastSeq)
	assert.True(t, err == nil)

	cache.AddLateSequence(testLogEntry(5, "foo", "1-a"))
	cache.AddLateSequence(testLogEntry(8, "foo2", "1-a"))

	// Retrieve since 0
	entries, lastSeq, err = cache.GetLateSequencesSince(0)
	log.Println("entries:", entries)
	assert.Len(t, entries, 2)
	assert.Equal(t, uint64(8), lastSeq)
	assert.Equal(t, uint64(1), cache.lateLogs[2].getListenerCount())
	assert.True(t, err == nil)

	// Add Sequences.  Will trigger purge on old sequences without listeners
	cache.AddLateSequence(testLogEntry(2, "foo3", "1-a"))
	cache.AddLateSequence(testLogEntry(7, "foo4", "1-a"))
	assert.Len(t, cache.lateLogs, 3)
	assert.Equal(t, uint64(8), cache.lateLogs[0].logEntry.Sequence)
	assert.Equal(t, uint64(2), cache.lateLogs[1].logEntry.Sequence)
	assert.Equal(t, uint64(7), cache.lateLogs[2].logEntry.Sequence)
	assert.Equal(t, uint64(1), cache.lateLogs[0].getListenerCount())

	// Retrieve since previous
	entries, lastSeq, err = cache.GetLateSequencesSince(lastSeq)
	log.Println("entries:", entries)
	assert.Len(t, entries, 2)
	assert.Equal(t, uint64(7), lastSeq)
	assert.Equal(t, uint64(0), cache.lateLogs[0].getListenerCount())
	assert.Equal(t, uint64(1), cache.lateLogs[2].getListenerCount())
	log.Println("cache.lateLogs:", cache.lateLogs)
	assert.True(t, err == nil)

	// Purge.  We have a listener sitting at seq=7, so purge should only clear previous
	cache.AddLateSequence(testLogEntry(15, "foo5", "1-a"))
	cache.AddLateSequence(testLogEntry(11, "foo6", "1-a"))
	log.Println("cache.lateLogs:", cache.lateLogs)
	cache.purgeLateLogEntries()
	assert.Len(t, cache.lateLogs, 3)
	assert.Equal(t, uint64(7), cache.lateLogs[0].logEntry.Sequence)
	assert.Equal(t, uint64(15), cache.lateLogs[1].logEntry.Sequence)
	assert.Equal(t, uint64(11), cache.lateLogs[2].logEntry.Sequence)
	log.Println("cache.lateLogs:", cache.lateLogs)
	assert.True(t, err == nil)

	// Release the listener, and purge again
	cache.ReleaseLateSequenceClient(uint64(7))
	cache.purgeLateLogEntries()
	assert.Len(t, cache.lateLogs, 1)
	assert.True(t, err == nil)

}

func TestLateSequenceHandlingWithMultipleListeners(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	collectionID := collection.GetCollectionID()

	stats, err := base.NewSyncGatewayStats()
	require.NoError(t, err)
	dbstats, err := stats.NewDBStats("", false, false, false, nil, nil)
	require.NoError(t, err)

	cache := newSingleChannelCache(collection, channels.NewID("Test1", collectionID), 0, dbstats.CacheStats)
	assert.True(t, cache != nil)

	// Add Listener before late entries arrive
	startSequence := cache.RegisterLateSequenceClient()
	entries, lastSeq1, err := cache.GetLateSequencesSince(startSequence)
	assert.Len(t, entries, 0)
	assert.Equal(t, uint64(0), lastSeq1)
	assert.True(t, err == nil)

	// Add two entries
	cache.AddLateSequence(testLogEntry(5, "foo", "1-a"))
	cache.AddLateSequence(testLogEntry(8, "foo2", "1-a"))

	// Add a second client.  Expect the first listener at [0], and the new one at [2]
	startSequence = cache.RegisterLateSequenceClient()
	_, lastSeq2, err := cache.GetLateSequencesSince(startSequence)
	require.NoError(t, err)
	assert.Equal(t, uint64(8), startSequence)
	assert.Equal(t, uint64(8), lastSeq2)

	assert.Equal(t, uint64(1), cache.lateLogs[0].getListenerCount())
	assert.Equal(t, uint64(1), cache.lateLogs[2].getListenerCount())

	cache.AddLateSequence(testLogEntry(3, "foo3", "1-a"))
	// First client requests again.  Expect first client at latest (3), second still at (8).
	_, lastSeq1, err = cache.GetLateSequencesSince(lastSeq1)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), lastSeq1)
	assert.Equal(t, uint64(1), cache.lateLogs[2].getListenerCount())
	assert.Equal(t, uint64(1), cache.lateLogs[3].getListenerCount())

	// Add another sequence, which triggers a purge.  Ensure we don't lose our listeners
	cache.AddLateSequence(testLogEntry(12, "foo4", "1-a"))
	assert.Equal(t, uint64(1), cache.lateLogs[0].getListenerCount())
	assert.Equal(t, uint64(1), cache.lateLogs[1].getListenerCount())

	// Release the first listener - ensure we maintain the second
	cache.ReleaseLateSequenceClient(lastSeq1)
	assert.Equal(t, uint64(1), cache.lateLogs[0].getListenerCount())
	assert.Equal(t, uint64(0), cache.lateLogs[1].getListenerCount())

	// Release the second listener
	cache.ReleaseLateSequenceClient(lastSeq2)
	assert.Equal(t, uint64(0), cache.lateLogs[0].getListenerCount())
	assert.Equal(t, uint64(0), cache.lateLogs[1].getListenerCount())

}

// Verify that a continuous changes feed hitting an error when building its late sequence feed will roll back to
// its low sequence value, then recover and successfully send subsequent late sequences.
func TestLateSequenceErrorRecovery(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	require.NotNil(t, authenticator, "db.Authenticator(db.Ctx) returned nil")
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new user")
	require.NoError(t, authenticator.Save(user))

	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Start continuous changes feed
	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	ctx, changesCtxCancel := context.WithCancel(ctx)
	options.ChangesCtx = ctx
	defer changesCtxCancel()
	options.Continuous = true
	options.Wait = true
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), options)
	require.NoError(t, err, "Feed initialization error")

	// Reads events until it gets a nil event, which indicates the changes loop has entered wait mode.
	// Returns slice of non-nil events received.
	nextFeedIteration := func() []*ChangeEntry {
		events := make([]*ChangeEntry, 0)
		for {
			select {
			case event := <-feed:
				if event == nil {
					return events
				}
				events = append(events, event)
			case <-time.After(10 * time.Second):
				assert.Fail(t, "Expected sequence didn't arrive over feed")
				return nil
			}
		}
	}

	nextEvents := nextFeedIteration()
	assert.Equal(t, len(nextEvents), 0) // Empty feed indicates changes is in wait mode

	collection := dbCollection.DatabaseCollection
	// Write sequence 1, wait for it on feed
	WriteDirect(t, collection, []string{"ABC"}, 1)

	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "1")

	// Write sequence 6, wait for it on feed
	WriteDirect(t, collection, []string{"ABC"}, 6)

	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "1::6")

	collectionID := collection.GetCollectionID()

	// Modify the cache's late logs to remove the changes feed's lateFeedHandler sequence from the
	// cache's lateLogs.  This will trigger an error on the next feed iteration, which should trigger
	// rollback to resend all changes since low sequence (1)
	c, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("ABC", collectionID))
	require.NoError(t, err)
	abcCache := c.(*singleChannelCacheImpl)
	abcCache.lateLogs[0].logEntry.Sequence = 1

	// Write sequence 3.  Error should trigger rollback that resends everything since low sequence (1)
	WriteDirect(t, collection, []string{"ABC"}, 4)

	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 2)
	assert.Equal(t, nextEvents[0].Seq.String(), "1::4")
	assert.Equal(t, nextEvents[1].Seq.String(), "1::6")

	// Write non-late sequence 7, should arrive normally
	WriteDirect(t, collection, []string{"ABC"}, 7)
	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "1::7")

	// Write late sequence 3, validates late handling recovery
	WriteDirect(t, collection, []string{"ABC"}, 3)
	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "1::3")

	// Write sequence 2.
	WriteDirect(t, collection, []string{"ABC"}, 2)
	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "2")

	// Write sequence 8, 5 should still be pending
	WriteDirect(t, collection, []string{"ABC"}, 8)
	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "4::8")

	// Write sequence 5 (all skipped sequences have arrived)
	WriteDirect(t, collection, []string{"ABC"}, 5)
	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "5")

	// Write sequence 9, validate non-compound sequences
	WriteDirect(t, collection, []string{"ABC"}, 9)
	nextEvents = nextFeedIteration()
	require.Equal(t, len(nextEvents), 1)
	assert.Equal(t, nextEvents[0].Seq.String(), "9")

}

// Verify that a continuous changes that has an active late feed serves the expected results if the
// channel cache associated with the late feed is compacted out of the cache
func TestLateSequenceHandlingDuringCompact(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	cacheOptions := shortWaitCache()
	cacheOptions.ChannelCacheOptions.MaxNumChannels = 100
	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)

	caughtUpStart := db.DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()

	changesCtx, changesCtxCancel := context.WithCancel(ctx)
	var changesFeedsWg sync.WaitGroup
	var seq1Wg, seq2Wg, seq3Wg sync.WaitGroup
	// Start 100 continuous changes feeds
	for i := 0; i < 100; i++ {
		changesFeedsWg.Add(1)
		seq1Wg.Add(1)
		seq2Wg.Add(1)
		seq3Wg.Add(1)
		go func(i int) {
			defer changesFeedsWg.Done()
			var options ChangesOptions
			options.Since = SequenceID{Seq: 0}
			options.ChangesCtx = changesCtx
			options.Continuous = true
			options.Wait = true
			channelName := fmt.Sprintf("chan_%d", i)
			perRequestDb, err := CreateDatabase(db.DatabaseContext)
			dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, perRequestDb)
			assert.NoError(t, err)
			ctx = base.CorrelationIDLogCtx(ctx, fmt.Sprintf("context_%s", channelName))
			feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf(channelName), options)
			require.NoError(t, err, "Feed initialization error")

			// Process feed until closed by terminator in main goroutine
			feedCount := 0
			seqArrived := make([]bool, 4)
			for event := range feed {

				if event == nil {
					continue
				}
				if event.Seq.Seq == 1 {
					seq1Wg.Done()
				} else if event.Seq.Seq == 2 {
					seq2Wg.Done()
				} else if event.Seq.Seq == 3 && seqArrived[3] == false {
					// seq 3 may arrive twice for feeds that roll back their low sequence after eviction.  Check flag to
					// only notify arrival the first time
					seq3Wg.Done()
				}
				seqArrived[event.Seq.Seq] = true
				log.Printf("Got feed event for %v: %v", channelName, event)
				feedCount++
			}
			log.Printf("Feed closed for %s", channelName)
			// Feeds that stay resident in the cache will get seqs 1, 3, 2
			// Feeds that do not stay resident will get seq 3 twice (i.e. 1, 3, 2, 3) due to late sequence feed reset
			assert.True(t, feedCount >= 3, "Expected at least three feed events")
			assert.True(t, seqArrived[1])
			assert.True(t, seqArrived[2])
			assert.True(t, seqArrived[3])
		}(i)
	}

	// Wait for everyone to be caught up
	require.NoError(t, db.WaitForCaughtUp(caughtUpStart+int64(100)))
	log.Printf("Everyone is caught up")

	// Write sequence 1 to all channels, wait for it on feed
	channelSet := make([]string, 100)
	for i := 0; i < 100; i++ {
		channelSet[i] = fmt.Sprintf("chan_%d", i)
	}
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	WriteDirect(t, collection, channelSet, 1)
	seq1Wg.Wait()
	log.Printf("Everyone's seq 1 arrived")

	// Wait for everyone to be caught up again
	require.NoError(t, db.WaitForCaughtUp(caughtUpStart+int64(100)))

	// Write sequence 3 to all channels, wait for it on feed
	WriteDirect(t, collection, channelSet, 3)
	seq3Wg.Wait()
	log.Printf("Everyone's seq 3 arrived")

	// Write the late (previously skipped) sequence 2
	WriteDirect(t, collection, channelSet, 2)
	seq2Wg.Wait()
	log.Printf("Everyone's seq 2 arrived")

	// Wait for everyone to be caught up again
	require.NoError(t, db.WaitForCaughtUp(caughtUpStart+int64(100)))

	// Cancel the changes context
	changesCtxCancel()

	log.Printf("terminator is closed")

	// Wake everyone up to detect termination
	// TODO: why is this not automatic
	WriteDirect(t, collection, channelSet, 4)
	changesFeedsWg.Wait()

}

func writeUserDirect(t *testing.T, db *Database, username string, sequence uint64) {
	docId := db.MetadataKeys.UserKey(username)
	_, err := db.MetadataStore.Add(docId, 0, Body{"sequence": sequence, "name": username})
	require.NoError(t, err)
}

// Test notification when buffered entries are processed after a user doc arrives.
func TestChannelCacheBufferingWithUserDoc(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyDCP)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	collectionID := collection.GetCollectionID()

	// Simulate seq 1 (user doc) being delayed - write 2 first
	WriteDirect(t, collection, []string{"ABC"}, 2)

	// Start wait for doc in ABC
	chans := channels.SetOfNoValidate(
		channels.NewID("ABC", collectionID))
	waiter := db.mutationListener.NewWaiterWithChannels(chans, nil, false)

	successChan := make(chan bool)
	go func() {
		waiter.Wait(ctx)
		close(successChan)
	}()

	// Simulate a user doc update
	writeUserDirect(t, db, "bernard", 1)

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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC", "PBS", "NBC", "TBS"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	// Simulate seq 3 being delayed - write 1,2,4,5
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 5)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 6)

	// Test that retrieval isn't blocked by skipped sequences
	require.NoError(t, db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence))
	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)
	changes, err := collectionWithUser.GetChanges(ctx, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))
	assert.NoError(t, err, "Couldn't GetChanges")
	assert.Len(t, changes, 4)

	collectionID := collection.GetCollectionID()

	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:           "doc-1",
		Changes:      []ChangeRev{{"rev": "1-a"}},
		collectionID: collectionID}, changes[0])

	lastSeq := changes[len(changes)-1].Seq

	// Validate insert to various cache states
	WriteDirect(t, collection, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(t, collection, []string{"CBS"}, 7)
	require.NoError(t, db.changeCache.waitForSequence(ctx, 7, base.DefaultWaitForSequence))

	// verify insert at start (PBS)
	pbsCache, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("PBS", collectionID))
	require.NoError(t, err)
	assert.True(t, verifyCacheSequences(pbsCache, []uint64{3, 5, 6}))
	// verify insert at middle (ABC)
	abcCache, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("ABC", collectionID))
	require.NoError(t, err)
	assert.True(t, verifyCacheSequences(abcCache, []uint64{1, 2, 3, 5, 6}))
	// verify insert at end (NBC)
	nbcCache, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("NBC", collectionID))
	require.NoError(t, err)
	assert.True(t, verifyCacheSequences(nbcCache, []uint64{1, 3}))
	// verify insert to empty cache (TBS)
	tbsCache, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("TBS", collectionID))
	require.NoError(t, err)
	assert.True(t, verifyCacheSequences(tbsCache, []uint64{3}))

	// verify changes has three entries (needs to resend all since previous LowSeq, which
	// will be the late arriver (3) along with 5, 6)
	changes, err = collectionWithUser.GetChanges(ctx, base.SetOf("*"), getChangesOptionsWithSeq(t, lastSeq))
	require.NoError(t, err)
	assert.Len(t, changes, 3)
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 3, LowSeq: 3},
		ID:           "doc-3",
		Changes:      []ChangeRev{{"rev": "1-a"}},
		collectionID: collectionID,
	}, changes[0])

}

// Test backfill of late arriving sequences to a continuous changes feed
func TestContinuousChangesBackfill(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyChanges, base.KeyDCP)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC", "PBS", "NBC", "CBS"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"PBS"}, 5)
	WriteDirect(t, collection, []string{"CBS"}, 6)

	db.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)

	// Start changes feed
	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	ctx, changesCtxCancel := context.WithCancel(ctx)
	options.ChangesCtx = ctx
	options.Continuous = true
	options.Wait = true
	defer changesCtxCancel()

	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("*"), options)
	assert.True(t, err == nil)

	time.Sleep(50 * time.Millisecond)

	collection = dbCollection.DatabaseCollection
	// Write some more docs
	WriteDirect(t, collection, []string{"CBS"}, 3)
	WriteDirect(t, collection, []string{"PBS"}, 12)
	require.NoError(t, dbCollection.changeCache().waitForSequence(ctx, 12, base.DefaultWaitForSequence))

	// Test multiple backfill in single changes loop iteration
	WriteDirect(t, collection, []string{"ABC", "NBC", "PBS", "CBS"}, 4)
	WriteDirect(t, collection, []string{"ABC", "NBC", "PBS", "CBS"}, 7)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 8)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 13)
	require.NoError(t, dbCollection.changeCache().waitForSequence(ctx, 13, base.DefaultWaitForSequence))
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

	assert.Len(t, expectedDocs, 0)
}

// Test low sequence handling of late arriving sequences to a continuous changes feed
func TestLowSequenceHandling(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyQuery)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	assert.True(t, authenticator != nil, "db.Authenticator(ctx) returned nil")
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC", "PBS", "NBC", "TBS"))
	assert.NoError(t, err, fmt.Sprintf("Error creating new user: %v", err))
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 5)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 6)

	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	require.NoError(t, dbCollection.changeCache().waitForSequence(ctx, 6, base.DefaultWaitForSequence))
	dbCollection.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	changesCtx, changesCtxCancel := context.WithCancel(base.TestCtx(t))
	options.ChangesCtx = changesCtx
	defer changesCtxCancel()
	options.Continuous = true
	options.Wait = true
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("*"), options)
	assert.True(t, err == nil)

	changes, err := verifySequencesInFeed(feed, []uint64{1, 2, 5, 6})
	assert.True(t, err == nil)
	require.Len(t, changes, 4)

	collectionID := collection.GetCollectionID()

	require.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:           "doc-1",
		Changes:      []ChangeRev{{"rev": "1-a"}},
		collectionID: collectionID}, changes[0])

	// Test backfill clear - sequence numbers go back to standard handling
	WriteDirect(t, collection, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 4)

	_, err = verifySequencesInFeed(feed, []uint64{3, 4})
	assert.True(t, err == nil)

	WriteDirect(t, collection, []string{"ABC"}, 7)
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 8)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 9)
	_, err = verifySequencesInFeed(feed, []uint64{7, 8, 9})
	assert.True(t, err == nil)

}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user doesn't have visibility to some of the late arriving sequences
func TestLowSequenceHandlingAcrossChannels(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyQuery)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, err, fmt.Sprintf("db.Authenticator(db.Ctx) returned err: %v", err))
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(t, collection, []string{"ABC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"PBS"}, 5)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 6)

	require.NoError(t, db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence))
	db.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)

	// Start changes feed

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	ctx, changesCtxCancel := context.WithCancel(ctx)
	options.ChangesCtx = ctx
	options.Continuous = true
	options.Wait = true
	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("*"), options)
	assert.True(t, err == nil)

	_, err = verifySequencesInFeed(feed, []uint64{1, 2, 6})
	assert.True(t, err == nil)

	// Test backfill of sequence the user doesn't have visibility to
	WriteDirect(t, collection, []string{"PBS"}, 3)
	WriteDirect(t, collection, []string{"ABC"}, 9)

	_, err = verifySequencesInFeed(feed, []uint64{9})
	assert.True(t, err == nil)

	changesCtxCancel()
}

// Test low sequence handling of late arriving sequences to a continuous changes feed, when the
// user gets added to a new channel with existing entries (and existing backfill)
func TestLowSequenceHandlingWithAccessGrant(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyQuery)

	cacheOptions := shortWaitCache()
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &cacheOptions, // use default collection based on use of GetPrincipalForTest
		Scopes:       GetScopesOptionsDefaultCollectionOnly(t),
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(t, collection, []string{"ABC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"PBS"}, 5)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 6)

	require.NoError(t, db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence))
	db.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)

	// Start changes feed

	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	ctx, changesCtxCancel := context.WithCancel(ctx)
	options.ChangesCtx = ctx
	options.Continuous = true
	options.Wait = true
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("*"), options)
	require.NoError(t, err)

	var changes = make([]*ChangeEntry, 0, 50)

	// Validate the initial sequences arrive as expected
	err = appendFromFeed(&changes, feed, 3, base.DefaultWaitForSequence)
	require.NoError(t, err)
	assert.Len(t, changes, 3)
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6"}))

	_, incrErr := dbCollection.dataStore.Incr(db.MetadataKeys.SyncSeqKey(), 7, 7, 0)
	require.NoError(t, incrErr)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipalForTest(t, "naomi", true)
	require.NoError(t, err)
	require.NotNil(t, userInfo)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, _, err = db.UpdatePrincipal(ctx, userInfo, true, true)
	require.NoError(t, err, "UpdatePrincipal failed")

	WriteDirect(t, collection, []string{"PBS"}, 9)
	require.NoError(t, db.changeCache.waitForSequence(ctx, 9, base.DefaultWaitForSequence))

	// FIXME CBG-2554 expected 4 entries only received 3
	err = appendFromFeed(&changes, feed, 4, base.DefaultWaitForSequence)
	require.NoError(t, err, "Expected more changes to be sent on feed, but never received")
	assert.Len(t, changes, 7)
	// FIXME CBG-2554 changes don't match expected (missing seq 8 (the user sequence))
	// [
	//   {Seq:1, ID:doc-1, Changes:[map[rev:1-a]]}
	//   {Seq:2, ID:doc-2, Changes:[map[rev:1-a]]}
	//   {Seq:2::6, ID:doc-6, Changes:[map[rev:1-a]]}
	//   {Seq:2::5, ID:doc-5, Changes:[map[rev:1-a]]} // should be 2:8:5
	//   {Seq:2::6, ID:doc-6, Changes:[map[rev:1-a]]} // should be 2:8:6
	//                                                // MISSING 2::8
	//   {Seq:2::9, ID:doc-9, Changes:[map[rev:1-a]]}
	// ]
	assert.True(t, verifyChangesFullSequences(changes, []string{"1", "2", "2::6", "2:8:5", "2:8:6", "2::8", "2::9"}))
	// Notes:
	// 1. 2::8 is the user sequence
	// 2. The duplicate send of sequence '6' is the standard behaviour when a channel is added - we don't know
	// whether the user has already seen the documents on the channel previously, so it gets resent

	changesCtxCancel()
}

// Tests channel cache backfill with slow query, validates that a request that is terminated while
// waiting for the view lock doesn't trigger a view query.  Runs multiple goroutines, using a channel
// and two waitgroups to ensure expected ordering of events, as follows
//  1. Define a PostQueryCallback (via leaky bucket) that does two things:
//     - closes a notification channel (queryBlocked) to indicate that the initial query is blocking
//     - blocks via a WaitGroup (queryWg)
//  2. Start a goroutine to issue a changes request
//     - when view executes, will trigger callback handling above
//  3. Start a second goroutine to issue another changes request.
//     - will block waiting for queryLock, which increments StatKeyChannelCachePendingQueries stat
//  4. Wait until StatKeyChannelCachePendingQueries is incremented
//  5. Terminate second changes request
//  6. Unblock the initial view query
//     - releases the view lock, second changes request is unblocked
//     - since it's been terminated, should return error before executing a second view query
func TestChannelQueryCancellation(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency test when running in integration")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

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

	db, ctx := setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	// Write a handful of docs/sequences to the bucket
	_, _, err := collection.Put(ctx, "key1", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	_, _, err = collection.Put(ctx, "key2", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	_, _, err = collection.Put(ctx, "key3", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	_, _, err = collection.Put(ctx, "key4", Body{"channels": "ABC"})
	assert.NoError(t, err, "Put failed with error: %v", err)
	require.NoError(t, db.changeCache.waitForSequence(ctx, 4, base.DefaultWaitForSequence))

	// Flush the cache, to ensure view query on subsequent changes requests
	// require.NoError(t, db.FlushChannelCache(ctx))

	// Issue two one-shot since=0 changes request.  Both will attempt a view query.  The first will block based on queryWg,
	// the second will block waiting for the view lock
	initialQueryCount := db.DbStats.Cache().ViewQueries.Value()
	changesWg.Add(1)
	go func() {
		defer changesWg.Done()
		var options ChangesOptions
		options.Since = SequenceID{Seq: 0}
		options.ChangesCtx = base.TestCtx(t)
		options.Continuous = false
		options.Wait = false
		options.Limit = 2 // Avoid prepending results in cache, as we don't want second changes to serve results from cache
		_, err := collection.GetChanges(ctx, base.SetOf("ABC"), options)
		assert.NoError(t, err, "Expect no error for first changes request")
	}()

	// Wait for queryBlocked=true - ensures the first goroutine has acquired view lock
	select {
	case <-queryBlocked:
		// continue
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Changes goroutine failed to initiate view query in 10 seconds.")
	}

	initialPendingQueries := db.DbStats.Cache().ChannelCachePendingQueries.Value()

	// Start a second goroutine that should block waiting for the view lock
	changesCtx, changesCtxCancel := context.WithCancel(base.TestCtx(t))
	changesWg.Add(1)
	go func() {
		defer changesWg.Done()
		var options ChangesOptions
		options.Since = SequenceID{Seq: 0}
		options.ChangesCtx = changesCtx
		options.Continuous = false
		options.Limit = 2
		options.Wait = false
		_, err := collection.GetChanges(ctx, base.SetOf("ABC"), options)
		assert.Error(t, err, "Expected error for second changes")
	}()

	// wait for second goroutine to be queued for the view lock (based on expvar)
	var pendingQueries int64
	for i := 0; i < 1000; i++ {
		pendingQueries = db.DbStats.Cache().ChannelCachePendingQueries.Value()
		if pendingQueries > initialPendingQueries {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, pendingQueries > initialPendingQueries, "Pending queries (%d) didn't exceed initialPendingQueries (%d) after 10s", pendingQueries, initialPendingQueries)

	// Terminate the second changes request
	changesCtxCancel()

	// Unblock the first goroutine
	queryWg.Done()

	// Wait for both goroutines to complete and evaluate their asserts
	changesWg.Wait()

	// Validate only a single query was executed
	finalQueryCount := db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initialQueryCount+1, finalQueryCount)
}

func TestLowSequenceHandlingNoDuplicates(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	assert.True(t, authenticator != nil, "db.Authenticator(db.Ctx) returned nil")
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC", "PBS", "NBC", "TBS"))
	assert.NoError(t, err, fmt.Sprintf("Error creating new user: %v", err))
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 5)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 6)

	require.NoError(t, db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence))
	db.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)

	// Start changes feed

	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	ctx, changesCtxCancel := context.WithCancel(ctx)
	options.ChangesCtx = ctx
	defer changesCtxCancel()
	options.Continuous = true
	options.Wait = true
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("*"), options)
	assert.True(t, err == nil)

	// Array to read changes from feed to support assertions
	var changes = make([]*ChangeEntry, 0, 50)

	err = appendFromFeed(&changes, feed, 4, base.DefaultWaitForSequence)

	// Validate the initial sequences arrive as expected
	assert.True(t, err == nil)
	assert.Len(t, changes, 4)
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 1, TriggeredBy: 0, LowSeq: 2},
		ID:           "doc-1",
		collectionID: dbCollection.GetCollectionID(),
		Changes:      []ChangeRev{{"rev": "1-a"}}}, changes[0])

	// Test backfill clear - sequence numbers go back to standard handling
	WriteDirect(t, collection, []string{"ABC", "NBC", "PBS", "TBS"}, 3)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 4)

	require.NoError(t, db.changeCache.waitForSequenceNotSkipped(ctx, 4, base.DefaultWaitForSequence))

	err = appendFromFeed(&changes, feed, 2, base.DefaultWaitForSequence)
	assert.True(t, err == nil)
	assert.Len(t, changes, 6)
	assert.True(t, verifyChangesSequencesIgnoreOrder(changes, []uint64{1, 2, 5, 6, 3, 4}))

	WriteDirect(t, collection, []string{"ABC"}, 7)
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 8)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 9)
	require.NoError(t, db.changeCache.waitForSequence(ctx, 9, base.DefaultWaitForSequence))
	newChanges, err := verifySequencesInFeed(feed, []uint64{7, 8, 9})
	require.NoError(t, err)

	assert.True(t, verifyChangesSequencesIgnoreOrder(append(changes, newChanges...), []uint64{1, 2, 5, 6, 3, 4, 7, 8, 9}))
}

// Test race condition causing skipped sequences in changes feed.  Channel feeds are processed sequentially
// in the main changes.go iteration loop, without a lock on the underlying channel caches.  The following
// sequence is possible while running a changes feed for channels "A", "B":
//  1. Sequence 100, Channel A arrives, and triggers changes loop iteration
//  2. Changes loop calls changes.changesFeed(A) - gets 100
//  3. Sequence 101, Channel A arrives
//  4. Sequence 102, Channel B arrives
//  5. Changes loop calls changes.changesFeed(B) - gets 102
//  6. Changes sends 100, 102, and sets since=102
//
// Here 101 is skipped, and never gets sent.  There are a number of ways a sufficient delay between #2 and #5
// could be introduced in a real-world scenario
//   - there are channels C,D,E,F,G with large caches that get processed between A and B
//
// To test, uncomment the following
// lines at the start of changesFeed() in changes.go to simulate slow processing:
//
//	base.Infof(base.KeyChanges, "Simulate slow processing time for channel %s - sleeping for 100 ms", channel)
//	time.Sleep(100 * time.Millisecond)
func TestChannelRace(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges)

	db, ctx := setupTestDBWithCacheOptions(t, shortWaitCache())
	defer db.Close(ctx)

	// Create a user with access to channels "Odd", "Even"
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "Even", "Odd"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	// Write initial sequences
	WriteDirect(t, collection, []string{"Odd"}, 1)
	WriteDirect(t, collection, []string{"Even"}, 2)
	WriteDirect(t, collection, []string{"Odd"}, 3)

	require.NoError(t, db.changeCache.waitForSequence(ctx, 3, base.DefaultWaitForSequence))
	db.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)

	// Start changes feed
	dbCollection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	var options ChangesOptions
	options.Since = SequenceID{Seq: 0}
	ctx, changesCtxCancel := context.WithCancel(ctx)
	options.ChangesCtx = ctx
	options.Continuous = true
	options.Wait = true
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("Even", "Odd"), options)
	assert.True(t, err == nil)
	feedClosed := false

	// Go-routine to work the feed channel and write to an array for use by assertions
	var changes struct {
		lock    sync.RWMutex
		entries []*ChangeEntry
	}
	changes.entries = make([]*ChangeEntry, 0, 50)
	go func() {
		for feedClosed == false {
			select {
			case entry, ok := <-feed:
				if ok {
					// feed sends nil after each continuous iteration
					if entry != nil {
						log.Println("Changes entry:", entry.Seq)
						changes.lock.Lock()
						changes.entries = append(changes.entries, entry)
						changes.lock.Unlock()
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
	changes.lock.RLock()
	assert.Len(t, changes.entries, 3)
	changes.lock.RUnlock()

	// Send update to trigger the start of the next changes iteration
	WriteDirect(t, collection, []string{"Even"}, 4)
	time.Sleep(150 * time.Millisecond)
	// After read of "Even" channel, but before read of "Odd" channel, send three new entries
	WriteDirect(t, collection, []string{"Odd"}, 5)
	WriteDirect(t, collection, []string{"Even"}, 6)
	WriteDirect(t, collection, []string{"Odd"}, 7)

	time.Sleep(100 * time.Millisecond)

	// At this point we've haven't sent sequence 6, but the continuous changes feed has since=7

	// Write a few more to validate that we're not catching up on the missing '6' later
	WriteDirect(t, collection, []string{"Even"}, 8)
	WriteDirect(t, collection, []string{"Odd"}, 9)
	time.Sleep(750 * time.Millisecond)
	changes.lock.RLock()
	assert.Len(t, changes.entries, 9)
	assert.True(t, verifyChangesFullSequences(changes.entries, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}))
	changesString := ""
	for _, change := range changes.entries {
		changesString = fmt.Sprintf("%s%d, ", changesString, change.Seq.Seq)
	}
	changes.lock.RUnlock()
	fmt.Println("changes: ", changesString)

	changesCtxCancel()
}

// Test that housekeeping goroutines get terminated when change cache is stopped
func TestStopChangeCache(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyDCP)

	// Setup short-wait cache to ensure cleanup goroutines fire often
	cacheOptions := DefaultCacheOptions()
	cacheOptions.CachePendingSeqMaxWait = 10 * time.Millisecond
	cacheOptions.CachePendingSeqMaxNum = 50
	cacheOptions.CacheSkippedSeqMaxWait = 1 * time.Second

	// Use leaky bucket to have the tap feed 'lose' document 3
	leakyConfig := base.LeakyBucketConfig{
		DCPFeedMissingDocs: []string{"doc-3"},
	}
	db, ctx := setupTestLeakyDBWithCacheOptions(t, cacheOptions, leakyConfig)

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Write sequences direct
	WriteDirect(t, collection, []string{"ABC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"ABC"}, 3)

	// Artificially add 3 skipped, and back date skipped entry by 2 hours to trigger attempted view retrieval during Clean call
	db.changeCache.skippedSeqs.PushSkippedSequenceEntry(NewSingleSkippedSequenceEntryAt(3, time.Now().Unix()-7200))

	// tear down the DB.  Should stop the cache before view retrieval of the skipped sequence is attempted.
	db.Close(ctx)

	// Hang around a while to see if the housekeeping tasks fire and panic
	time.Sleep(1 * time.Second)

}

// Test size config
func TestChannelCacheSize(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	options := DefaultCacheOptions()
	options.ChannelCacheOptions.ChannelCacheMinLength = 600
	options.ChannelCacheOptions.ChannelCacheMaxLength = 600

	log.Printf("Options in test:%+v", options)
	db, ctx := setupTestDBWithCacheOptions(t, options)
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Write 750 docs to channel ABC
	for i := 1; i <= 750; i++ {
		WriteDirect(t, collection, []string{"ABC"}, uint64(i))
	}

	// Validate that retrieval returns expected sequences
	require.NoError(t, db.changeCache.waitForSequence(ctx, 750, base.DefaultWaitForSequence))
	collectionWithUser, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionWithUser.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)
	changes, err := collectionWithUser.GetChanges(ctx, base.SetOf("ABC"), getChangesOptionsWithZeroSeq(t))
	assert.NoError(t, err, "Couldn't GetChanges")
	assert.Len(t, changes, 750)

	// Validate that cache stores the expected number of values
	collectionID := collection.GetCollectionID()

	abcCache, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("ABC", collectionID))
	require.NoError(t, err)

	assert.Len(t, abcCache.(*singleChannelCacheImpl).logs, 600)
}

func shortWaitCache() CacheOptions {

	// cacheOptions := DefaultCacheOptions()
	cacheOptions := DefaultCacheOptions()
	cacheOptions.CachePendingSeqMaxWait = 5 * time.Millisecond
	cacheOptions.CachePendingSeqMaxNum = 50
	cacheOptions.CacheSkippedSeqMaxWait = 2 * time.Minute
	return cacheOptions
}

func verifyCacheSequences(singleCache SingleChannelCache, sequences []uint64) bool {

	cache, ok := singleCache.(*singleChannelCacheImpl)
	if !ok {
		return false
	}
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
			log.Printf("verifyChangesSequencesIgnorerder: sequenceID %d missing in changes: %+v", seq, changes)
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
		err := appendFromFeed(&changes, feed, 1, base.DefaultWaitForSequence)
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

	// Enable relevant logging
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyChanges)

	// Create a test db that uses channel cache
	options := DefaultCacheOptions()
	options.ChannelCacheOptions.ChannelCacheMinLength = 600
	options.ChannelCacheOptions.ChannelCacheMaxLength = 600

	db, ctx := setupTestDBWithCacheOptions(t, options)
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	collectionID := collection.GetCollectionID()

	// -------- Setup notifyChange callback ----------------

	//  Detect whether the 2nd was ignored using an notifyChange listener callback and make sure it was not added to the ABC channel
	waitForOnChangeCallback := sync.WaitGroup{}
	waitForOnChangeCallback.Add(1)
	db.changeCache.notifyChange = func(_ context.Context, chans channels.Set) {
		expectedChan := channels.NewID("ABC", collectionID)
		for ch := range chans {
			if ch == expectedChan {
				waitForOnChangeCallback.Done()
			}
		}

	}

	// -------- Perform actions ----------------

	// Create doc1 w/ unused sequences 1, actual sequence 3.
	doc1Id := "doc1Id"
	doc1 := Document{
		ID: doc1Id,
	}
	doc1.SyncData = SyncData{
		UnusedSequences: []uint64{
			1,
		},
		CurrentRev: "1-abc",
		Sequence:   3,
	}
	var doc1DCPBytes []byte
	if base.TestUseXattrs() {
		body, syncXattr, _, err := doc1.MarshalWithXattrs()
		require.NoError(t, err)
		doc1DCPBytes = sgbucket.EncodeValueWithXattrs(body, sgbucket.Xattr{Name: base.SyncXattrName, Value: syncXattr})
	} else {
		var err error
		doc1DCPBytes, err = doc1.MarshalJSON()
		require.NoError(t, err)
	}
	// Create doc2 w/ sequence 2, channel ABC
	doc2Id := "doc2Id"
	doc2 := Document{
		ID: doc2Id,
	}
	channelMap := channels.ChannelMap{
		"ABC": nil,
	}
	doc2.SyncData = SyncData{
		CurrentRev: "1-cde",
		Sequence:   2,
		Channels:   channelMap,
	}
	var doc2DCPBytes []byte
	var dataType sgbucket.FeedDataType = base.MemcachedDataTypeJSON
	if base.TestUseXattrs() {
		dataType |= base.MemcachedDataTypeXattr
		body, syncXattr, _, err := doc2.MarshalWithXattrs()
		require.NoError(t, err)
		doc2DCPBytes = sgbucket.EncodeValueWithXattrs(body, sgbucket.Xattr{Name: base.SyncXattrName, Value: syncXattr})
	} else {
		var err error
		doc2DCPBytes, err = doc2.MarshalJSON()
		require.NoError(t, err)
	}

	// Send feed event for doc2. This won't trigger notifyChange, as buffering is waiting for seq 1
	feedEventDoc2 := sgbucket.FeedEvent{
		Synchronous:  true,
		Key:          []byte(doc2Id),
		Value:        doc2DCPBytes,
		CollectionID: collectionID,
		DataType:     dataType,
	}
	db.changeCache.DocChanged(feedEventDoc2)

	// Send feed event for doc1. This should trigger caching for doc2, and trigger notifyChange for channel ABC.
	feedEventDoc1 := sgbucket.FeedEvent{
		Synchronous:  true,
		Key:          []byte(doc1Id),
		Value:        doc1DCPBytes,
		CollectionID: collectionID,
	}
	db.changeCache.DocChanged(feedEventDoc1)

	// -------- Wait for waitgroup ----------------

	// Block until the notifyChange callback was invoked with the expected channels.
	// If the callback is never called back with expected, will block forever.
	waitForOnChangeCallback.Wait()

}

// Trigger initialization of empty cache, then write and validate a subsequent changes request returns expected data.
func TestInitializeEmptyCache(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges)

	// Increase the cache max size
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = 50

	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	cacheWaiter := db.NewDCPCachingCountWaiter(t)
	docCount := 0
	// Write docs to non-queried channel first, to increase cache.nextSequence
	for i := 0; i < 10; i++ {
		channels := []string{"islands"}
		body := Body{"serialnumber": int64(i), "channels": channels}
		docID := fmt.Sprintf("loadCache-ch-%d", i)
		_, _, err := collection.Put(ctx, docID, body)
		assert.NoError(t, err, "Couldn't create document")
		docCount++
	}

	// Issue getChanges for empty channel
	changes, err := collection.GetChanges(ctx, channels.BaseSetOf(t, "zero"), getChangesOptionsWithCtxOnly(t))
	assert.NoError(t, err, "Couldn't GetChanges")
	changesCount := len(changes)
	assert.Equal(t, 0, changesCount)

	// Write some documents to channel zero
	for i := 0; i < 10; i++ {
		channels := []string{"zero"}
		body := Body{"serialnumber": int64(i), "channels": channels}
		docID := fmt.Sprintf("loadCache-z-%d", i)
		_, _, err := collection.Put(ctx, docID, body)
		assert.NoError(t, err, "Couldn't create document")
		docCount++
	}

	cacheWaiter.Add(docCount)
	cacheWaiter.Wait()

	changes, err = collection.GetChanges(ctx, channels.BaseSetOf(t, "zero"), getChangesOptionsWithCtxOnly(t))
	assert.NoError(t, err, "Couldn't GetChanges")
	changesCount = len(changes)
	assert.Equal(t, 10, changesCount)
}

// Trigger initialization of the channel cache under load via getChanges.  Ensures validFrom handling correctly
// sets query/cache boundaries
func TestInitializeCacheUnderLoad(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges)

	// Increase the cache max size
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = 50

	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	// Writes [docCount] documents.  Use wait group (writesDone)to identify when all docs have been written.
	// Use another waitGroup (writesInProgress) to trigger getChanges midway through writes
	docCount := 1000
	inProgressCount := 100

	cacheWaiter := db.NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(docCount)
	var writesInProgress sync.WaitGroup
	writesInProgress.Add(inProgressCount)

	// Start writing docs
	go func() {
		for i := 0; i < docCount; i++ {
			channels := []string{"zero"}
			body := Body{"serialnumber": int64(i), "channels": channels}
			docID := fmt.Sprintf("loadCache-%d", i)
			_, _, err := collection.Put(ctx, docID, body)
			require.NoError(t, err, "Couldn't create document")
			if i < inProgressCount {
				writesInProgress.Done()
			}
		}
	}()

	// Wait for writes to be in progress, then getChanges for channel zero
	writesInProgress.Wait()
	changes, err := collection.GetChanges(ctx, channels.BaseSetOf(t, "zero"), getChangesOptionsWithCtxOnly(t))
	require.NoError(t, err, "Couldn't GetChanges")
	firstChangesCount := len(changes)
	var lastSeq SequenceID
	if firstChangesCount > 0 {
		lastSeq = changes[len(changes)-1].Seq
	}

	// Wait for all writes to be cached, then getChanges again
	cacheWaiter.Wait()

	changes, err = collection.GetChanges(ctx, channels.BaseSetOf(t, "zero"), getChangesOptionsWithSeq(t, lastSeq))
	require.NoError(t, err, "Couldn't GetChanges")
	secondChangesCount := len(changes)
	assert.Equal(t, docCount, firstChangesCount+secondChangesCount)

}

// Verify that notifyChange for channel zero is sent even when the channel isn't active in the cache.
func TestNotifyForInactiveChannel(t *testing.T) {

	// Enable relevant logging
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyDCP)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)
	collectionID := collection.GetCollectionID()

	// -------- Setup notifyChange callback ----------------

	notifyChannel := make(chan struct{})
	db.changeCache.notifyChange = func(_ context.Context, chans channels.Set) {
		expectedChan := channels.NewID("zero", collectionID)
		if chans.Contains(expectedChan) {
			notifyChannel <- struct{}{}
		}
	}

	// Write a document to channel zero
	body := Body{"channels": []string{"zero"}}
	_, _, err := collection.Put(ctx, "inactiveCacheNotify", body)
	assert.NoError(t, err)

	// Wait for notify to arrive
	select {
	case <-notifyChannel:
		// success
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Timed out waiting for notify to fire")
	}

}

func TestMaxChannelCacheConfig(t *testing.T) {
	channelCacheMaxChannels := []int{10, 50000, 100000}

	for _, val := range channelCacheMaxChannels {
		t.Run(fmt.Sprintf("TestMaxChannelCacheConfig-%d", val), func(t *testing.T) {
			options := DefaultCacheOptions()
			options.ChannelCacheOptions.MaxNumChannels = val
			db, ctx := setupTestDBWithCacheOptions(t, options)
			assert.Equal(t, val, db.DatabaseContext.Options.CacheOptions.MaxNumChannels)
			defer db.Close(ctx)
		})
	}
}

// Validates InsertPendingEntries timing
func TestChangeCache_InsertPendingEntries(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	cacheOptions := DefaultCacheOptions()
	cacheOptions.CachePendingSeqMaxWait = 100 * time.Millisecond

	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)

	// Create a user with access to some channels
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC", "PBS", "NBC", "TBS"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	// Simulate seq 3 + 4 being delayed - write 1,2,5,6
	WriteDirect(t, collection, []string{"ABC", "NBC"}, 1)
	WriteDirect(t, collection, []string{"ABC"}, 2)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 5)
	WriteDirect(t, collection, []string{"ABC", "PBS"}, 6)

	// wait for InsertPendingEntries to fire, move 3 and 4 to skipped and get seqs 5 + 6
	require.NoError(t, db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence))

}

// Generator for processEntry
type testProcessEntryFeed struct {
	nextSeq     uint64
	channelMaps []channels.ChannelMap
	sources     []uint64 // Used for non-sequential sequence delivery when numSources > 1
	fixedTime   time.Time
}

func NewTestProcessEntryFeed(numChannels int, numSources int) *testProcessEntryFeed {

	feed := &testProcessEntryFeed{
		fixedTime: time.Now(),
		sources:   make([]uint64, numSources),
	}

	feed.channelMaps = make([]channels.ChannelMap, numChannels)
	for i := 0; i < numChannels; i++ {
		channelName := fmt.Sprintf("channel_%d", i)
		feed.channelMaps[i] = channels.ChannelMap{
			channelName: nil,
		}
	}

	feed.reset()
	return feed
}

// Reset sets nextSeq to 1, and reseeds sources
func (f *testProcessEntryFeed) reset() {
	f.nextSeq = 1
	// Seed sources with sequences 1..numSources
	for i := 0; i < len(f.sources); i++ {
		f.sources[i] = f.nextSeq
		f.nextSeq++
	}
}

func (f *testProcessEntryFeed) Next() *LogEntry {

	// Select the next sequence from a source at random.  Simulates unordered global sequences arriving over DCP
	sourceIndex := rand.Intn(len(f.sources))
	entrySeq := f.sources[sourceIndex]
	f.sources[sourceIndex] = f.nextSeq
	f.nextSeq++

	return &LogEntry{
		Sequence:     entrySeq,
		DocID:        fmt.Sprintf("doc_%d", entrySeq),
		RevID:        "1-abcdefabcdefabcdef",
		Channels:     f.channelMaps[rand.Intn(len(f.channelMaps))],
		TimeReceived: f.fixedTime,
		TimeSaved:    f.fixedTime,
	}
}

// Cases for ProcessEntry benchmarking
//   - single thread, ordered sequence arrival, non-initialized cache, 100 channels
//   - single thread, ordered sequence arrival, initialized cache, 100 channels
//   - single thread, unordered sequence arrival, non-initialized cache, 100 channels
//   - single thread, unordered sequence arrival, initialized cache, 100 channels
//   - multiple threads, unordered sequence arrival, initialized cache, 100 channels
// other:
//   - non-unique doc ids?

func BenchmarkProcessEntry(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelError, base.KeyCache, base.KeyChanges)
	processEntryBenchmarks := []struct {
		name           string
		feed           *testProcessEntryFeed
		warmCacheCount int
	}{
		{
			"SingleThread_OrderedFeed_NoActiveChannels",
			NewTestProcessEntryFeed(100, 1),
			0,
		},
		{
			"SingleThread_OrderedFeed_ActiveChannels",
			NewTestProcessEntryFeed(100, 1),
			100,
		},
		{
			"SingleThread_OrderedFeed_ManyActiveChannels",
			NewTestProcessEntryFeed(35000, 1),
			35000,
		},
		{
			"SingleThread_NonOrderedFeed_NoActiveChannels",
			NewTestProcessEntryFeed(100, 10),
			0,
		},
		{
			"SingleThread_NonOrderedFeed_ActiveChannels",
			NewTestProcessEntryFeed(100, 10),
			100,
		},
		{
			"SingleThread_NonOrderedFeed_ManyActiveChannels",
			NewTestProcessEntryFeed(35000, 10),
			35000,
		},
	}

	for _, bm := range processEntryBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := base.TestCtx(b)
			context, err := NewDatabaseContext(ctx, "db", base.GetTestBucket(b), false, DatabaseContextOptions{})
			require.NoError(b, err)
			defer context.Close(ctx)

			ctx = context.AddDatabaseLogContext(ctx)
			err = context.StartOnlineProcesses(ctx)
			require.NoError(b, err)

			collection := GetSingleDatabaseCollection(b, context)
			collectionID := collection.GetCollectionID()

			changeCache := &changeCache{}
			if err := changeCache.Init(ctx, context, context.channelCache, nil, nil, context.MetadataKeys); err != nil {
				log.Printf("Init failed for changeCache: %v", err)
				b.Fail()
			}

			if err := changeCache.Start(0); err != nil {
				log.Printf("Start error for changeCache: %v", err)
				b.Fail()
			}
			defer changeCache.Stop(ctx)

			require.NoError(b, err)
			if bm.warmCacheCount > 0 {
				for i := 0; i < bm.warmCacheCount; i++ {
					channel := channels.NewID(fmt.Sprintf("channel_%d", i), collectionID)
					_, err := changeCache.GetChanges(ctx, channel, getChangesOptionsWithZeroSeq(b))
					if err != nil {
						log.Printf("GetChanges failed for changeCache: %v", err)
						b.Fail()
					}
				}

			}
			bm.feed.reset()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry := bm.feed.Next()
				_ = changeCache.processEntry(ctx, entry)
			}
		})
	}
}

type testDocChangedFeed struct {
	nextSeq      uint64
	channelNames []string
	sources      []uint64 // Used for non-sequential sequence delivery when numSources > 1
	fixedTime    time.Time
}

func NewTestDocChangedFeed(numChannels int, numSources int) *testDocChangedFeed {

	feed := &testDocChangedFeed{
		fixedTime: time.Now(),
		sources:   make([]uint64, numSources),
	}
	feed.channelNames = make([]string, numChannels)
	for i := 0; i < numChannels; i++ {
		feed.channelNames[i] = fmt.Sprintf("channel_%d", i)
	}
	feed.reset()
	return feed
}

// Reset sets nextSeq to 1, and reseeds sources
func (f *testDocChangedFeed) reset() {
	f.nextSeq = 1
	// Seed sources with sequences 1..numSources
	for i := 0; i < len(f.sources); i++ {
		f.sources[i] = f.nextSeq
		f.nextSeq++
	}
}

func (f *testDocChangedFeed) Next() sgbucket.FeedEvent {

	// Select the next sequence from a source at random.  Simulates unordered global sequences arriving over DCP
	sourceIndex := rand.Intn(len(f.sources))
	entrySeq := f.sources[sourceIndex]
	f.sources[sourceIndex] = f.nextSeq
	f.nextSeq++

	channelName := f.channelNames[rand.Intn(len(f.channelNames))]
	// build value, including xattr
	xattrValue := fmt.Sprintf(`{"rev":"1-d938e0614de222fe04463b9654e93156","sequence":%d,"recent_sequences":[%d],"history":{"revs":["1-d938e0614de222fe04463b9654e93156"],"parents":[-1],"channels":[["%s"]]},"channels":{"%s":null},"cas":"0x0000aeed831bd415","value_crc32c":"0x8aa182c1","time_saved":"2019-11-04T16:07:03.300815-08:00"}`,
		entrySeq,
		entrySeq,
		channelName,
		channelName,
	)
	docBody := fmt.Sprintf(`{"channels":["%s"]}`, channelName)
	value := sgbucket.EncodeValueWithXattrs([]byte(docBody), sgbucket.Xattr{Name: base.SyncXattrName, Value: []byte(xattrValue)})

	return sgbucket.FeedEvent{
		Opcode:       sgbucket.FeedOpMutation,
		Key:          []byte(fmt.Sprintf("doc_%d", entrySeq)),
		Value:        value,
		DataType:     base.MemcachedDataTypeXattr,
		Cas:          192335130121237, // 0x0000aeed831bd415
		Expiry:       0,
		Synchronous:  true,
		TimeReceived: time.Now(),
		VbNo:         0,
	}
}

// Cases for DocChanged benchmarking
//   - single thread, ordered sequence arrival, non-initialized cache, 100 channels
//   - single thread, ordered sequence arrival, initialized cache, 100 channels
//   - single thread, unordered sequence arrival, non-initialized cache, 100 channels
//   - single thread, unordered sequence arrival, initialized cache, 100 channels
//   - multiple threads, unordered sequence arrival, initialized cache, 100 channels
// other:
//   - non-unique doc ids?

func BenchmarkDocChanged(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelError, base.KeyCache, base.KeyChanges)
	processEntryBenchmarks := []struct {
		name           string
		feed           *testDocChangedFeed
		warmCacheCount int
	}{
		{
			"SingleThread_OrderedFeed_NoActiveChannels",
			NewTestDocChangedFeed(100, 1),
			0,
		},
		{
			"SingleThread_OrderedFeed_ActiveChannels",
			NewTestDocChangedFeed(100, 1),
			100,
		},
		{
			"SingleThread_OrderedFeed_ManyActiveChannels",
			NewTestDocChangedFeed(35000, 1),
			35000,
		},
		{
			"SingleThread_NonOrderedFeed_NoActiveChannels",
			NewTestDocChangedFeed(100, 10),
			0,
		},
		{
			"SingleThread_NonOrderedFeed_ActiveChannels",
			NewTestDocChangedFeed(100, 10),
			100,
		},
		{
			"SingleThread_NonOrderedFeed_ManyActiveChannels",
			NewTestDocChangedFeed(35000, 10),
			35000,
		},
	}

	for _, bm := range processEntryBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := base.TestCtx(b)
			context, err := NewDatabaseContext(ctx, "db", base.GetTestBucket(b), false, DatabaseContextOptions{})
			require.NoError(b, err)
			defer context.Close(ctx)

			err = context.StartOnlineProcesses(ctx)
			require.NoError(b, err)

			collection := GetSingleDatabaseCollection(b, context)
			collectionID := collection.GetCollectionID()

			ctx = context.AddDatabaseLogContext(ctx)
			changeCache := &changeCache{}
			if err := changeCache.Init(ctx, context, context.channelCache, nil, nil, context.MetadataKeys); err != nil {
				log.Printf("Init failed for changeCache: %v", err)
				b.Fail()
			}
			if err := changeCache.Start(0); err != nil {
				log.Printf("Start error for changeCache: %v", err)
				b.Fail()
			}
			defer changeCache.Stop(ctx)

			require.NoError(b, err)
			if bm.warmCacheCount > 0 {
				for i := 0; i < bm.warmCacheCount; i++ {
					channel := channels.NewID(fmt.Sprintf("channel_%d", i), collectionID)
					_, err := changeCache.GetChanges(ctx, channel, getChangesOptionsWithZeroSeq(b))
					if err != nil {
						log.Printf("GetChanges failed for changeCache: %v", err)
						b.Fail()
					}
				}

			}

			bm.feed.reset()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				feedEntry := bm.feed.Next()
				changeCache.DocChanged(feedEntry)
			}

			// log.Printf("maxNumPending: %v", changeCache.context.DbStats.StatsCblReplicationPull().Get(base.StatKeyMaxPending))
			// log.Printf("cachingCount: %v", changeCache.context.DbStats.StatsDatabase().Get(base.StatKeyDcpCachingCount))
		})
	}
}

// TestProcessSkippedEntry:
//   - Creates change cache with minimal pending seq wait time to push sequences to skipped quick
//   - Push a sequence higher than expected to cache
//   - Have NewTestProcessEntryFeed push sequences to the cache and assert that the current skipped sequence count decrements each time
//   - Iterate through the skipped slice and cumulative add number of sequences currently in the slice, assert this is
//     equal to the corresponding stat
//   - Assert that the length of slice stat is equal to the length of the skipped sequence slice + assert that number of
//     sequences skipped overall stat is correct
func TestProcessSkippedEntry(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CacheSkippedSeqMaxWait: 2 * time.Minute,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	feed := NewTestProcessEntryFeed(1, 5)

	// add cache entry that is higher than expected
	highEntry := &LogEntry{
		Sequence:     20,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, highEntry)

	// update cache stats for assertions
	testChangeCache.updateStats(ctx)

	// assert this pushes an entry on the skipped sequence slice
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, len(testChangeCache.skippedSeqs.list))
	}, time.Second*10, time.Millisecond*100)

	// process some sequences over cache
	currNumSkippedSeqs := dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value()
	for j := 0; j < 10; j++ {
		en := feed.Next()
		_ = testChangeCache.processEntry(ctx, en)
		testChangeCache.updateStats(ctx)
		assert.Equal(t, currNumSkippedSeqs-1, dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		currNumSkippedSeqs = dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value()
	}

	// assert on skipped sequence slice stats after above operations on cache
	var numSeqsInList int64
	for _, v := range testChangeCache.skippedSeqs.list {
		numSeqsInList = numSeqsInList + v.getNumSequencesInEntry()
	}
	assert.Equal(t, numSeqsInList, dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
	assert.Equal(t, int64(len(testChangeCache.skippedSeqs.list)), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
	assert.Equal(t, int64(19), dbContext.DbStats.CacheStats.NumSkippedSeqs.Value())
}

// TestProcessSkippedEntryStats:
//   - Creates change cache with minimal pending seq wait time to push sequences to skipped quick
//   - Push a sequence higher than expected to cache
//   - Push some sequences that are skipped onto the cache and assert that the stats: number of current skipped sequences,
//     length of slice, cumulative number of skipped sequences and capacity of slice are all updated as expected
func TestProcessSkippedEntryStats(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CacheSkippedSeqMaxWait: 2 * time.Minute,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// add cache entry that is higher than expected
	highEntry := &LogEntry{
		Sequence:     20,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, highEntry)

	// update cache stats for assertions
	testChangeCache.updateStats(ctx)

	// assert this pushes an entry on the skipped sequence slice
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, len(testChangeCache.skippedSeqs.list))
	}, time.Second*10, time.Millisecond*100)

	// expected values for stats on skipped slice
	arrivingSeqs := []uint64{3, 15, 18, 2, 1}
	expSliceLen := []int64{2, 3, 4, 4, 3}
	expSliceCap := []int64{2, 4, 4, 4, 4}

	numSeqsInList := dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value()
	for j := 0; j < len(arrivingSeqs); j++ {
		newEntry := &LogEntry{
			DocID:    fmt.Sprintf("doc_%d", arrivingSeqs),
			RevID:    "1-abcdefabcdefabcdef",
			Sequence: arrivingSeqs[j],
		}

		_ = testChangeCache.processEntry(ctx, newEntry)
		// assert on skipped sequence slice stats
		testChangeCache.updateStats(ctx)
		assert.Equal(t, numSeqsInList-1, dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(t, expSliceLen[j], dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(t, int64(19), dbContext.DbStats.CacheStats.NumSkippedSeqs.Value())
		assert.Equal(t, expSliceCap[j], dbContext.DbStats.CacheStats.SkippedSeqCap.Value())
		numSeqsInList = dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value()
	}

}

// TestSkippedSequenceCompact:
//   - Creates change cache with minimal pending seq wait time to push sequences to skipped quick but with
//     small max wait on skipped sequences
//   - Push a sequence higher than expected to cache
//   - Assert that the skipped slice eventually ends up with 0 length and the number of abandoned sequences are as expected
func TestSkippedSequenceCompact(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CacheSkippedSeqMaxWait: 2 * time.Second,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// add cache entry that is higher than expected
	highEntry := &LogEntry{
		Sequence:     20,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, highEntry)

	// assert this pushes an entry on the skipped sequence slice
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, len(testChangeCache.skippedSeqs.list))
	}, time.Second*10, time.Millisecond*100)

	// assert that compaction empties the skipped slice and we have correct value for abandoned sequences
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(19), dbContext.DbStats.CacheStats.AbandonedSeqs.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingEverythingSkipped:
//   - Test release of an unused sequence range when all sequences in the range are skipped sequences
func TestReleasedSequenceRangeHandlingEverythingSkipped(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 5 * time.Millisecond,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// add cache entry that is higher than expected
	highEntry := &LogEntry{
		Sequence:     20,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, highEntry)

	// assert that skipped list is filled and next seq at cache is updated
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(19), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(21), testChangeCache.nextSequence)
	}, time.Second*10, time.Millisecond*100)

	// process unused sequence range
	testChangeCache.releaseUnusedSequenceRange(ctx, 1, 20, time.Now())

	// assert on cache stats after removal from, skipped list
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(20), dbContext.DbStats.CacheStats.HighSeqCached.Value())
		assert.Equal(c, uint64(21), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, uint64(20), testChangeCache._getMaxStableCached(ctx))
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingEverythingPending:
//   - Test release of an unused sequence range when all range sequences will be pushed top pending
func TestReleasedSequenceRangeHandlingEverythingPending(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  5,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// add cache entry
	entry := &LogEntry{
		Sequence:     1,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// process unused sequence range
	testChangeCache.releaseUnusedSequenceRange(ctx, 21, 25, time.Now())

	// assert that whole range is processed onto pending
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, uint64(2), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(25), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingEverythingPendingAndProcessPending:
//   - Test release of an unused sequence range when all sequences in the range are pushed to pending.
//   - Then add a sequence to unblock pending and assert that the sequence range is processed off pending
func TestReleasedSequenceRangeHandlingEverythingPendingAndProcessPending(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  5,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// process unused sequence range
	testChangeCache.releaseUnusedSequenceRange(ctx, 2, 25, time.Now())

	// assert that we have pending as expected
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
	}, time.Second*10, time.Millisecond*100)

	// add cache entry to unblock pending
	entry := &LogEntry{
		Sequence:     1,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert on stats after unblocking pending
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(25), dbContext.DbStats.CacheStats.HighSeqCached.Value())
		assert.Equal(c, uint64(26), testChangeCache.nextSequence)
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingEverythingPendingLowPendingCapacity:
//   - Test release of an unused sequence range when all sequences in the range are pushed to pending.
//   - Then add another pending entry to take over pending capacity and assert that the range is processed correctly
//   - Then add another range to completely unblock pending
func TestReleasedSequenceRangeHandlingEverythingPendingLowPendingCapacity(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  1,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// process unused sequence range
	testChangeCache.releaseUnusedSequenceRange(ctx, 2, 25, time.Now())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
	}, time.Second*10, time.Millisecond*100)

	// add cache entry that will be pushed to pending
	entry := &LogEntry{
		Sequence:     30,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert on cache stats:
	// - Sequence 1 should be skipped
	// - Pending should hold sequence 30
	// - Range 2-25 should've been processed pushing nextSeq up to 26
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, uint64(26), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(25), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// add a new contiguous sequence
	entry = &LogEntry{
		Sequence:     26,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert we update cache as expected
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(26), dbContext.DbStats.CacheStats.HighSeqCached.Value())
		assert.Equal(c, uint64(27), testChangeCache.nextSequence)
	}, time.Second*10, time.Millisecond*100)

	// process unused sequence range to catch up with pending seq in cache
	testChangeCache.releaseUnusedSequenceRange(ctx, 27, 29, time.Now())

	// assert on cache state after cache is caught up with pending
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, uint64(31), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(30), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingSingleSequence:
//   - Test release of single sequence range being pushed to pending
//   - Assert this single range is processed off pending after new entry is added taking pending over the limit
//   - Test release of another single range that is now skipped and assert that skipped is emptied
func TestReleasedSequenceRangeHandlingSingleSequence(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  1,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// process single unused sequence range that should end up going to pending
	testChangeCache.releaseUnusedSequenceRange(ctx, 2, 2, time.Now())

	// assert single sequence range is pending
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, uint64(1), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(2), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// process change that should overload pending and push sequence 1 to skipped
	entry := &LogEntry{
		Sequence:     4,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, uint64(3), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(2), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// process single unused sequence range that should empty skipped list
	testChangeCache.releaseUnusedSequenceRange(ctx, 1, 1, time.Now())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, uint64(3), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(2), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingEdgeCase1:
//   - Test releasing unused sequence range with everything in range being pushed to pending
//   - Test the handling at the cache when endSeq on unused sequence range is in the pending list
//   - This test is lower level version of TestJumpInSequencesAtAllocatorRangeInPending
func TestReleasedSequenceRangeHandlingEdgeCase1(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  1,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// process change that should push pending and subsequently be cached
	entry := &LogEntry{
		Sequence:     20,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert that the pending list + next sequence is as we expect (not immediately hitting skipped sequence handling)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(1), testChangeCache.nextSequence)
	}, time.Second*10, time.Millisecond*100)

	// process unusedSeq range with pending seq equal to end
	testChangeCache.releaseUnusedSequenceRange(ctx, 1, 20, time.Now())

	// assert that the pending list is empty + high seq cached + next seq is as expected
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(21), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(20), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingEdgeCase2:
//   - Test releasing unused sequence range with everything startSeq to endSeq - 1 in skipped
//   - Test the handling at the cache when endSeq on unused sequence range is in the pending list
//   - This test is lower level version of TestJumpInSequencesAtAllocatorSkippedSequenceFill
func TestReleasedSequenceRangeHandlingEdgeCase2(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 100 * time.Millisecond,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  1,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// process change that should push pending and subsequently be cached
	entry := &LogEntry{
		Sequence:     20,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert that the skipped list is filled
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(19), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(20), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// process unusedSeq range with pending seq equal to end
	testChangeCache.releaseUnusedSequenceRange(ctx, 1, 20, time.Now())

	// assert that the pending list is empty + high seq cached + next seq is as expected
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(21), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(20), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingDuplicateSequencesInPending:
//   - Test releasing unused sequence range that should be pushed to pending
//   - Mock having a sequences in middle of the range already in pending
//   - Assert that the range pushed is split into separate ranges onto pending list
//   - Add new unused range to unblocks pending , assert it empties pending and skipped is not filled
func TestReleasedSequenceRangeHandlingDuplicateSequencesInPending(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  20,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// push two entries that will be pushed to pending
	entry := &LogEntry{
		Sequence:     14,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	entry = &LogEntry{
		Sequence:     18,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// process unusedSeq range with pending range containing duplicate sequences that are in pending list
	// Pending should contain: (10-13), 14, (15-17) 18, (19-20)
	testChangeCache.releaseUnusedSequenceRange(ctx, 10, 20, time.Now())

	// assert pending has 4 elements
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(5), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(1), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(20), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// unblock pending and assert items are processed correct
	testChangeCache.releaseUnusedSequenceRange(ctx, 1, 10, time.Now())

	// assert pending empties
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(21), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(20), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}

// TestReleasedSequenceRangeHandlingDuplicateSequencesInSkipped:
//   - Test releasing unused sequence range that has duplicate sequences in skipped sequence list
//   - Assert sequences in the range are removed from skipped
//   - Empty skipped through unused sequence release
//   - Add new contiguous sequence and assert it is processed correctly
func TestReleasedSequenceRangeHandlingDuplicateSequencesInSkipped(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	dbContext, err := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	})
	require.NoError(t, err)
	defer dbContext.Close(ctx)

	ctx = dbContext.AddDatabaseLogContext(ctx)
	err = dbContext.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	testChangeCache := &changeCache{}
	if err := testChangeCache.Init(ctx, dbContext, dbContext.channelCache, nil, &CacheOptions{
		CachePendingSeqMaxWait: 20 * time.Minute,
		CacheSkippedSeqMaxWait: 20 * time.Minute,
		CachePendingSeqMaxNum:  0,
	}, dbContext.MetadataKeys); err != nil {
		log.Printf("Init failed for testChangeCache: %v", err)
		t.Fail()
	}

	if err := testChangeCache.Start(0); err != nil {
		log.Printf("Start error for testChangeCache: %v", err)
		t.Fail()
	}
	defer testChangeCache.Stop(ctx)
	require.NoError(t, err)

	// push two entries that will be pushed to pending and subsequently skipped will be filled with sequence gaps
	entry := &LogEntry{
		Sequence:     14,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	entry = &LogEntry{
		Sequence:     18,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert skipped is filled
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(2), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(16), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(19), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(18), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// process unusedSeq range with range containing duplicate sipped sequences
	// Skipped should contain: (1-13), (15-17) before processing this range
	testChangeCache.releaseUnusedSequenceRange(ctx, 10, 17, time.Now())

	// assert skipped list altered to reflect the above range is processed
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(1), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(9), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(19), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(18), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// Skipped should contain: (1-9) before processing this range
	testChangeCache.releaseUnusedSequenceRange(ctx, 1, 9, time.Now())

	// assert skipped list is emptied after the above range is processed
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(19), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(18), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// assert a new contiguous sequence is processed correctly
	entry = &LogEntry{
		Sequence:     19,
		DocID:        fmt.Sprintf("doc_%d", 50),
		RevID:        "1-abcdefabcdefabcdef",
		TimeReceived: time.Now(),
		TimeSaved:    time.Now(),
	}
	_ = testChangeCache.processEntry(ctx, entry)

	// assert on stats
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		testChangeCache.updateStats(ctx)
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.SkippedSeqLen.Value())
		assert.Equal(c, int64(0), dbContext.DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, uint64(20), testChangeCache.nextSequence)
		dbContext.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(19), dbContext.DbStats.CacheStats.HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)
}
