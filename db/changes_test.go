//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/google/uuid"
)

func TestFilterToAvailableChannels(t *testing.T) {
	testCases := []struct {
		name                 string
		genChanAndDocs       int      // Amount of docs and channels to generate (ie. doc1 ch1, doc2 ch2...)
		userChans            base.Set // Channels user is in
		accessChans          base.Set // Channels to get changes for
		expectedDocsReturned []string // Expected Doc IDs returned
	}{
		{
			// Should log "Channels [ ch2 ] request without access by user test" - CBG-1326
			name:                 "Info logged when channels dropped from list",
			genChanAndDocs:       3,
			userChans:            base.SetOf("ch1", "ch3"),
			accessChans:          base.SetOf("ch1", "ch2", "ch3"),
			expectedDocsReturned: []string{"doc1", "doc3"},
		}, {
			name:                 "No info logged if no channels dropped from list",
			genChanAndDocs:       3,
			userChans:            base.SetOf("ch1", "ch3"),
			accessChans:          base.SetOf("ch1", "ch3"),
			expectedDocsReturned: []string{"doc1", "doc3"},
		}, {
			name:                 "No info logged when using wildcard",
			genChanAndDocs:       3,
			userChans:            base.SetOf("ch1", "ch3"),
			accessChans:          base.SetOf("*"),
			expectedDocsReturned: []string{"doc1", "doc3"},
		},
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges)
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

			auth := db.Authenticator(base.TestCtx(t))
			user, err := auth.NewUser("test", "pass", testCase.userChans)
			require.NoError(t, err)
			require.NoError(t, auth.Save(user))

			for i := 0; i < testCase.genChanAndDocs; i++ {
				id := fmt.Sprintf("%d", i+1)
				_, _, err = collection.Put(ctx, "doc"+id, Body{"channels": []string{"ch" + id}})
				require.NoError(t, err)
			}
			db.WaitForPendingChanges(t)

			collection.user, err = auth.GetUser("test")
			require.NoError(t, err)

			ch := getChanges(t, collection, testCase.accessChans, getChangesOptionsWithZeroSeq(t))
			require.NoError(t, err)
			require.Len(t, ch, len(testCase.expectedDocsReturned))

			match := true // Check if expected matches with actual in-order
			for i, change := range ch {
				if change.ID != testCase.expectedDocsReturned[i] {
					match = false
				}
			}
			assert.True(t, match)
		})
	}
}

// Unit test for bug #314
func TestChangesAfterChannelAdded(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)
	db, ctx := setupTestDBDefaultCollection(t)
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(base.TestCtx(t))
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create a doc on two channels (sequence 1):
	_, _, err = collection.Put(ctx, "doc1", Body{"channels": []string{"ABC", "PBS"}})
	require.NoError(t, err)
	cacheWaiter.AddAndWait(1)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipalForTest(t, "naomi", true)
	require.NoError(t, err)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")

	_, _, err = db.UpdatePrincipal(base.TestCtx(t), userInfo, true, true)
	assert.NoError(t, err, "UpdatePrincipal failed")

	db.WaitForPendingChanges(t)

	// Check the _changes feed:
	collection.user, err = authenticator.GetUser("naomi")
	require.NoError(t, err)
	changes := getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))
	printChanges(changes)
	require.Len(t, changes, 3)

	// doc1, from ABC
	assert.Equal(t, "doc1", changes[0].ID)
	assert.True(t, changes[0].Seq.TriggeredBy == 0)

	// doc1, from PBS backfill
	assert.Equal(t, "doc1", changes[1].ID)
	assert.True(t, changes[1].Seq.TriggeredBy > 0)
	assert.True(t, changes[0].Seq.Seq == changes[1].Seq.Seq)

	// User doc
	assert.Equal(t, "_user/naomi", changes[2].ID)
	assert.True(t, changes[2].principalDoc)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = ParsePlainSequenceID(lastSeq.String())

	// Add a new doc (sequence 3):
	revid, _, err := collection.Put(ctx, "doc2", Body{"channels": []string{"PBS"}})
	require.NoError(t, err)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	cacheWaiter.AddAndWait(1)
	changes = getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithSeq(t, lastSeq))

	assert.NoError(t, err, "Couldn't GetChanges (2nd)")

	require.Len(t, changes, 1)
	assert.Equal(t, "doc2", changes[0].ID)
	assert.Equal(t, []ChangeByVersionType{{"rev": revid}}, changes[0].Changes)

	// validate from zero
	changes = getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))
	printChanges(changes)

}

func printChanges(changes []*ChangeEntry) {
	for _, change := range changes {
		log.Printf("Change:%+v", change)
	}
}

func getLastSeq(changes []*ChangeEntry) SequenceID {
	if len(changes) > 0 {
		return changes[len(changes)-1].Seq
	}
	return SequenceID{}
}

// Makes changes options starting at sequence 0, with a new changes context
func getChangesOptionsWithZeroSeq(t testing.TB) ChangesOptions {
	return ChangesOptions{Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t)}
}

// Makes changes options with a since value of seq and a new changes context
func getChangesOptionsWithSeq(t *testing.T, seq SequenceID) ChangesOptions {
	return ChangesOptions{Since: seq, ChangesCtx: base.TestCtx(t)}
}

// Makes changes options a new changes context
func getChangesOptionsWithCtxOnly(t *testing.T) ChangesOptions {
	return ChangesOptions{ChangesCtx: base.TestCtx(t)}
}

func TestDocDeletionFromChannelCoalescedRemoved(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test is known to be failing against couchbase server with XATTRS enabled.  See https://gist.github.com/tleyden/a41632355fadde54f19e84ba68015512")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	// Create a user with access to channel A
	authenticator := db.Authenticator(base.TestCtx(t))
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "A"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create a doc on two channels (sequence 1):
	revid, _, err := collection.Put(ctx, "alpha", Body{"channels": []string{"A", "B"}})
	require.NoError(t, err)
	cacheWaiter.AddAndWait(1)

	collection.user, err = authenticator.GetUser("alice")
	require.NoError(t, err)
	changes := getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))
	printChanges(changes)
	assert.Len(t, changes, 1)
	collectionID := collection.GetCollectionID()
	require.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 1},
		ID:           "alpha",
		Changes:      []ChangeByVersionType{{"rev": revid}},
		collectionID: collectionID}, changes[0])

	lastSeq := getLastSeq(changes)
	lastSeq, _ = ParsePlainSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := collection.dataStore.GetRaw(ctx, "alpha") // cas, err

	// Unmarshall into nested maps
	var x map[string]any
	assert.NoError(t, base.JSONUnmarshal(rv, &x))

	sync := x[base.SyncXattrName].(map[string]any)
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	cm := make(channels.ChannelMap)
	cm["A"] = &channels.ChannelRemoval{Seq: 2, Rev: channels.RevAndVersion{RevTreeID: "2-e99405a23fa102238fa8c3fd499b15bc"}}
	sync["channels"] = cm

	history := sync["history"].(map[string]any)
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("B"), base.SetOf("B")}

	// Marshall back to JSON
	b, err := base.JSONMarshal(x)
	require.NoError(t, err)

	// Update raw document in the bucket
	assert.NoError(t, collection.dataStore.SetRaw(ctx, "alpha", 0, nil, b))

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 3 and isn't stuck waiting for it.
	cacheWaiter.AddAndWait(1)
	changes = getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithSeq(t, lastSeq))

	assert.Len(t, changes, 1)
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 2},
		ID:           "alpha",
		Removed:      base.SetOf("A"),
		allRemoved:   true,
		Changes:      []ChangeByVersionType{{"rev": "2-e99405a23fa102238fa8c3fd499b15bc"}},
		collectionID: collectionID}, changes[0])

	printChanges(changes)
}

func TestCVPopulationOnChangeEntry(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionID := collection.GetCollectionID()
	sourceID := db.EncodedSourceID

	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	authenticator := db.Authenticator(base.TestCtx(t))
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "A"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection.user, _ = authenticator.GetUser("alice")

	// Make channel active
	changesOpts := getChangesOptionsWithZeroSeq(t)
	changesOpts.VersionType = ChangesVersionTypeCV
	_, err = db.channelCache.GetChanges(ctx, channels.NewID("A", collectionID), changesOpts)
	require.NoError(t, err)

	_, doc, err := collection.Put(ctx, "doc1", Body{"channels": []string{"A"}})
	require.NoError(t, err)

	db.WaitForPendingChanges(t)

	changes := getChanges(t, collection, base.SetOf("A"), changesOpts)
	require.NoError(t, err)

	docVersion := GetChangeEntryCV(t, changes[0])
	assert.Equal(t, doc.ID, changes[0].ID)
	assert.Equal(t, sourceID, docVersion.SourceID)
	assert.Equal(t, doc.HLV.Version, docVersion.Value)
}

func TestDocDeletionFromChannelCoalesced(t *testing.T) {
	if base.TestUseXattrs() {
		t.Skip("This test is known to be failing against couchbase server with XATTRS enabled.  Same error as TestDocDeletionFromChannelCoalescedRemoved")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	// Create a user with access to channel A
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "A"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create a doc on two channels (sequence 1):
	revid, _, err := collection.Put(ctx, "alpha", Body{"channels": []string{"A", "B"}})
	require.NoError(t, err)
	cacheWaiter.AddAndWait(1)

	collection.user, err = authenticator.GetUser("alice")
	require.NoError(t, err)
	changes := getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))
	printChanges(changes)

	collectionID := collection.GetCollectionID()
	assert.Len(t, changes, 1)
	require.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 1},
		ID:           "alpha",
		Changes:      []ChangeByVersionType{{"rev": revid}},
		collectionID: collectionID}, changes[0])

	lastSeq := getLastSeq(changes)
	lastSeq, _ = ParsePlainSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := collection.dataStore.GetRaw(ctx, "alpha") // cas, err

	// Unmarshall into nested maps
	var x map[string]any
	assert.NoError(t, base.JSONUnmarshal(rv, &x))

	sync := x[base.SyncXattrName].(map[string]any)
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	history := sync["history"].(map[string]any)
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("A", "B"), base.SetOf("A", "B")}

	// Marshall back to JSON
	b, err := base.JSONMarshal(x)
	require.NoError(t, err)

	// Update raw document in the bucket
	require.NoError(t, collection.dataStore.SetRaw(ctx, "alpha", 0, nil, b))

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 3 (the modified document) and isn't stuck waiting for it.
	cacheWaiter.AddAndWait(1)

	changes = getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithSeq(t, lastSeq))

	assert.Len(t, changes, 1)
	require.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 3},
		ID:           "alpha",
		Changes:      []ChangeByVersionType{{"rev": "3-e99405a23fa102238fa8c3fd499b15bc"}},
		collectionID: collectionID}, changes[0])

	printChanges(changes)
}

func TestActiveOnlyCacheUpdate(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)
	// Create 10 documents
	revId := ""
	var err error
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		body := Body{"foo": "bar"}
		revId, _, err = collection.Put(ctx, key, body)
		require.NoError(t, err, "Couldn't create document")
	}

	// Tombstone 5 documents
	for i := 2; i <= 6; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, _, err = collection.DeleteDoc(ctx, key, DocVersion{RevTreeID: revId})
		require.NoError(t, err, "Couldn't delete document")
	}

	db.WaitForPendingChanges(t)

	changesOptions := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		ChangesCtx: base.TestCtx(t),
	}

	initQueryCount := db.DbStats.Cache().ViewQueries.Value()

	// Get changes with active_only=true
	activeChanges := getChanges(t, collection, base.SetOf("*"), changesOptions)
	require.Len(t, activeChanges, 5)

	// Ensure the test is triggering a query, and not serving from DCP-generated cache
	postChangesQueryCount := db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initQueryCount+1, postChangesQueryCount)

	// Get changes with active_only=false, validate that triggers a new query
	changesOptions.ActiveOnly = false
	allChanges := getChanges(t, collection, base.SetOf("*"), changesOptions)
	require.Len(t, allChanges, 10)

	postChangesQueryCount = db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initQueryCount+2, postChangesQueryCount)

	// Get changes with active_only=false again, verify results are served from the cache
	changesOptions.ActiveOnly = false
	allChanges = getChanges(t, collection, base.SetOf("*"), changesOptions)
	require.Len(t, allChanges, 10)

	postChangesQueryCount = db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initQueryCount+2, postChangesQueryCount)

}

func TestChangesOptionsStringer(t *testing.T) {
	opts := ChangesOptions{}
	var stringerFields []string
	for key := range strings.SplitSeq(opts.String()[1:len(opts.String())-1], ",") {
		fieldName, _, found := strings.Cut(strings.Trim(key, `" ,`), ":")
		require.True(t, found, "Expected , in %s", key)
		stringerFields = append(stringerFields, fieldName)
	}
	ignoredFields := map[string]struct{}{
		"ChangesCtx": {},
		"clientType": {},
	}
	var expectedFields []string
	for _, field := range reflect.VisibleFields(reflect.TypeOf(ChangesOptions{})) {
		// some field names are not in stringer
		if _, ok := ignoredFields[field.Name]; ok {
			continue
		}
		expectedFields = append(expectedFields, field.Name)
	}
	require.ElementsMatch(t, expectedFields, stringerFields)
}

// TestCurrentVersionPopulationOnChannelCache:
//   - Make channel active on cache
//   - Add a doc that is assigned this channel
//   - Get the sync data of that doc to assert against the HLV defined on it
//   - Wait for the channel cache to be populated with this doc write
//   - Assert the CV in the entry fetched from channel cache matches the sync data CV and the bucket UUID on the database context
func TestCurrentVersionPopulationOnChannelCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport, base.KeyDCP, base.KeyCache, base.KeyHTTP)
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionID := collection.GetCollectionID()
	sourceID := db.EncodedSourceID
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	// Make channel active
	_, err := db.channelCache.GetChanges(ctx, channels.NewID("ABC", collectionID), getChangesOptionsWithZeroSeq(t))
	require.NoError(t, err)

	// Put a doc that gets assigned a CV to populate the channel cache with
	_, _, err = collection.Put(ctx, "doc1", Body{"channels": []string{"ABC"}})
	require.NoError(t, err)
	db.WaitForPendingChanges(t)

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	require.NoError(t, err)

	// get entry of above doc from channel cache
	entries, err := db.channelCache.GetChanges(ctx, channels.NewID("ABC", collectionID), getChangesOptionsWithZeroSeq(t))
	require.NoError(t, err)
	require.NotNil(t, entries)

	// assert that the source and version has been populated with the channel cache entry for the doc
	assert.Equal(t, "doc1", entries[0].DocID)
	require.NotZero(t, entries[0].Version)
	assert.Equal(t, sourceID, entries[0].SourceID)
	assert.Equal(t, doc.HLV.SourceID, entries[0].SourceID)
	assert.Equal(t, doc.HLV.Version, entries[0].Version)
}

// TestActiveOnlyWithLimit verifies that when querying with ActiveOnly: true and a Limit,
// the pagination inside changesFeed does not terminate prematurely due to counting inactive/deleted
// entries as "sent", ensuring the client receives the requested number of active changes when available.
func TestActiveOnlyWithLimit(t *testing.T) {
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelQueryLimit = 3
	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	// 1. Create 6 documents that we will subsequently delete
	revs := make(map[string]string)
	for i := 1; i <= 6; i++ {
		key := fmt.Sprintf("doc_del_%d", i)
		body := Body{"foo": "bar"}
		revId, _, err := collection.Put(ctx, key, body)
		require.NoError(t, err)
		revs[key] = revId
	}

	// 2. Delete those 6 documents so we have 6 deleted sequences at the start of the feed
	for i := 1; i <= 6; i++ {
		key := fmt.Sprintf("doc_del_%d", i)
		_, _, err := collection.DeleteDoc(ctx, key, DocVersion{RevTreeID: revs[key]})
		require.NoError(t, err)
	}

	// 3. Create 4 active documents
	for i := 1; i <= 4; i++ {
		key := fmt.Sprintf("doc_act_%d", i)
		body := Body{"foo": "bar"}
		_, _, err := collection.Put(ctx, key, body)
		require.NoError(t, err)
	}

	db.WaitForPendingChanges(t)

	// Get changes with active_only=true and Limit=3
	changesOptions := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      3,
		ChangesCtx: base.TestCtx(t),
	}

	changes := getChanges(t, collection, base.SetOf("*"), changesOptions)
	// We should receive exactly 3 active changes ("doc_act_1", "doc_act_2", "doc_act_3")
	require.Len(t, changes, 3)
	assert.Equal(t, "doc_act_1", changes[0].ID)
	assert.Equal(t, "doc_act_2", changes[1].ID)
	assert.Equal(t, "doc_act_3", changes[2].ID)
}

// stubSingleChannelCache is a minimal SingleChannelCache test double that returns a scripted
// sequence of batches from GetChanges, one per call, in call order. It lets tests drive
// changesFeed's own pagination bookkeeping directly, without needing real documents, DCP, or a
// backing query/view implementation. Only GetChanges and ChannelID are exercised by changesFeed;
// the remaining methods are unused stubs to satisfy the interface.
type stubSingleChannelCache struct {
	channelID channels.ID
	batches   [][]*LogEntry // one slice per call to GetChanges; calls past the end return empty
	calls     int
}

func (s *stubSingleChannelCache) GetChanges(_ context.Context, _ ChangesOptions) ([]*LogEntry, error) {
	if s.calls >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.calls]
	s.calls++
	return batch, nil
}

func (s *stubSingleChannelCache) GetCachedChanges(_ ChangesOptions) (uint64, []*LogEntry) {
	return 0, nil
}

func (s *stubSingleChannelCache) ChannelID() channels.ID {
	return s.channelID
}

func (s *stubSingleChannelCache) SupportsLateFeed() bool {
	return false
}

func (s *stubSingleChannelCache) LateSequenceUUID() uuid.UUID {
	return uuid.UUID{}
}

func (s *stubSingleChannelCache) GetLateSequencesSince(_ uint64) ([]*LogEntry, uint64, error) {
	return nil, 0, nil
}

func (s *stubSingleChannelCache) RegisterLateSequenceClient() uint64 {
	return 0
}

func (s *stubSingleChannelCache) ReleaseLateSequenceClient(_ uint64) bool {
	return false
}

// drainChangesFeed reads a changesFeed's output channel to completion, failing the test on any
// error entry.
func drainChangesFeed(t *testing.T, feed <-chan *ChangeEntry) []*ChangeEntry {
	var received []*ChangeEntry
	for entry := range feed {
		require.NoError(t, entry.Err)
		received = append(received, entry)
	}
	return received
}

// TestChangesFeedActiveOnlyContinuesPastInactiveBatch reproduces the CBG-5555 bug directly against
// changesFeed, bypassing the cache/query layer entirely: a changesFeed that counts every entry it
// forwards (active or not) against the caller's requested Limit will believe it's done as soon as
// it has forwarded `Limit` raw entries - even if none of them were active. Here the first batch is
// two channel-removal entries (exactly Limit=2 raw rows, zero active), and the second batch holds
// the two active entries the caller actually asked for. A correct changesFeed must call GetChanges
// a second time to find them.
func TestChangesFeedActiveOnlyContinuesPastInactiveBatch(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionID := collection.GetCollectionID()
	channelID := channels.NewID("active", collectionID)

	stub := &stubSingleChannelCache{
		channelID: channelID,
		batches: [][]*LogEntry{
			{
				{DocID: "removed1", RevID: "1-a", Sequence: 1, Flags: channels.Removed, CollectionID: collectionID},
				{DocID: "removed2", RevID: "1-a", Sequence: 2, Flags: channels.Removed, CollectionID: collectionID},
			},
			{
				{DocID: "active1", RevID: "1-a", Sequence: 3, CollectionID: collectionID},
				{DocID: "active2", RevID: "1-a", Sequence: 4, CollectionID: collectionID},
			},
		},
	}

	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      2,
		ChangesCtx: base.TestCtx(t),
	}

	received := drainChangesFeed(t, collection.changesFeed(ctx, stub, options, "test"))

	// changesFeed forwards every entry it sees, active or not - ActiveOnly filtering happens
	// upstream in SimpleMultiChangesFeed. What matters here is that all 4 entries were retrieved at
	// all, which requires a second call to GetChanges.
	require.Len(t, received, 4)
	assert.Equal(t, "removed1", received[0].ID)
	assert.Equal(t, "removed2", received[1].ID)
	assert.Equal(t, "active1", received[2].ID)
	assert.Equal(t, "active2", received[3].ID)
	assert.Equal(t, 2, stub.calls, "changesFeed should have called GetChanges a second time to find the requested 2 active entries")
}

// TestChangesFeedActiveOnlyMultipleInactiveBatches is the same shape as
// TestChangesFeedActiveOnlyContinuesPastInactiveBatch but spans three all-inactive batches before
// the active entries appear, verifying pagination genuinely continues round after round rather than
// tolerating a single extra retry.
func TestChangesFeedActiveOnlyMultipleInactiveBatches(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionID := collection.GetCollectionID()
	channelID := channels.NewID("active", collectionID)

	inactiveBatch := func(seqStart uint64) []*LogEntry {
		return []*LogEntry{
			{DocID: fmt.Sprintf("removed%d", seqStart), RevID: "1-a", Sequence: seqStart, Flags: channels.Removed, CollectionID: collectionID},
			{DocID: fmt.Sprintf("removed%d", seqStart+1), RevID: "1-a", Sequence: seqStart + 1, Flags: channels.Removed, CollectionID: collectionID},
		}
	}

	stub := &stubSingleChannelCache{
		channelID: channelID,
		batches: [][]*LogEntry{
			inactiveBatch(1),
			inactiveBatch(3),
			inactiveBatch(5),
			{
				{DocID: "active1", RevID: "1-a", Sequence: 7, CollectionID: collectionID},
				{DocID: "active2", RevID: "1-a", Sequence: 8, CollectionID: collectionID},
			},
		},
	}

	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      2,
		ChangesCtx: base.TestCtx(t),
	}

	received := drainChangesFeed(t, collection.changesFeed(ctx, stub, options, "test"))

	require.Len(t, received, 8)
	assert.Equal(t, "active1", received[6].ID)
	assert.Equal(t, "active2", received[7].ID)
	assert.Equal(t, 4, stub.calls, "changesFeed should have paged through all three inactive batches to find the requested 2 active entries")
}

// TestChangesFeedActiveOnlyStopsWhenChannelExhausted verifies changesFeed terminates (rather than
// looping forever) when the channel runs out of data before satisfying the requested ActiveOnly
// Limit: the final batch is shorter than the pagination limit requested for that call, which is
// changesFeed's signal that the channel has no more data.
func TestChangesFeedActiveOnlyStopsWhenChannelExhausted(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collectionID := collection.GetCollectionID()
	channelID := channels.NewID("active", collectionID)

	stub := &stubSingleChannelCache{
		channelID: channelID,
		batches: [][]*LogEntry{
			{
				{DocID: "removed1", RevID: "1-a", Sequence: 1, Flags: channels.Removed, CollectionID: collectionID},
			},
		},
	}

	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      5,
		ChangesCtx: base.TestCtx(t),
	}

	received := drainChangesFeed(t, collection.changesFeed(ctx, stub, options, "test"))

	require.Len(t, received, 1)
	assert.Equal(t, "removed1", received[0].ID)
	assert.Equal(t, 1, stub.calls, "changesFeed should stop after a short batch signals the channel is exhausted")
}
