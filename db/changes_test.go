//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			err = collection.WaitForPendingChanges(base.TestCtx(t))
			require.NoError(t, err)

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

	err = collection.WaitForPendingChanges(base.TestCtx(t))
	assert.NoError(t, err)

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

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

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
	rv, _, _ := collection.dataStore.GetRaw("alpha") // cas, err

	// Unmarshall into nested maps
	var x map[string]interface{}
	assert.NoError(t, base.JSONUnmarshal(rv, &x))

	sync := x[base.SyncXattrName].(map[string]interface{})
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	cm := make(channels.ChannelMap)
	cm["A"] = &channels.ChannelRemoval{Seq: 2, Rev: channels.RevAndVersion{RevTreeID: "2-e99405a23fa102238fa8c3fd499b15bc"}}
	sync["channels"] = cm

	history := sync["history"].(map[string]interface{})
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("B"), base.SetOf("B")}

	// Marshall back to JSON
	b, err := base.JSONMarshal(x)
	require.NoError(t, err)

	// Update raw document in the bucket
	assert.NoError(t, collection.dataStore.SetRaw("alpha", 0, nil, b))

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

	require.NoError(t, collection.WaitForPendingChanges(base.TestCtx(t)))

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
	rv, _, _ := collection.dataStore.GetRaw("alpha") // cas, err

	// Unmarshall into nested maps
	var x map[string]interface{}
	assert.NoError(t, base.JSONUnmarshal(rv, &x))

	sync := x[base.SyncXattrName].(map[string]interface{})
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	history := sync["history"].(map[string]interface{})
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("A", "B"), base.SetOf("A", "B")}

	// Marshall back to JSON
	b, err := base.JSONMarshal(x)
	require.NoError(t, err)

	// Update raw document in the bucket
	require.NoError(t, collection.dataStore.SetRaw("alpha", 0, nil, b))

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

	waitErr := collection.WaitForPendingChanges(base.TestCtx(t))
	assert.NoError(t, waitErr)

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

// Benchmark to validate fix for https://github.com/couchbase/sync_gateway/issues/2428
func BenchmarkChangesFeedDocUnmarshalling(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelWarn, base.KeyHTTP)

	db, ctx := setupTestDB(b)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, b, db)

	fieldVal := func(valSizeBytes int) string {
		buffer := bytes.Buffer{}
		for i := 0; i < valSizeBytes; i++ {
			buffer.WriteString("a")
		}
		return buffer.String()
	}

	createDoc := func(numKeys, valSizeBytes int) Body {
		doc := Body{}
		for keyNum := 0; keyNum < numKeys; keyNum++ {
			doc[fmt.Sprintf("%v", keyNum)] = fieldVal(valSizeBytes)
		}
		return doc
	}

	numDocs := 400
	numKeys := 200
	valSizeBytes := 1024

	// Create 2k docs of size 50k, 1000 keys with branches, 1 parent + 2 child branches -- doesn't matter which API .. bucket api
	for docNum := 0; docNum < numDocs; docNum++ {

		// Create the parent rev
		docid, err := base.GenerateRandomID()
		require.NoError(b, err)
		docBody := createDoc(numKeys, valSizeBytes)
		revId, _, err := collection.Put(ctx, docid, docBody)
		if err != nil {
			b.Fatalf("Error creating doc: %v", err)
		}

		// Create child rev 1
		docBody["child"] = "A"
		_, _, err = collection.PutExistingRevWithBody(ctx, docid, docBody, []string{"2-A", revId}, false, ExistingVersionWithUpdateToHLV)
		if err != nil {
			b.Fatalf("Error creating child1 rev: %v", err)
		}

		// Create child rev 2
		docBody["child"] = "B"
		_, _, err = collection.PutExistingRevWithBody(ctx, docid, docBody, []string{"2-B", revId}, false, ExistingVersionWithUpdateToHLV)
		if err != nil {
			b.Fatalf("Error creating child2 rev: %v", err)
		}

	}

	// Start changes feed
	var options ChangesOptions
	options.Conflicts = true  // style=all_docs
	options.ActiveOnly = true // active_only=true
	options.Since = SequenceID{Seq: 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		// Changes params: POST /pm/_changes?feed=normal&heartbeat=30000&style=all_docs&active_only=true
		// Changes request of all docs (could also do GetDoc call, but misses other possible things). One shot, .. etc

		changesCtx, changesCtxCancel := context.WithCancel(base.TestCtx(b))
		options.ChangesCtx = changesCtx
		feed, err := collection.MultiChangesFeed(ctx, base.SetOf("*"), options)
		if err != nil {
			b.Fatalf("Error getting changes feed: %v", err)
		}
		for changeEntry := range feed {
			// log.Printf("changeEntry: %v", changeEntry)
			if changeEntry == nil {
				break
			}
		}
		changesCtxCancel()

	}

}

func TestChangesOptionsStringer(t *testing.T) {
	opts := ChangesOptions{}
	var stringerFields []string
	for _, key := range strings.Split(opts.String()[1:len(opts.String())-1], ",") {
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
	err = collection.WaitForPendingChanges(base.TestCtx(t))
	require.NoError(t, err)

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

	require.NoError(t, collection.WaitForPendingChanges(ctx))

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

// stubSingleChannelCache is a minimal SingleChannelCache test double that serves entries from a
// fixed, sequence-ordered list, honoring Since and Limit the way a real channel cache would. This
// lets tests drive changesFeed's own pagination bookkeeping directly, without needing real
// documents, DCP, or a backing query/view implementation, while still having changesFeed's actual
// Since/Limit choices (not a pre-scripted call count) determine what gets returned. Only GetChanges
// and ChannelID are exercised by changesFeed; the remaining methods are unused stubs to satisfy the
// interface.
type stubSingleChannelCache struct {
	channelID channels.ID
	entries   []*LogEntry // full ordered set of entries in the channel, by sequence
	calls     int
}

func (s *stubSingleChannelCache) GetChanges(_ context.Context, options ChangesOptions) ([]*LogEntry, error) {
	s.calls++
	var result []*LogEntry
	for _, entry := range s.entries {
		if entry.Sequence <= options.Since.Seq {
			continue
		}
		result = append(result, entry)
		if options.Limit > 0 && len(result) >= options.Limit {
			break
		}
	}
	return result, nil
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

// TestChangesFeedActiveOnlyContinuesPastInactiveBatch verifies that ActiveOnly feeds correctly page
// past inactive/deleted entries and don't prematurely terminate when a user-requested limit is specified.
// For ActiveOnly feeds, changesFeed must query using the database query pagination limit (ChannelQueryLimit)
// rather than the user-requested limit, since the number of raw entries and active entries differ.
// The test ensures changesFeed continues to iterate over the result set to retrieve all active entries
// even when an inactive page precedes them.
func TestChangesFeedActiveOnlyContinuesPastInactiveBatch(t *testing.T) {
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelQueryLimit = 3
	ctx, _, collection := setupDBWithChannelCacheSettings(t, cacheOptions)
	collectionID := collection.GetCollectionID()
	channelID := channels.NewID("active", collectionID)

	stub := &stubSingleChannelCache{
		channelID: channelID,
		entries: []*LogEntry{
			{DocID: "removed1", RevID: "1-a", Sequence: 1, Flags: channels.Removed, CollectionID: collectionID},
			{DocID: "removed2", RevID: "1-a", Sequence: 2, Flags: channels.Removed, CollectionID: collectionID},
			{DocID: "removed3", RevID: "1-a", Sequence: 3, Flags: channels.Removed, CollectionID: collectionID},
			{DocID: "active1", RevID: "1-a", Sequence: 4, CollectionID: collectionID},
			{DocID: "active2", RevID: "1-a", Sequence: 5, CollectionID: collectionID},
		},
	}

	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      1,
		ChangesCtx: base.TestCtx(t),
	}

	received := drainChangesFeed(t, collection.changesFeed(ctx, stub, options, "test"))

	// changesFeed forwards every entry it sees, active or not - ActiveOnly filtering happens
	// upstream in SimpleMultiChangesFeed. What matters here is that all 5 entries were retrieved,
	// including active2, which the buggy version dropped.
	require.Len(t, received, 5)
	assert.Equal(t, "removed1", received[0].ID)
	assert.Equal(t, "removed2", received[1].ID)
	assert.Equal(t, "removed3", received[2].ID)
	assert.Equal(t, "active1", received[3].ID)
	assert.Equal(t, "active2", received[4].ID)
	assert.Equal(t, 2, stub.calls, "changesFeed should page by ChannelQueryLimit, not the much smaller requested Limit, for ActiveOnly feeds")
}

// TestChangesFeedActiveOnlyMultipleInactiveBatches is the same shape as
// TestChangesFeedActiveOnlyContinuesPastInactiveBatch but spans three all-inactive, full-page batches
// before four active entries appear - more than the requested Limit of 2. The buggy version stopped
// as soon as it had sent Limit-many active entries, so it would give up after active2 and never see
// active3 or active4, even though the channel has more data.
func TestChangesFeedActiveOnlyMultipleInactiveBatches(t *testing.T) {
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelQueryLimit = 3
	ctx, _, collection := setupDBWithChannelCacheSettings(t, cacheOptions)
	collectionID := collection.GetCollectionID()
	channelID := channels.NewID("active", collectionID)

	var entries []*LogEntry
	for seq := uint64(1); seq <= 9; seq++ {
		entries = append(entries, &LogEntry{DocID: fmt.Sprintf("removed%d", seq), RevID: "1-a", Sequence: seq, Flags: channels.Removed, CollectionID: collectionID})
	}
	for i, seq := 1, uint64(10); seq <= 13; i, seq = i+1, seq+1 {
		entries = append(entries, &LogEntry{DocID: fmt.Sprintf("active%d", i), RevID: "1-a", Sequence: seq, CollectionID: collectionID})
	}

	stub := &stubSingleChannelCache{
		channelID: channelID,
		entries:   entries,
	}

	options := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
		Limit:      2,
		ChangesCtx: base.TestCtx(t),
	}

	received := drainChangesFeed(t, collection.changesFeed(ctx, stub, options, "test"))

	require.Len(t, received, 13)
	assert.Equal(t, "active1", received[9].ID)
	assert.Equal(t, "active2", received[10].ID)
	assert.Equal(t, "active3", received[11].ID)
	assert.Equal(t, "active4", received[12].ID)
	assert.Equal(t, 5, stub.calls, "changesFeed should have paged through all three inactive batches and kept going to find all four active entries")
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
		entries: []*LogEntry{
			{DocID: "removed1", RevID: "1-a", Sequence: 1, Flags: channels.Removed, CollectionID: collectionID},
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

// docMutation names one write-order step in a single-channel feed: whether the doc written at that
// step is left active in the channel, or removed from it (shows up with the Removed flag). A []docMutation
// list documents a test case's write sequence directly, without needing to decode a pattern string.
type docMutation int

const (
	// iota+1 so the zero value is never a valid mutation - an unset docMutation fails loudly
	// (removedFlagsFromMutations) instead of silently behaving like docActive.
	docActive docMutation = iota + 1
	docRemoved
)

func (m docMutation) String() string {
	switch m {
	case docActive:
		return "docActive"
	case docRemoved:
		return "docRemoved"
	default:
		return fmt.Sprintf("docMutation(%d)", int(m))
	}
}

// channelFeedEntry is a compact description of one document to materialize in a target channel's feed.
// Lets stub-style scenarios run against a real channel cache instead.
type channelFeedEntry struct {
	docID   string
	removed bool
}

// seedChannelFeed writes entries into targetChannel in order, moving removed entries into otherChannel
// so they show up as removals. Writes happen before the cache is primed, so a since=0 request backfills
// through real query pagination rather than an already-warm cache.
func seedChannelFeed(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, targetChannel, otherChannel string, entries []channelFeedEntry) {
	for _, e := range entries {
		if e.removed {
			revID, _, err := collection.Put(ctx, e.docID, Body{"channels": targetChannel})
			require.NoError(t, err)
			_, _, err = collection.Put(ctx, e.docID, Body{"channels": otherChannel, "_rev": revID})
			require.NoError(t, err)
		} else {
			_, _, err := collection.Put(ctx, e.docID, Body{"channels": targetChannel})
			require.NoError(t, err)
		}
	}
}

// removedFlagsFromMutations converts a []docMutation into a per-index removed slice.
func removedFlagsFromMutations(t testing.TB, mutations []docMutation) []bool {
	removed := make([]bool, len(mutations))
	for i, m := range mutations {
		switch m {
		case docActive:
			removed[i] = false
		case docRemoved:
			removed[i] = true
		default:
			t.Fatalf("unknown %v at index %d", m, i)
		}
	}
	return removed
}

// expectedActiveOnlyDocIDs derives the expected result ("doc<i+1>" per write-order entry) from the same
// removed/write-order data used to seed the feed, so the assertion can't drift from the seed: drop
// removed docs when activeOnly, then cap at limit.
func expectedActiveOnlyDocIDs(removed []bool, activeOnly bool, limit int) []string {
	var expected []string
	for i, r := range removed {
		if activeOnly && r {
			continue
		}
		expected = append(expected, fmt.Sprintf("doc%d", i+1))
	}
	if limit > 0 && len(expected) > limit {
		expected = expected[:limit]
	}
	return expected
}

// TestChangesQueryLimitBoundaries is TestActiveOnlyWithLimit run against a real database/collection
// changes feed, verifying that database query pagination limit (ChannelQueryLimit) boundary combinations
// correctly return active entries without premature termination.
//
// It ensures that inactive/deleted entries are not counted toward the user-requested limit, preventing
// the feed from stopping early.
//
// Note: This end-to-end test does not verify that we avoid shrinking the database query pagination limit
// for ActiveOnly feeds; if it were shrunk, the queries would still return correct results but with more
// round trips. The stub-based tests instead verify that we query with the full ChannelQueryLimit.
func TestChangesQueryLimitBoundaries(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	const targetChannel = "target"
	const otherChannel = "other"

	testCases := []struct {
		name         string
		queryLimit   int  // CacheOptions.ChannelQueryLimit (database query pagination limit)
		requestLimit int  // ChangesOptions.Limit (user-requested limit, 0 == no limit)
		activeOnly   bool // ChangesOptions.ActiveOnly
		mutations    []docMutation
	}{
		// --- ActiveOnly=false: every entry (active + removal) is returned, capped by requestLimit ---
		{ // active run spans batches; requestLimit stops mid-second-batch (pagination limit 3, request 4)
			name: "false/active_across_batches_request_lt_total", queryLimit: 3, requestLimit: 4, activeOnly: false,
			mutations: []docMutation{docActive, docActive, docActive, docActive, docActive, docActive},
		},
		{ // requestLimit > queryLimit and > total: full result, having paged through several batches
			name: "false/request_gt_total_multi_batch", queryLimit: 3, requestLimit: 100, activeOnly: false,
			mutations: []docMutation{docActive, docRemoved, docActive, docRemoved, docActive, docRemoved, docActive, docRemoved},
		},
		{ // requestLimit == queryLimit, more data than one batch: must page past the first batch
			name: "false/request_eq_query_limit", queryLimit: 3, requestLimit: 3, activeOnly: false,
			mutations: []docMutation{docActive, docActive, docActive, docActive, docActive},
		},
		{ // no requestLimit: drain everything across many small batches
			name: "false/no_limit_many_batches", queryLimit: 2, requestLimit: 0, activeOnly: false,
			mutations: []docMutation{docActive, docRemoved, docActive, docRemoved, docActive, docRemoved, docActive, docRemoved},
		},
		{ // total is an exact multiple of the query pagination limit (boundary lands exactly at end of data)
			name: "false/exact_multiple_of_query_limit", queryLimit: 3, requestLimit: 0, activeOnly: false,
			mutations: []docMutation{docActive, docActive, docActive, docActive, docActive, docActive},
		},

		// --- ActiveOnly=true: removals are filtered out; changesFeed must keep paging past
		//     all-inactive batches to reach the requested number of active entries ---
		{ // a full leading batch of removals (zero active), then active entries - request < active available
			name: "true/removals_fill_first_batch", queryLimit: 3, requestLimit: 3, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docRemoved, docActive, docActive, docActive, docActive},
		},
		{ // active entries straddle a query-batch boundary; requestLimit lands mid-run
			name: "true/active_straddles_batch_boundary", queryLimit: 3, requestLimit: 3, activeOnly: true,
			mutations: []docMutation{docActive, docActive, docRemoved, docRemoved, docActive, docActive, docActive},
		},
		{ // multiple all-removal batches before any active entry (like the stub multi-inactive-batch test)
			name: "true/multiple_removal_batches", queryLimit: 3, requestLimit: 2, activeOnly: true,
			mutations: []docMutation{
				docRemoved, docRemoved, docRemoved, docRemoved, docRemoved, docRemoved, docRemoved,
				docActive, docActive, docActive, docActive,
			},
		},
		{ // requestLimit > queryLimit: active limit exceeds a single query page
			name: "true/request_gt_query_limit", queryLimit: 2, requestLimit: 5, activeOnly: true,
			mutations: []docMutation{
				docActive, docRemoved, docActive, docRemoved, docActive, docRemoved,
				docActive, docRemoved, docActive, docRemoved, docActive, docRemoved,
			},
		},
		{ // requestLimit < queryLimit: single page satisfies the request, no extra paging needed
			name: "true/request_lt_query_limit", queryLimit: 5, requestLimit: 2, activeOnly: true,
			mutations: []docMutation{docActive, docActive, docActive, docActive, docActive, docActive},
		},
		{ // no requestLimit: every active entry must be returned regardless of interleaved removals
			name: "true/no_limit_interleaved", queryLimit: 3, requestLimit: 0, activeOnly: true,
			mutations: []docMutation{
				docRemoved, docActive, docRemoved, docActive, docRemoved, docActive,
				docRemoved, docActive, docRemoved, docActive, docRemoved,
			},
		},
		{ // channel exhausted before requestLimit is met: return the few active entries and stop
			name: "true/exhausted_before_limit", queryLimit: 3, requestLimit: 10, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docActive, docActive, docRemoved, docRemoved, docActive},
		},
		{ // all removals: active-only feed is empty even though the channel has entries to page through
			name: "true/all_removed_empty", queryLimit: 2, requestLimit: 5, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docRemoved, docRemoved, docRemoved},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cacheOptions := DefaultCacheOptions()
			cacheOptions.ChannelQueryLimit = tc.queryLimit
			ctx, _, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

			removed := removedFlagsFromMutations(t, tc.mutations)
			entries := make([]channelFeedEntry, 0, len(removed))
			for i, r := range removed {
				entries = append(entries, channelFeedEntry{docID: fmt.Sprintf("doc%d", i+1), removed: r})
			}
			seedChannelFeed(t, ctx, collection, targetChannel, otherChannel, entries)
			require.NoError(t, collection.WaitForPendingChanges(ctx))

			changesOptions := ChangesOptions{
				Since:      SequenceID{Seq: 0},
				ActiveOnly: tc.activeOnly,
				Limit:      tc.requestLimit,
				ChangesCtx: base.TestCtx(t),
			}
			changes := getChanges(t, collection, base.SetOf(targetChannel), changesOptions)

			expected := expectedActiveOnlyDocIDs(removed, tc.activeOnly, tc.requestLimit)

			require.Len(t, changes, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp, changes[i].ID, "unexpected doc at feed index %d", i)
				if tc.activeOnly {
					assert.Empty(t, changes[i].Removed, "active-only feed should not contain removals (index %d)", i)
				}
			}
		})
	}
}

// TestChangesQueryCacheConcatenationBoundaries is the counterpart to TestChangesQueryLimitBoundaries,
// verifying correct merging and deduplication of query results (pruned prefix) and cache results (retained suffix)
// at the cache validFrom boundary.
//
// Like TestChangesQueryLimitBoundaries, this end-to-end test does not directly verify the optimization
// that avoids shrinking the database query pagination limit (ChannelQueryLimit) during pagination. This is because
// any active entries omitted in an iteration would still be retrieved in a subsequent query iteration as changesFeed
// continues to iterate over the resultset using ChannelQueryLimit.
func TestChangesQueryCacheConcatenationBoundaries(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	const targetChannel = "target"
	const otherChannel = "other"

	testCases := []struct {
		name           string
		cacheMaxLength int  // CacheOptions.ChannelCacheMaxLength (query/cache split point: last N entries stay cached)
		queryLimit     int  // CacheOptions.ChannelQueryLimit (database query pagination limit within the pruned/query-only prefix)
		requestLimit   int  // ChangesOptions.Limit (user-requested limit, 0 == no limit)
		activeOnly     bool // ChangesOptions.ActiveOnly
		mutations      []docMutation
	}{
		{ // active run straddles the prune boundary: docs 1-3 pruned to query-only, docs 4-6 stay cached
			name: "active_run_straddles_prune_boundary", cacheMaxLength: 3, queryLimit: 2, requestLimit: 4, activeOnly: true,
			mutations: []docMutation{docActive, docActive, docActive, docActive, docActive, docActive},
		},
		{ // pruned (query-only) prefix is all removals; every active entry lives in the cached suffix
			name: "removals_pruned_actives_cached", cacheMaxLength: 3, queryLimit: 2, requestLimit: 0, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docRemoved, docActive, docActive, docActive},
		},
		{ // same as removals_pruned_actives_cached but with a non-zero requestLimit (the original failure case)
			name: "removals_pruned_actives_cached_with_limit", cacheMaxLength: 3, queryLimit: 2, requestLimit: 2, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docRemoved, docActive, docActive, docActive},
		},
		{ // requestLimit fully satisfied within the pruned prefix; cache boundary is never reached
			name: "request_satisfied_before_prune_boundary", cacheMaxLength: 2, queryLimit: 2, requestLimit: 2, activeOnly: true,
			mutations: []docMutation{docActive, docActive, docActive, docActive, docActive, docActive},
		},
		{ // pruned prefix needs multiple query pages (queryLimit small relative to prefix) before the
			// cache boundary is reached and the cached suffix gets appended
			name: "multi_page_query_then_cache_append", cacheMaxLength: 2, queryLimit: 2, requestLimit: 0, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docRemoved, docRemoved, docRemoved, docActive, docActive},
		},
		{ // same as multi_page_query_then_cache_append but with a non-zero requestLimit
			name: "multi_page_query_then_cache_append_with_limit", cacheMaxLength: 2, queryLimit: 2, requestLimit: 1, activeOnly: true,
			mutations: []docMutation{docRemoved, docRemoved, docRemoved, docRemoved, docRemoved, docActive, docActive},
		},
		{ // no requestLimit: every active entry must be returned across the query/cache split with no
			// duplication of the overlap entry at the cache's validFrom boundary
			name: "no_limit_full_span", cacheMaxLength: 3, queryLimit: 3, requestLimit: 0, activeOnly: true,
			mutations: []docMutation{
				docActive, docRemoved, docActive, docRemoved, docActive,
				docRemoved, docActive, docRemoved, docActive, docRemoved,
			},
		},
		{ // ActiveOnly=false: plain concatenation across a prune boundary that doesn't align with the
			// query pagination limit, to check for off-by-one duplication/loss at the query/cache seam
			name: "activeOnly_false_misaligned_boundary", cacheMaxLength: 4, queryLimit: 3, requestLimit: 0, activeOnly: false,
			mutations: []docMutation{
				docActive, docRemoved, docActive, docRemoved, docActive,
				docRemoved, docActive, docRemoved, docActive, docRemoved,
			},
		},
		{ // cache boundary and query pagination limit coincide exactly (aligned case)
			name: "aligned_boundary", cacheMaxLength: 3, queryLimit: 3, requestLimit: 0, activeOnly: true,
			mutations: []docMutation{docActive, docActive, docActive, docRemoved, docRemoved, docRemoved, docActive, docActive, docActive},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cacheOptions := DefaultCacheOptions()
			cacheOptions.ChannelCacheMaxLength = tc.cacheMaxLength
			cacheOptions.ChannelQueryLimit = tc.queryLimit
			ctx, _, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

			// Prime the cache before writing so live writes populate it directly (and get pruned live
			// once ChannelCacheMaxLength is exceeded), rather than requiring a query to backfill it.
			primingOptions := ChangesOptions{Since: SequenceID{Seq: 0}, ChangesCtx: base.TestCtx(t)}
			_ = getChanges(t, collection, base.SetOf(targetChannel), primingOptions)

			removed := removedFlagsFromMutations(t, tc.mutations)
			entries := make([]channelFeedEntry, 0, len(removed))
			for i, r := range removed {
				entries = append(entries, channelFeedEntry{docID: fmt.Sprintf("doc%d", i+1), removed: r})
			}
			seedChannelFeed(t, ctx, collection, targetChannel, otherChannel, entries)
			require.NoError(t, collection.WaitForPendingChanges(ctx))

			changesOptions := ChangesOptions{
				Since:      SequenceID{Seq: 0},
				ActiveOnly: tc.activeOnly,
				Limit:      tc.requestLimit,
				ChangesCtx: base.TestCtx(t),
			}
			changes := getChanges(t, collection, base.SetOf(targetChannel), changesOptions)

			expected := expectedActiveOnlyDocIDs(removed, tc.activeOnly, tc.requestLimit)

			require.Len(t, changes, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp, changes[i].ID, "unexpected doc at feed index %d", i)
				if tc.activeOnly {
					assert.Empty(t, changes[i].Removed, "active-only feed should not contain removals (index %d)", i)
				}
			}
		})
	}
}

// multiChannelFeedEntry seeds one doc across one or more channels at once. removedFrom is the subset
// of channels it's later removed from: if it covers every channel, the doc is fully inactive; if it's a
// strict subset, the doc stays active via the remaining channel(s) - see fullyRemoved.
type multiChannelFeedEntry struct {
	channels    []string
	removedFrom []string
}

// fullyRemoved reports whether the doc was removed from every channel it's in.
func (e multiChannelFeedEntry) fullyRemoved() bool {
	return len(e.removedFrom) > 0 && len(e.removedFrom) == len(e.channels)
}

// seedMultiChannelFeed is seedChannelFeed generalized to multiple (and shared) channels: entries are
// written in slice order, so write order equals global sequence order. A fully-removed doc moves to
// otherChannel; a partially-removed doc just drops the removedFrom channels, staying live elsewhere.
func seedMultiChannelFeed(t *testing.T, ctx context.Context, collection *DatabaseCollectionWithUser, otherChannel string, entries []multiChannelFeedEntry) {
	for i, e := range entries {
		docID := fmt.Sprintf("doc%d", i+1)
		revID, _, err := collection.Put(ctx, docID, Body{"channels": e.channels})
		require.NoError(t, err)
		if len(e.removedFrom) == 0 {
			continue
		}
		removedSet := make(map[string]bool, len(e.removedFrom))
		for _, c := range e.removedFrom {
			removedSet[c] = true
		}
		var newChannels []string
		for _, c := range e.channels {
			if !removedSet[c] {
				newChannels = append(newChannels, c)
			}
		}
		if len(newChannels) == 0 {
			// Fully removed: move to a channel outside the requested set rather than leaving the doc
			// with an empty channel list, mirroring seedChannelFeed's single-channel convention.
			newChannels = []string{otherChannel}
		}
		_, _, err = collection.Put(ctx, docID, Body{"channels": newChannels, "_rev": revID})
		require.NoError(t, err)
	}
}

// multiChannelMutation names one write-order step across two channels (ch1/ch2 in this test): which
// channel(s) a doc is written to, and whether/how it's later removed. The "both" values cover a single
// doc shared by both channels at once; bothRemoveCh1Only/bothRemoveCh2Only remove it from just one of
// the two, leaving it active via the other.
type multiChannelMutation int

const (
	// iota+1 so the zero value is never a valid mutation - an unset multiChannelMutation fails loudly
	// (entriesFromMultiChannelMutations) instead of silently being dropped from the seed.
	ch1Active         multiChannelMutation = iota + 1 // active in ch1 only
	ch1Removed                                        // removed from ch1 (was ch1-only)
	ch2Active                                         // active in ch2 only
	ch2Removed                                        // removed from ch2 (was ch2-only)
	bothActive                                        // written to both channels, stays active in both
	bothRemoved                                       // removed from both channels at once (fully inactive)
	bothRemoveCh1Only                                 // written to both, removed from ch1 only
	bothRemoveCh2Only                                 // written to both, removed from ch2 only
)

func (m multiChannelMutation) String() string {
	switch m {
	case ch1Active:
		return "ch1Active"
	case ch1Removed:
		return "ch1Removed"
	case ch2Active:
		return "ch2Active"
	case ch2Removed:
		return "ch2Removed"
	case bothActive:
		return "bothActive"
	case bothRemoved:
		return "bothRemoved"
	case bothRemoveCh1Only:
		return "bothRemoveCh1Only"
	case bothRemoveCh2Only:
		return "bothRemoveCh2Only"
	default:
		return fmt.Sprintf("multiChannelMutation(%d)", int(m))
	}
}

// entriesFromMultiChannelMutations converts a []multiChannelMutation into multiChannelFeedEntry values.
func entriesFromMultiChannelMutations(t testing.TB, mutations []multiChannelMutation, ch1, ch2 string) []multiChannelFeedEntry {
	entries := make([]multiChannelFeedEntry, len(mutations))
	for i, m := range mutations {
		switch m {
		case ch1Active:
			entries[i] = multiChannelFeedEntry{channels: []string{ch1}}
		case ch1Removed:
			entries[i] = multiChannelFeedEntry{channels: []string{ch1}, removedFrom: []string{ch1}}
		case ch2Active:
			entries[i] = multiChannelFeedEntry{channels: []string{ch2}}
		case ch2Removed:
			entries[i] = multiChannelFeedEntry{channels: []string{ch2}, removedFrom: []string{ch2}}
		case bothActive:
			entries[i] = multiChannelFeedEntry{channels: []string{ch1, ch2}}
		case bothRemoved:
			entries[i] = multiChannelFeedEntry{channels: []string{ch1, ch2}, removedFrom: []string{ch1, ch2}}
		case bothRemoveCh1Only:
			entries[i] = multiChannelFeedEntry{channels: []string{ch1, ch2}, removedFrom: []string{ch1}}
		case bothRemoveCh2Only:
			entries[i] = multiChannelFeedEntry{channels: []string{ch1, ch2}, removedFrom: []string{ch2}}
		default:
			t.Fatalf("unknown %v at index %d", m, i)
		}
	}
	return entries
}

// TestChangesMultiChannelActiveOnlyLimit exercises MultiChangesFeed's cross-channel merge with
// ActiveOnly+Limit across two real channels, verifying correct global write order and deduplication of
// multi-channel documents.
//
// It ensures that inactive/deleted entries on one channel do not incorrectly starve or block active
// entries on another channel, and that we correctly page past inactive entries across channel merges.
func TestChangesMultiChannelActiveOnlyLimit(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	const ch1 = "ch1"
	const ch2 = "ch2"
	const otherChannel = "other"

	testCases := []struct {
		name         string
		queryLimit   int
		requestLimit int
		activeOnly   bool
		mutations    []multiChannelMutation
	}{
		{ // simple alternation between channels, all active: verifies merge preserves global write order
			name: "interleaved_actives_request_lt_total", queryLimit: 2, requestLimit: 3, activeOnly: true,
			mutations: []multiChannelMutation{ch1Active, ch2Active, ch1Active, ch2Active, ch1Active, ch2Active},
		},
		{ // ch1 has a long removal run while ch2 keeps producing actives: ch1's inactive backlog must
			// not starve or misorder ch2's actives in the merged, globally-ordered output
			name: "one_channel_removed_run_other_active", queryLimit: 2, requestLimit: 3, activeOnly: true,
			mutations: []multiChannelMutation{
				ch1Removed, ch1Removed, ch1Removed, ch2Active, ch1Removed, ch2Active, ch2Active, ch1Active,
			},
		},
		{ // both channels have enough entries to require multiple internal query batches each
			// (queryLimit=2); no requestLimit, so the full merged result must be complete and ordered
			name: "both_channels_paginate_independently", queryLimit: 2, requestLimit: 0, activeOnly: true,
			mutations: []multiChannelMutation{
				ch1Active, ch1Removed, ch2Active, ch2Removed, ch1Active,
				ch1Removed, ch2Active, ch2Removed, ch1Active, ch2Active,
			},
		},
		{ // requestLimit lands exactly between two actives contributed by different channels
			name: "limit_lands_at_cross_channel_boundary", queryLimit: 3, requestLimit: 2, activeOnly: true,
			mutations: []multiChannelMutation{ch1Active, ch2Removed, ch2Removed, ch2Removed, ch1Active, ch2Active},
		},
		{ // ActiveOnly=false across channels: plain merge/limit behavior with removals present
			name: "activeOnly_false_multichannel", queryLimit: 2, requestLimit: 5, activeOnly: false,
			mutations: []multiChannelMutation{
				ch1Active, ch2Removed, ch1Removed, ch2Active, ch1Active, ch2Removed, ch2Active, ch1Removed,
			},
		},
		{ // ch1 exhausts after a single entry while ch2 continues; the merge loop must keep draining
			// ch2 correctly after ch1's changesFeed goroutine has already finished
			name: "channel_exhausted_early_other_continues", queryLimit: 2, requestLimit: 4, activeOnly: true,
			mutations: []multiChannelMutation{
				ch1Active, ch2Active, ch2Removed, ch2Active, ch2Removed, ch2Active, ch2Removed, ch2Active,
			},
		},

		// --- shared docs: a single doc written to both channels at once, exercising MultiChangesFeed's
		//     per-sequence allRemoved dedup across channel feeds ---
		{ // doc removed from only one of its two channels stays active via the other; must still be
			// returned under ActiveOnly, not dropped as if fully removed
			name: "partial_removal_stays_active", queryLimit: 2, requestLimit: 0, activeOnly: true,
			mutations: []multiChannelMutation{bothRemoveCh1Only, ch1Active, ch2Active},
		},
		{ // same, removing the *other* channel first, to rule out any asymmetry in which channel-feed
			// slot the allRemoved dedup happens to inspect first
			name: "partial_removal_other_direction_stays_active", queryLimit: 2, requestLimit: 0, activeOnly: true,
			mutations: []multiChannelMutation{bothRemoveCh2Only, ch1Active, ch2Active},
		},
		{ // doc removed from both of its channels (in the same write) is fully inactive, unlike the
			// partial-removal cases above - the dedup must correctly distinguish the two
			name: "full_removal_of_shared_doc_is_inactive", queryLimit: 2, requestLimit: 0, activeOnly: true,
			mutations: []multiChannelMutation{bothRemoved, ch1Active, ch2Active},
		},
		{ // a partially-removed shared doc mixed with an ordinary single-channel removal run and a
			// requestLimit landing after both, forced through multi-batch pagination on each channel
			name: "partial_removal_mixed_with_limit", queryLimit: 2, requestLimit: 3, activeOnly: true,
			mutations: []multiChannelMutation{
				ch1Removed, ch1Removed, bothRemoveCh1Only, ch2Active, ch1Active, ch2Removed, bothRemoveCh2Only, ch2Active,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cacheOptions := DefaultCacheOptions()
			cacheOptions.ChannelQueryLimit = tc.queryLimit
			ctx, _, collection := setupDBWithChannelCacheSettings(t, cacheOptions)

			entries := entriesFromMultiChannelMutations(t, tc.mutations, ch1, ch2)
			seedMultiChannelFeed(t, ctx, collection, otherChannel, entries)
			require.NoError(t, collection.WaitForPendingChanges(ctx))

			changesOptions := ChangesOptions{
				Since:      SequenceID{Seq: 0},
				ActiveOnly: tc.activeOnly,
				Limit:      tc.requestLimit,
				ChangesCtx: base.TestCtx(t),
			}
			changes := getChanges(t, collection, base.SetOf(ch1, ch2), changesOptions)

			removed := make([]bool, len(entries))
			entriesByDocID := make(map[string]multiChannelFeedEntry, len(entries))
			for i, e := range entries {
				removed[i] = e.fullyRemoved()
				entriesByDocID[fmt.Sprintf("doc%d", i+1)] = e
			}
			expected := expectedActiveOnlyDocIDs(removed, tc.activeOnly, tc.requestLimit)

			require.Len(t, changes, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp, changes[i].ID, "unexpected doc at feed index %d", i)
				if !tc.activeOnly {
					continue
				}
				// A doc partially removed (still active via another channel) legitimately carries a
				// non-empty Removed set recording which channel(s) it left, even though it wasn't
				// filtered out - so check it matches removedFrom exactly, rather than requiring empty.
				entry := entriesByDocID[exp]
				if len(entry.removedFrom) == 0 {
					assert.Empty(t, changes[i].Removed, "expected no partial removals for %s (index %d)", exp, i)
				} else {
					assert.Equal(t, base.SetOf(entry.removedFrom...), changes[i].Removed, "unexpected partial removal set for %s (index %d)", exp, i)
				}
			}
		})
	}
}
