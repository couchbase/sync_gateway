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
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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
			db := setupTestDB(t)
			defer db.Close()

			auth := db.Authenticator(base.TestCtx(t))
			user, err := auth.NewUser("test", "pass", testCase.userChans)
			require.NoError(t, err)
			require.NoError(t, auth.Save(user))

			for i := 0; i < testCase.genChanAndDocs; i++ {
				id := fmt.Sprintf("%d", i+1)
				_, _, err = db.Put("doc"+id, Body{"channels": []string{"ch" + id}})
				require.NoError(t, err)
			}
			err = db.WaitForPendingChanges(base.TestCtx(t))
			require.NoError(t, err)

			db.user, err = auth.GetUser("test")
			require.NoError(t, err)

			ch, err := db.GetChanges(testCase.accessChans, getZeroSequence())
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

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	db := setupTestDB(t)
	defer db.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(base.TestCtx(t))
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf(t, "ABC"))
	require.NoError(t, authenticator.Save(user))

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create a doc on two channels (sequence 1):
	revid, _, err := db.Put("doc1", Body{"channels": []string{"ABC", "PBS"}})
	require.NoError(t, err)
	cacheWaiter.AddAndWait(1)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipalForTest(t, "naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(base.TestCtx(t), *userInfo, true, true)
	assert.NoError(t, err, "UpdatePrincipal failed")

	err = db.WaitForPendingChanges(base.TestCtx(t))
	assert.NoError(t, err)

	// Check the _changes feed:
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence())
	assert.NoError(t, err, "Couldn't GetChanges")
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
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Add a new doc (sequence 3):
	revid, _, err = db.Put("doc2", Body{"channels": []string{"PBS"}})
	require.NoError(t, err)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	cacheWaiter.AddAndWait(1)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assert.NoError(t, err, "Couldn't GetChanges (2nd)")

	require.Len(t, changes, 1)
	assert.Equal(t, "doc2", changes[0].ID)
	assert.Equal(t, []ChangeRev{{"rev": revid}}, changes[0].Changes)

	// validate from zero
	changes, err = db.GetChanges(base.SetOf("*"), getZeroSequence())
	assert.NoError(t, err, "Couldn't GetChanges")
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

func getZeroSequence() ChangesOptions {
	return ChangesOptions{Since: SequenceID{Seq: 0}}
}

func TestDocDeletionFromChannelCoalescedRemoved(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	if !base.UnitTestUrlIsWalrus() && base.TestUseXattrs() {
		t.Skip("This test is known to be failing against couchbase server with XATTRS enabled.  See https://gist.github.com/tleyden/a41632355fadde54f19e84ba68015512")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel A
	authenticator := db.Authenticator(base.TestCtx(t))
	user, _ := authenticator.NewUser("alice", "letmein", channels.SetOf(t, "A"))
	require.NoError(t, authenticator.Save(user))

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create a doc on two channels (sequence 1):
	revid, _, err := db.Put("alpha", Body{"channels": []string{"A", "B"}})
	require.NoError(t, err)
	cacheWaiter.AddAndWait(1)

	db.user, _ = authenticator.GetUser("alice")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence())
	assert.NoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equal(t, 1, len(changes))
	assert.Equal(t, &ChangeEntry{
		Seq:     SequenceID{Seq: 1},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": revid}}}, changes[0])

	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := db.Bucket.GetRaw("alpha") // cas, err

	// Unmarshall into nested maps
	var x map[string]interface{}
	assert.NoError(t, base.JSONUnmarshal(rv, &x))

	sync := x[base.SyncXattrName].(map[string]interface{})
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	cm := make(channels.ChannelMap)
	cm["A"] = &channels.ChannelRemoval{Seq: 2, RevID: "2-e99405a23fa102238fa8c3fd499b15bc"}
	sync["channels"] = cm

	history := sync["history"].(map[string]interface{})
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("B"), base.SetOf("B")}

	// Marshall back to JSON
	b, err := base.JSONMarshal(x)

	// Update raw document in the bucket
	assert.NoError(t, db.Bucket.SetRaw("alpha", 0, nil, b))

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 3 and isn't stuck waiting for it.
	cacheWaiter.AddAndWait(1)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assert.NoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equal(t, 1, len(changes))
	assert.Equal(t, &ChangeEntry{
		Seq:        SequenceID{Seq: 2},
		ID:         "alpha",
		Removed:    base.SetOf("A"),
		allRemoved: true,
		Changes:    []ChangeRev{{"rev": "2-e99405a23fa102238fa8c3fd499b15bc"}}}, changes[0])

	printChanges(changes)
}

func TestDocDeletionFromChannelCoalesced(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	if !base.UnitTestUrlIsWalrus() && base.TestUseXattrs() {
		t.Skip("This test is known to be failing against couchbase server with XATTRS enabled.  Same error as TestDocDeletionFromChannelCoalescedRemoved")
	}

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel A
	authenticator := db.Authenticator(base.TestCtx(t))
	user, _ := authenticator.NewUser("alice", "letmein", channels.SetOf(t, "A"))
	require.NoError(t, authenticator.Save(user))

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create a doc on two channels (sequence 1):
	revid, _, err := db.Put("alpha", Body{"channels": []string{"A", "B"}})
	require.NoError(t, err)
	cacheWaiter.AddAndWait(1)

	db.user, _ = authenticator.GetUser("alice")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence())
	assert.NoError(t, err, "Couldn't GetChanges")
	printChanges(changes)

	assert.Equal(t, 1, len(changes))
	assert.Equal(t, &ChangeEntry{
		Seq:     SequenceID{Seq: 1},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": revid}}}, changes[0])

	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := db.Bucket.GetRaw("alpha") // cas, err

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

	// Update raw document in the bucket
	require.NoError(t, db.Bucket.SetRaw("alpha", 0, nil, b))

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 3 (the modified document) and isn't stuck waiting for it.
	cacheWaiter.AddAndWait(1)

	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assert.NoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equal(t, 1, len(changes))
	assert.Equal(t, &ChangeEntry{
		Seq:     SequenceID{Seq: 3},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": "3-e99405a23fa102238fa8c3fd499b15bc"}}}, changes[0])

	printChanges(changes)
}

func TestActiveOnlyCacheUpdate(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)
	// Create 10 documents
	revId := ""
	var err error
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		body := Body{"foo": "bar"}
		revId, _, err = db.Put(key, body)
		require.NoError(t, err, "Couldn't create document")
	}

	// Tombstone 5 documents
	for i := 2; i <= 6; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, err = db.DeleteDoc(key, revId)
		require.NoError(t, err, "Couldn't delete document")
	}

	waitErr := db.WaitForPendingChanges(base.TestCtx(t))
	assert.NoError(t, waitErr)

	changesOptions := ChangesOptions{
		Since:      SequenceID{Seq: 0},
		ActiveOnly: true,
	}

	initQueryCount := db.DbStats.Cache().ViewQueries.Value()

	// Get changes with active_only=true
	activeChanges, err := db.GetChanges(base.SetOf("*"), changesOptions)
	require.NoError(t, err, "Error getting changes with active_only true")
	require.Equal(t, 5, len(activeChanges))

	// Ensure the test is triggering a query, and not serving from DCP-generated cache
	postChangesQueryCount := db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initQueryCount+1, postChangesQueryCount)

	// Get changes with active_only=false, validate that triggers a new query
	changesOptions.ActiveOnly = false
	allChanges, err := db.GetChanges(base.SetOf("*"), changesOptions)
	require.NoError(t, err, "Error getting changes with active_only true")
	require.Equal(t, 10, len(allChanges))

	postChangesQueryCount = db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initQueryCount+2, postChangesQueryCount)

	// Get changes with active_only=false again, verify results are served from the cache
	changesOptions.ActiveOnly = false
	allChanges, err = db.GetChanges(base.SetOf("*"), changesOptions)
	require.NoError(t, err, "Error getting changes with active_only true")
	require.Equal(t, 10, len(allChanges))

	postChangesQueryCount = db.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, initQueryCount+2, postChangesQueryCount)

}

// Benchmark to validate fix for https://github.com/couchbase/sync_gateway/issues/2428
func BenchmarkChangesFeedDocUnmarshalling(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelWarn, base.KeyHTTP)

	db := setupTestDB(b)
	defer db.Close()

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
		revId, _, err := db.Put(docid, docBody)
		if err != nil {
			b.Fatalf("Error creating doc: %v", err)
		}

		// Create child rev 1
		docBody["child"] = "A"
		_, _, err = db.PutExistingRevWithBody(docid, docBody, []string{"2-A", revId}, false)
		if err != nil {
			b.Fatalf("Error creating child1 rev: %v", err)
		}

		// Create child rev 2
		docBody["child"] = "B"
		_, _, err = db.PutExistingRevWithBody(docid, docBody, []string{"2-B", revId}, false)
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

		options.Terminator = make(chan bool)
		feed, err := db.MultiChangesFeed(base.SetOf("*"), options)
		if err != nil {
			b.Fatalf("Error getting changes feed: %v", err)
		}
		for changeEntry := range feed {
			// log.Printf("changeEntry: %v", changeEntry)
			if changeEntry == nil {
				break
			}
		}
		close(options.Terminator)

	}

}
