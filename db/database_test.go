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
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/robertkrimen/otto/underscore"
	"github.com/stretchr/testify/assert"
)

func init() {
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

// Note: It is important to call db.Close() on the returned database.
func setupTestDB(t testing.TB) (*Database, context.Context) {
	return setupTestDBWithCacheOptions(t, DefaultCacheOptions())
}

func setupTestDBForBucket(t testing.TB, bucket *base.TestBucket) (*Database, context.Context) {
	cacheOptions := DefaultCacheOptions()
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &cacheOptions,
	}
	return SetupTestDBForDataStoreWithOptions(t, bucket, dbcOptions)
}

func setupTestDBWithOptionsAndImport(t testing.TB, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	ctx := base.TestCtx(t)
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	if dbcOptions.GroupID == "" && base.IsEnterpriseEdition() {
		dbcOptions.GroupID = t.Name()
	}
	dbCtx, err := NewDatabaseContext(ctx, "db", base.GetTestBucket(t), true, dbcOptions)
	require.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(dbCtx)
	require.NoError(t, err, "Couldn't create database 'db'")
	ctx = db.AddDatabaseLogContext(ctx)
	return db, ctx
}

func setupTestDBWithCacheOptions(t testing.TB, options CacheOptions) (*Database, context.Context) {

	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	return SetupTestDBWithOptions(t, dbcOptions)
}

// Forces UseViews:true in the database context.  Useful for testing w/ views while running
// tests against Couchbase Server
func setupTestDBWithViewsEnabled(t testing.TB) (*Database, context.Context) {
	if !base.TestsDisableGSI() {
		t.Skip("GSI is not compatible with views")
	}
	dbcOptions := DatabaseContextOptions{
		UseViews: true,
	}
	return SetupTestDBWithOptions(t, dbcOptions)
}

// Sets up a test bucket with _sync:seq initialized to a high value prior to database creation.  Used to test
// issues with custom _sync:seq values without triggering skipped sequences between 0 and customSeq
func setupTestDBWithCustomSyncSeq(t testing.TB, customSeq uint64) (*Database, context.Context) {

	ctx := base.TestCtx(t)
	dbcOptions := DatabaseContextOptions{}
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	tBucket := base.GetTestBucket(t)

	// This may need to change when we move to a non-default metadata collection...
	metadataStore := tBucket.DefaultDataStore()

	log.Printf("Initializing test %s to %d", base.SyncSeqKey, customSeq)
	_, incrErr := metadataStore.Incr(base.SyncSeqKey, customSeq, customSeq, 0)
	assert.NoError(t, incrErr, fmt.Sprintf("Couldn't increment %s by %d", base.SyncSeqKey, customSeq))

	dbCtx, err := NewDatabaseContext(ctx, "db", tBucket, false, dbcOptions)
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(dbCtx)
	assert.NoError(t, err, "Couldn't create database 'db'")

	atomic.StoreUint32(&dbCtx.State, DBOnline)
	ctx = db.AddDatabaseLogContext(ctx)

	return db, ctx
}

func setupTestLeakyDBWithCacheOptions(t *testing.T, options CacheOptions, leakyOptions base.LeakyBucketConfig) (*Database, context.Context) {
	ctx := base.TestCtx(t)
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	testBucket := base.GetTestBucket(t)
	leakyBucket := base.NewLeakyBucket(testBucket, leakyOptions)
	dbCtx, err := NewDatabaseContext(ctx, "db", leakyBucket, false, dbcOptions)
	if err != nil {
		testBucket.Close()
		t.Fatalf("Unable to create database context: %v", err)
	}
	db, err := CreateDatabase(dbCtx)
	if err != nil {
		dbCtx.Close(ctx)
		t.Fatalf("Unable to create database: %v", err)
	}
	ctx = db.AddDatabaseLogContext(ctx)
	return db, ctx
}

// Sets up test db with the specified database context options using a named collection from the pool.  Note that environment variables can
// override somedbcOptions properties.
func setupTestNamedCollectionDBWithOptions(t testing.TB, dbcOptions DatabaseContextOptions) (*Database, context.Context) {

	tBucket := base.GetTestBucket(t)
	return SetupTestDBForDataStoreWithOptions(t, tBucket, dbcOptions)
}

// Sets up test db with the specified database context options in _default scope and collection.  Note that environment variables can
// override somedbcOptions properties.
func setupTestDefaultCollectionDBWithOptions(t testing.TB, dbcOptions DatabaseContextOptions) (*Database, context.Context) {

	return SetupTestDBWithOptions(t, dbcOptions)
}

func assertHTTPError(t *testing.T, err error, status int) bool {
	var httpErr *base.HTTPError
	return assert.Error(t, err) &&
		assert.ErrorAs(t, err, &httpErr) &&
		assert.Equal(t, status, httpErr.Status)
}

func TestDatabase(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, doc, err := collection.Put(ctx, "doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = rev1id
	assert.NoError(t, err, "Couldn't create document")
	assert.Equal(t, "1-cb0c9a22be0e5a1b01084ec019defa81", rev1id)

	log.Printf("Create rev 2...")
	body["key1"] = "new value"
	body["key2"] = int64(4321)
	rev2id, _, err := collection.Put(ctx, "doc1", body)
	body[BodyId] = "doc1"
	body[BodyRev] = rev2id
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "2-488724414d0ed6b398d6d2aeb228d797", rev2id)

	// Retrieve the document:
	log.Printf("Retrieve doc...")
	gotbody, err := collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	AssertEqualBodies(t, body, gotbody)

	log.Printf("Retrieve rev 1...")
	gotbody, err = collection.Get1xRevBody(ctx, "doc1", rev1id, false, nil)
	assert.NoError(t, err, "Couldn't get document with rev 1")
	expectedResult := Body{"key1": "value1", "key2": 1234, BodyId: "doc1", BodyRev: rev1id}
	AssertEqualBodies(t, expectedResult, gotbody)

	log.Printf("Retrieve rev 2...")
	gotbody, err = collection.Get1xRevBody(ctx, "doc1", rev2id, false, nil)
	assert.NoError(t, err, "Couldn't get document with rev")
	AssertEqualBodies(t, body, gotbody)

	gotbody, err = collection.Get1xRevBody(ctx, "doc1", "bogusrev", false, nil)
	status, _ := base.ErrorAsHTTPStatus(err)
	assert.Equal(t, 404, status)

	// Test the _revisions property:
	log.Printf("Check _revisions...")
	gotbody, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	revisions := gotbody[BodyRevisions].(Revisions)
	assert.Equal(t, 2, revisions[RevisionsStart])
	assert.Equal(t, []string{"488724414d0ed6b398d6d2aeb228d797",
		"cb0c9a22be0e5a1b01084ec019defa81"}, revisions[RevisionsIds])

	// Test RevDiff:
	log.Printf("Check RevDiff...")
	missing, possible := collection.RevDiff(ctx, "doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"2-488724414d0ed6b398d6d2aeb228d797"})
	assert.True(t, missing == nil)
	assert.True(t, possible == nil)

	missing, possible = collection.RevDiff(ctx, "doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assert.Equal(t, []string{"3-foo"}, missing)
	assert.Equal(t, []string{"2-488724414d0ed6b398d6d2aeb228d797"}, possible)

	missing, possible = collection.RevDiff(ctx, "nosuchdoc",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assert.Equal(t, []string{"1-cb0c9a22be0e5a1b01084ec019defa81",
		"3-foo"}, missing)

	assert.True(t, possible == nil)

	// Test PutExistingRev:
	log.Printf("Check PutExistingRev...")
	body[BodyRev] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		"1-cb0c9a22be0e5a1b01084ec019defa81"}
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", body, history, false)
	body[BodyId] = doc.ID
	body[BodyRev] = newRev
	assert.NoError(t, err, "PutExistingRev failed")

	// Retrieve the document:
	log.Printf("Check Get...")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	AssertEqualBodies(t, body, gotbody)

}

func TestGetDeleted(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	body := Body{"key1": 1234}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Put")

	rev2id, err := collection.DeleteDoc(ctx, "doc1", rev1id)
	assert.NoError(t, err, "DeleteDoc")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	expectedResult := Body{
		BodyId:        "doc1",
		BodyRev:       rev2id,
		BodyDeleted:   true,
		BodyRevisions: Revisions{RevisionsStart: 2, RevisionsIds: []string{"bc6d97f6e97c0d034a34f8aac2bf8b44", "dfd5e19813767eeddd08270fc5f385cd"}},
	}
	AssertEqualBodies(t, expectedResult, body)

	// Get the raw doc and make sure the sync data has the current revision
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Err getting doc")
	assert.Equal(t, rev2id, doc.SyncData.CurrentRev)

	// Try again but with a user who doesn't have access to this revision (see #179)
	authenticator := auth.NewAuthenticator(db.MetadataStore, db, auth.DefaultAuthenticatorOptions())
	collection.user, err = authenticator.GetUser("")
	assert.NoError(t, err, "GetUser")
	collection.user.SetExplicitChannels(nil, 1)

	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	AssertEqualBodies(t, expectedResult, body)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemovedAsUser(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, _, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRev:    rev1id,
	}
	rev2id, _, err := collection.Put(ctx, "doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = collection.Put(ctx, "doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	rev2digest := rev2id[2:]
	rev1digest := rev1id[2:]
	expectedResult := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
		BodyId:  "doc1",
		BodyRev: rev2id,
	}
	AssertEqualBodies(t, expectedResult, body)

	// Manually remove the temporary backup doc from the bucket
	// Manually flush the rev cache
	// After expiry from the rev cache and removal of doc backup, try again
	cacheHitCounter, cacheMissCounter := db.DatabaseContext.DbStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.Cache().RevisionCacheMisses
	collection.revisionCache = NewShardedLRURevisionCache(DefaultRevisionCacheShardCount, DefaultRevisionCacheSize, collection, cacheHitCounter, cacheMissCounter)
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2id)
	assert.NoError(t, err, "Purge old revision JSON")

	// Try again with a user who doesn't have access to this revision
	authenticator := auth.NewAuthenticator(db.MetadataStore, db, auth.DefaultAuthenticatorOptions())
	collection.user, err = authenticator.GetUser("")
	assert.NoError(t, err, "GetUser")

	var chans channels.TimedSet
	chans = channels.AtSequence(base.SetOf("ABC"), 1)
	collection.user.SetExplicitChannels(chans, 1)

	// Get the removal revision with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	require.NoError(t, err, "Get1xRevBody")
	expectedResult = Body{
		BodyId:      "doc1",
		BodyRev:     rev2id,
		BodyRemoved: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
	}
	assert.Equal(t, expectedResult, body)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev1id)
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

func TestIsServerless(t *testing.T) {
	testCases := []struct {
		title      string
		serverless bool
	}{
		{
			serverless: true,
		},
		{
			serverless: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestIsServerless with Serverless=%t", testCase.serverless), func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)

			db.Options.Serverless = testCase.serverless
			assert.Equal(t, testCase.serverless, db.IsServerless())
		})
	}
}

// Test removal handling for unavailable multi-channel revisions.
func TestGetRemovalMultiChannel(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	auth := db.Authenticator(base.TestCtx(t))

	// Create a user who have access to both channel ABC and NBC.
	userAlice, err := auth.NewUser("alice", "pass", base.SetOf("ABC", "NBC"))
	require.NoError(t, err, "Error creating user")

	// Create a user who have access to channel NBC.
	userBob, err := auth.NewUser("bob", "pass", base.SetOf("NBC"))
	require.NoError(t, err, "Error creating user")

	// Create the first revision of doc1.
	rev1Body := Body{
		"k1":       "v1",
		"channels": append([]string{"ABC", "NBC"}),
	}
	rev1ID, _, err := collection.Put(ctx, "doc1", rev1Body)
	require.NoError(t, err, "Error creating doc")

	// Create the second revision of doc1 on channel ABC as removal from channel NBC.
	rev2Body := Body{
		"k2":       "v2",
		"channels": []string{"ABC"},
		BodyRev:    rev1ID,
	}
	rev2ID, _, err := collection.Put(ctx, "doc1", rev2Body)
	require.NoError(t, err, "Error creating doc")

	// Create the third revision of doc1 on channel ABC.
	rev3Body := Body{
		"k3":       "v3",
		"channels": []string{"ABC"},
		BodyRev:    rev2ID,
	}
	rev3ID, _, err := collection.Put(ctx, "doc1", rev3Body)
	require.NoError(t, err, "Error creating doc")
	require.NotEmpty(t, rev3ID, "Error creating doc")

	// Get rev2 of the doc as a user who have access to this revision.
	collection.user = userAlice
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	require.NoError(t, err, "Error getting 1x rev body")

	_, rev1Digest := ParseRevID(rev1ID)
	_, rev2Digest := ParseRevID(rev2ID)

	bodyExpected := Body{
		"k2":       "v2",
		"channels": []string{"ABC"},
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2Digest, rev1Digest},
		},
		BodyId:  "doc1",
		BodyRev: rev2ID,
	}
	require.Equal(t, bodyExpected, body)

	// Get rev2 of the doc as a user who doesn't have access to this revision.
	collection.user = userBob
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	require.NoError(t, err, "Error getting 1x rev body")
	bodyExpected = Body{
		BodyRemoved: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2Digest, rev1Digest},
		},
		BodyId:  "doc1",
		BodyRev: rev2ID,
	}
	require.Equal(t, bodyExpected, body)

	// Flush the revision cache and purge the old revision backup.
	collection.FlushRevisionCacheForTest()
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2ID)
	require.NoError(t, err, "Error purging old revision JSON")

	// Try with a user who has access to this revision.
	collection.user = userAlice
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	assertHTTPError(t, err, 404)

	// Get rev2 of the doc as a user who doesn't have access to this revision.
	collection.user = userBob
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	require.NoError(t, err, "Error getting 1x rev body")
	bodyExpected = Body{
		BodyRemoved: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2Digest, rev1Digest},
		},
		BodyId:  "doc1",
		BodyRev: rev2ID,
	}
	require.Equal(t, bodyExpected, body)
}

// Test delta sync behavior when the fromRevision is a channel removal.
func TestDeltaSyncWhenFromRevIsChannelRemoval(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create the first revision of doc1.
	rev1Body := Body{
		"k1":       "v1",
		"channels": append([]string{"ABC", "NBC"}),
	}
	rev1ID, _, err := collection.Put(ctx, "doc1", rev1Body)
	require.NoError(t, err, "Error creating doc")

	// Create the second revision of doc1 on channel ABC as removal from channel NBC.
	rev2Body := Body{
		"k2":       "v2",
		"channels": []string{"ABC"},
		BodyRev:    rev1ID,
	}
	rev2ID, _, err := collection.Put(ctx, "doc1", rev2Body)
	require.NoError(t, err, "Error creating doc")

	// Create the third revision of doc1 on channel ABC.
	rev3Body := Body{
		"k3":       "v3",
		"channels": []string{"ABC"},
		BodyRev:    rev2ID,
	}
	rev3ID, _, err := collection.Put(ctx, "doc1", rev3Body)
	require.NoError(t, err, "Error creating doc")
	require.NotEmpty(t, rev3ID, "Error creating doc")

	// Flush the revision cache and purge the old revision backup.
	collection.FlushRevisionCacheForTest()
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2ID)
	require.NoError(t, err, "Error purging old revision JSON")

	// Request delta between rev2ID and rev3ID (toRevision "rev2ID" is channel removal)
	// as a user who doesn't have access to the removed revision via any other channel.
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "pass", base.SetOf("NBC"))
	require.NoError(t, err, "Error creating user")

	collection.user = user
	require.NoError(t, db.DbStats.InitDeltaSyncStats())

	delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2ID, rev3ID)
	require.Equal(t, base.HTTPErrorf(404, "missing"), err)
	assert.Nil(t, delta)
	assert.Nil(t, redactedRev)

	// Request delta between rev2ID and rev3ID (toRevision "rev2ID" is channel removal)
	// as a user who has access to the removed revision via another channel.
	user, err = authenticator.NewUser("bob", "pass", base.SetOf("ABC"))
	require.NoError(t, err, "Error creating user")

	collection.user = user
	require.NoError(t, db.DbStats.InitDeltaSyncStats())

	delta, redactedRev, err = collection.GetDelta(ctx, "doc1", rev2ID, rev3ID)
	require.Equal(t, base.HTTPErrorf(404, "missing"), err)
	assert.Nil(t, delta)
	assert.Nil(t, redactedRev)
}

// Test delta sync behavior when the toRevision is a channel removal.
func TestDeltaSyncWhenToRevIsChannelRemoval(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create the first revision of doc1.
	rev1Body := Body{
		"k1":       "v1",
		"channels": append([]string{"ABC", "NBC"}),
	}
	rev1ID, _, err := collection.Put(ctx, "doc1", rev1Body)
	require.NoError(t, err, "Error creating doc")

	// Create the second revision of doc1 on channel ABC as removal from channel NBC.
	rev2Body := Body{
		"k2":       "v2",
		"channels": []string{"ABC"},
		BodyRev:    rev1ID,
	}
	rev2ID, _, err := collection.Put(ctx, "doc1", rev2Body)
	require.NoError(t, err, "Error creating doc")

	// Create the third revision of doc1 on channel ABC.
	rev3Body := Body{
		"k3":       "v3",
		"channels": []string{"ABC"},
		BodyRev:    rev2ID,
	}
	rev3ID, _, err := collection.Put(ctx, "doc1", rev3Body)
	require.NoError(t, err, "Error creating doc")
	require.NotEmpty(t, rev3ID, "Error creating doc")

	// Flush the revision cache and purge the old revision backup.
	collection.FlushRevisionCacheForTest()
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2ID)
	require.NoError(t, err, "Error purging old revision JSON")

	// Request delta between rev1ID and rev2ID (toRevision "rev2ID" is channel removal)
	// as a user who doesn't have access to the removed revision via any other channel.
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "pass", base.SetOf("NBC"))
	require.NoError(t, err, "Error creating user")

	collection.user = user
	require.NoError(t, db.DbStats.InitDeltaSyncStats())

	delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev1ID, rev2ID)
	require.NoError(t, err)
	assert.Nil(t, delta)
	assert.Equal(t, `{"_removed":true}`, string(redactedRev.BodyBytes))

	// Request delta between rev1ID and rev2ID (toRevision "rev2ID" is channel removal)
	// as a user who has access to the removed revision via another channel.
	user, err = authenticator.NewUser("bob", "pass", base.SetOf("ABC"))
	require.NoError(t, err, "Error creating user")

	collection.user = user
	require.NoError(t, db.DbStats.InitDeltaSyncStats())

	delta, redactedRev, err = collection.GetDelta(ctx, "doc1", rev1ID, rev2ID)
	require.Equal(t, base.HTTPErrorf(404, "missing"), err)
	assert.Nil(t, delta)
	assert.Nil(t, redactedRev)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemoved(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, _, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRev:    rev1id,
	}
	rev2id, _, err := collection.Put(ctx, "doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = collection.Put(ctx, "doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	rev2digest := rev2id[2:]
	rev1digest := rev1id[2:]
	expectedResult := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
		BodyId:  "doc1",
		BodyRev: rev2id,
	}
	AssertEqualBodies(t, expectedResult, body)

	// Manually remove the temporary backup doc from the bucket
	// Manually flush the rev cache
	// After expiry from the rev cache and removal of doc backup, try again
	cacheHitCounter, cacheMissCounter := db.DatabaseContext.DbStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.Cache().RevisionCacheMisses
	collection.revisionCache = NewShardedLRURevisionCache(DefaultRevisionCacheShardCount, DefaultRevisionCacheSize, collection, cacheHitCounter, cacheMissCounter)
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2id)
	assert.NoError(t, err, "Purge old revision JSON")

	// Get the removal revision with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assertHTTPError(t, err, 404)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev1id)
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemovedAndDeleted(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, _, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":      1234,
		BodyDeleted: true,
		BodyRev:     rev1id,
	}
	rev2id, _, err := collection.Put(ctx, "doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = collection.Put(ctx, "doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	rev2digest := rev2id[2:]
	rev1digest := rev1id[2:]
	expectedResult := Body{
		"key1":      1234,
		BodyDeleted: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
		BodyId:  "doc1",
		BodyRev: rev2id,
	}
	AssertEqualBodies(t, expectedResult, body)

	// Manually remove the temporary backup doc from the bucket
	// Manually flush the rev cache
	// After expiry from the rev cache and removal of doc backup, try again
	cacheHitCounter, cacheMissCounter := db.DatabaseContext.DbStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.Cache().RevisionCacheMisses
	collection.revisionCache = NewShardedLRURevisionCache(DefaultRevisionCacheShardCount, DefaultRevisionCacheSize, collection, cacheHitCounter, cacheMissCounter)
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2id)
	assert.NoError(t, err, "Purge old revision JSON")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assertHTTPError(t, err, 404)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev1id)
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

type AllDocsEntry struct {
	IDRevAndSequence
	Channels []string
}

func (e AllDocsEntry) Equal(e2 AllDocsEntry) bool {
	return e.DocID == e2.DocID && e.RevID == e2.RevID && e.Sequence == e2.Sequence &&
		base.SetFromArray(e.Channels).Equals(base.SetFromArray(e2.Channels))
}

var options ForEachDocIDOptions

func allDocIDs(ctx context.Context, collection *DatabaseCollection) (docs []AllDocsEntry, err error) {
	err = collection.ForEachDocID(ctx, func(doc IDRevAndSequence, channels []string) (bool, error) {
		docs = append(docs, AllDocsEntry{
			IDRevAndSequence: doc,
			Channels:         channels,
		})
		return true, nil
	}, options)
	return
}

func TestAllDocsOnly(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	// Lower the log max length so no more than 50 items will be kept.
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = 50

	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	collectionID := collection.GetCollectionID()

	// Trigger creation of the channel cache for channel "all"
	collection.changeCache.getChannelCache().getSingleChannelCache(channels.NewID("all", collectionID))

	ids := make([]AllDocsEntry, 100)
	for i := 0; i < 100; i++ {
		channels := []string{"all"}
		if i%10 == 0 {
			channels = append(channels, "KFJC")
		}
		body := Body{"serialnumber": int64(i), "channels": channels}
		ids[i].DocID = fmt.Sprintf("alldoc-%02d", i)
		revid, _, err := collection.Put(ctx, ids[i].DocID, body)
		ids[i].RevID = revid
		ids[i].Sequence = uint64(i + 1)
		ids[i].Channels = channels
		assert.NoError(t, err, "Couldn't create document")
	}

	alldocs, err := allDocIDs(ctx, collection.DatabaseCollection)
	assert.NoError(t, err, "AllDocIDs failed")
	require.Len(t, alldocs, 100)
	for i, entry := range alldocs {
		assert.True(t, entry.Equal(ids[i]))
	}

	// Now delete one document and try again:
	_, err = collection.DeleteDoc(ctx, ids[23].DocID, ids[23].RevID)
	assert.NoError(t, err, "Couldn't delete doc 23")

	alldocs, err = allDocIDs(ctx, collection.DatabaseCollection)
	assert.NoError(t, err, "AllDocIDs failed")
	require.Len(t, alldocs, 99)
	for i, entry := range alldocs {
		j := i
		if i >= 23 {
			j++
		}
		assert.True(t, entry.Equal(ids[j]))
	}

	// Inspect the channel log to confirm that it's only got the last 50 sequences.
	// There are 101 sequences overall, so the 1st one it has should be #52.
	err = collection.changeCache.waitForSequence(ctx, 101, base.DefaultWaitForSequence)
	require.NoError(t, err)

	changeLog := collection.GetChangeLog(channels.NewID("all", collectionID), 0)
	require.Len(t, changeLog, 50)
	assert.Equal(t, "alldoc-51", changeLog[0].DocID)

	// Now check the changes feed:
	var options ChangesOptions
	changesCtx, changesCtxCancel := context.WithCancel(context.Background())
	options.ChangesCtx = changesCtx
	defer changesCtxCancel()
	changes, err := collection.GetChanges(ctx, channels.BaseSetOf(t, "all"), options)
	assert.NoError(t, err, "Couldn't GetChanges")
	require.Len(t, changes, 100)

	for i, change := range changes {
		docIndex := i
		if i >= 23 {
			docIndex++
		}
		if i == len(changes)-1 {
			// The last entry in the changes response should be the deleted document
			assert.True(t, change.Deleted)
			assert.Equal(t, "alldoc-23", change.ID)
			assert.Equal(t, channels.BaseSetOf(t, "all"), change.Removed)
		} else {
			// Verify correct ordering for all other documents
			assert.Equal(t, fmt.Sprintf("alldoc-%02d", docIndex), change.ID)
		}
	}
	// Check whether sequences are ascending for all entries in the changes response
	sortedSeqAsc := func(changes []*ChangeEntry) bool {
		return sort.SliceIsSorted(changes, func(i, j int) bool {
			return changes[i].Seq.Seq < changes[j].Seq.Seq
		})
	}
	assert.True(t, sortedSeqAsc(changes), "Sequences should be ascending for all entries in the changes response")

	options.IncludeDocs = true
	changes, err = collection.GetChanges(ctx, channels.BaseSetOf(t, "KFJC"), options)
	assert.NoError(t, err, "Couldn't GetChanges")
	assert.Len(t, changes, 10)
	for i, change := range changes {
		assert.Equal(t, ids[10*i].DocID, change.ID)
		assert.False(t, change.Deleted)
		assert.Equal(t, base.Set(nil), change.Removed)
		var changeBody Body
		require.NoError(t, changeBody.Unmarshal(change.Doc))
		// unmarshalled as json.Number, so just compare the strings
		assert.Equal(t, strconv.FormatInt(int64(10*i), 10), changeBody["serialnumber"].(json.Number).String())
	}
	assert.True(t, sortedSeqAsc(changes), "Sequences should be ascending for all entries in the changes response")
}

// Unit test for bug #673
func TestUpdatePrincipal(t *testing.T) {

	if base.TestsUseNamedCollections() {
		t.Skip("Disabled for non-default collection based on use of GetPrincipalForTest")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, _ := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, authenticator.Save(user))

	// Validate that a call to UpdatePrincipals with no changes to the user doesn't allocate a sequence
	userInfo, err := db.GetPrincipalForTest(t, "naomi", true)
	userInfo.ExplicitChannels = base.SetOf("ABC")
	_, err = db.UpdatePrincipal(ctx, userInfo, true, true)
	assert.NoError(t, err, "Unable to update principal")

	nextSeq, err := db.sequences.nextSequence()
	assert.Equal(t, uint64(1), nextSeq)

	// Validate that a call to UpdatePrincipals with changes to the user does allocate a sequence
	userInfo, err = db.GetPrincipalForTest(t, "naomi", true)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(ctx, userInfo, true, true)
	assert.NoError(t, err, "Unable to update principal")

	nextSeq, err = db.sequences.nextSequence()
	assert.Equal(t, uint64(3), nextSeq)
}

// Re-apply one of the conflicting changes to make sure that PutExistingRevWithBody() treats it as a no-op (SG Issue #3048)
func TestRepeatedConflict(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")

	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	_, newRev, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Get the _rev that was set in the body by PutExistingRevWithBody() and make assertions on it
	revGen, _ := ParseRevID(newRev)
	assert.Equal(t, 2, revGen)

	// Remove the _rev key from the body, and call PutExistingRevWithBody() again, which should re-add it
	delete(body, BodyRev)
	_, newRev, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err)

	// The _rev should pass the same assertions as before, since PutExistingRevWithBody() should re-add it
	revGen, _ = ParseRevID(newRev)
	assert.Equal(t, 2, revGen)

}

func TestConflicts(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Instantiate channel cache for channel 'all'
	collectionID := collection.GetCollectionID()

	allChannel := channels.NewID("all", collectionID)
	collection.changeCache.getChannelCache().getSingleChannelCache(allChannel)

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Wait for rev to be cached
	cacheWaiter.AddAndWait(1)

	changeLog := collection.GetChangeLog(channels.NewID("all", collectionID), 0)
	assert.Equal(t, 1, len(changeLog))

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	cacheWaiter.Add(2)

	rawBody, _, _ := collection.dataStore.GetRaw("doc")

	log.Printf("got raw body: %s", rawBody)

	// Verify the change with the higher revid won:
	gotBody, err := collection.Get1xBody(ctx, "doc")
	expectedResult := Body{BodyId: "doc", BodyRev: "2-b", "n": 2, "channels": []string{"all", "2b"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify we can still get the other two revisions:
	gotBody, err = collection.Get1xRevBody(ctx, "doc", "1-a", false, nil)
	expectedResult = Body{BodyId: "doc", BodyRev: "1-a", "n": 1, "channels": []string{"all", "1"}}
	AssertEqualBodies(t, expectedResult, gotBody)
	gotBody, err = collection.Get1xRevBody(ctx, "doc", "2-a", false, nil)
	expectedResult = Body{BodyId: "doc", BodyRev: "2-a", "n": 3, "channels": []string{"all", "2a"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify the change-log of the "all" channel:
	cacheWaiter.Wait()
	changeLog = collection.GetChangeLog(allChannel, 0)
	assert.Equal(t, 1, len(changeLog))
	assert.Equal(t, uint64(3), changeLog[0].Sequence)
	assert.Equal(t, "doc", changeLog[0].DocID)
	assert.Equal(t, "2-b", changeLog[0].RevID)
	assert.Equal(t, uint8(channels.Hidden|channels.Branched|channels.Conflict), changeLog[0].Flags)

	// Verify the _changes feed:
	options := ChangesOptions{
		Conflicts:  true,
		ChangesCtx: context.Background(),
	}
	changes, err := collection.GetChanges(ctx, channels.BaseSetOf(t, "all"), options)
	assert.NoError(t, err, "Couldn't GetChanges")
	assert.Equal(t, 1, len(changes))
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 3},
		ID:           "doc",
		Changes:      []ChangeRev{{"rev": "2-b"}, {"rev": "2-a"}},
		branched:     true,
		collectionID: collectionID,
	}, changes[0],
	)

	// Delete 2-b; verify this makes 2-a current:
	rev3, err := collection.DeleteDoc(ctx, "doc", "2-b")
	assert.NoError(t, err, "delete 2-b")

	rawBody, _, _ = collection.dataStore.GetRaw("doc")
	log.Printf("post-delete, got raw body: %s", rawBody)

	gotBody, err = collection.Get1xBody(ctx, "doc")
	expectedResult = Body{BodyId: "doc", BodyRev: "2-a", "n": 3, "channels": []string{"all", "2a"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify channel assignments are correct for channels defined by 2-a:
	doc, _ := collection.GetDocument(ctx, "doc", DocUnmarshalAll)
	chan2a, found := doc.Channels["2a"]
	assert.True(t, found)
	assert.True(t, chan2a == nil)             // currently in 2a
	assert.True(t, doc.Channels["2b"] != nil) // has been removed from 2b

	// Wait for delete mutation to arrive over feed
	cacheWaiter.AddAndWait(1)

	// Verify the _changes feed:
	changes, err = collection.GetChanges(ctx, channels.BaseSetOf(t, "all"), options)
	assert.NoError(t, err, "Couldn't GetChanges")
	assert.Equal(t, 1, len(changes))
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 4},
		ID:           "doc",
		Changes:      []ChangeRev{{"rev": "2-a"}, {"rev": rev3}},
		branched:     true,
		collectionID: collectionID,
	}, changes[0])

}

func TestConflictRevLimit(t *testing.T) {

	// Test Default Is the higher of the two
	db, ctx := setupTestDB(t)
	assert.Equal(t, uint32(DefaultRevsLimitConflicts), db.RevsLimit)
	defer db.Close(ctx)

	// Test AllowConflicts
	dbOptions := DatabaseContextOptions{
		AllowConflicts: base.BoolPtr(true),
	}

	db, ctx = SetupTestDBWithOptions(t, dbOptions)
	assert.Equal(t, uint32(DefaultRevsLimitConflicts), db.RevsLimit)
	defer db.Close(ctx)

	// Test AllowConflicts false
	dbOptions = DatabaseContextOptions{
		AllowConflicts: base.BoolPtr(false),
	}

	db, ctx = SetupTestDBWithOptions(t, dbOptions)
	assert.Equal(t, uint32(DefaultRevsLimitNoConflicts), db.RevsLimit)
	defer db.Close(ctx)

}

func TestNoConflictsMode(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()
	// Strictly speaking, this flag should be set before opening the database, but it only affects
	// Put operations and replication, so it doesn't make a difference if we do it afterwards.
	db.Options.AllowConflicts = base.BoolPtr(false)

	// Create revs 1 and 2 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	body["n"] = 2
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Try to create a conflict branching from rev 1:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-b", "1-a"}, false)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with no common ancestor:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-c", "1-c"}, false)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with a longer history:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"4-d", "3-d", "2-d", "1-a"}, false)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with no history:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-e"}, false)
	assertHTTPError(t, err, 409)

	// Create a non-conflict with a longer history, ending in a deletion:
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "add 4-a")
	delete(body, BodyDeleted)

	// Try to resurrect the document with a conflicting branch
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"4-f", "3-a"}, false)
	assertHTTPError(t, err, 409)

	// Resurrect the tombstoned document with a disconnected branch):
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-f"}, false)
	assert.NoError(t, err, "add 1-f")

	// Tombstone the resurrected branch
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-f", "1-f"}, false)
	assert.NoError(t, err, "add 2-f")
	delete(body, BodyDeleted)

	// Resurrect the tombstoned document with a valid history (descendents of leaf)
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"5-f", "4-a"}, false)
	assert.NoError(t, err, "add 5-f")
	delete(body, BodyDeleted)

	// Create a new document with a longer history:
	_, _, err = collection.PutExistingRevWithBody(ctx, "COD", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "add COD")
	delete(body, BodyDeleted)

	// Now use Put instead of PutExistingRev:

	// Successfully add a new revision:
	_, _, err = collection.Put(ctx, "doc", Body{BodyRev: "5-f", "foo": -1})
	assert.NoError(t, err, "Put rev after 1-f")

	// Try to create a conflict:
	_, _, err = collection.Put(ctx, "doc", Body{BodyRev: "3-a", "foo": 7})
	assertHTTPError(t, err, 409)

	// Conflict with no ancestry:
	_, _, err = collection.Put(ctx, "doc", Body{"foo": 7})
	assertHTTPError(t, err, 409)
}

// Test tombstoning of existing conflicts after AllowConflicts is set to false via Put
func TestAllowConflictsFalseTombstoneExistingConflict(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create documents with multiple non-deleted branches
	log.Printf("Creating docs")
	body := Body{"n": 1}
	doc, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	doc, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Set AllowConflicts to false
	db.Options.AllowConflicts = base.BoolPtr(false)

	// Attempt to tombstone a non-leaf node of a conflicted document
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-c", "1-a"}, false)
	assert.True(t, err != nil, "expected error tombstoning non-leaf")

	// Tombstone the non-winning branch of a conflicted document
	body[BodyRev] = "2-a"
	body[BodyDeleted] = true
	tombstoneRev, _, putErr := collection.Put(ctx, "doc1", body)
	assert.NoError(t, putErr, "tombstone 2-a")
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.CurrentRev)

	// Attempt to add a tombstone rev w/ the previous tombstone as parent
	body[BodyRev] = tombstoneRev
	body[BodyDeleted] = true
	_, _, putErr = collection.Put(ctx, "doc1", body)
	assert.True(t, putErr != nil, "Expect error tombstoning a tombstone")

	// Tombstone the winning branch of a conflicted document
	body[BodyRev] = "2-b"
	body[BodyDeleted] = true
	_, _, putErr = collection.Put(ctx, "doc2", body)
	assert.NoError(t, putErr, "tombstone 2-b")
	doc, err = collection.GetDocument(ctx, "doc2", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-a", doc.CurrentRev)

	// Set revs_limit=1, then tombstone non-winning branch of a conflicted document.  Validate retrieval still works.
	db.RevsLimit = uint32(1)
	body[BodyRev] = "2-a"
	body[BodyDeleted] = true
	_, _, putErr = collection.Put(ctx, "doc3", body)
	assert.NoError(t, putErr, "tombstone 2-a w/ revslimit=1")
	doc, err = collection.GetDocument(ctx, "doc3", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.CurrentRev)

	log.Printf("tombstoned conflicts: %+v", doc)

}

// Test tombstoning of existing conflicts after AllowConflicts is set to false via PutExistingRev
func TestAllowConflictsFalseTombstoneExistingConflictNewEditsFalse(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create documents with multiple non-deleted branches
	log.Printf("Creating docs")
	body := Body{"n": 1}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Set AllowConflicts to false
	db.Options.AllowConflicts = base.BoolPtr(false)
	delete(body, "n")

	// Attempt to tombstone a non-leaf node of a conflicted document
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-c", "1-a"}, false)
	assert.True(t, err != nil, "expected error tombstoning non-leaf")

	// Tombstone the non-winning branch of a conflicted document
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a (tombstone)")
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.CurrentRev)

	// Tombstone the winning branch of a conflicted document
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"3-b", "2-b"}, false)
	assert.NoError(t, err, "add 3-b (tombstone)")
	doc, err = collection.GetDocument(ctx, "doc2", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-a", doc.CurrentRev)

	// Set revs_limit=1, then tombstone non-winning branch of a conflicted document.  Validate retrieval still works.
	body[BodyDeleted] = true
	db.RevsLimit = uint32(1)
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a (tombstone)")
	doc, err = collection.GetDocument(ctx, "doc3", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.CurrentRev)

	log.Printf("tombstoned conflicts: %+v", doc)
}

func TestSyncFnOnPush(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		log("doc _id = "+doc._id+", _rev = "+doc._rev);
		if (oldDoc)
			log("oldDoc _id = "+oldDoc._id+", _rev = "+oldDoc._rev);
		channel(doc.channels);
	}`, 0)

	// Create first revision:
	body := Body{"key1": "value1", "key2": 1234, "channels": []string{"public"}}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	// Add several revisions at once to a doc, as on a push:
	log.Printf("Check PutExistingRev...")
	body[BodyRev] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	body["channels"] = "clibup"
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		rev1id}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, history, false)
	assert.NoError(t, err, "PutExistingRev failed")

	// Check that the doc has the correct channel (test for issue #300)
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.Equal(t, channels.ChannelMap{
		"clibup": nil,
		"public": &channels.ChannelRemoval{Seq: 2, RevID: "4-four"},
	}, doc.Channels)

	assert.Equal(t, base.SetOf("clibup"), doc.History["4-four"].Channels)
}

func TestInvalidChannel(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	body := Body{"channels": []string{"bad,name"}}
	_, _, err := collection.Put(ctx, "doc", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionValidation(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`, 0)

	body := Body{"users": []string{"username"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"role:rolename"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc2", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"bad,username"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc3", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"role:bad:rolename"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc4", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{",.,.,.,.,."}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc5", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"username"}, "userChannels": []string{"bad,name"}}
	_, _, err = collection.Put(ctx, "doc6", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionDb(t *testing.T) {

	if base.TestsUseNamedCollections() {
		t.Skip("Disabled for non-default collection until CBG-2554")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	authenticator := auth.NewAuthenticator(db.MetadataStore, db, auth.DefaultAuthenticatorOptions())

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`, 0)

	user, _ := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "Netflix"))
	user.SetExplicitRoles(channels.TimedSet{"animefan": channels.NewVbSimpleSequence(1), "tumblr": channels.NewVbSimpleSequence(1)}, 1)
	assert.NoError(t, authenticator.Save(user), "Save")

	body := Body{"users": []string{"naomi"}, "userChannels": []string{"Hulu"}}
	_, _, err = collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"role:animefan"}, "userChannels": []string{"CrunchyRoll"}}
	_, _, err = collection.Put(ctx, "doc2", body)
	assert.NoError(t, err, "")

	// Create the role _after_ creating the documents, to make sure the previously-indexed access
	// privileges are applied.
	role, _ := authenticator.NewRole("animefan", nil)
	assert.NoError(t, authenticator.Save(role))

	user, err = authenticator.GetUser("naomi")
	assert.NoError(t, err, "GetUser")
	expected := channels.AtSequence(channels.BaseSetOf(t, "Hulu", "Netflix", "!"), 1)
	assert.Equal(t, expected, user.Channels())

	expected.AddChannel("CrunchyRoll", 2)
	assert.Equal(t, expected, user.InheritedChannels())
}

func TestDocIDs(t *testing.T) {
	tests := []struct {
		name  string
		docID string
		valid bool
	}{
		{name: "normal doc ID", docID: "foo", valid: true},
		{name: "non-prefix underscore", docID: "foo_", valid: true},

		{name: "spaces", docID: "foo bar", valid: true},
		{name: "symbols", docID: "foo!", valid: true},

		{name: "symbols (ASCII hex)", docID: "foo\x21", valid: true},
		{name: "control chars (NUL)", docID: "\x00foo", valid: true},
		{name: "control chars (BEL)", docID: "foo\x07", valid: true},

		{name: "empty", docID: ""}, // disallow empty doc IDs

		// disallow underscore prefixes
		{name: "underscore prefix", docID: "_"},
		{name: "underscore prefix", docID: "_foo"},
		{name: "underscore prefix", docID: "_design/foo"},
		{name: "underscore prefix", docID: base.RevPrefix + "x"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := ""
			if test.valid {
				expected = test.docID
			}
			assert.Equal(t, expected, realDocID(test.docID))
		})
	}
}

func TestUpdateDesignDoc(t *testing.T) {

	if base.TestsUseNamedCollections() {
		t.Skip("Design docs not supported for non-default collection")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	mapFunction := `function (doc, meta) { emit(); }`
	err := db.PutDesignDoc("official", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"TestView": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "add design doc as admin")

	// Validate retrieval of the design doc by admin
	var result sgbucket.DesignDoc
	result, err = db.GetDesignDoc("official")
	log.Printf("design doc: %+v", result)

	assert.NoError(t, err, "retrieve design doc as admin")
	retrievedView, ok := result.Views["TestView"]
	assert.True(t, ok)
	assert.True(t, strings.Contains(retrievedView.Map, "emit()"))
	assert.NotEqual(t, mapFunction, retrievedView.Map) // SG should wrap the map function, so they shouldn't be equal

	authenticator := auth.NewAuthenticator(db.MetadataStore, db, auth.DefaultAuthenticatorOptions())
	db.user, _ = authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "Netflix"))
	err = db.PutDesignDoc("_design/pwn3d", sgbucket.DesignDoc{})
	assertHTTPError(t, err, 403)
}

func TestPostWithExistingId(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err := collection.Post(ctx, body)
	require.NoError(t, err, "Couldn't create document")
	assert.True(t, rev1id != "")
	assert.True(t, docid == customDocId)

	// Test retrieval
	doc, err := collection.GetDocument(ctx, customDocId, DocUnmarshalAll)
	assert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using custom id")

	// Test that standard UUID creation still works:
	log.Printf("Create document with existing id...")
	body = Body{"notAnId": customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err = collection.Post(ctx, body)
	require.NoError(t, err, "Couldn't create document")
	assert.True(t, rev1id != "")
	assert.True(t, docid != customDocId)

	// Test retrieval
	doc, err = collection.GetDocument(ctx, docid, DocUnmarshalAll)
	assert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using generated uuid")

}

// Unit test for issue #976
func TestWithNullPropertyKey(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Test creating a document with null property key
	customDocId := "customIdValue"
	log.Printf("Create document with empty property key")
	body := Body{BodyId: customDocId, "": "value1"}
	docid, rev1id, _, err := collection.Post(ctx, body)
	require.NoError(t, err)
	assert.True(t, rev1id != "")
	assert.True(t, docid != "")
}

// Unit test for issue #507, modified for CBG-1995 (allowing special properties)
func TestPostWithUserSpecialProperty(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err := collection.Post(ctx, body)
	require.NoError(t, err, "Couldn't create document")
	require.NotEqual(t, "", rev1id)
	require.Equal(t, customDocId, docid)

	// Test retrieval
	doc, err := collection.GetDocument(ctx, customDocId, DocUnmarshalAll)
	require.NotNil(t, doc)
	assert.NoError(t, err, "Unable to retrieve doc using custom id")

	// Test that posting an update with a user special property does update the document
	log.Printf("Update document with existing id...")
	body = Body{BodyId: customDocId, BodyRev: rev1id, "_special": "value", "key1": "value1", "key2": "existing"}
	rev2id, _, err := collection.Put(ctx, docid, body)
	assert.NoError(t, err)

	// Test retrieval gets rev2
	doc, err = collection.GetDocument(ctx, docid, DocUnmarshalAll)
	require.NotNil(t, doc)
	assert.Equal(t, rev2id, doc.CurrentRev)
	assert.Equal(t, "value", doc.Body()["_special"])
	assert.NoError(t, err, "Unable to retrieve doc using generated uuid")
}

func TestRecentSequenceHistory(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	seqTracker := uint64(0)

	// Validate recent sequence is written
	body := Body{"val": "one"}
	revid, doc, err := collection.Put(ctx, "doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++

	expectedRecent := make([]uint64, 0)
	assert.True(t, revid != "")
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	expectedRecent = append(expectedRecent, seqTracker)
	assert.True(t, err == nil)
	assert.Equal(t, expectedRecent, doc.RecentSequences)

	// Add up to kMaxRecentSequences revisions - validate they are retained when total is less than max
	for i := 1; i < kMaxRecentSequences; i++ {
		revid, doc, err = collection.Put(ctx, "doc1", body)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		seqTracker++
		expectedRecent = append(expectedRecent, seqTracker)
	}

	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.True(t, err == nil)
	assert.Equal(t, expectedRecent, doc.RecentSequences)

	// Recent sequence pruning only prunes entries older than what's been seen over DCP
	// (to ensure it's not pruning something that may still be coalesced).  Because of this, test waits
	// for caching before attempting to trigger pruning.
	err = collection.changeCache.waitForSequence(ctx, seqTracker, base.DefaultWaitForSequence)
	require.NoError(t, err)

	// Add another sequence to validate pruning when past max (20)
	revid, doc, err = collection.Put(ctx, "doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.True(t, err == nil)
	log.Printf("recent:%d, max:%d", len(doc.RecentSequences), kMaxRecentSequences)
	assert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

	// Ensure pruning works when sequences aren't sequential
	doc2Body := Body{"val": "two"}
	for i := 0; i < kMaxRecentSequences; i++ {
		revid, doc, err = collection.Put(ctx, "doc1", body)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		seqTracker++
		revid, doc, err = collection.Put(ctx, "doc2", doc2Body)
		doc2Body[BodyId] = doc.ID
		doc2Body[BodyRev] = revid
		seqTracker++
	}

	err = collection.changeCache.waitForSequence(ctx, seqTracker, base.DefaultWaitForSequence) //
	require.NoError(t, err)
	revid, doc, err = collection.Put(ctx, "doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.True(t, err == nil)
	log.Printf("Recent sequences: %v (shouldn't exceed %v)", len(doc.RecentSequences), kMaxRecentSequences)
	assert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

}

func TestChannelView(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()
	collectionID := collection.GetCollectionID()

	// Create doc
	log.Printf("Create doc 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Couldn't create document")
	assert.Equal(t, "1-cb0c9a22be0e5a1b01084ec019defa81", rev1id)

	var entries LogEntries
	// Query view (retry loop to wait for indexing)
	for i := 0; i < 10; i++ {
		var err error
		entries, err = db.getChangesInChannelFromQuery(ctx, channels.ID{Name: "*", CollectionID: collectionID}, 0, 100, 0, false)

		assert.NoError(t, err, "Couldn't create document")
		if len(entries) >= 1 {
			break
		}
		log.Printf("No entries found - retrying (%d/10)", i+1)
		time.Sleep(500 * time.Millisecond)
	}

	for i, entry := range entries {
		log.Printf("View Query returned entry (%d): %v", i, entry)
	}
	assert.Equal(t, 1, len(entries))

}

// ////// XATTR specific tests.  These tests current require setting DefaultUseXattrs=true, and must be run against a Couchbase bucket

func TestConcurrentImport(t *testing.T) {

	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("Test only works with a Couchbase server and XATTRS")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport)

	// Add doc to the underlying bucket:
	_, err := collection.dataStore.Add("directWrite", 0, Body{"value": "hi"})
	require.NoError(t, err)

	// Attempt concurrent retrieval of the docs, and validate that they are only imported once (based on revid)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doc, err := collection.GetDocument(ctx, "directWrite", DocUnmarshalAll)
			assert.True(t, doc != nil)
			assert.NoError(t, err, "Document retrieval error")
			assert.Equal(t, "1-36fa688dc2a2c39a952ddce46ab53d12", doc.SyncData.CurrentRev)
		}()
	}
	wg.Wait()
}

// ////// BENCHMARKS

func BenchmarkDatabase(b *testing.B) {
	base.DisableTestLogging(b)

	for i := 0; i < b.N; i++ {
		ctx := base.TestCtx(b)
		bucket, _ := connectToBucket(ctx, base.BucketSpec{
			Server:     base.UnitTestUrl(),
			BucketName: fmt.Sprintf("b-%d", i)})
		dbCtx, _ := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{})
		db, _ := CreateDatabase(dbCtx)
		collection := db.GetSingleDatabaseCollectionWithUser()

		body := Body{"key1": "value1", "key2": 1234}
		_, _, _ = collection.Put(ctx, fmt.Sprintf("doc%d", i), body)

		db.Close(ctx)
	}
}

func BenchmarkPut(b *testing.B) {
	base.DisableTestLogging(b)

	ctx := base.TestCtx(b)
	bucket, _ := connectToBucket(ctx, base.BucketSpec{
		Server:     base.UnitTestUrl(),
		BucketName: "Bucket"})
	context, _ := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{})
	db, _ := CreateDatabase(context)
	collection := db.GetSingleDatabaseCollectionWithUser()

	body := Body{"key1": "value1", "key2": 1234}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = collection.Put(ctx, fmt.Sprintf("doc%d", i), body)
	}

	db.Close(ctx)
}

var (
	defaultProvider      = "Google"
	clientID             = "CouchbaseSynGatewayDev"
	callbackURL          = "https://127.0.0.1:4985/_callback"
	callbackURLWithQuery = "https://127.0.0.1:4985/_callback?"
	validationKey        = "some.validation.key"
)

func mockOIDCProvider() auth.OIDCProvider {
	return auth.OIDCProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:   "https://accounts.google.com",
			ClientID: base.StringPtr(clientID),
		},
		Name:          "Google",
		CallbackURL:   &callbackURL,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithCallbackURLQuery() auth.OIDCProvider {
	return auth.OIDCProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:   "https://accounts.google.com",
			ClientID: base.StringPtr(clientID),
		},
		Name:          "Google",
		CallbackURL:   &callbackURLWithQuery,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoIss() auth.OIDCProvider {
	return auth.OIDCProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			ClientID: base.StringPtr(clientID),
		},
		Name:          "Microsoft",
		CallbackURL:   &callbackURL,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoClientID() auth.OIDCProvider {
	return auth.OIDCProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer: "https://accounts.google.com",
		},
		Name:          "Amazon",
		CallbackURL:   &callbackURL,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoValidationKey() auth.OIDCProvider {
	return auth.OIDCProvider{
		Name:        "Yahoo",
		CallbackURL: &callbackURL,
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:   "https://accounts.yahoo.com",
			ClientID: base.StringPtr(clientID),
		},
	}
}

func mockOIDCOptions() *auth.OIDCOptions {
	provider := mockOIDCProvider()
	providers := auth.OIDCProviderMap{provider.Name: &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func mockOIDCOptionsWithMultipleProviders() *auth.OIDCOptions {
	differentProvider := "Couchbase"
	provider1 := mockOIDCProvider()
	provider2 := mockOIDCProvider()
	providers := auth.OIDCProviderMap{provider1.Name: &provider1}
	provider2.Name = "Youtube"
	providers[provider2.Name] = &provider2
	return &auth.OIDCOptions{DefaultProvider: &differentProvider, Providers: providers}
}

func mockOIDCOptionsWithMultipleProvidersCBQ() *auth.OIDCOptions {
	differentProvider := "SalesForce"
	provider1 := mockOIDCProvider()
	provider2 := mockOIDCProviderWithCallbackURLQuery()
	providers := auth.OIDCProviderMap{provider1.Name: &provider1}
	provider2.Name = "Youtube"
	providers[provider2.Name] = &provider2
	return &auth.OIDCOptions{DefaultProvider: &differentProvider, Providers: providers}
}

func mockOIDCOptionsWithNoIss() *auth.OIDCOptions {
	provider := mockOIDCProviderWithNoIss()
	providers := auth.OIDCProviderMap{provider.Name: &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func mockOIDCOptionsWithNoClientID() *auth.OIDCOptions {
	provider := mockOIDCProviderWithNoClientID()
	providers := auth.OIDCProviderMap{provider.Name: &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func mockOIDCOptionsWithNoValidationKey() *auth.OIDCOptions {
	provider := mockOIDCProviderWithNoValidationKey()
	providers := auth.OIDCProviderMap{provider.Name: &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func mockOIDCOptionsWithBadName() *auth.OIDCOptions {
	provider := mockOIDCProvider()
	providers := auth.OIDCProviderMap{provider.Name + "_": &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func TestNewDatabaseContextWithOIDCProviderOptions(t *testing.T) {
	tests := []struct {
		name          string
		inputOptions  *auth.OIDCOptions
		expectedError string
	}{
		// Mock valid OIDCOptions
		{
			name:          "TestWithValidOptions",
			inputOptions:  mockOIDCOptions(),
			expectedError: "",
		},
		// If Validation Key not defined in config for provider, it should warn auth code flow will not be
		// supported for this provider.
		{
			name:          "TestWithNoValidationKey",
			inputOptions:  mockOIDCOptionsWithNoValidationKey(),
			expectedError: "",
		},

		// Input to simulate the scenario where the current provider is the default provider, or there's are
		// providers defined, don't set IsDefault.
		{
			name:          "TestWithMultipleProviders",
			inputOptions:  mockOIDCOptionsWithMultipleProviders(),
			expectedError: "",
		},
		// Input to simulate the scenario where the current provider isn't the default provider, add the provider
		// to the callback URL (needed to identify provider to _oidc_callback)
		{
			name:          "TestWithMultipleProvidersAndCustomCallbackURL",
			inputOptions:  mockOIDCOptionsWithMultipleProvidersCBQ(),
			expectedError: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			options := DatabaseContextOptions{
				OIDCOptions: tc.inputOptions,
			}
			AddOptionsFromEnvironmentVariables(&options)
			ctx := base.TestCtx(t)
			dbCtx, err := NewDatabaseContext(ctx, "db", base.GetTestBucket(t), false, options)
			assert.NoError(t, err, "Couldn't create context for database 'db'")
			defer dbCtx.Close(ctx)
			assert.NotNil(t, dbCtx, "Database context should be created")

			database, err := CreateDatabase(dbCtx)
			assert.NotNil(t, database, "Database should be created with context options")
			assert.NoError(t, err, "Couldn't create database 'db'")
		})
	}
}

func TestGetOIDCProvider(t *testing.T) {
	options := DatabaseContextOptions{OIDCOptions: mockOIDCOptions()}
	context, ctx := SetupTestDBWithOptions(t, options)
	defer context.Close(ctx)

	// Lookup default provider by empty name, which exists in database context
	provider, err := context.GetOIDCProvider("")
	assert.Equal(t, defaultProvider, provider.Name)
	assert.Equal(t, defaultProvider, *options.OIDCOptions.DefaultProvider)
	log.Printf("%v", provider)

	// Lookup a provider by name which exists in database context.
	provider, err = context.GetOIDCProvider(defaultProvider)
	assert.Equal(t, defaultProvider, provider.Name)
	assert.Equal(t, defaultProvider, *options.OIDCOptions.DefaultProvider)
	log.Printf("%v", provider)

	// Lookup a provider which doesn't exists in database context
	provider, err = context.GetOIDCProvider("Unknown")
	assert.Nil(t, provider, "Provider doesn't exists in database context")
	assert.Contains(t, err.Error(), `No provider found for provider name "Unknown"`)
}

// TestSyncFnMutateBody ensures that any mutations made to the body by the sync function aren't persisted
func TestSyncFnMutateBody(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		doc.key1 = "mutatedValue"
		doc.key2.subkey1 = "mutatedSubValue"
		channel(doc.channels);
	}`, 0)

	// Create first revision:
	body := Body{"key1": "value1", "key2": Body{"subkey1": "subvalue1"}, "channels": []string{"public"}}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	rev, err := collection.GetRev(ctx, "doc1", rev1id, false, nil)
	revBody, err := rev.Body()
	require.NoError(t, err, "Couldn't get mutable body")
	assert.Equal(t, "value1", revBody["key1"])
	assert.Equal(t, map[string]interface{}{"subkey1": "subvalue1"}, revBody["key2"])
	log.Printf("rev: %s", rev.BodyBytes)

}

// Multiple clients are attempting to push the same new revision concurrently; first writer should be successful,
// subsequent writers should fail on CAS, and then identify that revision already exists on retry.
func TestConcurrentPushSameNewRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	var revId string
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Bob", "age": 52}
			collection := db.GetSingleDatabaseCollectionWithUser()
			revId, _, err := collection.Put(ctx, "doc1", body)
			assert.NoError(t, err, "Couldn't create document")
			assert.NotEmpty(t, revId)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	enableCallback = true

	body := Body{"name": "Bob", "age": 52}
	_, _, err := collection.Put(ctx, "doc1", body)
	require.Error(t, err)
	assert.Equal(t, "409 Document exists", err.Error())

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.Equal(t, revId, doc.RevID)
	assert.NoError(t, err, "Couldn't retrieve document")
	assert.Equal(t, "Bob", doc.Body()["name"])
	assert.Equal(t, json.Number("52"), doc.Body()["age"])
}

// Multiple clients are attempting to push the same new, non-winning revision concurrently; non-winning is an
// update to a non-winning branch that leaves the branch still non-winning (i.e. shorter) than the active branch
func TestConcurrentPushSameNewNonWinningRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Emily", "age": 20}
			collection := db.GetSingleDatabaseCollectionWithUser()
			_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b", "2-b", "1-a"}, false)
			assert.NoError(t, err, "Adding revision 3-b")
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Charlie", "age": 10}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 4-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-b")

	enableCallback = true

	body = Body{"name": "Emily", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b", "2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-b")

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc after adding 3-b")
	assert.Equal(t, "4-a", doc.CurrentRev)
}

// Multiple clients are attempting to push the same tombstone of the winning revision for a branched document
// First writer should be successful, subsequent writers should fail on CAS, then identify rev already exists
func TestConcurrentPushSameTombstoneWinningRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Charlie", "age": 10, BodyDeleted: true}
			collection := db.GetSingleDatabaseCollectionWithUser()
			_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
			assert.NoError(t, err, "Couldn't add revision 4-a (tombstone)")
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-b")

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc before tombstone")
	assert.Equal(t, "3-a", doc.CurrentRev)

	enableCallback = true

	body = Body{"name": "Charlie", "age": 10, BodyDeleted: true}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Couldn't add revision 4-a (tombstone)")

	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.CurrentRev)
}

// Multiple clients are attempting to push conflicting non-winning revisions; multiple clients pushing different
// updates to non-winning branches that leave the branch(es) non-winning.
func TestConcurrentPushDifferentUpdateNonWinningRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Joshua", "age": 11}
			collection := db.GetSingleDatabaseCollectionWithUser()
			_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b1", "2-b", "1-a"}, false)
			assert.NoError(t, err, "Couldn't add revision 3-b1")
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Charlie", "age": 10}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 4-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-b")

	enableCallback = true

	body = Body{"name": "Liam", "age": 12}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b2", "2-b", "1-a"}, false)
	assert.NoError(t, err, "Couldn't add revision 3-b2")

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc after adding 3-b")
	assert.Equal(t, "4-a", doc.CurrentRev)

	rev, err := collection.GetRev(ctx, "doc1", "3-b1", false, nil)
	assert.NoError(t, err, "Retrieve revision 3-b1")
	revBody, err := rev.Body()
	assert.NoError(t, err, "Retrieve body of revision 3-b1")
	assert.Equal(t, "Joshua", revBody["name"])
	assert.Equal(t, json.Number("11"), revBody["age"])

	rev, err = collection.GetRev(ctx, "doc1", "3-b2", false, nil)
	assert.NoError(t, err, "Retrieve revision 3-b2")
	revBody, err = rev.Body()
	assert.NoError(t, err, "Retrieve body of revision 3-b2")
	assert.Equal(t, "Liam", revBody["name"])
	assert.Equal(t, json.Number("12"), revBody["age"])
}

func TestIncreasingRecentSequences(t *testing.T) {
	var db *Database
	var enableCallback bool
	var body Body
	var revid string
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			// Write a doc
			collection := db.GetSingleDatabaseCollectionWithUser()
			_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-abc", revid}, true)
			assert.NoError(t, err)
		}
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), base.LeakyBucketConfig{UpdateCallback: writeUpdateCallback})
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	err := json.Unmarshal([]byte(`{"prop": "value"}`), &body)
	assert.NoError(t, err)

	// Create a doc
	revid, _, err = collection.Put(ctx, "doc1", body)
	assert.NoError(t, err)

	enableCallback = true
	doc, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-abc", "2-abc", revid}, true)
	assert.NoError(t, err)

	assert.True(t, sort.IsSorted(base.SortedUint64Slice(doc.SyncData.RecentSequences)))
}

func TestRepairUnorderedRecentSequences(t *testing.T) {
	if base.TestUseXattrs() {
		t.Skip("xattr=false only - test modifies doc _sync property")
	}

	var db *Database
	var body Body
	var revid string
	var ctx context.Context

	db, ctx = setupTestDB(t)
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	err := json.Unmarshal([]byte(`{"prop": "value"}`), &body)
	require.NoError(t, err)

	// Create a doc
	revid, _, err = collection.Put(ctx, "doc1", body)
	require.NoError(t, err)

	// Update the doc a few times to populate recent sequences
	for i := 0; i < 10; i++ {
		updateBody := make(map[string]interface{})
		updateBody["prop"] = i
		updateBody["_rev"] = revid
		revid, _, err = collection.Put(ctx, "doc1", updateBody)
		require.NoError(t, err)
	}

	syncData, err := collection.GetDocSyncData(ctx, "doc1")
	require.NoError(t, err)
	assert.True(t, sort.IsSorted(base.SortedUint64Slice(syncData.RecentSequences)))

	// Update document directly in the bucket to scramble recent sequences
	var rawBody Body
	_, err = collection.dataStore.Get("doc1", &rawBody)
	require.NoError(t, err)
	rawSyncData, ok := rawBody["_sync"].(map[string]interface{})
	require.True(t, ok)
	rawSyncData["recent_sequences"] = []uint64{3, 5, 9, 11, 1, 2, 4, 8, 7, 10, 5}
	assert.NoError(t, collection.dataStore.Set("doc1", 0, nil, rawBody))

	// Validate non-ordered
	var rawBodyCheck Body
	_, err = collection.dataStore.Get("doc1", &rawBodyCheck)
	require.NoError(t, err)
	log.Printf("raw body check %v", rawBodyCheck)
	rawSyncDataCheck, ok := rawBody["_sync"].(map[string]interface{})
	require.True(t, ok)
	recentSequences, ok := rawSyncDataCheck["recent_sequences"].([]uint64)
	require.True(t, ok)
	assert.False(t, sort.IsSorted(base.SortedUint64Slice(recentSequences)))

	// Update the doc again. expect sequences to now be ordered
	updateBody := make(map[string]interface{})
	updateBody["prop"] = 12
	updateBody["_rev"] = revid
	_, _, err = collection.Put(ctx, "doc1", updateBody)
	require.NoError(t, err)

	syncData, err = collection.GetDocSyncData(ctx, "doc1")
	require.NoError(t, err)
	assert.True(t, sort.IsSorted(base.SortedUint64Slice(syncData.RecentSequences)))
}

func TestDeleteWithNoTombstoneCreationSupport(t *testing.T) {

	// TODO: re-enable after adding ability to override bucket capabilities (CBG-1593)
	t.Skip("GoCB bucket required for cluster compatibility override")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Requires gocb bucket")
	}

	if !base.TestUseXattrs() {
		t.Skip("Xattrs required")
	}

	db, ctx := setupTestDBWithOptionsAndImport(t, DatabaseContextOptions{})
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	// gocbBucket, _ := base.AsGoCBBucket(db.Bucket)

	// Set something lower than version required for CreateAsDeleted subdoc flag
	// gocbBucket.OverrideClusterCompatVersion(5, 5)

	// Ensure empty doc is imported correctly
	added, err := collection.dataStore.Add("doc1", 0, map[string]interface{}{})
	assert.NoError(t, err)
	assert.True(t, added)

	waitAndAssertCondition(t, func() bool {
		return db.DbStats.SharedBucketImport().ImportCount.Value() == 1
	})

	// Ensure deleted doc with double operation isn't treated as import
	_, _, err = collection.Put(ctx, "doc", map[string]interface{}{"_deleted": true})
	assert.NoError(t, err)

	var doc Body
	var xattr Body

	// Ensure document has been added
	waitAndAssertCondition(t, func() bool {
		_, err = collection.dataStore.GetWithXattr("doc", "_sync", "", &doc, &xattr, nil)
		return err == nil
	})

	assert.Equal(t, int64(1), db.DbStats.SharedBucketImport().ImportCount.Value())

	assert.Nil(t, doc)
	assert.Equal(t, "1-2cac91faf7b3f5e5fd56ff377bdb5466", xattr["rev"])
	assert.Equal(t, float64(2), xattr["sequence"])
}

func TestResyncUpdateAllDocChannels(t *testing.T) {
	syncFn := `
	function(doc) {
		channel("x")
	}`

	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{QueryPaginationLimit: 5000})
	collection := db.GetSingleDatabaseCollectionWithUser()

	_, err := db.UpdateSyncFun(ctx, syncFn)
	assert.NoError(t, err)

	defer db.Close(ctx)

	for i := 0; i < 10; i++ {
		updateBody := make(map[string]interface{})
		updateBody["val"] = i
		_, _, err := collection.Put(ctx, fmt.Sprintf("doc%d", i), updateBody)
		require.NoError(t, err)
	}

	err = db.TakeDbOffline(ctx, "")
	assert.NoError(t, err)

	waitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&db.State)
		return state == DBOffline
	})

	_, err = collection.UpdateAllDocChannels(ctx, false, func(docsProcessed, docsChanged *int) {}, base.NewSafeTerminator())
	assert.NoError(t, err)

	syncFnCount := int(db.DbStats.Database().SyncFunctionCount.Value())
	assert.Equal(t, 20, syncFnCount)
}

func TestTombstoneCompactionStopWithManager(t *testing.T) {

	// FIXME (bbrks)
	t.Skip("leaky data store needs passing down into database to trigger callbacks properly")

	if !base.TestUseXattrs() {
		t.Skip("Compaction requires xattrs")
	}

	bucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{})
	db, ctx := SetupTestDBForDataStoreWithOptions(t, bucket, DatabaseContextOptions{})
	db.PurgeInterval = 0
	defer db.Close(ctx)
	collection := db.GetSingleDatabaseCollectionWithUser()

	for i := 0; i < 300; i++ {
		docID := fmt.Sprintf("doc%d", i)
		rev, _, err := collection.Put(ctx, docID, Body{})
		assert.NoError(t, err)
		_, err = collection.DeleteDoc(ctx, docID, rev)
		assert.NoError(t, err)
	}

	require.NoError(t, collection.WaitForPendingChanges(ctx))

	leakyDataStore, ok := base.AsLeakyDataStore(collection.dataStore)
	require.True(t, ok)

	queryCount := 0
	callbackFunc := func() {
		fmt.Println("leakyDataStore query callbackFunc")
		queryCount++
		if queryCount == 2 {
			assert.NoError(t, db.TombstoneCompactionManager.Stop())
		}
	}

	// FIXME (bbrks): These callbacks are not firing due to leakyDataStore being set in test and not inside SG
	if base.TestsDisableGSI() {
		leakyDataStore.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			callbackFunc()
		})
	} else {
		leakyDataStore.SetPostN1QLQueryCallback(func() {
			callbackFunc()
		})
	}

	assert.NoError(t, db.TombstoneCompactionManager.Start(ctx, map[string]interface{}{"database": db}))

	waitAndAssertConditionWithOptions(t, func() bool {
		return db.TombstoneCompactionManager.GetRunState(t) == BackgroundProcessStateStopped
	}, 60, 1000)

	var tombstoneCompactionStatus TombstoneManagerResponse
	status, err := db.TombstoneCompactionManager.GetStatus()
	assert.NoError(t, err)
	err = base.JSONUnmarshal(status, &tombstoneCompactionStatus)
	assert.NoError(t, err)

	// Ensure only 250 docs have been purged which is one iteration of querying - Means stop did terminate the compaction
	assert.Equal(t, QueryTombstoneBatch, int(tombstoneCompactionStatus.DocsPurged))
}

func TestGetAllUsers(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db, ctx := setupTestDB(t)
	db.Options.QueryPaginationLimit = 100
	defer db.Close(ctx)

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	log.Printf("Creating users...")
	// Create users
	authenticator := db.Authenticator(ctx)
	user, _ := authenticator.NewUser("userA", "letmein", channels.BaseSetOf(t, "ABC"))
	_ = user.SetEmail("userA@test.org")
	assert.NoError(t, authenticator.Save(user))
	user, _ = authenticator.NewUser("userB", "letmein", channels.BaseSetOf(t, "ABC"))
	_ = user.SetEmail("userB@test.org")
	assert.NoError(t, authenticator.Save(user))
	user, _ = authenticator.NewUser("userC", "letmein", channels.BaseSetOf(t, "ABC"))
	user.SetDisabled(true)
	assert.NoError(t, authenticator.Save(user))
	user, _ = authenticator.NewUser("userD", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, authenticator.Save(user))

	log.Printf("Getting users...")
	users, err := db.GetUsers(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(users))
	log.Printf("THE USERS: %+v", users)
	marshalled, err := json.Marshal(users)
	log.Printf("THE USERS MARSHALLED: %s", marshalled)

	limitedUsers, err := db.GetUsers(ctx, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(limitedUsers))
}

func TestGetRoleIDs(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	db.Options.QueryPaginationLimit = 100
	db.ChannelMapper = channels.NewDefaultChannelMapper()
	authenticator := db.Authenticator(ctx)

	rolename1 := uuid.NewString()
	rolename2 := uuid.NewString()
	username := uuid.NewString()

	user1, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err)
	user1.SetExplicitRoles(channels.TimedSet{rolename1: channels.NewVbSimpleSequence(1), rolename2: channels.NewVbSimpleSequence(1)}, 1)
	require.NoError(t, authenticator.Save(user1))

	role1, err := authenticator.NewRole(rolename1, nil)
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(role1))

	role2, err := authenticator.NewRole(rolename2, nil)
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(role2))

	err = db.DeleteRole(ctx, role2.Name(), false)
	require.NoError(t, err)
	roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
	require.NoError(t, err)
	assert.True(t, roleGet.IsDeleted())

	t.Log("user1:", user1.Name())
	t.Log("role1:", role1.Name())
	t.Log("role2:", role2.Name())

	// assert allprincipals still returns users and deleted roles
	users, roles, err := db.AllPrincipalIDs(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{user1.Name()}, users)
	assert.ElementsMatch(t, []string{role1.Name(), role2.Name()}, roles)

	roles, err = db.GetRoleIDs(ctx, db.UseViews(), false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{role1.Name()}, roles)
}

// Regression test for CBG-2058.
func TestImportCompactPanic(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("requires xattrs")
	}

	// Set the compaction and purge interval unrealistically low to reproduce faster
	db, ctx := setupTestDBWithOptionsAndImport(t, DatabaseContextOptions{
		CompactInterval: 1,
	})
	defer db.Close(ctx)
	db.PurgeInterval = 0
	collection := db.GetSingleDatabaseCollectionWithUser()

	// Create a document, then delete it, to create a tombstone
	rev, doc, err := collection.Put(ctx, "test", Body{})
	require.NoError(t, err)
	_, err = collection.DeleteDoc(ctx, doc.ID, rev)
	require.NoError(t, err)
	require.NoError(t, collection.WaitForPendingChanges(ctx))

	// Wait for Compact to run - in the failing case it'll panic before incrementing the stat
	_, ok := base.WaitForStat(func() int64 {
		return db.DbStats.Database().NumTombstonesCompacted.Value()
	}, 1)
	require.True(t, ok)
}

func TestGetDatabaseCollectionWithUserScopesNil(t *testing.T) {
	testCases := []struct {
		scope      string
		collection string
	}{
		{
			scope:      "",
			collection: "",
		},
		{
			scope:      "foo",
			collection: "bar",
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s.%s", testCase.scope, testCase.collection), func(t *testing.T) {

			db := Database{DatabaseContext: &DatabaseContext{}}
			col, err := db.GetDatabaseCollectionWithUser(testCase.scope, testCase.collection)
			require.Error(t, err)
			require.Nil(t, col)
		})
	}
}

func TestGetDatabaseCollectionWithUserNoScopesConfigured(t *testing.T) {
	testCases := []struct {
		scope      string
		collection string
	}{
		{
			scope:      "",
			collection: "",
		},
		{
			scope:      "foo",
			collection: "bar",
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s.%s", testCase.scope, testCase.collection), func(t *testing.T) {

			db := Database{DatabaseContext: &DatabaseContext{}}
			col, err := db.GetDatabaseCollectionWithUser(testCase.scope, testCase.collection)
			require.Error(t, err)
			require.Nil(t, col)
		})
	}
}

func TestGetDatabaseCollectionWithUserDefaultCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	ds := bucket.GetNamedDataStore()
	require.NotNil(t, ds)

	dataStoreName, ok := base.AsDataStoreName(ds)
	require.True(t, ok)

	testCases := []struct {
		name       string
		scope      string
		collection string
		err        bool
		options    DatabaseContextOptions
	}{
		{
			name:       "emptyscopecollection",
			scope:      "",
			collection: "",
			err:        true,
		},
		{
			name:       "foo.bar-notconfigured",
			scope:      "foo",
			collection: "bar",
			err:        true,
		},
		{
			name:       "_default._default-noconfiguration",
			scope:      base.DefaultScope,
			collection: base.DefaultCollection,
			err:        false,
		},
		/* CBG-2568 This test passes under walrus/views but not GSI, needs to be fixed when making walrus collection aware.
		{
			name:       "_default._default-not-in-config",
			scope:      base.DefaultScope,
			collection: base.DefaultCollection,
			err:        true,
			options: DatabaseContextOptions{
				Scopes: map[string]ScopeOptions{
					"foo": ScopeOptions{
						Collections: map[string]CollectionOptions{
							"bar": {},
						},
					},
				},
			},
		},
		*/
		{
			name:       "_default._default-inconfig",
			scope:      base.DefaultScope,
			collection: base.DefaultCollection,
			err:        false,
			options: DatabaseContextOptions{
				Scopes: map[string]ScopeOptions{
					dataStoreName.ScopeName(): ScopeOptions{
						Collections: map[string]CollectionOptions{
							dataStoreName.CollectionName(): {},
						},
					},
					base.DefaultScope: ScopeOptions{
						Collections: map[string]CollectionOptions{
							base.DefaultCollection: {},
						},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf(testCase.name), func(t *testing.T) {
			ctx := base.TestCtx(t)
			dbCtx, err := NewDatabaseContext(ctx, "db", bucket.NoCloseClone(), false, testCase.options)
			require.NoError(t, err)

			db, err := GetDatabase(dbCtx, nil)
			defer db.Close(ctx)
			require.NoError(t, err)
			col, err := db.GetDatabaseCollectionWithUser(testCase.scope, testCase.collection)
			if testCase.err {
				require.Error(t, err)
				require.Nil(t, col)
			} else {
				require.NoError(t, err)
				require.NotNil(t, col)
				require.Equal(t, col.ScopeName(), testCase.scope)
				require.Equal(t, col.Name(), testCase.collection)
			}

		})
	}

}

func waitAndAssertConditionWithOptions(t *testing.T, fn func() bool, retryCount, msSleepTime int, failureMsgAndArgs ...interface{}) {
	for i := 0; i <= retryCount; i++ {
		if i == retryCount {
			assert.Fail(t, "Condition failed to be satisfied", failureMsgAndArgs...)
		}
		if fn() {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(msSleepTime))
	}
}

func waitAndAssertCondition(t *testing.T, fn func() bool, failureMsgAndArgs ...interface{}) {
	waitAndAssertConditionWithOptions(t, fn, 20, 100, failureMsgAndArgs...)
}
