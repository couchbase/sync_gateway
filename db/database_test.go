//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/robertkrimen/otto/underscore"
	"github.com/stretchr/testify/assert"
)

func init() {
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

// Note: It is important to call db.Close() on the returned database.
func setupTestDB(t testing.TB) *Database {
	return setupTestDBWithCacheOptions(t, DefaultCacheOptions())
}

// Sets up test db with the specified database context options.  Note that environment variables can
// override somedbcOptions properties.
func setupTestDBWithOptions(t testing.TB, dbcOptions DatabaseContextOptions) *Database {

	AddOptionsFromEnvironmentVariables(&dbcOptions)
	tBucket := base.GetTestBucket(t)
	context, err := NewDatabaseContext("db", tBucket, false, dbcOptions)
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")
	return db
}

func setupTestDBWithCacheOptions(t testing.TB, options CacheOptions) *Database {

	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	return setupTestDBWithOptions(t, dbcOptions)
}

// Forces UseViews:true in the database context.  Useful for testing w/ views while running
// tests against Couchbase Server
func setupTestDBWithViewsEnabled(t testing.TB) *Database {

	dbcOptions := DatabaseContextOptions{
		UseViews: true,
	}
	return setupTestDBWithOptions(t, dbcOptions)
}

// Sets up a test bucket with _sync:seq initialized to a high value prior to database creation.  Used to test
// issues with custom _sync:seq values without triggering skipped sequences between 0 and customSeq
func setupTestDBWithCustomSyncSeq(t testing.TB, customSeq uint64) *Database {

	dbcOptions := DatabaseContextOptions{}
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	tBucket := base.GetTestBucket(t)

	log.Printf("Initializing test %s to %d", base.SyncSeqPrefix, customSeq)
	_, incrErr := tBucket.Incr(base.SyncSeqKey, customSeq, customSeq, 0)
	assert.NoError(t, incrErr, fmt.Sprintf("Couldn't increment %s seq by %d", base.SyncSeqPrefix, customSeq))

	context, err := NewDatabaseContext("db", tBucket, false, dbcOptions)
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")
	return db
}

func setupTestLeakyDBWithCacheOptions(t *testing.T, options CacheOptions, leakyOptions base.LeakyBucketConfig) *Database {
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	testBucket := base.GetTestBucket(t)
	leakyBucket := base.NewLeakyBucket(testBucket, leakyOptions)
	context, err := NewDatabaseContext("db", leakyBucket, false, dbcOptions)
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")
	return db
}

// If certain environemnt variables are set, for example to turn on XATTR support, then update
// the DatabaseContextOptions accordingly
func AddOptionsFromEnvironmentVariables(dbcOptions *DatabaseContextOptions) {
	if base.TestUseXattrs() {
		dbcOptions.EnableXattr = true
	}

	if base.TestsDisableGSI() {
		dbcOptions.UseViews = true
	}
}

func assertHTTPError(t *testing.T, err error, status int) {
	httpErr, ok := err.(*base.HTTPError)
	if !ok {
		goassert.Errorf(t, "assertHTTPError: Expected an HTTP %d; got error %T %v", status, err, err)
	} else {
		goassert.Equals(t, httpErr.Status, status)
	}
}

func TestDatabase(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	db := setupTestDB(t)
	defer db.Close()

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, doc, err := db.Put("doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = rev1id
	assert.NoError(t, err, "Couldn't create document")
	goassert.Equals(t, rev1id, "1-cb0c9a22be0e5a1b01084ec019defa81")

	log.Printf("Create rev 2...")
	body["key1"] = "new value"
	body["key2"] = int64(4321)
	rev2id, _, err := db.Put("doc1", body)
	body[BodyId] = "doc1"
	body[BodyRev] = rev2id
	assert.NoError(t, err, "Couldn't update document")
	goassert.Equals(t, rev2id, "2-488724414d0ed6b398d6d2aeb228d797")

	// Retrieve the document:
	log.Printf("Retrieve doc...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	AssertEqualBodies(t, body, gotbody)

	log.Printf("Retrieve rev 1...")
	gotbody, err = db.Get1xRevBody("doc1", rev1id, false, nil)
	assert.NoError(t, err, "Couldn't get document with rev 1")
	expectedResult := Body{"key1": "value1", "key2": 1234, BodyId: "doc1", BodyRev: rev1id}
	AssertEqualBodies(t, expectedResult, gotbody)

	log.Printf("Retrieve rev 2...")
	gotbody, err = db.Get1xRevBody("doc1", rev2id, false, nil)
	assert.NoError(t, err, "Couldn't get document with rev")
	AssertEqualBodies(t, body, gotbody)

	gotbody, err = db.Get1xRevBody("doc1", "bogusrev", false, nil)
	status, _ := base.ErrorAsHTTPStatus(err)
	goassert.Equals(t, status, 404)

	// Test the _revisions property:
	log.Printf("Check _revisions...")
	gotbody, err = db.Get1xRevBody("doc1", rev2id, true, nil)
	revisions := gotbody[BodyRevisions].(Revisions)
	goassert.Equals(t, revisions[RevisionsStart], 2)
	goassert.DeepEquals(t, revisions[RevisionsIds],
		[]string{"488724414d0ed6b398d6d2aeb228d797",
			"cb0c9a22be0e5a1b01084ec019defa81"})

	// Test RevDiff:
	log.Printf("Check RevDiff...")
	missing, possible := db.RevDiff("doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"2-488724414d0ed6b398d6d2aeb228d797"})
	goassert.True(t, missing == nil)
	goassert.True(t, possible == nil)

	missing, possible = db.RevDiff("doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	goassert.DeepEquals(t, missing, []string{"3-foo"})
	goassert.DeepEquals(t, possible, []string{"2-488724414d0ed6b398d6d2aeb228d797"})

	missing, possible = db.RevDiff("nosuchdoc",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	goassert.DeepEquals(t, missing, []string{"1-cb0c9a22be0e5a1b01084ec019defa81",
		"3-foo"})
	goassert.True(t, possible == nil)

	// Test PutExistingRev:
	log.Printf("Check PutExistingRev...")
	body[BodyRev] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		"1-cb0c9a22be0e5a1b01084ec019defa81"}
	doc, newRev, err := db.PutExistingRevWithBody("doc1", body, history, false)
	body[BodyId] = doc.ID
	body[BodyRev] = newRev
	assert.NoError(t, err, "PutExistingRev failed")

	// Retrieve the document:
	log.Printf("Check Get...")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	AssertEqualBodies(t, body, gotbody)

}

func TestGetDeleted(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	body := Body{"key1": 1234}
	rev1id, _, err := db.Put("doc1", body)
	assert.NoError(t, err, "Put")

	rev2id, err := db.DeleteDoc("doc1", rev1id)
	assert.NoError(t, err, "DeleteDoc")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = db.Get1xRevBody("doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	expectedResult := Body{
		BodyId:        "doc1",
		BodyRev:       rev2id,
		BodyDeleted:   true,
		BodyRevisions: Revisions{RevisionsStart: 2, RevisionsIds: []string{"bc6d97f6e97c0d034a34f8aac2bf8b44", "dfd5e19813767eeddd08270fc5f385cd"}},
	}
	AssertEqualBodies(t, expectedResult, body)

	// Get the raw doc and make sure the sync data has the current revision
	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Err getting doc")
	goassert.Equals(t, doc.SyncData.CurrentRev, rev2id)

	// Try again but with a user who doesn't have access to this revision (see #179)
	authenticator := auth.NewAuthenticator(db.Bucket, db)
	db.user, err = authenticator.GetUser("")
	assert.NoError(t, err, "GetUser")
	db.user.SetExplicitChannels(nil)

	body, err = db.Get1xRevBody("doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	AssertEqualBodies(t, expectedResult, body)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemovedAsUser(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, _, err := db.Put("doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRev:    rev1id,
	}
	rev2id, _, err := db.Put("doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = db.Put("doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := db.Get1xRevBody("doc1", rev2id, true, nil)
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
	cacheHitCounter, cacheMissCounter := db.DatabaseContext.DbStats.NewStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.NewStats.Cache().RevisionCacheMisses
	db.DatabaseContext.revisionCache = NewShardedLRURevisionCache(DefaultRevisionCacheShardCount, DefaultRevisionCacheSize, db.DatabaseContext, cacheHitCounter, cacheMissCounter)
	err = db.purgeOldRevisionJSON("doc1", rev2id)
	assert.NoError(t, err, "Purge old revision JSON")

	// Try again with a user who doesn't have access to this revision
	authenticator := auth.NewAuthenticator(db.Bucket, db)
	db.user, err = authenticator.GetUser("")
	assert.NoError(t, err, "GetUser")

	var chans channels.TimedSet
	chans = channels.AtSequence(base.SetOf("ABC"), 1)
	db.user.SetExplicitChannels(chans)

	// Get the removal revision with its history; equivalent to GET with ?revs=true
	body, err = db.Get1xRevBody("doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	expectedResult = Body{
		BodyId:      "doc1",
		BodyRev:     rev2id,
		BodyRemoved: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
	}
	goassert.DeepEquals(t, body, expectedResult)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	err = db.purgeOldRevisionJSON("doc1", rev1id)
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = db.Get1xRevBody("doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemoved(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, _, err := db.Put("doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRev:    rev1id,
	}
	rev2id, _, err := db.Put("doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = db.Put("doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := db.Get1xRevBody("doc1", rev2id, true, nil)
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
	cacheHitCounter, cacheMissCounter := db.DatabaseContext.DbStats.NewStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.NewStats.Cache().RevisionCacheMisses
	db.DatabaseContext.revisionCache = NewShardedLRURevisionCache(DefaultRevisionCacheShardCount, DefaultRevisionCacheSize, db.DatabaseContext, cacheHitCounter, cacheMissCounter)
	err = db.purgeOldRevisionJSON("doc1", rev2id)
	assert.NoError(t, err, "Purge old revision JSON")

	// Get the removal revision with its history; equivalent to GET with ?revs=true
	body, err = db.Get1xRevBody("doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	expectedResult = Body{
		BodyId:      "doc1",
		BodyRev:     rev2id,
		BodyRemoved: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
	}
	goassert.DeepEquals(t, body, expectedResult)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	err = db.purgeOldRevisionJSON("doc1", rev1id)
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = db.Get1xRevBody("doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemovedAndDeleted(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, _, err := db.Put("doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":      1234,
		BodyDeleted: true,
		BodyRev:     rev1id,
	}
	rev2id, _, err := db.Put("doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = db.Put("doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := db.Get1xRevBody("doc1", rev2id, true, nil)
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
	cacheHitCounter, cacheMissCounter := db.DatabaseContext.DbStats.NewStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.NewStats.Cache().RevisionCacheMisses
	db.DatabaseContext.revisionCache = NewShardedLRURevisionCache(DefaultRevisionCacheShardCount, DefaultRevisionCacheSize, db.DatabaseContext, cacheHitCounter, cacheMissCounter)
	err = db.purgeOldRevisionJSON("doc1", rev2id)
	assert.NoError(t, err, "Purge old revision JSON")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = db.Get1xRevBody("doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	expectedResult = Body{
		BodyId:      "doc1",
		BodyRev:     rev2id,
		BodyRemoved: true,
		BodyDeleted: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
	}
	AssertEqualBodies(t, expectedResult, body)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	err = db.purgeOldRevisionJSON("doc1", rev1id)
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = db.Get1xRevBody("doc1", rev1id, true, nil)
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

func allDocIDs(db *Database) (docs []AllDocsEntry, err error) {
	err = db.ForEachDocID(func(doc IDRevAndSequence, channels []string) (bool, error) {
		docs = append(docs, AllDocsEntry{
			IDRevAndSequence: doc,
			Channels:         channels,
		})
		return true, nil
	}, options)
	return
}

func TestAllDocsOnly(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyCache)()

	// Lower the log max length so no more than 50 items will be kept.
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = 50

	db := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Trigger creation of the channel cache for channel "all"
	db.changeCache.getChannelCache().getSingleChannelCache("all")

	ids := make([]AllDocsEntry, 100)
	for i := 0; i < 100; i++ {
		channels := []string{"all"}
		if i%10 == 0 {
			channels = append(channels, "KFJC")
		}
		body := Body{"serialnumber": int64(i), "channels": channels}
		ids[i].DocID = fmt.Sprintf("alldoc-%02d", i)
		revid, _, err := db.Put(ids[i].DocID, body)
		ids[i].RevID = revid
		ids[i].Sequence = uint64(i + 1)
		ids[i].Channels = channels
		assert.NoError(t, err, "Couldn't create document")
	}

	alldocs, err := allDocIDs(db)
	assert.NoError(t, err, "AllDocIDs failed")
	require.Len(t, alldocs, 100)
	for i, entry := range alldocs {
		assert.True(t, entry.Equal(ids[i]))
	}

	// Now delete one document and try again:
	_, err = db.DeleteDoc(ids[23].DocID, ids[23].RevID)
	assert.NoError(t, err, "Couldn't delete doc 23")

	alldocs, err = allDocIDs(db)
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
	err = db.changeCache.waitForSequence(context.TODO(), 101, base.DefaultWaitForSequence)
	require.NoError(t, err)
	changeLog := db.GetChangeLog("all", 0)
	require.Len(t, changeLog, 50)
	assert.Equal(t, "alldoc-51", changeLog[0].DocID)

	// Now check the changes feed:
	var options ChangesOptions
	options.Terminator = make(chan bool)
	defer close(options.Terminator)
	changes, err := db.GetChanges(channels.SetOf(t, "all"), options)
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
			assert.Equal(t, channels.SetOf(t, "all"), change.Removed)
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
	changes, err = db.GetChanges(channels.SetOf(t, "KFJC"), options)
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

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache, base.KeyChanges)()

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf(t, "ABC"))
	assert.NoError(t, authenticator.Save(user))

	// Validate that a call to UpdatePrincipals with no changes to the user doesn't allocate a sequence
	userInfo, err := db.GetPrincipal("naomi", true)
	userInfo.ExplicitChannels = base.SetOf("ABC")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assert.NoError(t, err, "Unable to update principal")

	nextSeq, err := db.sequences.nextSequence()
	goassert.Equals(t, nextSeq, uint64(1))

	// Validate that a call to UpdatePrincipals with changes to the user does allocate a sequence
	userInfo, err = db.GetPrincipal("naomi", true)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assert.NoError(t, err, "Unable to update principal")

	nextSeq, err = db.sequences.nextSequence()
	goassert.Equals(t, nextSeq, uint64(3))
}

// Re-apply one of the conflicting changes to make sure that PutExistingRevWithBody() treats it as a no-op (SG Issue #3048)
func TestRepeatedConflict(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := db.PutExistingRevWithBody("doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")

	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	_, newRev, err := db.PutExistingRevWithBody("doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Get the _rev that was set in the body by PutExistingRevWithBody() and make assertions on it
	revGen, _ := ParseRevID(newRev)
	goassert.Equals(t, revGen, 2)

	// Remove the _rev key from the body, and call PutExistingRevWithBody() again, which should re-add it
	delete(body, BodyRev)
	_, newRev, err = db.PutExistingRevWithBody("doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err)

	// The _rev should pass the same assertions as before, since PutExistingRevWithBody() should re-add it
	revGen, _ = ParseRevID(newRev)
	goassert.Equals(t, revGen, 2)

}

func TestConflicts(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Instantiate channel cache for channel 'all'
	db.changeCache.getChannelCache().getSingleChannelCache("all")

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := db.PutExistingRevWithBody("doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Wait for rev to be cached
	cacheWaiter.AddAndWait(1)

	changeLog := db.GetChangeLog("all", 0)
	goassert.Equals(t, len(changeLog), 1)

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	cacheWaiter.Add(2)

	rawBody, _, _ := db.Bucket.GetRaw("doc")

	log.Printf("got raw body: %s", rawBody)

	// Verify the change with the higher revid won:
	gotBody, err := db.Get1xBody("doc")
	expectedResult := Body{BodyId: "doc", BodyRev: "2-b", "n": 2, "channels": []string{"all", "2b"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify we can still get the other two revisions:
	gotBody, err = db.Get1xRevBody("doc", "1-a", false, nil)
	expectedResult = Body{BodyId: "doc", BodyRev: "1-a", "n": 1, "channels": []string{"all", "1"}}
	AssertEqualBodies(t, expectedResult, gotBody)
	gotBody, err = db.Get1xRevBody("doc", "2-a", false, nil)
	expectedResult = Body{BodyId: "doc", BodyRev: "2-a", "n": 3, "channels": []string{"all", "2a"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify the change-log of the "all" channel:
	cacheWaiter.Wait()
	changeLog = db.GetChangeLog("all", 0)
	goassert.Equals(t, len(changeLog), 1)
	goassert.Equals(t, changeLog[0].Sequence, uint64(3))
	goassert.Equals(t, changeLog[0].DocID, "doc")
	goassert.Equals(t, changeLog[0].RevID, "2-b")
	goassert.Equals(t, changeLog[0].Flags, uint8(channels.Hidden|channels.Branched|channels.Conflict))

	// Verify the _changes feed:
	options := ChangesOptions{
		Conflicts: true,
	}
	changes, err := db.GetChanges(channels.SetOf(t, "all"), options)
	assert.NoError(t, err, "Couldn't GetChanges")
	goassert.Equals(t, len(changes), 1)
	goassert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:      SequenceID{Seq: 3},
		ID:       "doc",
		Changes:  []ChangeRev{{"rev": "2-b"}, {"rev": "2-a"}},
		branched: true})

	// Delete 2-b; verify this makes 2-a current:
	rev3, err := db.DeleteDoc("doc", "2-b")
	assert.NoError(t, err, "delete 2-b")

	rawBody, _, _ = db.Bucket.GetRaw("doc")
	log.Printf("post-delete, got raw body: %s", rawBody)

	gotBody, err = db.Get1xBody("doc")
	expectedResult = Body{BodyId: "doc", BodyRev: "2-a", "n": 3, "channels": []string{"all", "2a"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify channel assignments are correct for channels defined by 2-a:
	doc, _ := db.GetDocument("doc", DocUnmarshalAll)
	chan2a, found := doc.Channels["2a"]
	goassert.True(t, found)
	goassert.True(t, chan2a == nil)             // currently in 2a
	goassert.True(t, doc.Channels["2b"] != nil) // has been removed from 2b

	// Wait for delete mutation to arrive over feed
	cacheWaiter.AddAndWait(1)

	// Verify the _changes feed:
	changes, err = db.GetChanges(channels.SetOf(t, "all"), options)
	assert.NoError(t, err, "Couldn't GetChanges")
	goassert.Equals(t, len(changes), 1)
	goassert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:      SequenceID{Seq: 4},
		ID:       "doc",
		Changes:  []ChangeRev{{"rev": "2-a"}, {"rev": rev3}},
		branched: true})
}

func TestConflictRevLimit(t *testing.T) {

	//Test Default Is the higher of the two
	db := setupTestDB(t)
	assert.Equal(t, uint32(DefaultRevsLimitConflicts), db.RevsLimit)
	db.Close()

	//Test AllowConflicts
	dbOptions := DatabaseContextOptions{
		AllowConflicts: base.BoolPtr(true),
	}

	db = setupTestDBWithOptions(t, dbOptions)
	assert.Equal(t, uint32(DefaultRevsLimitConflicts), db.RevsLimit)
	db.Close()

	//Test AllowConflicts false
	dbOptions = DatabaseContextOptions{
		AllowConflicts: base.BoolPtr(false),
	}

	db = setupTestDBWithOptions(t, dbOptions)
	assert.Equal(t, uint32(DefaultRevsLimitNoConflicts), db.RevsLimit)
	db.Close()

}

func TestNoConflictsMode(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()
	// Strictly speaking, this flag should be set before opening the database, but it only affects
	// Put operations and replication, so it doesn't make a difference if we do it afterwards.
	db.Options.AllowConflicts = base.BoolPtr(false)

	// Create revs 1 and 2 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := db.PutExistingRevWithBody("doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	body["n"] = 2
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Try to create a conflict branching from rev 1:
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-b", "1-a"}, false)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with no common ancestor:
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-c", "1-c"}, false)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with a longer history:
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"4-d", "3-d", "2-d", "1-a"}, false)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with no history:
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"1-e"}, false)
	assertHTTPError(t, err, 409)

	// Create a non-conflict with a longer history, ending in a deletion:
	body[BodyDeleted] = true
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "add 4-a")
	delete(body, BodyDeleted)

	// Create a non-conflict with no history (re-creating the document, but with an invalid rev):
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"1-f"}, false)
	assertHTTPError(t, err, 409)

	// Resurrect the tombstoned document with a valid history
	_, _, err = db.PutExistingRevWithBody("doc", body, []string{"5-f", "4-a"}, false)
	assert.NoError(t, err, "add 5-f")
	delete(body, BodyDeleted)

	// Create a new document with a longer history:
	_, _, err = db.PutExistingRevWithBody("COD", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "add COD")
	delete(body, BodyDeleted)

	// Now use Put instead of PutExistingRev:

	// Successfully add a new revision:
	_, _, err = db.Put("doc", Body{BodyRev: "5-f", "foo": -1})
	assert.NoError(t, err, "Put rev after 1-f")

	// Try to create a conflict:
	_, _, err = db.Put("doc", Body{BodyRev: "3-a", "foo": 7})
	assertHTTPError(t, err, 409)

	// Conflict with no ancestry:
	_, _, err = db.Put("doc", Body{"foo": 7})
	assertHTTPError(t, err, 409)
}

// Test tombstoning of existing conflicts after AllowConflicts is set to false via Put
func TestAllowConflictsFalseTombstoneExistingConflict(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create documents with multiple non-deleted branches
	log.Printf("Creating docs")
	body := Body{"n": 1}
	doc, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	doc, _, err = db.PutExistingRevWithBody("doc2", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	doc, _, err = db.PutExistingRevWithBody("doc3", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	doc, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	doc, _, err = db.PutExistingRevWithBody("doc2", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	doc, _, err = db.PutExistingRevWithBody("doc3", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	doc, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	doc, _, err = db.PutExistingRevWithBody("doc2", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	doc, _, err = db.PutExistingRevWithBody("doc3", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Set AllowConflicts to false
	db.Options.AllowConflicts = base.BoolPtr(false)

	// Attempt to tombstone a non-leaf node of a conflicted document
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-c", "1-a"}, false)
	assert.True(t, err != nil, "expected error tombstoning non-leaf")

	// Tombstone the non-winning branch of a conflicted document
	body[BodyRev] = "2-a"
	body[BodyDeleted] = true
	tombstoneRev, _, putErr := db.Put("doc1", body)
	assert.NoError(t, putErr, "tombstone 2-a")
	doc, err = db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	goassert.Equals(t, doc.CurrentRev, "2-b")

	// Attempt to add a tombstone rev w/ the previous tombstone as parent
	body[BodyRev] = tombstoneRev
	body[BodyDeleted] = true
	_, _, putErr = db.Put("doc1", body)
	assert.True(t, putErr != nil, "Expect error tombstoning a tombstone")

	// Tombstone the winning branch of a conflicted document
	body[BodyRev] = "2-b"
	body[BodyDeleted] = true
	_, _, putErr = db.Put("doc2", body)
	assert.NoError(t, putErr, "tombstone 2-b")
	doc, err = db.GetDocument("doc2", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	goassert.Equals(t, doc.CurrentRev, "2-a")

	// Set revs_limit=1, then tombstone non-winning branch of a conflicted document.  Validate retrieval still works.
	db.RevsLimit = uint32(1)
	body[BodyRev] = "2-a"
	body[BodyDeleted] = true
	_, _, putErr = db.Put("doc3", body)
	assert.NoError(t, putErr, "tombstone 2-a w/ revslimit=1")
	doc, err = db.GetDocument("doc3", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	goassert.Equals(t, doc.CurrentRev, "2-b")

	log.Printf("tombstoned conflicts: %+v", doc)

}

// Test tombstoning of existing conflicts after AllowConflicts is set to false via PutExistingRev
func TestAllowConflictsFalseTombstoneExistingConflictNewEditsFalse(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create documents with multiple non-deleted branches
	log.Printf("Creating docs")
	body := Body{"n": 1}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	_, _, err = db.PutExistingRevWithBody("doc2", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")
	_, _, err = db.PutExistingRevWithBody("doc3", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	_, _, err = db.PutExistingRevWithBody("doc2", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	_, _, err = db.PutExistingRevWithBody("doc3", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	_, _, err = db.PutExistingRevWithBody("doc2", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	_, _, err = db.PutExistingRevWithBody("doc3", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Set AllowConflicts to false
	db.Options.AllowConflicts = base.BoolPtr(false)
	delete(body, "n")

	// Attempt to tombstone a non-leaf node of a conflicted document
	body[BodyDeleted] = true
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-c", "1-a"}, false)
	assert.True(t, err != nil, "expected error tombstoning non-leaf")

	// Tombstone the non-winning branch of a conflicted document
	body[BodyDeleted] = true
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a (tombstone)")
	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	goassert.Equals(t, doc.CurrentRev, "2-b")

	// Tombstone the winning branch of a conflicted document
	body[BodyDeleted] = true
	_, _, err = db.PutExistingRevWithBody("doc2", body, []string{"3-b", "2-b"}, false)
	assert.NoError(t, err, "add 3-b (tombstone)")
	doc, err = db.GetDocument("doc2", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	goassert.Equals(t, doc.CurrentRev, "2-a")

	// Set revs_limit=1, then tombstone non-winning branch of a conflicted document.  Validate retrieval still works.
	body[BodyDeleted] = true
	db.RevsLimit = uint32(1)
	_, _, err = db.PutExistingRevWithBody("doc3", body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a (tombstone)")
	doc, err = db.GetDocument("doc3", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	goassert.Equals(t, doc.CurrentRev, "2-b")

	log.Printf("tombstoned conflicts: %+v", doc)
}

func TestSyncFnOnPush(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		log("doc _id = "+doc._id+", _rev = "+doc._rev);
		if (oldDoc)
			log("oldDoc _id = "+oldDoc._id+", _rev = "+oldDoc._rev);
		channel(doc.channels);
	}`)

	// Create first revision:
	body := Body{"key1": "value1", "key2": 1234, "channels": []string{"public"}}
	rev1id, _, err := db.Put("doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	// Add several revisions at once to a doc, as on a push:
	log.Printf("Check PutExistingRev...")
	body[BodyRev] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	body["channels"] = "clibup"
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		rev1id}
	_, _, err = db.PutExistingRevWithBody("doc1", body, history, false)
	assert.NoError(t, err, "PutExistingRev failed")

	// Check that the doc has the correct channel (test for issue #300)
	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	goassert.DeepEquals(t, doc.Channels, channels.ChannelMap{
		"clibup": nil, // i.e. it is currently in this channel (no removal)
		"public": &channels.ChannelRemoval{Seq: 2, RevID: "4-four"},
	})
	goassert.DeepEquals(t, doc.History["4-four"].Channels, base.SetOf("clibup"))
}

func TestInvalidChannel(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	body := Body{"channels": []string{"bad,name"}}
	_, _, err := db.Put("doc", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionValidation(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)

	body := Body{"users": []string{"username"}, "userChannels": []string{"BBC1"}}
	_, _, err = db.Put("doc1", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"role:rolename"}, "userChannels": []string{"BBC1"}}
	_, _, err = db.Put("doc2", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"bad username"}, "userChannels": []string{"BBC1"}}
	_, _, err = db.Put("doc3", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"role:bad rolename"}, "userChannels": []string{"BBC1"}}
	_, _, err = db.Put("doc4", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"roll:over"}, "userChannels": []string{"BBC1"}}
	_, _, err = db.Put("doc5", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"username"}, "userChannels": []string{"bad,name"}}
	_, _, err = db.Put("doc6", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionDb(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	authenticator := auth.NewAuthenticator(db.Bucket, db)

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)

	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf(t, "Netflix"))
	user.SetExplicitRoles(channels.TimedSet{"animefan": channels.NewVbSimpleSequence(1), "tumblr": channels.NewVbSimpleSequence(1)})
	assert.NoError(t, authenticator.Save(user), "Save")

	body := Body{"users": []string{"naomi"}, "userChannels": []string{"Hulu"}}
	_, _, err = db.Put("doc1", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"role:animefan"}, "userChannels": []string{"CrunchyRoll"}}
	_, _, err = db.Put("doc2", body)
	assert.NoError(t, err, "")

	// Create the role _after_ creating the documents, to make sure the previously-indexed access
	// privileges are applied.
	role, _ := authenticator.NewRole("animefan", nil)
	assert.NoError(t, authenticator.Save(role))

	user, err = authenticator.GetUser("naomi")
	assert.NoError(t, err, "GetUser")
	expected := channels.AtSequence(channels.SetOf(t, "Hulu", "Netflix", "!"), 1)
	goassert.DeepEquals(t, user.Channels(), expected)

	expected.AddChannel("CrunchyRoll", 2)
	goassert.DeepEquals(t, user.InheritedChannels(), expected)
}

func TestDocIDs(t *testing.T) {
	goassert.Equals(t, realDocID(""), "")
	goassert.Equals(t, realDocID("_"), "")
	goassert.Equals(t, realDocID("_foo"), "")
	goassert.Equals(t, realDocID("foo"), "foo")
	goassert.Equals(t, realDocID("_design/foo"), "")
	goassert.Equals(t, realDocID(base.RevPrefix+"x"), "")
}

func TestUpdateDesignDoc(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	mapFunction := `function (doc, meta) { emit(); }`
	err := db.PutDesignDoc("official", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"TestView": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "add design doc as admin")

	// Validate retrieval of the design doc by admin
	var result sgbucket.DesignDoc
	err = db.GetDesignDoc("official", &result)
	log.Printf("design doc: %+v", result)

	// Try to retrieve it as an empty interface as well, and make sure no errors
	var resultEmptyInterface interface{}
	err = db.GetDesignDoc("official", &resultEmptyInterface)
	assert.NoError(t, err, "retrieve design doc (empty interface) as admin")

	assert.NoError(t, err, "retrieve design doc as admin")
	retrievedView, ok := result.Views["TestView"]
	goassert.True(t, ok)
	goassert.True(t, strings.Contains(retrievedView.Map, "emit()"))
	goassert.NotEquals(t, retrievedView.Map, mapFunction) // SG should wrap the map function, so they shouldn't be equal

	authenticator := auth.NewAuthenticator(db.Bucket, db)
	db.user, _ = authenticator.NewUser("naomi", "letmein", channels.SetOf(t, "Netflix"))
	err = db.PutDesignDoc("_design/pwn3d", sgbucket.DesignDoc{})
	assertHTTPError(t, err, 403)
}

func TestPostWithExistingId(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err := db.Post(body)
	goassert.True(t, rev1id != "")
	goassert.True(t, docid == customDocId)
	assert.NoError(t, err, "Couldn't create document")

	// Test retrieval
	doc, err := db.GetDocument(customDocId, DocUnmarshalAll)
	goassert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using custom id")

	// Test that standard UUID creation still works:
	log.Printf("Create document with existing id...")
	body = Body{"notAnId": customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err = db.Post(body)
	goassert.True(t, rev1id != "")
	goassert.True(t, docid != customDocId)
	assert.NoError(t, err, "Couldn't create document")

	// Test retrieval
	doc, err = db.GetDocument(docid, DocUnmarshalAll)
	goassert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using generated uuid")

}

// Unit test for issue #507
func TestPutWithUserSpecialProperty(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "_key2": "existing"}
	docid, rev1id, _, err := db.Post(body)
	goassert.True(t, rev1id == "")
	goassert.True(t, docid == "")
	goassert.True(t, err.Error() == "400 user defined top level properties beginning with '_' are not allowed in document body")
}

// Unit test for issue #976
func TestWithNullPropertyKey(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Test creating a document with null property key
	customDocId := "customIdValue"
	log.Printf("Create document with empty property key")
	body := Body{BodyId: customDocId, "": "value1"}
	docid, rev1id, _, _ := db.Post(body)
	goassert.True(t, rev1id != "")
	goassert.True(t, docid != "")
}

// Unit test for issue #507
func TestPostWithUserSpecialProperty(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err := db.Post(body)
	goassert.True(t, rev1id != "")
	goassert.True(t, docid == customDocId)
	assert.NoError(t, err, "Couldn't create document")

	// Test retrieval
	doc, err := db.GetDocument(customDocId, DocUnmarshalAll)
	goassert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using custom id")

	// Test that posting an update with a user special property does not update the
	//document
	log.Printf("Update document with existing id...")
	body = Body{BodyId: customDocId, BodyRev: rev1id, "_special": "value", "key1": "value1", "key2": "existing"}
	_, _, err = db.Put(docid, body)
	goassert.True(t, err.Error() == "400 user defined top level properties beginning with '_' are not allowed in document body")

	// Test retrieval gets rev1
	doc, err = db.GetDocument(docid, DocUnmarshalAll)
	goassert.True(t, doc != nil)
	goassert.True(t, doc.CurrentRev == rev1id)
	assert.NoError(t, err, "Unable to retrieve doc using generated uuid")

}

func TestRecentSequenceHistory(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	seqTracker := uint64(0)

	// Validate recent sequence is written
	body := Body{"val": "one"}
	revid, doc, err := db.Put("doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++

	expectedRecent := make([]uint64, 0)
	goassert.True(t, revid != "")
	doc, err = db.GetDocument("doc1", DocUnmarshalAll)
	expectedRecent = append(expectedRecent, seqTracker)
	goassert.True(t, err == nil)
	goassert.DeepEquals(t, doc.RecentSequences, expectedRecent)

	// Add up to kMaxRecentSequences revisions - validate they are retained when total is less than max
	for i := 1; i < kMaxRecentSequences; i++ {
		revid, doc, err = db.Put("doc1", body)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		seqTracker++
		expectedRecent = append(expectedRecent, seqTracker)
	}

	doc, err = db.GetDocument("doc1", DocUnmarshalAll)
	goassert.True(t, err == nil)
	goassert.DeepEquals(t, doc.RecentSequences, expectedRecent)

	// Recent sequence pruning only prunes entries older than what's been seen over DCP
	// (to ensure it's not pruning something that may still be coalesced).  Because of this, test waits
	// for caching before attempting to trigger pruning.
	err = db.changeCache.waitForSequence(context.TODO(), seqTracker, base.DefaultWaitForSequence)
	require.NoError(t, err)

	// Add another sequence to validate pruning when past max (20)
	revid, doc, err = db.Put("doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++
	doc, err = db.GetDocument("doc1", DocUnmarshalAll)
	goassert.True(t, err == nil)
	log.Printf("recent:%d, max:%d", len(doc.RecentSequences), kMaxRecentSequences)
	goassert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

	// Ensure pruning works when sequences aren't sequential
	doc2Body := Body{"val": "two"}
	for i := 0; i < kMaxRecentSequences; i++ {
		revid, doc, err = db.Put("doc1", body)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		seqTracker++
		revid, doc, err = db.Put("doc2", doc2Body)
		doc2Body[BodyId] = doc.ID
		doc2Body[BodyRev] = revid
		seqTracker++
	}

	err = db.changeCache.waitForSequence(context.TODO(), seqTracker, base.DefaultWaitForSequence) //
	require.NoError(t, err)
	revid, doc, err = db.Put("doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++
	doc, err = db.GetDocument("doc1", DocUnmarshalAll)
	goassert.True(t, err == nil)
	log.Printf("Recent sequences: %v (shouldn't exceed %v)", len(doc.RecentSequences), kMaxRecentSequences)
	goassert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

}

func TestChannelView(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Create doc
	log.Printf("Create doc 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, _, err := db.Put("doc1", body)
	assert.NoError(t, err, "Couldn't create document")
	goassert.Equals(t, rev1id, "1-cb0c9a22be0e5a1b01084ec019defa81")

	var entries LogEntries
	// Query view (retry loop to wait for indexing)
	for i := 0; i < 10; i++ {
		var err error
		entries, err = db.getChangesInChannelFromQuery("*", 0, 100, 0, false)

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
	goassert.Equals(t, len(entries), 1)

}

//////// XATTR specific tests.  These tests current require setting DefaultUseXattrs=true, and must be run against a Couchbase bucket

func TestConcurrentImport(t *testing.T) {

	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("Test only works with a Couchbase server and XATTRS")
	}

	db := setupTestDB(t)
	defer db.Close()

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyImport)()

	// Add doc to the underlying bucket:
	_, err := db.Bucket.Add("directWrite", 0, Body{"value": "hi"})
	require.NoError(t, err)

	// Attempt concurrent retrieval of the docs, and validate that they are only imported once (based on revid)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doc, err := db.GetDocument("directWrite", DocUnmarshalAll)
			assert.True(t, doc != nil)
			assert.NoError(t, err, "Document retrieval error")
			assert.Equal(t, "1-36fa688dc2a2c39a952ddce46ab53d12", doc.SyncData.CurrentRev)
		}()
	}
	wg.Wait()
}

func TestViewCustom(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test may not pass under non-walrus, if views aren't enabled, as ViewAllDocs won't exist")
	}

	db := setupTestDB(t)
	defer db.Close()

	// add some docs
	docId := base.GenerateRandomID()
	_, _, err := db.Put(docId, Body{"val": "one"})
	if err != nil {
		log.Printf("error putting doc: %v", err)
	}
	assert.True(t, err == nil)

	// query all docs using ViewCustom query.
	opts := Body{"stale": false, "reduce": false}
	viewResult := sgbucket.ViewResult{}

	// Add retry to avoid race condition where queryAllDocs doesn't return the doc we just added
	// TODO: stale=false guarantees no race when using a couchbase bucket, but this test
	//	may be hitting something related to https://github.com/couchbaselabs/walrus/issues/18.  Can remove sleep when
	// that gets fixed
	foundDoc := false
	for i := 0; i < 10; i++ {
		errViewCustom := db.Bucket.ViewCustom(DesignDocSyncHousekeeping(), ViewAllDocs, opts, &viewResult)
		assert.True(t, errViewCustom == nil)

		// assert that the doc added earlier is in the results
		for _, viewRow := range viewResult.Rows {
			if viewRow.ID == docId {
				foundDoc = true
			}
		}
		if foundDoc {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, foundDoc)

}

//////// BENCHMARKS

func BenchmarkDatabase(b *testing.B) {
	defer base.DisableTestLogging()()

	for i := 0; i < b.N; i++ {
		bucket, _ := ConnectToBucket(base.BucketSpec{
			Server:          base.UnitTestUrl(),
			CouchbaseDriver: base.ChooseCouchbaseDriver(base.DataBucket),
			BucketName:      fmt.Sprintf("b-%d", i)})
		context, _ := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
		db, _ := CreateDatabase(context)

		body := Body{"key1": "value1", "key2": 1234}
		_, _, _ = db.Put(fmt.Sprintf("doc%d", i), body)

		db.Close()
	}
}

func BenchmarkPut(b *testing.B) {
	defer base.DisableTestLogging()()

	bucket, _ := ConnectToBucket(base.BucketSpec{
		Server:          base.UnitTestUrl(),
		CouchbaseDriver: base.ChooseCouchbaseDriver(base.DataBucket),
		BucketName:      "Bucket"})
	context, _ := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	db, _ := CreateDatabase(context)

	body := Body{"key1": "value1", "key2": 1234}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = db.Put(fmt.Sprintf("doc%d", i), body)
	}

	db.Close()
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
		Name:          "Google",
		Issuer:        "https://accounts.google.com",
		CallbackURL:   &callbackURL,
		ClientID:      clientID,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithCallbackURLQuery() auth.OIDCProvider {
	return auth.OIDCProvider{
		Name:          "Google",
		Issuer:        "https://accounts.google.com",
		CallbackURL:   &callbackURLWithQuery,
		ClientID:      clientID,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoIss() auth.OIDCProvider {
	return auth.OIDCProvider{
		Name:          "Microsoft",
		CallbackURL:   &callbackURL,
		ClientID:      clientID,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoClientID() auth.OIDCProvider {
	return auth.OIDCProvider{
		Name:          "Amazon",
		Issuer:        "https://accounts.amazon.com",
		CallbackURL:   &callbackURL,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoValidationKey() auth.OIDCProvider {
	return auth.OIDCProvider{
		Name:        "Yahoo",
		Issuer:      "https://accounts.yahoo.com",
		CallbackURL: &callbackURL,
		ClientID:    clientID,
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

func TestNewDatabaseContextWithOIDCProviderOptionErrors(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	tests := []struct {
		name          string
		inputOptions  *auth.OIDCOptions
		expectedError string
	}{
		// Provider should be skipped if no issuer is not provided in OIDCOptions. It should throw the error
		// "OpenID Connect defined in config, but no valid OpenID Connect providers specified". Also a warning
		// should be logged saying "Issuer and ClientID required for OIDC Provider - skipping provider"
		{
			name:          "TestWithNoIss",
			inputOptions:  mockOIDCOptionsWithNoIss(),
			expectedError: "OpenID Connect defined in config, but no valid OpenID Connect providers specified",
		},
		// Provider should be skipped if no client ID is not provided in OIDCOptions. It should throw the error
		// "OpenID Connect defined in config, but no valid OpenID Connect providers specified". Also a warning
		//	should be logged saying "Issuer and ClientID required for OIDC Provider - skipping provider"
		{
			name:          "TestWithNoClientID",
			inputOptions:  mockOIDCOptionsWithNoClientID(),
			expectedError: "OpenID Connect defined in config, but no valid OpenID Connect providers specified",
		},
		// If the provider name is illegal; meaning an underscore character in provider name is considered as
		// illegal, it should throw an error stating OpenID Connect provider names cannot contain underscore.
		{
			name:          "TestWithWithBadName",
			inputOptions:  mockOIDCOptionsWithBadName(),
			expectedError: "OpenID Connect provider names cannot contain underscore",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			options := DatabaseContextOptions{
				OIDCOptions: tc.inputOptions,
			}
			AddOptionsFromEnvironmentVariables(&options)

			context, err := NewDatabaseContext("db", testBucket, false, options)
			assert.Error(t, err, "Couldn't create context for database 'db'")
			assert.Contains(t, err.Error(), tc.expectedError)
			assert.Nil(t, context, "Database context shouldn't be created")
		})
	}

	testBucket.Close()
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
			context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, options)
			assert.NoError(t, err, "Couldn't create context for database 'db'")
			defer context.Close()
			assert.NotNil(t, context, "Database context should be created")

			database, err := CreateDatabase(context)
			assert.NotNil(t, database, "Database should be created with context options")
			assert.NoError(t, err, "Couldn't create database 'db'")
		})
	}
}

func TestGetOIDCProvider(t *testing.T) {
	options := DatabaseContextOptions{OIDCOptions: mockOIDCOptions()}
	context := setupTestDBWithOptions(t, options)
	defer context.Close()

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

	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		doc.key1 = "mutatedValue"
		doc.key2.subkey1 = "mutatedSubValue"
		channel(doc.channels);
	}`)

	// Create first revision:
	body := Body{"key1": "value1", "key2": Body{"subkey1": "subvalue1"}, "channels": []string{"public"}}
	rev1id, _, err := db.Put("doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	rev, err := db.GetRev("doc1", rev1id, false, nil)
	revBody, err := rev.MutableBody()
	require.NoError(t, err, "Couldn't get mutable body")
	assert.Equal(t, "value1", revBody["key1"])
	assert.Equal(t, map[string]interface{}{"subkey1": "subvalue1"}, revBody["key2"])
	log.Printf("rev: %s", rev.BodyBytes)

}

// Multiple clients are attempting to push the same new revision concurrently; first writer should be successful,
// subsequent writers should fail on CAS, and then identify that revision already exists on retry.
func TestConcurrentPushSameNewRevision(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCRUD)()
	var db *Database
	var enableCallback bool
	var revId string

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Bob", "age": 52}
			revId, _, err := db.Put("doc1", body)
			assert.NoError(t, err, "Couldn't create document")
			assert.NotEmpty(t, revId)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		WriteUpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close()

	enableCallback = true

	body := Body{"name": "Bob", "age": 52}
	_, _, err := db.Put("doc1", body)
	assert.Equal(t, "409 Document exists", err.Error())

	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	assert.Equal(t, revId, doc.RevID)
	assert.NoError(t, err, "Couldn't retrieve document")
	assert.Equal(t, "Bob", doc.Body()["name"])
	assert.Equal(t, json.Number("52"), doc.Body()["age"])
}

// Multiple clients are attempting to push the same new, non-winning revision concurrently; non-winning is an
// update to a non-winning branch that leaves the branch still non-winning (i.e. shorter) than the active branch
func TestConcurrentPushSameNewNonWinningRevision(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCRUD)()
	var db *Database
	var enableCallback bool

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Emily", "age": 20}
			_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"3-b", "2-b", "1-a"}, false)
			assert.NoError(t, err, "Adding revision 3-b")
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		WriteUpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close()

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Charlie", "age": 10}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 4-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-b")

	enableCallback = true

	body = Body{"name": "Emily", "age": 20}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"3-b", "2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-b")

	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc after adding 3-b")
	assert.Equal(t, "4-a", doc.CurrentRev)
}

// Multiple clients are attempting to push the same tombstone of the winning revision for a branched document
// First writer should be successful, subsequent writers should fail on CAS, then identify rev already exists
func TestConcurrentPushSameTombstoneWinningRevision(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCRUD)()
	var db *Database
	var enableCallback bool

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Charlie", "age": 10, BodyDeleted: true}
			_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
			assert.NoError(t, err, "Couldn't add revision 4-a (tombstone)")
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		WriteUpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close()

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-b")

	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc before tombstone")
	assert.Equal(t, "3-a", doc.CurrentRev)

	enableCallback = true

	body = Body{"name": "Charlie", "age": 10, BodyDeleted: true}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Couldn't add revision 4-a (tombstone)")

	doc, err = db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.CurrentRev)
}

// Multiple clients are attempting to push conflicting non-winning revisions; multiple clients pushing different
// updates to non-winning branches that leave the branch(es) non-winning.
func TestConcurrentPushDifferentUpdateNonWinningRevision(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCRUD)()
	var db *Database
	var enableCallback bool

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Joshua", "age": 11}
			_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"3-b1", "2-b", "1-a"}, false)
			assert.NoError(t, err, "Couldn't add revision 3-b1")
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		WriteUpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close()

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Charlie", "age": 10}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 4-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "Adding revision 2-b")

	enableCallback = true

	body = Body{"name": "Liam", "age": 12}
	_, _, err = db.PutExistingRevWithBody("doc1", body, []string{"3-b2", "2-b", "1-a"}, false)
	assert.NoError(t, err, "Couldn't add revision 3-b2")

	doc, err := db.GetDocument("doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc after adding 3-b")
	assert.Equal(t, "4-a", doc.CurrentRev)

	rev, err := db.GetRev("doc1", "3-b1", false, nil)
	assert.NoError(t, err, "Retrieve revision 3-b1")
	revBody, err := rev.MutableBody()
	assert.NoError(t, err, "Retrieve body of revision 3-b1")
	assert.Equal(t, "Joshua", revBody["name"])
	assert.Equal(t, json.Number("11"), revBody["age"])

	rev, err = db.GetRev("doc1", "3-b2", false, nil)
	assert.NoError(t, err, "Retrieve revision 3-b2")
	revBody, err = rev.MutableBody()
	assert.NoError(t, err, "Retrieve body of revision 3-b2")
	assert.Equal(t, "Liam", revBody["name"])
	assert.Equal(t, json.Number("12"), revBody["age"])
}
