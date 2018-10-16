package db

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbaselabs/go.assert"
)

func makeExternalBucket() base.TestBucket {

	// Call this for the side effect of emptying out the data bucket, in case it interferes
	// with bucket shadowing tests by causing unwanted data to get pulled into shadow bucket
	tempBucket := base.GetTestBucketOrPanic()
	tempBucket.Close()

	return base.GetTestShadowBucketOrPanic()
}

// Evaluates a condition every 100ms until it becomes true. If 3sec elapse, fails an assertion
func waitFor(t *testing.T, condition func() bool) bool {
	var start = time.Now()
	for !condition() {
		if time.Since(start) >= 15*time.Second {
			assertFailed(t, "Timeout!")
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}

func TestShadowerPull(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("BucketShadowing with XATTRS is not a supported configuration")
	}

	testBucket := makeExternalBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	bucket.Set("key1", 0, Body{"foo": 1})
	bucket.Set("key2", 0, Body{"bar": -1})
	bucket.SetRaw("key3", 0, []byte("qwertyuiop")) //will be ignored

	db := setupTestDBForShadowing(t)
	defer tearDownTestDB(t, db)

	shadower, err := NewShadower(db.DatabaseContext, bucket, nil)
	assertNoError(t, err, "NewShadower")
	defer shadower.Stop()

	t.Logf("Waiting for shadower to catch up...")
	var doc1, doc2 *document
	waitFor(t, func() bool {
		seq, _ := db.LastSequence()
		return seq >= 2
	})
	doc1, _ = db.GetDocument("key1", DocUnmarshalAll)
	doc2, _ = db.GetDocument("key2", DocUnmarshalAll)
	assert.DeepEquals(t, doc1.Body(), Body{"foo": float64(1)})
	assert.DeepEquals(t, doc2.Body(), Body{"bar": float64(-1)})

	t.Logf("Deleting remote doc")
	bucket.Delete("key1")

	waitFor(t, func() bool {
		seq, _ := db.LastSequence()
		return seq >= 3
	})

	doc1, _ = db.GetDocument("key1", DocUnmarshalAll)
	assert.True(t, doc1.hasFlag(channels.Deleted))
	_, err = db.Get("key1")
	assert.DeepEquals(t, err, &base.HTTPError{Status: 404, Message: "deleted"})

	waitFor(t, func() bool {
		return atomic.LoadUint64(&shadower.pullCount) >= 4
	})
}

func TestShadowerPullWithNotifications(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("BucketShadowing with XATTRS is not a supported configuration")
	}

	//Create shadow bucket
	testBucket := makeExternalBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	//New docs should write notification events
	bucket.Set("key1", 0, Body{"foo": 1})
	bucket.Set("key2", 0, Body{"bar": -1})

	db := setupTestDBForShadowing(t)

	//Create an event manager and start it
	em := db.EventMgr
	em.Start(0, -1)
	resultChannel := make(chan Body, 10)
	//Setup test handler
	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)

	defer tearDownTestDB(t, db)

	shadower, err := NewShadower(db.DatabaseContext, bucket, nil)
	assertNoError(t, err, "NewShadower")
	defer shadower.Stop()

	t.Logf("Waiting for shadower to catch up...")
	waitFor(t, func() bool {
		seq, _ := db.LastSequence()
		return seq >= 2
	})

	//Write shadow doc with same body, should not generate an notification event
	t.Logf("Updating remote doc without any changes to body")
	bucket.Set("key1", 0, Body{"foo": 1})

	waitFor(t, func() bool {
		seq, _ := db.LastSequence()
		return seq >= 3
	})

	//Write shadow doc with different body, should generate an notification event
	t.Logf("Updating remote doc with changes to body")
	bucket.Set("key2", 0, Body{"foo": 1})

	waitFor(t, func() bool {
		seq, _ := db.LastSequence()
		return seq >= 4
	})

	// wait for Event Manager queue worker to process
	time.Sleep(500 * time.Millisecond)

	channelSize := len(resultChannel)

	assert.True(t, channelSize == 3)

	waitFor(t, func() bool {
		return atomic.LoadUint64(&shadower.pullCount) >= 4
	})
}

func TestShadowerPush(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("BucketShadowing with XATTRS is not a supported configuration")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyShadow)()

	testBucket := makeExternalBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	db := setupTestDBForShadowing(t)
	defer tearDownTestDB(t, db)

	var err error
	db.Shadower, err = NewShadower(db.DatabaseContext, bucket, nil)
	assertNoError(t, err, "NewShadower")

	key1rev1, err := db.Put("key1", Body{"aaa": "bbb"})
	assertNoError(t, err, "Put")
	_, err = db.Put("key2", Body{"ccc": "ddd"})
	assertNoError(t, err, "Put")

	t.Log("Waiting for shadower to catch up...")
	var doc1, doc2 Body
	waitFor(t, func() bool {
		_, err1 := bucket.Get("key1", &doc1)
		_, err2 := bucket.Get("key2", &doc2)
		return err1 == nil && err2 == nil
	})
	assert.DeepEquals(t, doc1, Body{"aaa": "bbb"})
	assert.DeepEquals(t, doc2, Body{"ccc": "ddd"})

	t.Logf("Deleting local doc")
	db.DeleteDoc("key1", key1rev1)

	waitFor(t, func() bool {
		_, err = bucket.Get("key1", &doc1)
		return err != nil
	})
	assert.True(t, base.IsDocNotFoundError(err))

	waitFor(t, func() bool {
		return atomic.LoadUint64(&db.Shadower.pullCount) >= 3
	})
}

// Make sure a rev inserted into the db by a client replicator doesn't get echoed from the
// shadower as a different revision.
func TestShadowerPushEchoCancellation(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("BucketShadowing with XATTRS is not a supported configuration")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyShadow)()

	testBucket := makeExternalBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	db := setupTestDBForShadowing(t)
	defer tearDownTestDB(t, db)

	var err error
	db.Shadower, err = NewShadower(db.DatabaseContext, bucket, nil)
	assertNoError(t, err, "NewShadower")

	// Push an existing doc revision (the way a client's push replicator would)
	db.PutExistingRev("foo", Body{"a": "b"}, []string{"1-madeup"}, false)
	waitFor(t, func() bool {
		return atomic.LoadUint64(&db.Shadower.pullCount) >= 1
	})

	// Make sure the echoed pull didn't create a new revision:
	doc, _ := db.GetDocument("foo", DocUnmarshalAll)
	assert.Equals(t, len(doc.History), 1)
}

// Ensure that a new rev pushed from a shadow bucket update, wehre the UpstreamRev does not exist as a parent func init() {
// the documents rev tree does not panic, it should generate a new conflicting branch instead.
// see #1603
func TestShadowerPullRevisionWithMissingParentRev(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test is currently not passing against Couchbase server.  Needs investigation. " +
			"Logs: https://gist.github.com/tleyden/795df447314a521aba5bd1aa6d0ed42e")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyShadow)()

	testBucket := makeExternalBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	db := setupTestDBForShadowing(t)
	defer tearDownTestDB(t, db)

	var err error
	db.Shadower, err = NewShadower(db.DatabaseContext, bucket, nil)
	assertNoError(t, err, "NewShadower")

	// Push an existing doc revision (the way a client's push replicator would)
	db.PutExistingRev("foo", Body{"a": "b"}, []string{"1-madeup"}, false)
	waitFor(t, func() bool {
		return atomic.LoadUint64(&db.Shadower.pullCount) >= 1
	})

	//Directly edit the "upstream_rev" _sync property of the doc
	//We don't want to trigger a push to the shadow bucket
	raw, _, _ := db.Bucket.GetRaw("foo")

	//Unmarshal to JSON
	var docObj map[string]interface{}
	json.Unmarshal(raw, &docObj)

	docObj["upstream_rev"] = "1-notexist"

	docBytes, _ := json.Marshal(docObj)

	//Write raw doc bytes back to bucket
	db.Bucket.SetRaw("foo", 0, docBytes)

	//Now edit the raw file in the shadow bucket to
	// trigger a shadow pull
	bucket.SetRaw("foo", 0, []byte("{\"a\":\"c\"}"))

	//validate that upstream_rev was changed in DB
	raw, _, _ = db.Bucket.GetRaw("foo")
	json.Unmarshal(raw, &docObj)
	assert.Equals(t, docObj["upstream_rev"], "1-notexist")

	waitFor(t, func() bool {
		return atomic.LoadUint64(&db.Shadower.pullCount) >= 2
	})

	//Assert that we can get the two conflicing revisions
	gotBody, err := db.GetRev("foo", "1-madeup", false, nil)
	assert.DeepEquals(t, gotBody, Body{BodyId: "foo", "a": "b", BodyRev: "1-madeup"})
	gotBody, err = db.GetRev("foo", "2-edce85747420ad6781bdfccdebf82180", false, nil)
	assert.DeepEquals(t, gotBody, Body{BodyId: "foo", "a": "c", BodyRev: "2-edce85747420ad6781bdfccdebf82180"})
}

func TestShadowerPattern(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("BucketShadowing with XATTRS is not a supported configuration")
	}

	testBucket := makeExternalBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	bucket.Set("key1", 0, Body{"foo": 1})
	bucket.Set("ignorekey", 0, Body{"bar": -1})
	bucket.Set("key2", 0, Body{"bar": -1})

	db := setupTestDBForShadowing(t)
	defer tearDownTestDB(t, db)

	pattern, _ := regexp.Compile(`key\d+`)
	shadower, err := NewShadower(db.DatabaseContext, bucket, pattern)
	assertNoError(t, err, "NewShadower")
	defer shadower.Stop()

	t.Logf("Waiting for shadower to catch up...")
	waitFor(t, func() bool {
		seq, _ := db.LastSequence()
		return seq >= 2
	})
	doc1, err := db.GetDocument("key1", DocUnmarshalAll)
	assertNoError(t, err, fmt.Sprintf("Error getting key1: %v", err))
	docI, _ := db.GetDocument("ignorekey", DocUnmarshalAll)
	doc2, err := db.GetDocument("key2", DocUnmarshalAll)
	assertNoError(t, err, fmt.Sprintf("Error getting key2: %v", err))

	assert.DeepEquals(t, doc1.Body(), Body{"foo": float64(1)})
	assert.True(t, docI == nil)
	assert.DeepEquals(t, doc2.Body(), Body{"bar": float64(-1)})

	waitFor(t, func() bool {
		return atomic.LoadUint64(&shadower.pullCount) >= 2
	})
}
