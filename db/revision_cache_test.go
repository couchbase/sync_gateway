package db

import (
	"expvar"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

// testBackingStore always returns an empty doc at rev:"1-abc" in channel "*" except for docs not in 'notFoundDocIDs'
type testBackingStore struct {
	// notFoundDocIDs is a list of doc IDs that GetDocument returns a 404 for.
	notFoundDocIDs     []string
	getDocumentCounter *expvar.Int
	getRevisionCounter *expvar.Int
}

func (t *testBackingStore) GetDocument(docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	t.getDocumentCounter.Add(1)

	for _, d := range t.notFoundDocIDs {
		if docid == d {
			return nil, base.HTTPErrorf(404, "missing")
		}
	}

	doc = NewDocument(docid)
	doc._body = Body{
		"testing": true,
	}
	doc.CurrentRev = "1-abc"
	doc.History = RevTree{
		doc.CurrentRev: {
			Channels: base.SetOf("*"),
		},
	}
	return doc, nil
}

func (t *testBackingStore) getRevision(doc *Document, revid string) ([]byte, error) {
	t.getRevisionCounter.Add(1)

	b := Body{
		"testing":     true,
		BodyId:        doc.ID,
		BodyRev:       doc.CurrentRev,
		BodyRevisions: Revisions{RevisionsStart: 1},
	}
	return base.JSONMarshal(b)
}

type noopBackingStore struct{}

func (*noopBackingStore) GetDocument(docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	return nil, nil
}

func (*noopBackingStore) getRevision(doc *Document, revid string) ([]byte, error) {
	return nil, nil
}

// Tests the eviction from the LRURevisionCache
func TestLRURevisionCacheEviction(t *testing.T) {
	cacheHitCounter, cacheMissCounter := expvar.Int{}, expvar.Int{}
	cache := NewLRURevisionCache(10, &noopBackingStore{}, &cacheHitCounter, &cacheMissCounter)

	// Fill up the rev cache with the first 10 docs
	for docID := 0; docID < 10; docID++ {
		id := strconv.Itoa(docID)
		cache.Put(DocumentRevision{DocID: id, RevID: "1-abc", History: Revisions{"start": 1}})
	}

	// Get them back out
	for i := 0; i < 10; i++ {
		docID := strconv.Itoa(i)
		docRev, err := cache.Get(docID, "1-abc")
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", docID)
		assert.Equal(t, docID, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, int64(i+1), cacheHitCounter.Value())
	}

	// Add 3 more docs to the now full revcache
	for i := 10; i < 13; i++ {
		docID := strconv.Itoa(i)
		cache.Put(DocumentRevision{DocID: docID, RevID: "1-abc", History: Revisions{"start": 1}})
	}

	// Check that the first 3 docs were evicted
	prevCacheHitCount := cacheHitCounter.Value()
	for i := 0; i < 3; i++ {
		docID := strconv.Itoa(i)
		docRev, ok := cache.Peek(docID, "1-abc")
		assert.False(t, ok)
		assert.Nil(t, docRev)
		assert.Equal(t, int64(0), cacheMissCounter.Value()) // peek incurs no cache miss if not found
		assert.Equal(t, int64(prevCacheHitCount), cacheHitCounter.Value())
	}

	// and check we can Get up to and including the last 3 we put in
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		docRev, err := cache.Get(id, "1-abc")
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", id)
		assert.Equal(t, id, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, prevCacheHitCount+int64(i)+1, cacheHitCounter.Value())
	}
}

func TestBackingStore(t *testing.T) {

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := expvar.Int{}, expvar.Int{}, expvar.Int{}, expvar.Int{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"Peter"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	docRev, err := cache.Get("Jens", "1-abc")
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Doc doesn't exist, so miss the cache, and fail when getting the doc
	docRev, err = cache.Get("Peter", "1-abc")
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev is already resident, but still issue GetDocument to check for later revisions
	docRev, err = cache.Get("Jens", "1-abc")
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, err = cache.Get("Peter", "1-abc")
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(3), cacheMissCounter.Value())
	assert.Equal(t, int64(3), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())
}

// Ensure internal properties aren't being incorrectly stored in revision cache
func TestRevisionCacheInternalProperties(t *testing.T) {

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Invalid _revisions property will be stripped.  Should also not be present in the rev cache.
	rev1body := Body{
		"value":       1234,
		BodyRevisions: "unexpected data",
	}
	rev1id, _, err := db.Put("doc1", rev1body)
	assert.NoError(t, err, "Put")

	// Get the raw document directly from the bucket, validate _revisions property isn't found
	var bucketBody Body
	testBucket.Bucket.Get("doc1", &bucketBody)
	_, ok := bucketBody[BodyRevisions]
	if ok {
		t.Error("_revisions property still present in document retrieved directly from bucket.")
	}

	// Get the doc while still resident in the rev cache w/ history=false, validate _revisions property isn't found
	body, err := db.GetRev1xBody("doc1", rev1id, false, nil)
	assert.NoError(t, err, "GetRev1xBody")
	badRevisions, ok := body[BodyRevisions]
	if ok {
		t.Errorf("_revisions property still present in document retrieved from rev cache: %s", badRevisions)
	}

	// Get the doc while still resident in the rev cache w/ history=true, validate _revisions property is returned with expected
	// properties ("start", "ids")
	bodyWithHistory, err := db.GetRev1xBody("doc1", rev1id, true, nil)
	assert.NoError(t, err, "GetRev1xBody")
	validRevisions, ok := bodyWithHistory[BodyRevisions]
	if !ok {
		t.Errorf("Expected _revisions property not found in document retrieved from rev cache: %s", validRevisions)
	}

	validRevisionsMap, ok := validRevisions.(Revisions)
	_, startOk := validRevisionsMap[RevisionsStart]
	goassert.True(t, startOk)
	_, idsOk := validRevisionsMap[RevisionsIds]
	goassert.True(t, idsOk)
}

func TestBypassRevisionCache(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	docBody := Body{
		"value": 1234,
	}
	key := "doc1"
	rev1, _, err := db.Put(key, docBody)
	assert.NoError(t, err)

	docBody["_rev"] = rev1
	docBody["value"] = 5678
	rev2, _, err := db.Put(key, docBody)
	assert.NoError(t, err)

	bypassStat := expvar.Int{}
	rc := NewBypassRevisionCache(db.DatabaseContext, &bypassStat)

	// Peek always returns false for BypassRevisionCache
	_, ok := rc.Peek(key, rev1)
	assert.False(t, ok)
	_, ok = rc.Peek(key, rev2)
	assert.False(t, ok)

	// Get non-existing doc
	doc, err := rc.Get("invalid", rev1)
	assert.True(t, base.IsDocNotFoundError(err))

	// Get non-existing revision
	doc, err = rc.Get(key, "3-abc")
	assertHTTPError(t, err, 404)

	// Get specific revision
	doc, err = rc.Get(key, rev1)
	assert.NoError(t, err)
	assert.Equal(t, `{"value":1234}`, doc.BodyBytes)

	// Check peek is still returning false for "Get"
	_, ok = rc.Peek(key, rev1)
	assert.False(t, ok)

	// Put no-ops
	rc.Put(doc)

	// Check peek is still returning false for "Put"
	_, ok = rc.Peek(key, rev1)
	assert.False(t, ok)

	// Get active revision
	doc, err = rc.GetActive(key)
	assert.NoError(t, err)
	assert.Equal(t, `{"value":5678}`, doc.BodyBytes)

}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via Put
func TestPutRevisionCacheAttachmentProperty(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	rev1body := Body{
		"value":         1234,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	rev1key := "doc1"
	rev1id, _, err := db.Put(rev1key, rev1body)
	assert.NoError(t, err, "Unexpected error calling db.Put")

	// Get the raw document directly from the bucket, validate _attachments property isn't found
	var bucketBody Body
	_, err = testBucket.Bucket.Get(rev1key, &bucketBody)
	assert.NoError(t, err, "Unexpected error calling bucket.Get")
	_, ok := bucketBody[BodyAttachments]
	assert.False(t, ok, "_attachments property still present in document body retrieved from bucket: %#v", bucketBody)

	// Get the raw document directly from the revcache, validate _attachments property isn't found
	docRevision, ok := db.revisionCache.Peek(rev1key, rev1id)
	assert.True(t, ok)
	assert.NotContains(t, docRevision.BodyBytes, BodyAttachments, "_attachments property still present in document body retrieved from rev cache: %#v", bucketBody)
	_, ok = docRevision.Attachments["myatt"]
	assert.True(t, ok, "'myatt' not found in revcache attachments metadata")

	// db.getRev stamps _attachments back in from revcache Attachment metadata
	body, err := db.GetRev1xBody(rev1key, rev1id, false, nil)
	assert.NoError(t, err, "Unexpected error calling db.GetRev1xBody")
	atts, ok := body[BodyAttachments]
	assert.True(t, ok, "_attachments property was not stamped back in body during db.GetRev1xBody: %#v", body)

	attsMap, ok := atts.(AttachmentsMeta)
	_, ok = attsMap["myatt"]
	assert.True(t, ok, "'myatt' not found in attachment map")
}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via PutExistingRev
func TestPutExistingRevRevisionCacheAttachmentProperty(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	docKey := "doc1"
	rev1body := Body{
		"value": 1234,
	}
	rev1id, _, err := db.Put(docKey, rev1body)
	assert.NoError(t, err, "Unexpected error calling db.Put")

	rev2id := "2-xxx"
	rev2body := Body{
		"value":         1235,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	_, err = db.PutExistingRevWithBody(docKey, rev2body, []string{rev2id, rev1id}, false)
	assert.NoError(t, err, "Unexpected error calling db.PutExistingRev")

	// Get the raw document directly from the bucket, validate _attachments property isn't found
	var bucketBody Body
	_, err = testBucket.Bucket.Get(docKey, &bucketBody)
	assert.NoError(t, err, "Unexpected error calling bucket.Get")
	_, ok := bucketBody[BodyAttachments]
	assert.False(t, ok, "_attachments property still present in document body retrieved from bucket: %#v", bucketBody)

	// Get the raw document directly from the revcache, validate _attachments property isn't found
	docRevision, err := db.revisionCache.Get(docKey, rev2id)
	assert.NoError(t, err, "Unexpected error calling db.revisionCache.Get")
	assert.NotContains(t, docRevision.BodyBytes, BodyAttachments, "_attachments property still present in document body retrieved from rev cache: %#v", bucketBody)
	_, ok = docRevision.Attachments["myatt"]
	assert.True(t, ok, "'myatt' not found in revcache attachments metadata")

	// db.getRev stamps _attachments back in from revcache Attachment metadata
	body, err := db.GetRev1xBody(docKey, rev2id, false, nil)
	assert.NoError(t, err, "Unexpected error calling db.GetRev1xBody")
	atts, ok := body[BodyAttachments]
	assert.True(t, ok, "_attachments property was not stamped back in body during db.GetRev1xBody: %#v", body)

	attsMap, ok := atts.(AttachmentsMeta)
	_, ok = attsMap["myatt"]
	assert.True(t, ok, "'myatt' not found in attachment map")
}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestRevisionImmutableDelta(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := expvar.Int{}, expvar.Int{}, expvar.Int{}, expvar.Int{}
	cache := NewLRURevisionCache(10, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	firstDelta := []byte("delta")
	secondDelta := []byte("modified delta")

	// Trigger load into cache
	_, err := cache.Get("doc1", "1-abc")
	assert.NoError(t, err, "Error adding to cache")
	cache.UpdateDelta("doc1", "1-abc", &RevisionDelta{ToRevID: "rev2", DeltaBytes: firstDelta})

	// Retrieve from cache
	retrievedRev, err := cache.Get("doc1", "1-abc")
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Update delta again, validate data in retrievedRev isn't mutated
	cache.UpdateDelta("doc1", "1-abc", &RevisionDelta{ToRevID: "rev3", DeltaBytes: secondDelta})
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Retrieve again, validate delta is correct
	updatedRev, err := cache.Get("doc1", "1-abc")
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev3", updatedRev.Delta.ToRevID)
	assert.Equal(t, secondDelta, updatedRev.Delta.DeltaBytes)

	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

}

func BenchmarkRevisionCacheRead(b *testing.B) {

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := expvar.Int{}, expvar.Int{}, expvar.Int{}, expvar.Int{}
	cache := NewLRURevisionCache(5000, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	// trigger load into cache
	for i := 0; i < 5000; i++ {
		_, err := cache.Get(fmt.Sprintf("doc%d", i), "1-abc")
		assert.NoError(b, err, "Error initializing cache for BenchmarkRevisionCacheRead")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		//GET the document until test run has completed
		for pb.Next() {
			docId := fmt.Sprintf("doc%d", rand.Intn(5000))
			docrev, err := cache.Get(docId, "1-abc")
			if err != nil {
				assert.Fail(b, "Unexpected error for docrev:%+v", docrev)
			}
		}
	})
}
