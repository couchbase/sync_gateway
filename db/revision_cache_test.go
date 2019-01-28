package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func testDocRev(revId string, body Body, history Revisions, channels base.Set, expiry *time.Time, attachments AttachmentsMeta) DocumentRevision {
	return DocumentRevision{
		RevID:       revId,
		Body:        body,
		History:     history,
		Channels:    channels,
		Expiry:      expiry,
		Attachments: attachments,
	}
}

func TestRevisionCache(t *testing.T) {
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	revForTest := func(i int) (Body, Revisions, base.Set) {
		body := Body{
			BodyId:  ids[i],
			BodyRev: "x",
		}
		history := Revisions{RevisionsStart: i}
		return body, history, nil
	}
	verify := func(body Body, history Revisions, channels base.Set, i int) {
		if body == nil {
			t.Fatalf("nil body at #%d", i)
		}
		goassert.True(t, body != nil)
		goassert.Equals(t, body[BodyId], ids[i])
		goassert.True(t, history != nil)
		goassert.Equals(t, history[RevisionsStart], i)
		goassert.DeepEquals(t, channels, base.Set(nil))
	}

	cache := NewRevisionCache(10, nil, nil)
	for i := 0; i < 10; i++ {
		body, history, channels := revForTest(i)
		docRev := testDocRev(body[BodyRev].(string), body, history, channels, nil, nil)
		cache.Put(body[BodyId].(string), docRev)
	}

	for i := 0; i < 10; i++ {
		getDocRev, _ := cache.Get(ids[i], "x")
		verify(getDocRev.Body, getDocRev.History, getDocRev.Channels, i)
	}

	for i := 10; i < 13; i++ {
		body, history, channels := revForTest(i)
		docRev := testDocRev(body[BodyRev].(string), body, history, channels, nil, nil)
		cache.Put(body[BodyId].(string), docRev)
	}

	for i := 0; i < 3; i++ {
		docRev, _ := cache.Get(ids[i], "x")
		goassert.True(t, docRev.Body == nil)
	}
	for i := 3; i < 13; i++ {
		docRev, _ := cache.Get(ids[i], "x")
		verify(docRev.Body, docRev.History, docRev.Channels, i)
	}
}

func TestLoaderFunction(t *testing.T) {
	var callsToLoader = 0
	loader := func(id IDAndRev) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error) {
		callsToLoader++
		if id.DocID[0] != 'J' {
			err = base.HTTPErrorf(404, "missing")
		} else {
			body = Body{
				BodyId:  id.DocID,
				BodyRev: id.RevID,
			}
			history = Revisions{RevisionsStart: 1}
			channels = base.SetOf("*")
		}
		return
	}
	cache := NewRevisionCache(10, loader, nil)

	docRev, err := cache.Get("Jens", "1")
	goassert.Equals(t, docRev.Body[BodyId], "Jens")
	goassert.True(t, docRev.History != nil)
	goassert.True(t, docRev.Channels != nil)
	goassert.Equals(t, err, error(nil))
	goassert.Equals(t, callsToLoader, 1)

	docRev, err = cache.Get("Peter", "1")
	goassert.DeepEquals(t, docRev.Body, Body(nil))
	goassert.DeepEquals(t, err, base.HTTPErrorf(404, "missing"))
	goassert.Equals(t, callsToLoader, 2)

	docRev, err = cache.Get("Jens", "1")
	goassert.Equals(t, docRev.Body[BodyId], "Jens")
	goassert.True(t, docRev.History != nil)
	goassert.True(t, docRev.Channels != nil)
	goassert.Equals(t, err, error(nil))
	goassert.Equals(t, callsToLoader, 2)

	docRev, err = cache.Get("Peter", "1")
	goassert.DeepEquals(t, docRev.Body, Body(nil))
	goassert.DeepEquals(t, err, base.HTTPErrorf(404, "missing"))
	goassert.Equals(t, callsToLoader, 3)
}

// Ensure internal properties aren't being incorrectly stored in revision cache
func TestRevisionCacheInternalProperties(t *testing.T) {

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Invalid _revisions property will be stripped.  Should also not be present in the rev cache.
	rev1body := Body{
		"value":       1234,
		BodyRevisions: "unexpected data",
	}
	rev1id, err := db.Put("doc1", rev1body)
	assert.NoError(t, err, "Put")

	// Get the raw document directly from the bucket, validate _revisions property isn't found
	var bucketBody Body
	testBucket.Bucket.Get("doc1", &bucketBody)
	_, ok := bucketBody[BodyRevisions]
	if ok {
		t.Error("_revisions property still present in document retrieved directly from bucket.")
	}

	// Get the doc while still resident in the rev cache w/ history=false, validate _revisions property isn't found
	body, err := db.GetRev("doc1", rev1id, false, nil)
	assert.NoError(t, err, "GetRev")
	badRevisions, ok := body[BodyRevisions]
	if ok {
		t.Errorf("_revisions property still present in document retrieved from rev cache: %s", badRevisions)
	}

	// Get the doc while still resident in the rev cache w/ history=true, validate _revisions property is returned with expected
	// properties ("start", "ids")
	bodyWithHistory, err := db.GetRev("doc1", rev1id, true, nil)
	assert.NoError(t, err, "GetRev")
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

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestRevisionImmutableDelta(t *testing.T) {
	loader := func(id IDAndRev) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error) {
		body = Body{
			BodyId:  id.DocID,
			BodyRev: id.RevID,
		}
		history = Revisions{RevisionsStart: 1}
		channels = base.SetOf("*")
		return
	}
	cache := NewRevisionCache(10, loader, nil)

	firstDelta := []byte("delta")
	secondDelta := []byte("modified delta")

	// Trigger load into cache
	_, err := cache.Get("doc1", "rev1")
	assert.NoError(t, err, "Error adding to cache")
	cache.UpdateDelta("doc1", "rev1", "rev2", firstDelta)

	// Retrieve from cache
	retrievedRev, err := cache.Get("doc1", "rev1")
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Update delta again, validate data in retrievedRev isn't mutated
	cache.UpdateDelta("doc1", "rev1", "rev3", secondDelta)
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Retrieve again, validate delta is correct
	updatedRev, err := cache.Get("doc1", "rev1")
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev3", updatedRev.Delta.ToRevID)
	assert.Equal(t, secondDelta, updatedRev.Delta.DeltaBytes)

	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

}

func BenchmarkRevisionCacheRead(b *testing.B) {

	//Create test document
	loader := func(id IDAndRev) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error) {
		body = Body{
			BodyId:  id.DocID,
			BodyRev: id.RevID,
		}
		history = Revisions{RevisionsStart: 1}
		channels = base.SetOf("*")
		return
	}
	cache := NewRevisionCache(5000, loader, nil)

	// trigger load into cache

	_, err := cache.Get("doc1", "rev1")
	assert.NoError(b, err, "Error initializing cache for BenchmarkRevisionCacheRead")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		//GET the document until test run has completed
		for pb.Next() {
			docrev, err := cache.Get("doc1", "rev1")
			if err != nil {
				assert.Fail(b, "Unexpected error for docrev:%+v", docrev)
			}
		}
	})
}
