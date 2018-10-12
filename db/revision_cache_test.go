package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func TestRevisionCache(t *testing.T) {
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	revForTest := func(i int) (Body, Body, base.Set) {
		body := Body{
			BodyId:  ids[i],
			BodyRev: "x",
		}
		history := Body{"start": i}
		return body, history, nil
	}
	verify := func(body Body, history Body, channels base.Set, i int) {
		if body == nil {
			t.Fatalf("nil body at #%d", i)
		}
		assert.True(t, body != nil)
		assert.Equals(t, body[BodyId], ids[i])
		assert.True(t, history != nil)
		assert.Equals(t, history["start"], i)
		assert.DeepEquals(t, channels, base.Set(nil))
	}

	cache := NewRevisionCache(10, nil)
	for i := 0; i < 10; i++ {
		body, history, channels := revForTest(i)
		cache.Put(body[BodyId].(string), body[BodyRev].(string), body, history, channels)
	}

	for i := 0; i < 10; i++ {
		body, history, channels, _ := cache.Get(ids[i], "x")
		verify(body, history, channels, i)
	}

	for i := 10; i < 13; i++ {
		body, history, channels := revForTest(i)
		cache.Put(body[BodyId].(string), body[BodyRev].(string), body, history, channels)
	}

	for i := 0; i < 3; i++ {
		body, _, _, _ := cache.Get(ids[i], "x")
		assert.True(t, body == nil)
	}
	for i := 3; i < 13; i++ {
		body, history, channels, _ := cache.Get(ids[i], "x")
		verify(body, history, channels, i)
	}
}

func TestLoaderFunction(t *testing.T) {
	var callsToLoader = 0
	loader := func(id IDAndRev) (body Body, history Body, channels base.Set, err error) {
		callsToLoader++
		if id.DocID[0] != 'J' {
			err = base.HTTPErrorf(404, "missing")
		} else {
			body = Body{
				BodyId:  id.DocID,
				BodyRev: id.RevID,
			}
			history = Body{"start": 1}
			channels = base.SetOf("*")
		}
		return
	}
	cache := NewRevisionCache(10, loader)

	body, history, channels, err := cache.Get("Jens", "1")
	assert.Equals(t, body[BodyId], "Jens")
	assert.True(t, history != nil)
	assert.True(t, channels != nil)
	assert.Equals(t, err, error(nil))
	assert.Equals(t, callsToLoader, 1)

	body, history, channels, err = cache.Get("Peter", "1")
	assert.DeepEquals(t, body, Body(nil))
	assert.DeepEquals(t, err, base.HTTPErrorf(404, "missing"))
	assert.Equals(t, callsToLoader, 2)

	body, history, channels, err = cache.Get("Jens", "1")
	assert.Equals(t, body[BodyId], "Jens")
	assert.True(t, history != nil)
	assert.True(t, channels != nil)
	assert.Equals(t, err, error(nil))
	assert.Equals(t, callsToLoader, 2)

	body, history, channels, err = cache.Get("Peter", "1")
	assert.DeepEquals(t, body, Body(nil))
	assert.DeepEquals(t, err, base.HTTPErrorf(404, "missing"))
	assert.Equals(t, callsToLoader, 3)
}

// Ensure internal properties aren't being incorrectly stored in revision cache
func TestRevisionCacheInternalProperties(t *testing.T) {

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Invalid _revisions property will be stripped.  Should also not be present in the rev cache.
	rev1body := Body{
		"value":      1234,
		"_revisions": "unexpected data",
	}
	rev1id, err := db.Put("doc1", rev1body)
	assertNoError(t, err, "Put")

	// Get the raw document directly from the bucket, validate _revisions property isn't found
	var bucketBody Body
	testBucket.Bucket.Get("doc1", &bucketBody)
	_, ok := bucketBody["_revisions"]
	if ok {
		t.Error("_revisions property still present in document retrieved directly from bucket.")
	}

	// Get the doc while still resident in the rev cache w/ history=false, validate _revisions property isn't found
	body, err := db.GetRev("doc1", rev1id, false, nil)
	assertNoError(t, err, "GetRev")
	badRevisions, ok := body["_revisions"]
	if ok {
		t.Errorf("_revisions property still present in document retrieved from rev cache: %s", badRevisions)
	}

	// Get the doc while still resident in the rev cache w/ history=true, validate _revisions property is returned with expected
	// properties ("start", "ids")
	bodyWithHistory, err := db.GetRev("doc1", rev1id, true, nil)
	assertNoError(t, err, "GetRev")
	validRevisions, ok := bodyWithHistory["_revisions"]
	if !ok {
		t.Errorf("Expected _revisions property not found in document retrieved from rev cache: %s", validRevisions)
	}
	validRevisionsMap, ok := validRevisions.(map[string]interface{})
	_, startOk := validRevisionsMap["start"]
	assert.True(t, startOk)
	_, idsOk := validRevisionsMap["ids"]
	assert.True(t, idsOk)
}
