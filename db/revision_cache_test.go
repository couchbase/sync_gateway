/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBackingStore always returns an empty doc at rev:"1-abc" in channel "*" except for docs not in 'notFoundDocIDs'
type testBackingStore struct {
	// notFoundDocIDs is a list of doc IDs that GetDocument returns a 404 for.
	notFoundDocIDs     []string
	getDocumentCounter *base.SgwIntStat
	getRevisionCounter *base.SgwIntStat
}

func (t *testBackingStore) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
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

func (t *testBackingStore) getRevision(ctx context.Context, doc *Document, revid string) ([]byte, Body, AttachmentsMeta, error) {
	t.getRevisionCounter.Add(1)

	b := Body{
		"testing":     true,
		BodyId:        doc.ID,
		BodyRev:       doc.CurrentRev,
		BodyRevisions: Revisions{RevisionsStart: 1},
	}
	bodyBytes, err := base.JSONMarshal(b)
	return bodyBytes, b, nil, err
}

type noopBackingStore struct{}

func (*noopBackingStore) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	return nil, nil
}

func (*noopBackingStore) getRevision(ctx context.Context, doc *Document, revid string) ([]byte, Body, AttachmentsMeta, error) {
	return nil, nil, nil, nil
}

// Tests the eviction from the LRURevisionCache
func TestLRURevisionCacheEviction(t *testing.T) {
	cacheHitCounter, cacheMissCounter := base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &noopBackingStore{}, &cacheHitCounter, &cacheMissCounter)

	ctx := base.TestCtx(t)

	// Fill up the rev cache with the first 10 docs
	for docID := 0; docID < 10; docID++ {
		id := strconv.Itoa(docID)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", History: Revisions{"start": 1}})
	}

	// Get them back out
	for i := 0; i < 10; i++ {
		docID := strconv.Itoa(i)
		docRev, err := cache.Get(ctx, docID, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", docID)
		assert.Equal(t, docID, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, int64(i+1), cacheHitCounter.Value())
	}

	// Add 3 more docs to the now full revcache
	for i := 10; i < 13; i++ {
		docID := strconv.Itoa(i)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: docID, RevID: "1-abc", History: Revisions{"start": 1}})
	}

	// Check that the first 3 docs were evicted
	prevCacheHitCount := cacheHitCounter.Value()
	for i := 0; i < 3; i++ {
		docID := strconv.Itoa(i)
		docRev, ok := cache.Peek(ctx, docID, "1-abc")
		assert.False(t, ok)
		assert.Nil(t, docRev.BodyBytes)
		assert.Equal(t, int64(0), cacheMissCounter.Value()) // peek incurs no cache miss if not found
		assert.Equal(t, int64(prevCacheHitCount), cacheHitCounter.Value())
	}

	// and check we can Get up to and including the last 3 we put in
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		docRev, err := cache.Get(ctx, id, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", id)
		assert.Equal(t, id, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, prevCacheHitCount+int64(i)+1, cacheHitCounter.Value())
	}
}

func TestBackingStore(t *testing.T) {

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"Peter"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	docRev, err := cache.Get(base.TestCtx(t), "Jens", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Doc doesn't exist, so miss the cache, and fail when getting the doc
	docRev, err = cache.Get(base.TestCtx(t), "Peter", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev is already resident, but still issue GetDocument to check for later revisions
	docRev, err = cache.Get(base.TestCtx(t), "Jens", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, err = cache.Get(base.TestCtx(t), "Peter", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(3), cacheMissCounter.Value())
	assert.Equal(t, int64(3), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())
}

// Ensure internal properties aren't being incorrectly stored in revision cache
func TestRevisionCacheInternalProperties(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	// Invalid _revisions property will be stripped.  Should also not be present in the rev cache.
	rev1body := Body{
		"value":       1234,
		BodyRevisions: "unexpected data",
	}
	rev1id, _, err := db.Put("doc1", rev1body)
	assert.NoError(t, err, "Put")

	// Get the raw document directly from the bucket, validate _revisions property isn't found
	var bucketBody Body
	_, err = db.Bucket.Get("doc1", &bucketBody)
	require.NoError(t, err)
	_, ok := bucketBody[BodyRevisions]
	if ok {
		t.Error("_revisions property still present in document retrieved directly from bucket.")
	}

	// Get the doc while still resident in the rev cache w/ history=false, validate _revisions property isn't found
	body, err := db.Get1xRevBody("doc1", rev1id, false, nil)
	assert.NoError(t, err, "Get1xRevBody")
	badRevisions, ok := body[BodyRevisions]
	if ok {
		t.Errorf("_revisions property still present in document retrieved from rev cache: %s", badRevisions)
	}

	// Get the doc while still resident in the rev cache w/ history=true, validate _revisions property is returned with expected
	// properties ("start", "ids")
	bodyWithHistory, err := db.Get1xRevBody("doc1", rev1id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	validRevisions, ok := bodyWithHistory[BodyRevisions]
	if !ok {
		t.Errorf("Expected _revisions property not found in document retrieved from rev cache: %s", validRevisions)
	}

	validRevisionsMap, ok := validRevisions.(Revisions)
	_, startOk := validRevisionsMap[RevisionsStart]
	assert.True(t, startOk)
	_, idsOk := validRevisionsMap[RevisionsIds]
	assert.True(t, idsOk)
}

func TestBypassRevisionCache(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db := setupTestDB(t)
	defer db.Close()

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

	bypassStat := base.SgwIntStat{}
	rc := NewBypassRevisionCache(db.DatabaseContext, &bypassStat)

	// Peek always returns false for BypassRevisionCache
	_, ok := rc.Peek(base.TestCtx(t), key, rev1)
	assert.False(t, ok)
	_, ok = rc.Peek(base.TestCtx(t), key, rev2)
	assert.False(t, ok)

	// Get non-existing doc
	doc, err := rc.Get(base.TestCtx(t), "invalid", rev1, RevCacheOmitBody, RevCacheOmitDelta)
	assert.True(t, base.IsDocNotFoundError(err))

	// Get non-existing revision
	doc, err = rc.Get(base.TestCtx(t), key, "3-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)

	// Get specific revision
	doc, err = rc.Get(base.TestCtx(t), key, rev1, RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	require.NotNil(t, doc)
	assert.Equal(t, `{"value":1234}`, string(doc.BodyBytes))

	// Check peek is still returning false for "Get"
	_, ok = rc.Peek(base.TestCtx(t), key, rev1)
	assert.False(t, ok)

	// Put no-ops
	rc.Put(base.TestCtx(t), doc)

	// Check peek is still returning false for "Put"
	_, ok = rc.Peek(base.TestCtx(t), key, rev1)
	assert.False(t, ok)

	// Get active revision
	doc, err = rc.GetActive(base.TestCtx(t), key, false)
	assert.NoError(t, err)
	assert.Equal(t, `{"value":5678}`, string(doc.BodyBytes))

}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via Put
func TestPutRevisionCacheAttachmentProperty(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db := setupTestDB(t)
	defer db.Close()

	rev1body := Body{
		"value":         1234,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	rev1key := "doc1"
	rev1id, _, err := db.Put(rev1key, rev1body)
	assert.NoError(t, err, "Unexpected error calling db.Put")

	// Get the raw document directly from the bucket, validate _attachments property isn't found
	var bucketBody Body
	_, err = db.Bucket.Get(rev1key, &bucketBody)
	assert.NoError(t, err, "Unexpected error calling bucket.Get")
	_, ok := bucketBody[BodyAttachments]
	assert.False(t, ok, "_attachments property still present in document body retrieved from bucket: %#v", bucketBody)

	// Get the raw document directly from the revcache, validate _attachments property isn't found
	docRevision, ok := db.revisionCache.Peek(base.TestCtx(t), rev1key, rev1id)
	assert.True(t, ok)
	assert.NotContains(t, docRevision.BodyBytes, BodyAttachments, "_attachments property still present in document body retrieved from rev cache: %#v", bucketBody)
	_, ok = docRevision.Attachments["myatt"]
	assert.True(t, ok, "'myatt' not found in revcache attachments metadata")

	// db.getRev stamps _attachments back in from revcache Attachment metadata
	body, err := db.Get1xRevBody(rev1key, rev1id, false, nil)
	assert.NoError(t, err, "Unexpected error calling db.Get1xRevBody")
	atts, ok := body[BodyAttachments]
	assert.True(t, ok, "_attachments property was not stamped back in body during db.Get1xRevBody: %#v", body)

	attsMap, ok := atts.(AttachmentsMeta)
	_, ok = attsMap["myatt"]
	assert.True(t, ok, "'myatt' not found in attachment map")
}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via PutExistingRev
func TestPutExistingRevRevisionCacheAttachmentProperty(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db := setupTestDB(t)
	defer db.Close()

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
	_, _, err = db.PutExistingRevWithBody(docKey, rev2body, []string{rev2id, rev1id}, false)
	assert.NoError(t, err, "Unexpected error calling db.PutExistingRev")

	// Get the raw document directly from the bucket, validate _attachments property isn't found
	var bucketBody Body
	_, err = db.Bucket.Get(docKey, &bucketBody)
	assert.NoError(t, err, "Unexpected error calling bucket.Get")
	_, ok := bucketBody[BodyAttachments]
	assert.False(t, ok, "_attachments property still present in document body retrieved from bucket: %#v", bucketBody)

	// Get the raw document directly from the revcache, validate _attachments property isn't found
	docRevision, err := db.revisionCache.Get(base.TestCtx(t), docKey, rev2id, RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err, "Unexpected error calling db.revisionCache.Get")
	assert.NotContains(t, docRevision.BodyBytes, BodyAttachments, "_attachments property still present in document body retrieved from rev cache: %#v", bucketBody)
	_, ok = docRevision.Attachments["myatt"]
	assert.True(t, ok, "'myatt' not found in revcache attachments metadata")

	// db.getRev stamps _attachments back in from revcache Attachment metadata
	body, err := db.Get1xRevBody(docKey, rev2id, false, nil)
	assert.NoError(t, err, "Unexpected error calling db.Get1xRevBody")
	atts, ok := body[BodyAttachments]
	assert.True(t, ok, "_attachments property was not stamped back in body during db.Get1xRevBody: %#v", body)

	attsMap, ok := atts.(AttachmentsMeta)
	_, ok = attsMap["myatt"]
	assert.True(t, ok, "'myatt' not found in attachment map")
}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestRevisionImmutableDelta(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	firstDelta := []byte("delta")
	secondDelta := []byte("modified delta")

	// Trigger load into cache
	_, err := cache.Get(base.TestCtx(t), "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error adding to cache")
	cache.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", RevisionDelta{ToRevID: "rev2", DeltaBytes: firstDelta})

	// Retrieve from cache
	retrievedRev, err := cache.Get(base.TestCtx(t), "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Update delta again, validate data in retrievedRev isn't mutated
	cache.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", RevisionDelta{ToRevID: "rev3", DeltaBytes: secondDelta})
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Retrieve again, validate delta is correct
	updatedRev, err := cache.Get(base.TestCtx(t), "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev3", updatedRev.Delta.ToRevID)
	assert.Equal(t, secondDelta, updatedRev.Delta.DeltaBytes)

	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestSingleLoad(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc123", RevID: "1-abc", History: Revisions{"start": 1}})
	_, err := cache.Get(base.TestCtx(t), "doc123", "1-abc", true, false)
	assert.NoError(t, err)
}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestConcurrentLoad(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc1", RevID: "1-abc", History: Revisions{"start": 1}})

	// Trigger load into cache
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			_, err := cache.Get(base.TestCtx(t), "doc1", "1-abc", true, false)
			assert.NoError(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

}

func TestInvalidate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rev1id, _, err := db.Put("doc", Body{"val": 123})
	assert.NoError(t, err)

	docRev, err := db.revisionCache.Get(base.TestCtx(t), "doc", rev1id, true, true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.False(t, docRev.Invalid)
	assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheMisses.Value())

	db.revisionCache.Invalidate(base.TestCtx(t), "doc", rev1id)

	docRev, err = db.revisionCache.Get(base.TestCtx(t), "doc", rev1id, true, true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.True(t, docRev.Invalid)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = db.revisionCache.GetActive(base.TestCtx(t), "doc", true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.True(t, docRev.Invalid)
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = db.GetRev("doc", docRev.RevID, true, nil)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.True(t, docRev.Invalid)
	assert.Equal(t, int64(3), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = db.GetRev("doc", "", true, nil)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.True(t, docRev.Invalid)
	assert.Equal(t, int64(4), db.DbStats.Cache().RevisionCacheMisses.Value())
}

func BenchmarkRevisionCacheRead(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelDebug, base.KeyAll)

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(5000, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	ctx := base.TestCtx(b)

	// trigger load into cache
	for i := 0; i < 5000; i++ {
		_, _ = cache.Get(ctx, fmt.Sprintf("doc%d", i), "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			docId := fmt.Sprintf("doc%d", rand.Intn(5000))
			_, _ = cache.Get(ctx, docId, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
		}
	})
}
