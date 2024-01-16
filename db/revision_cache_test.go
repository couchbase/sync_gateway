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
	"github.com/couchbase/sync_gateway/channels"
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
			return nil, ErrMissing
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
	doc.Channels = channels.ChannelMap{
		"*": &channels.ChannelRemoval{RevID: doc.CurrentRev},
	}
	// currentRevChannels usually populated on JSON unmarshal
	doc.currentRevChannels = doc.getCurrentChannels()

	doc.HLV = &HybridLogicalVector{
		SourceID: "test",
		Version:  123,
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

func (t *testBackingStore) getCurrentVersion(ctx context.Context, doc *Document) ([]byte, Body, AttachmentsMeta, error) {
	t.getRevisionCounter.Add(1)

	b := Body{
		"testing":         true,
		BodyId:            doc.ID,
		BodyRev:           doc.CurrentRev,
		"current_version": &Version{Value: doc.HLV.Version, SourceID: doc.HLV.SourceID},
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

func (*noopBackingStore) getCurrentVersion(ctx context.Context, doc *Document) ([]byte, Body, AttachmentsMeta, error) {
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
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", CV: &Version{Value: uint64(docID), SourceID: "test"}, History: Revisions{"start": 1}})
	}

	// Get them back out
	for i := 0; i < 10; i++ {
		docID := strconv.Itoa(i)
		docRev, err := cache.GetWithRev(ctx, docID, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", docID)
		assert.Equal(t, docID, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, int64(i+1), cacheHitCounter.Value())
	}

	// Add 3 more docs to the now full revcache
	for i := 10; i < 13; i++ {
		docID := strconv.Itoa(i)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: docID, RevID: "1-abc", CV: &Version{Value: uint64(i), SourceID: "test"}, History: Revisions{"start": 1}})
	}

	// Check that the first 3 docs were evicted
	prevCacheHitCount := cacheHitCounter.Value()
	for i := 0; i < 3; i++ {
		docID := strconv.Itoa(i)
		docRev, ok := cache.Peek(ctx, docID, "1-abc")
		assert.False(t, ok)
		assert.Nil(t, docRev.BodyBytes)
		assert.Equal(t, int64(0), cacheMissCounter.Value()) // peek incurs no cache miss if not found
		assert.Equal(t, prevCacheHitCount, cacheHitCounter.Value())
	}

	// and check we can Get up to and including the last 3 we put in
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		docRev, err := cache.GetWithRev(ctx, id, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", id)
		assert.Equal(t, id, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, prevCacheHitCount+int64(i)+1, cacheHitCounter.Value())
	}
}

// TestLRURevisionCacheEvictionMixedRevAndCV:
//   - Add 10 docs to the cache
//   - Assert that the cache list and relevant lookup maps have correct lengths
//   - Add 3 more docs
//   - Assert that lookup maps and the cache list still only have 10 elements in
//   - Perform a Get with CV specified on all 10 elements in the cache and assert we get a hit for each element and no misses,
//     testing the eviction worked correct
//   - Then do the same but for rev lookup
func TestLRURevisionCacheEvictionMixedRevAndCV(t *testing.T) {
	cacheHitCounter, cacheMissCounter := base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &noopBackingStore{}, &cacheHitCounter, &cacheMissCounter)

	ctx := base.TestCtx(t)

	// Fill up the rev cache with the first 10 docs
	for docID := 0; docID < 10; docID++ {
		id := strconv.Itoa(docID)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", CV: &Version{Value: uint64(docID), SourceID: "test"}, History: Revisions{"start": 1}})
	}

	// assert that the list has 10 elements along with both lookup maps
	assert.Equal(t, 10, len(cache.hlvCache))
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, cache.lruList.Len())

	// Add 3 more docs to the now full rev cache to trigger eviction
	for docID := 10; docID < 13; docID++ {
		id := strconv.Itoa(docID)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", CV: &Version{Value: uint64(docID), SourceID: "test"}, History: Revisions{"start": 1}})
	}
	// assert the cache and associated lookup maps only have 10 items in them (i.e.e is eviction working?)
	assert.Equal(t, 10, len(cache.hlvCache))
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, cache.lruList.Len())

	// assert we can get a hit on all 10 elements in the cache by CV lookup
	prevCacheHitCount := cacheHitCounter.Value()
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		cv := Version{Value: uint64(i + 3), SourceID: "test"}
		docRev, err := cache.GetWithCV(ctx, id, &cv, RevCacheOmitBody, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", id)
		assert.Equal(t, id, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, prevCacheHitCount+int64(i)+1, cacheHitCounter.Value())
	}

	// now do same but for rev lookup
	prevCacheHitCount = cacheHitCounter.Value()
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		docRev, err := cache.GetWithRev(ctx, id, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
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
	docRev, err := cache.GetWithRev(base.TestCtx(t), "Jens", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Doc doesn't exist, so miss the cache, and fail when getting the doc
	docRev, err = cache.GetWithRev(base.TestCtx(t), "Peter", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev is already resident, but still issue GetDocument to check for later revisions
	docRev, err = cache.GetWithRev(base.TestCtx(t), "Jens", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, err = cache.GetWithRev(base.TestCtx(t), "Peter", "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(3), cacheMissCounter.Value())
	assert.Equal(t, int64(3), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())
}

// TestBackingStoreCV:
// - Perform a Get on a doc by cv that is not currently in the rev cache, assert we get cache miss
// - Perform a Get again on the same doc and assert we get cache hit
// - Perform a Get on doc that doesn't exist, so misses cache and will fail on retrieving doc from bucket
// - Try a Get again on the same doc and assert it wasn't loaded into the cache as it doesn't exist
func TestBackingStoreCV(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"not_found"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	cv := Version{SourceID: "test", Value: 123}
	docRev, err := cache.GetWithCV(base.TestCtx(t), "doc1", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Perform a get on the same doc as above, check that we get cache hit
	docRev, err = cache.GetWithCV(base.TestCtx(t), "doc1", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Doc doesn't exist, so miss the cache, and fail when getting the doc
	cv = Version{SourceID: "test11", Value: 100}
	docRev, err = cache.GetWithCV(base.TestCtx(t), "not_found", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, err = cache.GetWithCV(base.TestCtx(t), "not_found", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(3), cacheMissCounter.Value())
	assert.Equal(t, int64(3), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())
}

// Ensure internal properties aren't being incorrectly stored in revision cache
func TestRevisionCacheInternalProperties(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	// Invalid _revisions property will be stripped.  Should also not be present in the rev cache.
	rev1body := Body{
		"value":       1234,
		BodyRevisions: "unexpected data",
	}
	rev1id, _, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	// Get the raw document directly from the bucket, validate _revisions property isn't found
	var bucketBody Body
	_, err = collection.dataStore.Get("doc1", &bucketBody)
	require.NoError(t, err)
	_, ok := bucketBody[BodyRevisions]
	if ok {
		t.Error("_revisions property still present in document retrieved directly from bucket.")
	}

	// Get the doc while still resident in the rev cache w/ history=false, validate _revisions property isn't found
	body, err := collection.Get1xRevBody(ctx, "doc1", rev1id, false, nil)
	assert.NoError(t, err, "Get1xRevBody")
	badRevisions, ok := body[BodyRevisions]
	if ok {
		t.Errorf("_revisions property still present in document retrieved from rev cache: %s", badRevisions)
	}

	// Get the doc while still resident in the rev cache w/ history=true, validate _revisions property is returned with expected
	// properties ("start", "ids")
	bodyWithHistory, err := collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	validRevisions, ok := bodyWithHistory[BodyRevisions]
	if !ok {
		t.Errorf("Expected _revisions property not found in document retrieved from rev cache: %s", validRevisions)
	}

	validRevisionsMap, ok := validRevisions.(Revisions)
	require.True(t, ok)
	_, startOk := validRevisionsMap[RevisionsStart]
	assert.True(t, startOk)
	_, idsOk := validRevisionsMap[RevisionsIds]
	assert.True(t, idsOk)
}

func TestBypassRevisionCache(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection := GetSingleDatabaseCollectionWithUser(t, db)

	docBody := Body{
		"value": 1234,
	}
	key := "doc1"
	rev1, _, err := collection.Put(ctx, key, docBody)
	assert.NoError(t, err)

	docBody["_rev"] = rev1
	docBody["value"] = 5678
	rev2, _, err := collection.Put(ctx, key, docBody)
	assert.NoError(t, err)

	bypassStat := base.SgwIntStat{}
	rc := NewBypassRevisionCache(collection, &bypassStat)

	// Peek always returns false for BypassRevisionCache
	_, ok := rc.Peek(base.TestCtx(t), key, rev1)
	assert.False(t, ok)
	_, ok = rc.Peek(base.TestCtx(t), key, rev2)
	assert.False(t, ok)

	// Get non-existing doc
	_, err = rc.GetWithRev(base.TestCtx(t), "invalid", rev1, RevCacheOmitBody, RevCacheOmitDelta)
	assert.True(t, base.IsDocNotFoundError(err))

	// Get non-existing revision
	_, err = rc.GetWithRev(base.TestCtx(t), key, "3-abc", RevCacheOmitBody, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)

	// Get specific revision
	doc, err := rc.GetWithRev(base.TestCtx(t), key, rev1, RevCacheOmitBody, RevCacheOmitDelta)
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

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	rev1body := Body{
		"value":         1234,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	rev1key := "doc1"
	rev1id, _, err := collection.Put(ctx, rev1key, rev1body)
	assert.NoError(t, err, "Unexpected error calling collection.Put")

	// Get the raw document directly from the bucket, validate _attachments property isn't found
	var bucketBody Body
	_, err = collection.dataStore.Get(rev1key, &bucketBody)
	assert.NoError(t, err, "Unexpected error calling bucket.Get")
	_, ok := bucketBody[BodyAttachments]
	assert.False(t, ok, "_attachments property still present in document body retrieved from bucket: %#v", bucketBody)

	// Get the raw document directly from the revcache, validate _attachments property isn't found
	docRevision, ok := collection.revisionCache.Peek(base.TestCtx(t), rev1key, rev1id)
	assert.True(t, ok)
	assert.NotContains(t, docRevision.BodyBytes, BodyAttachments, "_attachments property still present in document body retrieved from rev cache: %#v", bucketBody)
	_, ok = docRevision.Attachments["myatt"]
	assert.True(t, ok, "'myatt' not found in revcache attachments metadata")

	// db.getRev stamps _attachments back in from revcache Attachment metadata
	body, err := collection.Get1xRevBody(ctx, rev1key, rev1id, false, nil)
	assert.NoError(t, err, "Unexpected error calling collection.Get1xRevBody")
	atts, ok := body[BodyAttachments]
	assert.True(t, ok, "_attachments property was not stamped back in body during collection.Get1xRevBody: %#v", body)

	attsMap, ok := atts.(AttachmentsMeta)
	require.True(t, ok)
	_, ok = attsMap["myatt"]
	assert.True(t, ok, "'myatt' not found in attachment map")
}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via PutExistingRev
func TestPutExistingRevRevisionCacheAttachmentProperty(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	docKey := "doc1"
	rev1body := Body{
		"value": 1234,
	}
	rev1id, _, err := collection.Put(ctx, docKey, rev1body)
	assert.NoError(t, err, "Unexpected error calling collection.Put")

	rev2id := "2-xxx"
	rev2body := Body{
		"value":         1235,
		BodyAttachments: map[string]interface{}{"myatt": map[string]interface{}{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	_, _, err = collection.PutExistingRevWithBody(ctx, docKey, rev2body, []string{rev2id, rev1id}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Unexpected error calling collection.PutExistingRev")

	// Get the raw document directly from the bucket, validate _attachments property isn't found
	var bucketBody Body
	_, err = collection.dataStore.Get(docKey, &bucketBody)
	assert.NoError(t, err, "Unexpected error calling bucket.Get")
	_, ok := bucketBody[BodyAttachments]
	assert.False(t, ok, "_attachments property still present in document body retrieved from bucket: %#v", bucketBody)

	// Get the raw document directly from the revcache, validate _attachments property isn't found
	docRevision, err := collection.revisionCache.GetWithRev(base.TestCtx(t), docKey, rev2id, RevCacheOmitBody, RevCacheOmitDelta)
	assert.NoError(t, err, "Unexpected error calling collection.revisionCache.Get")
	assert.NotContains(t, docRevision.BodyBytes, BodyAttachments, "_attachments property still present in document body retrieved from rev cache: %#v", bucketBody)
	_, ok = docRevision.Attachments["myatt"]
	assert.True(t, ok, "'myatt' not found in revcache attachments metadata")

	// db.getRev stamps _attachments back in from revcache Attachment metadata
	body, err := collection.Get1xRevBody(ctx, docKey, rev2id, false, nil)
	assert.NoError(t, err, "Unexpected error calling collection.Get1xRevBody")
	atts, ok := body[BodyAttachments]
	assert.True(t, ok, "_attachments property was not stamped back in body during collection.Get1xRevBody: %#v", body)

	attsMap, ok := atts.(AttachmentsMeta)
	require.True(t, ok)
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
	_, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error adding to cache")
	cache.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", RevisionDelta{ToRevID: "rev2", DeltaBytes: firstDelta})

	// Retrieve from cache
	retrievedRev, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Update delta again, validate data in retrievedRev isn't mutated
	cache.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", RevisionDelta{ToRevID: "rev3", DeltaBytes: secondDelta})
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Retrieve again, validate delta is correct
	updatedRev, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
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

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc123", RevID: "1-abc", CV: &Version{Value: uint64(123), SourceID: "test"}, History: Revisions{"start": 1}})
	_, err := cache.GetWithRev(base.TestCtx(t), "doc123", "1-abc", true, false)
	assert.NoError(t, err)
}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestConcurrentLoad(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc1", RevID: "1-abc", CV: &Version{Value: uint64(1234), SourceID: "test"}, History: Revisions{"start": 1}})

	// Trigger load into cache
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			_, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", true, false)
			assert.NoError(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

}

func TestRevisionCacheRemove(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	rev1id, _, err := collection.Put(ctx, "doc", Body{"val": 123})
	assert.NoError(t, err)

	docRev, err := collection.revisionCache.GetWithRev(base.TestCtx(t), "doc", rev1id, true, true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheMisses.Value())

	collection.revisionCache.RemoveWithRev("doc", rev1id)

	docRev, err = collection.revisionCache.GetWithRev(base.TestCtx(t), "doc", rev1id, true, true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.revisionCache.GetActive(base.TestCtx(t), "doc", true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.GetRev(ctx, "doc", docRev.RevID, true, nil)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.GetRev(ctx, "doc", "", true, nil)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())
}

// TestRevCacheOperationsCV:
//   - Create doc revision, put the revision into the cache
//   - Perform a get on that doc by cv and assert that it has correctly been handled
//   - Updated doc revision and upsert the cache
//   - Get the updated doc by cv and assert iot has been correctly handled
//   - Peek the doc by cv and assert it has been found
//   - Peek the rev id cache for the same doc and assert that doc also has been updated in that lookup cache
//   - Remove the doc by cv, and asser that the doc is gone
func TestRevCacheOperationsCV(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	cv := Version{SourceID: "test", Value: 123}
	documentRevision := DocumentRevision{
		DocID:     "doc1",
		RevID:     "1-abc",
		BodyBytes: []byte(`{"test":"1234"}`),
		Channels:  base.SetOf("chan1"),
		History:   Revisions{"start": 1},
		CV:        &cv,
	}
	cache.Put(base.TestCtx(t), documentRevision)

	docRev, err := cache.GetWithCV(base.TestCtx(t), "doc1", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, base.SetOf("chan1"), docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(0), cacheMissCounter.Value())

	documentRevision.BodyBytes = []byte(`{"test":"12345"}`)

	cache.Upsert(base.TestCtx(t), documentRevision)

	docRev, err = cache.GetWithCV(base.TestCtx(t), "doc1", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, base.SetOf("chan1"), docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, []byte(`{"test":"12345"}`), docRev.BodyBytes)
	assert.Equal(t, int64(2), cacheHitCounter.Value())
	assert.Equal(t, int64(0), cacheMissCounter.Value())

	// remove the doc rev from the cache and assert that the document is no longer present in cache
	cache.RemoveWithCV("doc1", &cv)
	assert.Equal(t, 0, len(cache.cache))
	assert.Equal(t, 0, len(cache.hlvCache))
	assert.Equal(t, 0, cache.lruList.Len())
}

func BenchmarkRevisionCacheRead(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelDebug, base.KeyAll)

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(5000, &testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	ctx := base.TestCtx(b)

	// trigger load into cache
	for i := 0; i < 5000; i++ {
		_, _ = cache.GetWithRev(ctx, fmt.Sprintf("doc%d", i), "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			docId := fmt.Sprintf("doc%d", rand.Intn(5000))
			_, _ = cache.GetWithRev(ctx, docId, "1-abc", RevCacheOmitBody, RevCacheOmitDelta)
		}
	})
}

// TestLoaderMismatchInCV:
//   - Get doc that is not in cache by CV to trigger a load from bucket
//   - Ensure the CV passed into the GET operation won't match the doc in the bucket
//   - Assert we get error and the value is not loaded into the cache
func TestLoaderMismatchInCV(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	// create cv with incorrect version to the one stored in backing store
	cv := Version{SourceID: "test", Value: 1234}

	_, err := cache.GetWithCV(base.TestCtx(t), "doc1", &cv, RevCacheOmitBody, RevCacheOmitDelta)
	require.Error(t, err)
	assert.ErrorContains(t, err, "mismatch between specified current version and fetched document current version for doc")
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, 0, cache.lruList.Len())
	assert.Equal(t, 0, len(cache.hlvCache))
	assert.Equal(t, 0, len(cache.cache))
}

// TestConcurrentLoadByCVAndRevOnCache:
//   - Create cache
//   - Now perform two concurrent Gets, one by CV and one by revid on a document that doesn't exist in the cache
//   - This will trigger two concurrent loads from bucket in the CV code path and revid code path
//   - In doing so we will have two processes trying to update lookup maps at the same time and a race condition will appear
//   - In doing so will cause us to potentially have two of the same elements the cache, one with nothing referencing it
//   - Assert after both gets are processed, that the cache only has one element in it and that both lookup maps have only one
//     element
//   - Grab the single element in the list and assert that both maps point to that element in the cache list
func TestConcurrentLoadByCVAndRevOnCache(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	ctx := base.TestCtx(t)

	wg := sync.WaitGroup{}
	wg.Add(2)

	cv := Version{SourceID: "test", Value: 123}
	go func() {
		_, err := cache.GetWithRev(ctx, "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := cache.GetWithCV(ctx, "doc1", &cv, RevCacheOmitBody, RevCacheIncludeDelta)
		require.NoError(t, err)
		wg.Done()
	}()

	wg.Wait()

	revElement := cache.cache[IDAndRev{RevID: "1-abc", DocID: "doc1"}]
	cvElement := cache.hlvCache[IDandCV{DocID: "doc1", Source: "test", Version: 123}]
	assert.Equal(t, 1, cache.lruList.Len())
	assert.Equal(t, 1, len(cache.cache))
	assert.Equal(t, 1, len(cache.hlvCache))
	// grab the single elem in the cache list
	cacheElem := cache.lruList.Front()
	// assert that both maps point to the same element in cache list
	assert.Equal(t, cacheElem, cvElement)
	assert.Equal(t, cacheElem, revElement)
}

// TestGetActive:
//   - Create db, create a doc on the db
//   - Call GetActive pn the rev cache and assert that the rev and cv are correct
func TestGetActive(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	rev1id, doc, err := collection.Put(ctx, "doc", Body{"val": 123})
	require.NoError(t, err)

	expectedCV := Version{
		SourceID: db.BucketUUID,
		Value:    doc.Cas,
	}

	// remove the entry form the rev cache to force the cache to not have the active version in it
	collection.revisionCache.RemoveWithCV("doc", &expectedCV)

	// call get active to get the active version from the bucket
	docRev, err := collection.revisionCache.GetActive(base.TestCtx(t), "doc", true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, expectedCV, *docRev.CV)
}

// TestConcurrentPutAndGetOnRevCache:
//   - Perform a Get with rev on the cache for a doc not in the cache
//   - Concurrently perform a PUT on the cache with doc revision the same as the GET
//   - Assert we get consistent cache with only 1 entry in lookup maps and the cache itself
func TestConcurrentPutAndGetOnRevCache(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cache := NewLRURevisionCache(10, &testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, &cacheHitCounter, &cacheMissCounter)

	ctx := base.TestCtx(t)

	wg := sync.WaitGroup{}
	wg.Add(2)

	cv := Version{SourceID: "test", Value: 123}
	docRev := DocumentRevision{
		DocID:     "doc1",
		RevID:     "1-abc",
		BodyBytes: []byte(`{"test":"1234"}`),
		Channels:  base.SetOf("chan1"),
		History:   Revisions{"start": 1},
		CV:        &cv,
	}

	go func() {
		_, err := cache.GetWithRev(ctx, "doc1", "1-abc", RevCacheOmitBody, RevCacheIncludeDelta)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		cache.Put(ctx, docRev)
		wg.Done()
	}()

	wg.Wait()

	revElement := cache.cache[IDAndRev{RevID: "1-abc", DocID: "doc1"}]
	cvElement := cache.hlvCache[IDandCV{DocID: "doc1", Source: "test", Version: 123}]

	assert.Equal(t, 1, cache.lruList.Len())
	assert.Equal(t, 1, len(cache.cache))
	assert.Equal(t, 1, len(cache.hlvCache))
	cacheElem := cache.lruList.Front()
	// assert that both maps point to the same element in cache list
	assert.Equal(t, cacheElem, cvElement)
	assert.Equal(t, cacheElem, revElement)
}
