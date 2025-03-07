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
	"log"
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

	doc.HLV = &HybridLogicalVector{
		SourceID: "test",
		Version:  123,
	}
	_, _, err = doc.updateChannels(ctx, base.SetOf("*"))
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func (t *testBackingStore) getRevision(ctx context.Context, doc *Document, revid string) ([]byte, AttachmentsMeta, error) {
	t.getRevisionCounter.Add(1)

	b := Body{
		"testing":     true,
		BodyId:        doc.ID,
		BodyRev:       doc.CurrentRev,
		BodyRevisions: Revisions{RevisionsStart: 1},
	}
	bodyBytes, err := base.JSONMarshal(b)
	return bodyBytes, nil, err
}

func (t *testBackingStore) getCurrentVersion(ctx context.Context, doc *Document, cv Version) ([]byte, AttachmentsMeta, error) {
	t.getRevisionCounter.Add(1)

	b := Body{
		"testing":         true,
		BodyId:            doc.ID,
		BodyRev:           doc.CurrentRev,
		"current_version": &Version{Value: doc.HLV.Version, SourceID: doc.HLV.SourceID},
	}
	if err := doc.HasCurrentVersion(ctx, cv); err != nil {
		return nil, nil, err
	}
	bodyBytes, err := base.JSONMarshal(b)
	return bodyBytes, nil, err
}

type noopBackingStore struct{}

func (*noopBackingStore) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	return nil, nil
}

func (*noopBackingStore) getRevision(ctx context.Context, doc *Document, revid string) ([]byte, AttachmentsMeta, error) {
	return nil, nil, nil
}

func (*noopBackingStore) getCurrentVersion(ctx context.Context, doc *Document, cv Version) ([]byte, AttachmentsMeta, error) {
	return nil, nil, nil
}

// testCollectionID is a test collection ID to use for a key in the backing store map to point to a tests backing store.
// This should only be used in tests that have no database context being created.
const testCollectionID = 0

// CreateTestSingleBackingStoreMap will create map of rev cache backing stores and assign the specified backing store to collection ID specified for testing purposes
func CreateTestSingleBackingStoreMap(backingStore RevisionCacheBackingStore, collectionID uint32) map[uint32]RevisionCacheBackingStore {
	backingStoreMap := make(map[uint32]RevisionCacheBackingStore)
	backingStoreMap[collectionID] = backingStore
	return backingStoreMap
}

// Tests the eviction from the LRURevisionCache
func TestLRURevisionCacheEviction(t *testing.T) {
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	ctx := base.TestCtx(t)

	// Fill up the rev cache with the first 10 docs
	for docID := 0; docID < 10; docID++ {
		id := strconv.Itoa(docID)
		vrs := uint64(docID)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", CV: &Version{Value: vrs, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)
	}
	assert.Equal(t, int64(10), cacheNumItems.Value())
	assert.Equal(t, int64(20), memoryBytesCounted.Value())
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, len(cache.hlvCache))

	// Get them back out
	for i := 0; i < 10; i++ {
		docID := strconv.Itoa(i)
		docRev, err := cache.GetWithRev(ctx, docID, "1-abc", testCollectionID, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", docID)
		assert.Equal(t, docID, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, int64(i+1), cacheHitCounter.Value())
	}
	assert.Equal(t, int64(10), cacheNumItems.Value())
	assert.Equal(t, int64(20), memoryBytesCounted.Value())
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, len(cache.hlvCache))

	// Add 3 more docs to the now full revcache
	for i := 10; i < 13; i++ {
		docID := strconv.Itoa(i)
		vrs := uint64(i)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: docID, RevID: "1-abc", CV: &Version{Value: vrs, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)
	}
	assert.Equal(t, int64(10), cacheNumItems.Value())
	assert.Equal(t, int64(20), memoryBytesCounted.Value())
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, len(cache.hlvCache))

	// Check that the first 3 docs were evicted
	prevCacheHitCount := cacheHitCounter.Value()
	for i := 0; i < 3; i++ {
		docID := strconv.Itoa(i)
		docRev, ok := cache.Peek(ctx, docID, "1-abc", testCollectionID)
		assert.False(t, ok)
		assert.Nil(t, docRev.BodyBytes)
		assert.Equal(t, int64(0), cacheMissCounter.Value()) // peek incurs no cache miss if not found
		assert.Equal(t, prevCacheHitCount, cacheHitCounter.Value())
	}
	assert.Equal(t, int64(10), cacheNumItems.Value())
	assert.Equal(t, int64(20), memoryBytesCounted.Value())
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, len(cache.hlvCache))

	// and check we can Get up to and including the last 3 we put in
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		docRev, err := cache.GetWithRev(ctx, id, "1-abc", testCollectionID, RevCacheOmitDelta)
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

	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	ctx := base.TestCtx(t)

	// Fill up the rev cache with the first 10 docs
	for docID := 0; docID < 10; docID++ {
		id := strconv.Itoa(docID)
		vrs := uint64(docID)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", CV: &Version{Value: vrs, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)
	}

	// assert that the list has 10 elements along with both lookup maps
	assert.Equal(t, 10, len(cache.hlvCache))
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, cache.lruList.Len())

	// Add 3 more docs to the now full rev cache to trigger eviction
	for docID := 10; docID < 13; docID++ {
		id := strconv.Itoa(docID)
		vrs := uint64(docID)
		cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{}`), DocID: id, RevID: "1-abc", CV: &Version{Value: vrs, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)
	}
	// assert the cache and associated lookup maps only have 10 items in them (i.e.e is eviction working?)
	assert.Equal(t, 10, len(cache.hlvCache))
	assert.Equal(t, 10, len(cache.cache))
	assert.Equal(t, 10, cache.lruList.Len())

	// assert we can get a hit on all 10 elements in the cache by CV lookup
	prevCacheHitCount := cacheHitCounter.Value()
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i + 3)
		vrs := uint64(i + 3)
		cv := Version{Value: vrs, SourceID: "test"}
		docRev, err := cache.GetWithCV(ctx, id, &cv, testCollectionID, RevCacheOmitDelta)

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
		docRev, err := cache.GetWithRev(ctx, id, "1-abc", testCollectionID, RevCacheOmitDelta)
		assert.NoError(t, err)
		assert.NotNil(t, docRev.BodyBytes, "nil body for %s", id)
		assert.Equal(t, id, docRev.DocID)
		assert.Equal(t, int64(0), cacheMissCounter.Value())
		assert.Equal(t, prevCacheHitCount+int64(i)+1, cacheHitCounter.Value())
	}
	assert.Equal(t, int64(10), cacheNumItems.Value())
	assert.Equal(t, int64(20), memoryBytesCounted.Value())
}

func TestLRURevisionCacheEvictionMemoryBased(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			dbcOptions := DatabaseContextOptions{
				RevisionCacheOptions: &RevisionCacheOptions{
					MaxBytes:     725,
					MaxItemCount: 10,
				},
			}
			db, ctx := SetupTestDBWithOptions(t, dbcOptions)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			cacheStats := db.DbStats.Cache()

			smallBody := Body{
				"channels": "_default", // add channel for default sync func in default collection test runs
			}

			var currMem, expValue, revZeroSize int64
			var rev1Version *Version
			for i := 0; i < 10; i++ {
				currMem = cacheStats.RevisionCacheTotalMemory.Value()
				revSize, _, docVersion := createDocAndReturnSizeAndRev(t, ctx, fmt.Sprint(i), collection, smallBody)
				if i == 0 {
					revZeroSize = int64(revSize)
				}
				if i == 1 {
					rev1Version = docVersion
				}
				expValue = currMem + int64(revSize)
				assert.Equal(t, expValue, cacheStats.RevisionCacheTotalMemory.Value())
			}

			// test eviction by number of items (adding new doc from createDocAndReturnSizeAndRev shouldn't take memory over threshold defined as 730 bytes)
			expValue -= revZeroSize // for doc being evicted
			docSize, rev, _ := createDocAndReturnSizeAndRev(t, ctx, fmt.Sprint(11), collection, smallBody)
			expValue += int64(docSize)
			// assert doc 0 been evicted
			docRev, ok := db.revisionCache.Peek(ctx, "0", rev, collection.GetCollectionID())
			assert.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)

			currMem = cacheStats.RevisionCacheTotalMemory.Value()
			// assert total memory is as expected
			assert.Equal(t, expValue, currMem)

			// remove doc "1" to give headroom for memory based eviction
			if testCase.UseCVCache {
				db.revisionCache.RemoveWithCV(ctx, "1", rev1Version, collection.GetCollectionID())
			} else {
				db.revisionCache.RemoveWithRev(ctx, "1", rev, collection.GetCollectionID())
			}
			docRev, ok = db.revisionCache.Peek(ctx, "1", rev, collection.GetCollectionID())
			assert.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)

			// assert current memory from rev cache decreases by the doc size (all docs added thus far are same size)
			afterRemoval := currMem - int64(docSize)
			assert.Equal(t, afterRemoval, cacheStats.RevisionCacheTotalMemory.Value())

			// add new doc that will trigger eviction due to taking over memory size
			largeBody := Body{
				"type":     "test",
				"doc":      "testDocument",
				"foo":      "bar",
				"lets":     "test",
				"larger":   "document",
				"for":      "eviction",
				"channels": "_default", // add channel for default sync func in default collection test runs
			}
			_, _, err := collection.Put(ctx, "12", largeBody)
			require.NoError(t, err)

			// assert doc "2" has been evicted even though we only have 9 items in cache with capacity of 10, so memory based
			// eviction took place
			docRev, ok = db.revisionCache.Peek(ctx, "2", rev, collection.GetCollectionID())
			assert.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)

			// assert that the overall memory for rev cache is not over maximum
			assert.LessOrEqual(t, cacheStats.RevisionCacheTotalMemory.Value(), dbcOptions.RevisionCacheOptions.MaxBytes)
		})
	}

}

func TestBackingStoreMemoryCalculation(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"doc2"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			var maxBytes int64
			if testCase.UseCVCache {
				maxBytes = 233
			} else {
				maxBytes = 205
			}
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 10,
				MaxBytes:     maxBytes,
			}
			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)
			ctx := base.TestCtx(t)
			var err error

			var docRev DocumentRevision
			if testCase.UseCVCache {
				docRev, err = cache.GetWithCV(ctx, "doc1", &Version{Value: 123, SourceID: "test"}, testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc1", "1-abc", testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			}
			assert.Equal(t, "doc1", docRev.DocID)
			assert.NotNil(t, docRev.History)
			assert.NotNil(t, docRev.Channels)

			currMemStat := memoryBytesCounted.Value()
			// assert stats is incremented by appropriate bytes on doc rev
			assert.Equal(t, docRev.MemoryBytes, currMemStat)

			// Test get active code pathway of a load from bucket
			docRev, err = cache.GetActive(ctx, "doc", testCollectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc", docRev.DocID)
			assert.NotNil(t, docRev.History)
			assert.NotNil(t, docRev.Channels)

			newMemStat := currMemStat + docRev.MemoryBytes
			// assert stats is incremented by appropriate bytes on doc rev
			assert.Equal(t, newMemStat, memoryBytesCounted.Value())

			// test fail load event doesn't increment memory stat
			if testCase.UseCVCache {
				docRev, err = cache.GetWithCV(ctx, "doc2", &Version{Value: 123, SourceID: "test"}, testCollectionID, RevCacheOmitDelta)
				assertHTTPError(t, err, 404)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc2", "1-abc", testCollectionID, RevCacheOmitDelta)
				assertHTTPError(t, err, 404)
			}
			assert.Nil(t, docRev.BodyBytes)
			assert.Equal(t, newMemStat, memoryBytesCounted.Value())

			// assert length is 2 as expected
			assert.Equal(t, 2, cache.lruList.Len())

			memStatBeforeThirdLoad := memoryBytesCounted.Value()
			// test another load from bucket but doing so should trigger memory based eviction
			if testCase.UseCVCache {
				docRev, err = cache.GetWithCV(ctx, "doc3", &Version{Value: 123, SourceID: "test"}, testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc3", "1-abc", testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			}
			assert.Equal(t, "doc3", docRev.DocID)
			assert.NotNil(t, docRev.History)
			assert.NotNil(t, docRev.Channels)

			// assert length is still 2 (eviction took place) + test Peek for first added doc is failure
			assert.Equal(t, 2, cache.lruList.Len())
			memStatAfterEviction := (memStatBeforeThirdLoad + docRev.MemoryBytes) - currMemStat
			assert.Equal(t, memStatAfterEviction, memoryBytesCounted.Value())
			_, ok := cache.Peek(ctx, "doc1", "1-abc", testCollectionID)
			assert.False(t, ok)
		})
	}
}

func TestBackingStore(t *testing.T) {

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"Peter"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	docRev, err := cache.GetWithRev(base.TestCtx(t), "Jens", "1-abc", testCollectionID, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Doc doesn't exist, so miss the cache, and fail when getting the doc
	docRev, err = cache.GetWithRev(base.TestCtx(t), "Peter", "1-abc", testCollectionID, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev is already resident, but still issue GetDocument to check for later revisions
	docRev, err = cache.GetWithRev(base.TestCtx(t), "Jens", "1-abc", testCollectionID, RevCacheOmitDelta)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, err = cache.GetWithRev(base.TestCtx(t), "Peter", "1-abc", testCollectionID, RevCacheOmitDelta)
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
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}

	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"not_found"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	cv := Version{SourceID: "test", Value: 123}
	docRev, err := cache.GetWithCV(base.TestCtx(t), "doc1", &cv, testCollectionID, RevCacheOmitDelta)
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
	docRev, err = cache.GetWithCV(base.TestCtx(t), "doc1", &cv, testCollectionID, RevCacheOmitDelta)
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
	docRev, err = cache.GetWithCV(base.TestCtx(t), "not_found", &cv, testCollectionID, RevCacheOmitDelta)

	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, err = cache.GetWithCV(base.TestCtx(t), "not_found", &cv, testCollectionID, RevCacheOmitDelta)
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
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

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
	t.Skip("Revs are backed up by hash of CV now, test needs to fetch backup rev by revID, CBG-3748 (backwards compatibility for revID)")
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	backingStoreMap := CreateTestSingleBackingStoreMap(collection, testCollectionID)

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
	rc := NewBypassRevisionCache(backingStoreMap, &bypassStat)

	// Peek always returns false for BypassRevisionCache
	_, ok := rc.Peek(ctx, key, rev1, 0)
	assert.False(t, ok)
	_, ok = rc.Peek(ctx, key, rev2, 0)
	assert.False(t, ok)

	// Get non-existing doc
	_, err = rc.GetWithRev(base.TestCtx(t), "invalid", rev1, testCollectionID, RevCacheOmitDelta)
	assert.True(t, base.IsDocNotFoundError(err))

	// Get non-existing revision
	_, err = rc.GetWithRev(base.TestCtx(t), key, "3-abc", testCollectionID, RevCacheOmitDelta)
	assertHTTPError(t, err, 404)

	// Get specific revision
	doc, err := rc.GetWithRev(base.TestCtx(t), key, rev1, testCollectionID, RevCacheOmitDelta)
	assert.NoError(t, err)
	require.NotNil(t, doc)
	assert.Equal(t, `{"value":1234}`, string(doc.BodyBytes))

	// Check peek is still returning false for "Get"
	_, ok = rc.Peek(ctx, key, rev1, testCollectionID)
	assert.False(t, ok)

	// Put no-ops
	rc.Put(ctx, doc, testCollectionID)

	// Check peek is still returning false for "Put"
	_, ok = rc.Peek(ctx, key, rev1, testCollectionID)
	assert.False(t, ok)

	// Get active revision
	doc, err = rc.GetActive(ctx, key, testCollectionID)
	assert.NoError(t, err)
	assert.Equal(t, `{"value":5678}`, string(doc.BodyBytes))

}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via Put
func TestPutRevisionCacheAttachmentProperty(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

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
	docRevision, ok := collection.revisionCache.Peek(ctx, rev1key, rev1id)
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
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

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
	docRevision, err := collection.revisionCache.GetWithRev(base.TestCtx(t), docKey, rev2id, RevCacheOmitDelta)
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
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	firstDelta := []byte("delta")
	secondDelta := []byte("modified delta")

	// Trigger load into cache
	_, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", testCollectionID, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error adding to cache")
	cache.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", testCollectionID, RevisionDelta{ToRevID: "rev2", DeltaBytes: firstDelta})

	// Retrieve from cache
	retrievedRev, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", testCollectionID, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Update delta again, validate data in retrievedRev isn't mutated
	cache.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", testCollectionID, RevisionDelta{ToRevID: "rev3", DeltaBytes: secondDelta})
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Retrieve again, validate delta is correct
	updatedRev, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", testCollectionID, RevCacheIncludeDelta)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev3", updatedRev.Delta.ToRevID)
	assert.Equal(t, secondDelta, updatedRev.Delta.DeltaBytes)

	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

}

func TestUpdateDeltaRevCacheMemoryStat(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 10,
				MaxBytes:     125,
			}
			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

			firstDelta := []byte("delta")
			secondDelta := []byte("modified delta")
			thirdDelta := []byte("another delta further modified")
			ctx := base.TestCtx(t)

			// Trigger load into cache
			docRev, err := cache.GetWithRev(ctx, "doc1", "1-abc", testCollectionID, RevCacheIncludeDelta)
			assert.NoError(t, err, "Error adding to cache")

			revCacheMem := memoryBytesCounted.Value()
			revCacheDelta := newRevCacheDelta(firstDelta, "1-abc", docRev, false, nil)
			cache.UpdateDelta(ctx, "doc1", "1-abc", testCollectionID, revCacheDelta)
			// assert that rev cache memory increases by expected amount
			newMem := revCacheMem + revCacheDelta.totalDeltaBytes
			assert.Equal(t, newMem, memoryBytesCounted.Value())
			oldDeltaSize := revCacheDelta.totalDeltaBytes

			newMem = memoryBytesCounted.Value()
			revCacheDelta = newRevCacheDelta(secondDelta, "1-abc", docRev, false, nil)
			cache.UpdateDelta(ctx, "doc1", "1-abc", testCollectionID, revCacheDelta)

			// assert the overall memory stat is correctly updated (by the diff between the old delta and the new delta)
			newMem += revCacheDelta.totalDeltaBytes - oldDeltaSize
			assert.Equal(t, newMem, memoryBytesCounted.Value())

			revCacheDelta = newRevCacheDelta(thirdDelta, "1-abc", docRev, false, nil)
			cache.UpdateDelta(ctx, "doc1", "1-abc", testCollectionID, revCacheDelta)

			// assert that eviction took place and as result stat is now 0 (only item in cache was doc1)
			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, 0, cache.lruList.Len())
		})
	}
}

func TestImmediateRevCacheMemoryBasedEviction(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 10,
				MaxBytes:     10,
			}
			ctx := base.TestCtx(t)
			var err error
			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

			cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "doc1", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			cache.Upsert(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "doc2", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			// assert we can still fetch this upsert doc
			var docRev DocumentRevision
			if testCase.UseCVCache {
				docRev, err = cache.GetWithCV(ctx, "doc2", &Version{Value: 123, SourceID: "test"}, testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc2", "1-abc", testCollectionID, false)
				require.NoError(t, err)
			}
			assert.Equal(t, "doc2", docRev.DocID)
			if testCase.UseCVCache {
				assert.Equal(t, int64(130), docRev.MemoryBytes)
			} else {
				assert.Equal(t, int64(102), docRev.MemoryBytes)
			}
			assert.NotNil(t, docRev.BodyBytes)
			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			if testCase.UseCVCache {
				docRev, err = cache.GetWithCV(ctx, "doc1", &Version{Value: 123, SourceID: "test"}, testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc1", "1-abc", testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			}
			assert.NotNil(t, docRev.BodyBytes)

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			docRev, err = cache.GetActive(ctx, "doc1", testCollectionID)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())
		})
	}
}

// TestShardedMemoryEviction:
//   - Test adding a doc to each shard in the test
//   - Assert that each shard has individual count for memory usage as expected
//   - Add new doc that will take over the shard memory capacity and assert that that eviction takes place and
//     all stats are as expected
func TestShardedMemoryEviction(t *testing.T) {
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxBytes:     160,
			MaxItemCount: 10,
			ShardCount:   2,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	cacheStats := db.DbStats.Cache()

	docBody := Body{
		"channels": "_default",
	}

	// add doc that will be added to one shard
	size, _, _ := createDocAndReturnSizeAndRev(t, ctx, "doc1", collection, docBody)
	assert.Equal(t, int64(size), cacheStats.RevisionCacheTotalMemory.Value())
	// grab this particular shard + assert that the shard memory usage is as expected
	shardedCache := db.revisionCache.(*ShardedLRURevisionCache)
	doc1Shard := shardedCache.getShard("doc1")
	assert.Equal(t, int64(size), doc1Shard.currMemoryUsage.Value())

	// add new doc in diff shard + assert that the shard memory usage is as expected
	size, _, _ = createDocAndReturnSizeAndRev(t, ctx, "doc2", collection, docBody)
	doc2Shard := shardedCache.getShard("doc2")
	assert.Equal(t, int64(size), doc2Shard.currMemoryUsage.Value())
	// overall mem usage should be combination oif the two added docs
	assert.Equal(t, int64(size*2), cacheStats.RevisionCacheTotalMemory.Value())

	// two docs should reside in cache at this time
	assert.Equal(t, int64(2), cacheStats.RevisionCacheNumItems.Value())

	docBody = Body{
		"channels": "_default",
		"some":     "field",
	}
	// add new doc to trigger eviction and assert stats are as expected
	newDocSize, _, _ := createDocAndReturnSizeAndRev(t, ctx, "doc3", collection, docBody)
	doc3Shard := shardedCache.getShard("doc3")
	assert.Equal(t, int64(newDocSize), doc3Shard.currMemoryUsage.Value())
	assert.Equal(t, int64(2), cacheStats.RevisionCacheNumItems.Value())
	assert.Equal(t, int64(size+newDocSize), cacheStats.RevisionCacheTotalMemory.Value())
}

// TestShardedMemoryEvictionWhenShardEmpty:
//   - Test adding a doc to sharded revision cache that will immediately be evicted due to size
//   - Assert that stats look as expected
func TestShardedMemoryEvictionWhenShardEmpty(t *testing.T) {
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxBytes:     100,
			MaxItemCount: 10,
			ShardCount:   2,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	cacheStats := db.DbStats.Cache()

	docBody := Body{
		"channels": "_default",
	}

	// add doc that will be added to one shard
	rev, _, err := collection.Put(ctx, "doc1", docBody)
	require.NoError(t, err)
	shardedCache := db.revisionCache.(*ShardedLRURevisionCache)

	// assert that doc was not added to cache as it's too large
	doc1Shard := shardedCache.getShard("doc1")
	assert.Equal(t, int64(0), doc1Shard.currMemoryUsage.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())

	// test we can still fetch this doc
	docRev, err := collection.GetRev(ctx, "doc1", rev, false, nil)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.NotNil(t, docRev.BodyBytes)

	// assert rev cache is still empty
	assert.Equal(t, int64(0), doc1Shard.currMemoryUsage.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())
}

func TestImmediateRevCacheItemBasedEviction(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 1,
				MaxBytes:     0, // turn off memory based eviction
			}
			ctx := base.TestCtx(t)
			var err error

			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)
			// load up item to hit max capacity
			cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "doc1", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			// eviction starts from here in test
			cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "newDoc", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			assert.Equal(t, int64(15), memoryBytesCounted.Value())
			assert.Equal(t, int64(1), cacheNumItems.Value())

			cache.Upsert(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "doc2", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			assert.Equal(t, int64(15), memoryBytesCounted.Value())
			assert.Equal(t, int64(1), cacheNumItems.Value())

			var docRev DocumentRevision
			if testCase.UseCVCache {
				docRev, err = cache.GetWithCV(ctx, "doc3", &Version{Value: 123, SourceID: "test"}, testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc3", "1-abc", testCollectionID, RevCacheOmitDelta)
				require.NoError(t, err)
			}
			assert.NotNil(t, docRev.BodyBytes)

			// memory usage is higher for cv (body bytes on doc rev is higher given the cv definition)
			if testCase.UseCVCache {
				assert.Equal(t, int64(130), memoryBytesCounted.Value())
			} else {
				assert.Equal(t, int64(102), memoryBytesCounted.Value())
			}
			assert.Equal(t, int64(1), cacheNumItems.Value())

			docRev, err = cache.GetActive(ctx, "doc4", testCollectionID)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)

			assert.Equal(t, int64(102), memoryBytesCounted.Value())
			assert.Equal(t, int64(1), cacheNumItems.Value())
		})
	}
}

func TestResetRevCache(t *testing.T) {
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxBytes:     100,
			MaxItemCount: 10,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	cacheStats := db.DbStats.Cache()

	// add a doc
	docSize, _, _ := createDocAndReturnSizeAndRev(t, ctx, "doc1", collection, Body{"test": "doc"})
	assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())
	assert.Equal(t, int64(1), cacheStats.RevisionCacheNumItems.Value())

	// re create rev cache
	db.FlushRevisionCacheForTest()

	// assert rev cache is reset as expected
	assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
}

func TestBasicOperationsOnCacheWithMemoryStat(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			dbcOptions := DatabaseContextOptions{
				RevisionCacheOptions: &RevisionCacheOptions{
					MaxBytes:     730,
					MaxItemCount: 10,
				},
			}
			db, ctx := SetupTestDBWithOptions(t, dbcOptions)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			cacheStats := db.DbStats.Cache()
			collctionID := collection.GetCollectionID()
			var err error

			// Test Put on new doc
			docSize, revID, docCV := createDocAndReturnSizeAndRev(t, ctx, "doc1", collection, Body{"test": "doc"})
			assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())

			// Test Get with item in the cache
			var docRev DocumentRevision
			if testCase.UseCVCache {
				docRev, err = db.revisionCache.GetWithCV(ctx, "doc1", docCV, collctionID, false)
				require.NoError(t, err)
			} else {
				docRev, err = db.revisionCache.GetWithRev(ctx, "doc1", revID, collctionID, RevCacheOmitDelta)
				require.NoError(t, err)
			}
			assert.NotNil(t, docRev.BodyBytes)
			assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())
			revIDDoc1 := docRev.RevID
			cvDoc1 := docRev.CV

			// Test Get operation with load from bucket, need to first create and remove from rev cache
			prevMemStat := cacheStats.RevisionCacheTotalMemory.Value()
			revDoc2 := createThenRemoveFromRevCache(t, ctx, "doc2", db, collection)
			// load from doc from bucket
			if testCase.UseCVCache {
				docRev, err = db.revisionCache.GetWithCV(ctx, "doc2", &revDoc2.CV, collctionID, false)
				require.NoError(t, err)
			} else {
				docRev, err = db.revisionCache.GetWithRev(ctx, "doc2", docRev.RevID, collctionID, RevCacheOmitDelta)
				require.NoError(t, err)
			}
			assert.NotNil(t, docRev.BodyBytes)
			assert.Equal(t, "doc2", docRev.DocID)
			assert.Greater(t, cacheStats.RevisionCacheTotalMemory.Value(), prevMemStat)

			// Test Get active with item resident in cache
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			docRev, err = db.revisionCache.GetActive(ctx, "doc2", collctionID)
			require.NoError(t, err)
			assert.Equal(t, "doc2", docRev.DocID)
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Get active with item to be loaded from bucket, need to first create and remove from rev cache
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			revDoc3 := createThenRemoveFromRevCache(t, ctx, "doc3", db, collection)
			docRev, err = db.revisionCache.GetActive(ctx, "doc3", collctionID)
			require.NoError(t, err)
			assert.Equal(t, "doc3", docRev.DocID)
			assert.Greater(t, cacheStats.RevisionCacheTotalMemory.Value(), prevMemStat)

			// Test Peek at item not in cache, assert stats unchanged
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			docRev, ok := db.revisionCache.Peek(ctx, "doc4", "1-abc", collctionID)
			require.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Peek in cache, assert stat unchanged
			docRev, ok = db.revisionCache.Peek(ctx, "doc3", revDoc3.RevTreeID, collctionID)
			require.True(t, ok)
			assert.Equal(t, "doc3", docRev.DocID)
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Upsert with item in cache + assert stat is expected
			docRev.CalculateBytes()
			doc3Size := docRev.MemoryBytes
			expMem := cacheStats.RevisionCacheTotalMemory.Value() - doc3Size
			newDocRev := documentRevisionForCacheTestUpdate("doc3", `"some": "body"`, revDoc3)

			expMem = expMem + 14 // size for above doc rev
			db.revisionCache.Upsert(ctx, newDocRev, collctionID)
			assert.Equal(t, expMem, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Upsert with item not in cache, assert stat is as expected
			newDocRev = documentRevisionForCacheTest("doc5", `"some": "body"`)
			expMem = cacheStats.RevisionCacheTotalMemory.Value() + 14 // size for above doc rev
			db.revisionCache.Upsert(ctx, newDocRev, collctionID)
			assert.Equal(t, expMem, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Remove with something in cache, assert stat decrements by expected value
			if testCase.UseCVCache {
				db.revisionCache.RemoveWithCV(ctx, "doc5", newDocRev.CV, collctionID)
			} else {
				db.revisionCache.RemoveWithRev(ctx, "doc5", "1-abc", collctionID)
			}
			expMem -= 14
			assert.Equal(t, expMem, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Remove with item not in cache, assert stat is unchanged
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			if testCase.UseCVCache {
				cv := Version{SourceID: "test", Value: 123}
				db.revisionCache.RemoveWithCV(ctx, "doc6", &cv, collctionID)
			} else {
				db.revisionCache.RemoveWithRev(ctx, "doc6", "1-abc", collctionID)
			}
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Update Delta, assert stat increases as expected
			revDelta := newRevCacheDelta([]byte(`"rev":"delta"`), "1-abc", newDocRev, false, nil)
			expMem = prevMemStat + revDelta.totalDeltaBytes
			if testCase.UseCVCache {
				db.revisionCache.UpdateDeltaCV(ctx, "doc3", &revDoc3.CV, collctionID, revDelta)
			} else {
				db.revisionCache.UpdateDelta(ctx, "doc3", revDoc3.RevTreeID, collctionID, revDelta)
			}
			assert.Equal(t, expMem, cacheStats.RevisionCacheTotalMemory.Value())

			// Empty cache and see memory stat is 0
			if testCase.UseCVCache {
				db.revisionCache.RemoveWithCV(ctx, "doc3", &revDoc3.CV, collctionID)
				db.revisionCache.RemoveWithCV(ctx, "doc2", &revDoc2.CV, collctionID)
				db.revisionCache.RemoveWithCV(ctx, "doc1", cvDoc1, collctionID)
			} else {
				db.revisionCache.RemoveWithRev(ctx, "doc3", revDoc3.RevTreeID, collctionID)
				db.revisionCache.RemoveWithRev(ctx, "doc2", revDoc2.RevTreeID, collctionID)
				db.revisionCache.RemoveWithRev(ctx, "doc1", revIDDoc1, collctionID)
			}

			assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
			assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())
		})
	}

}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestSingleLoad(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc123", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)
	_, err := cache.GetWithRev(base.TestCtx(t), "doc123", "1-abc", testCollectionID, false)

	assert.NoError(t, err)
}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestConcurrentLoad(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc1", RevID: "1-abc", CV: &Version{Value: 1234, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

	// Trigger load into cache
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			_, err := cache.GetWithRev(base.TestCtx(t), "doc1", "1-abc", testCollectionID, false)
			assert.NoError(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

}

func TestRevisionCacheRemove(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	rev1id, _, err := collection.Put(ctx, "doc", Body{"val": 123})
	assert.NoError(t, err)

	docRev, err := collection.revisionCache.GetWithRev(base.TestCtx(t), "doc", rev1id, true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheMisses.Value())

	collection.revisionCache.RemoveWithRev(ctx, "doc", rev1id)

	docRev, err = collection.revisionCache.GetWithRev(base.TestCtx(t), "doc", rev1id, true)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.revisionCache.GetActive(ctx, "doc")
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

// TestRevCacheHitMultiCollection:
//   - Create db with two collections
//   - Add docs of the same ID to each collection the database
//   - Perform a Get for each doc in each collection
//   - Assert each doc returned is the correct one (correct rev ID etc)
//   - Assert that each doc is found at the rev cache and no misses are reported
func TestRevCacheHitMultiCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))
	dbOptions := DatabaseContextOptions{}
	dbOptions.Scopes = GetScopesOptions(t, tb, 2)

	db, ctx := SetupTestDBForDataStoreWithOptions(t, tb, dbOptions)
	defer db.Close(ctx)

	var collectionList []*DatabaseCollectionWithUser
	var revList []string
	// add a doc with the same docs id to each collection on the database
	for _, collection := range db.CollectionByID {
		collWIthUser := &DatabaseCollectionWithUser{
			DatabaseCollection: collection,
		}
		collectionList = append(collectionList, collWIthUser)
	}

	// add a doc to each collection on the db with the same document id (testing the cache still gets unique keys)
	for i, collection := range collectionList {
		ctx := collection.AddCollectionContext(ctx)
		docRevID, _, err := collection.Put(ctx, "doc", Body{"test": fmt.Sprintf("doc%d", i)})
		require.NoError(t, err)
		revList = append(revList, docRevID)
	}

	// Perform a get for the doc in each collection
	for i, collection := range collectionList {
		ctx := collection.AddCollectionContext(ctx)
		docRev, err := collection.GetRev(ctx, "doc", revList[i], false, nil)
		require.NoError(t, err)
		assert.Equal(t, "doc", docRev.DocID)
		assert.Equal(t, revList[i], docRev.RevID)
	}

	// assert that both docs were found in rev cache and no cache misses are being reported
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheHits.Value())
	assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheMisses.Value())
}

// TestRevCacheHitMultiCollectionLoadFromBucket:
//   - Create db with two collections and have rev cache size of 1
//   - Create two docs one on each collection
//   - Perform Get on first doc to trigger load from bucket, assert doc is as expected
//   - This in turn evicts the second doc
//   - Perform Get on that second doc to trigger load from the bucket, assert doc is as expected
func TestRevCacheHitMultiCollectionLoadFromBucket(t *testing.T) {

	t.Skip("Pending CBG-4164")
	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))
	// create database context with rev cache size 1
	dbOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 1,
		},
	}
	dbOptions.Scopes = GetScopesOptions(t, tb, 2)

	db, ctx := SetupTestDBForDataStoreWithOptions(t, tb, dbOptions)
	defer db.Close(ctx)

	var collectionList []*DatabaseCollectionWithUser
	var revList []string
	// add a doc with the same docs id to each collection on the database
	for _, collection := range db.CollectionByID {
		collWIthUser := &DatabaseCollectionWithUser{
			DatabaseCollection: collection,
		}
		collectionList = append(collectionList, collWIthUser)
	}

	// add a doc to each collection on the db with the same document id (testing the cache still gets unique keys)
	for i, collection := range collectionList {
		ctx := collection.AddCollectionContext(ctx)
		docRevID, _, err := collection.Put(ctx, fmt.Sprintf("doc%d", i), Body{"test": fmt.Sprintf("doc%d", i)})
		require.NoError(t, err)
		revList = append(revList, docRevID)
	}

	// at this point the second doc added should be the only doc in the cache, the first one being evicted
	// perform a get on the first doc to trigger load from bucket and assert its correct document
	collection0Ctx := collectionList[0].AddCollectionContext(ctx)
	docRev, err := collectionList[0].GetRev(collection0Ctx, "doc0", revList[0], false, nil)
	require.NoError(t, err)
	assert.Equal(t, "doc0", docRev.DocID)
	assert.Equal(t, revList[0], docRev.RevID)

	// now do the same with doc1 and assert it is correctly loaded
	collection1Ctx := collectionList[1].AddCollectionContext(ctx)
	docRev, err = collectionList[1].GetRev(collection1Ctx, "doc1", revList[1], false, nil)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, revList[1], docRev.RevID)

	// assert that both docs were not found in rev cache and had to be loaded from bucket
	assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheHits.Value())
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())
}

func TestRevCacheCapacityStat(t *testing.T) {
	testCases := []struct {
		name       string
		UseCVCache bool
	}{
		{
			name:       "Rev cache pathway",
			UseCVCache: false,
		},
		{
			name:       "CV cache pathway",
			UseCVCache: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, cacheMemoryStat := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"badDoc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 4,
				MaxBytes:     0,
			}
			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &cacheMemoryStat)
			ctx := base.TestCtx(t)
			var err error

			assert.Equal(t, int64(0), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Create a new doc + asert num items increments
			cache.Put(ctx, documentRevisionForCacheTest("doc1", `{"test":"1234"}`), testCollectionID)
			assert.Equal(t, int64(1), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// test not found doc, assert that the stat isn't incremented
			if testCase.UseCVCache {
				cv := Version{SourceID: "test", Value: 123}
				_, err = cache.GetWithCV(ctx, "badDoc", &cv, testCollectionID, false)
				require.Error(t, err)
			} else {
				_, err = cache.GetWithRev(ctx, "badDoc", "1-abc", testCollectionID, false)
				require.Error(t, err)
			}
			assert.Equal(t, int64(1), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Get on a doc that doesn't exist in cache, assert num items increments
			var docRev DocumentRevision
			if testCase.UseCVCache {
				cv := Version{SourceID: "test", Value: 123}
				docRev, err = cache.GetWithCV(ctx, "doc2", &cv, testCollectionID, false)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc2", "1-abc", testCollectionID, false)
				require.NoError(t, err)
			}
			assert.Equal(t, "doc2", docRev.DocID)
			assert.Equal(t, int64(2), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Get on item in cache, assert num items remains the same
			if testCase.UseCVCache {
				cv := Version{SourceID: "test", Value: 123}
				docRev, err = cache.GetWithCV(ctx, "doc1", &cv, testCollectionID, false)
				require.NoError(t, err)
			} else {
				docRev, err = cache.GetWithRev(ctx, "doc1", "1-abc", testCollectionID, false)
				require.NoError(t, err)
			}
			assert.Equal(t, "doc1", docRev.DocID)
			assert.Equal(t, int64(2), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Get Active on doc not in cache, assert num items increments
			docRev, err = cache.GetActive(ctx, "doc3", testCollectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc3", docRev.DocID)
			assert.Equal(t, int64(3), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Get Active on doc in the cache, assert that the num items stat remains unchanged
			docRev, err = cache.GetActive(ctx, "doc1", testCollectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc1", docRev.DocID)
			assert.Equal(t, int64(3), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Upsert a doc resident in cache, assert stat is unchanged
			cache.Upsert(ctx, documentRevisionForCacheTest("doc1", `{"test":"12345"}`), testCollectionID)
			assert.Equal(t, int64(3), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Upsert new doc, assert the num items stat increments
			cache.Upsert(ctx, documentRevisionForCacheTest("doc4", `{"test":"1234}`), testCollectionID)
			assert.Equal(t, int64(4), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Peek a doc that is resident in cache, assert stat is unchanged
			docRev, ok := cache.Peek(ctx, "doc4", "1-abc", testCollectionID)
			require.True(t, ok)
			assert.Equal(t, "doc4", docRev.DocID)
			assert.Equal(t, int64(4), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Peek a doc that is not resident in cache, assert stat is unchanged
			docRev, ok = cache.Peek(ctx, "doc5", "1-abc", testCollectionID)
			require.False(t, ok)
			assert.Equal(t, int64(4), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Eviction situation and assert stat doesn't go over the capacity set
			cache.Put(ctx, documentRevisionForCacheTest("doc5", `{"test":"1234"}`), testCollectionID)
			assert.Equal(t, int64(4), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// test case of eviction for upsert
			cache.Upsert(ctx, documentRevisionForCacheTest("doc6", `{"test":"12345"}`), testCollectionID)
			assert.Equal(t, int64(4), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())

			// Empty cache
			if testCase.UseCVCache {
				cv := Version{SourceID: "test", Value: 123}
				cache.RemoveWithCV(ctx, "doc1", &cv, testCollectionID)
				cache.RemoveWithCV(ctx, "doc4", &cv, testCollectionID)
				cache.RemoveWithCV(ctx, "doc5", &cv, testCollectionID)
				cache.RemoveWithCV(ctx, "doc6", &cv, testCollectionID)
			} else {
				cache.RemoveWithRev(ctx, "doc1", "1-abc", testCollectionID)
				cache.RemoveWithRev(ctx, "doc4", "1-abc", testCollectionID)
				cache.RemoveWithRev(ctx, "doc5", "1-abc", testCollectionID)
				cache.RemoveWithRev(ctx, "doc6", "1-abc", testCollectionID)
			}

			// Assert num items goes back to 0
			assert.Equal(t, int64(0), cacheNumItems.Value())
			assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())
		})
	}
}

func TestRevCacheOnDemand(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 2,
			ShardCount:   1,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := "doc1"
	revID, _, err := collection.Put(ctx, docID, Body{"ver": "1"})
	require.NoError(t, err)

	testCtx, testCtxCancel := context.WithCancel(base.TestCtx(t))
	defer testCtxCancel()

	for i := 0; i < 2; i++ {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, err = db.revisionCache.GetWithRev(ctx, docID, revID, collection.GetCollectionID(), RevCacheOmitDelta) //nolint:errcheck
				}
			}
		}()
	}
	log.Printf("Updating doc to trigger on-demand import")
	err = collection.dataStore.Set(docID, 0, nil, []byte(`{"ver": "2"}`))
	require.NoError(t, err)
	log.Printf("Calling getRev for %s, %s", docID, revID)
	rev, err := collection.getRev(ctx, docID, revID, 0, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing")
	// returns empty doc rev
	assert.Equal(t, "", rev.DocID)
}

// documentRevisionForCacheTest creates a document revision with the specified body and key, and a hardcoded revID, cv and history:
//
//	RevID: 1-abc
//	CV: Version{SourceID: "test", Value: 123}
//	History: Revisions{"start": 1}}
func documentRevisionForCacheTest(key string, body string) DocumentRevision {
	cv := Version{SourceID: "test", Value: 123}
	return DocumentRevision{
		BodyBytes: []byte(body),
		DocID:     key,
		RevID:     "1-abc",
		History:   Revisions{"start": 1},
		CV:        &cv,
	}
}

// documentRevisionForCacheTestUpsert creates a document revision with the specified body and key and Version
//
//	History: Revisions{"start": 1}}
func documentRevisionForCacheTestUpdate(key string, body string, version DocVersion) DocumentRevision {
	return DocumentRevision{
		BodyBytes: []byte(body),
		DocID:     key,
		RevID:     version.RevTreeID,
		History:   Revisions{"start": 1},
		CV:        &version.CV,
	}
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

	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	cv := Version{SourceID: "test", Value: 123}
	documentRevision := DocumentRevision{
		DocID:     "doc1",
		RevID:     "1-abc",
		BodyBytes: []byte(`{"test":"1234"}`),
		Channels:  base.SetOf("chan1"),
		History:   Revisions{"start": 1},
		CV:        &cv,
	}
	cache.Put(base.TestCtx(t), documentRevision, testCollectionID)

	docRev, err := cache.GetWithCV(base.TestCtx(t), "doc1", &cv, testCollectionID, RevCacheOmitDelta)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, base.SetOf("chan1"), docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(0), cacheMissCounter.Value())

	documentRevision.BodyBytes = []byte(`{"test":"12345"}`)

	cache.Upsert(base.TestCtx(t), documentRevision, testCollectionID)

	docRev, err = cache.GetWithCV(base.TestCtx(t), "doc1", &cv, testCollectionID, RevCacheOmitDelta)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, base.SetOf("chan1"), docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, []byte(`{"test":"12345"}`), docRev.BodyBytes)
	assert.Equal(t, int64(2), cacheHitCounter.Value())
	assert.Equal(t, int64(0), cacheMissCounter.Value())

	// remove the doc rev from the cache and assert that the document is no longer present in cache
	cache.RemoveWithCV(base.TestCtx(t), "doc1", &cv, testCollectionID)
	assert.Equal(t, 0, len(cache.cache))
	assert.Equal(t, 0, len(cache.hlvCache))
	assert.Equal(t, 0, cache.lruList.Len())
}

func BenchmarkRevisionCacheRead(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelDebug, base.KeyAll)

	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: DefaultRevisionCacheSize,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	ctx := base.TestCtx(b)

	// trigger load into cache
	for i := 0; i < 5000; i++ {
		_, _ = cache.GetWithRev(ctx, fmt.Sprintf("doc%d", i), "1-abc", testCollectionID, RevCacheOmitDelta)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			docId := fmt.Sprintf("doc%d", rand.Intn(5000))
			_, _ = cache.GetWithRev(ctx, docId, "1-abc", testCollectionID, RevCacheOmitDelta)
		}
	})
}

// createThenRemoveFromRevCache will create a doc and then immediately remove it from the rev cache
func createThenRemoveFromRevCache(t *testing.T, ctx context.Context, docID string, db *Database, collection *DatabaseCollectionWithUser) DocVersion {
	revIDDoc, doc, err := collection.Put(ctx, docID, Body{"test": "doc"})
	require.NoError(t, err)

	db.revisionCache.RemoveWithRev(ctx, docID, revIDDoc, collection.GetCollectionID())
	docVersion := DocVersion{
		RevTreeID: doc.CurrentRev,
	}
	if doc.HLV != nil {
		docVersion.CV = *doc.HLV.ExtractCurrentVersionFromHLV()
	}
	return docVersion
}

// createDocAndReturnSizeAndRev creates a rev and measures its size based on rev cache measurements
func createDocAndReturnSizeAndRev(t *testing.T, ctx context.Context, docID string, collection *DatabaseCollectionWithUser, body Body) (int, string, *Version) {

	rev, doc, err := collection.Put(ctx, docID, body)
	require.NoError(t, err)

	var expectedSize int
	his, err := doc.SyncData.History.getHistory(rev)
	require.NoError(t, err)

	historyBytes := 32 * len(his)
	expectedSize += historyBytes
	expectedSize += len(doc._rawBody)

	// channels
	chanArray := doc.Channels.KeySet()
	for _, v := range chanArray {
		expectedSize += len([]byte(v))
	}

	return expectedSize, rev, doc.HLV.ExtractCurrentVersionFromHLV()
}

func TestRevCacheOnDemandImport(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 2,
			ShardCount:   1,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := "doc1"
	revID, _, err := collection.Put(ctx, docID, Body{"ver": "1"})
	require.NoError(t, err)

	ctx, testCtxCancel := context.WithCancel(ctx)
	defer testCtxCancel()

	for i := 0; i < 2; i++ {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_, err = db.revisionCache.GetWithRev(ctx, docID, revID, collection.GetCollectionID(), RevCacheOmitDelta) //nolint:errcheck
				}
			}
		}()
	}
	err = collection.dataStore.Set(docID, 0, nil, []byte(`{"ver": "2"}`))
	require.NoError(t, err)
	rev, err := collection.getRev(ctx, docID, revID, 0, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing")
	// returns empty doc rev
	assert.Equal(t, "", rev.DocID)
}

func TestRevCacheOnDemandMemoryEviction(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 20,
			ShardCount:   1,
			MaxBytes:     112, // equivalent to max size 2 items
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := "doc1"
	revID, _, err := collection.Put(ctx, docID, Body{"ver": "1"})
	require.NoError(t, err)

	testCtx, testCtxCancel := context.WithCancel(base.TestCtx(t))
	defer testCtxCancel()

	for i := 0; i < 2; i++ {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, err = db.revisionCache.GetWithRev(ctx, docID, revID, collection.GetCollectionID(), RevCacheOmitDelta) //nolint:errcheck
				}
			}
		}()
	}
	err = collection.dataStore.Set(docID, 0, nil, []byte(`{"ver": "2"}`))
	require.NoError(t, err)
	rev, err := collection.getRev(ctx, docID, revID, 0, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing")
	// returns empty doc rev
	assert.Equal(t, "", rev.DocID)

}

func TestRevCacheOnDemandImportNoCache(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := "doc1"
	revID1, _, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)

	_, exists := collection.revisionCache.Peek(ctx, docID, revID1)
	require.True(t, exists)

	require.NoError(t, collection.dataStore.Set(docID, 0, nil, []byte(`{"foo": "baz"}`)))

	doc, err := collection.GetDocument(ctx, docID, DocUnmarshalSync)
	require.NoError(t, err)
	require.Equal(t, Body{"foo": "baz"}, doc.Body(ctx))

	// rev1 still exists in cache but not on server
	_, exists = collection.revisionCache.Peek(ctx, docID, revID1)
	require.True(t, exists)

	// rev2 is not in cache but is on server
	_, exists = collection.revisionCache.Peek(ctx, docID, doc.CurrentRev)
	require.False(t, exists)
}

// TestLoaderMismatchInCV:
//   - Get doc that is not in cache by CV to trigger a load from bucket
//   - Ensure the CV passed into the GET operation won't match the doc in the bucket
//   - Assert we get error and the value is not loaded into the cache
func TestLoaderMismatchInCV(t *testing.T) {
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	// create cv with incorrect version to the one stored in backing store
	cv := Version{SourceID: "test", Value: 1234}

	_, err := cache.GetWithCV(base.TestCtx(t), "doc1", &cv, testCollectionID, RevCacheOmitDelta)
	require.Error(t, err)
	require.Error(t, err, base.ErrNotFound)
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
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

	ctx := base.TestCtx(t)

	wg := sync.WaitGroup{}
	wg.Add(2)

	cv := Version{SourceID: "test", Value: 123}
	go func() {
		_, err := cache.GetWithRev(ctx, "doc1", "1-abc", testCollectionID, RevCacheIncludeDelta)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := cache.GetWithCV(ctx, "doc1", &cv, testCollectionID, RevCacheIncludeDelta)
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
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	rev1id, doc, err := collection.Put(ctx, "doc", Body{"val": 123})
	require.NoError(t, err)

	expectedCV := Version{
		SourceID: db.EncodedSourceID,
		Value:    doc.Cas,
	}

	// remove the entry form the rev cache to force the cache to not have the active version in it
	collection.revisionCache.RemoveWithCV(ctx, "doc", &expectedCV)

	// call get active to get the active version from the bucket
	docRev, err := collection.revisionCache.GetActive(base.TestCtx(t), "doc")
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, expectedCV, *docRev.CV)
}

// TestConcurrentPutAndGetOnRevCache:
//   - Perform a Get with rev on the cache for a doc not in the cache
//   - Concurrently perform a PUT on the cache with doc revision the same as the GET
//   - Assert we get consistent cache with only 1 entry in lookup maps and the cache itself
func TestConcurrentPutAndGetOnRevCache(t *testing.T) {
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), &cacheHitCounter, &cacheMissCounter, &cacheNumItems, &memoryBytesCounted)

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
		_, err := cache.GetWithRev(ctx, "doc1", "1-abc", testCollectionID, RevCacheIncludeDelta)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		cache.Put(ctx, docRev, testCollectionID)
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

func TestLoadActiveDocFromBucketRevCacheChurn(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 2,
			ShardCount:   1,
		},
	}
	var wg sync.WaitGroup
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := "doc1"
	_, _, err := collection.Put(ctx, docID, Body{"ver": "0"})
	require.NoError(t, err)
	wg.Add(1)

	testCtx, testCtxCancel := context.WithCancel(base.TestCtx(t))
	defer testCtxCancel()

	for i := 0; i < 2; i++ {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, err = db.revisionCache.GetWithRev(ctx, docID, revID, collection.GetCollectionID(), RevCacheOmitDelta) //nolint:errcheck
				}
			}
		}()
	}

	go func() {
		for i := 0; i < 100; i++ {
			err = collection.dataStore.Set(docID, 0, nil, []byte(fmt.Sprintf(`{"ver": "%d"}`, i)))
			require.NoError(t, err)
			_, err := db.revisionCache.GetActive(ctx, docID, collection.GetCollectionID())
			if err != nil {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	require.NoError(t, err)
}

func TestLoadRequestedRevFromBucketHighChurn(t *testing.T) {

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 2,
			ShardCount:   1,
		},
	}
	var wg sync.WaitGroup
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := "doc1"
	rev1ID, _, err := collection.Put(ctx, docID, Body{"ver": "0"})
	require.NoError(t, err)
	wg.Add(1)

	testCtx, testCtxCancel := context.WithCancel(base.TestCtx(t))
	defer testCtxCancel()

	for i := 0; i < 2; i++ {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, err = db.revisionCache.GetWithRev(ctx, docID, revID, collection.GetCollectionID(), RevCacheOmitDelta) //nolint:errcheck
				}
			}
		}()
	}

	var getErr error
	go func() {
		for i := 0; i < 100; i++ {
			_, getErr = db.revisionCache.GetWithRev(ctx, docID, rev1ID, collection.GetCollectionID(), true)
			if getErr != nil {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	require.NoError(t, getErr)

}

func TestPutRevHighRevCacheChurn(t *testing.T) {

	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 2,
			ShardCount:   1,
		},
	}
	var wg sync.WaitGroup
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := "doc1"
	wg.Add(1)

	testCtx, testCtxCancel := context.WithCancel(base.TestCtx(t))
	defer testCtxCancel()

	for i := 0; i < 2; i++ {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, err = db.revisionCache.GetWithRev(ctx, docID, revID, collection.GetCollectionID(), RevCacheOmitDelta) //nolint:errcheck
				}
			}
		}()
	}

	go func() {
		for i := 0; i < 100; i++ {
			docRev := DocumentRevision{DocID: docID, RevID: fmt.Sprintf("1-%d", i), CV: &Version{SourceID: "someSrc", Value: uint64(i)}, BodyBytes: []byte(fmt.Sprintf(`{"ver": "%d"}`, i)), History: Revisions{"start": 1}}
			db.revisionCache.Put(ctx, docRev, collection.GetCollectionID())
		}
		wg.Done()
	}()
	wg.Wait()
}
