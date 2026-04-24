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
	"errors"
	"fmt"
	"log"
	"math/rand"
	"slices"
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

	if slices.Contains(t.notFoundDocIDs, docid) {
		return nil, ErrMissing
	}

	doc = NewDocument(docid)
	doc._body = Body{
		"testing": true,
	}
	const revTreeID = "1-abc"
	doc.SetRevTreeID(revTreeID)
	doc.History = RevTree{
		revTreeID: {},
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

func (t *testBackingStore) getRevision(ctx context.Context, doc *Document, revid string) ([]byte, AttachmentsMeta, base.Set, error) {
	t.getRevisionCounter.Add(1)

	revTreeID := doc.GetRevTreeID()
	ch, _ := doc.channelsForRevTreeID(revTreeID)
	b := Body{
		"testing":     true,
		BodyId:        doc.ID,
		BodyRev:       revTreeID,
		BodyRevisions: Revisions{RevisionsStart: 1},
	}
	if doc.HLV != nil {
		b[BodyCV] = doc.HLV.GetCurrentVersionString()
	}
	bodyBytes, err := base.JSONMarshal(b)
	return bodyBytes, nil, ch, err
}

func (t *testBackingStore) getCurrentVersion(ctx context.Context, doc *Document, cv Version, loadBackup bool) ([]byte, AttachmentsMeta, base.Set, bool, error) {
	t.getRevisionCounter.Add(1)

	revTreeID := doc.GetRevTreeID()
	ch, _ := doc.channelsForRevTreeID(revTreeID)
	b := Body{
		"testing":     true,
		BodyId:        doc.ID,
		BodyRev:       revTreeID,
		BodyRevisions: Revisions{RevisionsStart: 1},
	}
	if doc.HLV != nil {
		b[BodyCV] = doc.HLV.GetCurrentVersionString()
	}
	if err := doc.HasCurrentVersion(ctx, cv); err != nil {
		return nil, nil, nil, false, err
	}
	bodyBytes, err := base.JSONMarshal(b)
	return bodyBytes, nil, ch, false, err
}

type noopBackingStore struct{}

func (*noopBackingStore) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	return nil, nil
}

func (*noopBackingStore) getRevision(ctx context.Context, doc *Document, revid string) ([]byte, AttachmentsMeta, base.Set, error) {
	return nil, nil, nil, nil
}

func (*noopBackingStore) getCurrentVersion(ctx context.Context, doc *Document, cv Version, loadBackup bool) ([]byte, AttachmentsMeta, base.Set, bool, error) {
	return nil, nil, nil, false, nil
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

	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, cacheNumItems, getDocumentCounter, getRevisionCounter, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 10,
				MaxBytes:     0,
			}
			revCacheStats := revisionCacheStats{
				cacheHitStat:      &cacheHitCounter,
				cacheMissStat:     &cacheMissCounter,
				cacheNumItemsStat: &cacheNumItems,
				cacheMemoryStat:   &memoryBytesCounted,
			}
			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(0, revCacheStats.cacheMemoryStat))

			ctx := base.TestCtx(t)

			revOrCV := "1-abc"
			if testCase.useCVKey {
				revOrCV = Version{Value: 123, SourceID: "test"}.String()
			}

			// Fill up the rev cache with the first 10 docs
			for docID := range 10 {
				id := strconv.Itoa(docID)
				_, _, err := cache.Get(ctx, id, revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
				require.NoError(t, err)
			}
			assert.Equal(t, int64(10), cacheNumItems.Value())
			assert.Equal(t, int64(1150), memoryBytesCounted.Value())
			assert.Equal(t, 10, len(cache.cache))

			// Get them back out
			for i := range 10 {
				docID := strconv.Itoa(i)
				docRev, _, err := cache.Get(ctx, docID, revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
				assert.NoError(t, err)
				assert.NotNil(t, docRev.BodyBytes, "nil body for %s", docID)
				assert.Equal(t, docID, docRev.DocID)
			}
			assert.Equal(t, int64(10), cacheNumItems.Value())
			assert.Equal(t, int64(1150), memoryBytesCounted.Value())
			assert.Equal(t, 10, len(cache.cache))

			// Add 3 more docs to the now full revcache
			for i := 10; i < 13; i++ {
				docID := strconv.Itoa(i)
				_, _, err := cache.Get(ctx, docID, revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
				require.NoError(t, err)
			}
			assert.Equal(t, int64(10), cacheNumItems.Value())
			assert.Equal(t, int64(1153), memoryBytesCounted.Value())
			assert.Equal(t, 10, len(cache.cache))

			// Check that the first 3 docs were evicted
			prevCacheHitCount := cacheHitCounter.Value()
			prevCacheMissCount := cacheMissCounter.Value()
			for i := range 3 {
				docID := strconv.Itoa(i)
				docRev, ok := cache.Peek(ctx, docID, revOrCV, testCollectionID)
				assert.False(t, ok)
				assert.Nil(t, docRev.BodyBytes)
				assert.Equal(t, prevCacheMissCount, cacheMissCounter.Value()) // peek incurs no cache miss if not found
				assert.Equal(t, prevCacheHitCount, cacheHitCounter.Value())
			}
			assert.Equal(t, int64(10), cacheNumItems.Value())
			assert.Equal(t, int64(1153), memoryBytesCounted.Value())
			assert.Equal(t, 10, len(cache.cache))

			// and check we can Get up to and including the last 3 we put in
			for i := range 10 {
				id := strconv.Itoa(i + 3)
				docRev, _, err := cache.Get(ctx, id, revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
				assert.NoError(t, err)
				assert.NotNil(t, docRev.BodyBytes, "nil body for %s", id)
				assert.Equal(t, id, docRev.DocID)
				assert.Equal(t, prevCacheMissCount, cacheMissCounter.Value())
				assert.Equal(t, prevCacheHitCount+int64(i)+1, cacheHitCounter.Value())
			}
		})
	}
}

func TestLRURevisionCacheEvictionMemoryBased(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("Revision cache disabled, eviction test needs revision cache enabled")
	}
	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
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

			versionKey := func(cv *Version, revTreeID string) string {
				if testCase.useCVKey && cv != nil {
					return cv.String()
				}
				return revTreeID
			}

			smallBody := Body{
				"channels": "_default", // add channel for default sync func in default collection test runs
			}

			var currMem, expValue, revZeroSize int64
			var versions []*Version
			for i := range 10 {
				currMem = cacheStats.RevisionCacheTotalMemory.Value()
				revSize, _, docVersion := createDocAndReturnSizeAndRev(t, ctx, fmt.Sprint(i), collection, smallBody, testCase.useCVKey)
				if i == 0 {
					revZeroSize = int64(revSize)
				}
				versions = append(versions, docVersion)
				expValue = currMem + int64(revSize)
				assert.Equal(t, expValue, cacheStats.RevisionCacheTotalMemory.Value())
			}

			// test eviction by number of items (adding new doc from createDocAndReturnSizeAndRev shouldn't take memory over threshold defined as 730 bytes)
			expValue -= revZeroSize // for doc being evicted
			docSize, rev, docVersion := createDocAndReturnSizeAndRev(t, ctx, "11", collection, smallBody, testCase.useCVKey)
			// load into cache
			_, _, err := db.revisionCache.Get(ctx, "11", versionKey(docVersion, rev), collection.GetCollectionID(), RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			expValue += int64(docSize)
			// assert doc 0 been evicted
			docRev, ok := db.revisionCache.Peek(ctx, "0", versionKey(versions[0], rev), collection.GetCollectionID())
			assert.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)

			currMem = cacheStats.RevisionCacheTotalMemory.Value()
			// assert total memory is as expected
			assert.Equal(t, expValue, currMem)

			// remove doc "1" to give headroom for memory based eviction
			db.revisionCache.Remove(ctx, "1", versionKey(versions[1], rev), collection.GetCollectionID())
			docRev, ok = db.revisionCache.Peek(ctx, "1", versionKey(versions[1], rev), collection.GetCollectionID())
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
			revID, doc, err := collection.Put(ctx, "12", largeBody)
			require.NoError(t, err)

			// load into cache
			_, _, err = db.revisionCache.Get(ctx, "12", versionKey(doc.HLV.ExtractCurrentVersionFromHLV(), revID), collection.GetCollectionID(), RevCacheDontLoadBackupRev)
			require.NoError(t, err)

			// assert doc "2" has been evicted even though we only have 9 items in cache with capacity of 10, so memory based
			// eviction took place
			docRev, ok = db.revisionCache.Peek(ctx, "2", versionKey(versions[2], rev), collection.GetCollectionID())
			assert.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)

			// assert that the overall memory for rev cache is not over maximum
			assert.LessOrEqual(t, cacheStats.RevisionCacheTotalMemory.Value(), dbcOptions.RevisionCacheOptions.MaxBytes)
		})
	}

}

func TestBackingStoreMemoryCalculation(t *testing.T) {
	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"doc2"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			maxBytes := int64(235)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 10,
				MaxBytes:     maxBytes,
			}
			revCacheStats := revisionCacheStats{
				cacheHitStat:      &cacheHitCounter,
				cacheMissStat:     &cacheMissCounter,
				cacheNumItemsStat: &cacheNumItems,
				cacheMemoryStat:   &memoryBytesCounted,
			}
			cache := NewRevisionCacheOrchestrator(cacheOptions, backingStoreMap, revCacheStats, nil, false)
			ctx := base.TestCtx(t)
			var err error

			revOrCV := "1-abc"
			if testCase.useCVKey {
				revOrCV = Version{Value: 123, SourceID: "test"}.String()
			}

			var docRev DocumentRevision
			docRev, _, err = cache.Get(ctx, "doc1", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.Equal(t, "doc1", docRev.DocID)
			assert.NotNil(t, docRev.History)
			assert.NotNil(t, docRev.Channels)

			currMemStat := memoryBytesCounted.Value()
			// assert stats is incremented by appropriate bytes on doc rev
			assert.Equal(t, docRev.MemoryBytes, currMemStat)

			// Test get active code pathway of a load from bucket
			docRev, _, err = cache.GetActive(ctx, "doc", testCollectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc", docRev.DocID)
			assert.NotNil(t, docRev.History)
			assert.NotNil(t, docRev.Channels)

			newMemStat := currMemStat + docRev.MemoryBytes
			// assert stats is incremented by appropriate bytes on doc rev
			assert.Equal(t, newMemStat, memoryBytesCounted.Value())

			// test fail load event doesn't increment memory stat
			docRev, _, err = cache.Get(ctx, "doc2", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			assertHTTPError(t, err, 404)
			assert.Nil(t, docRev.BodyBytes)
			assert.Equal(t, newMemStat, memoryBytesCounted.Value())

			// assert length is 2 as expected
			assert.Equal(t, 2, cache.revisionCache.lruList.Len())

			memStatBeforeThirdLoad := memoryBytesCounted.Value()
			// test another load from bucket but doing so should trigger memory based eviction
			docRev, _, err = cache.Get(ctx, "doc3", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.Equal(t, "doc3", docRev.DocID)
			assert.NotNil(t, docRev.History)
			assert.NotNil(t, docRev.Channels)

			// assert length is still 2 (eviction took place) + test Peek for first added doc is failure
			assert.Equal(t, 2, cache.revisionCache.lruList.Len())
			memStatAfterEviction := (memStatBeforeThirdLoad + docRev.MemoryBytes) - currMemStat
			assert.Equal(t, memStatAfterEviction, memoryBytesCounted.Value())
			_, ok := cache.Peek(ctx, "doc1", revOrCV, testCollectionID)
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
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	docRev, _, err := cache.Get(base.TestCtx(t), "Jens", "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, int64(1), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Doc doesn't exist, so miss the cache, and fail when getting the doc
	docRev, _, err = cache.Get(base.TestCtx(t), "Peter", "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev is already resident, but still issue GetDocument to check for later revisions
	docRev, _, err = cache.Get(base.TestCtx(t), "Jens", "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
	assert.NoError(t, err)
	assert.Equal(t, "Jens", docRev.DocID)
	assert.NotNil(t, docRev.History)
	assert.NotNil(t, docRev.Channels)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, _, err = cache.Get(base.TestCtx(t), "Peter", "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
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
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	// Get Rev for the first time - miss cache, but fetch the doc and revision to store
	cv := Version{SourceID: "test", Value: 123}
	docRev, _, err := cache.Get(base.TestCtx(t), "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
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
	docRev, _, err = cache.Get(base.TestCtx(t), "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
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
	docRev, _, err = cache.Get(base.TestCtx(t), "not_found", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)

	assertHTTPError(t, err, 404)
	assert.Nil(t, docRev.BodyBytes)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(2), cacheMissCounter.Value())
	assert.Equal(t, int64(2), getDocumentCounter.Value())
	assert.Equal(t, int64(1), getRevisionCounter.Value())

	// Rev still doesn't exist, make sure it wasn't cached
	docRev, _, err = cache.Get(base.TestCtx(t), "not_found", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
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

	// fetch doc as it won't be resident in cache after Put
	_, err = collection.getRev(ctx, "doc1", rev1id, 0, nil)
	require.NoError(t, err)

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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	backingStoreMap := CreateTestSingleBackingStoreMap(collection, collection.GetCollectionID())

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
	_, ok := rc.Peek(ctx, key, rev1, collection.GetCollectionID())
	assert.False(t, ok)
	_, ok = rc.Peek(ctx, key, rev2, collection.GetCollectionID())
	assert.False(t, ok)

	// Get non-existing doc
	_, _, err = rc.Get(base.TestCtx(t), "invalid", rev1, collection.GetCollectionID(), RevCacheDontLoadBackupRev)
	assert.True(t, base.IsDocNotFoundError(err))

	// Get non-existing revision
	_, _, err = rc.Get(base.TestCtx(t), key, "3-abc", collection.GetCollectionID(), RevCacheDontLoadBackupRev)
	assertHTTPError(t, err, 404)

	// Get specific revision
	doc, _, err := rc.Get(base.TestCtx(t), key, rev1, collection.GetCollectionID(), RevCacheDontLoadBackupRev)
	assert.NoError(t, err)
	require.NotNil(t, doc)
	assert.Equal(t, `{"value":1234}`, string(doc.BodyBytes))

	// Check peek is still returning false for "Get"
	_, ok = rc.Peek(ctx, key, rev1, collection.GetCollectionID())
	assert.False(t, ok)

	// Put no-ops
	rc.Put(ctx, doc, collection.GetCollectionID())

	// Check peek is still returning false for "Put"
	_, ok = rc.Peek(ctx, key, rev1, collection.GetCollectionID())
	assert.False(t, ok)

	// Get active revision
	doc, _, err = rc.GetActive(ctx, key, collection.GetCollectionID())
	assert.NoError(t, err)
	assert.Equal(t, `{"value":5678}`, string(doc.BodyBytes))

}

// Ensure attachment properties aren't being incorrectly stored in revision cache body when inserted via Put
func TestPutRevisionCacheAttachmentProperty(t *testing.T) {

	if base.TestDisableRevCache() {
		t.Skip("Revision cache expected to be used for this test")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	rev1body := Body{
		"value":         1234,
		BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
	}
	rev1key := "doc1"
	rev1id, _, err := collection.Put(ctx, rev1key, rev1body)
	assert.NoError(t, err, "Unexpected error calling collection.Put")

	_, err = collection.getRev(ctx, rev1key, rev1id, 0, nil) // preload rev cache
	require.NoError(t, err)

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

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docKey := "doc1"
	rev1body := Body{
		"value": 1234,
	}
	rev1id, _, err := collection.Put(ctx, docKey, rev1body)
	assert.NoError(t, err, "Unexpected error calling collection.Put")

	_, err = collection.getRev(ctx, docKey, rev1id, 0, nil) // preload rev cache
	require.NoError(t, err)

	rev2id := "2-xxx"
	rev2body := Body{
		"value":         1235,
		BodyAttachments: map[string]any{"myatt": map[string]any{"content_type": "text/plain", "data": "SGVsbG8gV29ybGQh"}},
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
	docRevision, err := collection.revisionCache.Get(base.TestCtx(t), docKey, rev2id, RevCacheLoadBackupRev)
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
	deltaCacheHit, deltaCacheMiss, deltaCacheCount := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	deltaStats := &base.DeltaSyncStats{
		DeltaCacheMiss:     &deltaCacheMiss,
		DeltaCacheHit:      &deltaCacheHit,
		DeltaCacheNumItems: &deltaCacheCount,
	}
	orchestrator := NewRevisionCacheOrchestrator(cacheOptions, backingStoreMap, revCacheStats, deltaStats, true)

	firstDelta := []byte("delta")
	secondDelta := []byte("modified delta")

	// Trigger load into cache
	_, _, err := orchestrator.Get(base.TestCtx(t), "doc1", "1-abc", testCollectionID, false)
	assert.NoError(t, err, "Error adding to cache")
	orchestrator.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", "rev2", testCollectionID, RevisionDelta{ToRevID: "rev2", DeltaBytes: firstDelta})

	// Retrieve from cache
	retrievedRev, err := orchestrator.GetWithDelta(base.TestCtx(t), "doc1", "1-abc", "rev2", testCollectionID)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Update delta again, validate data in retrievedRev isn't mutated
	orchestrator.UpdateDelta(base.TestCtx(t), "doc1", "1-abc", "rev3", testCollectionID, RevisionDelta{ToRevID: "rev3", DeltaBytes: secondDelta})
	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

	// Retrieve again, validate delta is correct
	updatedRev, err := orchestrator.GetWithDelta(base.TestCtx(t), "doc1", "1-abc", "rev3", testCollectionID)
	assert.NoError(t, err, "Error retrieving from cache")
	assert.Equal(t, "rev3", updatedRev.Delta.ToRevID)
	assert.Equal(t, secondDelta, updatedRev.Delta.DeltaBytes)

	assert.Equal(t, "rev2", retrievedRev.Delta.ToRevID)
	assert.Equal(t, firstDelta, retrievedRev.Delta.DeltaBytes)

}

func TestUpdateDeltaRevCacheMemoryStat(t *testing.T) {
	t.Skip("pending CBG-5234")

	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount: 10,
				MaxBytes:     140,
			}
			revCacheStats := revisionCacheStats{
				cacheHitStat:      &cacheHitCounter,
				cacheMissStat:     &cacheMissCounter,
				cacheNumItemsStat: &cacheNumItems,
				cacheMemoryStat:   &memoryBytesCounted,
			}
			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

			firstDelta := []byte("delta")
			secondDelta := []byte("modified delta")
			thirdDelta := []byte("another delta further modified")
			ctx := base.TestCtx(t)

			revOrCV := "1-abc"
			if testCase.useCVKey {
				revOrCV = Version{Value: 123, SourceID: "test"}.String()
			}

			key := func(cv *Version, revTreeID string) string {
				if testCase.useCVKey && cv != nil {
					return cv.String()
				}
				return revTreeID
			}

			// Trigger load into cache
			var docRev DocumentRevision
			var err error
			docRev, _, err = cache.Get(ctx, "doc1", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err, "Error adding to cache")

			revCacheMem := memoryBytesCounted.Value()
			revCacheDelta := newRevCacheDelta(firstDelta, "1-abc", docRev, false, nil)
			cache.UpdateDelta(ctx, "doc1", revOrCV, key(docRev.CV, docRev.RevID), testCollectionID, revCacheDelta)
			// assert that rev cache memory increases by expected amount
			newMem := revCacheMem + revCacheDelta.totalDeltaBytes
			assert.Equal(t, newMem, memoryBytesCounted.Value())
			oldDeltaSize := revCacheDelta.totalDeltaBytes

			newMem = memoryBytesCounted.Value()
			revCacheDelta = newRevCacheDelta(secondDelta, "1-abc", docRev, false, nil)
			cache.UpdateDelta(ctx, "doc1", revOrCV, key(docRev.CV, docRev.RevID), testCollectionID, revCacheDelta)

			// assert the overall memory stat is correctly updated (by the diff between the old delta and the new delta)
			newMem += revCacheDelta.totalDeltaBytes - oldDeltaSize
			assert.Equal(t, newMem, memoryBytesCounted.Value())

			revCacheDelta = newRevCacheDelta(thirdDelta, "1-abc", docRev, false, nil)
			cache.UpdateDelta(ctx, "doc1", revOrCV, key(docRev.CV, docRev.RevID), testCollectionID, revCacheDelta)

			// assert that eviction took place and as result stat is now 0 (only item in cache was doc1)
			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, 0, cache.lruList.Len())
		})
	}
}

func TestImmediateRevCacheMemoryBasedEviction(t *testing.T) {
	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
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
			revCacheStats := revisionCacheStats{
				cacheHitStat:      &cacheHitCounter,
				cacheMissStat:     &cacheMissCounter,
				cacheNumItemsStat: &cacheNumItems,
				cacheMemoryStat:   &memoryBytesCounted,
			}
			ctx := base.TestCtx(t)
			var err error
			cache := NewRevisionCacheOrchestrator(cacheOptions, backingStoreMap, revCacheStats, nil, false)

			revOrCV := "1-abc"
			if testCase.useCVKey {
				revOrCV = Version{Value: 123, SourceID: "test"}.String()
			}

			cache.Put(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "doc1", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			cache.Upsert(ctx, DocumentRevision{BodyBytes: []byte(`{"some":"test"}`), DocID: "doc2", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

			if !testCase.useCVKey {
				// fetch each doc added to load into cache
				_, _, err = cache.Get(ctx, "doc1", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
				require.NoError(t, err)
				_, _, err = cache.Get(ctx, "doc2", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
				require.NoError(t, err)
			}

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			// assert we can still fetch this upsert doc
			var docRev DocumentRevision
			docRev, _, err = cache.Get(ctx, "doc2", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.Equal(t, "doc2", docRev.DocID)
			assert.Equal(t, int64(118), docRev.MemoryBytes)
			assert.NotNil(t, docRev.BodyBytes)
			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			docRev, _, err = cache.Get(ctx, "doc1", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)

			assert.Equal(t, int64(0), memoryBytesCounted.Value())
			assert.Equal(t, int64(0), cacheNumItems.Value())

			docRev, _, err = cache.GetActive(ctx, "doc1", testCollectionID)
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
	if base.TestDisableRevCache() {
		t.Skip("Test is sharded revision cache specific")
	}
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxBytes:      160,
			MaxItemCount:  10,
			ShardCount:    2,
			InsertOnWrite: true,
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
	size, _, _ := createDocAndReturnSizeAndRev(t, ctx, "doc1", collection, docBody, false)
	assert.Equal(t, int64(size), cacheStats.RevisionCacheTotalMemory.Value())
	// grab this particular shard + assert that the shard memory usage is as expected
	shardedCache := db.revisionCache.(*ShardedLRURevisionCache)
	doc1Shard := shardedCache.getShard("doc1")
	assert.Equal(t, int64(size), doc1Shard.memoryController.bytesInUseForShard.Load())

	// add new doc in diff shard + assert that the shard memory usage is as expected
	size, _, _ = createDocAndReturnSizeAndRev(t, ctx, "doc2", collection, docBody, false)
	doc2Shard := shardedCache.getShard("doc2")
	assert.Equal(t, int64(size), doc2Shard.memoryController.bytesInUseForShard.Load())
	// overall mem usage should be combination oif the two added docs
	assert.Equal(t, int64(size*2), cacheStats.RevisionCacheTotalMemory.Value())

	// two docs should reside in cache at this time
	assert.Equal(t, int64(2), cacheStats.RevisionCacheNumItems.Value())

	docBody = Body{
		"channels": "_default",
		"some":     "field",
	}
	// add new doc to trigger eviction and assert stats are as expected
	newDocSize, _, _ := createDocAndReturnSizeAndRev(t, ctx, "doc3", collection, docBody, false)
	doc3Shard := shardedCache.getShard("doc3")
	assert.Equal(t, int64(newDocSize), doc3Shard.memoryController.bytesInUseForShard.Load())
	assert.Equal(t, int64(2), cacheStats.RevisionCacheNumItems.Value())
	assert.Equal(t, int64(size+newDocSize), cacheStats.RevisionCacheTotalMemory.Value())
}

// TestShardedMemoryEvictionWhenShardEmpty:
//   - Test adding a doc to sharded revision cache that will immediately be evicted due to size
//   - Assert that stats look as expected
func TestShardedMemoryEvictionWhenShardEmpty(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test is sharded revision cache specific")
	}
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxBytes:      100,
			MaxItemCount:  10,
			ShardCount:    2,
			InsertOnWrite: true,
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
	assert.Equal(t, int64(0), doc1Shard.memoryController.bytesInUseForShard.Load())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())

	// test we can still fetch this doc
	docRev, err := collection.GetRev(ctx, "doc1", rev, false, nil)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.NotNil(t, docRev.BodyBytes)

	// assert rev cache is still empty
	assert.Equal(t, int64(0), doc1Shard.memoryController.bytesInUseForShard.Load())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())
}

func TestImmediateRevCacheItemBasedEviction(t *testing.T) {
	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
			cacheOptions := &RevisionCacheOptions{
				MaxItemCount:  1,
				MaxBytes:      0, // turn off memory based eviction
				InsertOnWrite: true,
			}
			ctx := base.TestCtx(t)
			var err error

			revCacheStats := revisionCacheStats{
				cacheHitStat:      &cacheHitCounter,
				cacheMissStat:     &cacheMissCounter,
				cacheNumItemsStat: &cacheNumItems,
				cacheMemoryStat:   &memoryBytesCounted,
			}

			cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))
			revOrCV := "1-abc"
			if testCase.useCVKey {
				revOrCV = Version{Value: 123, SourceID: "test"}.String()
			}
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
			docRev, _, err = cache.Get(ctx, "doc3", revOrCV, testCollectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)

			assert.Equal(t, int64(118), memoryBytesCounted.Value())
			assert.Equal(t, int64(1), cacheNumItems.Value())

			docRev, _, err = cache.GetActive(ctx, "doc4", testCollectionID)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)

			assert.Equal(t, int64(118), memoryBytesCounted.Value())
			assert.Equal(t, int64(1), cacheNumItems.Value())
		})
	}
}

func TestResetRevCache(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("Revision cache expected to be used for this test")
	}
	dbcOptions := DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxBytes:      90,
			MaxItemCount:  10,
			InsertOnWrite: true,
		},
	}
	db, ctx := SetupTestDBWithOptions(t, dbcOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	cacheStats := db.DbStats.Cache()

	// add a doc
	docSize, _, _ := createDocAndReturnSizeAndRev(t, ctx, "doc1", collection, Body{"test": "doc"}, false)
	assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())
	assert.Equal(t, int64(1), cacheStats.RevisionCacheNumItems.Value())

	// re create rev cache
	db.FlushRevisionCacheForTest()

	// assert rev cache is reset as expected
	assert.Equal(t, int64(0), cacheStats.RevisionCacheTotalMemory.Value())
	assert.Equal(t, int64(0), cacheStats.RevisionCacheNumItems.Value())
}

func TestBasicOperationsOnCacheWithMemoryStat(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("Revision cache expected to be used for this test")
	}
	testCases := []struct {
		name     string
		useCVKey bool
	}{
		{
			name:     "revID key",
			useCVKey: false,
		},
		{
			name:     "cv key",
			useCVKey: true,
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
			collectionID := collection.GetCollectionID()
			var err error

			versionKey := func(cv *Version, revTreeID string) string {
				if testCase.useCVKey && cv != nil {
					return cv.String()
				}
				return revTreeID
			}

			// Test Put on new doc
			docSize, revID, docCV := createDocAndReturnSizeAndRev(t, ctx, "doc1", collection, Body{"test": "doc"}, testCase.useCVKey)
			assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())

			// Test Get with item in the cache
			var docRev DocumentRevision
			docRev, _, err = db.revisionCache.Get(ctx, "doc1", versionKey(docCV, revID), collectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)
			assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())
			revIDDoc1 := docRev.RevID
			cvDoc1 := docRev.CV

			// get again and ensure memory stat doesn't change
			docRev, _, err = db.revisionCache.Get(ctx, "doc1", versionKey(docCV, revID), collectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.Equal(t, int64(docSize), cacheStats.RevisionCacheTotalMemory.Value())

			// Test Get operation with load from bucket, need to first create and remove from rev cache
			prevMemStat := cacheStats.RevisionCacheTotalMemory.Value()
			revDoc2 := createThenRemoveFromRevCache(t, ctx, "doc2", db, collection)
			// load from doc from bucket
			docRev, _, err = db.revisionCache.Get(ctx, "doc2", versionKey(&revDoc2.CV, revDoc2.RevTreeID), collectionID, RevCacheDontLoadBackupRev)
			require.NoError(t, err)
			assert.NotNil(t, docRev.BodyBytes)
			assert.Equal(t, "doc2", docRev.DocID)
			assert.Greater(t, cacheStats.RevisionCacheTotalMemory.Value(), prevMemStat)

			// Test Get active with item resident in cache
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			docRev, _, err = db.revisionCache.GetActive(ctx, "doc2", collectionID)
			require.NoError(t, err)
			var doc2RevID string
			if testCase.useCVKey {
				// GetActive loads by revID and will key by revID as result
				docRev.CalculateBytes()
				assert.Equal(t, "doc2", docRev.DocID)
				assert.Equal(t, prevMemStat+docRev.MemoryBytes, cacheStats.RevisionCacheTotalMemory.Value())
				doc2RevID = docRev.RevID
			} else {
				assert.Equal(t, "doc2", docRev.DocID)
				assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())
			}

			// Test Get active with item to be loaded from bucket, need to first create and remove from rev cache
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			revDoc3 := createThenRemoveFromRevCache(t, ctx, "doc3", db, collection)
			docRev, _, err = db.revisionCache.GetActive(ctx, "doc3", collectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc3", docRev.DocID)
			assert.Greater(t, cacheStats.RevisionCacheTotalMemory.Value(), prevMemStat)
			var doc3RevID string
			if testCase.useCVKey {
				doc3RevID = docRev.RevID
			}

			// Test Peek at item not in cache, assert stats unchanged
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			docRev, ok := db.revisionCache.Peek(ctx, "doc4", versionKey(&Version{SourceID: "test", Value: 123}, "1-abc"), collectionID)
			require.False(t, ok)
			assert.Nil(t, docRev.BodyBytes)
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Peek in cache, assert stat unchanged
			docRev, ok = db.revisionCache.Peek(ctx, "doc3", versionKey(&revDoc3.CV, revDoc3.RevTreeID), collectionID)
			if testCase.useCVKey {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				assert.Equal(t, "doc3", docRev.DocID)
			}
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Test Remove with item not in cache, assert stat is unchanged
			prevMemStat = cacheStats.RevisionCacheTotalMemory.Value()
			db.revisionCache.Remove(ctx, "doc6", versionKey(&Version{SourceID: "test", Value: 123}, "1-abc"), collectionID)
			assert.Equal(t, prevMemStat, cacheStats.RevisionCacheTotalMemory.Value())

			// Empty cache and see memory stat is 0
			db.revisionCache.Remove(ctx, "doc3", versionKey(&revDoc3.CV, revDoc3.RevTreeID), collectionID)
			db.revisionCache.Remove(ctx, "doc2", versionKey(&revDoc2.CV, revDoc2.RevTreeID), collectionID)
			db.revisionCache.Remove(ctx, "doc1", versionKey(cvDoc1, revIDDoc1), collectionID)
			// remove items added by GetActive calls (only needed in cv key mode since GetActive keys by revID)
			if testCase.useCVKey {
				db.revisionCache.Remove(ctx, "doc3", doc3RevID, collectionID)
				db.revisionCache.Remove(ctx, "doc2", doc2RevID, collectionID)
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
		MaxItemCount:  10,
		MaxBytes:      0,
		InsertOnWrite: true,
	}
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc123", RevID: "1-abc", CV: &Version{Value: 123, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)
	_, _, err := cache.Get(base.TestCtx(t), "doc123", "1-abc", testCollectionID, false)

	assert.NoError(t, err)
}

// Ensure subsequent updates to delta don't mutate previously retrieved deltas
func TestConcurrentLoad(t *testing.T) {
	cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&testBackingStore{nil, &getDocumentCounter, &getRevisionCounter}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount:  10,
		MaxBytes:      0,
		InsertOnWrite: true,
	}
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	cache.Put(base.TestCtx(t), DocumentRevision{BodyBytes: []byte(`{"test":"1234"}`), DocID: "doc1", RevID: "1-abc", CV: &Version{Value: 1234, SourceID: "test"}, History: Revisions{"start": 1}}, testCollectionID)

	// Trigger load into cache
	var wg sync.WaitGroup
	wg.Add(20)
	for range 20 {
		go func() {
			_, _, err := cache.Get(base.TestCtx(t), "doc1", "1-abc", testCollectionID, false)
			assert.NoError(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

}

func TestRevisionCacheRemove(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test requires revision cache to be enabled")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	rev1id, _, err := collection.Put(ctx, "doc", Body{"val": 123})
	assert.NoError(t, err)

	docRev, err := collection.revisionCache.Get(base.TestCtx(t), "doc", rev1id, RevCacheLoadBackupRev)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())

	collection.revisionCache.Remove(ctx, "doc", rev1id)

	docRev, err = collection.revisionCache.Get(base.TestCtx(t), "doc", rev1id, RevCacheLoadBackupRev)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.revisionCache.GetActive(ctx, "doc")
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.GetRev(ctx, "doc", docRev.RevID, true, nil)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())

	docRev, err = collection.GetRev(ctx, "doc", "", true, nil)
	assert.NoError(t, err)
	assert.Equal(t, rev1id, docRev.RevID)
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())
}

// TestRevCacheHitMultiCollection:
//   - Create db with two collections
//   - Add docs of the same ID to each collection the database
//   - Perform a Get for each doc in each collection
//   - Assert each doc returned is the correct one (correct rev ID etc)
//   - Assert that each doc is found at the rev cache and no misses are reported
func TestRevCacheHitMultiCollection(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test requires revision cache to be enabled")
	}
	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))
	dbOptions := DatabaseContextOptions{}
	dbOptions.Scopes = GetScopesOptions(t, tb, 2)

	db, ctx := SetupTestDBForBucketWithOptions(t, tb, dbOptions)
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
	assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheHits.Value())
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())

	// Perform a get for the doc in each collection again asserting we get from rev cache this time
	for i, collection := range collectionList {
		ctx := collection.AddCollectionContext(ctx)
		docRev, err := collection.GetRev(ctx, "doc", revList[i], false, nil)
		require.NoError(t, err)
		assert.Equal(t, "doc", docRev.DocID)
		assert.Equal(t, revList[i], docRev.RevID)
	}

	// assert two hits now recorded
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheHits.Value())
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())
}

// TestRevCacheHitMultiCollectionLoadFromBucket:
//   - Create db with two collections and have rev cache size of 1
//   - Create two docs one on each collection
//   - Perform Get on first doc to trigger load from bucket, assert doc is as expected
//   - This in turn evicts the second doc
//   - Perform Get on that second doc to trigger load from the bucket, assert doc is as expected
func TestRevCacheHitMultiCollectionLoadFromBucket(t *testing.T) {

	if base.TestDisableRevCache() {
		t.Skip("test requires use of revision cache")
	}
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

	db, ctx := SetupTestDBForBucketWithOptions(t, tb, dbOptions)
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
		useCVCache bool
	}{
		{name: "revID key", useCVCache: false},
		{name: "cv key", useCVCache: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cacheHitCounter, cacheMissCounter, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
			cacheNumItems, cacheMemoryStat := base.SgwIntStat{}, base.SgwIntStat{}
			backingStoreMap := CreateTestSingleBackingStoreMap(
				&testBackingStore{[]string{"badDoc"}, &getDocumentCounter, &getRevisionCounter},
				testCollectionID,
			)
			revCacheStats := revisionCacheStats{
				cacheHitStat:      &cacheHitCounter,
				cacheMissStat:     &cacheMissCounter,
				cacheNumItemsStat: &cacheNumItems,
				cacheMemoryStat:   &cacheMemoryStat,
			}
			cache := NewLRURevisionCache(&RevisionCacheOptions{MaxItemCount: 5, MaxBytes: 0}, backingStoreMap, revCacheStats, newCacheMemoryController(0, revCacheStats.cacheMemoryStat))
			ctx := base.TestCtx(t)
			cv := &Version{SourceID: "test", Value: 123}
			const revID = "1-abc"

			// assertItems checks both the stat and the underlying map stay consistent.
			assertItems := func(expected int64) {
				t.Helper()
				assert.Equal(t, expected, cacheNumItems.Value())
				assert.Equal(t, int64(len(cache.cache)), cacheNumItems.Value())
			}

			// getDoc abstracts over the two lookup pathways under test.
			getDoc := func(docID string) (DocumentRevision, error) {
				if tc.useCVCache {
					docRev, _, err := cache.Get(ctx, docID, cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
					return docRev, err
				}
				docRev, _, err := cache.Get(ctx, docID, revID, testCollectionID, RevCacheLoadBackupRev)
				return docRev, err
			}

			// removeDoc removes an entry using the key type appropriate to the pathway under test.
			removeDoc := func(docID string) {
				if tc.useCVCache {
					cache.Remove(ctx, docID, cv.String(), testCollectionID)
				} else {
					cache.Remove(ctx, docID, revID, testCollectionID)
				}
			}

			assertItems(0)

			// Put adds a CV-keyed entry.
			cache.Put(ctx, documentRevisionForCacheTest("doc1", `{"test":"1"}`), testCollectionID)
			assertItems(1)

			// A failed load does not leave a stale entry in the cache.
			_, err := getDoc("badDoc")
			require.Error(t, err)
			assertItems(1)

			// Cache miss: entry is loaded from the backing store and added to the cache.
			docRev, err := getDoc("doc2")
			require.NoError(t, err)
			assert.Equal(t, "doc2", docRev.DocID)
			assertItems(2)

			// Cache hit: count is unchanged.
			docRev, err = getDoc("doc2")
			require.NoError(t, err)
			assert.Equal(t, "doc2", docRev.DocID)
			assertItems(2)

			// GetActive cache miss: loads from backing store via a revID key.
			docRev, _, err = cache.GetActive(ctx, "doc3", testCollectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc3", docRev.DocID)
			assertItems(3)

			// GetActive cache hit: count is unchanged.
			docRev, _, err = cache.GetActive(ctx, "doc3", testCollectionID)
			require.NoError(t, err)
			assert.Equal(t, "doc3", docRev.DocID)
			assertItems(3)

			// Upsert on an existing doc replaces its entry without incrementing the count.
			cache.Upsert(ctx, documentRevisionForCacheTest("doc1", `{"test":"updated"}`), testCollectionID)
			assertItems(3)

			// Upsert on a new doc increments the count.
			cache.Upsert(ctx, documentRevisionForCacheTest("doc4", `{"test":"4"}`), testCollectionID)
			assertItems(4)

			// Peek on a cached doc does not modify the count.
			peekKey := cv.String()
			if !tc.useCVCache {
				peekKey = revID
			}
			docRev, ok := cache.Peek(ctx, "doc2", peekKey, 0)
			require.True(t, ok)
			assert.Equal(t, "doc2", docRev.DocID)
			assertItems(4)

			// Peek on a non-cached doc does not modify the count.
			notFoundKey := cv.String()
			if !tc.useCVCache {
				notFoundKey = revID
			}
			_, ok = cache.Peek(ctx, "doc5", notFoundKey, 0)
			require.False(t, ok)
			assertItems(4)

			// Put a new doc reaching capacity: count increments but no eviction occurs.
			cache.Put(ctx, documentRevisionForCacheTest("doc5", `{"test":"5"}`), testCollectionID)
			assertItems(5)

			// Upsert a new doc beyond capacity: the LRU entry (doc3) is evicted to stay within the limit.
			cache.Upsert(ctx, documentRevisionForCacheTest("doc6", `{"test":"6"}`), testCollectionID)
			assertItems(5)

			// Remove all remaining entries. doc3 was evicted above so its removal is a no-op.
			// doc1, doc4, doc5, doc6 were always Put/Upserted (CV key). doc2 follows the test pathway key type.
			cache.Remove(ctx, "doc3", revID, testCollectionID)
			cache.Remove(ctx, "doc1", cv.String(), testCollectionID)
			removeDoc("doc2")
			cache.Remove(ctx, "doc4", cv.String(), testCollectionID)
			cache.Remove(ctx, "doc5", cv.String(), testCollectionID)
			cache.Remove(ctx, "doc6", cv.String(), testCollectionID)
			assertItems(0)
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
	_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
	require.NoError(t, err)

	testCtx, testCtxCancel := context.WithCancelCause(base.TestCtx(t))
	defer testCtxCancel(errors.New("test teardown"))

	for i := range 2 {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, _, err = db.revisionCache.Get(ctx, docID, revID, collection.GetCollectionID(), RevCacheDontLoadBackupRev) //nolint:errcheck
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
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

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

	docRev, _, err := cache.Get(base.TestCtx(t), "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, base.SetOf("chan1"), docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, int64(1), cacheHitCounter.Value())
	assert.Equal(t, int64(0), cacheMissCounter.Value())

	documentRevision.BodyBytes = []byte(`{"test":"12345"}`)

	cache.Upsert(base.TestCtx(t), documentRevision, testCollectionID)

	docRev, _, err = cache.Get(base.TestCtx(t), "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
	require.NoError(t, err)
	assert.Equal(t, "doc1", docRev.DocID)
	assert.Equal(t, base.SetOf("chan1"), docRev.Channels)
	assert.Equal(t, "test", docRev.CV.SourceID)
	assert.Equal(t, uint64(123), docRev.CV.Value)
	assert.Equal(t, []byte(`{"test":"12345"}`), docRev.BodyBytes)
	assert.Equal(t, int64(2), cacheHitCounter.Value())
	assert.Equal(t, int64(0), cacheMissCounter.Value())

	// remove the doc rev from the cache and assert that the document is no longer present in cache
	cache.Remove(base.TestCtx(t), "doc1", cv.String(), testCollectionID)
	assert.Equal(t, 0, len(cache.cache))
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
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	ctx := base.TestCtx(b)

	// trigger load into cache
	for i := range 5000 {
		_, _, _ = cache.Get(ctx, fmt.Sprintf("doc%d", i), "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// GET the document until test run has completed
		for pb.Next() {
			docId := fmt.Sprintf("doc%d", rand.Intn(5000))
			_, _, _ = cache.Get(ctx, docId, "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
		}
	})
}

// createThenRemoveFromRevCache will create a doc and then immediately remove it from the rev cache
func createThenRemoveFromRevCache(t *testing.T, ctx context.Context, docID string, db *Database, collection *DatabaseCollectionWithUser) DocVersion {
	revIDDoc, doc, err := collection.Put(ctx, docID, Body{"test": "doc"})
	require.NoError(t, err)

	db.revisionCache.Remove(ctx, docID, revIDDoc, collection.GetCollectionID())
	docVersion := DocVersion{
		RevTreeID: doc.GetRevTreeID(),
	}
	if doc.HLV != nil {
		docVersion.CV = *doc.HLV.ExtractCurrentVersionFromHLV()
	}
	return docVersion
}

// createDocAndReturnSizeAndRev creates a rev and measures its size based on rev cache measurements
func createDocAndReturnSizeAndRev(t *testing.T, ctx context.Context, docID string, collection *DatabaseCollectionWithUser, body Body, useCVKey bool) (int, string, *Version) {

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

	revOrCV := rev
	if useCVKey {
		revOrCV = doc.HLV.GetCurrentVersionString()
	}

	// do fetch to load into cache
	_, err = collection.getRev(ctx, docID, revOrCV, 0, nil)
	require.NoError(t, err)

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
	_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
	require.NoError(t, err)

	ctx, testCtxCancel := context.WithCancelCause(ctx)
	defer testCtxCancel(errors.New("test teardown"))

	for i := range 2 {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_, _, err = db.revisionCache.Get(ctx, docID, revID, collection.GetCollectionID(), RevCacheDontLoadBackupRev) //nolint:errcheck
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
	_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
	require.NoError(t, err)

	testCtx, testCtxCancel := context.WithCancelCause(base.TestCtx(t))
	defer testCtxCancel(errors.New("test teardown"))

	for i := range 2 {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, _, err = db.revisionCache.Get(ctx, docID, revID, collection.GetCollectionID(), RevCacheDontLoadBackupRev) //nolint:errcheck
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
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	// create cv with incorrect version to the one stored in backing store
	cv := Version{SourceID: "test", Value: 1234}

	_, _, err := cache.Get(base.TestCtx(t), "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
	require.Error(t, err)
	require.Error(t, err, base.ErrNotFound)
	assert.Equal(t, int64(0), cacheHitCounter.Value())
	assert.Equal(t, int64(1), cacheMissCounter.Value())
	assert.Equal(t, 0, cache.lruList.Len())
	assert.Equal(t, 0, len(cache.cache))
}

// TestConcurrentLoadByCVAndRevOnCache:
//   - Create cache
//   - Now perform two concurrent Gets, one by CV and one by revid on a document that doesn't exist in the cache
//   - This will trigger two concurrent loads from bucket in the CV code path and revid code path
//   - Ending up with two elements in the cache for the same document, one keyed by revID and the other keyed by cv
func TestConcurrentLoadByCVAndRevOnCache(t *testing.T) {
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted, getDocumentCounter, getRevisionCounter := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	ctx := base.TestCtx(t)

	wg := sync.WaitGroup{}
	wg.Add(2)

	cv := Version{SourceID: "test", Value: 123}
	go func() {
		_, _, err := cache.Get(ctx, "doc1", "1-abc", testCollectionID, RevCacheDontLoadBackupRev)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, _, err := cache.Get(ctx, "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
		require.NoError(t, err)
		wg.Done()
	}()

	wg.Wait()

	cvKey := CreateRevisionCacheKey("doc1", cv.String(), testCollectionID)
	revKey := CreateRevisionCacheKey("doc1", "1-abc", testCollectionID)
	revElement := cache.cache[revKey].Value.(*revCacheValue)
	cvElement := cache.cache[cvKey].Value.(*revCacheValue)

	assert.LessOrEqual(t, cache.lruList.Len(), 2)
	assert.Equal(t, 2, len(cache.cache))
	assert.Equal(t, revElement.revID, cvElement.revID)
	assert.Equal(t, revElement.id, cvElement.id)
	assert.Equal(t, revElement.cv.String(), cvElement.cv.String())
	require.JSONEq(t, string(revElement.bodyBytes), string(cvElement.bodyBytes))
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
	collection.revisionCache.Remove(ctx, "doc", expectedCV.String())

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
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, CreateTestSingleBackingStoreMap(&testBackingStore{[]string{"test_doc"}, &getDocumentCounter, &getRevisionCounter}, testCollectionID), revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	ctx := base.TestCtx(t)

	wg := sync.WaitGroup{}
	wg.Add(2)

	cv := Version{SourceID: "test", Value: 123}
	docRev := DocumentRevision{
		DocID:     "doc1",
		RevID:     "1-abc",
		BodyBytes: []byte(`{"testing":true}`),
		Channels:  base.SetOf("chan1"),
		History:   Revisions{"start": 1},
		CV:        &cv,
	}

	go func() {
		_, _, err := cache.Get(ctx, "doc1", cv.String(), testCollectionID, RevCacheDontLoadBackupRev)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		cache.Put(ctx, docRev, testCollectionID)
		wg.Done()
	}()

	wg.Wait()

	key := CreateRevisionCacheKey("doc1", cv.String(), testCollectionID)
	elem := cache.cache[key].Value.(*revCacheValue)

	assert.Equal(t, cache.lruList.Len(), 1)
	assert.Equal(t, 1, len(cache.cache))
	assert.Equal(t, docRev.RevID, elem.revID)
	assert.Equal(t, docRev.DocID, elem.id)
	assert.Equal(t, docRev.CV.String(), elem.cv.String())
	var docRevElem Body
	var cacheElem Body
	err := base.JSONUnmarshal(docRev.BodyBytes, &docRevElem)
	require.NoError(t, err)
	err = base.JSONUnmarshal(elem.bodyBytes, &cacheElem)
	require.NoError(t, err)
	assert.Equal(t, docRevElem["testing"].(bool), cacheElem["testing"].(bool))
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
	revID, _, err := collection.Put(ctx, docID, Body{"ver": "0"})
	require.NoError(t, err)
	_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
	require.NoError(t, err)
	wg.Add(1)

	testCtx, testCtxCancel := context.WithCancelCause(base.TestCtx(t))
	defer testCtxCancel(errors.New("test teardown"))

	for i := range 2 {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, _, err = db.revisionCache.Get(ctx, docID, revID, collection.GetCollectionID(), RevCacheDontLoadBackupRev) //nolint:errcheck
				}
			}
		}()
	}

	go func() {
		for i := range 100 {
			err = collection.dataStore.Set(docID, 0, nil, fmt.Appendf(nil, `{"ver": "%d"}`, i))
			require.NoError(t, err)
			_, _, err := db.revisionCache.GetActive(ctx, docID, collection.GetCollectionID())
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
	_, err = collection.getRev(ctx, docID, rev1ID, 0, nil) // load into cache
	require.NoError(t, err)
	wg.Add(1)

	testCtx, testCtxCancel := context.WithCancelCause(base.TestCtx(t))
	defer testCtxCancel(errors.New("test teardown"))

	for i := range 2 {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, _, err = db.revisionCache.Get(ctx, docID, revID, collection.GetCollectionID(), RevCacheDontLoadBackupRev) //nolint:errcheck
				}
			}
		}()
	}

	var getErr error
	go func() {
		for range 100 {
			_, _, getErr = db.revisionCache.Get(ctx, docID, rev1ID, collection.GetCollectionID(), RevCacheDontLoadBackupRev)
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

	testCtx, testCtxCancel := context.WithCancelCause(base.TestCtx(t))
	defer testCtxCancel(errors.New("test teardown"))

	for i := range 2 {
		docID := fmt.Sprintf("extraDoc%d", i)
		revID, _, err := collection.Put(ctx, docID, Body{"fake": "body"})
		require.NoError(t, err)
		_, err = collection.getRev(ctx, docID, revID, 0, nil) // load into cache
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					_, _, err = db.revisionCache.Get(ctx, docID, revID, collection.GetCollectionID(), RevCacheDontLoadBackupRev) //nolint:errcheck
				}
			}
		}()
	}

	go func() {
		for i := range 100 {
			docRev := DocumentRevision{DocID: docID, RevID: fmt.Sprintf("1-%d", i), CV: &Version{SourceID: "someSrc", Value: uint64(i)}, BodyBytes: fmt.Appendf(nil, `{"ver": "%d"}`, i), History: Revisions{"start": 1}}
			db.revisionCache.Put(ctx, docRev, collection.GetCollectionID())
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestRevCacheOnDemandImportNoCache(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test requires rev cache enabled")
	}
	base.SkipImportTestsIfNotEnabled(t)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := "doc1"
	revID1, doc, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)
	cv := doc.HLV.ExtractCurrentVersionFromHLV()

	// rev 1 is not in cache given we don't write to cache on write
	_, exists := collection.revisionCache.Peek(ctx, docID, revID1)
	require.False(t, exists)
	_, exists = collection.revisionCache.Peek(ctx, docID, cv.String())
	require.False(t, exists)

	require.NoError(t, collection.dataStore.Set(docID, 0, nil, []byte(`{"foo": "baz"}`)))

	doc, err = collection.GetDocument(ctx, docID, DocUnmarshalSync)
	require.NoError(t, err)
	require.Equal(t, Body{"foo": "baz"}, doc.Body(ctx))

	// rev1 still won't exist in cache
	_, exists = collection.revisionCache.Peek(ctx, docID, revID1)
	require.False(t, exists)
	_, exists = collection.revisionCache.Peek(ctx, docID, cv.String())
	require.False(t, exists)

	// rev2 is not in cache but is on server
	_, exists = collection.revisionCache.Peek(ctx, docID, doc.GetRevTreeID())
	require.False(t, exists)
	_, exists = collection.revisionCache.Peek(ctx, docID, doc.HLV.GetCurrentVersionString())
	require.False(t, exists)
}

func TestFetchBackupWithDeletedFlag(t *testing.T) {

	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{
		// enable delta sync so CV revs are backed up
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled:          true,
			RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
		},
	})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := t.Name()
	revID1, doc1, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)

	deleteVrs := DocVersion{
		RevTreeID: revID1,
	}
	revID2, deleteDoc, err := collection.DeleteDoc(ctx, docID, deleteVrs)
	require.NoError(t, err)

	// flush cache
	db.FlushRevisionCacheForTest()

	docRev, err := collection.revisionCache.Get(ctx, docID, doc1.HLV.GetCurrentVersionString(), RevCacheLoadBackupRev)
	require.NoError(t, err)

	assert.Equal(t, doc1.HLV.GetCurrentVersionString(), docRev.CV.String())
	// assert backup rev is not marked as deleted
	assert.False(t, docRev.Deleted)

	// resurrect the doc
	_, _, err = collection.Put(ctx, docID, Body{"foo": "baz", BodyRev: revID2})
	require.NoError(t, err)

	// flush cache
	db.FlushRevisionCacheForTest()

	// fetch deleted, will get backup rev and assert that the deleted flag is true
	docRev, err = collection.revisionCache.Get(ctx, docID, deleteDoc.HLV.GetCurrentVersionString(), RevCacheLoadBackupRev)
	require.NoError(t, err)

	assert.Equal(t, deleteDoc.HLV.GetCurrentVersionString(), docRev.CV.String())
	// assert backup rev is marked as deleted
	assert.True(t, docRev.Deleted)
}

func TestCorrectHLVWhenFetchingBackupRev(t *testing.T) {
	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{
		// enable delta sync so CV revs are backed up
		DeltaSyncOptions: DeltaSyncOptions{
			Enabled:          true,
			RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
		},
	})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := t.Name()
	agent := NewHLVAgent(t, collection.dataStore, "someSourceID", base.VvXattrName)

	_ = agent.InsertWithHLV(ctx, docID, nil)

	docRev, err := collection.getRev(ctx, docID, "", 0, nil)
	require.NoError(t, err)
	cv := docRev.CV.String()

	_, _, err = collection.Put(ctx, docID, Body{"foo": "bar", BodyRev: docRev.RevID})
	require.NoError(t, err)

	// flush cache
	db.FlushRevisionCacheForTest()

	// fetch backup rev and assert that the HLV is correct
	_, err = collection.getRev(ctx, docID, docRev.CV.String(), 0, nil)
	require.ErrorContains(t, err, "missing")

	// flush cache
	db.FlushRevisionCacheForTest()

	// fetch using lower level cache fetch with load from bucket bool true to get the backup rev
	docRev, err = collection.revisionCache.Get(ctx, docID, docRev.CV.String(), RevCacheLoadBackupRev)
	require.NoError(t, err)

	// hlv history should be empty as we are fetching from backup rev
	assert.Empty(t, docRev.HlvHistory)
	assert.Equal(t, cv, docRev.CV.String())
}

func TestLoadFromBucketLegacyRevsThatAreBackedUpPreUpgrade(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test requires rev cache enabled")
	}
	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{
		OldRevExpirySeconds: base.DefaultOldRevExpirySeconds,
		RevisionCacheOptions: &RevisionCacheOptions{
			ShardCount:   1, // turn off sharding for simplicity
			MaxItemCount: DefaultRevisionCacheSize,
			MaxBytes:     0, // turn off memory limit
		},
	})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a few legacy revs
	docID := t.Name()
	revIDs := make([]string, 0)
	legacyRev, _ := collection.CreateDocNoHLV(t, ctx, docID, Body{"foo": "bar"})
	revIDs = append(revIDs, legacyRev)
	for range 2 {
		newRev, _ := collection.CreateDocNoHLV(t, ctx, docID, Body{BodyRev: legacyRev, "foo": "bar"})
		revIDs = append(revIDs, newRev)
		legacyRev = newRev // OCC val
	}
	// simulate doc revs that are backed up to bucket pre upgrade
	for i := range 2 {
		err := collection.setOldRevisionJSONBody(ctx, docID, revIDs[i], []byte(`{"foo":"bar"}`), collection.oldRevExpirySeconds())
		require.NoError(t, err)
	}

	// flush all revisions from rev cache to force load from bucket
	db.FlushRevisionCacheForTest()

	// fetch all three legacy revisions, first two should be loaded from old backup revisions
	for i := range 3 {
		docRev, err := collection.getRev(ctx, docID, revIDs[i], 0, nil)
		require.NoError(t, err)
		assert.Equal(t, revIDs[i], docRev.RevID)
	}

	// assert that each legacy rev is populated in rev lookup
	for _, revID := range revIDs {
		docRev, ok := collection.revisionCache.Peek(ctx, docID, revID)
		require.True(t, ok)
		assert.Equal(t, revID, docRev.RevID)
	}

	// assert these were all fetched from the rev cache and not loaded from bucket (no rev cache misses)
	// 3 misses are from the initial load from bucket after rev cache flush above
	assert.Equal(t, int64(3), db.DbStats.Cache().RevisionCacheMisses.Value())
	assert.Equal(t, int64(3), db.DbStats.Cache().RevisionCacheNumItems.Value())
}

func TestRaceRemovingStaleCVValue(t *testing.T) {
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryBytesCounted := base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}, base.SgwIntStat{}
	backingStoreMap := CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID)
	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 10,
		MaxBytes:     0,
	}
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHitCounter,
		cacheMissStat:     &cacheMissCounter,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &memoryBytesCounted,
	}
	cache := NewLRURevisionCache(cacheOptions, backingStoreMap, revCacheStats, newCacheMemoryController(cacheOptions.MaxBytes, revCacheStats.cacheMemoryStat))

	ctx := base.TestCtx(t)

	docRev := DocumentRevision{
		BodyBytes: []byte(`{"some":"data"}`),
		DocID:     "doc1",
		RevID:     "1-abc",
		CV:        &Version{Value: 123, SourceID: "test"},
		History:   Revisions{"start": 1},
	}
	docRev.CalculateBytes()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		cache.Put(ctx, docRev, testCollectionID)
		wg.Done()
	}()

	go func() {
		cache.Remove(ctx, "doc1", Version{Value: 123, SourceID: "test"}.String(), testCollectionID)
		wg.Done()
	}()

	wg.Wait()

	// Two valid outcomes depending on goroutine scheduling:
	//   - Remove ran before Put inserted the item (Remove was a no-op): item is still in the
	//     cache and the stat reflects its bytes.
	//   - Remove ran after Put's getValue (our memState fix handles the race): the item has
	//     been removed and the stat must be 0.
	// In either case the stat must be consistent with what is actually in the cache.
	_, inCache := cache.Peek(ctx, "doc1", Version{Value: 123, SourceID: "test"}.String(), testCollectionID)
	if inCache {
		assert.Equal(t, docRev.MemoryBytes, memoryBytesCounted.Value(), "item is in cache so stat should equal item bytes")
	} else {
		assert.Equal(t, int64(0), memoryBytesCounted.Value(), "item was removed so stat should be 0")
	}
}

func TestItemResidentInCacheBackupRevLoaded(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test requires rev cache enabled")
	}
	testCases := []struct {
		name     string
		useRevID bool
	}{
		{"FetchByRevID", true},
		{"FetchByCV", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{
				OldRevExpirySeconds: base.DefaultOldRevExpirySeconds,
				RevisionCacheOptions: &RevisionCacheOptions{
					ShardCount:   1,  // turn off sharding for simplicity
					MaxItemCount: 10, // turn off size based eviction
					MaxBytes:     0,  // turn off memory limit
				},
				DeltaSyncOptions: DeltaSyncOptions{
					Enabled:          true,
					RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
				},
			})
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

			docID := SafeDocumentName(t, t.Name())
			var firstRevisionID, firstCV string
			_, doc1, err := collection.Put(ctx, docID, Body{"foo": "bar"})
			require.NoError(t, err)

			firstRevisionID = doc1.GetRevTreeID()
			firstCV = doc1.HLV.GetCurrentVersionString()

			// make revision 2
			_, doc1, err = collection.Put(ctx, docID, Body{BodyRev: firstRevisionID, "foo": "baz"})
			require.NoError(t, err)

			// flush cache, this can be removed pending CBG-4542
			db.FlushRevisionCacheForTest()
			// grab current version of the doc
			if tc.useRevID {
				_, err = collection.getRev(ctx, docID, doc1.GetRevTreeID(), 0, nil)
			} else {
				_, err = collection.getRev(ctx, docID, doc1.HLV.GetCurrentVersionString(), 0, nil)
			}
			require.NoError(t, err)

			// now fetch for backup rev, should load rev and only populate CV map and not revID map when fetching
			// by CV and vice versa for fetch by revID
			if tc.useRevID {
				_, err = collection.getRev(ctx, docID, firstRevisionID, 0, nil)
				require.NoError(t, err)
				assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheNumItems.Value())
			} else {
				_, err = collection.getRev(ctx, docID, firstCV, 0, nil)
				require.Error(t, err)
				require.ErrorContains(t, err, "missing")
				assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheNumItems.Value())
			}
			// assert on stats after fetch
			assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheMisses.Value())
			assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheHits.Value())

			// now fetch the current revision by the opposite method to which we fetched the backup rev above,
			// assert we get the correct document
			var docRev DocumentRevision
			if tc.useRevID {
				docRev, err = collection.getRev(ctx, docID, doc1.HLV.GetCurrentVersionString(), 0, nil)
			} else {
				docRev, err = collection.getRev(ctx, docID, doc1.GetRevTreeID(), 0, nil)
			}
			require.NoError(t, err)
			assert.Equal(t, doc1.GetRevTreeID(), docRev.RevID)
			assert.Equal(t, doc1.HLV.GetCurrentVersionString(), docRev.CV.String())
			assert.JSONEq(t, `{"foo": "baz"}`, string(docRev.BodyBytes))
			if tc.useRevID {
				assert.Equal(t, int64(3), db.DbStats.Cache().RevisionCacheNumItems.Value())
			} else {
				assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheNumItems.Value())
			}
			assert.Equal(t, int64(3), db.DbStats.Cache().RevisionCacheMisses.Value())
			assert.Equal(t, int64(0), db.DbStats.Cache().RevisionCacheHits.Value())
		})
	}
}

// TestMultipleRevCacheItemsForSameDocs:
// - Tests fetching same doc by revID and CV will create separate entries for the same doc
func TestMultipleRevCacheItemsForSameDocs(t *testing.T) {
	if base.TestDisableRevCache() {
		t.Skip("test requires rev cache enabled")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docID := SafeDocumentName(t, t.Name())
	docID2 := SafeDocumentName(t, t.Name()+"_2")

	_, doc1, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)

	doc2Rev, doc2, err := collection.Put(ctx, docID2, Body{"foo": "bar"})
	require.NoError(t, err)

	// get active docID, get active uses revID so will populate rev cache with revID key
	_, err = collection.getRev(ctx, docID, "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheNumItems.Value())

	// get by cv value
	_, err = collection.getRev(ctx, docID, doc1.HLV.GetCurrentVersionString(), 0, nil)
	require.NoError(t, err)
	// assert that two items are in cache now (both for same docID)
	assert.Equal(t, int64(2), db.DbStats.Cache().RevisionCacheNumItems.Value())

	// fetch docID2 by revID
	_, err = collection.getRev(ctx, docID2, doc2Rev, 0, nil)
	require.NoError(t, err)
	// assert that three items are in cache now
	assert.Equal(t, int64(3), db.DbStats.Cache().RevisionCacheNumItems.Value())

	// fetch docID2 by CV
	_, err = collection.getRev(ctx, docID2, doc2.HLV.GetCurrentVersionString(), 0, nil)
	require.NoError(t, err)
	// assert four items now in cache
	assert.Equal(t, int64(4), db.DbStats.Cache().RevisionCacheNumItems.Value())
}

// TestMemoryBasedEvictionBetweenRevAndDeltaCache verifies that the shared CacheMemoryController
// evicts across both LRURevisionCache and LRUDeltaCache, always choosing the globally oldest item
// (by access order). The test drives two round-trips:
//
//  1. Get rev1  → loaded from backing store, memory at capacity (no eviction).
//  2. UpdateDelta(rev1→rev2) → total exceeds limit; rev1 (older access order) is evicted from
//     the revision cache.
//  3. Get rev1 again → loaded from backing store, total exceeds limit again; the delta (now the
//     oldest item) is evicted from the delta cache.
//
// At each step the cache item counts and shared memory byte stat are asserted for consistency.
func TestMemoryBasedEvictionBetweenRevAndDeltaCache(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta sync and delta cache are EE only")
	}

	ctx := base.TestCtx(t)
	const docID = "testDoc"

	// testBackingStore always serves docID at cv{Value:123, SourceID:"test"}.
	rev1CV := Version{Value: 123, SourceID: "test"}
	rev2CV := Version{Value: 456, SourceID: "test"} // only used as the delta's toVersion key

	// Delta body bytes.  With no channels or revision history, totalDeltaBytes = len(DeltaBytes).
	deltaBodyBytes := []byte("delta_data") // 10 bytes
	const expectedDeltaBytes = int64(10)

	// testBackingStore produces 121 bytes for "testDoc" (verified empirically: body JSON + 1
	// history digest at 32 bytes + 1 channel byte "*").
	const expectedRev1Bytes = int64(121)

	// maxBytes=121: rev1 alone just fits (121 is NOT > 121), delta alone fits (10 < 121), but
	// together (131 > 121) they trigger memory-based eviction.
	const maxBytes = expectedRev1Bytes

	// Revision cache stats.
	var cacheHits, cacheMisses, cacheNumItems, cacheMemoryBytes base.SgwIntStat
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHits,
		cacheMissStat:     &cacheMisses,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &cacheMemoryBytes,
	}

	// Delta cache stats.
	var deltaCacheHit, deltaCacheMiss, deltaCacheNumItems base.SgwIntStat
	deltaStats := &base.DeltaSyncStats{
		DeltaCacheHit:      &deltaCacheHit,
		DeltaCacheMiss:     &deltaCacheMiss,
		DeltaCacheNumItems: &deltaCacheNumItems,
	}

	var docCounter, revCounter base.SgwIntStat
	bs := &testBackingStore{getDocumentCounter: &docCounter, getRevisionCounter: &revCounter}

	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 100, // high enough that number-based eviction never fires
		MaxBytes:     maxBytes,
	}
	orchestrator := NewRevisionCacheOrchestrator(
		cacheOptions,
		CreateTestSingleBackingStoreMap(bs, testCollectionID),
		revCacheStats,
		deltaStats,
		true, // initDeltaCache
	)

	// -----------------------------------------------------------------------
	// Step 1: Load rev1 via Get (cache miss — loaded from backing store).
	// Memory reaches the limit exactly; IsOverCapacity returns false (> not >=),
	// so no eviction occurs.
	// -----------------------------------------------------------------------
	_, wasMiss, err := orchestrator.Get(ctx, docID, rev1CV.String(), testCollectionID, false)
	require.NoError(t, err)
	require.True(t, wasMiss, "first Get should be a cache miss triggering a backing-store load")

	_, rev1InCache := orchestrator.Peek(ctx, docID, rev1CV.String(), testCollectionID)
	assert.True(t, rev1InCache, "rev1 should be in revision cache after Get")
	assert.Equal(t, int64(1), cacheNumItems.Value(), "revision cache should have one item")
	assert.Equal(t, int64(0), deltaCacheNumItems.Value(), "delta cache should be empty")
	assert.Equal(t, expectedRev1Bytes, cacheMemoryBytes.Value(), "memory stat should equal rev1 bytes")
	assert.Equal(t, int64(1), cacheMisses.Value())
	assert.Equal(t, int64(0), cacheHits.Value())

	// -----------------------------------------------------------------------
	// Step 2: Add delta rev1→rev2 via UpdateDelta.
	// Total (121+10=131) exceeds maxBytes (121).  Revision 1 is evicted due to round-
	// robin eviction approach
	// -----------------------------------------------------------------------
	delta := RevisionDelta{DeltaBytes: deltaBodyBytes}
	delta.CalculateDeltaBytes()
	orchestrator.UpdateDelta(ctx, docID, rev1CV.String(), rev2CV.String(), testCollectionID, delta)

	_, rev1InCacheAfterDelta := orchestrator.Peek(ctx, docID, rev1CV.String(), testCollectionID)
	assert.False(t, rev1InCacheAfterDelta, "rev1 should have been evicted")

	cachedDelta := orchestrator.deltaCache.getCachedDelta(ctx, docID, rev1CV.String(), rev2CV.String(), testCollectionID)
	assert.NotNil(t, cachedDelta, "delta still be present")

	assert.Equal(t, int64(0), cacheNumItems.Value(), "revision cache should not be empty after evicting rev1")
	assert.Equal(t, int64(1), deltaCacheNumItems.Value(), "delta cache should be empty")
	assert.Equal(t, expectedDeltaBytes, cacheMemoryBytes.Value(), "memory stat should equal rev1 bytes only after eviction")

	// -----------------------------------------------------------------------
	// Step 3: Load rev1 again via Get (cache miss — it was evicted in step 2).
	// Total (10+121=131) exceeds maxBytes again.  Deltas turn to be evicted.
	// -----------------------------------------------------------------------
	_, wasMiss2, err := orchestrator.Get(ctx, docID, rev1CV.String(), testCollectionID, false)
	require.NoError(t, err)
	require.True(t, wasMiss2, "second Get should be a cache miss (rev1 was evicted)")

	_, rev1BackInCache := orchestrator.Peek(ctx, docID, rev1CV.String(), testCollectionID)
	assert.True(t, rev1BackInCache, "rev1 should be back in revision cache after second Get")

	cachedDeltaAfter := orchestrator.deltaCache.getCachedDelta(ctx, docID, rev1CV.String(), rev2CV.String(), testCollectionID)
	assert.Nil(t, cachedDeltaAfter, "delta should've been evicted")

	assert.Equal(t, int64(1), cacheNumItems.Value(), "revision cache should be empty")
	assert.Equal(t, int64(0), deltaCacheNumItems.Value(), "delta cache should have one item")
	assert.Equal(t, expectedRev1Bytes, cacheMemoryBytes.Value(), "memory stat should equal delta bytes only after rev1 evicted")
	assert.Equal(t, int64(2), cacheMisses.Value(), "Two Get's were a cache miss")
	assert.Equal(t, int64(0), cacheHits.Value())
}

// TestMemoryBasedEvictionRevisionCacheOnly verifies memory-based eviction when the delta cache
// is disabled (delta sync off).  Two documents are loaded sequentially; each fits within the
// memory limit on its own, but together they exceed it, so the older document must be evicted
// when the second is loaded.
func TestMemoryBasedEvictionRevisionCacheOnly(t *testing.T) {
	ctx := base.TestCtx(t)

	// testBackingStore serves any docID at cv{Value:123, SourceID:"test"}.
	rev1CV := Version{Value: 123, SourceID: "test"}

	// testBackingStore produces 118 bytes for a 4-char docID (verified empirically: body JSON
	// + 1 history digest at 32 bytes + 1 channel byte "*").
	const expectedBytesPerDoc = int64(118)

	// maxBytes=118: one document just fits (118 is NOT > 118); two together (236 > 118)
	// trigger memory-based eviction of the older document.
	const maxBytes = expectedBytesPerDoc

	var cacheHits, cacheMisses, cacheNumItems, cacheMemoryBytes base.SgwIntStat
	revCacheStats := revisionCacheStats{
		cacheHitStat:      &cacheHits,
		cacheMissStat:     &cacheMisses,
		cacheNumItemsStat: &cacheNumItems,
		cacheMemoryStat:   &cacheMemoryBytes,
	}

	var docCounter, revCounter base.SgwIntStat
	bs := &testBackingStore{getDocumentCounter: &docCounter, getRevisionCounter: &revCounter}

	cacheOptions := &RevisionCacheOptions{
		MaxItemCount: 100, // number-based eviction disabled
		MaxBytes:     maxBytes,
	}
	// initDeltaCache=false mirrors the configuration when delta sync is disabled.
	orchestrator := NewRevisionCacheOrchestrator(
		cacheOptions,
		CreateTestSingleBackingStoreMap(bs, testCollectionID),
		revCacheStats,
		&base.DeltaSyncStats{},
		false,
	)

	// -----------------------------------------------------------------------
	// Step 1: Load doc1 via Get (cache miss).
	// Memory reaches the limit exactly; no eviction occurs.
	// -----------------------------------------------------------------------
	_, wasMiss, err := orchestrator.Get(ctx, "doc1", rev1CV.String(), testCollectionID, false)
	require.NoError(t, err)
	require.True(t, wasMiss, "first Get should be a cache miss")

	_, doc1InCache := orchestrator.Peek(ctx, "doc1", rev1CV.String(), testCollectionID)
	assert.True(t, doc1InCache, "doc1 should be in revision cache after Get")
	assert.Equal(t, int64(1), cacheNumItems.Value())
	assert.Equal(t, expectedBytesPerDoc, cacheMemoryBytes.Value(), "memory stat should equal doc1 bytes")
	assert.Equal(t, int64(1), cacheMisses.Value())
	assert.Equal(t, int64(0), cacheHits.Value())

	// -----------------------------------------------------------------------
	// Step 2: Load doc2 via Get (cache miss).
	// Total (118+118=236) exceeds maxBytes (118).  doc1 carries the older access
	// order, so it is evicted, leaving only doc2 in the revision cache.
	// -----------------------------------------------------------------------
	_, wasMiss2, err := orchestrator.Get(ctx, "doc2", rev1CV.String(), testCollectionID, false)
	require.NoError(t, err)
	require.True(t, wasMiss2, "second Get should be a cache miss")

	_, doc1InCacheAfter := orchestrator.Peek(ctx, "doc1", rev1CV.String(), testCollectionID)
	assert.False(t, doc1InCacheAfter, "doc1 should have been evicted from revision cache by memory pressure")

	_, doc2InCache := orchestrator.Peek(ctx, "doc2", rev1CV.String(), testCollectionID)
	assert.True(t, doc2InCache, "doc2 should be in revision cache")

	assert.Equal(t, int64(1), cacheNumItems.Value(), "only doc2 should remain in revision cache")
	assert.Equal(t, expectedBytesPerDoc, cacheMemoryBytes.Value(), "memory stat should equal doc2 bytes only after eviction")
	assert.Equal(t, int64(2), cacheMisses.Value(), "both Gets were cache misses")
	assert.Equal(t, int64(0), cacheHits.Value())
}

// makeTestRevision creates a minimal DocumentRevision for memory-accounting tests.
// MemoryBytes = len(body) because History is an empty (but non-nil) Revisions map
// and there are no channels, so CalculateBytes contributes 0 history/channel overhead.
func makeTestRevision(docID string, cv Version, body string) DocumentRevision {
	return DocumentRevision{
		DocID:     docID,
		CV:        &cv,
		BodyBytes: []byte(body),
		History:   Revisions{}, // non-nil satisfies Put's nil-history guard; contributes 0 bytes
	}
}

// newTestRevCacheStats returns zero-value stats for standalone cache tests.
func newTestRevCacheStats() revisionCacheStats {
	var hits, misses, numItems, cacheMemoryStat base.SgwIntStat
	return revisionCacheStats{
		cacheHitStat:      &hits,
		cacheMissStat:     &misses,
		cacheNumItemsStat: &numItems,
		cacheMemoryStat:   &cacheMemoryStat,
	}
}

// newTestDeltaStats returns zero-value delta stats for standalone cache tests.
func newTestDeltaStats() *base.DeltaSyncStats {
	var hit, miss, numItems base.SgwIntStat
	return &base.DeltaSyncStats{
		DeltaCacheHit:      &hit,
		DeltaCacheMiss:     &miss,
		DeltaCacheNumItems: &numItems,
	}
}

// TestEvictLRUTailOnLoadingItemReturnsZeroBytes verifies that evicting an item still in
// memStateLoading (bytes not yet accounted) returns 0 bytes and does not corrupt the
// memory stat.  It also verifies that the CAS Loading→Sized, which would normally
// follow a backing-store load, correctly fails after eviction — preventing a double-count.
func TestEvictLRUTailOnLoadingItemReturnsZeroBytes(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()

	opts := &RevisionCacheOptions{MaxItemCount: 100, MaxBytes: 0}
	mc := newCacheMemoryController(0, revStats.cacheMemoryStat)
	rc := NewLRURevisionCache(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID), revStats, mc,
	)

	docCV := Version{Value: 123, SourceID: "test"}

	// getValue(create=true) inserts a shell entry in memStateLoading with itemBytes=0.
	value := rc.getValue(ctx, "doc1", docCV.String(), testCollectionID, true)
	require.NotNil(t, value)
	assert.Equal(t, memStateLoading, value.memState.Load(), "item should start in Loading state")
	assert.Equal(t, int64(0), revStats.cacheMemoryStat.Value(), "no bytes counted while item is still loading")
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())

	// Evict the still-loading item — must return 0 bytes, not inflate or deflate the stat.
	bytesReturned, evicted := rc.evictLRUTail()
	assert.True(t, evicted)
	assert.Equal(t, int64(0), bytesReturned, "evicting a loading item must return 0 bytes")
	assert.Equal(t, int64(0), revStats.cacheMemoryStat.Value(), "memory stat must not go negative")
	assert.Equal(t, memStateRemoved, value.memState.Load(), "evicted item should be in Removed state")
	assert.Equal(t, int64(0), revStats.cacheNumItemsStat.Value())

	// Simulate the backing-store load completing after eviction: store bytes then attempt
	// the CAS that Get would perform.  The CAS must fail (state is already Removed),
	// preventing any bytes from being counted for a no-longer-cached item.
	value.itemBytes.Store(int64(50))
	casSucceeded := value.memState.CompareAndSwap(memStateLoading, memStateSized)
	assert.False(t, casSucceeded, "CAS Loading→Sized must fail on an already-evicted item")
	assert.Equal(t, int64(0), revStats.cacheMemoryStat.Value(), "memory stat must stay at 0 after the failed CAS")
}

// TestUpsertReplacesItemMemoryBytes verifies that Upserting a document with a different
// body size correctly decrements the old bytes and increments the new bytes, so the stat
// always reflects the current item's size only — no double-counting, no stale bytes.
func TestUpsertReplacesItemMemoryBytes(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()

	opts := &RevisionCacheOptions{MaxItemCount: 100, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	docCV := Version{Value: 1, SourceID: "test"}

	// First Upsert: small body — MemoryBytes = len(`{"v":1}`) = 7.
	orchestrator.Upsert(ctx, makeTestRevision("doc1", docCV, `{"v":1}`), testCollectionID)
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())
	assert.Equal(t, int64(7), revStats.cacheMemoryStat.Value(), "first upsert should count 7 bytes")

	// Second Upsert: same doc/CV, larger body — MemoryBytes = 22.
	// Old bytes (7) must be decremented and new bytes (22) incremented; stat = 22, not 29.
	orchestrator.Upsert(ctx, makeTestRevision("doc1", docCV, `{"v":2,"extra":"data"}`), testCollectionID)
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value(), "upsert replaces in-place — still 1 item")
	assert.Equal(t, int64(22), revStats.cacheMemoryStat.Value(),
		"stat must equal new body size only — no double-counting of old bytes")

	// Third Upsert: smaller body — MemoryBytes = 7.  Stat must shrink, not accumulate.
	orchestrator.Upsert(ctx, makeTestRevision("doc1", docCV, `{"v":3}`), testCollectionID)
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())
	assert.Equal(t, int64(7), revStats.cacheMemoryStat.Value(), "stat must shrink when the replacement body is smaller")
}

// TestCombinedNumberAndMemoryEviction verifies that when both MaxItemCount and MaxBytes
// are configured, number-based eviction (inside addDelta) and memory-based eviction
// (triggerMemoryEviction) interact correctly, leaving consistent item-count and memory stats.
func TestCombinedNumberAndMemoryEviction(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta cache is EE only")
	}

	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	deltaStats := newTestDeltaStats()

	// rev body = 15 bytes; each delta body = 10 bytes.
	// maxBytes=20: rev(15) fits alone; delta(10) fits alone; rev+delta(25) triggers memory
	// eviction; two deltas(20) exactly at the limit — NOT > 20 — so no memory eviction fires
	// after the second delta is added via number-based eviction.
	const maxBytes = int64(20)

	// MaxItemCount=1 caps both rev and delta caches at 1 item, ensuring number-based
	// eviction fires when the second delta is added.
	opts := &RevisionCacheOptions{MaxItemCount: 1, MaxBytes: maxBytes}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, deltaStats, true,
	)

	makeDelta := func() RevisionDelta {
		d := RevisionDelta{DeltaBytes: []byte("0123456789")} // 10 bytes
		d.CalculateDeltaBytes()
		return d
	}

	// Put a revision (15 bytes) — fits within maxBytes, no eviction.
	orchestrator.Put(ctx, makeTestRevision("doc1", Version{Value: 1, SourceID: "src"}, `{"key":"value"}`), testCollectionID)
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())
	assert.Equal(t, int64(15), revStats.cacheMemoryStat.Value())

	// UpdateDelta A: total = 15+10 = 25 > 20 → memory eviction fires and evicts the revision immediately
	// After eviction: only delta doc1 in memory (15 bytes).
	orchestrator.UpdateDelta(ctx, "doc1", "from1", "to1", testCollectionID, makeDelta())
	assert.Equal(t, int64(0), revStats.cacheNumItemsStat.Value())
	assert.Equal(t, int64(1), deltaStats.DeltaCacheNumItems.Value())
	assert.Equal(t, int64(10), revStats.cacheMemoryStat.Value(), "only doc1 bytes should remain")

	// UpdateDelta B: number-based eviction (MaxItemCount=1) removes delta A and decrements
	// its 10 bytes, then delta B is added and its 10 bytes incremented.  Net = 10 bytes.
	// Memory (10) is NOT > maxBytes (20) so memory-based eviction does not fire.
	orchestrator.UpdateDelta(ctx, "doc1", "from2", "to2", testCollectionID, makeDelta())
	assert.Equal(t, int64(0), revStats.cacheNumItemsStat.Value(), "revision cache still empty")
	assert.Equal(t, int64(1), deltaStats.DeltaCacheNumItems.Value(), "only delta B should remain")
	assert.Equal(t, int64(10), revStats.cacheMemoryStat.Value(),
		"number eviction of delta A must have decremented bytes; stat should equal delta B only")
}

// TestMemoryStatTracksUsageWithUnlimitedCapacity verifies that with MaxBytes=0 (no memory
// limit), the shared stat still accurately tracks the sum of bytes in the revision cache
// as items are added and removed, and never goes negative.
func TestMemoryStatTracksUsageWithUnlimitedCapacity(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()

	// MaxBytes=0: IsOverCapacity() always false — eviction never fires, only accounting.
	opts := &RevisionCacheOptions{MaxItemCount: 100, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	cv1 := Version{Value: 1, SourceID: "test"}
	cv2 := Version{Value: 2, SourceID: "test"}

	// Put two revisions with different bodies.
	// rev1: len(`{"v":1}`) = 7 bytes; rev2: len(`{"v":2,"x":"y"}`) = 15 bytes.
	orchestrator.Put(ctx, makeTestRevision("doc1", cv1, `{"v":1}`), testCollectionID)
	assert.Equal(t, int64(7), revStats.cacheMemoryStat.Value())
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())

	orchestrator.Put(ctx, makeTestRevision("doc2", cv2, `{"v":2,"x":"y"}`), testCollectionID)
	assert.Equal(t, int64(22), revStats.cacheMemoryStat.Value(), "stat should be the sum of both revision bodies")
	assert.Equal(t, int64(2), revStats.cacheNumItemsStat.Value())

	// Remove rev1 — stat must decrease by exactly rev1's bytes.
	orchestrator.Remove(ctx, "doc1", cv1.String(), testCollectionID)
	assert.Equal(t, int64(15), revStats.cacheMemoryStat.Value(), "stat should reflect only rev2 after removing rev1")
	assert.Equal(t, int64(1), revStats.cacheNumItemsStat.Value())
	assert.True(t, revStats.cacheMemoryStat.Value() >= 0, "memory stat must never go negative")

	// Remove rev2 — stat must reach exactly zero.
	orchestrator.Remove(ctx, "doc2", cv2.String(), testCollectionID)
	assert.Equal(t, int64(0), revStats.cacheMemoryStat.Value(), "all items removed — stat must be 0")
	assert.Equal(t, int64(0), revStats.cacheNumItemsStat.Value())

	// Remove rev2 - item doesn't exist, stats stay same
	orchestrator.Remove(ctx, "doc2", cv2.String(), testCollectionID)
	assert.Equal(t, int64(0), revStats.cacheMemoryStat.Value(), "all items removed — stat must be 0")
	assert.Equal(t, int64(0), revStats.cacheNumItemsStat.Value())
}

// TestConcurrentPutAndRemoveRace exercises the Put/Remove race with many goroutines to ensure
// memory stats stay consistent under the race detector. After all goroutines finish, every item
// must either be in the cache with its bytes counted, or absent with its bytes not counted.
func TestConcurrentPutAndRemoveRace(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	opts := &RevisionCacheOptions{MaxItemCount: 1000, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	const numDocs = 50
	const iterations = 20

	for iter := range iterations {
		var wg sync.WaitGroup
		for i := range numDocs {
			docID := fmt.Sprintf("doc-%d-%d", iter, i)
			cv := Version{Value: uint64(iter*numDocs + i + 1), SourceID: "test"}
			body := fmt.Sprintf(`{"iter":%d,"i":%d}`, iter, i)

			wg.Add(2)
			go func() {
				defer wg.Done()
				orchestrator.Put(ctx, makeTestRevision(docID, cv, body), testCollectionID)
			}()
			go func() {
				defer wg.Done()
				orchestrator.Remove(ctx, docID, cv.String(), testCollectionID)
			}()
		}
		wg.Wait()
	}

	// Verify stat consistency: the memory stat must equal the sum of bytes of items
	// actually resident in the cache.
	var expectedBytes, numFound int64
	for iter := range iterations {
		for i := range numDocs {
			docID := fmt.Sprintf("doc-%d-%d", iter, i)
			cv := Version{Value: uint64(iter*numDocs + i + 1), SourceID: "test"}
			docRev, found := orchestrator.Peek(ctx, docID, cv.String(), testCollectionID)
			if found {
				docRev.CalculateBytes()
				expectedBytes += docRev.MemoryBytes
				numFound++
			}
		}
	}
	assert.Equal(t, expectedBytes, revStats.cacheMemoryStat.Value(),
		"memory stat must equal the sum of bytes of items actually in the cache")
	assert.Equal(t, numFound, revStats.cacheNumItemsStat.Value())
}

// TestConcurrentGetAndRemoveRace exercises the Get (cache-miss load)/Remove race. A backing store
// with a delay would make the race more likely, but even without delays the race detector can
// catch unsynchronized access. After all goroutines finish, memory stats must be consistent.
func TestConcurrentGetAndRemoveRace(t *testing.T) {
	ctx := base.TestCtx(t)

	var getDocCounter, getRevCounter base.SgwIntStat
	backingStore := &testBackingStore{nil, &getDocCounter, &getRevCounter}
	revStats := newTestRevCacheStats()
	opts := &RevisionCacheOptions{MaxItemCount: 1000, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(backingStore, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	const numDocs = 50

	var wg sync.WaitGroup
	for i := range numDocs {
		docID := fmt.Sprintf("doc-%d", i)
		// testBackingStore always returns docs at rev "1-abc" with HLV SourceID=test Version=123
		cv := Version{Value: 123, SourceID: "test"}

		wg.Add(2)
		go func() {
			defer wg.Done()
			// Get triggers a cache-miss load from the backing store
			_, _, _ = orchestrator.Get(ctx, docID, cv.String(), testCollectionID, false)
		}()
		go func() {
			defer wg.Done()
			orchestrator.Remove(ctx, docID, cv.String(), testCollectionID)
		}()
	}
	wg.Wait()

	// Memory stat must be non-negative and consistent with what's in the cache.
	// Verify stat consistency: the memory stat must equal the sum of bytes of items
	// actually resident in the cache.
	var expectedBytes, numFound int64
	for i := range numDocs {
		docID := fmt.Sprintf("doc-%d", i)
		cv := Version{Value: 123, SourceID: "test"}
		docRev, found := orchestrator.Peek(ctx, docID, cv.String(), testCollectionID)
		if found {
			docRev.CalculateBytes()
			expectedBytes += docRev.MemoryBytes
			numFound++
		}
	}
	assert.Equal(t, expectedBytes, revStats.cacheMemoryStat.Value())
	assert.Equal(t, numFound, revStats.cacheNumItemsStat.Value())
}

// TestRemoveDuringNumberBasedEviction verifies that when a Put triggers number-based eviction
// of an unsized item, and a concurrent Remove targets the same item, the memory stat remains
// consistent — no double-decrement, no negative stat.
func TestRemoveDuringNumberBasedEviction(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	// MaxItemCount=1: every Put evicts the previous tail.
	opts := &RevisionCacheOptions{MaxItemCount: 1, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	const numDocs = 100

	var wg sync.WaitGroup
	for i := range numDocs {
		cv := Version{Value: uint64(i + 1), SourceID: "test"}
		docID := fmt.Sprintf("doc-%d", i)
		body := fmt.Sprintf(`{"i":%d}`, i)

		wg.Add(2)
		go func() {
			defer wg.Done()
			orchestrator.Put(ctx, makeTestRevision(docID, cv, body), testCollectionID)
		}()
		go func() {
			defer wg.Done()
			orchestrator.Remove(ctx, docID, cv.String(), testCollectionID)
		}()
	}
	wg.Wait()

	// With MaxItemCount=1, 1 item should remain, assert on item that exists and
	// ensure memory stat equals that item's bytes
	var expectedBytes, numFound int64
	for i := range numDocs {
		cv := Version{Value: uint64(i + 1), SourceID: "test"}
		docID := fmt.Sprintf("doc-%d", i)
		docRev, found := orchestrator.Peek(ctx, docID, cv.String(), testCollectionID)
		if found {
			docRev.CalculateBytes()
			expectedBytes += docRev.MemoryBytes
			numFound++
		}
	}
	assert.Equal(t, expectedBytes, revStats.cacheMemoryStat.Value())
	assert.Equal(t, numFound, revStats.cacheNumItemsStat.Value())
}

// TestConcurrentUpsertAndRemoveRace exercises the Upsert/Remove race. Upsert replaces an
// existing entry (decrement old bytes, increment new bytes); a concurrent Remove must not
// cause double-decrement.
func TestConcurrentUpsertAndRemoveRace(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	opts := &RevisionCacheOptions{MaxItemCount: 1000, MaxBytes: 0}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	const numDocs = 50
	const upsertRounds = 10

	// Seed the cache with initial versions.
	for i := range numDocs {
		cv := Version{Value: 1, SourceID: "test"}
		docID := fmt.Sprintf("doc-%d", i)
		orchestrator.Put(ctx, makeTestRevision(docID, cv, `{"v":1}`), testCollectionID)
	}

	// Concurrently Upsert new versions and Remove entries.
	var wg sync.WaitGroup
	for round := range upsertRounds {
		for i := range numDocs {
			cv := Version{Value: 1, SourceID: "test"}
			docID := fmt.Sprintf("doc-%d", i)
			body := fmt.Sprintf(`{"v":%d,"round":%d}`, round+2, round)

			wg.Add(2)
			go func() {
				defer wg.Done()
				orchestrator.Upsert(ctx, makeTestRevision(docID, cv, body), testCollectionID)
			}()
			go func() {
				defer wg.Done()
				orchestrator.Remove(ctx, docID, cv.String(), testCollectionID)
			}()
		}
	}
	wg.Wait()

	var expectedBytes, numFound int64
	for i := range numDocs {
		docID := fmt.Sprintf("doc-%d", i)
		cv := Version{Value: 1, SourceID: "test"}
		docRev, found := orchestrator.Peek(ctx, docID, cv.String(), testCollectionID)
		if found {
			docRev.CalculateBytes()
			expectedBytes += docRev.MemoryBytes
			numFound++
		}
	}
	assert.Equal(t, expectedBytes, revStats.cacheMemoryStat.Value())
	assert.Equal(t, numFound, revStats.cacheNumItemsStat.Value())
}

// TestMemoryEvictionDuringConcurrentPuts verifies that when memory-based eviction fires during
// concurrent Puts, the memory stat remains consistent and eventually falls at or below capacity.
func TestMemoryEvictionDuringConcurrentPuts(t *testing.T) {
	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	// Each body is 10 bytes; maxBytes=50 means at most ~5 items before eviction.
	const maxBytes = int64(50)
	opts := &RevisionCacheOptions{MaxItemCount: 1000, MaxBytes: maxBytes}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(&noopBackingStore{}, testCollectionID),
		revStats, &base.DeltaSyncStats{}, false,
	)

	const numDocs = 30

	var wg sync.WaitGroup
	for i := range numDocs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cv := Version{Value: uint64(i + 1), SourceID: "test"}
			docID := fmt.Sprintf("doc-%d", i)
			orchestrator.Put(ctx, makeTestRevision(docID, cv, `{"k":"val"}`), testCollectionID)
		}()
	}
	wg.Wait()

	// After all concurrent Puts and evictions settle, the memory stat should be at or below capacity.
	assert.Equal(t, int64(44), revStats.cacheMemoryStat.Value(), "we should have 4 items worth of memory")
	assert.Equal(t, int64(4), revStats.cacheNumItemsStat.Value(), "we should have 4 items in cache")
}

// TestMemoryStatLongTermConsistency runs a high-concurrency mixed workload (Put, Upsert,
// Get, UpdateDelta, GetWithDelta, Remove) against an orchestrator with both rev and delta
// caches enabled and a memory limit that forces frequent memory-based eviction. After the
// workload settles, it audits the full internal state of both caches against the published
// memory stat and the controller's internal counter — catching any divergence between the
// stat and reality across all operation types and eviction paths.
//
// Mirrors the production invariant that any given CV maps to exactly one body: Put/Upsert
// use a writer-side CV, Get/GetWithDelta use the backing-store's CV, and the body bytes for
// each (docID, CV) pair are deterministic. Without this, the "Put-on-existing-Sized" path
// is reached (different bodies for the same cache key) which is impossible in production.
func TestMemoryStatLongTermConsistency(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("delta cache is EE only")
	}

	ctx := base.TestCtx(t)

	revStats := newTestRevCacheStats()
	deltaStats := newTestDeltaStats()

	var docCounter, revCounter base.SgwIntStat
	bs := &testBackingStore{getDocumentCounter: &docCounter, getRevisionCounter: &revCounter}

	// MaxBytes deliberately small so memory-based eviction fires constantly throughout the run.
	// MaxItemCount large so number-based eviction stays out of the way (covered by other tests).
	const maxBytes = int64(2000)
	opts := &RevisionCacheOptions{MaxItemCount: 10000, MaxBytes: maxBytes}
	orchestrator := NewRevisionCacheOrchestrator(
		opts, CreateTestSingleBackingStoreMap(bs, testCollectionID),
		revStats, deltaStats, true,
	)

	// loadedCV is what the backing store returns for any doc — Get/GetWithDelta cache under this key.
	// writeCV is what Put/Upsert cache under — distinct from loadedCV so the two paths never collide
	// on the same cache key (production invariant: each write produces a fresh CV).
	loadedCV := Version{Value: 123, SourceID: "test"}
	writeCV := Version{Value: 999, SourceID: "writer"}

	const numDocs = 100
	const opsPerWorker = 1000
	const numWorkers = 250

	docID := func(i int) string { return fmt.Sprintf("doc-%d", i) }
	// bodyForDoc is deterministic per docID so any Put or Upsert on (docID, writeCV) produces the
	// same byte count — upholding the "one CV ⇒ one body" invariant.
	bodyForDoc := func(i int) string {
		return fmt.Sprintf(`{"docID":%d,"payload":"abcdefghijklmnop"}`, i)
	}

	put := func(i int) {
		orchestrator.Put(ctx, makeTestRevision(docID(i), writeCV, bodyForDoc(i)), testCollectionID)
	}
	upsert := func(i int) {
		orchestrator.Upsert(ctx, makeTestRevision(docID(i), writeCV, bodyForDoc(i)), testCollectionID)
	}
	get := func(i int) {
		_, _, _ = orchestrator.Get(ctx, docID(i), loadedCV.String(), testCollectionID, false)
	}
	updateDelta := func(i int) {
		toCV := Version{Value: uint64(1000 + i), SourceID: "test"}.String()
		d := RevisionDelta{DeltaBytes: []byte(fmt.Sprintf("delta-payload-for-doc-%d", i))}
		d.CalculateDeltaBytes()
		orchestrator.UpdateDelta(ctx, docID(i), loadedCV.String(), toCV, testCollectionID, d)
	}
	getWithDelta := func(i int) {
		toCV := Version{Value: uint64(1000 + i), SourceID: "test"}.String()
		_, _ = orchestrator.GetWithDelta(ctx, docID(i), loadedCV.String(), toCV, testCollectionID)
	}
	removeWriter := func(i int) {
		orchestrator.Remove(ctx, docID(i), writeCV.String(), testCollectionID)
	}
	removeLoaded := func(i int) {
		orchestrator.Remove(ctx, docID(i), loadedCV.String(), testCollectionID)
	}

	ops := []func(int){put, upsert, get, updateDelta, getWithDelta, removeWriter, removeLoaded}

	var wg sync.WaitGroup
	for w := range numWorkers {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed)))
			for range opsPerWorker {
				op := ops[r.Intn(len(ops))]
				op(r.Intn(numDocs))
			}
		}(w)
	}
	wg.Wait()

	// Audit: walk both caches under their own locks and sum the bytes that should be
	// reflected in the memory stat. Items in memStateLoading must not be counted (their
	// bytes were never added). Items in memStateRemoved must not be present in the map at all.
	var revBytes, deltaBytes, revCount, deltaCount int64

	orchestrator.revisionCache.lock.Lock()
	for _, elem := range orchestrator.revisionCache.cache {
		val := elem.Value.(*revCacheValue)
		switch val.memState.Load() {
		case memStateSized:
			revBytes += val.itemBytes.Load()
		case memStateRemoved:
			t.Errorf("rev cache map still contains a removed entry for key %+v", val.itemKey)
		}
		revCount++
	}
	revListLen := int64(orchestrator.revisionCache.lruList.Len())
	orchestrator.revisionCache.lock.Unlock()

	orchestrator.deltaCache.lock.Lock()
	for _, elem := range orchestrator.deltaCache.cache {
		val := elem.Value.(*deltaCacheValue)
		deltaBytes += val.delta.totalDeltaBytes
		deltaCount++
	}
	deltaListLen := int64(orchestrator.deltaCache.lruList.Len())
	orchestrator.deltaCache.lock.Unlock()

	totalBytes := revBytes + deltaBytes

	// Stat published to the rest of the system must match the bytes actually held.
	assert.Equal(t, totalBytes, revStats.cacheMemoryStat.Value(),
		"published memory stat must equal sum of bytes in rev cache (%d) + delta cache (%d)", revBytes, deltaBytes)

	// Internal controller counter must agree with the published stat — they are mutated
	// together in incrementBytesCount/decrementBytesCount, so any drift between them
	// indicates a missed write site.
	assert.Equal(t, revStats.cacheMemoryStat.Value(), orchestrator.memoryController.bytesInUseForShard.Load(),
		"internal controller counter must equal published memory stat")

	// Item-count stats must match the actual number of entries in each cache.
	assert.Equal(t, revCount, revStats.cacheNumItemsStat.Value(),
		"rev cache num-items stat must equal number of map entries")
	assert.Equal(t, deltaCount, deltaStats.DeltaCacheNumItems.Value(),
		"delta cache num-items stat must equal number of map entries")

	// Map size and list length must agree (no orphaned list entries or leaked map keys).
	assert.Equal(t, revCount, revListLen, "rev cache map size must equal LRU list length")
	assert.Equal(t, deltaCount, deltaListLen, "delta cache map size must equal LRU list length")

	// Memory must remain at or below the configured limit (eviction may briefly go over, but
	// the controller drives back to <= maxBytes once triggerMemoryEviction completes).
	assert.LessOrEqual(t, revStats.cacheMemoryStat.Value(), maxBytes,
		"after workload settles, memory stat must be at or below maxBytes — eviction failed to keep up")
}
