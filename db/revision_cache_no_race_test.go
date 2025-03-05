//go:build !race
// +build !race

/*
Copyright 2025-Present Couchbase, Inc.

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
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// stop the goroutine above to allow for accurate assertion below
	testCtxCancel()
	time.Sleep(500 * time.Millisecond) // need to allow time for goroutine teardown

	revCache, ok := db.revisionCache.(*LRURevisionCache)
	require.True(t, ok)
	lruLen := revCache.lruList.Len()
	assert.Equal(t, lruLen, len(revCache.cache))
	assert.Equal(t, lruLen, len(revCache.hlvCache))
	var found bool
	for index := range revCache.cache {
		docID := index.DocID
		for index2 := range revCache.hlvCache {
			if docID == index2.DocID {
				found = true
			}
		}
		require.True(t, found)
		found = false // reset found for next iteration
	}

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

	// stop the goroutine above to allow for accurate assertion below
	testCtxCancel()
	time.Sleep(500 * time.Millisecond) // need to allow time for goroutine teardown

	revCache, ok := db.revisionCache.(*LRURevisionCache)
	require.True(t, ok)
	lruLen := revCache.lruList.Len()
	assert.Equal(t, lruLen, len(revCache.cache))
	assert.Equal(t, lruLen, len(revCache.hlvCache))
	var found bool
	for index := range revCache.cache {
		docID := index.DocID
		for index2 := range revCache.hlvCache {
			if docID == index2.DocID {
				found = true
			}
		}
		require.True(t, found)
		found = false // reset found for next iteration
	}
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

	// stop the goroutine above to allow for accurate assertion below
	testCtxCancel()
	time.Sleep(500 * time.Millisecond) // need to allow time for goroutine teardown

	revCache, ok := db.revisionCache.(*LRURevisionCache)
	require.True(t, ok)
	lruLen := revCache.lruList.Len()
	assert.Equal(t, lruLen, len(revCache.cache))
	assert.Equal(t, lruLen, len(revCache.hlvCache))
	var found bool
	for index := range revCache.cache {
		docID := index.DocID
		for index2 := range revCache.hlvCache {
			if docID == index2.DocID {
				found = true
			}
		}
		require.True(t, found)
		found = false // reset found for next iteration
	}
}
