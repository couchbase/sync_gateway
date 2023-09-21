// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"net/http"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestCollectionBlipHandler(t *testing.T) {
	allDBContext := &DatabaseContext{Name: "db"}
	ctx := base.TestCtx(t)
	dbStats, err := initDatabaseStats(ctx, allDBContext.Name, false, DatabaseContextOptions{})
	require.NoError(t, err)
	allDBContext.DbStats = dbStats
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	allDBCollection, err := newDatabaseCollection(ctx, allDBContext, bucket.GetSingleDataStore(), nil)
	require.NoError(t, err)
	allDBContext.CollectionByID = map[uint32]*DatabaseCollection{
		base.DefaultCollectionID: allDBCollection,
	}
	allDB := &Database{
		DatabaseContext: allDBContext,
	}
	allDBContext.Bucket = bucket
	require.NoError(t, err)
	realCollectionDB0, err := newDatabaseCollection(ctx, allDBContext, bucket.GetSingleDataStore(), nil)
	require.NoError(t, err)
	realCollectionDB1, err := newDatabaseCollection(ctx, allDBContext, bucket.GetSingleDataStore(), nil)
	require.NoError(t, err)
	testCases := []struct {
		name               string
		blipMessage        *blip.Message
		err                *base.HTTPError
		collection         *DatabaseCollectionWithUser
		collectionContexts []*blipSyncCollectionContext
	}{
		{
			name:        "NoCollections",
			blipMessage: &blip.Message{},
			err:         nil,
			collection:  &DatabaseCollectionWithUser{DatabaseCollection: allDBCollection},
		},
		{
			name: "emptyCollectionNoAnInt",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "a",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "emptyCollectionMapping",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "missingCollectionMapping",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "missingCollectionMapping",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "singlePresentCollection",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "0",
				},
			},
			err: nil,
			collection: &DatabaseCollectionWithUser{
				DatabaseCollection: realCollectionDB0,
			},
			collectionContexts: []*blipSyncCollectionContext{{dbCollection: realCollectionDB0}},
		},
		{
			name: "twoPresentCollections",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err: nil,
			collection: &DatabaseCollectionWithUser{
				DatabaseCollection: realCollectionDB1,
			},
			collectionContexts: []*blipSyncCollectionContext{{dbCollection: realCollectionDB0}, {dbCollection: realCollectionDB1}},
		},
		{
			name: "collectionPassedInGetCollectionsButHitErrorInGetCollections",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:                &base.HTTPError{Status: http.StatusBadRequest},
			collection:         nil,
			collectionContexts: []*blipSyncCollectionContext{{dbCollection: realCollectionDB0}, nil},
		},
		{
			name: "outOfRangeCollections",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "2",
				},
			},
			err:                &base.HTTPError{Status: http.StatusBadRequest},
			collection:         nil,
			collectionContexts: []*blipSyncCollectionContext{{dbCollection: realCollectionDB0}, {dbCollection: realCollectionDB1}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			bh := newBlipHandler(ctx,
				&BlipSyncContext{
					loggingCtx:  ctx,
					collections: &blipCollections{},
				},
				allDB,
				0,
			)
			bh.collections.set(testCase.collectionContexts)
			if testCase.collection != nil {
				bh.collections.setNonCollectionAware(newBlipSyncCollectionContext(ctx, testCase.collection.DatabaseCollection))
			}

			passedMiddleware := false
			success := func(bh *blipHandler, bm *blip.Message) error {
				passedMiddleware = true
				return nil
			}
			middleware := collectionBlipHandler(success)
			err := middleware(bh, testCase.blipMessage)
			if testCase.err == nil {
				require.NoError(t, err)
				require.True(t, passedMiddleware)
			} else {
				require.Error(t, err)
				require.Equal(t, testCase.err.Status, err.(*base.HTTPError).Status)
				require.False(t, passedMiddleware)
			}
			if testCase.collection == nil {
				require.Nil(t, bh.collection)
			} else {
				require.NotNil(t, testCase.collection)
				require.NotNil(t, bh.collection)
				require.Equal(t, testCase.collection.dbCtx, bh.collection.dbCtx)
			}
		})
	}
}
