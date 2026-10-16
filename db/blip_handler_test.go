// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestCollectionBlipHandler(t *testing.T) {
	allDBContext := &DatabaseContext{}
	allDBCollection := &DatabaseCollection{dbCtx: allDBContext}
	allDBContext.CollectionByID = map[uint32]*DatabaseCollection{
		base.DefaultCollectionID: allDBCollection,
	}
	allDB := &Database{
		DatabaseContext: allDBContext,
	}
	realCollectionDB0 := &DatabaseCollection{dbCtx: &DatabaseContext{}}
	realCollectionDB1 := &DatabaseCollection{dbCtx: &DatabaseContext{}}
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
			bh := blipHandler{
				db: allDB,
				BlipSyncContext: &BlipSyncContext{
					loggingCtx:  ctx,
					collections: &blipCollections{},
				},
			}
			bh.collections.set(testCase.collectionContexts)
			if testCase.collection != nil {
				bh.collections.setNonCollectionAware(&blipSyncCollectionContext{dbCollection: testCase.collection.DatabaseCollection})
			}

			passedMiddleware := false
			success := func(bh *blipHandler, bm *blip.Message) error {
				passedMiddleware = true
				return nil
			}
			middleware := collectionBlipHandler(success)
			err := middleware(&bh, testCase.blipMessage)
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

func TestIsPermanentRevStatus(t *testing.T) {
	permanent := []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusConflict,
		http.StatusRequestEntityTooLarge,
		http.StatusInternalServerError,
	}
	notPermanent := []int{
		// a failed delta, which the sender resends in full
		http.StatusUnprocessableEntity,
		// transient
		http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		// unknown, so held rather than skipped
		http.StatusMethodNotAllowed,
		http.StatusPreconditionFailed,
		http.StatusNotImplemented,
		0,
	}
	for _, status := range permanent {
		require.True(t, isPermanentRevStatus(status), "status %d", status)
	}
	for _, status := range notPermanent {
		require.False(t, isPermanentRevStatus(status), "status %d", status)
	}
}

// TestHandleRevPanicHoldsPullCheckpoint checks that a panic while handling a pulled rev does not mark it processed, so
// the checkpoint holds before it rather than skipping it.
func TestHandleRevPanicHoldsPullCheckpoint(t *testing.T) {
	db, ctx := SetupTestDB(t)
	defer db.Close(ctx)
	ctx, cancelFunc := context.WithCancelCause(ctx)
	defer cancelFunc(errors.New("test teardown"))

	ctx, blipContext, err := NewSGBlipContext(ctx, "", nil, nil)
	require.NoError(t, err)
	bsc, err := NewBlipSyncContext(ctx, blipContext, db, nil, cancelFunc)
	require.NoError(t, err)
	defer bsc.Close()

	// the handler has no collection, so processRev panics when it writes the rev
	bh := newBlipHandler(ctx, bsc, db, 1)
	marked := false
	bh.collectionCtx = &blipSyncCollectionContext{sgr2PullProcessedSeqCallback: func(*SequenceID, IDAndRev) { marked = true }}

	rq := blip.NewRequest()
	rq.SetProfile(MessageRev)
	rq.Properties[RevMessageID] = "doc"
	rq.Properties[RevMessageRev] = "1-abc"
	rq.Properties[RevMessageSequence] = "1"
	rq.SetBody([]byte(`{}`))
	require.PanicsWithError(t, "runtime error: invalid memory address or nil pointer dereference", func() { _ = bh.handleRev(rq) })
	require.False(t, marked)
}
