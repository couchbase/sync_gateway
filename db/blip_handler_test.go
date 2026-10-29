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
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	db, ctx := setupTestDB(t)
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

// TestHandleRevClosedSenderHoldsPullCheckpoint checks that a pulled rev that fails because the connection closed is not
// marked processed, so the checkpoint holds before it rather than skipping it.
func TestHandleRevClosedSenderHoldsPullCheckpoint(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	ctx, cancelFunc := context.WithCancelCause(ctx)
	defer cancelFunc(errors.New("test teardown"))

	ctx, blipContext, err := NewSGBlipContext(ctx, "", nil, nil)
	require.NoError(t, err)
	bsc, err := NewBlipSyncContext(ctx, blipContext, db, nil, cancelFunc)
	require.NoError(t, err)

	bh := newBlipHandler(ctx, bsc, db, 1)
	marked := false
	bh.collectionCtx = &blipSyncCollectionContext{sgr2PullProcessedSeqCallback: func(*SequenceID, IDAndRev) { marked = true }}

	// fill the rev throttle and close the connection, so processRev returns ErrClosedBLIPSender while it waits
	for range cap(bh.inFlightRevsThrottle) {
		bh.inFlightRevsThrottle <- struct{}{}
	}
	bsc.Close()

	rq := blip.NewRequest()
	rq.SetProfile(MessageRev)
	rq.Properties[RevMessageID] = "doc"
	rq.Properties[RevMessageRev] = "1-abc"
	rq.Properties[RevMessageSequence] = "1"
	rq.SetBody([]byte(`{}`))
	require.ErrorIs(t, bh.handleRev(rq), ErrClosedBLIPSender)
	require.False(t, marked)
}

// TestSendRevClosedConnectionHoldsPushCheckpoint checks that a pushed rev is not marked processed when the connection
// closes before the peer replies, so the checkpoint holds before it rather than skipping it.
func TestSendRevClosedConnectionHoldsPushCheckpoint(t *testing.T) {
	t.Skip("CBG-6047: go-blip completes a reply cut short by a closed connection as a successful empty reply")
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	// the passive peer holds each rev without replying until the test closes the connection
	revReceived := make(chan struct{}, 1)
	releaseRev := make(chan struct{})
	defer close(releaseRev)
	_, passiveBlipContext, err := NewSGBlipContext(ctx, t.Name()+"-passive", nil, nil)
	require.NoError(t, err)
	passiveBlipContext.HandlerForProfile[MessageRev] = func(*blip.Message) {
		revReceived <- struct{}{}
		<-releaseRev
	}
	srv := httptest.NewServer(passiveBlipContext.WebSocketServer())
	defer srv.Close()

	ctx, cancelFunc := context.WithCancelCause(ctx)
	defer cancelFunc(errors.New("test teardown"))
	ctx, activeBlipContext, err := NewSGBlipContext(ctx, t.Name()+"-active", nil, nil)
	require.NoError(t, err)
	sender, err := activeBlipContext.Dial("ws" + strings.TrimPrefix(srv.URL, "http"))
	require.NoError(t, err)
	defer sender.Close()

	bsc, err := NewBlipSyncContext(ctx, activeBlipContext, db, nil, cancelFunc)
	require.NoError(t, err)
	defer bsc.Close()
	var marked atomic.Bool
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)
	collectionCtx := newBlipSyncCollectionContext(ctx, collection)
	collectionCtx.sgr2PushProcessedSeqCallback = func(SequenceID) { marked.Store(true) }
	bsc.collections.setNonCollectionAware(collectionCtx)

	ctx = base.KeyspaceLogCtx(ctx, db.Bucket.GetName(), collection.ScopeName, collection.Name)
	require.NoError(t, bsc.sendRevisionWithProperties(ctx, sender, "doc", "1-abc", nil, []byte(`{}`), nil, blip.Properties{}, SequenceID{Seq: 1}, nil))
	select {
	case <-revReceived:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "passive peer did not receive the rev")
	}
	sender.Close()

	// the response handler counts the rev as sent or failed before it decides whether to mark it processed
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Positive(c, bsc.replicationStats.SendRevCount.Value()+bsc.replicationStats.SendRevErrorTotal.Value())
	}, 10*time.Second, 10*time.Millisecond)
	require.Never(t, marked.Load, 100*time.Millisecond, 10*time.Millisecond)
}
