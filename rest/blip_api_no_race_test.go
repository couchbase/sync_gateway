//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
//go:build !race
// +build !race

package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestBlipPusherUpdateDatabase pushes revisions while the database config is replaced underneath the replication.
// The reload must close the BLIP connection, and a new connection must be able to push to the reloaded database.
func TestBlipPusherUpdateDatabase(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyHTTPResp, base.KeySync)

	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	bt := NewBlipTesterFromSpecWithRT(rt, nil)
	defer bt.Close()

	var connectionClosed atomic.Bool
	bt.blipContext.OnExitCallback = func() {
		connectionClosed.Store(true)
	}

	// Push revisions one at a time until the server closes the connection, so a rev is in flight during the reload.
	ctx, cancel := context.WithCancelCause(rt.Context())
	var wg sync.WaitGroup
	defer func() {
		cancel(errors.New("test finished"))
		wg.Wait()
	}()
	wg.Go(func() {
		for i := 0; ctx.Err() == nil && !connectionClosed.Load(); i++ {
			revRequest := bt.newRevMessage(fmt.Sprintf("doc%d", i), "1-abc", fmt.Appendf(nil, `{"i":%d}`, i), blip.Properties{})
			if !bt.sender.Send(revRequest) {
				return
			}
			revRequest.Response()
		}
	})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		changes := rt.GetChanges("/{{.keyspace}}/_changes", "")
		assert.GreaterOrEqual(c, len(changes.Results), 5)
	}, time.Second*5, time.Millisecond*100)

	oldDatabase := rt.GetDatabase()
	dbConfig := rt.NewDbConfig()
	dbConfig.RevsLimit = new(uint32(1000))
	RequireStatus(t, rt.ReplaceDbConfig("db", dbConfig), http.StatusCreated)
	require.NotSame(t, oldDatabase, rt.GetDatabase())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, connectionClosed.Load())
	}, time.Second*10, time.Millisecond*100)

	// CBL reconnects after the server closes the connection.
	newBT := NewBlipTesterFromSpecWithRT(rt, nil)
	defer newBT.Close()
	const docID = "docAfterReload"
	newBT.SendRev(docID, "1-abc", []byte(`{"reloaded":true}`), blip.Properties{})
	rt.GetDoc(docID)
}

// TestBlipRevInFlightDuringDatabaseReload blocks a rev handler partway through its write while the database reloads,
// then releases it after the old database has closed its bucket. The handler must answer with an error.
func TestBlipRevInFlightDuringDatabaseReload(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	const blockedDocID = "blockedDoc"
	writeBlocked := make(chan struct{})
	signalWriteBlocked := sync.OnceFunc(func() { close(writeBlocked) })
	releaseWrite := make(chan struct{})
	unblockWrite := sync.OnceFunc(func() { close(releaseWrite) })
	defer unblockWrite()

	// Connect a fresh bucket for each database load so that closing the old database really closes its bucket.
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		ConnectToBucketFn: func(ctx context.Context, spec base.BucketSpec, failFast bool) (base.Bucket, error) {
			bucket, err := db.ConnectToBucket(ctx, spec, failFast)
			if err != nil {
				return nil, err
			}
			return base.NewLeakyBucket(bucket, base.LeakyBucketConfig{
				WriteUpdateWithXattrsCallback: func(key string) {
					if key != blockedDocID {
						return
					}
					signalWriteBlocked()
					<-releaseWrite
				},
			}), nil
		},
	})
	defer rt.Close()

	bt := NewBlipTesterFromSpecWithRT(rt, nil)
	defer bt.Close()

	revRequest := bt.newRevMessage(blockedDocID, "1-abc", []byte(`{"blocked":true}`), blip.Properties{})
	bt.Send(revRequest)
	select {
	case <-writeBlocked:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "rev handler did not reach the document write")
	}

	oldDatabase := rt.GetDatabase()
	dbConfig := rt.NewDbConfig()
	dbConfig.RevsLimit = new(uint32(1000))
	RequireStatus(t, rt.ReplaceDbConfig("db", dbConfig), http.StatusCreated)
	require.Equal(t, db.DBStopping, atomic.LoadUint32(&oldDatabase.State))

	// The connection closed with the old database, so the client never sees the response. Check the server side instead.
	revErrors := oldDatabase.DbStats.CBLReplicationPush().DocPushErrorCount
	base.AssertLogContains(t, "Type:rev Id:<ud>"+blockedDocID+"</ud>   --> 500 Internal error:", func() {
		unblockWrite()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, int64(1), revErrors.Value())
		}, 10*time.Second, 50*time.Millisecond)
	})
	RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+blockedDocID, ""), http.StatusNotFound)
}
