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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlipPusherUpdateDatabase starts a push replication and updates the database underneath the replication.
// Expect to see the connection closed with an error, instead of continuously panicking.
// This is the CBL version of TestPushReplicationAPIUpdateDatabase
//
// This test causes the race detector to flag the bucket=nil operation and any in-flight requests being made using that bucket, prior to the replication being reset.
// TODO CBG-1903: Can be fixed by draining in-flight requests before fully closing the database.
func TestBlipPusherUpdateDatabase(t *testing.T) {

	t.Skip("Skipping test - revisit in CBG-1908")

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyHTTPResp, base.KeySync)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rtConfig := RestTesterConfig{
		DatabaseConfig:   &DatabaseConfig{},
		GuestEnabled:     true,
		CustomTestBucket: tb.NoCloseClone(),
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		var lastPushRevErr atomic.Value

		// Wait for the background updates to finish at the end of the test
		shouldCreateDocs := base.NewAtomicBool(true)
		wg := sync.WaitGroup{}
		wg.Add(1)
		defer func() {
			shouldCreateDocs.Set(false)
			wg.Wait()
		}()

		// Start the test client creating and pushing documents in the background
		go func() {
			for i := 0; shouldCreateDocs.IsTrue(); i++ {
				// this will begin to error when the database is reloaded underneath the replication
				btcRunner.AddRev(client.id, fmt.Sprintf("doc%d", i), EmptyDocVersion(), []byte(fmt.Sprintf(`{"i":%d}`, i)))
			}
			rt.WaitForPendingChanges()
			wg.Done()
		}()

		// and wait for a few to be done before we proceed with updating database config underneath replication
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			changes := rt.GetChanges("/{{.keyspace}}/_changes", "")
			assert.GreaterOrEqual(c, 5, changes.Results)
		}, time.Second*5, time.Millisecond*100)

		// just change the sync function to cause the database to reload
		dbConfig := *rt.ServerContext().GetDbConfig("db")
		dbConfig.Sync = base.Ptr(`function(doc){console.log("update");}`)
		resp := rt.ReplaceDbConfig("db", dbConfig)
		RequireStatus(t, resp, http.StatusCreated)

		// Did we tell the client to close the connection (via HTTP/503)?
		// The BlipTesterClient doesn't implement reconnect - but CBL resets the replication connection.
		WaitAndAssertCondition(t, func() bool {
			lastErr, ok := lastPushRevErr.Load().(error)
			if !ok {
				return false
			}
			if lastErr == nil {
				return false
			}
			lastErrMsg := lastErr.Error()
			if !strings.Contains(lastErrMsg, "HTTP 503") {
				return false
			}
			if !strings.Contains(lastErrMsg, "Sync Gateway database went away - asking client to reconnect") {
				return false
			}
			return true
		}, "expected HTTP 503 error")
	})
}
