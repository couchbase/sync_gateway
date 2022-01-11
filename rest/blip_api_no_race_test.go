//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// This file contains tests which depend on the race detector being disabled.  Contains changes tests
// that have unpredictable timing when running w/ race detector due to longpoll/continuous changes request
// processing.
// +build !race

package rest

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// TestBlipPusherUpdateDatabase starts a push replication and updates the database underneath the replication.
// Expect to see the connection closed with an error, instead of continuously panicking.
func TestBlipPusherUpdateDatabase(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeyHTTPResp, base.KeySync)()

	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{},
		guestEnabled:   true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
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
			_, err := client.PushRev(fmt.Sprintf("doc%d", i), "", []byte(fmt.Sprintf(`{"i":%d}`, i)))
			if err != nil {
				lastPushRevErr.Store(err)
			}
		}
		_ = rt.WaitForPendingChanges()
		wg.Done()
	}()

	// and wait for a few to be done before we proceed with updating database config underneath replication
	_, err = rt.WaitForChanges(5, "/db/_changes", "", true)
	require.NoError(t, err)

	// just change the sync function to cause the database to reload
	resp, err := rt.UpsertDbConfig("db", DbConfig{Sync: base.StringPtr(`function(doc){console.log("update");}`)})
	require.NoError(t, err)
	assertStatus(t, resp, http.StatusCreated)

	// Did we tell the client to close the connection (via HTTP/503)?
	// The BlipTesterClient doesn't implement reconnect - but CBL resets the replication connection.
	lastErr, ok := lastPushRevErr.Load().(error)
	require.True(t, ok)
	require.Error(t, lastErr)
	lastErrMsg := lastErr.Error()
	assert.Contains(t, lastErrMsg, "HTTP 503")
	assert.Contains(t, lastErrMsg, "Sync Gateway database went away - asking client to reconnect")
}
