/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestSetLocalStatusKVOps checks that setLocalStatus, which the ISGR status reporter calls on every tick even when a
// replication has nothing to do, reports the KV operations it makes. Update counts as a read and a write, so the count
// can be higher than the number of calls the Leaky wrapper sees.
func TestSetLocalStatusKVOps(t *testing.T) {
	ctx := base.TestCtx(t)
	var recorder base.KVOpRecorder
	tBucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{KVOpCallback: recorder.DataStoreOp})
	defer tBucket.Close(ctx)
	metadataStore := tBucket.GetMetadataStore()
	statusKey := base.NewMetadataKeys(t.Name()).ReplicationStatusKey("repl1")

	for _, name := range []string{"new status doc", "existing status doc"} {
		t.Run(name, func(t *testing.T) {
			recorder.Arm()
			kvOps, err := setLocalStatus(ctx, metadataStore, statusKey, &ReplicationStatus{ID: "repl1", Status: ReplicationStateRunning}, 0)
			ops := recorder.Disarm()
			require.NoError(t, err)

			require.Equal(t, 3, kvOps)
			require.GreaterOrEqual(t, kvOps, base.SumKVOps(ops), "KV operations made: %v", ops)
		})
	}
}

// TestSetLocalStatusKVOpsFallback checks that setLocalStatus counts the extra KV operations a dual metadata store makes
// for a status doc that is still in the fallback collection.
func TestSetLocalStatusKVOpsFallback(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	primary := tBucket.GetSingleDataStore()
	fallback := tBucket.DefaultDataStore(ctx)
	if primary.GetName() == fallback.GetName() {
		t.Skip("needs a named collection to use as the primary")
	}
	statusKey := base.NewMetadataKeys(t.Name()).ReplicationStatusKey("repl1")
	_, err := setLocalStatus(ctx, fallback, statusKey, &ReplicationStatus{ID: "repl1", Status: ReplicationStateStopped}, 0)
	require.NoError(t, err)

	kvOps, err := setLocalStatus(ctx, base.NewMetadataStore(primary, fallback), statusKey, &ReplicationStatus{ID: "repl1", Status: ReplicationStateRunning}, 0)
	require.NoError(t, err)
	// The read misses the primary and hits the fallback. The update checks the primary, reads the fallback, and
	// inserts into the primary.
	require.Equal(t, 5, kvOps)
}

// TestBlipSyncErrorUserinfo ensures the websocket errors returned by blipSync contain no basic auth component.
func TestBlipSyncErrorUserinfo(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "no creds",
			username: "",
			password: "",
		},
		{
			name:     "username",
			username: "foo",
		},
		{
			name:     "user and password",
			username: "foo",
			password: "bar",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a HTTP server to get past the initial HTTP request inside blipSync.
			// HTTP errors have basic auth components redacted by the Go stdlib anyway.
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			srvURL, err := url.Parse(srv.URL)
			require.NoError(t, err)

			if test.username != "" && test.password != "" {
				srvURL.User = url.UserPassword(test.username, test.password)
			} else if test.username != "" {
				srvURL.User = url.User(test.username)
			}

			srvURL.Path = "/db1"
			t.Logf("srvURL: %v", srvURL.String())

			_, blipContext, err := NewSGBlipContext(base.TestCtx(t), t.Name(), nil, nil)
			require.NoError(t, err)

			_, err = blipSync(*srvURL, blipContext, false)
			require.Error(t, err)
			t.Logf("error: %v", err)
			if targetPassword, hasPassword := srvURL.User.Password(); hasPassword {
				assert.NotContains(t, err.Error(), targetPassword)
			}
		})
	}
}
