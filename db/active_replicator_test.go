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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestSetLocalStatusKVOps checks the KV operations of setLocalStatus, which the ISGR status reporter makes on every tick
// even when a replication has nothing to do.
func TestSetLocalStatusKVOps(t *testing.T) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close(ctx)
	statusKey := base.NewMetadataKeys(t.Name()).ReplicationStatusKey("repl1")

	for _, name := range []string{"new status doc", "existing status doc"} {
		t.Run(name, func(t *testing.T) {
			stat := &base.SgwIntStat{}
			err := setLocalStatus(ctx, base.NewKVOpStatDataStore(tBucket.GetSingleDataStore(), stat), statusKey, &ReplicationStatus{ID: "repl1", Status: ReplicationStateRunning}, 0)
			require.NoError(t, err)
			require.Equal(t, int64(3), stat.Value(), "a read, then an update that reads and writes")
		})
	}

	// The read misses the primary and hits the fallback. The update checks the primary, reads the fallback, and
	// inserts into the primary.
	t.Run("status doc in fallback", func(t *testing.T) {
		primary := tBucket.GetSingleDataStore()
		fallback := tBucket.DefaultDataStore(ctx)
		if primary.GetName() == fallback.GetName() {
			t.Skip("needs a named collection to use as the primary")
		}
		fallbackKey := statusKey + "_fallback"
		require.NoError(t, setLocalStatus(ctx, fallback, fallbackKey, &ReplicationStatus{ID: "repl1", Status: ReplicationStateStopped}, 0))

		stat := &base.SgwIntStat{}
		metadataStore := base.NewKVOpStatDataStore(base.NewMetadataStore(primary, fallback), stat)
		require.NoError(t, setLocalStatus(ctx, metadataStore, fallbackKey, &ReplicationStatus{ID: "repl1", Status: ReplicationStateRunning}, 0))
		require.Equal(t, int64(5), stat.Value())
	})
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

// TestConnectAndRunConnectionClosedDuringStart asserts that a connection closed by the remote before the replicator
// finishes starting is cleaned up and does not leave the replicator running.
func TestConnectAndRunConnectionClosedDuringStart(t *testing.T) {
	db, ctx := SetupTestDB(t)
	defer db.Close(ctx)
	ctx, cancelFunc := context.WithCancelCause(ctx)
	defer cancelFunc(errors.New("test teardown"))

	arc := &activeReplicatorCommon{ctx: ctx, state: ReplicationStateStopped}
	arc.replicatorConnectFn = func() error {
		blipCtx, blipContext, err := NewSGBlipContext(ctx, "", nil, nil)
		require.NoError(t, err)
		arc.blipSyncContext, err = NewBlipSyncContext(blipCtx, blipContext, db, nil, cancelFunc)
		require.NoError(t, err)
		require.False(t, arc.blipSyncContext.markRemoteDisconnected())
		return nil
	}

	require.ErrorIs(t, arc._connectAndRun(), errConnClosedDuringStart)
	require.Nil(t, arc.blipSyncContext)
	require.Equal(t, ReplicationStateStopped, arc.getState())
}

// TestConnectAndRunStopsCheckpointersAfterFailedStart asserts that a start that fails after starting a checkpointer
// stops that checkpointer before it returns.
func TestConnectAndRunStopsCheckpointersAfterFailedStart(t *testing.T) {
	db, ctx := SetupTestDB(t)
	defer db.Close(ctx)
	ctx, cancelFunc := context.WithCancelCause(ctx)
	defer cancelFunc(errors.New("test teardown"))

	replicationStats, err := db.DbStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)
	config := &ActiveReplicatorConfig{
		ActiveDB:            db,
		CheckpointInterval:  time.Minute,
		ReplicationStatsMap: replicationStats,
	}
	arc := &activeReplicatorCommon{ctx: ctx, config: config, state: ReplicationStateStopped, defaultCollection: &activeReplicatorCollection{}}
	errStart := errors.New("start failed")
	arc.replicatorConnectFn = func() error {
		arc.checkpointerCtx, arc.checkpointerCtxCancel = context.WithCancelCause(ctx)
		arc.defaultCollection.Checkpointer = NewCheckpointer(arc.checkpointerCtx, nil, nil, "", "", nil, config, nil)
		arc.defaultCollection.Checkpointer.Start(&arc.runningCheckpointers)
		require.Equal(t, int32(1), arc.runningCheckpointers.Load())
		return errStart
	}

	require.ErrorIs(t, arc._connectAndRun(), errStart)
	require.Equal(t, int32(0), arc.runningCheckpointers.Load())
	require.Equal(t, ReplicationStateStopped, arc.getState())
}
