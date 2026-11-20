/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

import (
	"testing"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestDrainNotifications checks that each drain returns only what was merged since the last one, and that an empty
// drain returns nil.
func TestDrainNotifications(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	cacheOptions := db.DefaultCacheOptions()
	cache := db.NewChangeCacheForTest(t)
	require.NoError(t, cache.Init(ctx, database.DatabaseContext, database.DatabaseContext.ChannelCacheForTest(t),
		&cacheOptions, database.DatabaseContext.MetadataKeys))
	require.NoError(t, cache.Start(0))
	defer cache.Stop(ctx)

	first, second := channels.NewID("first", 0), channels.NewID("second", 0)
	cache.MergeNotificationsForTest(t, first)
	assert.Equal(t, channels.SetOfNoValidate(first), cache.DrainNotificationsForTest(t))
	cache.MergeNotificationsForTest(t, second)
	assert.Equal(t, channels.SetOfNoValidate(second), cache.DrainNotificationsForTest(t), "only what was merged since the last drain")
	assert.Nil(t, cache.DrainNotificationsForTest(t), "an empty drain returns nil")
}

// fatalRecorder records a Fatalf instead of stopping the test, so a test can check that a helper refuses a call.
type fatalRecorder struct {
	testing.TB
	fatal bool
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatalf(string, ...any) { r.fatal = true }

// TestDrainNotificationsRefusesDatabaseCache checks that a test can't drain the database's own cache, which the
// listener's broadcast tick drains: the test would take notifications the listener never sees.
func TestDrainNotificationsRefusesDatabaseCache(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	recorder := &fatalRecorder{TB: t}
	database.ChangeCacheForTest(t).DrainNotificationsForTest(recorder)
	require.True(t, recorder.fatal, "draining the database's own cache must fail the test")

	standalone := db.NewChangeCacheForTest(t)
	cacheOptions := db.DefaultCacheOptions()
	require.NoError(t, standalone.Init(ctx, database.DatabaseContext, database.DatabaseContext.ChannelCacheForTest(t),
		&cacheOptions, database.DatabaseContext.MetadataKeys))
	require.NoError(t, standalone.Start(0))
	defer standalone.Stop(ctx)
	recorder = &fatalRecorder{TB: t}
	standalone.DrainNotificationsForTest(recorder)
	require.False(t, recorder.fatal, "a standalone cache may be drained")
}
