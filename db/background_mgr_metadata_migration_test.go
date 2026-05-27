/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldRunMetadataMigration is a truth table covering the guard that decides whether
// metadata migration may be armed: it requires both the UseSystemMetadataCollection opt-in
// and a cluster compatibility version of at least 4.1.
func TestShouldRunMetadataMigration(t *testing.T) {
	compatVersion := func(major, minor uint8) func() *base.ClusterCompatVersion {
		return func() *base.ClusterCompatVersion {
			v := base.NewClusterCompatVersion(major, minor)
			return &v
		}
	}
	nilCompatVersion := func() *base.ClusterCompatVersion { return nil }

	testCases := []struct {
		name           string
		useSystemMeta  bool
		compatVersion  func() *base.ClusterCompatVersion
		expectedResult bool
	}{
		{name: "opt-out, no compat version", useSystemMeta: false, compatVersion: nil, expectedResult: false},
		{name: "opt-out, compat 4.1", useSystemMeta: false, compatVersion: compatVersion(4, 1), expectedResult: false},
		{name: "opt-in, nil compat func", useSystemMeta: true, compatVersion: nil, expectedResult: false},
		{name: "opt-in, compat func returns nil", useSystemMeta: true, compatVersion: nilCompatVersion, expectedResult: false},
		{name: "opt-in, compat 3.0", useSystemMeta: true, compatVersion: compatVersion(3, 0), expectedResult: false},
		{name: "opt-in, compat 4.0", useSystemMeta: true, compatVersion: compatVersion(4, 0), expectedResult: false},
		{name: "opt-in, compat 4.1", useSystemMeta: true, compatVersion: compatVersion(4, 1), expectedResult: true},
		{name: "opt-in, compat 4.2", useSystemMeta: true, compatVersion: compatVersion(4, 2), expectedResult: true},
		{name: "opt-in, compat 5.0", useSystemMeta: true, compatVersion: compatVersion(5, 0), expectedResult: true},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			dbCtx := &DatabaseContext{
				Options: DatabaseContextOptions{
					UseSystemMetadataCollection: test.useSystemMeta,
				},
				ClusterCompatVersionFunc: test.compatVersion,
			}
			assert.Equal(t, test.expectedResult, dbCtx.shouldRunMetadataMigration())
		})
	}
}

// TestArmMetadataMigrationTaskNoCallback verifies that armMetadataMigrationTask returns
// immediately (rather than blocking on its poll ticker) when no ConfigFullyAppliedFunc has
// been set, so a misconfigured database cannot leak a goroutine waiting forever.
func TestArmMetadataMigrationTaskNoCallback(t *testing.T) {
	dbCtx := &DatabaseContext{
		Name:       "test",
		terminator: make(chan bool),
	}
	// ConfigFullyAppliedFunc is intentionally left nil.

	done := make(chan struct{})
	go func() {
		dbCtx.armMetadataMigrationTask(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "armMetadataMigrationTask did not return when ConfigFullyAppliedFunc was nil")
	}
}

// TestTryStartMetadataMigrationAlreadyRunning verifies that when the migration is already running on
// another node (the heartbeat lock doc exists), tryStartMetadataMigration reports done=true so the
// arm loop stops rather than re-acquiring the lock once that node completes - which would re-run the
// migration once per node in the cluster.
func TestTryStartMetadataMigrationAlreadyRunning(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-already-running")

	dbCtx := &DatabaseContext{
		Name:          "test",
		terminator:    make(chan bool),
		MetadataStore: metadataStore,
		MetadataKeys:  metaKeys,
	}
	dbCtx.MetadataMigrationManager = NewMetadataMigrationManager(dbCtx)

	// Simulate another node holding the migration heartbeat lock, so Start returns
	// errBackgroundManagerProcessAlreadyRunning.
	heartbeatDocID := dbCtx.MetadataMigrationManager.clusterAwareOptions.HeartbeatDocID()
	_, err := metadataStore.WriteCas(ctx, heartbeatDocID, BackgroundManagerHeartbeatExpirySecs, 0, []byte("{}"), sgbucket.Raw)
	require.NoError(t, err)

	// Config is fully applied so the attempt proceeds to Start.
	dbCtx.ConfigFullyAppliedFunc = func(ctx context.Context) (bool, []string, error) {
		return true, nil, nil
	}

	// done=true tells the arm loop to stop polling rather than retry and re-run later.
	require.True(t, dbCtx.tryStartMetadataMigration(ctx))

	// The local manager must not have started: it should still be in its initial (uninitialized)
	// run state, having backed off rather than acquiring the lock.
	assert.NotEqual(t, BackgroundProcessStateRunning, dbCtx.MetadataMigrationManager.GetRunState())

	// The other node's heartbeat lock must remain untouched.
	_, _, err = metadataStore.GetRaw(ctx, heartbeatDocID)
	assert.NoError(t, err, "heartbeat lock doc should still exist - attempt must not have taken it")
}

// TestTryStartMetadataMigrationConfigNotApplied verifies that while the cluster has not yet
// converged on the config, tryStartMetadataMigration reports done=false so the arm loop keeps
// polling, and does not start the migration.
func TestTryStartMetadataMigrationConfigNotApplied(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	dbCtx := &DatabaseContext{
		Name:          "test",
		terminator:    make(chan bool),
		MetadataStore: testBucket.DefaultDataStore(ctx),
		MetadataKeys:  base.NewMetadataKeys("test-not-applied"),
	}
	dbCtx.MetadataMigrationManager = NewMetadataMigrationManager(dbCtx)

	dbCtx.ConfigFullyAppliedFunc = func(ctx context.Context) (bool, []string, error) {
		return false, []string{"node2"}, nil
	}

	assert.False(t, dbCtx.tryStartMetadataMigration(ctx))
	assert.NotEqual(t, BackgroundProcessStateRunning, dbCtx.MetadataMigrationManager.GetRunState())
}
