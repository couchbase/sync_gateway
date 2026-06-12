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
	"maps"
	"slices"
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

// inMemoryMigrationStatusUpdater returns a MetadataMigrationStatusUpdater backed by a
// throwaway in-memory MetadataMigrationStatus. The production wiring CAS-writes the
// shared bucket-level status doc; these tests don't care about that contract, they just
// need a non-nil updater so the manager's per-DB in_progress → complete writes succeed
// and we can exercise the migration logic.
func inMemoryMigrationStatusUpdater() func(context.Context, func(*base.MetadataMigrationStatus) error) error {
	status := &base.MetadataMigrationStatus{Databases: map[string]*base.DatabaseMigrationStatus{}}
	return func(_ context.Context, mutator func(*base.MetadataMigrationStatus) error) error {
		return mutator(status)
	}
}

// TestMetadataMigrationManagerMovesUsersAndRoles is the end-to-end check for the wired-up
// background task: it seeds user and role docs onto the fallback collection, starts the
// MetadataMigrationManager, waits for it to reach Completed, and verifies the docs were
// actually relocated to the primary collection and removed from the fallback. It also
// asserts the manager's externally-visible counters (DocsProcessed / DocsFailed) match
// the seeded population and that SetMigrationComplete was flipped on a clean run.
func TestMetadataMigrationManagerMovesUsersAndRoles(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	primary, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	fallback := bucket.DefaultDataStore(ctx)
	if _, ok := base.AsRangeScanStore(fallback); !ok {
		t.Skipf("metadata migration requires KV range scan support on the fallback datastore")
	}
	ms := base.NewMetadataStore(primary, fallback)

	const metadataID = "test-mgr-move-userrole"
	metaKeys := base.NewMetadataKeys(metadataID)
	seeded := map[string][]byte{
		metaKeys.UserKey("alice"):   []byte(`{"name":"alice"}`),
		metaKeys.UserKey("bob"):     []byte(`{"name":"bob"}`),
		metaKeys.RoleKey("admins"):  []byte(`{"name":"admins"}`),
		metaKeys.RoleKey("readers"): []byte(`{"name":"readers"}`),
	}
	for k, v := range seeded {
		_, err := ms.Fallback().AddRaw(ctx, k, 0, v)
		require.NoError(t, err, "seed fallback %s", k)
	}

	// A first-pass range scan that misses the just-seeded docs would migrate nothing and
	// complete with DocsProcessed=0, failing the assertions below. Wait for the seeds to be
	// visible to scan before starting.
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), slices.Collect(maps.Keys(seeded)))

	dbCtx := &DatabaseContext{
		Name:                           "test",
		terminator:                     make(chan bool),
		MetadataStore:                  ms,
		MetadataKeys:                   metaKeys,
		Options:                        DatabaseContextOptions{MetadataID: metadataID},
		MetadataMigrationStatusUpdater: inMemoryMigrationStatusUpdater(),
	}
	dbCtx.MetadataMigrationManager = NewMetadataMigrationManager(dbCtx)

	require.NoError(t, dbCtx.MetadataMigrationManager.Start(ctx, nil))
	RequireBackgroundManagerState(t, dbCtx.MetadataMigrationManager, BackgroundProcessStateCompleted)

	rawStatus, err := dbCtx.MetadataMigrationManager.GetStatus(ctx)
	require.NoError(t, err)
	var resp MigrationManagerResponse
	require.NoError(t, base.JSONUnmarshal(rawStatus, &resp))
	assert.Equal(t, int64(len(seeded)), resp.DocsProcessed, "manager DocsProcessed should equal seeded user+role count")
	assert.Zero(t, resp.DocsFailed)

	for k, want := range seeded {
		got, _, getErr := ms.Primary().GetRaw(ctx, k)
		require.NoError(t, getErr, "primary should hold %s", k)
		assert.Equal(t, want, got)

		_, _, getErr = ms.Fallback().GetRaw(ctx, k)
		assert.True(t, base.IsDocNotFoundError(getErr), "fallback should no longer hold %s", k)
	}
	assert.True(t, ms.MigrationComplete(), "clean run must flip MigrationComplete so future reads skip the fallback")
}

// TestMetadataMigrationManagerCompletesWithUnknownPrefixLeftInPlace pins the policy that an
// unrecognised `_sync:`-prefixed doc does NOT wedge the migration. The dispatcher classifies it
// as DocsUnknownPrefix and leaves it on the fallback every pass, but unknown-prefix docs no longer
// count toward the completion gate (only per-doc move/delete errors do). So the migration must
// complete, flip MigrationComplete, and leave the unrecognised doc untouched on the fallback —
// rather than retrying to maxPasses and erroring. This protects against a foreign doc (written
// directly to the bucket, bypassing SG's public API) being able to abort an operator's upgrade.
func TestMetadataMigrationManagerCompletesWithUnknownPrefixLeftInPlace(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	primary, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	fallback := bucket.DefaultDataStore(ctx)
	if _, ok := base.AsRangeScanStore(fallback); !ok {
		t.Skipf("metadata migration requires KV range scan support on the fallback datastore")
	}
	ms := base.NewMetadataStore(primary, fallback)

	const metadataID = "test-mgr-unknown-prefix"
	// `_sync:m_<id>:wat` is in-scope (starts with our standard-form prefix) but matches
	// no known family, so handleMigrationKey hits the default branch and counts it as
	// DocsUnknownPrefix without removing it.
	unknownKey := base.SyncDocPrefix + base.MetadataIdPrefix + metadataID + ":wat"
	_, err = ms.Fallback().AddRaw(ctx, unknownKey, 0, []byte(`{"body":"left-in-place"}`))
	require.NoError(t, err, "seed in-scope unknown-prefix fallback doc")

	// KV range scan reads from a per-vBucket snapshot view, so a scan issued immediately
	// after the write can miss the seeded doc until the vBucket's scan view catches up. Wait
	// for the seed to be visible to scan so we exercise the unknown-prefix path rather than a
	// trivially-empty first pass.
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), []string{unknownKey})

	dbCtx := &DatabaseContext{
		Name:                           "test",
		terminator:                     make(chan bool),
		MetadataStore:                  ms,
		MetadataKeys:                   base.NewMetadataKeys(metadataID),
		Options:                        DatabaseContextOptions{MetadataID: metadataID},
		MetadataMigrationStatusUpdater: inMemoryMigrationStatusUpdater(),
	}
	dbCtx.MetadataMigrationManager = NewMetadataMigrationManager(dbCtx)

	require.NoError(t, dbCtx.MetadataMigrationManager.Start(ctx, nil))
	// Unknown-prefix docs no longer block completion — the manager reaches the Completed state.
	RequireBackgroundManagerState(t, dbCtx.MetadataMigrationManager, BackgroundProcessStateCompleted)

	assert.True(t, ms.MigrationComplete(), "MigrationComplete should be flipped — unknown-prefix docs do not block completion")

	_, _, err = ms.Fallback().GetRaw(ctx, unknownKey)
	assert.NoError(t, err, "unrecognised doc should be left in place on the fallback (non-destructive)")
}

// TestMetadataMigrationManagerDCPCheckpointGroupIDCollisionCompletesEndToEnd is the
// end-to-end counterpart to the low-level TestMigrateMetadataDcpCheckpointGroupIDCollisionKnownLimitation:
//
// Scenario: a default-metadataID DB (_default._default configured) whose own DCP checkpoint
// embeds its config group ID "default" as the first body segment (`_sync:dcp_ck:default:<ver>`).
// A pathological sibling DB whose metadataID is literally "default" is registered via
// SiblingMetadataIDFunc, so the classifier subtracts that sibling and misclassifies the default
// DB's own checkpoint as the sibling's.
func TestMetadataMigrationManagerDCPCheckpointGroupIDCollisionCompletesEndToEnd(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	primary, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	fallback := bucket.DefaultDataStore(ctx)
	if _, ok := base.AsRangeScanStore(fallback); !ok {
		t.Skipf("metadata migration requires KV range scan support on the fallback datastore")
	}
	ms := base.NewMetadataStore(primary, fallback)

	metaKeys := base.NewMetadataKeys(base.DefaultMetadataID)
	// The default DB's own DCP checkpoint embeds its config group ID as the first body
	// segment: _sync:dcp_ck:default:1. "default" is the literal value of the rest package's
	// PersistentConfigDefaultGroupID (not importable here without a cycle).
	const defaultGroupID = "default"
	ownCheckpoint := metaKeys.DCPCheckpointPrefix(defaultGroupID) + "1"
	_, err = ms.Fallback().AddRaw(ctx, ownCheckpoint, 0, []byte(`{"seq":42}`))
	require.NoError(t, err, "seed fallback %s", ownCheckpoint)

	// KV range scans read from a per-vBucket snapshot view, so a scan issued immediately after
	// the write can miss the just-seeded doc until the vBucket's scan view catches up. Wait for
	// the seed to be visible to scan before starting, otherwise a first-pass miss would make the
	// run complete trivially for the wrong reason.
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), []string{ownCheckpoint})

	dbCtx := &DatabaseContext{
		Name:                           "test",
		terminator:                     make(chan bool),
		MetadataStore:                  ms,
		MetadataKeys:                   metaKeys,
		Options:                        DatabaseContextOptions{MetadataID: base.DefaultMetadataID},
		MetadataMigrationStatusUpdater: inMemoryMigrationStatusUpdater(),
		// Pathological sibling whose metadataID == this default DB's DCP group ID ("default")
		// triggers the documented collision in handleMigrationKey's isOursInverted exclusion.
		SiblingMetadataIDFunc: func(context.Context) ([]string, error) {
			return []string{defaultGroupID}, nil
		},
	}
	dbCtx.MetadataMigrationManager = NewMetadataMigrationManager(dbCtx)

	require.NoError(t, dbCtx.MetadataMigrationManager.Start(ctx, nil))
	// The collision yields an out-of-scope (not unknown-prefix) classification, so the run
	// still reaches Completed rather than the bounded-pass Errored give-up branch.
	RequireBackgroundManagerState(t, dbCtx.MetadataMigrationManager, BackgroundProcessStateCompleted)

	rawStatus, err := dbCtx.MetadataMigrationManager.GetStatus(ctx)
	require.NoError(t, err)
	var resp MigrationManagerResponse
	require.NoError(t, base.JSONUnmarshal(rawStatus, &resp))
	assert.Zero(t, resp.DocsProcessed, "known limitation: own checkpoint is NOT migrated under the group-id/sibling-id collision")
	assert.Zero(t, resp.DocsFailed, "no per-doc errors: the collision is a classification outcome, not a move failure")
	assert.Equal(t, int64(1), resp.DocsOutOfScope, "own checkpoint is (mis)classified out-of-scope, not unknown-prefix")

	assert.True(t, ms.MigrationComplete(), "out-of-scope docs do not block completion - the run still flips MigrationComplete")

	_, _, getErr := ms.Fallback().GetRaw(ctx, ownCheckpoint)
	assert.NoError(t, getErr, "own checkpoint is stranded on the fallback under the known limitation")
	_, _, getErr = ms.Primary().GetRaw(ctx, ownCheckpoint)
	assert.True(t, base.IsDocNotFoundError(getErr), "own checkpoint did not reach primary")
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
