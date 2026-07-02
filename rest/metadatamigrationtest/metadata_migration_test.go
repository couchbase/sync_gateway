/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadatamigrationtest

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/google/uuid"
)

// TestMetadataMigrationNotStartedWithoutOptIn creates a database without setting
// UseSystemMobileMetadataCollection and verifies that the metadata migration
// background task is never started.
func TestMetadataMigrationNotStartedWithoutOptIn(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	dbCtx := rt.GetDatabase()
	require.NotNil(t, dbCtx.MetadataMigrationManager)

	state := dbCtx.MetadataMigrationManager.GetRunState()
	assert.Equal(t, db.BackgroundProcessState(""), state, "metadata migration should not have been started without UseSystemMobileMetadataCollection opt-in")
}

// TestMetadataMigrationNotStartedWithExplicitFalse creates a database with
// UseSystemMobileMetadataCollection explicitly set to false and verifies the
// metadata migration background task is never started.
func TestMetadataMigrationNotStartedWithExplicitFalse(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx := rt.GetDatabase()
	require.NotNil(t, dbCtx.MetadataMigrationManager)

	state := dbCtx.MetadataMigrationManager.GetRunState()
	assert.Equal(t, db.BackgroundProcessState(""), state, "metadata migration should not have been started with UseSystemMobileMetadataCollection=false")
}

// TestMetadataMigrationStartsAfterAllNodesApplyConfig uses two rest testers sharing the same
// bucket to simulate a two-node cluster. It verifies that:
//  1. Migration does not start while node B has not yet picked up the opt-in config.
//  2. Once node B applies the config and the registry reflects this, the migration starts.
func TestMetadataMigrationStartsAfterAllNodesApplyConfig(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		// rtA and rtB share a group to simulate one two-node cluster, but it must be unique per
		// run: a fixed group lets a stale "db" registry entry (from a recycled-but-still-flushing
		// pool bucket, or a prior test's lingering config-poll/heartbeat) collide with this test's
		// create ("Duplicate database name").
		GroupID: base.Ptr(uuid.NewString()),
	}

	rtA := rest.NewRestTester(t, rtConfig)
	defer rtA.Close()
	rtB := rest.NewRestTester(t, rtConfig)
	defer rtB.Close()

	// Create db on node A with migration disabled.
	dbConfig := rtA.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp := rtA.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Node B picks up the db via config polling.
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	// Flush both nodes' heartbeats to the registry so each node is visible.
	rtA.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rtB.ServerContext().ForceClusterCompatRefresh(t, ctx)

	// Seed a legacy per-DB metadata key in _default._default so the new-DB fast path doesn't
	// classify this as a fresh DB. Without this the probe at db construction sees an empty
	// _default._default for the db's metadataID, flips the MetadataStore wrapper straight to
	// MigrationComplete, and shouldRunMetadataMigration returns false — which is exactly what
	// we want for fresh DBs, but defeats this test's premise of verifying the multi-node gate.
	//
	// Seed via Incr so the doc matches the on-disk shape of a counter created by the
	// sequence allocator in real legacy data. SetRaw would write with explicit BinaryType
	// flag, which the SGJSONTranscoder used by GetCounter can't decode into a *uint64 —
	// fine on Rosmar (relaxed datatype handling) but a hard failure on Couchbase Server.
	dbCtxBefore := rtA.GetDatabase()
	_, err := tb.Bucket.DefaultDataStore(ctx).Incr(ctx, dbCtxBefore.MetadataKeys.SyncSeqKey(), 1, 0, 0)
	require.NoError(t, err)

	// Update db config on node A to enable the system metadata collection.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp = rtA.UpsertDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Flush node A's registry entry so its new db version is visible.
	rtA.ServerContext().ForceClusterCompatRefresh(t, ctx)

	// Node B has NOT yet picked up the updated config. Assert directly on the gate that
	// tryStartMetadataMigration consults: ConfigFullyAppliedFunc must report not-applied (with node
	// B outstanding), which is what blocks the migration from starting. This is deterministic,
	// unlike observing the run state of the background goroutine.
	dbCtxA := rtA.GetDatabase()
	require.NotNil(t, dbCtxA.MetadataMigrationManager)
	require.NotNil(t, dbCtxA.ConfigFullyAppliedFunc)
	applied, missing, err := dbCtxA.ConfigFullyAppliedFunc(ctx)
	require.NoError(t, err)
	assert.False(t, applied, "config must not be fully applied while node B is still on the old version")
	assert.Contains(t, missing, rtB.ServerContext().NodeUID, "node B must be reported as not having applied the new config version")

	// Node B picks up the updated config.
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	// Flush node B's registry entry so the new db version is visible.
	rtB.ServerContext().ForceClusterCompatRefresh(t, ctx)

	// The gate now reports fully applied across the cluster - this is what unblocks the migration.
	applied, missing, err = dbCtxA.ConfigFullyAppliedFunc(ctx)
	require.NoError(t, err)
	assert.True(t, applied, "config should be fully applied once node B has applied the new version")
	assert.Empty(t, missing)

	// Either node can win the race to start the background migration job, so check both.
	dbCtxB := rtB.GetDatabase()
	require.NotNil(t, dbCtxB.MetadataMigrationManager)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stateA := dbCtxA.MetadataMigrationManager.GetRunState()
		stateB := dbCtxB.MetadataMigrationManager.GetRunState()
		started := stateA != db.BackgroundProcessState("") || stateB != db.BackgroundProcessState("")
		assert.True(c, started,
			"migration should have been started on at least one node after all nodes applied the config")
	}, 30*time.Second, 100*time.Millisecond)
}

// TestMetadataMigrationStatsNotInitialisedWithoutOptIn verifies that the per-DB
// MigrationStats Prometheus section is omitted entirely for databases that have not
// opted into the system metadata collection. Pairs with InitMigrationStats being
// gated on the UseSystemMetadataCollection flag in NewDBStats.
func TestMetadataMigrationStatsNotInitialisedWithoutOptIn(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	dbCtx := rt.GetDatabase()
	require.NotNil(t, dbCtx.DbStats)
	assert.Nil(t, dbCtx.DbStats.MetadataMigration(), "migration stats section must be omitted when UseSystemMobileMetadataCollection is unset")
}

// TestMetadataMigrationStatsInitialisedWithOptIn verifies that opting into the
// system metadata collection wires up the per-DB MigrationStats Prometheus
// section and that its state gauge starts at idle.
func TestMetadataMigrationStatsInitialisedWithOptIn(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbCtx := rt.GetDatabase()
	require.NotNil(t, dbCtx.DbStats)
	migStats := dbCtx.DbStats.MetadataMigration()
	require.NotNil(t, migStats, "migration stats must be initialized when UseSystemMobileMetadataCollection=true")
	// Default counters all start at 0.
	assert.Equal(t, int64(0), migStats.DocsMigrated.Value())
	assert.Equal(t, int64(0), migStats.Errors.Value())
	assert.Equal(t, int64(0), migStats.Passes.Value())
}

// TestMetadataMigrationRESTGetReportsStatus verifies the new GET
// /{db}/_metadata_migration endpoint returns the background-task status payload.
func TestMetadataMigrationRESTGetReportsStatus(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_metadata_migration", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	body := resp.Body.String()
	// The payload is the BackgroundManager status JSON — assert on a couple of
	// stable fields rather than the full shape so the test isn't brittle to
	// additions to MigrationManagerResponse.
	assert.Contains(t, body, "docs_processed")
	assert.Contains(t, body, "passes")
}

// TestMetadataMigrationNotRestartedAfterCompletion is the CBG-5475 regression: once a database's
// metadata migration has completed, a repeat manual start must be an idempotent no-op. It must NOT
// reset the durable status doc back through in_progress — that flap (triggered when the ~10s
// auto-arm poller re-fired after a fast REST-triggered completion) is what made status assertions
// in longer-running tests flaky. reset=true remains an operator override and is exercised separately
// in unit tests of Init; here we pin the default (reset=false) no-op path, which is fully deterministic
// because the guard skips Start entirely and leaves the status doc untouched.
func TestMetadataMigrationNotRestartedAfterCompletion(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()
	ctx := rt.Context()

	// Create in legacy mode and write a user so real per-DB metadata (a user doc + the _sync:seq
	// counter) lands in _default._default. Without legacy data the new-DB fast path would flip the
	// wrapper straight to MigrationComplete at construction and no migration would actually run.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice",
		`{"name":"alice","password":"letmein","admin_channels":["public"]}`), http.StatusCreated)

	// Opt in and drive the migration to completion via the REST endpoint.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", ""), http.StatusOK)
	completed := rt.WaitForMetadataMigrationStatus(db.BackgroundProcessStateCompleted)
	require.NotEmpty(t, completed.MigrationID)

	// The completion signal both start paths consult is now true.
	require.True(t, rt.GetDatabase().MetadataMigrationComplete(ctx), "migration should report complete after finishing")

	// Repeat the manual start (reset defaults to false): the guard skips Start, so the durable status
	// doc is untouched — same state, same migration ID, same start time. No in_progress flap.
	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var after db.MigrationManagerResponse
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &after))
	assert.Equal(t, db.BackgroundProcessStateCompleted, after.State, "repeat start must leave the migration completed")
	assert.Equal(t, completed.MigrationID, after.MigrationID, "repeat start must not begin a fresh migration run")
	assert.True(t, completed.StartTime.Equal(after.StartTime), "repeat start must not reset the migration start time")
}

// TestMetadataMigrationGuardHonoursPeerCompletion covers the multi-node arm of CBG-5475: a node
// that never ran the migration itself (its process-local MetadataStore flag is a stale false) must
// still recognise completion from the authoritative bucket-level status doc — and, on observing it,
// converge its local flag so fallback reads stop. Mirrors the peer-completion simulation in
// TestRecheckConvergesLocalCacheOnPeerCompletedMigration.
func TestMetadataMigrationGuardHonoursPeerCompletion(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()
	ctx := rt.Context()

	// Create in legacy mode, then seed the _sync:seq counter in _default._default so the new-DB fast
	// path can't flip the wrapper to MigrationComplete at construction — the local flag must stay
	// false to model a peer that ran the migration while this node did not.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)
	legacyDB := rt.GetDatabase()
	_, err := legacyDB.Bucket.DefaultDataStore(ctx).Incr(ctx, legacyDB.MetadataKeys.SyncSeqKey(), 1, 0, 0)
	require.NoError(t, err)

	// Opt in but deliberately DON'T flush this node's applied-config version, so ConfigFullyAppliedFunc
	// reports not-applied and the auto-arm never actually starts a local migration.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)

	dbCtx := rt.GetDatabase()
	ms, ok := dbCtx.MetadataStore.(*base.MetadataStore)
	require.True(t, ok, "opted-in DB with legacy metadata should have a dual MetadataStore")
	require.False(t, ms.MigrationComplete(), "precondition: local node must not have marked migration complete")

	// Simulate a peer node completing this DB's migration by stamping the per-DB status entry to
	// complete directly — this writes only the durable doc, not this node's local flag.
	conn := rt.ServerContext().BootstrapContext.Connection
	_, err = conn.UpdateMetadataMigrationStatus(ctx, dbCtx.Bucket.GetName(), func(s *base.MetadataMigrationStatus) error {
		if s.Databases == nil {
			s.Databases = map[string]*base.DatabaseMigrationStatus{}
		}
		s.Databases[dbCtx.Options.MetadataID] = &base.DatabaseMigrationStatus{State: base.MigrationStateComplete}
		return nil
	})
	require.NoError(t, err)

	// The guard reads the authoritative status doc, reports complete, and converges the local flag.
	require.True(t, dbCtx.MetadataMigrationComplete(ctx), "guard must recognise a peer-completed migration from the status doc")
	require.True(t, ms.MigrationComplete(), "observing a peer-completed status doc must converge the local migration-complete flag")
}

// TestMetadataMigrationPreservesJSONDatatypeForUserDocs is a regression test for the
// AddRaw → SGRawTranscoder datatype regression: prior to the moveFallbackDoc datatype
// fix, every user/role/session doc migrated to _system._mobile landed with the Binary
// datatype flag (0x03000000), and subsequent reads through gocb.NewRawJSONTranscoder
// (used by the auth path) failed with HTTP 500 "binary datatype is not supported by
// RawJSONTranscoder". The seq-counter equivalent was fixed via Incr in CBG-5228;
// this test pins the same invariant for the per-doc move.
//
// The test only exercises the regression on a Couchbase Server backing store: Rosmar's
// transcoder handling is lax enough that the bug doesn't manifest in-memory. We rely on
// CI to run the SG_TEST_BACKING_STORE=Couchbase variant — under Rosmar the test still
// runs but the assertion is effectively trivial.
func TestMetadataMigrationPreservesJSONDatatypeForUserDocs(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	// Create the database without the opt-in so the user write lands in
	// _default._default (the eventual fallback collection). Authenticator.Update writes
	// users with the JSON datatype on disk — this gives us a real pre-migration shadow.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	const userPayload = `{"name":"alice","password":"letmein","admin_channels":["public"]}`
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", userPayload)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Read alice back to capture the working pre-migration shape.
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"alice"`)

	// Flip the opt-in on the existing database. This rebuilds the MetadataStore as a
	// dual-collection wrapper with primary=_system._mobile, fallback=_default._default —
	// so alice now lives on fallback exactly as she would after the upgrade in production.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp = rt.UpsertDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// The manual start endpoint gates on ConfigFullyAppliedFunc just like the auto-arming path,
	// so flush this node's registry entry to record its applied config version — even a
	// single-node cluster must report the opt-in config applied before it can start. Then drive
	// the migration and poll the GET endpoint until it reports completed (or error).
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_metadata_migration", "")
		body := st.Body.String()
		// "completed" is the happy path; bail out on "error" so we don't poll forever.
		assert.NotContains(c, body, `"status":"error"`, "migration must not error — payload: %s", body)
		assert.Contains(c, body, `"status":"completed"`, "migration should reach completed — payload: %s", body)
	}, 30*time.Second, 200*time.Millisecond)

	// THE regression check: reading alice goes through `auth.Authenticator.GetPrincipal`,
	// which on Couchbase Server uses gocb.NewRawJSONTranscoder. A Binary-tagged primary
	// doc surfaces here as a 500 with the message "binary datatype is not supported by
	// RawJSONTranscoder" — exactly the failure mode this fix addresses. A JSON-tagged
	// doc decodes cleanly and we get the user payload back at 200 OK.
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"alice"`, "post-migration user read must succeed — Binary datatype on the migrated doc would surface as a 500 here")
	assert.NotContains(t, resp.Body.String(), "binary datatype", "the migrated user doc must not be Binary-tagged on primary")
}

// TestMetadataMigrationSkipsPrimaryWriteWhenUpdatedUserAlreadyMigrated pins the
// in-flight-update / stale-fallback-shadow race through the real auth path:
//
//  1. A user is created in legacy mode, so the user doc lands on the eventual fallback
//     collection (_default._default).
//  2. The opt-in is flipped, switching the MetadataStore to the dual-collection wrapper.
//     At this point fallback still holds the original user body; primary is empty.
//  3. The user is updated via the admin API *before* the migration runs.
//     `auth.Authenticator.Update` → `MetadataStore.Update` is read-through-fallback /
//     write-to-primary, so the fresh body lands on primary while the stale original
//     stays on fallback as a shadow.
//  4. Migration runs and walks the fallback. For the user key, moveFallbackDoc's
//     Primary().Add returns (added=false, err=nil) because primary already holds the
//     fresher copy. That outcome MUST be treated as success — not an error — and the
//     stale fallback shadow MUST be deleted, leaving the fresh primary copy intact.
//
// What this would catch:
//   - moveFallbackDoc surfacing the already-exists Add as a stats.Errors increment.
//   - A naive migration that overwrote primary with the stale fallback bytes.
//   - A migration that skipped the fallback delete on the already-exists branch, leaving
//     a stale shadow that would resurrect on the next non-xattr read.
func TestMetadataMigrationSkipsPrimaryWriteWhenUpdatedUserAlreadyMigrated(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	// 1. Legacy mode: user write lands on _default._default.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	const originalUser = `{"name":"alice","password":"letmein","admin_channels":["public"]}`
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", originalUser)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// 2. Flip the opt-in: MetadataStore becomes the dual-collection wrapper. The user
	// doc lives on fallback (_default._default); primary (_system._mobile) is empty.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp = rt.UpsertDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	ms, ok := rt.GetDatabase().MetadataStore.(*base.MetadataStore)
	require.True(t, ok, "after opt-in the MetadataStore must be the dual-collection wrapper")
	userKey := rt.GetDatabase().MetadataKeys.UserKey("alice")

	// Sanity: before any update, the user body is only on fallback.
	_, _, err := ms.Fallback().GetRaw(rt.Context(), userKey)
	require.NoError(t, err, "user doc must be on fallback before the in-flight update")
	primaryExists, err := ms.Primary().Exists(rt.Context(), userKey)
	require.NoError(t, err)
	require.False(t, primaryExists, "primary must be empty before the in-flight update")

	// 3. Update the user before the migration runs. Authenticator.Update goes through
	// MetadataStore.Update, which reads through fallback and writes the fresh body to
	// primary. After this the fallback still holds the original body as a stale shadow.
	const updatedUser = `{"name":"alice","admin_channels":["public","secrets"]}`
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", updatedUser)
	rest.RequireStatus(t, resp, http.StatusOK)

	primaryBodyBeforeMigration, _, err := ms.Primary().GetRaw(rt.Context(), userKey)
	require.NoError(t, err, "primary must hold the post-update user body after the in-flight Update")
	require.Contains(t, string(primaryBodyBeforeMigration), `"secrets"`,
		"primary should carry the updated admin_channels, fallback should still hold the original")
	fallbackBodyBeforeMigration, _, err := ms.Fallback().GetRaw(rt.Context(), userKey)
	require.NoError(t, err, "stale fallback shadow must still exist before migration runs")
	require.NotEqual(t, primaryBodyBeforeMigration, fallbackBodyBeforeMigration,
		"primary fresh body and fallback stale shadow must structurally differ — otherwise this test isn't exercising the race")

	// 4. Flush this node's applied config version (the manual start gates on config-fully-applied),
	// then drive the migration through the admin endpoint and assert it reaches completed with zero
	// failures — the already-exists Add MUST NOT be classified as an error.
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_metadata_migration", "")
		body := st.Body.String()
		assert.NotContains(c, body, `"status":"error"`, "migration must not error on the already-exists branch — payload: %s", body)
		assert.Contains(c, body, `"status":"completed"`, "migration should reach completed — payload: %s", body)
		assert.Contains(c, body, `"docs_failed":0`, "already-exists Add must not be counted as a failure — payload: %s", body)
	}, 30*time.Second, 200*time.Millisecond)

	// 5a. Primary must still hold the FRESH body — the migration must not have
	// overwritten it with the stale fallback shadow.
	primaryBodyAfterMigration, _, err := ms.Primary().GetRaw(rt.Context(), userKey)
	require.NoError(t, err, "primary must still hold the user post-migration")
	assert.Equal(t, primaryBodyBeforeMigration, primaryBodyAfterMigration,
		"primary body must be byte-identical to the pre-migration fresh write — a regression that overwrote with the stale fallback shadow would change this")

	// 5b. Fallback shadow must be gone — the already-exists short-circuit must still
	// run the cleanup delete. Leaving the shadow would resurrect the stale body on
	// any subsequent non-xattr read path.
	_, _, err = ms.Fallback().GetRaw(rt.Context(), userKey)
	assert.True(t, base.IsDocNotFoundError(err),
		"stale fallback shadow must be cleaned up even when primary already held a fresher copy — got err: %v", err)

	// 5c. End-to-end: the public auth view of the user is the fresh post-update shape.
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"secrets"`,
		"GET /_user/alice must reflect the in-flight update — not the pre-update fallback shadow")
}

// TestMetadataMigrationOptInIsIrreversibleViaREST verifies that, once a database has opted in
// to the system metadata collection, a config update over the REST API that attempts to disable
// the opt-in (set to false or omit it) is rejected.
func TestMetadataMigrationOptInIsIrreversibleViaREST(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Attempting to set the opt-in back to false is rejected.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp = rt.ReplaceDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "use_system_metadata_collection cannot be disabled once enabled")

	// Omitting the field entirely (nil) is also rejected, since PUT replaces the whole config.
	dbConfig.UseSystemMobileMetadataCollection = nil
	resp = rt.ReplaceDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "use_system_metadata_collection cannot be disabled once enabled")

	// Keeping the opt-in enabled is permitted.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp = rt.ReplaceDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)
}

func TestMetadataMigrationLegacyDefaultDBSiblingClassifiedOutOfScope(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	const namedCollection = "sg_test_0"
	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	defer func() {
		assert.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	}()

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	// dbdefault: only _default._default → metadataID="_default", keys unprefixed.
	const dbDefaultName = "dbdefault"
	dbDefaultCfg := rt.NewDbConfig()
	dbDefaultCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				base.DefaultCollection: {},
			},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbDefaultName, dbDefaultCfg), http.StatusCreated)

	// Real legacy user key for dbdefault — `_sync:user:alice` lands in _default._default.
	resp := rt.SendAdminRequest(http.MethodPut, "/"+dbDefaultName+"/_user/alice",
		`{"name":"alice","password":"letmein","admin_channels":["public"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// dbnamed: only _default.sg_test_0 → metadataID is the db name (non-default).
	// Start in legacy mode (opt-in=false) so dbnamed's own metadata also lands in
	// _default._default alongside dbdefault's. This matches the production pre-migration
	// state for a namespaced DB about to be upgraded.
	const dbNamedName = "dbnamed"
	dbNamedCfg := rt.NewDbConfig()
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbNamedCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				namedCollection: {},
			},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbNamedName, dbNamedCfg), http.StatusCreated)

	// Real per-DB user key for dbnamed — `_sync:user:m_<id>:bob` lands in _default._default.
	resp = rt.SendAdminRequest(http.MethodPut, "/"+dbNamedName+"/_user/bob",
		`{"name":"bob","password":"letmein","admin_channels":["public"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Sanity-check that the topology actually matches the test's premise.
	dbDefaultCtx, err := rt.ServerContext().GetDatabase(ctx, dbDefaultName)
	require.NoError(t, err)
	require.Equal(t, base.DefaultMetadataID, dbDefaultCtx.Options.MetadataID,
		"dbdefault must use the legacy default metadataID (this is what makes its keys land unprefixed in _default._default)")

	dbNamedCtx, err := rt.ServerContext().GetDatabase(ctx, dbNamedName)
	require.NoError(t, err)
	require.NotEqual(t, base.DefaultMetadataID, dbNamedCtx.Options.MetadataID,
		"dbnamed must have a non-default metadataID (this is what gives its keys the m_<id>: prefix)")
	require.NotEmpty(t, dbNamedCtx.Options.MetadataID)

	// Sanity-check that both DBs' legacy keys are actually colocated in _default._default
	// — that's the precondition for the dispatcher classification scenario we're testing.
	defaultDS := tb.Bucket.DefaultDataStore(ctx)
	aliceKey := base.DefaultMetadataKeys.UserKey("alice")
	exists, err := defaultDS.Exists(ctx, aliceKey)
	require.NoError(t, err)
	require.True(t, exists, "dbdefault's user key %q must exist in _default._default before migration", aliceKey)

	bobKey := dbNamedCtx.MetadataKeys.UserKey("bob")
	exists, err = defaultDS.Exists(ctx, bobKey)
	require.NoError(t, err)
	require.True(t, exists, "dbnamed's user key %q must exist in _default._default before migration", bobKey)

	// Flip dbnamed's opt-in. The MetadataStore is rebuilt as the dual-collection wrapper
	// with primary=_system._mobile, fallback=_default._default. dbnamed's own legacy keys
	// are still on fallback exactly as they would be after a production opt-in upgrade.
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbNamedName, dbNamedCfg), http.StatusCreated)

	// Flush this node's applied config versions so the manual start passes the config-fully-applied
	// gate, then drive dbnamed's migration through the admin endpoint.
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp = rt.SendAdminRequest(http.MethodPost, "/"+dbNamedName+"/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Poll until the migration reaches a terminal state. maxPasses=16 of range scans
	// over a tiny bucket should be fast, but give the test a generous deadline.
	var finalBody string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := rt.SendAdminRequest(http.MethodGet, "/"+dbNamedName+"/_metadata_migration", "")
		finalBody = st.Body.String()
		terminal := strings.Contains(finalBody, `"status":"completed"`) ||
			strings.Contains(finalBody, `"status":"error"`) ||
			strings.Contains(finalBody, `"status":"stopped"`)
		assert.True(c, terminal, "migration should reach a terminal state — payload: %s", finalBody)
	}, 90*time.Second, 500*time.Millisecond)
	t.Logf("dbnamed final migration status: %s", finalBody)

	// Whatever the dispatcher decides about classification, the test must never cause
	// data loss in dbdefault — dbdefault's user MUST still be on _default._default and
	// MUST still be readable through dbdefault's admin API.
	exists, err = defaultDS.Exists(ctx, aliceKey)
	require.NoError(t, err)
	assert.True(t, exists, "dbdefault's user key %q must still exist after dbnamed's migration", aliceKey)

	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbDefaultName+"/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"alice"`,
		"dbdefault's user must still be readable through its own admin API after dbnamed's migration")

	// dbnamed's migration completes cleanly in a single pass. dbdefault's colocated legacy keys
	// carry a metadata prefix that doesn't match dbnamed's metadataID, so the dispatcher
	// classifies them as out-of-scope (DocsOutOfScope) and leaves them in place. They are NOT
	// treated as an unrecognised prefix (DocsUnknownPrefix) — that would keep remaining>0 and
	// stall the run until the maxPasses=16 give-up.
	assert.Contains(t, finalBody, `"status":"completed"`,
		"dbnamed's migration should complete cleanly — dbdefault's colocated legacy keys are "+
			"classified out-of-scope, not as an unrecognised prefix that would stall the run. "+
			"Final payload: %s", finalBody)
	assert.NotContains(t, finalBody, `"status":"error"`,
		"dbnamed's migration should not error — its own keys migrate and the sibling DB's "+
			"out-of-scope keys are skipped. Final payload: %s", finalBody)
}

// TestMetadataMigrationConcurrentDefaultAndNamedDB migrates two databases that share one bucket -
// a legacy default-metadataID DB on _default._default (unprefixed keys, e.g. `_sync:user:alice`)
// and a namespaced DB on a named collection (m_<id>:-prefixed keys, e.g. `_sync:user:m_<id>:bob`) -
// at the same time, each triggered through its own POST /{db}/_metadata_migration?action=start in
// its own goroutine.
//
// Both migrations scan and drain the shared _default._default fallback concurrently.
func TestMetadataMigrationConcurrentDefaultAndNamedDB(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	const namedCollection = "sg_test_0"
	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	defer func() {
		assert.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	}()

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	// dbdefault: → metadataID="_default"
	const dbDefaultName = "dbdefault"
	dbDefaultCfg := rt.NewDbConfig()
	dbDefaultCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbDefaultCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{base.DefaultCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbDefaultName, dbDefaultCfg), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/"+dbDefaultName+"/_user/alice",
		`{"name":"alice","password":"letmein","admin_channels":["public"]}`), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/"+dbDefaultName+"/_role/admins",
		`{"name":"admins","admin_channels":["public"]}`), http.StatusCreated)

	// dbnamed: → namespaced metadataID
	const dbNamedName = "dbnamed"
	dbNamedCfg := rt.NewDbConfig()
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbNamedCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{namedCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbNamedName, dbNamedCfg), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/"+dbNamedName+"/_user/bob",
		`{"name":"bob","password":"letmein","admin_channels":["public"]}`), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/"+dbNamedName+"/_role/readers",
		`{"name":"readers","admin_channels":["public"]}`), http.StatusCreated)

	// Confirm the topology matches the premise: distinct metadataIDs, the default one for dbdefault.
	dbDefaultCtx, err := rt.ServerContext().GetDatabase(ctx, dbDefaultName)
	require.NoError(t, err)
	require.Equal(t, base.DefaultMetadataID, dbDefaultCtx.Options.MetadataID, "dbdefault must use the legacy default metadataID")
	dbNamedCtx, err := rt.ServerContext().GetDatabase(ctx, dbNamedName)
	require.NoError(t, err)
	require.NotEqual(t, base.DefaultMetadataID, dbNamedCtx.Options.MetadataID, "dbnamed must have a non-default metadataID")
	require.NotEmpty(t, dbNamedCtx.Options.MetadataID)

	defaultDocKeys := []string{
		base.DefaultMetadataKeys.UserKey("alice"),
		base.DefaultMetadataKeys.RoleKey("admins"),
	}
	namedDocKeys := []string{
		dbNamedCtx.MetadataKeys.UserKey("bob"),
		dbNamedCtx.MetadataKeys.RoleKey("readers"),
	}
	allDocKeys := append(append([]string{}, defaultDocKeys...), namedDocKeys...)

	fallback := tb.Bucket.DefaultDataStore(ctx)
	for _, k := range allDocKeys {
		exists, existsErr := fallback.Exists(ctx, k)
		require.NoError(t, existsErr)
		require.Truef(t, exists, "%q must exist in _default._default before migration", k)
	}
	base.RequireDocsVisibleToRangeScan(t, fallback, allDocKeys)

	// Flip both DBs' opt-in
	dbDefaultCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbDefaultName, dbDefaultCfg), http.StatusCreated)
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbNamedName, dbNamedCfg), http.StatusCreated)

	// Flush this node's applied config versions so the manual starts pass the config-fully-applied gate.
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())

	// Kick both migrations off at the same time, each via its own admin endpoint POST in its own
	// goroutine
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp := rt.SendAdminRequest(http.MethodPost, "/"+dbDefaultName+"/_metadata_migration?action=start", "")
		rest.RequireStatus(t, resp, http.StatusOK)
	}()
	go func() {
		defer wg.Done()
		resp := rt.SendAdminRequest(http.MethodPost, "/"+dbNamedName+"/_metadata_migration?action=start", "")
		rest.RequireStatus(t, resp, http.StatusOK)
	}()
	wg.Wait()

	// Both per-DB managers must reach completed
	defaultResp := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbDefaultName)
	namedResp := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbNamedName)

	assert.Zero(t, defaultResp.DocsFailed, "dbdefault migration must have no per-doc failures")
	assert.Zero(t, namedResp.DocsFailed, "dbnamed migration must have no per-doc failures")
	// Each DB migrates only its own principals (user + role) + other metadata documents; the other's are out-of-scope.
	assert.GreaterOrEqual(t, defaultResp.DocsProcessed, int64(len(defaultDocKeys)), "dbdefault should migrate its own principals")
	assert.GreaterOrEqual(t, namedResp.DocsProcessed, int64(len(namedDocKeys)), "dbnamed should migrate its own principals")

	// Every principal moved to _system._mobile and left _default._default, regardless of owner.
	systemDS, err := rt.Bucket().NamedDataStore(ctx, base.MobileSystemScopeAndCollectionName())
	require.NoError(t, err)
	for _, k := range allDocKeys {
		onPrimary, existsErr := systemDS.Exists(ctx, k)
		require.NoError(t, existsErr)
		assert.Truef(t, onPrimary, "%q must be migrated to _system._mobile", k)
		onFallback, existsErr := fallback.Exists(ctx, k)
		require.NoError(t, existsErr)
		assert.Falsef(t, onFallback, "%q must be removed from _default._default after migration", k)
	}

	// Both DBs' principals remain functional through their own admin APIs
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/"+dbDefaultName+"/_user/alice", ""), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/"+dbNamedName+"/_user/bob", ""), http.StatusOK)

	// check the bucket level doc for migration, assert that both db's are in completed state and bootstrap migration is done.
	conn := rt.ServerContext().BootstrapContext.Connection
	bucketName := rt.Bucket().GetName()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, _, statusErr := conn.GetMetadataMigrationStatus(ctx, bucketName)
		require.NoError(c, statusErr)
		require.NotNil(c, status)
		for _, id := range []string{base.DefaultMetadataID, dbNamedCtx.Options.MetadataID} {
			entry, ok := status.Databases[id]
			require.Truef(c, ok, "status doc must have a per-DB entry for %q", id)
			assert.Equalf(c, base.MigrationStateComplete, entry.State, "per-DB migration entry for %q should be complete", id)
		}
		assert.Equal(c, base.MigrationStateComplete, status.Bootstrap.State, "bucket bootstrap migration should be complete once both DBs finish")
	}, 30*time.Second, 200*time.Millisecond)
}

// TestMetadataMigrationMovesOwnedSyncFunctionDocs verifies that a per-DB migration moves only
// this database's sync-function ("syncdata") doc and leaves a sibling DB's untouched. Unlike the
// rest of SG's metadata these docs are keyed by groupID + scope.collection rather than metadataID
// (see base.CollectionSyncFunctionKeyWithGroupID), so the dispatcher classifies them via the
// per-DB owned-collection set rather than the metadataID-prefix isOurs check.
//
// Two databases share one scope, each owning a different collection. db0's migration must move
// db0's syncdata doc to _system._mobile and leave db1's on _default._default. Migrating db1 then
// moves the second doc, leaving both on primary.
func TestMetadataMigrationMovesOwnedSyncFunctionDocs(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 2)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	const syncFn = `function(doc){channel(doc.channels);}`

	// dbConfigForCollection builds a single-collection DB config in legacy mode (opt-in=false)
	// with an explicit sync function, so the syncdata doc is written to _default._default at
	// creation — the pre-migration shape we want to exercise.
	dbConfigForCollection := func(coll sgbucket.DataStoreName) rest.DbConfig {
		cfg := rt.NewDbConfig()
		cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
		cfg.Scopes = rest.ScopesConfig{
			coll.ScopeName(): rest.ScopeConfig{
				Collections: rest.CollectionsConfig{
					coll.CollectionName(): {SyncFn: base.Ptr(syncFn)},
				},
			},
		}
		return cfg
	}

	rest.RequireStatus(t, rt.CreateDatabase("db0", dbConfigForCollection(dataStore1)), http.StatusCreated)
	rest.RequireStatus(t, rt.CreateDatabase("db1", dbConfigForCollection(dataStore2)), http.StatusCreated)

	db0Ctx, err := rt.ServerContext().GetDatabase(ctx, "db0")
	require.NoError(t, err)
	db1Ctx, err := rt.ServerContext().GetDatabase(ctx, "db1")
	require.NoError(t, err)

	// syncdata docs are keyed by groupID + scope.collection, not metadataID.
	syncFnKey0 := base.CollectionSyncFunctionKeyWithGroupID(db0Ctx.Options.GroupID, dataStore1.ScopeName(), dataStore1.CollectionName())
	syncFnKey1 := base.CollectionSyncFunctionKeyWithGroupID(db1Ctx.Options.GroupID, dataStore2.ScopeName(), dataStore2.CollectionName())
	require.NotEqual(t, syncFnKey0, syncFnKey1, "the two collections must produce distinct syncdata keys")

	fallback := tb.Bucket.DefaultDataStore(ctx)

	// Both syncdata docs land in _default._default in legacy mode — the precondition for the
	// migration to have anything to move.
	for _, k := range []string{syncFnKey0, syncFnKey1} {
		exists, err := fallback.Exists(ctx, k)
		require.NoError(t, err)
		require.True(t, exists, "syncdata doc %q must exist in _default._default before migration", k)
	}

	// migrateDB flips a DB's opt-in, drives the migration to completion via the admin API.
	migrateDB := func(dbName string, dbCtx *db.DatabaseContext, cfg rest.DbConfig, ownedKey string) *base.MetadataStore {

		cfg.UseSystemMobileMetadataCollection = base.Ptr(true)
		rest.RequireStatus(t, rt.UpsertDbConfig(dbName, cfg), http.StatusCreated)

		// Make sure the owned syncdata doc is visible to the range scan before we start, so the
		// first pass can't miss it and complete with nothing migrated.
		base.RequireDocsVisibleToRangeScan(t, fallback, []string{ownedKey})

		rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
		resp := rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_metadata_migration?action=start", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbName)

		reloaded, err := rt.ServerContext().GetDatabase(ctx, dbName)
		require.NoError(t, err)
		ms, ok := reloaded.MetadataStore.(*base.MetadataStore)
		require.True(t, ok, "%s MetadataStore must be the dual-collection wrapper after opt-in", dbName)
		return ms
	}

	// Migrate db0. Its syncdata doc moves to primary; db1's must stay on fallback.
	ms0 := migrateDB("db0", db0Ctx, dbConfigForCollection(dataStore1), syncFnKey0)

	primaryHas0, err := ms0.Primary().Exists(ctx, syncFnKey0)
	require.NoError(t, err)
	assert.True(t, primaryHas0, "db0's syncdata doc must be migrated to _system._mobile")
	fallbackHas0, err := fallback.Exists(ctx, syncFnKey0)
	require.NoError(t, err)
	assert.False(t, fallbackHas0, "db0's syncdata doc must be removed from _default._default after its migration")

	primaryHas1, err := ms0.Primary().Exists(ctx, syncFnKey1)
	require.NoError(t, err)
	assert.False(t, primaryHas1, "db1's syncdata doc must NOT be migrated by db0's run")
	fallbackHas1, err := fallback.Exists(ctx, syncFnKey1)
	require.NoError(t, err)
	assert.True(t, fallbackHas1, "db1's syncdata doc must remain on _default._default after db0's migration")

	// Migrate db1. Its syncdata doc now moves too, leaving both on primary.
	ms1 := migrateDB("db1", db1Ctx, dbConfigForCollection(dataStore2), syncFnKey1)

	primaryHas1, err = ms1.Primary().Exists(ctx, syncFnKey1)
	require.NoError(t, err)
	assert.True(t, primaryHas1, "db1's syncdata doc must be migrated to _system._mobile after its own migration")
	fallbackHas1, err = fallback.Exists(ctx, syncFnKey1)
	require.NoError(t, err)
	assert.False(t, fallbackHas1, "db1's syncdata doc must be removed from _default._default after its migration")
}

// TestMoveSyncDataDocumentForDefaultDB verifies that the sync function document for the default database is moved
// from the fallback collection to the primary collection when opting in to the system metadata collection.
func TestMoveSyncDataDocumentForDefaultDB(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbConfig.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{
				base.DefaultCollection: {SyncFn: base.Ptr(`function(doc){channel(doc.channels);}`)},
			},
		},
	}
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	fallback := tb.Bucket.DefaultDataStore(ctx)

	syncFnKey := base.CollectionSyncFunctionKeyWithGroupID(rt.ServerContext().Config.Bootstrap.ConfigGroupID, base.DefaultScope, base.DefaultCollection)
	// assert we find sync data doc on fallback before migration
	exists, err := fallback.Exists(ctx, syncFnKey)
	require.NoError(t, err)
	require.True(t, exists, "syncdata doc must exist in _default._default before migration")

	// Flip the opt-in and migrate.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)

	rt.ServerContext().ForceClusterCompatRefresh(t, ctx)
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	rt.WaitForMetadataMigrationStatus(db.BackgroundProcessStateCompleted)

	// assert the sync data doc is migrated to primary and removed from fallback
	dbCtx := rt.GetDatabase()
	ms, ok := dbCtx.MetadataStore.(*base.MetadataStore)
	require.True(t, ok, "MetadataStore must be the dual-collection wrapper after opt-in")
	primaryHasSyncData, err := ms.Primary().Exists(ctx, syncFnKey)
	require.NoError(t, err)
	assert.True(t, primaryHasSyncData, "syncdata doc must be migrated to _system._mobile")
	fallbackHasSyncData, err := fallback.Exists(ctx, syncFnKey)
	require.NoError(t, err)
	assert.False(t, fallbackHasSyncData, "syncdata doc must be removed from _default._default after migration")
}

// TestMetadataMigrationOptInRejectedWithViews verifies the DbConfig.validateVersion guard:
// system-scoped metadata relies on N1QL principal queries, so explicitly opting in while
// use_views is enabled is rejected at config-validation time rather than failing later at query
// time. Runs on any backing store — the config is rejected before the database starts.
func TestMetadataMigrationOptInRejectedWithViews(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UseViews = base.Ptr(true)
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)

	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "use_system_metadata_collection is not supported with use_views=true")
}

// TestMetadataMigrationListsPrincipalsAfterCompletion is a regression test for the
// post-completion principal-listing path. Single-principal reads (GET /{db}/_user/alice) are
// KV lookups the dual-collection wrapper handles, but *listing* runs an N1QL query. Before the
// fix the wrapper was passed straight to N1QLQueryWithStats once migration completed, which
// failed with "Cannot perform N1QL query on non-Couchbase bucket" because the wrapper is
// intentionally not an N1QLStore. The query functions now target the wrapper's primary
// collection directly once migration is complete.
//
// Couchbase-Server-only: system-scoped metadata is a GSI feature (principal listing under
// use_views was never supported with the dual store — and Rosmar only ever uses views), so this
// regression relies on the Couchbase Server (N1QL) test path.
func TestMetadataMigrationListsPrincipalsAfterCompletion(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("system-scoped metadata principal listing is a GSI/N1QL path; not supported under views (Rosmar)")
	}
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	// Legacy mode: principals (and the seq counter created alongside them) land in
	// _default._default, so the new-DB fast path does not fire and the migration runs.
	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", `{"name":"alice","password":"letmein","admin_channels":["public"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/bob", `{"name":"bob","password":"hunter2","admin_channels":["public"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/observer", `{"name":"observer","admin_channels":["public"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Opt in, flush this node's applied config version so the manual start passes the
	// config-fully-applied gate, then drive the migration to completion.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp = rt.UpsertDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_metadata_migration", "")
		body := st.Body.String()
		assert.NotContains(c, body, `"status":"error"`, "migration must not error — payload: %s", body)
		assert.Contains(c, body, `"status":"completed"`, "migration should reach completed — payload: %s", body)
	}, 30*time.Second, 200*time.Millisecond)

	// Regression check: after completion the principals live in _system._mobile and fallback
	// reads are disabled. Listing must query the primary and still return every principal.
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "alice", "alice must be listable after migration completes")
	assert.Contains(t, resp.Body.String(), "bob", "bob must be listable after migration completes")

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_role/", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "observer", "roles must be listable after migration completes")
}

// TestBootstrapTargetOmittedWhenNoCachedValue verifies that bootstrap_target is omitted from the
// /_cluster_info response when no database has been created on the bucket. Without a database, no
// _sync:registry exists, so probeRegistryLocation finds nothing authoritative and the per-bucket
// target cache stays empty — matching the window between node startup and the first DB creation.
func TestBootstrapTargetOmittedWhenNoCachedValue(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	var clusterInfoResponse rest.ClusterInfo
	resp := rt.SendAdminRequest(http.MethodGet, "/_cluster_info", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err := base.JSONUnmarshal(resp.BodyBytes(), &clusterInfoResponse)
	require.NoError(t, err)
	target := clusterInfoResponse.Buckets[rt.Bucket().GetName()].BootstrapTarget
	assert.Equal(t, "", target)
	assert.Empty(t, clusterInfoResponse.Buckets[rt.Bucket().GetName()].BootstrapTarget)
}

// TestMetadataMigrationEndToEndBucketComplete is a full end-to-end check of the bucket-level
// completion handoff: migrate a single database's metadata (a user doc), then assert that once
// it is the last database in the bucket to finish, PostMetadataMigrationCompleteFunc migrates
// the bucket bootstrap docs (registry/dbconfig/cfg) and the bucket-level bootstrap state in
// _sync:metadata_migration_status flips to complete.
func TestMetadataMigrationEndToEndBucketComplete(t *testing.T) {
	for _, useXattrConfig := range []bool{false, true} {
		t.Run(fmt.Sprintf("UseXattrConfig=%t", useXattrConfig), func(t *testing.T) {
			ctx := base.TestCtx(t)
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				PersistentConfig: true,
				UseXattrConfig:   useXattrConfig,
			})
			defer rt.Close()

			// Legacy mode so the user (and seq counter) land in _default._default and the migration runs.
			dbConfig := rt.NewDbConfig()
			dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
			resp := rt.CreateDatabase("db", dbConfig)
			rest.RequireStatus(t, resp, http.StatusCreated)

			var clusterInfoResponse rest.ClusterInfo
			resp = rt.SendAdminRequest(http.MethodGet, "/_cluster_info", "")
			rest.RequireStatus(t, resp, http.StatusOK)
			err := base.JSONUnmarshal(resp.BodyBytes(), &clusterInfoResponse)
			require.NoError(t, err)
			assert.Equal(t, "_default._default", clusterInfoResponse.Buckets[rt.Bucket().GetName()].BootstrapTarget)

			resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", `{"name":"alice","password":"letmein","admin_channels":["public"]}`)
			rest.RequireStatus(t, resp, http.StatusCreated)

			dbCtx := rt.GetDatabase()
			metadataID := dbCtx.Options.MetadataID
			require.NotEmpty(t, metadataID)
			bucketName := rt.Bucket().GetName()

			// Opt in, flush this node's applied config version so the manual start passes the
			// config-fully-applied gate, then drive the migration to completion.
			dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
			resp = rt.UpsertDbConfig("db", dbConfig)
			rest.RequireStatus(t, resp, http.StatusCreated)

			rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())

			// grab registry from default collection for later comparison
			defaultDS := rt.Bucket().DefaultDataStore(ctx)
			body, xattrs, _, err := defaultDS.GetWithXattrs(ctx, base.SGRegistryKey, []string{base.SyncXattrName})
			require.NoError(t, err)

			resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
			rest.RequireStatus(t, resp, http.StatusOK)
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				st := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_metadata_migration", "")
				body := st.Body.String()
				assert.NotContains(c, body, `"status":"error"`, "migration must not error — payload: %s", body)
				assert.Contains(c, body, `"status":"completed"`, "migration should reach completed — payload: %s", body)
			}, 30*time.Second, 200*time.Millisecond)

			// End-to-end assertion on the durable status doc: the per-DB entry is complete AND, because
			// this is the only (hence last) database in the bucket, the bucket bootstrap migration has
			// also completed.
			conn := rt.ServerContext().BootstrapContext.Connection
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				status, _, err := conn.GetMetadataMigrationStatus(ctx, bucketName)
				require.NoError(c, err)
				require.NotNil(c, status)
				entry, ok := status.Databases[metadataID]
				require.True(c, ok, "status doc must have a per-DB entry for %q", metadataID)
				assert.Equal(c, base.MigrationStateComplete, entry.State, "per-DB migration entry should be complete")
				assert.Equal(c, base.MigrationStateComplete, status.Bootstrap.State, "bucket bootstrap migration should be complete once the last DB finishes")
			}, 30*time.Second, 200*time.Millisecond)

			clusterInfoResponse = rest.ClusterInfo{}
			resp = rt.SendAdminRequest(http.MethodGet, "/_cluster_info", "")
			rest.RequireStatus(t, resp, http.StatusOK)
			err = base.JSONUnmarshal(resp.BodyBytes(), &clusterInfoResponse)
			require.NoError(t, err)
			assert.Equal(t, "_system._mobile", clusterInfoResponse.Buckets[rt.Bucket().GetName()].BootstrapTarget)

			// assert registry is moved and correctly stored in the new location with the same body and xattrs
			systemDS, err := rt.Bucket().NamedDataStore(ctx, base.MobileSystemScopeAndCollectionName())
			require.NoError(t, err)
			migratedBody, migratedXattrs, _, err := systemDS.GetWithXattrs(ctx, base.SGRegistryKey, []string{base.SyncXattrName})
			require.NoError(t, err)
			assert.JSONEq(t, string(body), string(migratedBody), "registry body must be unchanged by the migration")
			// The bootstrap config only lives in the _sync xattr under CouchbaseCluster +
			// XattrBootstrapPersistence (UseXattrConfig). RosmarCluster has no configPersistence and
			// always stores bootstrap config in the body, so the xattr is empty there regardless of
			// UseXattrConfig — only assert on it when xattr persistence is genuinely in use.
			if useXattrConfig && !base.UnitTestUrlIsWalrus() {
				assert.JSONEq(t, string(xattrs[base.SyncXattrName]), string(migratedXattrs[base.SyncXattrName]), "registry xattrs must be unchanged by the migration")
			}
			// assert that registry is removed from the old location
			_, _, _, err = defaultDS.GetWithXattrs(ctx, base.SGRegistryKey, []string{base.SyncXattrName})
			require.Error(t, err)
			assert.True(t, base.IsDocNotFoundError(err), "registry must be removed from the old location after migration")
		})
	}
}

// TestMetadataMigrationEndToEndBucketBootstrapDocsContent asserts on the full set of bucket-global bootstrap
// docs MigrateBootstrapDocs moves. For every such doc it captures the raw body
// and _sync xattr in _default._default before migration, then after migration asserts the doc in
// _system._mobile is identical and that the fallback copy is fully removed.
func TestMetadataMigrationEndToEndBucketBootstrapDocsContent(t *testing.T) {
	for _, useXattrConfig := range []bool{false, true} {
		t.Run(fmt.Sprintf("UseXattrConfig=%t", useXattrConfig), func(t *testing.T) {
			ctx := base.TestCtx(t)
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				PersistentConfig: true,
				UseXattrConfig:   useXattrConfig,
			})
			defer rt.Close()

			// Legacy mode so the bootstrap docs land in _default._default and the migration runs.
			dbConfig := rt.NewDbConfig()
			dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
			rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

			resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", `{"name":"alice","password":"letmein","admin_channels":["public"]}`)
			rest.RequireStatus(t, resp, http.StatusCreated)

			bucketName := rt.Bucket().GetName()
			metadataID := rt.GetDatabase().Options.MetadataID
			require.NotEmpty(t, metadataID)

			// Opt in and converge before snapshotting so the captured dbconfig reflects the opted-in config.
			dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
			rest.RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)
			rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())

			// Snapshot every bootstrap doc the migration will move, exactly as production enumerates them.
			// Capture raw body + _sync xattr from the fallback so the post-migration comparison is
			// independent of which persistence mode stores the config in the body vs the xattr.
			type docSnapshot struct {
				body      []byte
				syncXattr []byte
			}
			defaultDS := rt.Bucket().DefaultDataStore(ctx)
			keys := rt.ServerContext().BootstrapDocKeysToMigrate(t, ctx, bucketName)
			require.NotEmpty(t, keys)
			sourceDocs := make(map[string]docSnapshot)
			for _, key := range keys {
				body, xattrs, _, err := defaultDS.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
				if base.IsDocNotFoundError(err) {
					// Optional doc absent in this scenario (e.g. cbgt cfg keys without sharded import);
					// MigrateBootstrapDocs skips not-found docs too, so there is nothing to assert.
					continue
				}
				require.NoError(t, err, "reading source bootstrap doc %q", key)
				sourceDocs[key] = docSnapshot{body: body, syncXattr: xattrs[base.SyncXattrName]}
			}
			// Registry plus the db's config doc must always be present and migrated.
			require.Contains(t, maps.Keys(sourceDocs), base.SGRegistryKey)
			require.GreaterOrEqual(t, len(sourceDocs), 2, "expected at least the registry and one dbconfig doc to migrate")

			// Drive the migration to completion.
			resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
			rest.RequireStatus(t, resp, http.StatusOK)
			conn := rt.ServerContext().BootstrapContext.Connection
			// wait for db migration and bootstrap migration to complete
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				status, _, err := conn.GetMetadataMigrationStatus(ctx, bucketName)
				require.NoError(c, err)
				require.NotNil(c, status)
				assert.Equal(c, base.MigrationStateComplete, status.Bootstrap.State, "bucket bootstrap migration should complete")
			}, 30*time.Second, 200*time.Millisecond)

			// Every captured bootstrap doc must now live in _system._mobile identical to its
			// fallback source, and be fully removed from _default._default.
			systemDS, err := rt.Bucket().NamedDataStore(ctx, base.MobileSystemScopeAndCollectionName())
			require.NoError(t, err)
			for key, source := range sourceDocs {
				migratedBody, migratedXattrs, _, err := systemDS.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
				require.NoError(t, err, "bootstrap doc %q must exist in _system._mobile after migration", key)
				assert.JSONEqf(t, string(source.body), string(migratedBody), "body of %q must be unchanged by the migration", key)
				// The bootstrap config only lives in the _sync xattr under CouchbaseCluster +
				// XattrBootstrapPersistence (UseXattrConfig). RosmarCluster has no configPersistence and
				// always stores bootstrap config in the body, so the xattr is empty there regardless of
				// UseXattrConfig — only assert on it when xattr persistence is genuinely in use.
				if useXattrConfig && !base.UnitTestUrlIsWalrus() {
					assert.JSONEqf(t, string(source.syncXattr), string(migratedXattrs[base.SyncXattrName]), "_sync xattr of %q must be unchanged by the migration", key)
				}

				_, _, _, err = defaultDS.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
				require.Error(t, err, "bootstrap doc %q must be removed from _default._default after migration", key)
				assert.True(t, base.IsDocNotFoundError(err), "bootstrap doc %q removal from fallback must be a not-found, got: %v", key, err)
			}
		})
	}
}

// TestMetadataMigrationRESTStartRejectedWithoutOptIn verifies a manual start via the admin API is
// rejected for a database that has not enabled use_system_metadata_collection — its MetadataStore
// is not a dual-collection wrapper, so there is nothing to migrate.
func TestMetadataMigrationRESTStartRejectedWithoutOptIn(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "use_system_metadata_collection is not enabled")
}

// TestMetadataMigrationRESTStartRejectedBeforeConfigApplied verifies the manual REST start path
// (POST /{db}/_metadata_migration?action=start) is gated on cluster config convergence, exactly
// like the auto-arming path (DatabaseContext.tryStartMetadataMigration). Starting while a peer
// node has not yet applied the opt-in config would let that peer keep writing metadata to
// _default._default while this node migrates it to _system._mobile — splitting or losing those
// writes. The POST must be rejected until every node has applied the config.
func TestMetadataMigrationRESTStartRejectedBeforeConfigApplied(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		// rtA and rtB share a group to simulate one two-node cluster, but it must be unique per
		// run: a fixed group lets a stale "db" registry entry (from a recycled-but-still-flushing
		// pool bucket, or a prior test's lingering config-poll/heartbeat) collide with this test's
		// create ("Duplicate database name").
		GroupID: base.Ptr(uuid.NewString()),
	}
	rtA := rest.NewRestTester(t, rtConfig)
	defer rtA.Close()
	rtB := rest.NewRestTester(t, rtConfig)
	defer rtB.Close()

	// Create db on node A with migration disabled, let node B pick it up, register both nodes.
	dbConfig := rtA.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	rest.RequireStatus(t, rtA.CreateDatabase("db", dbConfig), http.StatusCreated)
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)
	rtA.ServerContext().ForceClusterCompatRefresh(t, ctx)
	rtB.ServerContext().ForceClusterCompatRefresh(t, ctx)

	// Seed a legacy seq counter so the new-DB fast path doesn't auto-complete the migration (see
	// TestMetadataMigrationStartsAfterAllNodesApplyConfig for the rationale on using Incr).
	_, err := tb.Bucket.DefaultDataStore(ctx).Incr(ctx, rtA.GetDatabase().MetadataKeys.SyncSeqKey(), 1, 0, 0)
	require.NoError(t, err)

	// Opt in on node A and flush its registry entry. Node B has NOT applied the new config version.
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rtA.UpsertDbConfig("db", dbConfig), http.StatusCreated)
	rtA.ServerContext().ForceClusterCompatRefresh(t, ctx)

	// Sanity: the gate the handler consults reports not-applied, with node B outstanding.
	applied, missing, err := rtA.GetDatabase().ConfigFullyAppliedFunc(ctx)
	require.NoError(t, err)
	require.False(t, applied, "config must not be fully applied while node B is behind")
	require.Contains(t, missing, rtB.ServerContext().NodeUID)

	// Manual REST start must be rejected while node B is behind, and migration must not start.
	resp := rtA.SendAdminRequest(http.MethodPost, "/db/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)
	assert.Contains(t, resp.Body.String(), "has been applied on all nodes")
	assert.Equal(t, db.BackgroundProcessState(""), rtA.GetDatabase().MetadataMigrationManager.GetRunState(),
		"migration must not start via the REST API while a peer node has not applied the opt-in config")

	// Node B applies the config; the cluster has now converged.
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)
	rtB.ServerContext().ForceClusterCompatRefresh(t, ctx)

	applied, _, err = rtA.GetDatabase().ConfigFullyAppliedFunc(ctx)
	require.NoError(t, err)
	require.True(t, applied, "config should be fully applied once node B has applied the new version")

	// Manual REST start now succeeds.
	resp = rtA.SendAdminRequest(http.MethodPost, "/db/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
}

// siblingMigrationKeys are the on-disk metadata keys produced by one database's principals,
// used to assert post-migration placement (which collection each key ends up in).
type siblingMigrationKeys struct {
	user      string
	userEmail string
	role      string
	session   string
	dcpCk     string // versioned import checkpoint: DCPVersionedCheckpointPrefix(group, version)
	dcpCkBg   string // non-versioned checkpoint as background processes write: DCPCheckpointPrefix(group)
}

func (k siblingMigrationKeys) all() []string {
	return []string{k.user, k.userEmail, k.role, k.session, k.dcpCk, k.dcpCkBg}
}

// seedSiblingDBMetadata creates a user (with an email), a role and a session for dbName, then
// seeds a DCP checkpoint doc on the fallback. It returns the resulting on-disk metadata keys,
// built from the database's OWN MetadataKeys so they are correct for both the default
// (unprefixed) and namespaced (`<id>:`) families. Together these cover all five inverted
// families the sibling-exclusion fix scopes (user / useremail / role / session / dcp_ck).
//
// The DCP checkpoint is seeded directly rather than driven by a live import feed: real feed
// checkpoint persistence is threshold/close-gated (see rest/importtest) and not deterministic
// while the DB stays up for migration.
func seedSiblingDBMetadata(t *testing.T, rt *rest.RestTester, fallback sgbucket.DataStore, dbName, user, role, email string) siblingMigrationKeys {
	t.Helper()
	ctx := rt.Context()

	resp := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/_user/"+user,
		fmt.Sprintf(`{"name":%q,"password":"letmein","email":%q,"admin_channels":["public"]}`, user, email))
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/_role/"+role,
		fmt.Sprintf(`{"name":%q,"admin_channels":["public"]}`, role))
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_session",
		fmt.Sprintf(`{"name":%q,"ttl":86400}`, user))
	rest.RequireStatus(t, resp, http.StatusOK)
	var sess struct {
		SessionID string `json:"session_id"`
	}
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &sess))
	require.NotEmpty(t, sess.SessionID, "admin session create must return a session_id")

	dbCtx, err := rt.ServerContext().GetDatabase(ctx, dbName)
	require.NoError(t, err)
	keys := dbCtx.MetadataKeys

	// Versioned import checkpoint (`_sync:dcp_ck:<group>:<version>:<vbno>`) — the import feed's shape.
	checkpointKey := fmt.Sprintf("%s%d", keys.DCPVersionedCheckpointPrefix(dbCtx.Options.GroupID, dbCtx.Options.ImportVersion), 42)
	_, err = fallback.AddRaw(ctx, checkpointKey, 0, []byte(`{"vbno":42}`))
	require.NoError(t, err, "seed versioned dcp_ck %s", checkpointKey)

	// Non-versioned checkpoint as background DCP processes (attachment compaction/migration,
	// resync) write them: DCPCheckpointPrefix(group) with a process suffix, no version segment.
	bgCheckpointKey := keys.DCPCheckpointPrefix(dbCtx.Options.GroupID) + ":sg:att_compaction:testcompact:mark:42"
	_, err = fallback.AddRaw(ctx, bgCheckpointKey, 0, []byte(`{"vbno":42}`))
	require.NoError(t, err, "seed background dcp_ck %s", bgCheckpointKey)

	return siblingMigrationKeys{
		user:      keys.UserKey(user),
		userEmail: keys.UserEmailKey(email),
		role:      keys.RoleKey(role),
		session:   keys.SessionKey(sess.SessionID),
		dcpCk:     checkpointKey,
		dcpCkBg:   bgCheckpointKey,
	}
}

func requireAllExist(t *testing.T, ctx context.Context, store sgbucket.DataStore, keys []string) {
	t.Helper()
	for _, key := range keys {
		exists, err := store.Exists(ctx, key)
		require.NoError(t, err, "checking %q in %s", key, store.GetName())
		assert.True(t, exists, "expected key %q to exist in %s", key, store.GetName())
	}
}

func requireNoneExist(t *testing.T, ctx context.Context, store sgbucket.DataStore, keys []string) {
	t.Helper()
	for _, key := range keys {
		exists, err := store.Exists(ctx, key)
		require.NoError(t, err, "checking %q in %s", key, store.GetName())
		assert.False(t, exists, "key %q must NOT exist in %s", key, store.GetName())
	}
}

// TestMetadataMigrationDefaultDBFirstThenNamedSiblingExclusion is the end-to-end guard for the
// sibling-misclassification when a default metadataID DB and a namespaced sibling share one bucket,
// and the DEFAULT DB migrates FIRST. With the registry-driven sibling list it must
// migrate only its own docs and leave the sibling's on _default._default — verified online
// through the admin API and the raw collections. Then the named DB migrates second and claims
// its own, with the default DB's docs left untouched.
func TestMetadataMigrationDefaultDBFirstThenNamedSiblingExclusion(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	const namedCollection = "sg_test_0"
	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	defer func() {
		assert.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	}()

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	defaultDS := rt.Bucket().DefaultDataStore(ctx)
	systemDS, err := rt.Bucket().NamedDataStore(ctx, base.MobileSystemScopeAndCollectionName())
	require.NoError(t, err)

	// dbdefault: only _default._default → metadataID "_default", unprefixed keys.
	const dbDefaultName = "dbdefault"
	dbDefaultCfg := rt.NewDbConfig()
	dbDefaultCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbDefaultCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{base.DefaultCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbDefaultName, dbDefaultCfg), http.StatusCreated)

	// dbnamed: only _default.sg_test_0 → namespaced metadataID. Legacy mode so its metadata
	// colocates in _default._default alongside dbdefault's, reproducing the pre-migration state.
	const dbNamedName = "dbnamed"
	dbNamedCfg := rt.NewDbConfig()
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbNamedCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{namedCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbNamedName, dbNamedCfg), http.StatusCreated)

	defaultKeys := seedSiblingDBMetadata(t, rt, defaultDS, dbDefaultName, "alice", "defaultadmins", "alice@example.com")
	namedKeys := seedSiblingDBMetadata(t, rt, defaultDS, dbNamedName, "bob", "namedadmins", "bob@example.com")

	// Sanity-check the topology the scenario depends on.
	dbDefaultCtx, err := rt.ServerContext().GetDatabase(ctx, dbDefaultName)
	require.NoError(t, err)
	require.Equal(t, base.DefaultMetadataID, dbDefaultCtx.Options.MetadataID, "dbdefault must use the legacy default metadataID")
	dbNamedCtx, err := rt.ServerContext().GetDatabase(ctx, dbNamedName)
	require.NoError(t, err)
	require.Equal(t, dbNamedName, dbNamedCtx.Options.MetadataID, "dbnamed must have a namespaced metadataID")

	// Precondition: every key for both DBs starts on the shared fallback, none on _system._mobile.
	requireAllExist(t, ctx, defaultDS, append(defaultKeys.all(), namedKeys.all()...))
	requireNoneExist(t, ctx, systemDS, append(defaultKeys.all(), namedKeys.all()...))

	// === migrate dbdefault FIRST ===
	dbDefaultCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbDefaultName, dbDefaultCfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp := rt.SendAdminRequest(http.MethodPost, "/"+dbDefaultName+"/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbDefaultName)
	assert.Zero(t, status.DocsFailed, "dbdefault migration must not fail any docs")
	assert.GreaterOrEqual(t, status.DocsOutOfScope, int64(len(namedKeys.all())),
		"dbnamed's colocated docs must be classified out-of-scope (not migrated, not unknown-prefix)")

	// dbdefault's own docs migrated to _system._mobile, gone from the fallback, readable online.
	requireAllExist(t, ctx, systemDS, defaultKeys.all())
	requireNoneExist(t, ctx, defaultDS, defaultKeys.all())
	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbDefaultName+"/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"alice"`, "dbdefault's user must remain readable after its own migration")

	// dbnamed's docs are untouched on the fallback, never reached _system._mobile,
	// and remain readable through dbnamed's own admin API.
	requireAllExist(t, ctx, defaultDS, namedKeys.all())
	requireNoneExist(t, ctx, systemDS, namedKeys.all())
	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbNamedName+"/_user/bob", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"bob"`, "dbnamed's user must be untouched by dbdefault's migration")

	// === migrate dbnamed SECOND ===
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbNamedName, dbNamedCfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp = rt.SendAdminRequest(http.MethodPost, "/"+dbNamedName+"/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status = rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbNamedName)
	assert.Zero(t, status.DocsFailed, "dbnamed migration must not fail any docs")

	// dbnamed's docs now migrated; dbdefault's docs remain on _system._mobile, untouched.
	requireAllExist(t, ctx, systemDS, namedKeys.all())
	requireNoneExist(t, ctx, defaultDS, namedKeys.all())
	requireAllExist(t, ctx, systemDS, defaultKeys.all())
	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbNamedName+"/_user/bob", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"bob"`)
}

// TestMetadataMigrationNamedDBFirstThenDefaultSiblingExclusion is the inverse migration order of
// TestMetadataMigrationDefaultDBFirstThenNamedSiblingExclusion: the NAMESPACED DB migrates FIRST
// and the default metadataID DB second. It proves order-independence and, critically, that the
// second migration does not disturb the first DB's already-migrated metadata:
//   - when dbnamed migrates first, it claims only its own docs and leaves dbdefault's on the
//     fallback (the namespaced direction never consulted the sibling list, but its docs must
//     still be not migrated);
//   - when dbdefault migrates second, its own (now-only) fallback docs move to _system._mobile,
//     and dbnamed's docs already sitting in _system._mobile are left untouched.
func TestMetadataMigrationNamedDBFirstThenDefaultSiblingExclusion(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	const namedCollection = "sg_test_0"
	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	defer func() {
		assert.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: namedCollection}))
	}()

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	defaultDS := rt.Bucket().DefaultDataStore(ctx)
	systemDS, err := rt.Bucket().NamedDataStore(ctx, base.MobileSystemScopeAndCollectionName())
	require.NoError(t, err)

	const dbDefaultName = "dbdefault"
	dbDefaultCfg := rt.NewDbConfig()
	dbDefaultCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbDefaultCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{base.DefaultCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbDefaultName, dbDefaultCfg), http.StatusCreated)

	const dbNamedName = "dbnamed"
	dbNamedCfg := rt.NewDbConfig()
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbNamedCfg.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{namedCollection: {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbNamedName, dbNamedCfg), http.StatusCreated)

	defaultKeys := seedSiblingDBMetadata(t, rt, defaultDS, dbDefaultName, "alice", "defaultadmins", "alice@example.com")
	namedKeys := seedSiblingDBMetadata(t, rt, defaultDS, dbNamedName, "bob", "namedadmins", "bob@example.com")

	dbDefaultCtx, err := rt.ServerContext().GetDatabase(ctx, dbDefaultName)
	require.NoError(t, err)
	require.Equal(t, base.DefaultMetadataID, dbDefaultCtx.Options.MetadataID, "dbdefault must use the legacy default metadataID")
	dbNamedCtx, err := rt.ServerContext().GetDatabase(ctx, dbNamedName)
	require.NoError(t, err)
	require.Equal(t, dbNamedName, dbNamedCtx.Options.MetadataID, "dbnamed must have a namespaced metadataID")

	requireAllExist(t, ctx, defaultDS, append(defaultKeys.all(), namedKeys.all()...))
	requireNoneExist(t, ctx, systemDS, append(defaultKeys.all(), namedKeys.all()...))

	// === migrate dbnamed FIRST ===
	dbNamedCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbNamedName, dbNamedCfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp := rt.SendAdminRequest(http.MethodPost, "/"+dbNamedName+"/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbNamedName)
	assert.Zero(t, status.DocsFailed, "dbnamed migration must not fail any docs")
	assert.GreaterOrEqual(t, status.DocsOutOfScope, int64(len(defaultKeys.all())),
		"dbdefault's colocated docs must be classified out-of-scope during dbnamed's migration")

	// dbnamed's own docs migrated; dbdefault's untouched on the fallback and readable.
	requireAllExist(t, ctx, systemDS, namedKeys.all())
	requireNoneExist(t, ctx, defaultDS, namedKeys.all())
	requireAllExist(t, ctx, defaultDS, defaultKeys.all())
	requireNoneExist(t, ctx, systemDS, defaultKeys.all())
	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbDefaultName+"/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"alice"`, "dbdefault's user must be untouched by dbnamed's migration")

	// === migrate dbdefault SECOND ===
	dbDefaultCfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig(dbDefaultName, dbDefaultCfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp = rt.SendAdminRequest(http.MethodPost, "/"+dbDefaultName+"/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status = rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, dbDefaultName)
	assert.Zero(t, status.DocsFailed, "dbdefault migration must not fail any docs")

	// dbdefault's docs now migrated; dbnamed's already-migrated docs remain in _system._mobile.
	requireAllExist(t, ctx, systemDS, defaultKeys.all())
	requireNoneExist(t, ctx, defaultDS, defaultKeys.all())
	requireAllExist(t, ctx, systemDS, namedKeys.all())
	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbDefaultName+"/_user/alice", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"alice"`)
	resp = rt.SendAdminRequest(http.MethodGet, "/"+dbNamedName+"/_user/bob", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), `"name":"bob"`, "dbnamed must remain readable after dbdefault's later migration")
}

func TestDeleteNonMigratedDbUnblocksBootstrapMigration(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	db1Cfg := rt.NewDbConfig()
	db1Cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	db1Cfg.Scopes = rest.ScopesConfig{
		dataStore1.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{dataStore1.CollectionName(): {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase("db1", db1Cfg), http.StatusCreated)

	db2Cfg := rt.NewDbConfig()
	db2Cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	db2Cfg.Scopes = rest.ScopesConfig{
		dataStore2.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{dataStore2.CollectionName(): {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase("db2", db2Cfg), http.StatusCreated)

	// Opt in for db1 and start migration
	db1Cfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig("db1", db1Cfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, "db1")

	// Delete db2
	resp = rt.SendAdminRequest(http.MethodDelete, "/db2/", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// force a recheck of pending bucket bootstrap migration, which takes place in config polling
	rt.ServerContext().RecheckPendingBucketMetadataMigrations(ctx)

	conn := rt.ServerContext().BootstrapContext.Connection
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		status, _, err := conn.GetMetadataMigrationStatus(ctx, tb.GetName())
		if !assert.NoError(c, err) {
			return
		}
		if !assert.NotNil(c, status) {
			return
		}
		assert.Equal(c, base.MigrationStateComplete, status.Bootstrap.State, "bucket bootstrap migration should be complete")
	}, 10*time.Second, 100*time.Millisecond)
}

// TestRecheckConvergesLocalCacheOnPeerCompletedMigration ensures when a node completes the bucket bootstrap migration,
// this node's status doc reads complete but its local "migration complete" cache (IsMigrationComplete) is updated too.
// RecheckPendingBucketMetadataMigrations should converge that cache so the node stops falling back
// to _default._default and stops re-doing the recheck work for that bucket every poll.
func TestRecheckConvergesLocalCacheOnPeerCompletedMigration(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	db1Cfg := rt.NewDbConfig()
	db1Cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	db1Cfg.Scopes = rest.ScopesConfig{
		dataStore1.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{dataStore1.CollectionName(): {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase("db1", db1Cfg), http.StatusCreated)

	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	db2Cfg := rt.NewDbConfig()
	db2Cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	db2Cfg.Scopes = rest.ScopesConfig{
		dataStore2.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{dataStore2.CollectionName(): {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase("db2", db2Cfg), http.StatusCreated)

	// Opt in for db1 and run its migration. db2 stays un-migrated, so this node never completes the
	// bucket migration on its own — its local IsMigrationComplete cache stays false. (db2 also
	// guarantees the recheck can't complete the bucket itself, isolating the test to the cache
	// convergence path.)
	db1Cfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig("db1", db1Cfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, "db1")

	conn := rt.ServerContext().BootstrapContext.Connection

	// Simulate a peer node winning the completion: stamp the bucket status doc to complete directly.
	// UpdateMetadataMigrationStatus only writes the doc — it does not touch this node's local cache,
	// so IsMigrationComplete is still false afterwards, exactly as it would be on a peer that didn't
	// run the completion itself.
	_, err = conn.UpdateMetadataMigrationStatus(ctx, tb.GetName(), func(s *base.MetadataMigrationStatus) error {
		s.Bootstrap.State = base.MigrationStateComplete
		return nil
	})
	require.NoError(t, err)
	require.False(t, conn.IsMigrationComplete(tb.GetName()), "precondition: local node should not have marked migration complete yet")

	// The recheck observes the completed status doc; it should converge the local cache.
	rt.ServerContext().RecheckPendingBucketMetadataMigrations(ctx)

	require.True(t, conn.IsMigrationComplete(tb.GetName()), "finding 2: recheck observing a peer-completed status doc should converge the local migration-complete cache")
}

// TestDeleteAllDbsCompletesPendingBootstrapMigration is similar to TestDeleteNonMigratedDbUnblocksBootstrapMigration
// except that it deletes *every* db (both the migrated db1 and the un-migrated db2) and asserts the same thing
func TestDeleteAllDbsCompletesPendingBootstrapMigration(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	dataStore1, err := tb.GetNamedDataStore(0)
	require.NoError(t, err)
	db1Cfg := rt.NewDbConfig()
	db1Cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	db1Cfg.Scopes = rest.ScopesConfig{
		dataStore1.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{dataStore1.CollectionName(): {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase("db1", db1Cfg), http.StatusCreated)

	dataStore2, err := tb.GetNamedDataStore(1)
	require.NoError(t, err)
	db2Cfg := rt.NewDbConfig()
	db2Cfg.UseSystemMobileMetadataCollection = base.Ptr(false)
	db2Cfg.Scopes = rest.ScopesConfig{
		dataStore2.ScopeName(): rest.ScopeConfig{
			Collections: rest.CollectionsConfig{dataStore2.CollectionName(): {}},
		},
	}
	rest.RequireStatus(t, rt.CreateDatabase("db2", db2Cfg), http.StatusCreated)

	// Opt in for db1 and start its migration. This stamps the bucket-level status doc with
	// Bootstrap.State == pending. The bucket cannot complete yet because db2 has not opted in.
	db1Cfg.UseSystemMobileMetadataCollection = base.Ptr(true)
	rest.RequireStatus(t, rt.UpsertDbConfig("db1", db1Cfg), http.StatusCreated)
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	resp := rt.SendAdminRequest(http.MethodPost, "/db1/_metadata_migration?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	rt.WaitForMetadataMigrationStatusForDB(db.BackgroundProcessStateCompleted, "db1")

	conn := rt.ServerContext().BootstrapContext.Connection

	// Sanity check: with db2 still present and un-migrated, the bucket bootstrap migration is pending.
	status, _, err := conn.GetMetadataMigrationStatus(ctx, tb.GetName())
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, base.MigrationStatePending, status.Bootstrap.State, "precondition: bucket migration should be pending while db2 is un-migrated")

	// Delete every database in the bucket — both the migrated db1 and the un-migrated db2 — so the
	// registry has no live entries left.
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/db2/", ""), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/db1/", ""), http.StatusOK)

	// Re-run the recheck on every tick (mirroring the config-polling cadence) and expect the bucket
	// migration to converge to complete now that nothing blocks it. It never does — this assertion
	// fails on the current code because maybeComplete bails at its len(expected) == 0 guard.
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		rt.ServerContext().RecheckPendingBucketMetadataMigrations(ctx)
		status, _, err := conn.GetMetadataMigrationStatus(ctx, tb.GetName())
		if !assert.NoError(c, err) {
			return
		}
		if !assert.NotNil(c, status) {
			return
		}
		assert.Equal(c, base.MigrationStateComplete, status.Bootstrap.State, "deleting all dbs should let the pending bucket migration complete")
	}, 5*time.Second, 100*time.Millisecond)
}
