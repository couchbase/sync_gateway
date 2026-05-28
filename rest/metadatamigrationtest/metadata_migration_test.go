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
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		GroupID:          base.Ptr("metadata_migration_cluster"),
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

	// Drive the migration directly through the admin endpoint — the auto-arming path
	// gates on ConfigFullyAppliedFunc which depends on cluster-compat polling cycles we
	// don't want to wait on for a single-node test. The POST returns the running status;
	// poll the GET endpoint until it reports completed (or error, which fails the test).
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
