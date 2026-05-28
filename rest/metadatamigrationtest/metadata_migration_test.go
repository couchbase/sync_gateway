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
