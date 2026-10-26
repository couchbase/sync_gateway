// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestConfigPollIdleKVOpsCounted runs ticks of the config polling loop on a node with no clients, and checks that the KV
// operations each tick makes are counted as idle. The exact counts of the bootstrap connection are checked by its own
// tests, so this checks a lower bound that holds on any backing store.
func TestConfigPollIdleKVOpsCounted(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			// Long enough that the background loop never ticks, so the test can run the loop body by itself.
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Hour)
		},
	})
	defer rt.Close()
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	sc := rt.ServerContext()

	for range 3 {
		// Refresh is rate limited, so clear its last run to make every tick write the registry heartbeat.
		func() {
			sc.ClusterCompat.mu.Lock()
			defer sc.ClusterCompat.mu.Unlock()
			sc.ClusterCompat.lastRefreshAt = time.Time{}
		}()

		idleKVOpsBefore := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value()
		sc.pollBootstrapConfig(rt.Context())
		// At least the registry and database config reads, the registry heartbeat read and write, and the migration
		// status read.
		require.GreaterOrEqual(t, base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value()-idleKVOpsBefore, int64(5))
	}
}
