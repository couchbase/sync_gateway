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
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestConfigPollIdleKVOpsCounted runs ticks of the config polling loop on a node with no clients, and checks that each
// tick reports the KV operations that the Leaky wrappers saw it make. The bootstrap connection reports its own op
// counts, which its own tests check, so this test checks that the counts get from there to the loop.
func TestConfigPollIdleKVOpsCounted(t *testing.T) {
	var recorder base.KVOpRecorder
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig:               true,
		LeakyBucketConfig:              &base.LeakyBucketConfig{KVOpCallback: recorder.DataStoreOp},
		LeakyBootstrapConnectionConfig: &base.LeakyBootstrapConnectionConfig{KVOpCallback: recorder.BootstrapOp},
		MutateStartupConfig: func(config *StartupConfig) {
			// Long enough that the background loop never ticks, so the test can run the loop body by itself.
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Hour)
		},
	})
	defer rt.Close()

	const dbName = "db"
	RequireStatus(t, rt.CreateDatabase(dbName, rt.NewDbConfig()), http.StatusCreated)

	sc := rt.ServerContext()
	ctx := rt.Context()
	// Peer observation reads the cbgt and SG Replicate cfg docs through the database's metadata store. Other
	// background work on the metadata store is counted elsewhere, so only cfg docs are compared here.
	cfgPrefix := rt.GetDatabase().MetadataKeys.SGCfgPrefix(sc.Config.Bootstrap.ConfigGroupID)

	for range 3 {
		// Refresh is rate limited, so clear its last run to make every tick write the registry heartbeat.
		func() {
			sc.ClusterCompat.mu.Lock()
			defer sc.ClusterCompat.mu.Unlock()
			sc.ClusterCompat.lastRefreshAt = time.Time{}
		}()

		recorder.Arm()
		kvOps := sc.pollBootstrapConfig(ctx)
		var pollOps []base.RecordedKVOp
		for _, op := range recorder.Disarm() {
			if strings.HasPrefix(op.Op, "bootstrap:") || strings.HasPrefix(op.Key, cfgPrefix) {
				pollOps = append(pollOps, op)
			}
		}

		require.NotEmpty(t, pollOps, "expected the config poll to make KV operations")
		require.Equal(t, base.SumKVOps(pollOps), kvOps, "config poll KV operation count does not match the operations made: %v", pollOps)
	}
}
