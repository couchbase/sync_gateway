// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// dbConfigOperation is one admin API operation that TestConcurrentDbConfigOperationsAcrossNodes sends to a random node.
type dbConfigOperation struct {
	name     string
	weight   int
	allowed  []int // statuses expected while other nodes create, update and delete the same database
	sendFunc func(rt *RestTester, rng *rand.Rand) *TestResponse
}

func dbConfigOperations() []dbConfigOperation {
	return []dbConfigOperation{
		{
			name:    "create",
			weight:  2,
			allowed: []int{http.StatusCreated, http.StatusPreconditionFailed},
			sendFunc: func(rt *RestTester, _ *rand.Rand) *TestResponse {
				return rt.SendAdminRequest(http.MethodPut, "/db/", string(base.MustJSONMarshal(rt.TB(), rt.NewDbConfig())))
			},
		},
		{
			name:    "upsert config",
			weight:  4,
			allowed: []int{http.StatusCreated, http.StatusNotFound},
			sendFunc: func(rt *RestTester, rng *rand.Rand) *TestResponse {
				dbConfig := rt.NewDbConfig()
				dbConfig.RevsLimit = new(uint32(100 + rng.IntN(100)))
				return rt.SendAdminRequest(http.MethodPost, "/db/_config", string(base.MustJSONMarshal(rt.TB(), dbConfig)))
			},
		},
		{
			name:    "replace config",
			weight:  2,
			allowed: []int{http.StatusCreated, http.StatusNotFound},
			sendFunc: func(rt *RestTester, rng *rand.Rand) *TestResponse {
				dbConfig := rt.NewDbConfig()
				dbConfig.RevsLimit = new(uint32(100 + rng.IntN(100)))
				return rt.SendAdminRequest(http.MethodPut, "/db/_config", string(base.MustJSONMarshal(rt.TB(), dbConfig)))
			},
		},
		{
			name:    "update sync function",
			weight:  2,
			allowed: []int{http.StatusOK, http.StatusNotFound},
			sendFunc: func(rt *RestTester, rng *rand.Rand) *TestResponse {
				return rt.SendAdminRequest(http.MethodPut, "/db/_config/sync", fmt.Sprintf(`function(doc) { channel("ch%d"); }`, rng.IntN(10)))
			},
		},
		{
			name:    "offline",
			weight:  1,
			allowed: []int{http.StatusOK, http.StatusNotFound},
			sendFunc: func(rt *RestTester, _ *rand.Rand) *TestResponse {
				return rt.SendAdminRequest(http.MethodPost, "/db/_offline", "")
			},
		},
		{
			name:    "online",
			weight:  1,
			allowed: []int{http.StatusOK, http.StatusNotFound},
			sendFunc: func(rt *RestTester, _ *rand.Rand) *TestResponse {
				return rt.SendAdminRequest(http.MethodPost, "/db/_online", "")
			},
		},
		{
			name:    "delete",
			weight:  1,
			allowed: []int{http.StatusOK, http.StatusNotFound},
			sendFunc: func(rt *RestTester, _ *rand.Rand) *TestResponse {
				return rt.SendAdminRequest(http.MethodDelete, "/db/", "")
			},
		},
	}
}

// isExpectedStatus reports whether resp is one of the operation's allowed statuses.
func (op dbConfigOperation) isExpectedStatus(resp *TestResponse) bool {
	return slices.Contains(op.allowed, resp.Code)
}

// pickDbConfigOperation returns a random operation, chosen in proportion to its weight.
func pickDbConfigOperation(rng *rand.Rand, operations []dbConfigOperation) dbConfigOperation {
	totalWeight := 0
	for _, op := range operations {
		totalWeight += op.weight
	}
	n := rng.IntN(totalWeight)
	for _, op := range operations {
		if n < op.weight {
			return op
		}
		n -= op.weight
	}
	return operations[len(operations)-1]
}

// TestConcurrentDbConfigOperationsAcrossNodes sends random database config operations to random nodes of a
// cluster that polls configs, and therefore heartbeats into the registry, more often than a database reloads.
// It then checks that every node and the registry converge on one config. The seed is logged, but goroutine
// scheduling still varies between runs, so a failure is not guaranteed to reproduce.
func TestConcurrentDbConfigOperationsAcrossNodes(t *testing.T) {
	t.Skip("CBG-5935 config writes can panic when a concurrent config write reloads the database")
	if testing.Short() {
		t.Skip("skipping randomized concurrency test in short mode")
	}
	const (
		numNodes        = 3
		numWorkers      = 4
		opsPerWorker    = 15
		pollInterval    = 50 * time.Millisecond
		reloadDelay     = 150 * time.Millisecond // longer than pollInterval, as a Couchbase Server backed reload is in a real cluster
		convergeTimeout = 30 * time.Second
	)
	seed := rand.Uint64()
	t.Logf("seed: %d", seed)

	ctx := base.TestCtx(t)
	rtc := NewRestTesterCluster(t, &RestTesterClusterConfig{
		NumNodes: numNodes,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(pollInterval)
		},
		ConnectToBucketFn: func(ctx context.Context, spec base.BucketSpec, failFast bool) (base.Bucket, error) {
			time.Sleep(reloadDelay)
			return db.ConnectToBucket(ctx, spec, failFast)
		},
	})
	defer rtc.Close(ctx)

	operations := dbConfigOperations()
	var wg sync.WaitGroup
	for worker := range numWorkers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(seed, uint64(worker)))
			for i := range opsPerWorker {
				op := pickDbConfigOperation(rng, operations)
				node := rng.IntN(numNodes)
				resp := op.sendFunc(rtc.Node(node), rng)
				assert.Truef(t, op.isExpectedStatus(resp), "worker %d op %d: %s on node %d returned %d, expected one of %v: %s",
					worker, i, op.name, node, resp.Code, op.allowed, resp.Body.String())
			}
		})
	}
	wg.Wait()

	// Leave the database online with a known config, then wait for every node and the registry to agree on it.
	rtA := rtc.Node(0)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rtA.CreateDatabase("db", rtA.NewDbConfig())
		assert.Contains(c, []int{http.StatusCreated, http.StatusPreconditionFailed}, resp.Code, resp.Body.String())
		resp = rtA.UpsertDbConfig("db", rtA.NewDbConfig())
		assert.Equal(c, http.StatusCreated, resp.Code, resp.Body.String())
		resp = rtA.SendAdminRequest(http.MethodPost, "/db/_online", "")
		assert.Equal(c, http.StatusOK, resp.Code, resp.Body.String())
	}, convergeTimeout, 100*time.Millisecond)

	bootstrap := rtA.ServerContext().BootstrapContext
	bucketName := rtA.Bucket().GetName()
	groupID := rtA.ServerContext().Config.Bootstrap.ConfigGroupID
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var bucketConfig DatabaseConfig
		_, err := bootstrap.GetConfig(ctx, bucketName, groupID, "db", &bucketConfig)
		if !assert.NoError(c, err) {
			return
		}
		registry, err := bootstrap.getGatewayRegistry(ctx, bucketName)
		if !assert.NoError(c, err) {
			return
		}
		registryDb, ok := registry.getRegistryDatabase(groupID, "db")
		if assert.True(c, ok, "db missing from registry") {
			assert.Equal(c, bucketConfig.Version, registryDb.Version, "registry version")
			assert.Nil(c, registryDb.PreviousVersion, "registry has an unfinished update")
		}
		rtc.ForEachNode(func(rt *RestTester) {
			nodeConfig := rt.ServerContext().GetDatabaseConfig("db")
			if assert.NotNil(c, nodeConfig, "db not loaded on node") {
				assert.Equal(c, bucketConfig.Version, nodeConfig.Version, "node config version")
			}
		})
	}, convergeTimeout, 100*time.Millisecond)
}
