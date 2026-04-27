// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterCompatRootEndpoint(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Trigger a refresh now that a database/bucket exists
	ccm, ok := rt.ServerContext().ClusterCompat.(*clusterCompatManager)
	require.True(t, ok)
	ccm.Refresh(base.TestCtx(t))

	// Admin port should include cluster_compat_version
	resp := rt.SendAdminRequest(http.MethodGet, "/", "")
	RequireStatus(t, resp, http.StatusOK)

	var rootResp rootResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &rootResp)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion.String(), rootResp.ClusterCompatVersion)

	// Public port also shows cluster_compat_version by default (HideProductVersion is false)
	pubResp := rt.SendRequest(http.MethodGet, "/", "")
	RequireStatus(t, pubResp, http.StatusOK)
	var pubRootResp rootResponse
	err = base.JSONUnmarshal(pubResp.BodyBytes(), &pubRootResp)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion.String(), pubRootResp.ClusterCompatVersion)
}

func TestClusterCompatEndpoint(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Trigger a refresh now that a database/bucket exists
	ccm, ok := rt.ServerContext().ClusterCompat.(*clusterCompatManager)
	require.True(t, ok)
	ccm.Refresh(base.TestCtx(t))

	resp := rt.SendAdminRequest(http.MethodGet, "/_cluster_compat", "")
	RequireStatus(t, resp, http.StatusOK)

	var ccResp clusterCompatResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &ccResp)
	require.NoError(t, err)

	// Should have a cluster compat version matching this node's version
	require.NotNil(t, ccResp.ClusterCompatVersion)
	assert.Equal(t, base.NodeClusterCompatVersion, *ccResp.ClusterCompatVersion)

	// Should have at least one node (this node)
	assert.GreaterOrEqual(t, len(ccResp.Nodes), 1)

	// Should have at least one bucket with node registrations
	assert.GreaterOrEqual(t, len(ccResp.Buckets), 1)

	// Verify this node appears in at least one bucket
	nodeUUID := rt.ServerContext().NodeUUID
	foundNode := false
	for _, bucket := range ccResp.Buckets {
		if _, ok := bucket.Nodes[nodeUUID]; ok {
			foundNode = true
			break
		}
	}
	assert.True(t, foundNode, "This node's UUID should appear in at least one bucket's node registrations")
}

func TestClusterCompatEndpointNotOnPublicPort(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	resp := rt.SendRequest(http.MethodGet, "/_cluster_compat", "")
	assert.Equal(t, http.StatusNotFound, resp.Code, "/_cluster_compat should not be accessible on public port")
}

// TestRegisterNodeVersionCASRetry concurrently registers many nodes in the same bucket registry
// and verifies the CAS-retry path converges: every node ends up in the registry, and no caller
// sees an error. Serialized get+set without retry would lose writes under this load.
func TestRegisterNodeVersionCASRetry(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyConfig)

	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	const n = 10
	version := base.NewClusterCompatVersion(4, 0)
	var wg sync.WaitGroup
	errs := make([]error, n)
	base.AssertLogContains(t, "CAS mismatch registering node version", func() {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = bc.RegisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i), version)
			}(i)
		}
		wg.Wait()
	})
	for i, err := range errs {
		assert.NoError(t, err, "RegisterNodeVersion for node-%d", i)
	}

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		uuid := fmt.Sprintf("node-%d", i)
		assert.Contains(t, registry.Nodes, uuid, "node %s should be in registry after concurrent registration", uuid)
	}
}

// TestDeregisterNodeVersionCASRetry concurrently deregisters many nodes from the same bucket
// registry and verifies the CAS-retry path converges: every node is removed.
func TestDeregisterNodeVersionCASRetry(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyConfig)

	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	const n = 10
	version := base.NewClusterCompatVersion(4, 0)
	for i := 0; i < n; i++ {
		_, err := bc.RegisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i), version)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	base.AssertLogContains(t, "CAS mismatch deregistering node", func() {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				bc.DeregisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i))
			}(i)
		}
		wg.Wait()
	})

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		uuid := fmt.Sprintf("node-%d", i)
		assert.NotContains(t, registry.Nodes, uuid, "node %s should have been deregistered", uuid)
	}
}

// TestClusterCompatMinVersionAcrossNodes seeds two synthetic node entries at differing
// versions into the bucket registry and verifies the manager surfaces the minimum across
// all registered nodes (this node + the two synthetics).
func TestClusterCompatMinVersionAcrossNodes(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	older := base.NewClusterCompatVersion(3, 5)
	newer := base.NewClusterCompatVersion(4, 0)
	_, err := bc.RegisterNodeVersion(ctx, bucketName, "synthetic-old", older)
	require.NoError(t, err)
	_, err = bc.RegisterNodeVersion(ctx, bucketName, "synthetic-new", newer)
	require.NoError(t, err)

	ccm, ok := rt.ServerContext().ClusterCompat.(*clusterCompatManager)
	require.True(t, ok)
	ccm.Refresh(ctx)

	got := ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	assert.Equal(t, older, *got, "ClusterCompatVersion should be the min across registered nodes")

	nodes := ccm.NodeVersions()
	assert.Equal(t, older, nodes["synthetic-old"])
	assert.Equal(t, newer, nodes["synthetic-new"])
	assert.Equal(t, base.NodeClusterCompatVersion, nodes[rt.ServerContext().NodeUUID])
}
