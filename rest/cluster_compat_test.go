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
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestClusterCompatRootEndpoint(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Trigger a refresh now that a database/bucket exists
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
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

// TestClusterCompatInStatusEndpoint verifies that /_status exposes the aggregated
// cluster compatibility version and this node's UID.
func TestClusterCompatInStatusEndpoint(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Trigger a refresh now that a database/bucket exists
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(base.TestCtx(t))

	resp := rt.SendAdminRequest(http.MethodGet, "/_status", "")
	RequireStatus(t, resp, http.StatusOK)

	var status Status
	err := base.JSONUnmarshal(resp.BodyBytes(), &status)
	require.NoError(t, err)

	assert.Equal(t, base.NodeClusterCompatVersion.String(), status.ClusterCompatVersion)
	assert.Equal(t, rt.ServerContext().NodeUID, status.NodeUID)
}

// TestClusterCompatNodesInClusterInfoRegistry verifies that the per-node version map
// is exposed via the bucket registry returned by /_cluster_info — registry data is the
// source of truth, /_cluster_info does not surface it as a top-level field.
func TestClusterCompatNodesInClusterInfoRegistry(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(base.TestCtx(t))

	resp := rt.SendAdminRequest(http.MethodGet, "/_cluster_info", "")
	RequireStatus(t, resp, http.StatusOK)

	var info ClusterInfo
	err := base.JSONUnmarshal(resp.BodyBytes(), &info)
	require.NoError(t, err)

	bucketName := rt.Bucket().GetName()
	bucket, ok := info.Buckets[bucketName]
	require.True(t, ok, "bucket %s should be in cluster info", bucketName)
	require.NotNil(t, bucket.Registry)

	nodeUID := rt.ServerContext().NodeUID
	registryNode, ok := bucket.Registry.Nodes[nodeUID]
	require.True(t, ok, "node %s should be in bucket registry", nodeUID)
	assert.Equal(t, base.NodeClusterCompatVersion, registryNode.Version)
}

// TestRegisterNodeVersionCASRetry concurrently registers many nodes in the same bucket registry
// and verifies the CAS-retry path converges: every node ends up in the registry, and no caller
// sees an error. Serialized get+set without retry would lose writes under this load.
func TestRegisterNodeVersionCASRetry(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	const n = 10
	version := base.NewClusterCompatVersion(4, 0)
	var eg errgroup.Group
	for i := 0; i < n; i++ {
		eg.Go(func() error {
			_, err := bc.RegisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i), version, time.Hour)
			return err
		})
	}
	assert.NoError(t, eg.Wait())

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
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	const n = 10
	version := base.NewClusterCompatVersion(4, 0)
	for i := 0; i < n; i++ {
		_, err := bc.RegisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i), version, time.Hour)
		require.NoError(t, err)
	}

	var eg errgroup.Group
	for i := 0; i < n; i++ {
		eg.Go(func() error {
			bc.DeregisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i))
			return nil
		})
	}
	require.NoError(t, eg.Wait())

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
	_, err := bc.RegisterNodeVersion(ctx, bucketName, "synthetic-old", older, time.Hour)
	require.NoError(t, err)
	_, err = bc.RegisterNodeVersion(ctx, bucketName, "synthetic-new", newer, time.Hour)
	require.NoError(t, err)

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	got := ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	assert.Equal(t, older, *got, "ClusterCompatVersion should be the min across registered nodes")

	nodes := ccm.NodeVersions()
	assert.Equal(t, older, nodes["synthetic-old"])
	assert.Equal(t, newer, nodes["synthetic-new"])
	assert.Equal(t, base.NodeClusterCompatVersion, nodes[rt.ServerContext().NodeUID])
}

// setNodeHeartbeatAt rewrites HeartbeatAt for a single node entry in the bucket registry.
// The registry write is CAS-checked so concurrent callers (e.g. the polling loop) can't
// silently clobber it.
func setNodeHeartbeatAt(t *testing.T, rt *RestTester, bucketName, nodeUID string, hb time.Time) {
	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	node, ok := registry.Nodes[nodeUID]
	require.True(t, ok, "node %s must exist before mutating its heartbeat", nodeUID)
	node.HeartbeatAt = hb
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))
}

// TestClusterCompatPruneStaleOnRefresh seeds a peer with an expired heartbeat and verifies
// that the next Refresh prunes it and removes it from the manager's cached node set.
func TestClusterCompatPruneStaleOnRefresh(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)

	// Seed a peer with a fresh heartbeat, then mutate it to be older than the expiry window.
	stalePeer := "stale-peer"
	_, err := bc.RegisterNodeVersion(ctx, bucketName, stalePeer, base.NewClusterCompatVersion(3, 5), time.Hour)
	require.NoError(t, err)
	setNodeHeartbeatAt(t, rt, bucketName, stalePeer, time.Now().Add(-2*ccm.heartbeatExpiry()))

	// Force-refresh past the rate limit.
	ccm.lastRefreshAt = time.Time{}
	ccm.Refresh(ctx)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.NotContains(t, registry.Nodes, stalePeer, "stale peer should have been pruned by Refresh")
	assert.Contains(t, registry.Nodes, rt.ServerContext().NodeUID, "self should remain after Refresh")
	assert.NotContains(t, ccm.NodeVersions(), stalePeer, "stale peer should not be in cached node set")
}

// TestClusterCompatPruneStaleOnRegisterBucket seeds a peer with an expired heartbeat and
// verifies that calling RegisterBucket through the startup path prunes it.
func TestClusterCompatPruneStaleOnRegisterBucket(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)

	stalePeer := "stale-peer"
	_, err := bc.RegisterNodeVersion(ctx, bucketName, stalePeer, base.NewClusterCompatVersion(3, 5), time.Hour)
	require.NoError(t, err)
	setNodeHeartbeatAt(t, rt, bucketName, stalePeer, time.Now().Add(-2*ccm.heartbeatExpiry()))

	// Drop bucket tracking so RegisterBucket re-runs the startup path.
	ccm.mu.Lock()
	delete(ccm.trackedBuckets, bucketName)
	ccm.mu.Unlock()
	ccm.RegisterBucket(ctx, bucketName)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.NotContains(t, registry.Nodes, stalePeer, "stale peer should have been pruned by RegisterBucket")
	assert.Contains(t, registry.Nodes, rt.ServerContext().NodeUID, "self should be present after RegisterBucket")
}

// TestClusterCompatPruneSelfNotPruned verifies that even with self's HeartbeatAt rewritten
// far in the past, RegisterNodeVersion retains self (since it's about to refresh self's
// heartbeat in the same write) — preventing the registry from going to an empty Nodes map.
func TestClusterCompatPruneSelfNotPruned(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()
	selfUID := rt.ServerContext().NodeUID

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)

	// Make sure self is registered, then make its heartbeat ancient.
	_, err := bc.RegisterNodeVersion(ctx, bucketName, selfUID, base.NodeClusterCompatVersion, time.Hour)
	require.NoError(t, err)
	staleTime := time.Now().Add(-100 * ccm.heartbeatExpiry())
	setNodeHeartbeatAt(t, rt, bucketName, selfUID, staleTime)

	// Re-register with a non-zero expiry. Self must survive and have a fresh heartbeat.
	registry, err := bc.RegisterNodeVersion(ctx, bucketName, selfUID, base.NodeClusterCompatVersion, ccm.heartbeatExpiry())
	require.NoError(t, err)
	require.Contains(t, registry.Nodes, selfUID)
	assert.True(t, registry.Nodes[selfUID].HeartbeatAt.After(staleTime), "self's heartbeat must have been refreshed")
}

// TestClusterCompatPruneFreshPeerNotPruned verifies that a peer with a fresh heartbeat
// survives a Refresh.
func TestClusterCompatPruneFreshPeerNotPruned(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)

	freshPeer := "fresh-peer"
	_, err := bc.RegisterNodeVersion(ctx, bucketName, freshPeer, base.NewClusterCompatVersion(3, 5), time.Hour)
	require.NoError(t, err)

	ccm.lastRefreshAt = time.Time{}
	ccm.Refresh(ctx)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.Contains(t, registry.Nodes, freshPeer, "fresh peer should not be pruned")
}

// TestClusterCompatHeartbeatExpiryConfigurable verifies the runtime expiry getter trusts
// the configured value (validation enforces the 2x floor — see TestStartupConfigValidate*)
// and falls back to defaultNodeHeartbeatExpiry when unset.
func TestClusterCompatHeartbeatExpiryConfigurable(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)

	// Unset → defaultNodeHeartbeatExpiry.
	rt.ServerContext().Config.Bootstrap.NodeHeartbeatExpiry = nil
	assert.Equal(t, defaultNodeHeartbeatExpiry, ccm.heartbeatExpiry())

	// Configured value is honored verbatim.
	want := 17 * ccm.refreshInterval()
	rt.ServerContext().Config.Bootstrap.NodeHeartbeatExpiry = base.NewConfigDuration(want)
	assert.Equal(t, want, ccm.heartbeatExpiry())
}

// TestClusterCompatRefreshIntervalUnclamped verifies refreshInterval returns the configured
// ConfigUpdateFrequency verbatim — no silent floor. The validator is responsible for rejecting
// pathological combinations (see TestStartupConfigNodeHeartbeatExpiryValidation), so the
// runtime must not disagree with what the validator approved.
func TestClusterCompatRefreshIntervalUnclamped(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)

	for _, d := range []time.Duration{500 * time.Millisecond, time.Second, 30 * time.Second} {
		rt.ServerContext().Config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(d)
		assert.Equal(t, d, ccm.refreshInterval(), "refreshInterval should return the configured value verbatim for %s", d)
	}
}
