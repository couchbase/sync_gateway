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
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
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
	version := base.NodeClusterCompatVersion
	var eg errgroup.Group
	for i := 0; i < n; i++ {
		eg.Go(func() error {
			_, err := bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
				BucketName:      bucketName,
				NodeUID:         fmt.Sprintf("node-%d", i),
				Version:         version,
				HeartbeatExpiry: time.Hour,
				RatchetHWM:      true,
			})
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
	version := base.NodeClusterCompatVersion
	for i := 0; i < n; i++ {
		_, err := bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
			BucketName:      bucketName,
			NodeUID:         fmt.Sprintf("node-%d", i),
			Version:         version,
			HeartbeatExpiry: time.Hour,
			RatchetHWM:      true,
		})
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
	bucketName := rt.Bucket().GetName()

	older := base.NewClusterCompatVersion(3, 5)
	newer := base.NewClusterCompatVersion(4, 0)
	seedRegistryNode(t, rt, bucketName, "synthetic-old", older)
	seedRegistryNode(t, rt, bucketName, "synthetic-new", newer)

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
	seedRegistryNode(t, rt, bucketName, stalePeer, base.NewClusterCompatVersion(3, 5))
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
	seedRegistryNode(t, rt, bucketName, stalePeer, base.NewClusterCompatVersion(3, 5))
	setNodeHeartbeatAt(t, rt, bucketName, stalePeer, time.Now().Add(-2*ccm.heartbeatExpiry()))

	// Drop bucket tracking so RegisterBucket re-runs the startup path.
	ccm.mu.Lock()
	delete(ccm.trackedBuckets, bucketName)
	ccm.mu.Unlock()
	require.NoError(t, ccm.RegisterBucket(ctx, bucketName))

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
	_, err := bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:      bucketName,
		NodeUID:         selfUID,
		Version:         base.NodeClusterCompatVersion,
		HeartbeatExpiry: time.Hour,
		RatchetHWM:      true,
	})
	require.NoError(t, err)
	staleTime := time.Now().Add(-100 * ccm.heartbeatExpiry())
	setNodeHeartbeatAt(t, rt, bucketName, selfUID, staleTime)

	// Re-register with a non-zero expiry. Self must survive and have a fresh heartbeat.
	registry, err := bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:      bucketName,
		NodeUID:         selfUID,
		Version:         base.NodeClusterCompatVersion,
		HeartbeatExpiry: ccm.heartbeatExpiry(),
		RatchetHWM:      true,
	})
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
	seedRegistryNode(t, rt, bucketName, freshPeer, base.NewClusterCompatVersion(3, 5))

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

// seedRegistryNode writes a synthetic node entry into the bucket registry, bypassing the
// cluster-compat downgrade gate in RegisterNodeVersion so tests can seed peers at arbitrary
// versions (including versions below HWM, which RegisterNodeVersion would refuse). HWM is
// ratcheted up to track the min cluster compat across all registered nodes — same invariant
// as RegisterNodeVersion — so tests that depend on HWM bumping (e.g. via this seed call)
// continue to observe it.
func seedRegistryNode(t *testing.T, rt *RestTester, bucketName, nodeUID string, version base.ClusterCompatVersion) {
	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	if registry.Nodes == nil {
		registry.Nodes = make(map[string]*base.RegistryNode)
	}
	registry.Nodes[nodeUID] = &base.RegistryNode{
		Version:     version,
		HeartbeatAt: time.Now().UTC(),
	}
	ccv := minRegistryNodeClusterCompatVersion(registry.Nodes)
	if ccv.GreaterThan(registry.ClusterCompatVersionHWM) {
		registry.ClusterCompatVersionHWM = ccv
	}
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))
}

// TestClusterCompatDowngradeBlockedByLiveNewerPeer verifies that a node refuses to load a
// database when a peer in the bucket registry has a higher major.minor compat version with
// a fresh heartbeat.
func TestClusterCompatDowngradeBlockedByLiveNewerPeer(t *testing.T) {
	nodeVersion := base.NewClusterCompatVersion(4, 0)
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig:         true,
		nodeClusterCompatVersion: &nodeVersion,
	})
	defer rt.Close()

	bucketName := rt.Bucket().GetName()
	seedRegistryNode(t, rt, bucketName, "newer-peer", base.NewClusterCompatVersion(99, 9))

	cfg := rt.NewDbConfig()
	cfg.StartOffline = base.Ptr(true)
	resp := rt.CreateDatabase("db1", cfg)
	RequireStatus(t, resp, http.StatusInternalServerError)
	assert.Contains(t, resp.Body.String(), bucketName)
	assert.Contains(t, resp.Body.String(), "newer Sync Gateway cluster compat version")
}

// TestClusterCompatDowngradeAllowedSameOrOlderPeers verifies that a node loads cleanly when
// the registry only contains peers at the same or older major.minor compat versions.
func TestClusterCompatDowngradeAllowedSameOrOlderPeers(t *testing.T) {
	nodeVersion := base.NewClusterCompatVersion(4, 0)
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig:         true,
		nodeClusterCompatVersion: &nodeVersion,
	})
	defer rt.Close()

	bucketName := rt.Bucket().GetName()
	seedRegistryNode(t, rt, bucketName, "older-peer", base.NewClusterCompatVersion(0, 1))

	cfg := rt.NewDbConfig()
	cfg.StartOffline = base.Ptr(true)
	resp := rt.CreateDatabase("db1", cfg)
	RequireStatus(t, resp, http.StatusCreated)
}

// TestClusterCompatDowngradeEmptyRegistry verifies that creating a database against a fresh
// (empty) bucket succeeds and ratchets ClusterCompatVersionHWM up to the node's compat
// version. The ratchet is performed by Refresh once the database is online — drive it
// explicitly here rather than waiting for the periodic ticker.
func TestClusterCompatDowngradeEmptyRegistry(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bucketName := rt.Bucket().GetName()
	bc := rt.ServerContext().BootstrapContext

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM, "HWM should be ratcheted to node version after first refresh")
}

// TestClusterCompatDowngradeBlockedByPersistentHWM verifies the persistent floor: a bucket
// whose ClusterCompatVersionHWM has been ratcheted past this node's compat version blocks
// startup even when no live peer is present.
func TestClusterCompatDowngradeBlockedByPersistentHWM(t *testing.T) {
	nodeVersion := base.NewClusterCompatVersion(4, 0)
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig:         true,
		nodeClusterCompatVersion: &nodeVersion,
	})
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	registry.ClusterCompatVersionHWM = base.NewClusterCompatVersion(99, 9)
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))

	cfg := rt.NewDbConfig()
	cfg.StartOffline = base.Ptr(true)
	resp := rt.CreateDatabase("db1", cfg)
	RequireStatus(t, resp, http.StatusInternalServerError)
	assert.Contains(t, resp.Body.String(), "newer Sync Gateway cluster compat version")

	// _all_dbs?verbose=true must surface the rejected db with the cluster-compat error code,
	// so an admin can tell why a config from the bucket failed to load instead of it
	// silently disappearing from the listing.
	summaries := rt.ServerContext().allDatabaseSummaries()
	require.Len(t, summaries, 1)
	require.NotNil(t, summaries[0].DatabaseError)
	assert.Equal(t, "db1", summaries[0].DBName)
	assert.Equal(t, db.DatabaseClusterCompatVersionError, summaries[0].DatabaseError.Code)
}

// TestClusterCompatDowngradeHWMRatchets verifies that the cluster compat downgrade gate in
// RegisterNodeVersion rejects a lower-version registration when the bucket's HWM is higher,
// and that the rejected attempt does not lower the HWM.
func TestClusterCompatDowngradeHWMRatchets(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	require.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM)

	preserved := base.NewClusterCompatVersion(base.NodeClusterCompatVersion.Major+1, 0)
	registry.ClusterCompatVersionHWM = preserved
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))

	_, err = bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:      bucketName,
		NodeUID:         "lower-peer",
		Version:         base.NewClusterCompatVersion(0, 1),
		HeartbeatExpiry: time.Hour,
		RatchetHWM:      true,
	})
	require.Error(t, err, "lower-version registration must be rejected when HWM is higher")
	require.Contains(t, err.Error(), "newer Sync Gateway cluster compat version")

	registry, err = bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.Equal(t, preserved, registry.ClusterCompatVersionHWM, "HWM must not be lowered by a rejected registration")
}

// TestClusterCompatDowngradeHWMTracksMinAcrossNodes verifies that ClusterCompatVersionHWM is
// the minimum cluster compat version (across registered nodes) ever observed — i.e. the HWM
// of the cluster compat version, not of individual node versions. A higher node coexisting
// with a lower one must not drag the HWM up past the cluster's actual compat version.
func TestClusterCompatDowngradeHWMTracksMinAcrossNodes(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	// After auto-create db + first refresh, HWM == self version (only node).
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	require.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM)

	// Add a higher-version peer. Cluster compat is still min(self, higher) == self, so HWM
	// must not budge.
	higher := base.NewClusterCompatVersion(base.NodeClusterCompatVersion.Major+1, 0)
	_, err = bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:      bucketName,
		NodeUID:         "higher-peer",
		Version:         higher,
		HeartbeatExpiry: time.Hour,
		RatchetHWM:      true,
	})
	require.NoError(t, err)
	registry, err = bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM, "HWM must follow min cluster compat, not max node version")
}

// TestClusterCompatAppliedDBVersionTracked verifies that creating a database via the REST API
// records its config version in clusterCompatManager, and that the next Refresh stamps it
// into the bucket registry's RegistryNode.Databases map.
func TestClusterCompatAppliedDBVersionTracked(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	sc := rt.ServerContext()
	ccm := sc.ClusterCompat
	require.NotNil(t, ccm)

	bucketName := rt.Bucket().GetName()

	tracked := ccm.getAppliedDBVersionsForBucket(bucketName)
	require.Contains(t, tracked, "db", "applied version should be tracked after _applyConfig")
	assert.NotEmpty(t, tracked["db"], "tracked version should not be empty")

	ccm.mu.Lock()
	ccm.lastRefreshAt = time.Time{}
	ccm.mu.Unlock()
	ccm.Refresh(ctx)

	registry, err := sc.BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	node, ok := registry.Nodes[sc.NodeUID]
	require.True(t, ok, "self should be in registry")
	require.NotNil(t, node.Databases, "Databases map should be stamped in registry after Refresh")
	assert.Equal(t, tracked["db"], node.Databases["db"])
}

// TestClusterCompatAppliedDBVersionUpdatedByHandlePutDbConfig verifies that updating a
// database config via POST /{db}/_config (handlePutDbConfig) records the new config version
// in clusterCompatManager, and that it differs from the original create version.
func TestClusterCompatAppliedDBVersionUpdatedByHandlePutDbConfig(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	sc := rt.ServerContext()
	ccm := sc.ClusterCompat
	require.NotNil(t, ccm)

	bucketName := rt.Bucket().GetName()

	versionAfterCreate := ccm.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionAfterCreate, "version must be tracked after initial create")

	dbConfig := rt.NewDbConfig()
	dbConfig.AutoImport = base.Ptr(false)
	resp := rt.UpsertDbConfig("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	versionAfterUpdate := ccm.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionAfterUpdate, "version must be tracked after PUT config")
	assert.NotEqual(t, versionAfterCreate, versionAfterUpdate, "version should change after config update")
}

// TestClusterCompatAppliedDBVersionUpdatedByUpdateConfigAndReloadDatabase verifies that
// taking a database offline via POST /{db}/_offline (which calls updateConfigAndReloadDatabase)
// records the updated config version in clusterCompatManager.
func TestClusterCompatAppliedDBVersionUpdatedByUpdateConfigAndReloadDatabase(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	sc := rt.ServerContext()
	ccm := sc.ClusterCompat
	require.NotNil(t, ccm)

	bucketName := rt.Bucket().GetName()

	versionAfterCreate := ccm.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionAfterCreate, "version must be tracked after initial create")

	resp := rt.SendAdminRequest(http.MethodPost, "/db/_offline", "")
	RequireStatus(t, resp, http.StatusOK)

	versionAfterOffline := ccm.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionAfterOffline, "version must be tracked after offline")
	assert.NotEqual(t, versionAfterCreate, versionAfterOffline, "version should change after taking db offline")
}

// TestIsConfigFullyApplied is a truth-table test covering the edge cases of
// GatewayRegistry.IsConfigFullyApplied.
func TestIsConfigFullyApplied(t *testing.T) {
	const (
		group   = "group-1"
		dbName  = "mydb"
		version = "2-abc"
	)

	freshHeartbeat := time.Now().UTC()
	staleHeartbeat := time.Now().UTC().Add(-2 * time.Hour)

	type testCase struct {
		name        string
		nodes       map[string]*base.RegistryNode
		pruneExpiry time.Duration // if >0, run pruneStaleNodes before the check
		wantAcked   bool
		wantMissing []string
		wantErr     error
	}

	tests := []testCase{
		{
			name:    "no alive nodes in config group",
			nodes:   map[string]*base.RegistryNode{},
			wantErr: ErrNoEligibleAckers,
		},
		{
			name: "all alive nodes acked at version",
			nodes: map[string]*base.RegistryNode{
				"node-1": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: version}},
				"node-2": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: version}},
			},
			wantAcked: true,
		},
		{
			name: "one alive node missing the DB entry",
			nodes: map[string]*base.RegistryNode{
				"node-1": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: version}},
				"node-2": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{}},
			},
			wantMissing: []string{"node-2"},
		},
		{
			name: "one alive node acked at wrong version",
			nodes: map[string]*base.RegistryNode{
				"node-1": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: version}},
				"node-2": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: "1-old"}},
			},
			wantMissing: []string{"node-2"},
		},
		{
			name: "node with different ConfigGroupID is ignored",
			nodes: map[string]*base.RegistryNode{
				"node-1": {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: version}},
				"node-2": {ConfigGroupID: "other-group", HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: "1-old"}},
			},
			wantAcked: true,
		},
		{
			name: "node with expired heartbeat pruned before check",
			nodes: map[string]*base.RegistryNode{
				"self":       {ConfigGroupID: group, HeartbeatAt: freshHeartbeat, Databases: map[string]string{dbName: version}},
				"stale-node": {ConfigGroupID: group, HeartbeatAt: staleHeartbeat, Databases: map[string]string{dbName: "1-old"}},
			},
			pruneExpiry: time.Hour,
			wantAcked:   true,
		},
	}

	ctx := base.TestCtx(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry := &GatewayRegistry{Nodes: tc.nodes}

			if tc.pruneExpiry > 0 {
				pruneStaleNodes(registry.Nodes, "self", tc.pruneExpiry)
			}

			acked, missing, err := registry.IsConfigFullyApplied(ctx, group, dbName, version)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.False(t, acked)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantAcked, acked)
			assert.ElementsMatch(t, tc.wantMissing, missing)
		})
	}
}

// refreshAndGetRegistry is a test helper that forces a heartbeat refresh for a node's
// clusterCompatManager and returns a fresh registry from the bucket.
func refreshAndGetRegistry(t *testing.T, rt *RestTester, bucketName string) *GatewayRegistry {
	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	ccm.mu.Lock()
	ccm.lastRefreshAt = time.Time{}
	ccm.mu.Unlock()
	ccm.Refresh(ctx)

	registry, err := rt.ServerContext().BootstrapContext.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	return registry
}

// TestIsConfigFullyAppliedTwoNodeConvergence creates a database on node A, polls
// node B to pick it up, refreshes both nodes' heartbeats, and asserts that
// IsConfigFullyApplied returns true once both nodes have applied the config.
func TestIsConfigFullyAppliedTwoNodeConvergence(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	}

	rtA := NewRestTester(t, rtConfig)
	defer rtA.Close()
	rtB := NewRestTester(t, rtConfig)
	defer rtB.Close()

	bucketName := tb.GetName()

	dbConfig := rtA.NewDbConfig()
	resp := rtA.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	_ = refreshAndGetRegistry(t, rtA, bucketName)
	_ = refreshAndGetRegistry(t, rtB, bucketName)

	dbVersion := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, dbVersion)

	registry := refreshAndGetRegistry(t, rtA, bucketName)
	acked, missing, err := registry.IsConfigFullyApplied(ctx, groupID, "db", dbVersion)
	require.NoError(t, err)
	assert.True(t, acked, "both nodes should have acked; missing: %v (registry nodes: %v)", missing, registry.Nodes)
	assert.Empty(t, missing)
}

// TestIsConfigFullyAppliedStaleNodePruned verifies that when node B's heartbeat
// expires it is pruned from the registry, so IsConfigFullyApplied passes on
// node A's view even though B never acked the latest version.
func TestIsConfigFullyAppliedStaleNodePruned(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	}

	rtA := NewRestTester(t, rtConfig)
	defer rtA.Close()
	rtB := NewRestTester(t, rtConfig)
	defer rtB.Close()

	bucketName := tb.GetName()

	dbConfig := rtA.NewDbConfig()
	resp := rtA.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	refreshAndGetRegistry(t, rtA, bucketName)
	refreshAndGetRegistry(t, rtB, bucketName)

	// use rtA as we never prune our own node entry so running as rtB will not prune the stale node
	setNodeHeartbeatAt(t, rtA, bucketName, rtB.ServerContext().NodeUID, time.Now().Add(-2*rtA.ServerContext().ClusterCompat.heartbeatExpiry()))

	registry := refreshAndGetRegistry(t, rtA, bucketName)
	assert.NotContains(t, registry.Nodes, rtB.ServerContext().NodeUID, "stale node B should be pruned")

	dbVersion := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	acked, missing, err := registry.IsConfigFullyApplied(ctx, groupID, "db", dbVersion)
	require.NoError(t, err)
	assert.True(t, acked, "gate should pass after stale node is pruned; missing: %v", missing)
	assert.Empty(t, missing)
}

// TestIsConfigFullyAppliedNodeRestart verifies that after node B restarts and
// picks up the config via polling, it re-acks and IsConfigFullyApplied remains true.
func TestIsConfigFullyAppliedNodeRestart(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	}

	rtA := NewRestTester(t, rtConfig)
	defer rtA.Close()

	bucketName := tb.GetName()

	dbConfig := rtA.NewDbConfig()
	resp := rtA.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	dbVersion := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, dbVersion)

	rtB := NewRestTester(t, rtConfig)
	defer rtB.Close()

	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	refreshAndGetRegistry(t, rtB, bucketName)

	registry := refreshAndGetRegistry(t, rtA, bucketName)
	acked, missing, err := registry.IsConfigFullyApplied(ctx, groupID, "db", dbVersion)
	require.NoError(t, err)
	assert.True(t, acked, "restarted node B should have re-acked; missing: %v", missing)
	assert.Empty(t, missing)
}

// TestIsConfigFullyAppliedCrossGroupIgnored verifies that a node in a different
// config group with the same database name does not contribute to or block the
// gate for the original group.
func TestIsConfigFullyAppliedCrossGroupIgnored(t *testing.T) {
	base.TestRequiresCollections(t)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupA := t.Name() + "-A"
	groupB := t.Name() + "-B"
	bucketName := tb.GetName()

	twoCollectionScopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(twoCollectionScopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{Collections: map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{Collections: map[string]*CollectionConfig{collection2Name: {}}}}

	rtA := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupA,
	})
	defer rtA.Close()

	dbConfigA := rtA.NewDbConfig()
	dbConfigA.Scopes = collection1ScopesConfig
	resp := rtA.CreateDatabase("db1", dbConfigA)
	RequireStatus(t, resp, http.StatusCreated)
	refreshAndGetRegistry(t, rtA, bucketName)

	dbVersion := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db1"]
	require.NotEmpty(t, dbVersion)

	rtB := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupB,
	})
	defer rtB.Close()

	dbConfigC := rtB.NewDbConfig()
	dbConfigC.Scopes = collection2ScopesConfig
	resp = rtB.CreateDatabase("db1", dbConfigC)
	RequireStatus(t, resp, http.StatusCreated)
	refreshAndGetRegistry(t, rtB, bucketName)

	registry := refreshAndGetRegistry(t, rtA, bucketName)
	acked, missing, err := registry.IsConfigFullyApplied(ctx, groupA, "db1", dbVersion)
	require.NoError(t, err)
	assert.True(t, acked, "cross-group node should not block; missing: %v", missing)
	assert.Empty(t, missing)

	// assert both db's are there, to verify the cross-group node is not being ignored entirely
	nodeA := registry.Nodes[rtA.ServerContext().NodeUID]
	require.NotNil(t, nodeA, "node A should be in registry")
	assert.Contains(t, nodeA.Databases, "db1", "node A registry entry should track db1")
	nodeADbVersion := nodeA.Databases["db1"]

	nodeC := registry.Nodes[rtB.ServerContext().NodeUID]
	require.NotNil(t, nodeC, "node B should be in registry")
	assert.Contains(t, nodeC.Databases, "db1", "node B registry entry should track db1 even though it's in a different group")
	nodeCDbVersion := nodeC.Databases["db1"]
	// assert that versions are different to verify that both nodes are actually tracking their own db's config versions, not just blindly sharing the same version across groups
	assert.NotEqual(t, nodeADbVersion, nodeCDbVersion)
}

// TestIsConfigFullyAppliedRollback updates a database to version V, waits for
// both nodes to converge, then rolls back to V1 (by upserting a new config).
// Asserts that IsConfigFullyApplied for V flips to false within one poll cycle
// and IsConfigFullyApplied for V1 becomes true.
func TestIsConfigFullyAppliedRollback(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	}

	rtA := NewRestTester(t, rtConfig)
	defer rtA.Close()
	rtB := NewRestTester(t, rtConfig)
	defer rtB.Close()

	bucketName := tb.GetName()

	dbConfig := rtA.NewDbConfig()
	resp := rtA.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	refreshAndGetRegistry(t, rtA, bucketName)
	refreshAndGetRegistry(t, rtB, bucketName)

	versionV := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionV)

	dbConfig.AutoImport = base.Ptr(false)
	resp = rtA.UpsertDbConfig("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	refreshAndGetRegistry(t, rtA, bucketName)
	refreshAndGetRegistry(t, rtB, bucketName)

	versionV1 := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionV1)
	require.NotEqual(t, versionV, versionV1)

	registry := refreshAndGetRegistry(t, rtA, bucketName)

	acked, _, err := registry.IsConfigFullyApplied(ctx, groupID, "db", versionV)
	require.NoError(t, err)
	assert.False(t, acked, "old version V should no longer be acked after rollback to V1")

	acked, missing, err := registry.IsConfigFullyApplied(ctx, groupID, "db", versionV1)
	require.NoError(t, err)
	assert.True(t, acked, "new version V1 should be acked by both nodes; missing: %v", missing)
	assert.Empty(t, missing)
}

// TestIsConfigFullyAppliedDeleteDBRemovesTracking creates a database, brings up
// a second node that picks it up via polling, then deletes the database on the
// first node. After the second node polls, the deleted database's version
// tracking should be removed from the second node's clusterCompatManager.
func TestIsConfigFullyAppliedDeleteDBRemovesTracking(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	}

	rtA := NewRestTester(t, rtConfig)
	defer rtA.Close()
	rtB := NewRestTester(t, rtConfig)
	defer rtB.Close()

	bucketName := tb.GetName()

	dbConfig := rtA.NewDbConfig()
	resp := rtA.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	dbVersion := rtA.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, dbVersion, "node A should track the db version after creating it")

	registryBefore := refreshAndGetRegistry(t, rtB, bucketName)
	nodeB := registryBefore.Nodes[rtB.ServerContext().NodeUID]
	require.NotNil(t, nodeB, "node B should be in registry")
	require.Contains(t, nodeB.Databases, "db", "node B registry entry should track the db version after loading it")

	resp = rtA.SendAdminRequest(http.MethodDelete, "/db/", "")
	RequireStatus(t, resp, http.StatusOK)

	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	registryAfter := refreshAndGetRegistry(t, rtB, bucketName)
	nodeB = registryAfter.Nodes[rtB.ServerContext().NodeUID]
	require.NotNil(t, nodeB, "node B should still be in registry")
	assert.NotContains(t, nodeB.Databases, "db", "node B registry entry should no longer track version for deleted db")
}

// TestIsConfigFullyAppliedDeleteDBRemovesTrackingOnDeletingNode tests a node taking remove request for a database will
// remove the database config tracking form its own manager
func TestIsConfigFullyAppliedDeleteDBRemovesTrackingOnDeletingNode(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	sc := rt.ServerContext()
	ccm := sc.ClusterCompat
	require.NotNil(t, ccm)

	bucketName := rt.Bucket().GetName()

	require.NotEmpty(t, ccm.getAppliedDBVersionsForBucket(bucketName)["db"], "version should be tracked after initial create")

	registryBefore := refreshAndGetRegistry(t, rt, bucketName)
	nodeBefore := registryBefore.Nodes[sc.NodeUID]
	require.NotNil(t, nodeBefore, "self should be in registry before delete")
	require.Contains(t, nodeBefore.Databases, "db", "self registry entry should track the db version before delete")

	resp := rt.SendAdminRequest(http.MethodDelete, "/db/", "")
	RequireStatus(t, resp, http.StatusOK)

	assert.NotContains(t, ccm.getAppliedDBVersionsForBucket(bucketName), "db", "appliedDBVersions should be cleared on the deleting node")

	registryAfter := refreshAndGetRegistry(t, rt, bucketName)
	nodeAfter := registryAfter.Nodes[sc.NodeUID]
	require.NotNil(t, nodeAfter, "self should still be in registry after delete (still heartbeating)")
	assert.NotContains(t, nodeAfter.Databases, "db", "self registry entry should no longer track version for deleted db")
}

// TestClusterCompatAppliedDBVersionUpdatedByMutateDbConfig verifies that mutating a database
// config via POST /{db}/_config/audit (which calls mutateDbConfig) records the updated config
// version in clusterCompatManager.
func TestClusterCompatAppliedDBVersionUpdatedByMutateDbConfig(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	sc := rt.ServerContext()
	ccm := sc.ClusterCompat
	require.NotNil(t, ccm)

	bucketName := rt.Bucket().GetName()

	versionAfterCreate := ccm.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionAfterCreate, "version must be tracked after initial create")

	resp := rt.SendAdminRequest(http.MethodPost, "/db/_config/audit", `{"enabled":true}`)
	RequireStatus(t, resp, http.StatusOK)

	versionAfterMutate := ccm.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, versionAfterMutate, "version must be tracked after mutateDbConfig")
	assert.NotEqual(t, versionAfterCreate, versionAfterMutate, "version should change after config mutation")
}

// TestClusterCompatMultipleDBsSameBucket creates two databases on the same bucket
// (each using a different collection) and verifies that both database versions appear
// in the node's Databases map in the registry after a Refresh.
func TestClusterCompatMultipleDBsSameBucket(t *testing.T) {
	base.TestRequiresCollections(t)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	bucketName := tb.GetName()

	twoCollectionScopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(twoCollectionScopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{Collections: map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{Collections: map[string]*CollectionConfig{collection2Name: {}}}}

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	})
	defer rt.Close()

	dbConfig1 := rt.NewDbConfig()
	dbConfig1.Scopes = collection1ScopesConfig
	resp := rt.CreateDatabase("db1", dbConfig1)
	RequireStatus(t, resp, http.StatusCreated)

	dbConfig2 := rt.NewDbConfig()
	dbConfig2.Scopes = collection2ScopesConfig
	resp = rt.CreateDatabase("db2", dbConfig2)
	RequireStatus(t, resp, http.StatusCreated)

	ccm := rt.ServerContext().ClusterCompat
	tracked := ccm.getAppliedDBVersionsForBucket(bucketName)
	require.Contains(t, tracked, "db1", "db1 version should be tracked")
	require.Contains(t, tracked, "db2", "db2 version should be tracked")
	assert.NotEmpty(t, tracked["db1"])
	assert.NotEmpty(t, tracked["db2"])

	registry := refreshAndGetRegistry(t, rt, bucketName)
	node, ok := registry.Nodes[rt.ServerContext().NodeUID]
	require.True(t, ok, "node should be in registry")
	require.NotNil(t, node.Databases, "Databases map should be stamped after Refresh")
	assert.Equal(t, tracked["db1"], node.Databases["db1"], "db1 version in registry should match tracked version")
	assert.Equal(t, tracked["db2"], node.Databases["db2"], "db2 version in registry should match tracked version")
}

// TestClusterCompatApplyConfigsBatchVersions creates two databases, then forces a
// config reload (simulating a poll cycle via _applyConfigs). Verifies that after
// the reload and a Refresh, both database versions are correctly present in the
// registry — exercising the path where _applyConfigs iterates multiple databases
// and the first triggers RegisterBucket (with incomplete applied versions) while
// the second skips via claimBucket.
func TestClusterCompatApplyConfigsBatchVersions(t *testing.T) {
	base.TestRequiresCollections(t)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupID := t.Name()
	bucketName := tb.GetName()

	twoCollectionScopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(twoCollectionScopesConfig)
	scopeName := dataStoreNames[0].ScopeName()
	collection1Name := dataStoreNames[0].CollectionName()
	collection2Name := dataStoreNames[1].CollectionName()
	collection1ScopesConfig := ScopesConfig{scopeName: ScopeConfig{Collections: map[string]*CollectionConfig{collection1Name: {}}}}
	collection2ScopesConfig := ScopesConfig{scopeName: ScopeConfig{Collections: map[string]*CollectionConfig{collection2Name: {}}}}

	rtA := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	})
	defer rtA.Close()

	dbConfig1 := rtA.NewDbConfig()
	dbConfig1.Scopes = collection1ScopesConfig
	resp := rtA.CreateDatabase("db1", dbConfig1)
	RequireStatus(t, resp, http.StatusCreated)

	dbConfig2 := rtA.NewDbConfig()
	dbConfig2.Scopes = collection2ScopesConfig
	resp = rtA.CreateDatabase("db2", dbConfig2)
	RequireStatus(t, resp, http.StatusCreated)

	rtB := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupID,
	})
	defer rtB.Close()

	rtB.ServerContext().ForceDbConfigsReload(t, ctx)

	ccmB := rtB.ServerContext().ClusterCompat
	trackedB := ccmB.getAppliedDBVersionsForBucket(bucketName)
	require.Contains(t, trackedB, "db1", "node B should have tracked db1 after poll")
	require.Contains(t, trackedB, "db2", "node B should have tracked db2 after poll")

	registry := refreshAndGetRegistry(t, rtB, bucketName)
	nodeB, ok := registry.Nodes[rtB.ServerContext().NodeUID]
	require.True(t, ok, "node B should be in registry")
	require.NotNil(t, nodeB.Databases)
	assert.Equal(t, trackedB["db1"], nodeB.Databases["db1"], "db1 version in registry should match node B tracked version")
	assert.Equal(t, trackedB["db2"], nodeB.Databases["db2"], "db2 version in registry should match node B tracked version")

	ccmA := rtA.ServerContext().ClusterCompat
	trackedA := ccmA.getAppliedDBVersionsForBucket(bucketName)
	assert.Equal(t, trackedA["db1"], trackedB["db1"], "both nodes should agree on db1 version")
	assert.Equal(t, trackedA["db2"], trackedB["db2"], "both nodes should agree on db2 version")
}

// TestIsConfigFullyAppliedNoEligibleAckersIntegration creates a database in group A,
// then verifies that IsConfigFullyApplied returns ErrNoEligibleAckers when queried
// for a config group that has no nodes registered in the bucket's registry.
func TestIsConfigFullyAppliedNoEligibleAckersIntegration(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	groupA := t.Name() + "-A"
	groupB := t.Name() + "-B"
	bucketName := tb.GetName()

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          &groupA,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	resp := rt.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	registry := refreshAndGetRegistry(t, rt, bucketName)

	dbVersion := rt.ServerContext().ClusterCompat.getAppliedDBVersionsForBucket(bucketName)["db"]
	require.NotEmpty(t, dbVersion)

	acked, missing, err := registry.IsConfigFullyApplied(ctx, groupA, "db", dbVersion)
	require.NoError(t, err)
	assert.True(t, acked, "group A node should have acked; missing: %v", missing)

	acked, missing, err = registry.IsConfigFullyApplied(ctx, groupB, "db", dbVersion)
	require.ErrorIs(t, err, ErrNoEligibleAckers, "group B has no nodes, should return ErrNoEligibleAckers")
	assert.False(t, acked)
	assert.Empty(t, missing)
}

// TestClusterCompatFreezeAndUnfreeze exercises the manager's Freeze/Unfreeze methods:
// Freeze captures the current cluster compat version into the registry; Unfreeze clears it.
func TestClusterCompatFreezeAndUnfreeze(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)
	require.NotNil(t, ccm.ClusterCompatVersion())

	freeze, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, freeze)
	assert.Equal(t, base.NodeClusterCompatVersion, freeze.Version)
	assert.False(t, freeze.FrozenAt.IsZero())

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	require.NotNil(t, registry.Frozen, "freeze should be persisted to bucket registry")
	assert.Equal(t, freeze.Version, registry.Frozen.Version)

	cleared, residual, err := ccm.Unfreeze(ctx)
	require.NoError(t, err)
	assert.Nil(t, residual)
	require.NotNil(t, cleared, "Unfreeze should return the freeze record that was cleared")
	assert.Equal(t, base.NodeClusterCompatVersion, cleared.Version)

	registry, err = bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.Nil(t, registry.Frozen, "freeze should be cleared from bucket registry")
	assert.Nil(t, ccm.getCachedFreeze())
}

// TestClusterCompatFreezeIdempotent verifies that calling Freeze a second time returns the
// existing freeze record rather than refreshing FrozenAt.
func TestClusterCompatFreezeIdempotent(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	first, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, first.Version, second.Version)
	assert.True(t, second.FrozenAt.Equal(first.FrozenAt), "FrozenAt should not change on a no-op re-freeze")
}

// TestClusterCompatFreezePinsAcrossNodeAdvances seeds peer nodes with a higher version into the
// registry and verifies that, while frozen, the reported cluster compat version stays at the
// captured value rather than advancing to the live-node minimum.
func TestClusterCompatFreezePinsAcrossNodeAdvances(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	freeze, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, freeze)
	frozenVersion := freeze.Version

	// Seed a peer at a higher version directly into the registry. The freeze pins reporting
	// at frozenVersion regardless of whether peer registrations would have advanced the
	// live-node minimum.
	higher := base.NewClusterCompatVersion(frozenVersion.Major+1, 0)
	seedRegistryNode(t, rt, bucketName, "synthetic-peer", higher)

	ccm.lastRefreshAt = time.Time{}
	ccm.Refresh(ctx)

	got := ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	assert.Equal(t, frozenVersion, *got, "while frozen, reported version should stay at the frozen value")
}

// TestClusterCompatFreezePreventsHWMAdvance verifies the rollback-preservation contract of
// the freeze: while a freeze is in effect, a subsequent RegisterNodeVersion at a higher
// version (e.g. a node that has been upgraded past the frozen version) must not ratchet
// ClusterCompatVersionHWM past the freeze. If HWM advances, the downgrade gate would later
// refuse rolling that node back to the frozen version — defeating the freeze's purpose.
func TestClusterCompatFreezePreventsHWMAdvance(t *testing.T) {
	lowVersion := base.NewClusterCompatVersion(1, 0)
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig:         true,
		nodeClusterCompatVersion: &lowVersion,
	})
	defer rt.Close()
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	freeze, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.Equal(t, lowVersion, freeze.Version)

	// Simulate a node upgrade by re-registering self at a higher version. Without the
	// freeze ceiling, RegisterNodeVersion would ratchet HWM up to this higher value.
	higher := base.NewClusterCompatVersion(2, 0)
	_, err = bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:      bucketName,
		NodeUID:         rt.ServerContext().NodeUID,
		Version:         higher,
		HeartbeatExpiry: time.Hour,
		RatchetHWM:      true,
	})
	require.NoError(t, err)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	assert.Equal(t, lowVersion, registry.ClusterCompatVersionHWM, "HWM must not advance past the frozen version")
}

// TestClusterCompatFreezeBeforeVersion verifies the manager refuses to freeze when no
// cluster compat version has been observed yet (e.g. before RegisterBucket has run).
func TestClusterCompatFreezeBeforeVersion(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Construct a fresh manager that has never refreshed — its cachedVersion is nil.
	freshManager := &clusterCompatManager{sc: rt.ServerContext()}
	ctx := base.TestCtx(t)
	freshManager.Start(ctx)
	defer freshManager.Stop(ctx)

	_, err := freshManager.Freeze(ctx)
	assert.ErrorIs(t, err, ErrFreezeNoVersion)
}

// TestClusterCompatFreezePartialFailure verifies that when a tracked bucket cannot be
// frozen (e.g. its registry doc is unparseable), Freeze returns ErrFreezePartial rather
// than silently succeeding. This makes Freeze success-on-all (mirror of Unfreeze), so the
// admin gets a clear error and can see in the response which buckets did get frozen.
func TestClusterCompatFreezePartialFailure(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	corruptGatewayRegistry(t, rt, rt.Bucket().GetName())

	aggregate, err := ccm.Freeze(ctx)
	assert.ErrorIs(t, err, ErrFreezePartial)
	assert.Nil(t, aggregate, "no bucket accepted the freeze, so the aggregate must be nil")
	assert.Nil(t, ccm.getCachedFreeze(), "cache must remain unset when no bucket accepted the freeze")
}

// TestClusterCompatFreezePartialFailureREST verifies the REST handler returns 503 with a
// ClusterCompatVersionState body when Freeze partially fails — mirroring unfreeze. The
// body must report the live cluster compat version but no frozen version, since the
// failed freeze did not pin any value.
func TestClusterCompatFreezePartialFailureREST(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	corruptGatewayRegistry(t, rt, rt.Bucket().GetName())

	resp := rt.SendAdminRequest(http.MethodPost, "/_cluster_compat_version/freeze", "")
	RequireStatus(t, resp, http.StatusServiceUnavailable)
	var state ClusterCompatVersionState
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &state))
	assert.Nil(t, state.FrozenClusterCompatVersion, "no bucket accepted the freeze, so no frozen version should be reported")
	assert.NotNil(t, state.ClusterCompatVersion, "the live cluster compat version should still be reported after a failed freeze")
}

// TestClusterCompatFreezePreservesCacheOnTotalFailure verifies that if Freeze fails on
// every tracked bucket (succeeded==0), the previously-cached freeze is preserved rather
// than wiped to nil. A transient bucket outage that prevents any SetRegistryFreeze from
// succeeding must not erase a real, persistent freeze from the reporting endpoint — the
// next periodic Refresh self-heals from the authoritative bucket registries.
func TestClusterCompatFreezePreservesCacheOnTotalFailure(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	preFreeze, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, preFreeze)
	require.NotNil(t, ccm.getCachedFreeze(), "cache must have a freeze record after successful Freeze")

	// Corrupt the registry so subsequent SetRegistryFreeze calls fail at the getRegistry
	// step — driving the all-buckets-failed (succeeded==0, aggregate==nil) branch.
	corruptGatewayRegistry(t, rt, rt.Bucket().GetName())

	aggregate, err := ccm.Freeze(ctx)
	assert.ErrorIs(t, err, ErrFreezePartial)
	assert.Nil(t, aggregate, "no bucket accepted the freeze, so the aggregate must be nil")

	cached := ccm.getCachedFreeze()
	require.NotNil(t, cached, "cache must be preserved when no bucket accepted the freeze")
	assert.Equal(t, preFreeze.Version, cached.Version, "cache should still reflect the pre-failure freeze")
	assert.True(t, cached.FrozenAt.Equal(preFreeze.FrozenAt), "FrozenAt should not have shifted")
}

// corruptGatewayRegistry overwrites the registry doc on the given bucket with a non-object
// JSON value so that getGatewayRegistry's unmarshal fails. This simulates a bucket whose
// registry has become inaccessible — used to drive the partial-failure branch of Unfreeze.
func corruptGatewayRegistry(t *testing.T, rt *RestTester, bucketName string) {
	t.Helper()
	ctx := base.TestCtx(t)
	conn := rt.ServerContext().BootstrapContext.Connection
	var existing map[string]any
	cas, err := conn.GetMetadataDocument(ctx, bucketName, base.SGRegistryKey, &existing)
	require.NoError(t, err)
	_, err = conn.WriteMetadataDocument(ctx, bucketName, base.SGRegistryKey, cas, "corrupted-registry-for-test")
	require.NoError(t, err)
}

// TestClusterCompatUnfreezePartialFailure verifies that when ClearRegistryFreeze fails on
// a tracked bucket AND the residual re-read also fails, Unfreeze returns
// ErrUnfreezePartial with residual==nil. The bucket's registry is overwritten with an
// unparseable value so getGatewayRegistry fails — which propagates through both
// ClearRegistryFreeze and the residual re-read, hitting the clearFailed>0 / residual==nil
// branch. Also verifies the cache is preserved (not wiped to nil) so admins see a stable
// view until Refresh self-heals.
func TestClusterCompatUnfreezePartialFailure(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	preFreeze, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, preFreeze)

	corruptGatewayRegistry(t, rt, rt.Bucket().GetName())

	cleared, residual, err := ccm.Unfreeze(ctx)
	assert.ErrorIs(t, err, ErrUnfreezePartial)
	require.NotNil(t, cleared, "Unfreeze should return the pre-op freeze record so the handler can surface it")
	assert.Equal(t, preFreeze.Version, cleared.Version)
	assert.Nil(t, residual, "re-read should fail on the corrupted registry, leaving residual unknown")

	cached := ccm.getCachedFreeze()
	require.NotNil(t, cached, "cache must be preserved when residual state could not be verified")
	assert.Equal(t, preFreeze.Version, cached.Version, "cache should still reflect the pre-op freeze")
}

// TestClusterCompatUnfreezePartialFailureREST verifies the REST handler returns 503 with
// an HTTP-Error body (not a ClusterCompatVersionState) when Unfreeze fails and the
// residual state could not be verified. The error reason must include the previously-
// frozen version so the admin has a recovery target.
func TestClusterCompatUnfreezePartialFailureREST(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	resp := rt.SendAdminRequest(http.MethodPost, "/_cluster_compat_version/freeze", "")
	RequireStatus(t, resp, http.StatusOK)

	preFreezeVersion := base.NodeClusterCompatVersion.String()

	corruptGatewayRegistry(t, rt, rt.Bucket().GetName())

	resp = rt.SendAdminRequest(http.MethodPost, "/_cluster_compat_version/unfreeze", "")
	RequireStatus(t, resp, http.StatusServiceUnavailable)

	var httpErr struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &httpErr))
	assert.NotEmpty(t, httpErr.Error)
	assert.Contains(t, httpErr.Reason, preFreezeVersion, "error reason should name the previously-frozen version so the admin has a recovery target")
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

// TestSyncInfoUpgradeGate tests syncInfo write gate during a rolling upgrade.
// With a 4.0 peer present, writes must stay legacy JSON and once all peers reach 4.1+, writes flip to V1.
func TestSyncInfoUpgradeGate(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bucketName := rt.Bucket().GetName()
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	dbCtx := rt.GetDatabase()
	ds := rt.GetSingleDataStore()

	const metadataID = "test_md"
	const fakePeerUID = "fake-4.0-peer"

	// Phase 1: mid-upgrade, a 4.0 peer is still registered. ccv should be 4.0.
	seedRegistryNode(t, rt, bucketName, fakePeerUID, base.NewClusterCompatVersion(4, 0))
	ccm.lastRefreshAt = time.Time{} // bypass refresh rate-limit
	ccm.Refresh(ctx)
	got := ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	require.Equal(t, base.NewClusterCompatVersion(4, 0), *got, "mixed-version min should be 4.0")

	// Resolve the production-wired accessor and feed its result to SetSyncInfoMetadataID
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	ccv := dbCtx.ClusterCompatVersion()
	require.NoError(t, base.SetSyncInfoMetadataID(ctx, ds, metadataID, ccv))
	raw, _, err := ds.GetRaw(ctx, base.SGSyncInfo)
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.Equal(t, byte('{'), raw[0], "during mixed-version cluster, syncInfo write must be legacy JSON")

	// Phase 2: 4.0 peer leaves. Only the local node (>=4.1) remains.
	rt.ServerContext().BootstrapContext.DeregisterNodeVersion(ctx, bucketName, fakePeerUID)
	ccm.lastRefreshAt = time.Time{} // bypass refresh rate-limit
	ccm.Refresh(ctx)
	got = ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	require.True(t, got.AtLeast(4, 1), "after 4.0 peer leaves, ccv should be >= 4.1; got %v", got)

	// Same accessor, called again — must resolve to the new value at call time, not the value
	// captured during phase 1.
	rt.ServerContext().ForceClusterCompatRefresh(t, rt.Context())
	ccv = dbCtx.ClusterCompatVersion()
	require.NoError(t, base.SetSyncInfoMetadataID(ctx, ds, metadataID, ccv))
	raw, _, err = ds.GetRaw(ctx, base.SGSyncInfo)
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.Equal(t, byte(base.SyncInfoTypeV1), raw[0], "after upgrade completes, syncInfo write should be V1")
}

// TestRegisterNodeVersion_PreCCVAwarePeerCapsHWM verifies that supplying an pre-CCV-aware-peer observation
// to RegisterNodeVersion writes the entry into registry.PreCCVAwareNodes, caps the bucket HWM
// against advancing past PreSGNodeVersionFallback, and surfaces the cap through the
// manager's reported CCV. HWM is a one-way ratchet, so this test resets it before exercising
// the cap path — otherwise the auto-create-db that happens during RestTester setup would have
// already ratcheted HWM to the node version (and the cap, which only prevents further
// advancement, would have nothing to do).
func TestRegisterNodeVersion_PreCCVAwarePeerCapsHWM(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()
	selfUID := rt.ServerContext().NodeUID

	// Reset HWM and Nodes so the upcoming RegisterNodeVersion is the first one to ratchet.
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	registry.ClusterCompatVersionHWM = base.ClusterCompatVersion{}
	registry.Nodes = nil
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))

	const peerUUID = "observed-db-uuid"
	peers := map[string]base.RegistryPreCCVAwareNode{peerUUID: {Version: base.PreSGNodeVersionFallback}}
	registry, err = bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:       bucketName,
		NodeUID:          selfUID,
		Version:          base.NodeClusterCompatVersion,
		HeartbeatExpiry:  time.Hour,
		PreCCVAwarePeers: peers,
		RatchetHWM:       true,
	})
	require.NoError(t, err)

	require.Contains(t, registry.PreCCVAwareNodes, peerUUID)
	assert.False(t, registry.PreCCVAwareNodes[peerUUID].LastObservedAt.IsZero(), "LastObservedAt must be stamped on upsert")

	// HWM must be capped at PreSGNodeVersionFallback (no observed version → conservative fallback).
	assert.Equal(t, base.PreSGNodeVersionFallback, registry.ClusterCompatVersionHWM, "HWM must be capped at the PreSGNodeVersionFallback fallback when version is unknown")

	// Manager-reported CCV reflects the cap on next refresh.
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.lastRefreshAt = time.Time{}
	ccm.Refresh(ctx)
	got := ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	assert.Equal(t, base.PreSGNodeVersionFallback, *got, "reported CCV must reflect the pre-CCV-aware-peer cap")
}

// TestRegisterNodeVersion_PreCCVAwarePeerIdempotentRefresh verifies that observing the same peer in (idempotent refresh)
// two consecutive RegisterNodeVersion calls refreshes LastObservedAt rather than producing a
// duplicate entry.
func TestRegisterNodeVersion_PreCCVAwarePeerIdempotentRefresh(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()
	selfUID := rt.ServerContext().NodeUID

	const peerUUID = "same-peer"
	peers := map[string]base.RegistryPreCCVAwareNode{peerUUID: {Version: base.PreSGNodeVersionFallback}}
	opts := RegisterNodeVersionOpts{
		BucketName:       bucketName,
		NodeUID:          selfUID,
		Version:          base.NodeClusterCompatVersion,
		HeartbeatExpiry:  time.Hour,
		PreCCVAwarePeers: peers,
		RatchetHWM:       true,
	}
	registry, err := bc.RegisterNodeVersion(ctx, opts)
	require.NoError(t, err)
	require.Contains(t, registry.PreCCVAwareNodes, peerUUID)
	firstObserved := registry.PreCCVAwareNodes[peerUUID].LastObservedAt

	// Force time to advance a touch — UTC timestamps with monotonic-stripped seconds resolution
	// would otherwise produce identical instants.
	time.Sleep(2 * time.Millisecond)

	registry, err = bc.RegisterNodeVersion(ctx, opts)
	require.NoError(t, err)
	assert.Len(t, registry.PreCCVAwareNodes, 1, "duplicate observation must not create a second entry")
	assert.True(t, registry.PreCCVAwareNodes[peerUUID].LastObservedAt.After(firstObserved), "LastObservedAt must be refreshed on re-observation")
}

// TestRegisterNodeVersion_PreCCVAwarePeerExpires verifies that an pre-CCV-aware-peer entry whose LastObservedAt
// has aged past heartbeatExpiry is pruned on the next RegisterNodeVersion call that does not
// re-observe it.
func TestRegisterNodeVersion_PreCCVAwarePeerExpires(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()
	selfUID := rt.ServerContext().NodeUID

	// Seed via initial observation, then mutate LastObservedAt into the past.
	const peerUUID = "aging-peer"
	peers := map[string]base.RegistryPreCCVAwareNode{peerUUID: {Version: base.PreSGNodeVersionFallback}}
	_, err := bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:       bucketName,
		NodeUID:          selfUID,
		Version:          base.NodeClusterCompatVersion,
		HeartbeatExpiry:  time.Hour,
		PreCCVAwarePeers: peers,
		RatchetHWM:       true,
	})
	require.NoError(t, err)

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	require.Contains(t, registry.PreCCVAwareNodes, peerUUID)
	registry.PreCCVAwareNodes[peerUUID].LastObservedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))

	// Re-register with no pre-CCV-aware peers and a short expiry. The aged entry should be pruned and
	// HWM should now advance to self's version.
	registry, err = bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:      bucketName,
		NodeUID:         selfUID,
		Version:         base.NodeClusterCompatVersion,
		HeartbeatExpiry: time.Minute,
		RatchetHWM:      true,
	})
	require.NoError(t, err)
	assert.NotContains(t, registry.PreCCVAwareNodes, peerUUID, "stale pre-CCV-aware-peer entry must be pruned")
	assert.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM, "HWM must advance after pre-CCV-aware-peer entry is pruned")
}

// TestComputeCCV_WithPreCCVAwareCap is a unit-level test of computeCCV that verifies pre-CCV-aware peers
// participate in the same min-fold as live nodes and the freeze ceiling, but only when their
// Version is at or above the HWM floor. Each pre-CCV-aware peer's Version is already a concrete
// major.minor — the observer parses and substitutes PreSGNodeVersionFallback before
// persisting, so computeCCV never sees a nil version.
func TestComputeCCV_WithPreCCVAwareCap(t *testing.T) {
	highNode := map[string]base.ClusterCompatVersion{
		"n1": base.NewClusterCompatVersion(4, 1),
		"n2": base.NewClusterCompatVersion(4, 1),
	}

	// No pre-CCV-aware peers, no HWM → reported CCV tracks the live-node min (4.1).
	got := computeCCV(highNode, nil, nil, nil)
	require.NotNil(t, got)
	assert.Equal(t, base.NewClusterCompatVersion(4, 1), *got)

	// Observed peer recorded with the fallback (observer's substitute for an absent version)
	// and no HWM → CCV capped at PreSGNodeVersionFallback (4.0).
	fallbackPreCCVAware := map[string]*base.RegistryPreCCVAwareNode{
		"unknown-source-peer": {Version: base.PreSGNodeVersionFallback},
	}
	got = computeCCV(highNode, nil, fallbackPreCCVAware, nil)
	require.NotNil(t, got)
	assert.Equal(t, base.PreSGNodeVersionFallback, *got)

	// Observed peer recorded as 3.3, no HWM → CCV folds in 3.3, beating the 4.0 fallback.
	knownPreCCVAware := map[string]*base.RegistryPreCCVAwareNode{
		"v33-peer": {Version: base.NewClusterCompatVersion(3, 3)},
	}
	got = computeCCV(highNode, nil, knownPreCCVAware, nil)
	require.NotNil(t, got)
	assert.Equal(t, base.NewClusterCompatVersion(3, 3), *got, "pre-CCV-aware peer version must be used directly")

	// Mix: a 3.3 peer and a fallback-versioned peer, no HWM — min is 3.3.
	mixed := map[string]*base.RegistryPreCCVAwareNode{
		"v33-peer":            {Version: base.NewClusterCompatVersion(3, 3)},
		"unknown-source-peer": {Version: base.PreSGNodeVersionFallback},
	}
	got = computeCCV(highNode, nil, mixed, nil)
	require.NotNil(t, got)
	assert.Equal(t, base.NewClusterCompatVersion(3, 3), *got)

	// Freeze at the pre-CCV-aware-peer cap is a no-op alongside a fallback-versioned pre-CCV-aware peer — both report 4.0.
	freeze := &base.RegistryFreeze{Version: base.PreSGNodeVersionFallback, FrozenAt: time.Now()}
	got = computeCCV(highNode, freeze, fallbackPreCCVAware, nil)
	require.NotNil(t, got)
	assert.Equal(t, base.PreSGNodeVersionFallback, *got)

	// HWM floor at 4.1: a pre-CCV-aware peer at 4.0 (or 3.3) is below HWM and must be excluded from
	// the fold — the cluster has already committed to 4.1 and an aged-in pre-CCV-aware peer
	// must not surface as a CCV regression.
	hwm41 := base.NewClusterCompatVersion(4, 1)
	got = computeCCV(highNode, nil, fallbackPreCCVAware, &hwm41)
	require.NotNil(t, got)
	assert.Equal(t, base.NewClusterCompatVersion(4, 1), *got, "pre-CCV-aware peer below HWM must not drag CCV back")

	got = computeCCV(highNode, nil, mixed, &hwm41)
	require.NotNil(t, got)
	assert.Equal(t, base.NewClusterCompatVersion(4, 1), *got, "all pre-CCV-aware peers below HWM are filtered, CCV tracks live-node min")

	// HWM floor at 3.3: the 3.3 pre-CCV-aware peer is *at* the floor (not below) so it still
	// contributes; the 4.0 fallback peer is above the floor so it also contributes. Min is 3.3.
	hwm33 := base.NewClusterCompatVersion(3, 3)
	got = computeCCV(highNode, nil, mixed, &hwm33)
	require.NotNil(t, got)
	assert.Equal(t, base.NewClusterCompatVersion(3, 3), *got, "pre-CCV-aware peer at HWM floor must still contribute")

	// All inputs empty → nil.
	got = computeCCV(nil, nil, nil, nil)
	assert.Nil(t, got)
}

// TestFreezeAndPreCCVAwareInteraction verifies that once HWM has ratcheted past a version, an
// aged-in pre-CCV-aware-peer observation below HWM does not drag the reported CCV back down. The
// freeze stays at its pinned version and the pre-CCV-aware peer is reported via the API for
// operator visibility but excluded from the CCV min-fold.
func TestFreezeAndPreCCVAwareInteraction(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	ctx := base.TestCtx(t)
	bc := rt.ServerContext().BootstrapContext
	bucketName := rt.Bucket().GetName()
	selfUID := rt.ServerContext().NodeUID

	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	// Freeze at the current (self) version. With only self registered, freeze pins at
	// base.NodeClusterCompatVersion. Refresh above also ratcheted HWM to that version, so the
	// floor is now base.NodeClusterCompatVersion.
	frozen, err := ccm.Freeze(ctx)
	require.NoError(t, err)
	require.NotNil(t, frozen)
	require.Equal(t, base.NodeClusterCompatVersion, frozen.Version)

	// Observe a pre-CCV-aware peer at PreSGNodeVersionFallback — below the HWM floor. The peer
	// is persisted in the registry and surfaced via PreCCVAwareNodeVersions, but must not drag
	// reported CCV back below HWM.
	peers := map[string]base.RegistryPreCCVAwareNode{"frozen-era-peer": {Version: base.PreSGNodeVersionFallback}}
	_, err = bc.RegisterNodeVersion(ctx, RegisterNodeVersionOpts{
		BucketName:       bucketName,
		NodeUID:          selfUID,
		Version:          base.NodeClusterCompatVersion,
		HeartbeatExpiry:  time.Hour,
		PreCCVAwarePeers: peers,
		RatchetHWM:       true,
	})
	require.NoError(t, err)

	ccm.lastRefreshAt = time.Time{}
	ccm.Refresh(ctx)

	got := ccm.ClusterCompatVersion()
	require.NotNil(t, got)
	assert.Equal(t, base.NodeClusterCompatVersion, *got, "HWM floor must prevent pre-CCV-aware peer from dragging CCV back")

	// The peer is still visible via the API for operator visibility.
	observed := ccm.PreCCVAwareNodeVersions()
	assert.Contains(t, observed, "frozen-era-peer", "pre-CCV-aware peer must remain reported even when excluded from CCV computation")
	assert.Equal(t, base.PreSGNodeVersionFallback, observed["frozen-era-peer"], "reported observed version must reflect the observation, not the HWM floor")
}

// TestMergeRegistryIntoCache_PreCCVAwarePeerLowerWins verifies that when the same pre-CCV-aware peer UUID
// is folded in from two consecutive bucket-registry reads (the shape produced by both the
// RegisterBucket path and the refreshNodeRegistrations cross-bucket loop), the cache retains
// the lower-versioned observation regardless of fold order, and LastObservedAt is advanced to
// the most recent observation. This pins down the lower-version-wins contract for both
// merge sites in lockstep — they share the algorithm.
func TestMergeRegistryIntoCache_PreCCVAwarePeerLowerWins(t *testing.T) {
	const peerUUID = "shared-pre-CCV-aware-peer"
	earlier := time.Now().Add(-time.Hour).UTC()
	later := time.Now().UTC()
	v33 := base.NewClusterCompatVersion(3, 3)
	v40 := base.PreSGNodeVersionFallback

	// Higher reading observed first, lower reading observed second: lower wins, LastObservedAt
	// advances to the second (more recent) observation.
	t.Run("higher-then-lower", func(t *testing.T) {
		m := &clusterCompatManager{}
		bucketA := map[string]*base.RegistryPreCCVAwareNode{peerUUID: {Version: v40, LastObservedAt: earlier}}
		m.mergeRegistryIntoCache(nil, nil, bucketA, base.ClusterCompatVersion{})

		bucketB := map[string]*base.RegistryPreCCVAwareNode{peerUUID: {Version: v33, LastObservedAt: later}}
		m.mergeRegistryIntoCache(nil, nil, bucketB, base.ClusterCompatVersion{})

		require.Contains(t, m.cachedPreCCVAwareNodes, peerUUID)
		assert.Equal(t, v33, m.cachedPreCCVAwareNodes[peerUUID].Version, "lower reading must win")
		assert.Equal(t, later, m.cachedPreCCVAwareNodes[peerUUID].LastObservedAt, "LastObservedAt must advance to the most recent observation")
	})

	// Lower reading observed first, higher reading observed second: lower still wins (sticky),
	// and LastObservedAt still advances to the more recent observation even though the higher
	// reading was discarded.
	t.Run("lower-then-higher", func(t *testing.T) {
		m := &clusterCompatManager{}
		bucketA := map[string]*base.RegistryPreCCVAwareNode{peerUUID: {Version: v33, LastObservedAt: earlier}}
		m.mergeRegistryIntoCache(nil, nil, bucketA, base.ClusterCompatVersion{})

		bucketB := map[string]*base.RegistryPreCCVAwareNode{peerUUID: {Version: v40, LastObservedAt: later}}
		m.mergeRegistryIntoCache(nil, nil, bucketB, base.ClusterCompatVersion{})

		require.Contains(t, m.cachedPreCCVAwareNodes, peerUUID)
		assert.Equal(t, v33, m.cachedPreCCVAwareNodes[peerUUID].Version, "lower reading must remain after a higher second observation")
		assert.Equal(t, later, m.cachedPreCCVAwareNodes[peerUUID].LastObservedAt, "LastObservedAt must advance even when the higher reading is discarded")
	})
}
