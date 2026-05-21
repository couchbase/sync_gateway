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
	version := base.NodeClusterCompatVersion
	var eg errgroup.Group
	for i := 0; i < n; i++ {
		eg.Go(func() error {
			_, err := bc.RegisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i), "", version, nil, time.Hour)
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
		_, err := bc.RegisterNodeVersion(ctx, bucketName, fmt.Sprintf("node-%d", i), "", version, nil, time.Hour)
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
	_, err := bc.RegisterNodeVersion(ctx, bucketName, selfUID, "", base.NodeClusterCompatVersion, nil, time.Hour)
	require.NoError(t, err)
	staleTime := time.Now().Add(-100 * ccm.heartbeatExpiry())
	setNodeHeartbeatAt(t, rt, bucketName, selfUID, staleTime)

	// Re-register with a non-zero expiry. Self must survive and have a fresh heartbeat.
	registry, err := bc.RegisterNodeVersion(ctx, bucketName, selfUID, "", base.NodeClusterCompatVersion, nil, ccm.heartbeatExpiry())
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
// (empty) bucket succeeds and ratchets ClusterCompatVersionHWM up to the node's compat version.
func TestClusterCompatDowngradeEmptyRegistry(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	bucketName := rt.Bucket().GetName()
	bc := rt.ServerContext().BootstrapContext

	registry, err := bc.getGatewayRegistry(base.TestCtx(t), bucketName)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM, "HWM should be ratcheted to node version after first apply")
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

	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	require.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM)

	preserved := base.NewClusterCompatVersion(base.NodeClusterCompatVersion.Major+1, 0)
	registry.ClusterCompatVersionHWM = preserved
	require.NoError(t, bc.setGatewayRegistry(ctx, bucketName, registry))

	_, err = bc.RegisterNodeVersion(ctx, bucketName, "lower-peer", "", base.NewClusterCompatVersion(0, 1), nil, time.Hour)
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

	// After auto-create db, HWM == self version (only node).
	registry, err := bc.getGatewayRegistry(ctx, bucketName)
	require.NoError(t, err)
	require.Equal(t, base.NodeClusterCompatVersion, registry.ClusterCompatVersionHWM)

	// Add a higher-version peer. Cluster compat is still min(self, higher) == self, so HWM
	// must not budge.
	higher := base.NewClusterCompatVersion(base.NodeClusterCompatVersion.Major+1, 0)
	_, err = bc.RegisterNodeVersion(ctx, bucketName, "higher-peer", "", higher, nil, time.Hour)
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

// TestClusterCompatRefreshIntervalUnclamped verifies refreshInterval returns the configured
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
