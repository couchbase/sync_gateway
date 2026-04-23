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
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var _ base.ClusterCompatChecker = (*clusterCompatManager)(nil)

// RegistryNode represents a single Sync Gateway node's version registration in the cluster.
type RegistryNode struct {
	Version     base.ClusterCompatVersion `json:"version"`
	HeartbeatAt time.Time                 `json:"heartbeat_at"`
}

// clusterCompatBucketNodes stores the node registrations for a single bucket's registry.
type clusterCompatBucketNodes struct {
	Nodes map[string]RegistryNode `json:"nodes,omitempty"`
}

// clusterCompatManager tracks the minimum Sync Gateway version across all nodes in the cluster.
// It is used to gate metadata writes so that new formats are only used once all nodes have been
// upgraded.
//
// Node versions are stored in each bucket's _sync:registry document. A node only registers
// itself in a bucket once it has a database configured on that bucket (see RegisterBucket),
// so buckets this SG node is not serving are never touched. Heartbeats and version
// recomputation are driven by the config polling goroutine.
type clusterCompatManager struct {
	sc *ServerContext
	// trackedBuckets is intent: the buckets this node has declared ownership of via
	// RegisterBucket. Used to drive Refresh and Stop. Append-only for the lifetime of
	// the manager — a transient RegisterNodeVersion failure must not cause us to drop
	// ownership or skip the bucket on next refresh / shutdown deregister.
	trackedBuckets map[string]struct{}
	// cachedVersion/cachedNodes/cachedBuckets are observed state from the most recent
	// successful refresh. cachedBuckets differs from trackedBuckets when a refresh
	// skips a bucket due to an error. cachedBuckets is kept around (not derivable from
	// cachedNodes) because /_cluster_compat exposes per-bucket node breakdowns.
	cachedVersion *base.ClusterCompatVersion
	cachedNodes   map[string]base.ClusterCompatVersion
	cachedBuckets map[string]clusterCompatBucketNodes
	lastRefreshAt time.Time // records when the last successful heartbeat refresh completed
	mu            sync.RWMutex
}

func (m *clusterCompatManager) getCachedVersion() *base.ClusterCompatVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cachedVersion
}

func (m *clusterCompatManager) setCached(version *base.ClusterCompatVersion, nodes map[string]base.ClusterCompatVersion, buckets map[string]clusterCompatBucketNodes) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cachedVersion = version
	m.cachedNodes = nodes
	m.cachedBuckets = buckets
	m.lastRefreshAt = time.Now()
}

// Start initializes the manager. It does not write to any bucket — node registration is
// performed lazily when a database is loaded on a given bucket (via RegisterBucket).
func (m *clusterCompatManager) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.trackedBuckets == nil {
		m.trackedBuckets = make(map[string]struct{})
	}
	return nil
}

// Stop best-effort deregisters this node from the buckets this node registered in.
func (m *clusterCompatManager) Stop(ctx context.Context) {
	for _, bucket := range m.trackedBucketList() {
		m.sc.BootstrapContext.DeregisterNodeVersion(ctx, bucket, m.sc.NodeUUID)
	}
}

// trackedBucketList returns a snapshot of the buckets this node has registered in.
func (m *clusterCompatManager) trackedBucketList() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	buckets := make([]string, 0, len(m.trackedBuckets))
	for b := range m.trackedBuckets {
		buckets = append(buckets, b)
	}
	return buckets
}

// RegisterBucket registers this node in the given bucket's registry and recomputes the
// cluster compat version across all tracked buckets. Intended to be called when a database
// is loaded on a bucket this node has not yet registered in. It is idempotent with respect
// to tracking: repeated calls refresh the heartbeat and re-read the registry state.
func (m *clusterCompatManager) RegisterBucket(ctx context.Context, bucket string) {
	m.mu.Lock()
	if m.trackedBuckets == nil {
		m.trackedBuckets = make(map[string]struct{})
	}
	_, alreadyTracked := m.trackedBuckets[bucket]
	m.trackedBuckets[bucket] = struct{}{}
	m.mu.Unlock()

	version, nodes, buckets, err := m.refreshNodeRegistrations(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Failed to register node version for bucket %s: %v", base.MD(bucket), err)
		return
	}
	oldVersion := m.getCachedVersion()
	if !alreadyTracked {
		base.InfofCtx(ctx, base.KeyConfig, "Registered node %s in bucket %s; cluster compatibility version is %v", m.sc.NodeUUID, base.MD(bucket), version)
	} else if !clusterCompatVersionEqual(oldVersion, version) {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, version)
	}
	m.setCached(version, nodes, buckets)
}

// ClusterCompatVersion returns the cached cluster compat version, or nil if not yet computed.
func (m *clusterCompatManager) ClusterCompatVersion() *base.ClusterCompatVersion {
	return m.getCachedVersion()
}

// ClusterIsAtLeast returns true if the cluster compat version is at least the given major.minor.
// Returns false if no version has been computed yet (conservative — don't advance until we know).
func (m *clusterCompatManager) ClusterIsAtLeast(major, minor uint8) bool {
	v := m.getCachedVersion()
	if v == nil {
		return false
	}
	return v.AtLeast(major, minor)
}

// NodeVersions returns the cluster compat version of each node in the cluster, keyed by node UUID.
// This is the union of nodes across all bucket registries this node has registered in.
func (m *clusterCompatManager) NodeVersions() map[string]base.ClusterCompatVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Return a copy to prevent callers from mutating cached state.
	if m.cachedNodes == nil {
		return nil
	}
	nodes := make(map[string]base.ClusterCompatVersion, len(m.cachedNodes))
	for k, v := range m.cachedNodes {
		nodes[k] = v
	}
	return nodes
}

// Refresh re-registers this node in every tracked bucket, prunes stale nodes, and recomputes
// the cluster compat version. Called from the config polling goroutine.
//
// To avoid excessive CAS contention on the registry document — which is shared with db-config
// updates — the refresh is rate-limited to at most once per refreshInterval(). This keeps the
// heartbeat well within the expiry window (refreshInterval << heartbeatExpiry) while preventing
// rapid-polling test configurations from writing the registry on every tick.
func (m *clusterCompatManager) Refresh(ctx context.Context) {
	// if config polling interval is very low we should rate limit heartbeat updates
	m.mu.RLock()
	lastRefresh := m.lastRefreshAt
	hasTrackedBuckets := len(m.trackedBuckets) > 0
	m.mu.RUnlock()
	if !hasTrackedBuckets {
		return
	}
	if time.Since(lastRefresh) < m.refreshInterval() {
		return
	}

	version, nodes, buckets, err := m.refreshNodeRegistrations(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Failed to refresh cluster compat version: %v", err)
		return
	}
	oldVersion := m.getCachedVersion()
	if !clusterCompatVersionEqual(oldVersion, version) {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, version)
	}
	m.setCached(version, nodes, buckets)
}

// refreshNodeRegistrations iterates the tracked buckets, registers this node, prunes stale
// nodes, and returns the minimum version across all nodes in those registries, a flat map of
// all node versions, and a per-bucket breakdown of node registrations.
func (m *clusterCompatManager) refreshNodeRegistrations(ctx context.Context) (*base.ClusterCompatVersion, map[string]base.ClusterCompatVersion, map[string]clusterCompatBucketNodes, error) {
	buckets := m.trackedBucketList()
	if len(buckets) == 0 {
		return nil, nil, nil, nil
	}

	nodeVersion := base.NodeClusterCompatVersion
	heartbeatExpiry := m.heartbeatExpiry()
	// Collect unique node versions across all bucket registries. A node appearing in multiple
	// bucket registries will have the same version — last-write-wins is fine.
	nodeMap := make(map[string]base.ClusterCompatVersion)
	bucketMap := make(map[string]clusterCompatBucketNodes, len(buckets))
	for _, bucket := range buckets {
		registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUUID, nodeVersion, heartbeatExpiry)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to register node version in bucket %s: %v", base.MD(bucket), err)
			continue
		}
		bucketNodes := make(map[string]RegistryNode, len(registry.Nodes))
		for nodeUUID, node := range registry.Nodes {
			nodeMap[nodeUUID] = node.Version
			bucketNodes[nodeUUID] = *node
		}
		bucketMap[bucket] = clusterCompatBucketNodes{Nodes: bucketNodes}
	}
	if len(nodeMap) == 0 {
		return nil, nil, nil, nil
	}
	versions := make([]base.ClusterCompatVersion, 0, len(nodeMap))
	for _, v := range nodeMap {
		versions = append(versions, v)
	}
	minCompactVersion := base.MinClusterCompatVersion(versions...)
	return &minCompactVersion, nodeMap, bucketMap, nil
}

// heartbeatExpiry returns the duration after which a node is considered stale.
// If ConfigUpdateFrequency is zero (e.g. disabled in tests), falls back to the default frequency.
func (m *clusterCompatManager) heartbeatExpiry() time.Duration {
	return m.refreshInterval() * time.Duration(m.nodeHeartbeatExpiryMultiplier())
}

// refreshInterval returns the effective interval between node registration refreshes.
// It applies the same normalisation used by heartbeatExpiry: if ConfigUpdateFrequency is
// very low (e.g. in some tests), it falls back to persistentConfigDefaultUpdateFrequency. This
// ensures that Refresh() writes to the registry no more often than once per refreshInterval,
// which is always significantly less than heartbeatExpiry (by the multiplier factor), so
// heartbeats never expire due to rate-limiting.
func (m *clusterCompatManager) refreshInterval() time.Duration {
	freq := m.sc.Config.Bootstrap.ConfigUpdateFrequency.Value()
	if freq <= time.Second {
		freq = persistentConfigDefaultUpdateFrequency
	}
	return freq
}

// nodeHeartbeatExpiryMultiplier returns the configured multiplier, falling back to the default.
func (m *clusterCompatManager) nodeHeartbeatExpiryMultiplier() int {
	if m.sc.Config.Bootstrap.NodeHeartbeatExpiryMultiplier != nil {
		return *m.sc.Config.Bootstrap.NodeHeartbeatExpiryMultiplier
	}
	return defaultNodeHeartbeatExpiryMultiplier
}

// BucketNodes returns a deep copy of the per-bucket node registrations.
func (m *clusterCompatManager) BucketNodes() map[string]clusterCompatBucketNodes {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cachedBuckets == nil {
		return nil
	}
	result := make(map[string]clusterCompatBucketNodes, len(m.cachedBuckets))
	for bucket, bn := range m.cachedBuckets {
		nodes := make(map[string]RegistryNode, len(bn.Nodes))
		for k, v := range bn.Nodes {
			nodes[k] = v
		}
		result[bucket] = clusterCompatBucketNodes{Nodes: nodes}
	}
	return result
}

// clusterCompatResponse is the JSON response for the /_cluster_compat endpoint.
type clusterCompatResponse struct {
	ClusterCompatVersion *base.ClusterCompatVersion           `json:"cluster_compat_version"`
	Nodes                map[string]base.ClusterCompatVersion `json:"nodes,omitempty"`
	Buckets              map[string]clusterCompatBucketNodes  `json:"buckets,omitempty"`
}

// clusterCompatVersionEqual compares two possibly-nil ClusterCompatVersion pointers.
func clusterCompatVersionEqual(a, b *base.ClusterCompatVersion) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
