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

	"github.com/couchbase/sync_gateway/base"
)

var _ base.ClusterCompatChecker = (*clusterCompatManager)(nil)

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
	cachedBuckets map[string]base.ClusterCompatBucketNodes
	mu            sync.RWMutex
}

// getCachedVersion returns a copy of the cached cluster compat version, or nil if not computed.
func (m *clusterCompatManager) getCachedVersion() *base.ClusterCompatVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cachedVersion == nil {
		return nil
	}
	cp := *m.cachedVersion
	return &cp
}

func (m *clusterCompatManager) setCached(version *base.ClusterCompatVersion, nodes map[string]base.ClusterCompatVersion, buckets map[string]base.ClusterCompatBucketNodes) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cachedVersion = version
	m.cachedNodes = nodes
	m.cachedBuckets = buckets
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
//
// TODO: RegisterBucket re-registers on every tracked bucket on each call — with N databases
// across M buckets this is O(N*M) registry writes on startup. Consider scoping the write
// to just the new bucket and deferring the full refresh to the config poller.
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

// ClusterCompatVersion returns a copy of the cached cluster compat version, or nil if not yet
// computed. Callers may mutate the returned value without affecting the cache.
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
	if m.cachedNodes == nil {
		return nil
	}
	nodes := make(map[string]base.ClusterCompatVersion, len(m.cachedNodes))
	for k, v := range m.cachedNodes {
		nodes[k] = v
	}
	return nodes
}

// Refresh re-registers this node in every tracked bucket and recomputes the cluster compat
// version. Called from the config polling goroutine.
func (m *clusterCompatManager) Refresh(ctx context.Context) {
	m.mu.RLock()
	hasTrackedBuckets := len(m.trackedBuckets) > 0
	m.mu.RUnlock()
	if !hasTrackedBuckets {
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

// refreshNodeRegistrations iterates the tracked buckets, registers this node, and returns
// the minimum version across all nodes in those registries, a flat map of all node versions,
// and a per-bucket breakdown of node registrations.
func (m *clusterCompatManager) refreshNodeRegistrations(ctx context.Context) (*base.ClusterCompatVersion, map[string]base.ClusterCompatVersion, map[string]base.ClusterCompatBucketNodes, error) {
	buckets := m.trackedBucketList()
	if len(buckets) == 0 {
		return nil, nil, nil, nil
	}

	nodeVersion := base.NodeClusterCompatVersion
	// Collect unique node versions across all bucket registries. A node appearing in multiple
	// bucket registries will have the same version — last-write-wins is fine.
	nodeMap := make(map[string]base.ClusterCompatVersion)
	bucketMap := make(map[string]base.ClusterCompatBucketNodes, len(buckets))
	for _, bucket := range buckets {
		registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUUID, nodeVersion)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to register node version in bucket %s: %v", base.MD(bucket), err)
			continue
		}
		bucketNodes := make(map[string]base.RegistryNode, len(registry.Nodes))
		for nodeUUID, node := range registry.Nodes {
			nodeMap[nodeUUID] = node.Version
			bucketNodes[nodeUUID] = *node
		}
		bucketMap[bucket] = base.ClusterCompatBucketNodes{Nodes: bucketNodes}
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

// BucketNodes returns a deep copy of the per-bucket node registrations.
func (m *clusterCompatManager) BucketNodes() map[string]base.ClusterCompatBucketNodes {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cachedBuckets == nil {
		return nil
	}
	result := make(map[string]base.ClusterCompatBucketNodes, len(m.cachedBuckets))
	for bucket, bn := range m.cachedBuckets {
		nodes := make(map[string]base.RegistryNode, len(bn.Nodes))
		for k, v := range bn.Nodes {
			nodes[k] = v
		}
		result[bucket] = base.ClusterCompatBucketNodes{Nodes: nodes}
	}
	return result
}

// clusterCompatResponse is the JSON response for the /_cluster_compat endpoint.
type clusterCompatResponse struct {
	ClusterCompatVersion *base.ClusterCompatVersion               `json:"cluster_compat_version"`
	Nodes                map[string]base.ClusterCompatVersion     `json:"nodes,omitempty"`
	Buckets              map[string]base.ClusterCompatBucketNodes `json:"buckets,omitempty"`
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
