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
// upgraded. Node versions are stored in each bucket's _sync:registry document. Heartbeats and
// version recomputation are driven by the config polling goroutine — no separate ticker is needed.
type clusterCompatManager struct {
	sc            *ServerContext
	cachedVersion *base.ClusterCompatVersion
	cachedNodes   map[string]base.ClusterCompatVersion
	cachedBuckets map[string]clusterCompatBucketNodes
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
}

// Start registers this node in all bucket registries and computes the initial cluster compat version.
func (m *clusterCompatManager) Start(ctx context.Context) error {
	version, nodes, buckets, err := m.refreshNodeRegistrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to start cluster compat manager: %w", err)
	}
	m.setCached(version, nodes, buckets)
	if version != nil {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version initialized to %s", version)
	}
	return nil
}

// Stop best-effort deregisters this node from all bucket registries.
func (m *clusterCompatManager) Stop(ctx context.Context) {
	buckets, err := m.sc.GetBucketNames(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Failed to get bucket names for node deregistration: %v", err)
		return
	}
	for _, bucket := range buckets {
		m.sc.BootstrapContext.DeregisterNodeVersion(ctx, bucket, m.sc.NodeUUID)
	}
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
// This is the union of nodes across all bucket registries.
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

// Refresh re-registers this node in all bucket registries, prunes stale nodes, and recomputes
// the cluster compat version. Called from the config polling goroutine.
func (m *clusterCompatManager) Refresh(ctx context.Context) {
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

// refreshNodeRegistrations iterates all buckets, registers this node, prunes stale nodes,
// and returns the minimum version across all nodes in all registries, a flat map of all node
// versions, and a per-bucket breakdown of node registrations.
func (m *clusterCompatManager) refreshNodeRegistrations(ctx context.Context) (*base.ClusterCompatVersion, map[string]base.ClusterCompatVersion, map[string]clusterCompatBucketNodes, error) {
	buckets, err := m.sc.GetBucketNames(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get bucket names: %w", err)
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
	multiplier := defaultNodeHeartbeatExpiryMultiplier
	if m.sc.Config.Bootstrap.NodeHeartbeatExpiryMultiplier != nil {
		multiplier = *m.sc.Config.Bootstrap.NodeHeartbeatExpiryMultiplier
	}
	freq := m.sc.Config.Bootstrap.ConfigUpdateFrequency.Value()
	if freq <= 0 {
		freq = persistentConfigDefaultUpdateFrequency
	}
	return freq * time.Duration(multiplier)
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

// handleGetClusterCompat returns the cluster compatibility version, per-node versions, and
// per-bucket node registrations.
func (h *handler) handleGetClusterCompat() error {
	resp := clusterCompatResponse{}
	if h.server.ClusterCompat != nil {
		resp.ClusterCompatVersion = h.server.ClusterCompat.ClusterCompatVersion()
		resp.Nodes = h.server.ClusterCompat.NodeVersions()
		if ccm, ok := h.server.ClusterCompat.(*clusterCompatManager); ok {
			resp.Buckets = ccm.BucketNodes()
		}
	}
	base.Audit(h.ctx(), base.AuditIDClusterCompatRead, nil)
	h.writeJSON(resp)
	return nil
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
