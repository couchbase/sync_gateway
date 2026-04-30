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

// clusterCompatManager tracks the minimum Sync Gateway version across all nodes in the cluster.
// It is used to gate metadata writes so that new formats are only used once all nodes have been
// upgraded.
//
// Node versions are stored in each bucket's _sync:registry document. A node only registers
// itself in a bucket once it has a database configured on that bucket (see RegisterBucket),
// so buckets this SG node is not serving are never touched. Heartbeats and version
// recomputation are driven by the config polling goroutine.
//
// TODO: CBG-5219 - A node that crashes or is killed before Stop() runs will leave a stale
// entry in the registry, pinning the min version to its last reported value.
// Pruning of stale heartbeats is deferred to follow-up ticket.
type clusterCompatManager struct {
	sc *ServerContext
	// trackedBuckets is intent: the buckets this node has declared ownership of via
	// RegisterBucket. Used to drive Refresh and Stop. Append-only for the lifetime of
	// the manager — a transient RegisterNodeVersion failure must not cause us to drop
	// ownership or skip the bucket on next refresh / shutdown deregister.
	trackedBuckets map[string]struct{}
	// cachedVersion/cachedNodes are observed state from the most recent successful refresh.
	cachedVersion *base.ClusterCompatVersion
	cachedNodes   map[string]base.ClusterCompatVersion
	// lastRefreshAt records when Refresh last completed a registry write cycle. Used to
	// rate-limit periodic Refresh — see Refresh(). RegisterBucket does not update this:
	// a new bucket coming into scope should still trigger the next periodic heartbeat.
	lastRefreshAt time.Time
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

// refreshInterval is the rate-limit window for periodic Refresh calls. The heartbeat write
// to each bucket's registry doc only needs to happen well within the (eventual) heartbeat
// expiry — rewriting on every poll tick just churns CAS for no benefit and worsens contention
// with other nodes' simultaneous refreshes. Falls back to the default if ConfigUpdateFrequency
// is unset or sub-second (some test setups effectively disable polling).
func (m *clusterCompatManager) refreshInterval() time.Duration {
	freq := m.sc.Config.Bootstrap.ConfigUpdateFrequency.Value()
	if freq <= time.Second {
		freq = persistentConfigDefaultUpdateFrequency
	}
	return freq
}

// Start initializes the manager. It does not write to any bucket — node registration is
// performed lazily when a database is loaded on a given bucket (via RegisterBucket).
func (m *clusterCompatManager) Start(_ context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trackedBuckets = make(map[string]struct{})
}

// Stop best-effort deregisters this node from the buckets this node registered in.
func (m *clusterCompatManager) Stop(ctx context.Context) {
	for _, bucket := range m.trackedBucketList() {
		m.sc.BootstrapContext.DeregisterNodeVersion(ctx, bucket, m.sc.NodeUID)
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

// RegisterBucket registers this node in the given bucket's registry and merges the bucket's
// node entries into the cached cluster view. Intended to be called when a database is loaded
// on a bucket. The first call for a bucket performs a single registry write and a one-bucket
// merge into the cached view; subsequent calls for an already-tracked bucket return
// immediately, since the periodic Refresh keeps the heartbeat fresh.
//
// This is deliberately a single-bucket write (not a sweep of every tracked bucket) so that
// loading N databases across M buckets at startup costs O(M) writes rather than O(N*M),
// and so the I/O performed under sc._databasesLock is bounded.
func (m *clusterCompatManager) RegisterBucket(ctx context.Context, bucket string) {
	m.mu.Lock()
	if _, alreadyTracked := m.trackedBuckets[bucket]; alreadyTracked {
		m.mu.Unlock()
		return
	}
	m.trackedBuckets[bucket] = struct{}{}
	m.mu.Unlock()

	registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUID, base.NodeClusterCompatVersion)
	if err != nil {
		// Bucket stays tracked — the next Refresh will retry the registry write.
		base.WarnfCtx(ctx, "Failed to register node version for bucket %s: %v", base.MD(bucket), err)
		return
	}

	// Merge this bucket's registry into the cached view and recompute the min. A node
	// removed from this bucket but still present in another tracked bucket would not
	// be evicted here — that's reconciled by the periodic Refresh, which rebuilds the
	// cache from scratch.
	m.mu.Lock()
	if m.cachedNodes == nil {
		m.cachedNodes = make(map[string]base.ClusterCompatVersion, len(registry.Nodes))
	}
	for nodeUID, node := range registry.Nodes {
		m.cachedNodes[nodeUID] = node.Version
	}
	newVersion := minClusterCompatVersion(m.cachedNodes)
	oldVersion := m.cachedVersion
	m.cachedVersion = &newVersion
	m.mu.Unlock()

	base.InfofCtx(ctx, base.KeyConfig, "Registered node %s in bucket %s; cluster compatibility version is %v", m.sc.NodeUID, base.MD(bucket), &newVersion)
	if oldVersion != nil && *oldVersion != newVersion {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, &newVersion)
	}
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

// NodeVersions returns the cluster compat version of each node in the cluster, keyed by node UID.
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
//
// Rate-limited to at most once per refreshInterval(): the heartbeat is well within its
// (eventual) expiry window if rewritten once per poll cycle, and skipping when the previous
// write is still fresh avoids piling extra CAS contention onto a registry doc that's also
// touched by db config writes.
func (m *clusterCompatManager) Refresh(ctx context.Context) {
	m.mu.RLock()
	hasTrackedBuckets := len(m.trackedBuckets) > 0
	lastRefresh := m.lastRefreshAt
	m.mu.RUnlock()
	if !hasTrackedBuckets {
		return
	}
	if !lastRefresh.IsZero() && time.Since(lastRefresh) < m.refreshInterval() {
		return
	}

	version, nodes, err := m.refreshNodeRegistrations(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Failed to refresh cluster compat version: %v", err)
		return
	}
	oldVersion := m.getCachedVersion()
	if !clusterCompatVersionEqual(oldVersion, version) {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, version)
	}
	m.mu.Lock()
	m.cachedVersion = version
	m.cachedNodes = nodes
	m.lastRefreshAt = time.Now().UTC()
	m.mu.Unlock()
}

// refreshNodeRegistrations iterates the tracked buckets, registers this node, and returns
// the minimum version across all nodes in those registries and a flat map of all node
// versions. Returns an error if every tracked bucket failed so callers can leave the
// previously-cached state in place — stale is preferable to flipping the cluster compat
// version to nil on a transient bucket outage.
func (m *clusterCompatManager) refreshNodeRegistrations(ctx context.Context) (*base.ClusterCompatVersion, map[string]base.ClusterCompatVersion, error) {
	buckets := m.trackedBucketList()
	if len(buckets) == 0 {
		return nil, nil, nil
	}

	nodeVersion := base.NodeClusterCompatVersion
	// Collect unique node versions across all bucket registries. A node appearing in multiple
	// bucket registries will have the same version — last-write-wins is fine.
	nodeMap := make(map[string]base.ClusterCompatVersion)
	succeeded := 0
	for _, bucket := range buckets {
		registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUID, nodeVersion)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to register node version in bucket %s: %v", base.MD(bucket), err)
			continue
		}
		succeeded++
		for nodeUID, node := range registry.Nodes {
			nodeMap[nodeUID] = node.Version
		}
	}
	if succeeded == 0 {
		return nil, nil, fmt.Errorf("no tracked bucket registries could be updated (%d tracked)", len(buckets))
	}
	minCompatVersion := minClusterCompatVersion(nodeMap)
	return &minCompatVersion, nodeMap, nil
}

// minClusterCompatVersion returns the minimum version across the given node map. Returns
// the zero value when the map is empty.
func minClusterCompatVersion(nodes map[string]base.ClusterCompatVersion) base.ClusterCompatVersion {
	versions := make([]base.ClusterCompatVersion, 0, len(nodes))
	for _, v := range nodes {
		versions = append(versions, v)
	}
	return base.MinClusterCompatVersion(versions...)
}

// clusterCompatResponse is the JSON response for the /_cluster_compat endpoint.
type clusterCompatResponse struct {
	ClusterCompatVersion *base.ClusterCompatVersion           `json:"cluster_compat_version"`
	Nodes                map[string]base.ClusterCompatVersion `json:"nodes,omitempty"`
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
