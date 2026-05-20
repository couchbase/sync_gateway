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

// defaultNodeHeartbeatExpiry is the fallback expiry used when node_heartbeat_expiry is unset.
// Sized to tolerate several missed refreshes (at the default config_update_frequency of 10s) before pruning a peer.
const defaultNodeHeartbeatExpiry = 60 * time.Second

// clusterCompatManager tracks the minimum Sync Gateway version across all nodes in the cluster.
// It is used to gate metadata writes so that new formats are only used once all nodes have been
// upgraded.
//
// Node versions are stored in each bucket's _sync:registry document. A node only registers
// itself in a bucket once it has a database configured on that bucket (see RegisterBucket),
// so buckets this SG node is not serving are never touched. Heartbeats and version
// recomputation are driven by the config polling goroutine.
//
// Stale node entries (HeartbeatAt older than the configured heartbeat expiry — see
// heartbeatExpiry()) are pruned by RegisterNodeVersion as part of the same CAS-checked
// registry write that refreshes self's heartbeat.
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
	// appliedDBVersions tracks the database config version this node has successfully
	// applied, keyed by bucket name then database name. Populated by _applyConfig on
	// successful load, consumed by RegisterNodeVersion to stamp the registry.
	appliedDBVersions map[string]map[string]string
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
// with other nodes' simultaneous refreshes. Returns the configured ConfigUpdateFrequency
// verbatim. The validator rejects non-positive values for end users; a zero value can still
// reach this code path via test-only post-Validate overrides that disable polling, in which
// case we fall back to the default so heartbeat math remains coherent.
func (m *clusterCompatManager) refreshInterval() time.Duration {
	if v := m.sc.Config.Bootstrap.ConfigUpdateFrequency.Value(); v > 0 {
		return v
	}
	return persistentConfigDefaultUpdateFrequency
}

// heartbeatExpiry returns the configured node heartbeat expiry, or defaultNodeHeartbeatExpiry if unset.
// The minimum value is enforced at config validation time (StartupConfig.Validate) so trust the config value here.
func (m *clusterCompatManager) heartbeatExpiry() time.Duration {
	if cfg := m.sc.Config.Bootstrap.NodeHeartbeatExpiry; cfg != nil {
		return cfg.Value()
	}
	return defaultNodeHeartbeatExpiry
}

// Start initializes the manager. It does not write to any bucket — node registration is
// performed lazily when a database is loaded on a given bucket (via RegisterBucket).
func (m *clusterCompatManager) Start(_ context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trackedBuckets = make(map[string]struct{})
	m.appliedDBVersions = make(map[string]map[string]string)
}

// RecordAppliedDatabaseVersion records that this node has successfully applied the given
// config version for a database. The version is stamped into the bucket's registry on
// the next RegisterNodeVersion call (heartbeat refresh).
func (m *clusterCompatManager) RecordAppliedDatabaseVersion(bucket, dbName, version string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.appliedDBVersions[bucket] == nil {
		m.appliedDBVersions[bucket] = make(map[string]string)
	}
	m.appliedDBVersions[bucket][dbName] = version
}

// RemoveAppliedDatabaseVersion removes the tracked config version for a database that is
// no longer served by this node.
func (m *clusterCompatManager) RemoveAppliedDatabaseVersion(bucket, dbName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.appliedDBVersions[bucket], dbName)
}

// getAppliedDBVersionsForBucket returns a copy of the applied database versions for the
// given bucket. Returns nil if no versions are tracked for the bucket.
func (m *clusterCompatManager) getAppliedDBVersionsForBucket(bucket string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bucketDBs := m.appliedDBVersions[bucket]
	if len(bucketDBs) == 0 {
		return nil
	}
	cp := make(map[string]string, len(bucketDBs))
	for k, v := range bucketDBs {
		cp[k] = v
	}
	return cp
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
//
// Returns an error if RegisterNodeVersion's cluster compat downgrade gate refuses this node,
// or any other registry write error. On any error the bucket is left untracked — Refresh
// only refreshes already-tracked buckets, so the natural retry path is the next _applyConfig
// (e.g. the next config poll cycle) rather than periodic Refresh.
func (m *clusterCompatManager) RegisterBucket(ctx context.Context, bucket string) error {
	if !m.claimBucket(bucket) {
		return nil
	}
	registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUID, m.sc.Config.Bootstrap.ConfigGroupID, m.sc.BootstrapContext.clusterCompatVersion, m.getAppliedDBVersionsForBucket(bucket), m.heartbeatExpiry())
	if err != nil {
		m.releaseBucket(bucket)
		return err
	}
	// Merge this bucket's registry into the cached view and recompute the min. A node
	// removed from this bucket but still present in another tracked bucket would not
	// be evicted here — that's reconciled by the periodic Refresh, which rebuilds the
	// cache from scratch.
	oldVersion, newVersion := m.mergeRegistryNodesIntoCache(registry.Nodes)
	base.InfofCtx(ctx, base.KeyConfig, "Registered node %s in bucket %s; cluster compatibility version is %v", m.sc.NodeUID, base.MD(bucket), &newVersion)
	if oldVersion != nil && *oldVersion != newVersion {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, &newVersion)
	}
	return nil
}

// claimBucket atomically marks the bucket as tracked and reports whether this call was the
// one that acquired tracking. Returns false if the bucket was already tracked, in which case
// the caller should skip the per-first-time registration work.
func (m *clusterCompatManager) claimBucket(bucket string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, alreadyTracked := m.trackedBuckets[bucket]; alreadyTracked {
		return false
	}
	m.trackedBuckets[bucket] = struct{}{}
	return true
}

// releaseBucket removes the bucket from the tracked set. Used to back out a claim after a
// failed registry write so the bucket isn't pinned to periodic Refresh retries.
func (m *clusterCompatManager) releaseBucket(bucket string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.trackedBuckets, bucket)
}

// mergeRegistryNodesIntoCache folds the given bucket's node entries into the cached cluster
// view, recomputes the cluster compat version, and returns the previous and new cached
// versions for caller-side change logging.
func (m *clusterCompatManager) mergeRegistryNodesIntoCache(nodes map[string]*base.RegistryNode) (oldVersion *base.ClusterCompatVersion, newVersion base.ClusterCompatVersion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cachedNodes == nil {
		m.cachedNodes = make(map[string]base.ClusterCompatVersion, len(nodes))
	}
	for nodeUID, node := range nodes {
		m.cachedNodes[nodeUID] = node.Version
	}
	newVersion = minClusterCompatVersion(m.cachedNodes)
	oldVersion = m.cachedVersion
	m.cachedVersion = &newVersion
	return oldVersion, newVersion
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

	nodeVersion := m.sc.BootstrapContext.clusterCompatVersion
	expiry := m.heartbeatExpiry()
	// Collect unique node versions across all bucket registries. A node appearing in multiple
	// bucket registries will have the same version — last-write-wins is fine.
	nodeMap := make(map[string]base.ClusterCompatVersion)
	succeeded := 0
	for _, bucket := range buckets {
		registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUID, m.sc.Config.Bootstrap.ConfigGroupID, nodeVersion, m.getAppliedDBVersionsForBucket(bucket), expiry)
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
