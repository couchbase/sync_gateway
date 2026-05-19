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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// ErrFreezeNoVersion is returned by clusterCompatManager.Freeze when no cluster compatibility
// version has been observed yet (e.g. immediately after startup, before any node has registered).
var ErrFreezeNoVersion = errors.New("cluster compatibility version not yet computed")

// ErrFreezeNoBucketsWritten is returned by clusterCompatManager.Freeze when there are no
// tracked buckets at all — there is nowhere to persist the freeze. Distinct from
// ErrFreezePartial which signals that some buckets accepted the freeze and others didn't.
var ErrFreezeNoBucketsWritten = errors.New("freeze could not be applied: no tracked buckets")

// ErrFreezePartial is returned by clusterCompatManager.Freeze when one or more tracked
// buckets did not accept the freeze. The cluster compatibility version may only be partially
// pinned in this case — the caller should surface the returned aggregate freeze record so
// the admin can see which version (if any) is still in effect.
var ErrFreezePartial = errors.New("freeze did not fully apply across all tracked buckets")

// ErrUnfreezePartial is returned by clusterCompatManager.Unfreeze when one or more tracked
// buckets still hold a freeze record after the operation. The cluster compatibility version
// may still be held back.
var ErrUnfreezePartial = errors.New("unfreeze did not fully apply across all tracked buckets")

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
	// cachedNodes is the union of node→version across all tracked bucket registries from
	// the most recent observation. Input to computeCCV.
	cachedNodes map[string]base.ClusterCompatVersion
	// cachedFreeze is the aggregate freeze record across tracked bucket registries: any
	// bucket having a freeze record means the cluster is frozen; the aggregate Version is
	// the minimum across those records and FrozenAt is the earliest. Nil when no bucket
	// reports a freeze. Stored separately for surfacing via the API and audit; its Version
	// participates in computeCCV alongside the node versions.
	cachedFreeze *base.RegistryFreeze
	// cachedVersion is the reported cluster compat version: computeCCV(cachedNodes, cachedFreeze).
	// Nil when nothing has been observed yet.
	cachedVersion *base.ClusterCompatVersion
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

// getCachedVersion returns a copy of the currently-reported cluster compat version, or nil
// if not computed.
func (m *clusterCompatManager) getCachedVersion() *base.ClusterCompatVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cachedVersion == nil {
		return nil
	}
	cp := *m.cachedVersion
	return &cp
}

// getCachedFreeze returns a copy of the aggregate cached freeze, or nil if no freeze is set.
func (m *clusterCompatManager) getCachedFreeze() *base.RegistryFreeze {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cachedFreeze == nil {
		return nil
	}
	cp := *m.cachedFreeze
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

// removeAppliedDatabaseVersion removes the tracked config version for a database that is
// no longer served by this node.
func (m *clusterCompatManager) removeAppliedDatabaseVersion(bucket, dbName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.appliedDBVersions[bucket], dbName)
	if len(m.appliedDBVersions[bucket]) == 0 {
		// if no db's are tracked for this bucket, remove the bucket key to avoid leaving an empty map around
		delete(m.appliedDBVersions, bucket)
	}
}

// recordAppliedDBVersion records that this node has successfully applied the given
// config version for a database. The version is stamped into the bucket's registry on
// the next RegisterNodeVersion call (heartbeat refresh). Only run if cluster compat tracking is enabled.
func (m *clusterCompatManager) recordAppliedDBVersion(bucket, dbName, version string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.appliedDBVersions[bucket] == nil {
		m.appliedDBVersions[bucket] = make(map[string]string)
	}
	m.appliedDBVersions[bucket][dbName] = version
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
	// ratchetHWM=false here: HWM is monotonic and cannot be rolled back, so an advance
	// committed off transient startup state would lock the cluster at a too-high value
	// forever. The HWM ratchet happens later via the periodic Refresh, gated per-bucket on
	// at least one database having reached DBOnline (see isBucketRatchetEligible).
	registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUID, m.sc.Config.Bootstrap.ConfigGroupID, m.sc.BootstrapContext.clusterCompatVersion, m.getAppliedDBVersionsForBucket(bucket), m.heartbeatExpiry(), false)
	if err != nil {
		m.releaseBucket(bucket)
		return err
	}
	// Merge this bucket's registry into the cached view and recompute. A node removed from
	// this bucket but still present in another tracked bucket would not be evicted here —
	// that's reconciled by the periodic Refresh, which rebuilds the cache from scratch. Same
	// for the freeze record: a freeze cleared elsewhere stays reflected here until the next
	// Refresh recomputes the aggregate.
	oldVersion, newVersion := m.mergeRegistryIntoCache(registry.Nodes, registry.Frozen)
	base.InfofCtx(ctx, base.KeyConfig, "Registered node %s in bucket %s; cluster compatibility version is %v", m.sc.NodeUID, base.MD(bucket), newVersion)
	if !clusterCompatVersionEqual(oldVersion, newVersion) {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, newVersion)
	}
	return nil
}

// isBucketRatchetEligible reports whether at least one database on this bucket has reached
// DBOnline. Used by Refresh to gate the HWM ratchet on per-bucket online state: ratcheting
// requires inputs (e.g. legacy-node detection via ISGR/cbgt) that are only available once
// the database is fully online. Returning false leaves the bucket heartbeat-only until the
// next refresh tick.
//
// Reads the database state directly from ServerContext rather than maintaining a shadow set
// — the DB state is authoritative and follows offline transitions automatically.
func (m *clusterCompatManager) isBucketRatchetEligible(bucket string) bool {
	m.sc._databasesLock.RLock()
	defer m.sc._databasesLock.RUnlock()
	for _, dbContext := range m.sc._databases {
		if dbContext.Bucket.GetName() != bucket {
			continue
		}
		if atomic.LoadUint32(&dbContext.State) == db.DBOnline {
			return true
		}
	}
	return false
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

// mergeRegistryIntoCache folds the given bucket's node entries and freeze record into the
// cached cluster view, recomputes the cluster compat version, and returns the previous and
// new cached versions for caller-side change logging.
func (m *clusterCompatManager) mergeRegistryIntoCache(nodes map[string]*base.RegistryNode, freeze *base.RegistryFreeze) (oldVersion, newVersion *base.ClusterCompatVersion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cachedNodes == nil {
		m.cachedNodes = make(map[string]base.ClusterCompatVersion, len(nodes))
	}
	for nodeUID, node := range nodes {
		m.cachedNodes[nodeUID] = node.Version
	}
	if freeze != nil {
		m.cachedFreeze = mergeFreeze(m.cachedFreeze, freeze)
	}
	oldVersion = m.cachedVersion
	newVersion = computeCCV(m.cachedNodes, m.cachedFreeze)
	m.cachedVersion = newVersion
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

	nodes, freeze, err := m.refreshNodeRegistrations(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Failed to refresh cluster compat version: %v", err)
		return
	}
	newVersion := computeCCV(nodes, freeze)

	// Race window: if a Freeze or Unfreeze completed between refreshNodeRegistrations
	// returning and the lock acquisition below, this write overwrites their mutation with
	// data that was observed *before* the admin op ran. The cache will be stale (a phantom
	// freeze or a missing freeze) until the next periodic Refresh rebuilds from authoritative
	// bucket state. Admin endpoints are rare, the window is bounded by the bucket I/O above,
	// and the self-heal is automatic — accepted as a transient over more locking machinery.
	m.mu.Lock()
	oldVersion := m.cachedVersion
	m.cachedNodes = nodes
	m.cachedFreeze = freeze
	m.cachedVersion = newVersion
	m.lastRefreshAt = time.Now().UTC()
	m.mu.Unlock()

	if !clusterCompatVersionEqual(oldVersion, newVersion) {
		base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version changed from %v to %v", oldVersion, newVersion)
	}
}

// refreshNodeRegistrations iterates the tracked buckets, registers this node, and returns
// the per-node version map and the aggregate freeze record across those registries. Returns
// an error if every tracked bucket failed so callers can leave the previously-cached state
// in place — stale is preferable to flipping the cluster compat version to nil on a transient
// bucket outage.
func (m *clusterCompatManager) refreshNodeRegistrations(ctx context.Context) (map[string]base.ClusterCompatVersion, *base.RegistryFreeze, error) {
	buckets := m.trackedBucketList()
	if len(buckets) == 0 {
		return nil, nil, nil
	}

	nodeVersion := m.sc.BootstrapContext.clusterCompatVersion
	expiry := m.heartbeatExpiry()
	// Collect unique node versions across all bucket registries. A node appearing in multiple
	// bucket registries will have the same version — last-write-wins is fine.
	nodeMap := make(map[string]base.ClusterCompatVersion)
	var aggregateFreeze *base.RegistryFreeze
	succeeded := 0
	for _, bucket := range buckets {
		// Gate HWM ratchet on per-bucket online state — see isBucketRatchetEligible. Heartbeat
		// refresh happens unconditionally so the node entry stays fresh even for buckets whose
		// databases haven't come online yet.
		ratchet := m.isBucketRatchetEligible(bucket)
		registry, err := m.sc.BootstrapContext.RegisterNodeVersion(ctx, bucket, m.sc.NodeUID, m.sc.Config.Bootstrap.ConfigGroupID, nodeVersion, m.getAppliedDBVersionsForBucket(bucket), expiry, ratchet)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to register node version in bucket %s: %v", base.MD(bucket), err)
			continue
		}
		succeeded++
		for nodeUID, node := range registry.Nodes {
			nodeMap[nodeUID] = node.Version
		}
		if registry.Frozen != nil {
			aggregateFreeze = mergeFreeze(aggregateFreeze, registry.Frozen)
		}
	}
	if succeeded == 0 {
		return nil, nil, fmt.Errorf("no tracked bucket registries could be updated (%d tracked)", len(buckets))
	}
	return nodeMap, aggregateFreeze, nil
}

// computeCCV returns the reported cluster compat version: the minimum across the live-node
// versions and the freeze ceiling (when set). Returns nil when no versions are available.
//
// The single combining point for all CCV inputs. Future inputs (e.g. an HWM floor) should
// participate here rather than as a separate override path.
func computeCCV(nodes map[string]base.ClusterCompatVersion, freeze *base.RegistryFreeze) *base.ClusterCompatVersion {
	versions := make([]base.ClusterCompatVersion, 0, len(nodes)+1)
	for _, v := range nodes {
		versions = append(versions, v)
	}
	if freeze != nil {
		versions = append(versions, freeze.Version)
	}
	if len(versions) == 0 {
		return nil
	}
	v := base.MinClusterCompatVersion(versions...)
	return &v
}

// mergeFreeze combines two freeze records into one. The result has the minimum Version (so a
// lower-versioned freeze wins) and the earliest FrozenAt (the original freeze time). Either
// argument may be nil. Returns a fresh RegistryFreeze pointer.
func mergeFreeze(a, b *base.RegistryFreeze) *base.RegistryFreeze {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		cp := *b
		return &cp
	}
	if b == nil {
		cp := *a
		return &cp
	}
	out := &base.RegistryFreeze{
		Version:  base.MinClusterCompatVersion(a.Version, b.Version),
		FrozenAt: a.FrozenAt,
	}
	if b.FrozenAt.Before(out.FrozenAt) {
		out.FrozenAt = b.FrozenAt
	}
	return out
}

// Freeze captures the currently-reported cluster compat version into every tracked bucket
// registry, pinning the cluster from advancing past it. Success requires that every tracked
// bucket accepts the freeze: a partial freeze would leave bucket registries (and any
// downgrade-gate decisions keyed off them) in an inconsistent state.
//
// Returns ErrFreezeNoVersion if no version has been observed yet (e.g. immediately after
// startup). Returns ErrFreezeNoBucketsWritten if there are no tracked buckets at all.
// Returns ErrFreezePartial alongside whatever aggregate freeze did take effect when one or
// more tracked buckets failed to accept the freeze — callers should surface the aggregate to
// the admin so they can see which version is currently pinned and on which buckets.
//
// On full success the manager's cached freeze and reported version are updated and the
// returned record is the aggregate freeze now in effect across all tracked buckets. On
// partial failure the cache is updated to reflect what did take effect, and the returned
// aggregate is the partial freeze. If *no* bucket accepted the freeze (succeeded==0) the
// cache is deliberately left untouched: a transient outage that fails every bucket must
// not erase a real, persistent freeze from the reporting endpoint — Refresh will
// self-heal once the buckets come back.
//
// Locking: the cached cluster compat version is snapshotted under m.mu.RLock at the start
// and the RLock is held across all bucket writes. This blocks concurrent Refresh from
// completing its post-I/O write phase while a freeze is in progress, so the version we
// write is guaranteed not to shift relative to what was reported by GET. Bucket I/O is
// otherwise rare admin work, so the contention cost is negligible.
//
// Cross-bucket drift on retry: SetRegistryFreeze is idempotent — once a bucket has a freeze
// record, subsequent calls return the existing record unchanged. If a Freeze partially
// fails and the cluster's live-node minimum then advances before the admin retries, the
// retry will leave already-frozen buckets at the original version while writing the newer
// version to the previously-failed buckets. The mergeFreeze aggregate keeps the reported
// version at the minimum so clients see a consistent CCV, but per-bucket HWM caps (see
// RegisterNodeVersion in config_manager.go) use each bucket's local freeze. The practical
// effect is that HWM may advance further on the higher-version bucket — which errs toward
// refusing downgrades, the safe direction.
func (m *clusterCompatManager) Freeze(ctx context.Context) (*base.RegistryFreeze, error) {
	m.mu.RLock()
	if m.cachedVersion == nil {
		m.mu.RUnlock()
		return nil, ErrFreezeNoVersion
	}
	current := *m.cachedVersion
	buckets := make([]string, 0, len(m.trackedBuckets))
	for b := range m.trackedBuckets {
		buckets = append(buckets, b)
	}
	if len(buckets) == 0 {
		m.mu.RUnlock()
		return nil, ErrFreezeNoBucketsWritten
	}
	// Hold RLock across the bucket writes so cachedVersion cannot shift under us.
	var aggregate *base.RegistryFreeze
	succeeded := 0
	for _, bucket := range buckets {
		freeze, err := m.sc.BootstrapContext.SetRegistryFreeze(ctx, bucket, current)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to set cluster compat version freeze in bucket %s: %v", base.MD(bucket), err)
			continue
		}
		succeeded++
		aggregate = mergeFreeze(aggregate, freeze)
	}
	m.mu.RUnlock()

	// Only mutate the cache when at least one bucket accepted the freeze. If every bucket
	// failed, aggregate is nil and writing it would wipe any previously-cached freeze — a
	// transient outage should not erase persistent state. Refresh self-heals from the
	// authoritative bucket registries on its next tick.
	if succeeded > 0 {
		m.mu.Lock()
		m.cachedFreeze = aggregate
		m.cachedVersion = computeCCV(m.cachedNodes, m.cachedFreeze)
		m.mu.Unlock()
	}

	if succeeded < len(buckets) {
		base.WarnfCtx(ctx, "Cluster compatibility version freeze did not fully apply: %d/%d tracked buckets accepted the freeze", succeeded, len(buckets))
		return aggregate, ErrFreezePartial
	}
	base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version frozen at %v (applied to %d/%d tracked buckets)", &aggregate.Version, succeeded, len(buckets))
	return aggregate, nil
}

// Unfreeze clears the freeze from every tracked bucket registry. Unlike Freeze, this is
// success-on-all: if any bucket still has a freeze record after the operation, the cluster
// remains held back, so an error is returned along with the residual freeze record so the
// caller can surface the remaining state to the admin.
//
// Returns ErrUnfreezePartial in two situations the caller should disambiguate using the
// returned residual:
//   - residual != nil: at least one bucket still has a freeze record on re-read. The cache
//     is updated to that aggregate so the reporting endpoint reflects on-disk truth.
//   - residual == nil: one or more bucket clears failed AND the post-failure re-reads also
//     failed, so the actual on-disk state is unknown. The cache is deliberately left
//     untouched in this case — the pre-op snapshot is the most honest representation until
//     the next periodic Refresh self-heals from authoritative bucket state.
//
// previousFreeze is the aggregate freeze that was in effect at the start of the call
// (captured under lock before any mutation) — i.e. the freeze record that the unfreeze is
// attempting to lift, not what got cleared. It is returned even on partial failure so
// handlers can populate audit fields or error messages without a separate peek-then-clear
// that could race with Refresh.
func (m *clusterCompatManager) Unfreeze(ctx context.Context) (previousFreeze, residual *base.RegistryFreeze, err error) {
	m.mu.RLock()
	if m.cachedFreeze != nil {
		cp := *m.cachedFreeze
		previousFreeze = &cp
	}
	m.mu.RUnlock()

	buckets := m.trackedBucketList()
	if len(buckets) == 0 {
		m.mu.Lock()
		m.cachedFreeze = nil
		m.cachedVersion = computeCCV(m.cachedNodes, nil)
		m.mu.Unlock()
		return previousFreeze, nil, nil
	}
	clearFailed := 0
	for _, bucket := range buckets {
		err := m.sc.BootstrapContext.ClearRegistryFreeze(ctx, bucket)
		if err != nil {
			clearFailed++
			base.WarnfCtx(ctx, "Failed to clear cluster compat version freeze in bucket %s: %v", base.MD(bucket), err)
			// Re-read to discover the freeze still in effect on this bucket.
			registry, getErr := m.sc.BootstrapContext.getGatewayRegistry(ctx, bucket)
			if getErr != nil {
				base.WarnfCtx(ctx, "Failed to re-read registry for bucket %s after clear failure: %v", base.MD(bucket), getErr)
				continue
			}
			if registry.Frozen != nil {
				residual = mergeFreeze(residual, registry.Frozen)
			}
		}
	}
	// Only mutate the cache when we have certainty about on-disk state: a full success
	// (clearFailed == 0) clears it; a verified residual replaces it. If buckets failed and
	// re-reads also failed, leave the cache as-is so the reporting endpoint keeps showing
	// the pre-op snapshot until Refresh self-heals.
	if clearFailed == 0 || residual != nil {
		m.mu.Lock()
		m.cachedFreeze = residual
		m.cachedVersion = computeCCV(m.cachedNodes, m.cachedFreeze)
		m.mu.Unlock()
	}
	if residual != nil {
		base.WarnfCtx(ctx, "Cluster compatibility version unfreeze did not fully apply: %d/%d buckets failed; cluster still frozen at %v", clearFailed, len(buckets), &residual.Version)
		return previousFreeze, residual, ErrUnfreezePartial
	}
	if clearFailed > 0 {
		// Buckets failed and re-read couldn't verify residual state — return error anyway,
		// since the admin asked for a guarantee and we can't make one. Cache is preserved.
		return previousFreeze, nil, ErrUnfreezePartial
	}
	base.InfofCtx(ctx, base.KeyConfig, "Cluster compatibility version freeze cleared on %d buckets", len(buckets))
	return previousFreeze, nil, nil
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
