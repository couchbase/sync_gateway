// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// MigrationStats are aggregate counters populated during a single MigrateMetadata
// invocation. Wiring to Prometheus is the caller's responsibility.
type MigrationStats struct {
	DocsScannedTotal     atomic.Int64
	DocsMigrated         atomic.Int64
	DocsOutOfScope       atomic.Int64
	DocsUnknownPrefix    atomic.Int64
	Errors               atomic.Int64
	SeqPoisonPillApplied atomic.Int64
}

// MigrateMetadata runs a single range-scan pass over the fallback collection and dispatches
// each `_sync:`-rooted key via handleMigrationKey: move to primary, delete, treat as
// out-of-scope, or count as unknown-prefix. The seq counter is handled separately by
// migrateSeqCounter, which the orchestrator calls once before entering the pass loop —
// MigrateMetadata is per-pass and does not re-pill the seq doc on each iteration.
//
// Returns the number of in-scope fallback keys still classified as remaining after the run
// (DocsUnknownPrefix from this pass). The orchestrator drives this in a loop until remaining
// reaches zero — a doc written underneath an in-flight scan can be missed on this pass but
// will surface on the next one.
func MigrateMetadata(ctx context.Context, ms *base.MetadataStore, metadataID string, stats *MigrationStats) (remaining int, err error) {
	if ms == nil {
		return 0, errors.New("MigrateMetadata: nil MetadataStore")
	}
	if stats == nil {
		stats = &MigrationStats{}
	}
	keys := base.NewMetadataKeys(metadataID)

	// Range-scan fallback for `_sync:` keys and dispatch each via handleMigrationKey,
	// which owns both the per-prefix action and the in-scope/out-of-scope decision for
	// this DB.
	rss, ok := base.AsRangeScanStore(ms.Fallback())
	if !ok {
		return 0, errors.New("metadata migration: fallback datastore does not support range scan")
	}
	iter, err := rss.Scan(ctx, sgbucket.NewRangeScanForPrefix(base.SyncDocPrefix), sgbucket.ScanOptions{IDsOnly: true})
	if err != nil {
		return 0, fmt.Errorf("metadata migration scan: %w", err)
	}
	defer func() {
		if closeErr := iter.Close(ctx); closeErr != nil {
			base.WarnfCtx(ctx, "metadata migration scan close: %v", closeErr)
		}
	}()

	for item := iter.Next(ctx); item != nil; item = iter.Next(ctx) {
		// Honour context cancellation between keys so a manager-initiated stop
		// (terminator bridged to ctx cancel) doesn't have to wait for the scan to drain.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return int(stats.DocsUnknownPrefix.Load()), ctxErr
		}
		stats.DocsScannedTotal.Add(1)
		handleMigrationKey(ctx, ms, keys, metadataID, item.ID, stats)
	}

	// In-scope leftovers tell the orchestrator whether to schedule another pass. Out-of-
	// scope keys belong to sibling DBs / bucket-level docs and aren't this pass's concern.
	return int(stats.DocsUnknownPrefix.Load()), nil
}

// migrateSeqCounter is the one-shot per-DB setup step that brings the fallback sequence
// counter forward to primary. It poison-pills the fallback doc (or detects an existing pill
// from a prior crashed run) and triggers the wrapper's Incr self-heal, which initializes
// the primary at LastSeq and deletes the fallback pill. Idempotent and safe to re-run —
// but the orchestrator only calls this once per migration attempt, before entering the
// range-scan pass loop. The pass loop only exists to catch docs written under an in-flight
// scan; the seq counter doesn't have that problem, since once the pill is in place any
// node's Incr can complete the handoff.
func migrateSeqCounter(ctx context.Context, ms *base.MetadataStore, seqKey string, stats *MigrationStats) error {
	err, _ := base.RetryLoopCas(ctx, "migrateSeqCounter", func() (shouldRetry bool, err error, value uint64) {
		// Honour ctx cancellation so a manager-initiated stop (terminator bridged to
		// ctx cancel) doesn't keep the unbounded sleeper spinning on transient errors.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr, 0
		}
		raw, cas, err := ms.Fallback().GetRaw(ctx, seqKey)
		if base.IsDocNotFoundError(err) {
			return false, nil, 0
		}
		if err != nil {
			return true, err, 0
		}
		var existing base.SyncSeqMigrationPill
		if jsonErr := base.JSONUnmarshal(raw, &existing); jsonErr != nil || !existing.SGMetadataMigrationPill {
			var legacy uint64
			if err := base.JSONUnmarshal(raw, &legacy); err != nil {
				return false, err, 0
			}
			pill := base.SyncSeqMigrationPill{
				PilledAt:                time.Now().UTC().Format(time.RFC3339Nano),
				LastSeq:                 legacy,
				SGMetadataMigrationPill: true,
			}
			payload, mErr := base.JSONMarshal(&pill)
			if mErr != nil {
				return false, mErr, 0
			}
			if _, wErr := ms.Fallback().WriteCas(ctx, seqKey, 0, cas, payload, 0); wErr != nil {
				return true, wErr, 0
			}
			stats.SeqPoisonPillApplied.Add(1)
		}
		return false, nil, 0
	}, base.CreateIndefiniteSleeperFunc(10))
	if err != nil {
		return err
	}

	// Touch the seq doc to force migration after the poison pill has been written.
	// Other nodes can opportunistically do this step for us, in case we crash on this exact line, or are too slow.
	if _, incrErr := ms.Incr(ctx, seqKey, 0, 0, 0); incrErr != nil {
		return incrErr
	}

	return nil
}

// handleMigrationKey is the per-key dispatcher. It owns both the in-scope/out-of-scope
// decision and the per-family policy. The switch cases use the database's own
// MetadataKeys prefix accessors so namespacing (the `m_<id>:` infix) is part of the prefix
// match for namespaced metadataIDs. For the legacy `_default` metadataID, where each
// inverted-form prefix (e.g. `_sync:user:`) overlaps with sibling DBs' inverted-form keys
// (`_sync:user:m_<other>:…`), an additional `+ base.MetadataIdPrefix` HasPrefix check
// excludes those sibling keys — for namespaced metadataIDs the additional check is a
// no-op because the prefix already contains `m_<id>:`.
//
// Anything that doesn't match a known family is split between out-of-scope (sibling-DB
// standard form, bucket-level bootstrap docs) and genuinely unknown.
func handleMigrationKey(ctx context.Context, ms *base.MetadataStore, keys *base.MetadataKeys, metadataID, key string, stats *MigrationStats) {
	// isOurs matches a prefix and, where the prefix doesn't already encode our DB's
	// metadataID, defensively excludes sibling-DB inverted-form keys via the trailing
	// `m_` guard. For namespaced metadataIDs the guard is a no-op (the
	// keys.XxxKeyPrefix() already encodes the unique namespace).
	isOurs := func(prefix string) bool {
		return strings.HasPrefix(key, prefix) && !strings.HasPrefix(key, prefix+base.MetadataIdPrefix)
	}
	// thisMetadataID is `_sync:m_<id>:` for namespaced metadataIDs — used to exclude OUR
	// standard-form keys from the sibling-DB out-of-scope check. Empty for default mode
	// (default DB has no `_sync:m_*` keys, so any `_sync:m_…` is necessarily a sibling).
	var thisMetadataID string
	if metadataID != "" && metadataID != base.DefaultMetadataID {
		thisMetadataID = base.SyncDocMetadataPrefix + metadataID + ":"
	}

	switch {
	case
		isOurs(keys.UserKeyPrefix()),
		isOurs(keys.RoleKeyPrefix()),
		isOurs(keys.UserEmailKey("")),
		isOurs(keys.SessionKey("")),
		key == keys.DatabaseStateKey(),
		isOurs(keys.ReplicationStatusKey("")),
		isOurs(keys.SGCfgPrefix("")),
		isOurs(keys.DCPCheckpointPrefix("")),
		isOurs(keys.BackgroundProcessHeartbeatPrefix("")),
		isOurs(keys.BackgroundProcessStatusPrefix("")),
		isOurs(keys.ResyncHeartbeaterPrefix()),
		isOurs(keys.ResyncCfgPrefix()):
		// move to primary (fetch, insert and delete)
		moveFallbackDoc(ctx, ms, key, stats)

	// Sibling DBs' standard form `_sync:m_<other>:…`, but NOT our own metadata ID
	case
		strings.HasPrefix(key, base.SyncDocMetadataPrefix) &&
			(thisMetadataID == "" || !strings.HasPrefix(key, thisMetadataID)):
		stats.DocsOutOfScope.Add(1)

	// Sibling DBs' inverted-form keys (per the migration design doc's `m_` convention)
	// and bucket-level bootstrap docs.
	case
		strings.HasPrefix(key, base.SyncDocPrefix+"user:"+base.MetadataIdPrefix),
		strings.HasPrefix(key, base.SyncDocPrefix+"role:"+base.MetadataIdPrefix),
		strings.HasPrefix(key, base.SyncDocPrefix+"useremail:"+base.MetadataIdPrefix),
		strings.HasPrefix(key, base.SyncDocPrefix+"session:"+base.MetadataIdPrefix),
		strings.HasPrefix(key, base.SyncDocPrefix+base.DCPCheckpointPrefix+base.MetadataIdPrefix),
		key == base.SGRegistryKey,
		strings.HasPrefix(key, base.PersistentConfigPrefixWithoutGroupID),
		key == base.SGSyncInfo:
		// ignore - either other DBs or bucket-level docs that require their own migration logic
		stats.DocsOutOfScope.Add(1)

	case
		// Heartbeater keys are matched on their full shape rather than just the
		// heartbeater prefix — in default mode that prefix is bare `_sync:` and a
		// HasPrefix would swallow every unrecognised in-scope key.
		keys.IsLegacyHeartbeaterKey(key):
		// delete from fallback (don't move)
		deleteFallbackDoc(ctx, ms, key, stats)

	default:
		base.WarnfCtx(ctx, "metadata migration: unrecognised prefix, leaving on fallback: %s", base.UD(key))
		stats.DocsUnknownPrefix.Add(1)
	}
}

// moveFallbackDoc moves a single doc from the fallback collection to the primary
// collection and removes the fallback copy. Idempotent and safe to re-run.
//
// The range scan that drives the migration runs with IDsOnly=true, so we never receive
// bodies on the scan path. The per-key fetch here is a single subdoc LookupIn that
// carries the body and the document's expiry (`$document.exptime`) together, so the
// move preserves TTL (critical for session docs, which have a Couchbase expiry set at
// creation by auth.Authenticator.CreateSession).
//
// The "already exists on primary" outcome is benign and treated as success: a normal
// Authenticator.Update() during an in-flight migration is "read-through fallback, write
// to primary" (see base/dual_metadata_store.go Update), so a primary copy that beat us
// here is fresher than ours and must not be overwritten — we just drop the stale
// fallback shadow. AddRaw returns (added=false, err=nil) on duplicate; the
// IsCasMismatch check is defensive for backends that surface a CAS error instead.
func moveFallbackDoc(ctx context.Context, ms *base.MetadataStore, key string, stats *MigrationStats) {
	raw, expiry, err := fetchFallbackBodyAndExpiry(ctx, ms, key)
	if base.IsDocNotFoundError(err) {
		return
	}
	if err != nil {
		base.WarnfCtx(ctx, "metadata migration: fallback fetch failed for %s: %v", base.UD(key), err)
		stats.Errors.Add(1)
		return
	}
	if _, addErr := ms.Primary().AddRaw(ctx, key, expiry, raw); addErr != nil && !base.IsCasMismatch(addErr) {
		base.WarnfCtx(ctx, "metadata migration: primary add failed for %s: %v", base.UD(key), addErr)
		stats.Errors.Add(1)
		return
	}
	if delErr := ms.Fallback().Delete(ctx, key); delErr != nil && !base.IsDocNotFoundError(delErr) {
		base.WarnfCtx(ctx, "metadata migration: fallback delete failed for %s: %v", base.UD(key), delErr)
		stats.Errors.Add(1)
		return
	}
	stats.DocsMigrated.Add(1)
}

// deleteFallbackDoc just removes the fallback doc
// This is used in cases like transient heartbeat documents where it does not make sense to move.
func deleteFallbackDoc(ctx context.Context, ms *base.MetadataStore, key string, stats *MigrationStats) {
	if delErr := ms.Fallback().Delete(ctx, key); delErr != nil && !base.IsDocNotFoundError(delErr) {
		base.WarnfCtx(ctx, "metadata migration: fallback delete failed for %s: %v", base.UD(key), delErr)
		stats.Errors.Add(1)
		return
	}
	stats.DocsMigrated.Add(1)
}

// fetchFallbackBodyAndExpiry reads a fallback doc's body and expiry in a single
// subdoc LookupIn round trip (one body fetch op + one `$document.exptime` xattr op,
// dispatched together by gocb's GetWithXattrs). Supported by both Couchbase Server and
// Rosmar, so there is no backend-specific path here.
func fetchFallbackBodyAndExpiry(ctx context.Context, ms *base.MetadataStore, key string) (body []byte, expiry uint32, err error) {
	body, xattrs, _, err := ms.Fallback().GetWithXattrs(ctx, key, []string{base.VirtualExpiry})
	if err != nil {
		return nil, 0, err
	}
	if rawExp, ok := xattrs[base.VirtualExpiry]; ok && len(rawExp) > 0 {
		if jsonErr := base.JSONUnmarshal(rawExp, &expiry); jsonErr != nil {
			return nil, 0, fmt.Errorf("parse %s for %s: %w", base.VirtualExpiry, base.UD(key), jsonErr)
		}
	}
	return body, expiry, nil
}
