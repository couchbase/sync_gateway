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
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func newMigrationTestStore(t *testing.T, bucket *base.TestBucket) *base.MetadataStore {
	t.Helper()
	ctx := base.TestCtx(t)
	primary, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	fallback := bucket.DefaultDataStore(ctx)
	if _, ok := base.AsRangeScanStore(fallback); !ok {
		t.Skipf("metadata migration requires KV range scan support on the fallback datastore")
	}
	return base.NewMetadataStore(primary, fallback)
}

func seedFallback(ctx context.Context, t *testing.T, ms *base.MetadataStore, key string, body []byte) {
	t.Helper()
	_, err := ms.Fallback().AddRaw(ctx, key, 0, body)
	require.NoError(t, err, "seed fallback %s", key)
	// KV range scans read from a per-vBucket snapshot view, so a scan issued immediately after
	// the write can miss the just-seeded doc until the vBucket's scan view catches up. Block here
	// so callers can't forget to wait for visibility before exercising scan-backed code.
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), []string{key})
}

// TestMigrateMetadataEmptyFallback verifies the new-DB fast path: empty fallback yields a
// zero remaining count and no errors.
func TestMigrateMetadataEmptyFallback(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newMigrationTestStore(t, bucket)
	stats := &MigrationStats{}

	remaining, err := MigrateMetadata(ctx, ms, "testEmpty", nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Zero(t, stats.DocsScannedTotal.Load())
}

// TestMigrateSeqCounterPromotesFallbackToPrimary verifies the one-shot seq counter setup
// step: pill the fallback doc, nudge the wrapper, end with the counter on primary and the
// fallback key gone. Called directly because the orchestrator hoists this out of the
// per-pass MigrateMetadata loop — it is a per-DB setup, not per-pass work.
func TestMigrateSeqCounterPromotesFallbackToPrimary(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testSeq"
	keys := base.NewMetadataKeys(metadataID)
	seqKey := keys.SyncSeqKey()

	ms := newMigrationTestStore(t, bucket)
	_, err := ms.Fallback().Incr(ctx, seqKey, 42, 42, 0)
	require.NoError(t, err)

	stats := &MigrationStats{}
	require.NoError(t, migrateSeqCounter(ctx, ms, seqKey, stats))
	assert.Equal(t, int64(1), stats.SeqPoisonPillApplied.Load())

	_, _, err = ms.Fallback().GetRaw(ctx, seqKey)
	assert.True(t, base.IsDocNotFoundError(err), "expected fallback seq key gone after the run")

	result, err := ms.Primary().Incr(ctx, seqKey, 1, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(43), result, "primary counter should be seeded from fallback last_seq")
}

// TestMigrateMetadataUnknownPrefixCountedAsRemaining verifies that an in-scope key matching
// none of the known families is warned about, counted as DocsUnknownPrefix, and reported as
// remaining so the orchestrator's loop can decide what to do.
func TestMigrateMetadataUnknownPrefixCountedAsRemaining(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testUnknown"
	ms := newMigrationTestStore(t, bucket)
	// `_sync:m_testUnknown:wat` is in scope (matches our standard-form prefix
	// `_sync:m_testUnknown:`) but doesn't match any known family (seq/hb/cfg/etc.).
	unknownKey := base.SyncDocPrefix + base.MetadataIdPrefix + metadataID + ":wat"
	seedFallback(ctx, t, ms, unknownKey, []byte(`{"body":"unknown"}`))

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 1, remaining, "unknown-prefix doc should be reported as remaining")
	assert.Equal(t, int64(1), stats.DocsUnknownPrefix.Load())

	_, _, getErr := ms.Fallback().GetRaw(ctx, unknownKey)
	assert.NoError(t, getErr, "unknown-prefix doc should not be deleted")
}

// TestMigrateMetadataNamespacedExcludesSiblingDB verifies that running with a namespaced
// metadataID treats a sibling DB's standard-form keys as out-of-scope without inspecting
// the registry.
func TestMigrateMetadataNamespacedExcludesSiblingDB(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newMigrationTestStore(t, bucket)

	// Sibling DB's standard form is structurally distinguishable: it starts with
	// `_sync:m_<other>:` (different namespace).
	const siblingKey = "_sync:m_otherDB:hb:groupA"
	seedFallback(ctx, t, ms, siblingKey, []byte(`{"heartbeat":"sibling"}`))

	stats := &MigrationStats{}
	_, err := MigrateMetadata(ctx, ms, "myDB", nil, nil, stats)
	require.NoError(t, err)

	assert.Equal(t, int64(1), stats.DocsOutOfScope.Load())

	_, _, err = ms.Fallback().GetRaw(ctx, siblingKey)
	assert.NoError(t, err, "sibling DB key must remain on the fallback")
}

// TestMigrateSeqCounterIdempotent verifies that a second invocation (e.g. after a manager
// restart, or any retry path) doesn't re-pill the fallback and doesn't fail. The function's
// idempotency guarantee is what lets the orchestrator simply call it once-per-attempt with
// no special crash-recovery bookkeeping.
func TestMigrateSeqCounterIdempotent(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testIdempotent"
	keys := base.NewMetadataKeys(metadataID)
	seqKey := keys.SyncSeqKey()

	ms := newMigrationTestStore(t, bucket)
	_, err := ms.Fallback().Incr(ctx, seqKey, 7, 7, 0)
	require.NoError(t, err)

	first := &MigrationStats{}
	require.NoError(t, migrateSeqCounter(ctx, ms, seqKey, first))
	assert.Equal(t, int64(1), first.SeqPoisonPillApplied.Load())

	second := &MigrationStats{}
	require.NoError(t, migrateSeqCounter(ctx, ms, seqKey, second))
	assert.Zero(t, second.SeqPoisonPillApplied.Load(), "no pill on second run — fallback already clean")
}

// TestMigrateMetadataMovesUserAndRoleDocs verifies the per-key migration action wired up
// for user and role docs: each seeded key is byte-identically promoted to the primary
// collection and removed from the fallback, and the stats counters reflect the moves.
func TestMigrateMetadataMovesUserAndRoleDocs(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testMoveUserRole"
	keys := base.NewMetadataKeys(metadataID)

	ms := newMigrationTestStore(t, bucket)
	seeded := map[string][]byte{
		keys.UserKey("alice"):   []byte(`{"name":"alice"}`),
		keys.UserKey("bob"):     []byte(`{"name":"bob"}`),
		keys.RoleKey("admins"):  []byte(`{"name":"admins"}`),
		keys.RoleKey("readers"): []byte(`{"name":"readers"}`),
	}
	for k, v := range seeded {
		seedFallback(ctx, t, ms, k, v)
	}

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(len(seeded)), stats.DocsMigrated.Load())
	assert.Zero(t, stats.Errors.Load())

	for k, want := range seeded {
		got, _, getErr := ms.Primary().GetRaw(ctx, k)
		require.NoError(t, getErr, "primary should hold %s", k)
		assert.Equal(t, want, got, "primary body for %s should match the seed", k)

		_, _, getErr = ms.Fallback().GetRaw(ctx, k)
		assert.True(t, base.IsDocNotFoundError(getErr), "fallback should no longer hold %s", k)
	}
}

// TestMigrateMetadataMovesUserEmailAndSession verifies that the per-key move action is
// also wired for useremail and session docs, and — critically for sessions — that the
// fallback doc's expiry is preserved when the doc lands on the primary collection.
// auth.Authenticator.CreateSession writes sessions with a TTL via Set's exp argument;
// if the migration stripped that TTL, sessions would silently become immortal.
func TestMigrateMetadataMovesUserEmailAndSession(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testMoveUserEmailSession"
	keys := base.NewMetadataKeys(metadataID)

	ms := newMigrationTestStore(t, bucket)

	emailKey := keys.UserEmailKey("alice@example.com")
	emailBody := []byte(`{"username":"alice"}`)
	seedFallback(ctx, t, ms, emailKey, emailBody)

	sessionKey := keys.SessionKey("tok1")
	sessionBody := []byte(`{"sid":"tok1","username":"alice"}`)
	// Use a 1h offset; both Rosmar and CBS convert this to an absolute epoch at write
	// time, so a later GetExpiry returns a non-zero value we can compare against.
	const sessionExpRelative uint32 = 3600
	_, err := ms.Fallback().AddRaw(ctx, sessionKey, sessionExpRelative, sessionBody)
	require.NoError(t, err, "seed fallback session with TTL")

	seededSessionExpiry, err := ms.Fallback().GetExpiry(ctx, sessionKey)
	require.NoError(t, err)
	require.NotZero(t, seededSessionExpiry, "fallback should have a non-zero absolute expiry after a TTL write")

	// Wait for the fallback docs to be visible to range scans before running the migration.
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), []string{emailKey, sessionKey})

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(2), stats.DocsMigrated.Load(), "useremail and session should both be moved")
	assert.Zero(t, stats.Errors.Load())

	// useremail — body intact, no expiry.
	gotEmail, _, err := ms.Primary().GetRaw(ctx, emailKey)
	require.NoError(t, err, "primary should hold useremail key")
	assert.Equal(t, emailBody, gotEmail)
	_, _, err = ms.Fallback().GetRaw(ctx, emailKey)
	assert.True(t, base.IsDocNotFoundError(err), "fallback useremail shadow should be gone")

	// session — body intact AND expiry preserved.
	gotSession, _, err := ms.Primary().GetRaw(ctx, sessionKey)
	require.NoError(t, err, "primary should hold session key")
	assert.Equal(t, sessionBody, gotSession)
	primarySessionExpiry, err := ms.Primary().GetExpiry(ctx, sessionKey)
	require.NoError(t, err)
	// Allow a small drift on the absolute epoch — gocb's InsertOptions take a
	// time.Duration, so absolute expiries are round-tripped through
	// time.Until(...) on read and re-derived from "now" on write. A few seconds
	// of wall-time elapse between the fallback read and the primary write means
	// primary lands marginally before fallback. The point of this assertion is
	// "TTL preserved", not "epoch byte-equal" — anything within ~10s is fine
	// for session TTLs measured in hours.
	assert.InDelta(t, seededSessionExpiry, primarySessionExpiry, 10, "session expiry must round-trip through migration approximately unchanged")
	_, _, err = ms.Fallback().GetRaw(ctx, sessionKey)
	assert.True(t, base.IsDocNotFoundError(err), "fallback session shadow should be gone")
}

// TestMigrateMetadataUserDocPrimaryWinsOnConflict verifies the already-exists short-
// circuit: when primary already has a (fresher) copy from an in-flight read-through
// Update, the migration must not overwrite it — but should still clean up the stale
// fallback shadow.
func TestMigrateMetadataUserDocPrimaryWinsOnConflict(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testMoveConflict"
	keys := base.NewMetadataKeys(metadataID)
	key := keys.UserKey("alice")

	ms := newMigrationTestStore(t, bucket)
	fresherPrimary := []byte(`{"name":"alice","fresh":true}`)
	stalerFallback := []byte(`{"name":"alice","fresh":false}`)
	_, err := ms.Primary().AddRaw(ctx, key, 0, fresherPrimary)
	require.NoError(t, err)
	seedFallback(ctx, t, ms, key, stalerFallback)

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "already-exists short-circuit should count as migrated")
	assert.Zero(t, stats.Errors.Load())

	got, _, err := ms.Primary().GetRaw(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, fresherPrimary, got, "primary body must not be overwritten by the stale fallback shadow")

	_, _, err = ms.Fallback().GetRaw(ctx, key)
	assert.True(t, base.IsDocNotFoundError(err), "stale fallback shadow should be cleaned up")
}

// TestMigrateMetadataLegacyDefaultUserAndRole verifies user/role moves work for the
// legacy default metadataID, whose keys have no `m_<id>:` infix (e.g. `_sync:user:alice`).
// The existing TestHandleMigrationKeyScoping/LegacyDefaultOurs case only verifies
// classification; this exercises the actual move.
func TestMigrateMetadataLegacyDefaultUserAndRole(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	keys := base.NewMetadataKeys(base.DefaultMetadataID)

	ms := newMigrationTestStore(t, bucket)
	seeded := map[string][]byte{
		keys.UserKey("alice"):  []byte(`{"name":"alice","legacy":true}`),
		keys.RoleKey("admins"): []byte(`{"name":"admins","legacy":true}`),
	}
	for k, v := range seeded {
		seedFallback(ctx, t, ms, k, v)
	}

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(len(seeded)), stats.DocsMigrated.Load())
	assert.Zero(t, stats.Errors.Load())

	for k, want := range seeded {
		got, _, getErr := ms.Primary().GetRaw(ctx, k)
		require.NoError(t, getErr, "primary should hold legacy-default %s", k)
		assert.Equal(t, want, got)

		_, _, getErr = ms.Fallback().GetRaw(ctx, k)
		assert.True(t, base.IsDocNotFoundError(getErr), "fallback should no longer hold legacy-default %s", k)
	}
}

// TestMigrateMetadataDefaultModeUnknownPreservedHeartbeatDeleted exercises the precise
// heartbeater-vs-unknown distinction in default mode end-to-end. The default-mode
// heartbeater prefix is bare `_sync:`, so before IsHeartbeaterKey the unknown legacy
// key would have been classified as a heartbeat and deleted. Both branches need a live
// fallback (Delete + leave-on-fallback), so this is a bucket-backed test rather than a
// nil-ms classification subtest.
func TestMigrateMetadataDefaultModeUnknownPreservedHeartbeatDeleted(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newMigrationTestStore(t, bucket)

	const unknownKey = "_sync:somethingnew:foo"
	const heartbeatKey = "_sync:heartbeat_timeout:nodeA"
	seedFallback(ctx, t, ms, unknownKey, []byte(`{"unknown":"shape"}`))
	seedFallback(ctx, t, ms, heartbeatKey, []byte("nodeA"))

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 1, remaining, "unknown-prefix doc must keep the migration in 'work remaining' state")
	assert.Equal(t, int64(1), stats.DocsUnknownPrefix.Load(), "%q must be classified as unknown-prefix, not as a heartbeat", unknownKey)
	assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "%q (real heartbeat) should be deleted and counted as migrated", heartbeatKey)
	assert.Zero(t, stats.Errors.Load())

	_, _, getErr := ms.Fallback().GetRaw(ctx, unknownKey)
	assert.NoError(t, getErr, "unknown-prefix doc must NOT be deleted — that was the bug")

	_, _, getErr = ms.Fallback().GetRaw(ctx, heartbeatKey)
	assert.True(t, base.IsDocNotFoundError(getErr), "heartbeat doc should be removed from fallback")
}

// TestMigrateMetadataNamespacedHeartbeatWithGroupIDDeletedViaShapeMatch pins the fix
// for EE-with-import heartbeater key matching: when a non-empty groupID is configured,
// `HeartbeaterPrefix` returns `_sync:m_<id>:hb:<groupID>:`, so the heartbeater writes
// `<heartbeaterPrefix>heartbeat_timeout:<nodeUUID>` — i.e. the on-disk key has an extra
// `<groupID>:` colon-segment between the namespace and the literal `heartbeat_timeout:`
// token. The original `IsHeartbeaterKey` only matched the no-groupID form, so the
// with-groupID heartbeat key fell into DocsUnknownPrefix and wedged the migration on
// every real EE bucket.
func TestMigrateMetadataNamespacedHeartbeatWithGroupIDDeletedViaShapeMatch(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testHbShapeGroup"
	const groupID = "groupA"
	keys := base.NewMetadataKeys(metadataID)
	heartbeatKey := keys.HeartbeaterPrefix(groupID) + "heartbeat_timeout:nodeA"

	ms := newMigrationTestStore(t, bucket)
	seedFallback(ctx, t, ms, heartbeatKey, []byte("nodeA"))

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining, "groupID-suffixed heartbeat must not surface as unknown-prefix")
	assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "groupID-suffixed heartbeat doc should be deleted via the shape match")
	assert.Zero(t, stats.DocsUnknownPrefix.Load())

	_, _, getErr := ms.Fallback().GetRaw(ctx, heartbeatKey)
	assert.True(t, base.IsDocNotFoundError(getErr), "groupID-suffixed heartbeat doc should be removed from fallback")
}

// TestMigrateMetadataNamespacedHeartbeatDeletedViaShapeMatch verifies the same shape-based
// heartbeater match works for namespaced metadataIDs, where the heartbeater prefix is
// `_sync:m_<id>:hb:` and the doc shape is `<prefix>heartbeat_timeout:<nodeUUID>`.
func TestMigrateMetadataNamespacedHeartbeatDeletedViaShapeMatch(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testHbShape"
	keys := base.NewMetadataKeys(metadataID)
	heartbeatKey := keys.HeartbeaterPrefix("") + "heartbeat_timeout:nodeA"

	ms := newMigrationTestStore(t, bucket)
	seedFallback(ctx, t, ms, heartbeatKey, []byte("nodeA"))

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "namespaced heartbeat doc should be deleted via the shape match")
	assert.Zero(t, stats.DocsUnknownPrefix.Load())

	_, _, getErr := ms.Fallback().GetRaw(ctx, heartbeatKey)
	assert.True(t, base.IsDocNotFoundError(getErr), "namespaced heartbeat doc should be removed from fallback")
}

// TestHandleMigrationKeyScoping covers the in-scope/out-of-scope decision baked into
// handleMigrationKey, verifying that namespaced and legacy-default modes classify keys
// correctly without going through the full scan path.
//
// Keys here are constructed via the live MetadataKeys helpers so the test follows the
// actual on-disk shape rather than the (currently inaccurate) shape suggested in the
// metadata-migration design notes.
func TestHandleMigrationKeyScoping(t *testing.T) {
	ctx := base.TestCtx(t)

	classify := func(metadataID string, siblingMetadataIDs []string, key string) *MigrationStats {
		stats := &MigrationStats{}
		handleMigrationKey(ctx, nil, base.NewMetadataKeys(metadataID), metadataID, siblingMetadataIDs, nil, key, stats)
		return stats
	}

	t.Run("NamespacedSiblingStandardForm", func(t *testing.T) {
		// Sibling DBs' standard-form keys are `_sync:m_<other>:…` — structurally
		// distinguishable from ours via the metadataID infix.
		for _, k := range []string{
			"_sync:m_otherDB:seq",
			"_sync:m_otherDB:hb:groupA",
		} {
			s := classify("myDB", nil, k)
			assert.Equal(t, int64(1), s.DocsOutOfScope.Load(), "%q should be out of scope for myDB", k)
		}
	})
	t.Run("BucketLevelOutOfScope", func(t *testing.T) {
		for _, k := range []string{
			"_sync:registry",
			"_sync:dbconfig:default",
			"_sync:syncInfo",
		} {
			s := classify("myDB", nil, k)
			assert.Equal(t, int64(1), s.DocsOutOfScope.Load(), "%q is a bucket-level doc — out of scope", k)
		}
	})

	// DefaultModeUnknownPrefixNotSwallowed pins the fix for the heartbeater catch-all
	// shadowing problem: with DefaultMetadataID the heartbeater prefix is bare `_sync:`,
	// which previously meant any unrecognised in-scope `_sync:…` key fell into the
	// heartbeater branch and got deleted. With the IsHeartbeaterKey shape check the
	// dispatcher must instead route unknown keys to the warn/DocsUnknownPrefix branch.
	t.Run("DefaultModeUnknownPrefixNotSwallowed", func(t *testing.T) {
		for _, k := range []string{
			"_sync:somethingnew:foo",
			"_sync:futurefeature",
		} {
			s := classify(base.DefaultMetadataID, nil, k)
			assert.Equal(t, int64(1), s.DocsUnknownPrefix.Load(), "%q must be unknown-prefix, not classified as a heartbeat", k)
			assert.Zero(t, s.DocsOutOfScope.Load(), "%q is in-scope (default mode owns `_sync:…`) so should not be out-of-scope", k)
		}
	})

	// LegacySiblingInvertedFormOutOfScope pins the sibling-side of the corner-case fix:
	// when the local DB has default metadataID, a co-located sibling's inverted-form
	// keys must be classified out-of-scope. The classifier recognises them by matching
	// the segment after the inverted prefix against the authoritative sibling-metadataID
	// list from the registry (`<siblingID>:`) — there is no `m_` in inverted keys
	// (`formatInvertedMetadataKey` omits it, so a real sibling "otherDB" produces
	// `_sync:user:otherDB:alice`. Without the sibling list the default DB cannot tell these from its own
	// keys, so the list must be supplied.
	t.Run("LegacySiblingInvertedFormOutOfScope", func(t *testing.T) {
		for _, k := range []string{
			"_sync:user:otherDB:alice",
			"_sync:role:otherDB:admins",
			"_sync:useremail:otherDB:alice@example.com",
			"_sync:session:otherDB:tok1",
		} {
			s := classify(base.DefaultMetadataID, []string{"otherDB"}, k)
			assert.Equal(t, int64(1), s.DocsOutOfScope.Load(), "%q is a sibling-DB inverted-form key — must remain out-of-scope", k)
		}
	})
}

// migrationDisposition is the per-key routing decision handleMigrationKey makes for a
// single fallback key. It is the unit of coverage for TestHandleMigrationKeyClassification.
type migrationDisposition int

const (
	// dispMigrated: an in-scope metadata doc moved from fallback to primary (DocsMigrated++).
	dispMigrated migrationDisposition = iota
	// dispDeleted: a transient heartbeater doc removed from fallback, NOT copied to primary.
	// deleteFallbackDoc also counts toward DocsMigrated, so the distinguishing assertion is
	// "absent from primary" rather than the counter.
	dispDeleted
	// dispOutOfScope: a sibling-DB or bucket-level doc left on fallback (DocsOutOfScope++).
	dispOutOfScope
	// dispUnknown: an unrecognised `_sync:` key left on fallback (DocsUnknownPrefix++).
	dispUnknown
)

// TestHandleMigrationKeyClassification is the exhaustive per-key routing matrix for
// handleMigrationKey: every switch arm, for the default metaID migrating DB (with and
// without a sibling) and for a namespaced migrating DB (the inverse direction). It asserts which collection
// each key family is routed to, not the byte-fidelity of the move (datatype/TTL/body
// equality live in the MigrateMetadata full-scan tests).
//
// Keys are built through the real base.MetadataKeys formatters rather than hardcoded, so a
// future change to a key shape is reflected here automatically.
func TestHandleMigrationKeyClassification(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	ms := newMigrationTestStore(t, bucket)

	existsOnPrimary := func(t *testing.T, key string) bool {
		_, _, err := ms.Primary().GetRaw(ctx, key)
		if base.IsDocNotFoundError(err) {
			return false
		}
		require.NoError(t, err)
		return true
	}
	fallbackHas := func(t *testing.T, key string) bool {
		_, _, err := ms.Fallback().GetRaw(ctx, key)
		if base.IsDocNotFoundError(err) {
			return false
		}
		require.NoError(t, err)
		return true
	}

	type keyCase struct {
		name string
		key  string
		want migrationDisposition
	}

	// run executes one subtest per key under a fixed (migratingID, siblings, syncFnKeys)
	// configuration. Keys destined to move/delete are seeded on the fallback first (an
	// in-scope key with no fallback doc is a silent no-op in moveFallbackDoc).
	run := func(t *testing.T, migratingID string, siblings []string, syncFnKeys map[string]struct{}, cases []keyCase) {
		keys := base.NewMetadataKeys(migratingID)
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.want == dispMigrated || tc.want == dispDeleted {
					_, err := ms.Fallback().AddRaw(ctx, tc.key, 0, []byte(`{"x":1}`))
					require.NoError(t, err, "seed fallback %s", tc.key)
				}
				stats := &MigrationStats{}
				handleMigrationKey(ctx, ms, keys, migratingID, siblings, syncFnKeys, tc.key, stats)

				switch tc.want {
				case dispMigrated:
					assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "%q should migrate", tc.key)
					assert.Zero(t, stats.DocsOutOfScope.Load(), "%q must not be out-of-scope", tc.key)
					assert.Zero(t, stats.DocsUnknownPrefix.Load(), "%q must not be unknown-prefix", tc.key)
					assert.Zero(t, stats.Errors.Load(), "%q should move without error", tc.key)
					assert.True(t, existsOnPrimary(t, tc.key), "%q should land on primary", tc.key)
					assert.False(t, fallbackHas(t, tc.key), "%q fallback shadow should be removed", tc.key)
				case dispDeleted:
					assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "%q delete is counted as migrated", tc.key)
					assert.Zero(t, stats.DocsOutOfScope.Load(), "%q must not be out-of-scope", tc.key)
					assert.Zero(t, stats.Errors.Load(), "%q should delete without error", tc.key)
					assert.False(t, existsOnPrimary(t, tc.key), "transient heartbeater %q must NOT be copied to primary", tc.key)
					assert.False(t, fallbackHas(t, tc.key), "%q should be removed from fallback", tc.key)
				case dispOutOfScope:
					assert.Equal(t, int64(1), stats.DocsOutOfScope.Load(), "%q should be out-of-scope", tc.key)
					assert.Zero(t, stats.DocsMigrated.Load(), "%q must not migrate (data theft / loss)", tc.key)
					assert.Zero(t, stats.DocsUnknownPrefix.Load(), "%q is recognised, not unknown", tc.key)
				case dispUnknown:
					assert.Equal(t, int64(1), stats.DocsUnknownPrefix.Load(), "%q should be unknown-prefix", tc.key)
					assert.Zero(t, stats.DocsMigrated.Load(), "%q must not migrate", tc.key)
					assert.Zero(t, stats.DocsOutOfScope.Load(), "%q must not be out-of-scope", tc.key)
				}
			})
		}
	}

	def := base.NewMetadataKeys(base.DefaultMetadataID)
	db2 := base.NewMetadataKeys("db2")

	// Direction 1 — the legacy-default DB migrates while a namespaced sibling "db2" shares
	// the bucket. The default DB's bare inverted prefixes overlap every sibling's inverted key,
	// so without the sibling list the default DB would steal db2's user/role/useremail/session/dcp_ck docs.
	t.Run("DefaultDB_with_namespaced_sibling_db2", func(t *testing.T) {
		run(t, base.DefaultMetadataID, []string{"db2"}, nil, []keyCase{
			// inverted families — ours (no sibling-id segment after the prefix)
			{"own user", def.UserKey("alice"), dispMigrated},
			{"own role", def.RoleKey("admins"), dispMigrated},
			{"own useremail", def.UserEmailKey("alice@example.com"), dispMigrated},
			{"own session", def.SessionKey("sessAlice"), dispMigrated},
			// own dcp_ck: group id "default" + vbno — colons in the body, but the
			// first segment ("default") is not the sibling id "db2", so it stays ours. This is
			// the case the naive "any colon ⇒ sibling" heuristic could not handle.
			{"own dcp_ck (group default)", def.DCPCheckpointPrefix("default") + "1", dispMigrated},

			// inverted families — sibling db2's keys (prefix + "db2:" + …) must stay put
			{"sibling user", db2.UserKey("bob"), dispOutOfScope},
			{"sibling role", db2.RoleKey("readers"), dispOutOfScope},
			{"sibling useremail", db2.UserEmailKey("bob@example.com"), dispOutOfScope},
			{"sibling session", db2.SessionKey("sessBob"), dispOutOfScope},
			{"sibling dcp_ck", db2.DCPCheckpointPrefix("default") + "1", dispOutOfScope},

			// non-inverted owned families (plain isOurs prefix match) — migrated
			{"own unusedSeq (binary)", def.UnusedSeqKey(5), dispMigrated},
			{"own unusedSeqRange (binary)", def.UnusedSeqRangeKey(5, 9), dispMigrated},
			{"own database state", def.DatabaseStateKey(), dispMigrated},
			{"own replication status", def.ReplicationStatusKey("repl1"), dispMigrated},
			{"own sgcfg", def.SGCfgPrefix("") + "nodeDefs-known", dispMigrated},
			{"own resync cfg", def.ResyncCfgPrefix() + cbgt.PLAN_PINDEXES_KEY, dispMigrated},
			{"own background process status", def.BackgroundProcessStatusPrefix("resync"), dispMigrated},

			// non-inverted sibling family (`_sync:m_db2:…`) — out-of-scope via SyncDocMetadataPrefix arm
			{"sibling non-inverted seq", db2.SyncSeqKey(), dispOutOfScope},
			{"sibling non-inverted unusedSeq", db2.UnusedSeqKey(7), dispOutOfScope},

			// heartbeater — deleted, never moved
			{"own heartbeater", def.HeartbeaterPrefix("") + "heartbeat_timeout:node1", dispDeleted},

			// the seq counter is NEVER range-scan migrated (the orchestrator promotes it
			// out-of-band) — out-of-scope even for the owning DB
			{"own seq counter (not migrated)", def.SyncSeqKey(), dispOutOfScope},

			// bucket-level docs — never migrated, never per-DB
			{"registry", base.SGRegistryKey, dispOutOfScope},
			{"syncInfo", base.SGSyncInfo, dispOutOfScope},
			{"dbconfig", base.PersistentConfigPrefixWithoutGroupID + "default", dispOutOfScope},
			{"unowned sync-function doc", base.SyncFunctionKeyWithoutGroupID + "_collection1", dispOutOfScope},

			// genuinely unrecognised `_sync:` key — must surface as unknown, not be swallowed
			{"unknown prefix", "_sync:somethingnew:foo", dispUnknown},
		})
	})

	// Direction 2 (the inverse) — a namespaced DB "db2" migrates while the legacy-default DB
	// is the sibling. isOursInverted early-returns true after the prefix check here (the
	// `m_<id>`/`<id>:` segment already uniquely identifies db2), so the sibling list is not
	// even consulted: behaviour is identical with or without it. This proves the direction
	// that already worked is left untouched.
	t.Run("NamespacedDB_db2_with_default_sibling", func(t *testing.T) {
		run(t, "db2", []string{base.DefaultMetadataID}, nil, []keyCase{
			// inverted families — ours
			{"own user", db2.UserKey("bob"), dispMigrated},
			{"own role", db2.RoleKey("readers"), dispMigrated},
			{"own useremail", db2.UserEmailKey("bob@example.com"), dispMigrated},
			{"own session", db2.SessionKey("sessBob2"), dispMigrated},
			{"own dcp_ck", db2.DCPCheckpointPrefix("default") + "1", dispMigrated},

			// the default sibling's inverted keys — out-of-scope (inverted-default arms)
			{"default-sibling user", def.UserKey("alice"), dispOutOfScope},
			{"default-sibling dcp_ck", def.DCPCheckpointPrefix("default") + "1", dispOutOfScope},

			// non-inverted owned — migrated
			{"own unusedSeq (binary)", db2.UnusedSeqKey(5), dispMigrated},
			{"own database state", db2.DatabaseStateKey(), dispMigrated},

			// a DIFFERENT sibling's non-inverted key — out-of-scope
			{"otherDB non-inverted key", base.NewMetadataKeys("otherDB").SGCfgPrefix(""), dispOutOfScope},

			// own heartbeater (namespaced shape `_sync:m_db2:hb:…heartbeat_timeout:`) — deleted
			{"own heartbeater", db2.HeartbeaterPrefix("") + "heartbeat_timeout:node1", dispDeleted},

			// own seq counter — out-of-scope (not range-scan migrated)
			{"own seq counter (not migrated)", db2.SyncSeqKey(), dispOutOfScope},
		})
	})

	// Sync-function docs are not metadataID-namespaced — ownership is an exact match against
	// the per-DB key set, not a prefix/registry decision. Cover both sides here.
	t.Run("SyncFunctionDocOwnership", func(t *testing.T) {
		ownedSyncFnKey := base.SyncFunctionKeyWithoutGroupID + ":scope1.collection1"
		run(t, base.DefaultMetadataID, []string{"db2"}, map[string]struct{}{ownedSyncFnKey: {}}, []keyCase{
			{"owned sync-function doc", ownedSyncFnKey, dispMigrated},
			{"unowned sync-function doc", base.SyncFunctionKeyWithoutGroupID + ":scope1.collection2", dispOutOfScope},
		})
	})
}

// TestMigrateMetadataUnusedSeqMoves verifies the per-DB unused-sequence docs migrate
// alongside the rest of the per-DB metadata. The bug this pins: prior to classification
// these tripped DocsUnknownPrefix on every pass, sending the migration into the
// bounded-pass give-up branch because both the singleton and range forms exist in the
// wild whenever the sequence allocator has released anything (import races, write
// conflicts).
func TestMigrateMetadataUnusedSeqMoves(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	const metadataID = "testUnusedSeq"
	keys := base.NewMetadataKeys(metadataID)
	ms := newMigrationTestStore(t, bucket)

	// Both forms are written as binary docs (8-/16-byte little-endian uint64 payloads).
	// AddRaw mirrors what sequenceAllocator.releaseSequence/releaseSequenceRange do in
	// production — datatype matters for the round-trip assertions below.
	singletonKey := keys.UnusedSeqKey(42)
	singletonBody := make([]byte, 8)
	binaryLE := func(v uint64) []byte {
		b := make([]byte, 8)
		for i := 0; i < 8; i++ {
			b[i] = byte(v >> (8 * i))
		}
		return b
	}
	copy(singletonBody, binaryLE(42))
	_, err := ms.Fallback().AddRaw(ctx, singletonKey, 0, singletonBody)
	require.NoError(t, err, "seed fallback unusedSeq")

	rangeKey := keys.UnusedSeqRangeKey(10, 20)
	rangeBody := append(binaryLE(10), binaryLE(20)...)
	_, err = ms.Fallback().AddRaw(ctx, rangeKey, 0, rangeBody)
	require.NoError(t, err, "seed fallback unusedSeqs range")

	// Wait for the fallback docs to be visible to range scans before running the migration.
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), []string{singletonKey, rangeKey})

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining, "unused-seq docs must clear in a single pass")
	assert.Equal(t, int64(2), stats.DocsMigrated.Load())
	assert.Zero(t, stats.DocsUnknownPrefix.Load(), "unused-seq docs must not be classified as unknown")

	for _, k := range []string{singletonKey, rangeKey} {
		_, _, getErr := ms.Primary().GetRaw(ctx, k)
		require.NoError(t, getErr, "primary should hold %s", k)
		_, _, getErr = ms.Fallback().GetRaw(ctx, k)
		assert.True(t, base.IsDocNotFoundError(getErr), "fallback should no longer hold %s", k)
	}
}

// TestMigrateMetadataLegacyDefaultUserWithMPrefixUsername is the end-to-end counterpart
// to the classification subtest above: a default-mode DB whose user is literally named
// `m_alice` must actually migrate end-to-end. Before the isOurs fix this user's doc
// fell into the unknown-prefix branch and was left on fallback forever.
func TestMigrateMetadataLegacyDefaultUserWithMPrefixUsername(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	keys := base.NewMetadataKeys(base.DefaultMetadataID)
	ms := newMigrationTestStore(t, bucket)

	mUserKey := keys.UserKey("m_alice")
	body := []byte(`{"name":"m_alice","legacy":true}`)
	seedFallback(ctx, t, ms, mUserKey, body)

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, nil, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "m_alice user in default mode must be migrated, not skipped as a sibling-DB shape")

	got, _, getErr := ms.Primary().GetRaw(ctx, mUserKey)
	require.NoError(t, getErr)
	assert.Equal(t, body, got)
	_, _, getErr = ms.Fallback().GetRaw(ctx, mUserKey)
	assert.True(t, base.IsDocNotFoundError(getErr))
}

// TestSyncFunctionKeysForDB verifies the owned sync-function key set is built per configured
// collection, using the legacy `_sync:syncdata` shape for the default collection and the
// `_sync:syncdata_collection:scope.collection` shape for named collections, with groupID
// applied to both.
func TestSyncFunctionKeysForDB(t *testing.T) {
	collections := map[string]map[string]struct{}{
		base.DefaultScope: {base.DefaultCollection: {}},
	}

	// default collection
	noGroup := syncFunctionKeysForDB("", collections)
	require.Len(t, noGroup, 1)
	assert.Contains(t, noGroup, base.SyncFunctionKeyWithoutGroupID)

	withGroup := syncFunctionKeysForDB("group1", collections)
	require.Len(t, withGroup, 1)
	assert.Contains(t, withGroup, base.SyncFunctionKeyWithoutGroupID+":group1")

	// named collection
	collections = map[string]map[string]struct{}{
		"myScope": {"collA": {}, "collB": {}},
	}
	noGroup = syncFunctionKeysForDB("", collections)
	require.Len(t, noGroup, 2)
	assert.Contains(t, noGroup, base.CollectionSyncFunctionKeyWithoutGroupID+":myScope.collA")
	assert.Contains(t, noGroup, base.CollectionSyncFunctionKeyWithoutGroupID+":myScope.collB")

	withGroup = syncFunctionKeysForDB("group1", collections)
	require.Len(t, withGroup, 2)
	assert.Contains(t, withGroup, base.CollectionSyncFunctionKeyWithoutGroupID+":myScope.collA:group1")
	assert.Contains(t, withGroup, base.CollectionSyncFunctionKeyWithoutGroupID+":myScope.collB:group1")
}

// requireSiblingExclusionScan seeds the own + sibling docs on the fallback, runs a full
// MigrateMetadata pass for migratingID with the given sibling list, and asserts the whole-run
// outcome: every own doc was promoted to primary byte-identically and removed from the
// fallback, every sibling doc stayed put on the fallback (and never reached primary), the
// run is clean (remaining/unknown/errors all zero), and the counters match the seed split.
func requireSiblingExclusionScan(ctx context.Context, t *testing.T, ms *base.MetadataStore, migratingID string, siblings []string, own, sibling map[string][]byte) {
	t.Helper()
	allKeys := make([]string, 0, len(own)+len(sibling))
	for k, v := range own {
		_, err := ms.Fallback().AddRaw(ctx, k, 0, v)
		require.NoError(t, err, "seed own %s", k)
		allKeys = append(allKeys, k)
	}
	for k, v := range sibling {
		_, err := ms.Fallback().AddRaw(ctx, k, 0, v)
		require.NoError(t, err, "seed sibling %s", k)
		allKeys = append(allKeys, k)
	}
	base.RequireDocsVisibleToRangeScan(t, ms.Fallback(), allKeys)

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, migratingID, siblings, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining, "no in-scope doc should be left unclassified")
	assert.Equal(t, int64(len(own)), stats.DocsMigrated.Load(), "all own docs should migrate")
	assert.Equal(t, int64(len(sibling)), stats.DocsOutOfScope.Load(), "all sibling docs should be out-of-scope")
	assert.Zero(t, stats.DocsUnknownPrefix.Load(), "no doc should be unknown-prefix (would stall the run)")
	assert.Zero(t, stats.Errors.Load(), "no per-doc move errors")

	for k, want := range own {
		got, _, getErr := ms.Primary().GetRaw(ctx, k)
		require.NoError(t, getErr, "primary should hold own %s", k)
		assert.Equal(t, want, got, "primary body for own %s must match the seed", k)
		_, _, getErr = ms.Fallback().GetRaw(ctx, k)
		assert.True(t, base.IsDocNotFoundError(getErr), "fallback shadow for own %s should be gone", k)
	}
	for k, want := range sibling {
		got, _, getErr := ms.Fallback().GetRaw(ctx, k)
		require.NoError(t, getErr, "fallback should still hold sibling %s", k)
		assert.Equal(t, want, got, "sibling fallback body for %s must be unchanged", k)
		_, _, getErr = ms.Primary().GetRaw(ctx, k)
		assert.True(t, base.IsDocNotFoundError(getErr), "sibling %s must NOT be promoted to primary", k)
	}
}

// TestMigrateMetadataDefaultDBExcludesNamespacedSibling is the full-scan counterpart to the
// classification matrix for Direction 1: the default metadataID DB migrates while a namespaced
// sibling "db2" shares the fallback collection. Every one of the default DB's own metadata
// families (inverted and non-inverted) is promoted to primary, and every one of db2's
// co-located docs is left untouched on the fallback.
func TestMigrateMetadataDefaultDBExcludesNamespacedSibling(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	ms := newMigrationTestStore(t, bucket)

	def := base.NewMetadataKeys(base.DefaultMetadataID)
	db2 := base.NewMetadataKeys("db2")

	own := map[string][]byte{
		def.UserKey("alice"):                  []byte(`{"name":"alice"}`),
		def.RoleKey("admins"):                 []byte(`{"name":"admins"}`),
		def.UserEmailKey("alice@example.com"): []byte(`{"username":"alice"}`),
		def.SessionKey("sessAlice"):           []byte(`{"session_id":"sessAlice"}`),
		def.DCPCheckpointPrefix("") + "1":     []byte(`{"seq":42}`),
		def.UnusedSeqKey(5):                   []byte(`{"unused":5}`),
		def.DatabaseStateKey():                []byte(`{"state":"Online"}`),
	}
	sibling := map[string][]byte{
		db2.UserKey("bob"):                  []byte(`{"name":"bob"}`),
		db2.RoleKey("readers"):              []byte(`{"name":"readers"}`),
		db2.UserEmailKey("bob@example.com"): []byte(`{"username":"bob"}`),
		db2.SessionKey("sessBob"):           []byte(`{"session_id":"sessBob"}`),
		db2.DCPCheckpointPrefix("") + "1":   []byte(`{"seq":7}`),
		db2.SyncSeqKey():                    []byte(`{"seq":100}`),
		db2.DatabaseStateKey():              []byte(`{"state":"Online"}`),
	}

	requireSiblingExclusionScan(ctx, t, ms, base.DefaultMetadataID, []string{"db2"}, own, sibling)
}

// TestMigrateMetadataNamespacedDBExcludesDefaultSibling is the inverse direction: a namespaced
// DB "db2" migrates while default metadataID DB (plus an unrelated namespaced sibling
// "otherDB") share the fallback. db2's own keys migrate; the default DB's inverted, dcp_ck,
// non-inverted and seq-counter docs all stay, as does otherDB's standard-form key.
func TestMigrateMetadataNamespacedDBExcludesDefaultSibling(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	ms := newMigrationTestStore(t, bucket)

	def := base.NewMetadataKeys(base.DefaultMetadataID)
	db2 := base.NewMetadataKeys("db2")

	own := map[string][]byte{
		db2.UserKey("bob"):                  []byte(`{"name":"bob"}`),
		db2.RoleKey("readers"):              []byte(`{"name":"readers"}`),
		db2.UserEmailKey("bob@example.com"): []byte(`{"username":"bob"}`),
		db2.SessionKey("sessBob"):           []byte(`{"session_id":"sessBob"}`),
		db2.DCPCheckpointPrefix("") + "1":   []byte(`{"seq":7}`),
		db2.UnusedSeqKey(5):                 []byte(`{"unused":5}`),
		db2.DatabaseStateKey():              []byte(`{"state":"Online"}`),
	}
	sibling := map[string][]byte{
		def.UserKey("alice"):                         []byte(`{"name":"alice"}`),
		def.DCPCheckpointPrefix("") + "1":            []byte(`{"seq":42}`),
		def.DatabaseStateKey():                       []byte(`{"state":"Online"}`),
		def.SyncSeqKey():                             []byte(`{"seq":100}`),
		base.NewMetadataKeys("otherDB").SyncSeqKey(): []byte(`{"seq":5}`),
	}

	requireSiblingExclusionScan(ctx, t, ms, "db2", []string{base.DefaultMetadataID}, own, sibling)
}

// TestMigrateMetadataDefaultDBExcludesNamespacedSiblingWhenUserIsNamedSiblingMetadataID tests when you name a user
// on default metadataID db the same as a metadataID from a different db, the user is not misclassified
// as a sibling key and left on fallback.
func TestMigrateMetadataDefaultDBExcludesNamespacedSiblingWhenUserIsNamedSiblingMetadataID(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	ms := newMigrationTestStore(t, bucket)

	def := base.NewMetadataKeys(base.DefaultMetadataID)
	db2 := base.NewMetadataKeys("db2")

	own := map[string][]byte{
		def.UserKey("db2"): []byte(`{"name":"alice"}`),
	}
	sibling := map[string][]byte{
		db2.UserKey("bob"): []byte(`{"name":"bob"}`),
	}

	requireSiblingExclusionScan(ctx, t, ms, base.DefaultMetadataID, []string{"db2"}, own, sibling)
}

// TestMigrateMetadataNamespacedDBExcludesDefaultSiblingWithUserNamedSiblingMetadataID is the inverse of the above test:
// a migration happening in the other direction
func TestMigrateMetadataNamespacedDBExcludesDefaultSiblingWithUserNamedSiblingMetadataID(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	ms := newMigrationTestStore(t, bucket)

	def := base.NewMetadataKeys(base.DefaultMetadataID)
	db2 := base.NewMetadataKeys("db2")

	own := map[string][]byte{
		db2.UserKey("bob"): []byte(`{"name":"bob"}`),
	}
	sibling := map[string][]byte{
		def.UserKey("db2"): []byte(`{"name":"alice"}`),
	}

	requireSiblingExclusionScan(ctx, t, ms, "db2", []string{base.DefaultMetadataID}, own, sibling)
}

// TestMigrateMetadataDcpCheckpointGroupIDCollisionKnownLimitation pins the one documented
// residual ambiguity of the registry-id approach. The default DB's own DCP checkpoint embeds
// its config group ID as the first body segment (`_sync:dcp_ck:<groupID>:<ver>:<vb>`), and the
// classifier excludes a key when that first segment matches a sibling metadataID. So a sibling
// DB whose metadataID is literally equal to the default DB's DCP group ID ("default") makes the
// default DB's own checkpoint look like that sibling's — and it is stranded on the fallback.
//
// This is a KNOWN, accepted limitation: calling a sibling db the same name as a groupID the default
// metadataID db is using collides. The effect of this means the default DB's checkpoint is not migrated,
// but we should self-heal by writing new checkpoints to the new primary.
func TestMigrateMetadataDcpCheckpointGroupIDCollisionKnownLimitation(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	ms := newMigrationTestStore(t, bucket)

	def := base.NewMetadataKeys(base.DefaultMetadataID)
	ownCheckpoint := def.DCPCheckpointPrefix("default") + "1" // _sync:dcp_ck:default:1
	seedFallback(ctx, t, ms, ownCheckpoint, []byte(`{"seq":42}`))

	stats := &MigrationStats{}
	// Pathological sibling whose metadataID == this default DB's DCP group ID ("default").
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, []string{"default"}, nil, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining, "out-of-scope docs do not count as remaining, so the run still 'completes'")

	// KNOWN LIMITATION: the default DB's own checkpoint is misclassified as the sibling's and
	// left on the fallback rather than migrated.
	assert.Zero(t, stats.DocsMigrated.Load(), "known limitation: own checkpoint is NOT migrated under the group-id/sibling-id collision")
	assert.Equal(t, int64(1), stats.DocsOutOfScope.Load(), "own checkpoint is (mis)classified out-of-scope")

	_, _, getErr := ms.Fallback().GetRaw(ctx, ownCheckpoint)
	assert.NoError(t, getErr, "own checkpoint is stranded on the fallback under the known limitation")
	_, _, getErr = ms.Primary().GetRaw(ctx, ownCheckpoint)
	assert.True(t, base.IsDocNotFoundError(getErr), "own checkpoint did not reach primary")
}
