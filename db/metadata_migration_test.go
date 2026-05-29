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

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
}

// TestMigrateMetadataEmptyFallback verifies the new-DB fast path: empty fallback yields a
// zero remaining count and no errors.
func TestMigrateMetadataEmptyFallback(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newMigrationTestStore(t, bucket)
	stats := &MigrationStats{}

	remaining, err := MigrateMetadata(ctx, ms, "testEmpty", stats)
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
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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
	_, err := MigrateMetadata(ctx, ms, "myDB", stats)
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
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, stats)
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
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, stats)
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
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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

	classify := func(metadataID, key string) *MigrationStats {
		stats := &MigrationStats{}
		handleMigrationKey(ctx, nil, base.NewMetadataKeys(metadataID), metadataID, key, stats)
		return stats
	}

	t.Run("NamespacedSiblingStandardForm", func(t *testing.T) {
		// Sibling DBs' standard-form keys are `_sync:m_<other>:…` — structurally
		// distinguishable from ours via the metadataID infix.
		for _, k := range []string{
			"_sync:m_otherDB:seq",
			"_sync:m_otherDB:hb:groupA",
		} {
			s := classify("myDB", k)
			assert.Equal(t, int64(1), s.DocsOutOfScope.Load(), "%q should be out of scope for myDB", k)
		}
	})
	t.Run("BucketLevelOutOfScope", func(t *testing.T) {
		for _, k := range []string{
			"_sync:registry",
			"_sync:dbconfig:default",
			"_sync:syncInfo",
		} {
			s := classify("myDB", k)
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
			s := classify(base.DefaultMetadataID, k)
			assert.Equal(t, int64(1), s.DocsUnknownPrefix.Load(), "%q must be unknown-prefix, not classified as a heartbeat", k)
			assert.Zero(t, s.DocsOutOfScope.Load(), "%q is in-scope (default mode owns `_sync:…`) so should not be out-of-scope", k)
		}
	})

	// LegacySiblingInvertedFormOutOfScope pins the sibling-side of the corner-case fix:
	// the inverted-form keys for a real sibling DB (with a trailing `:` after the
	// metadataID) must still be classified out-of-scope when the local DB is in legacy
	// default mode. The ours-side counterpart — a legacy default user literally named
	// `m_alice` — is covered end-to-end by TestMigrateMetadataLegacyDefaultUserWithMPrefixUsername
	// below (the dispatcher would call moveFallbackDoc on a real bucket for those).
	t.Run("LegacySiblingInvertedFormOutOfScope", func(t *testing.T) {
		for _, k := range []string{
			"_sync:user:m_otherDB:alice",
			"_sync:role:m_otherDB:admins",
			"_sync:useremail:m_otherDB:alice@example.com",
			"_sync:session:m_otherDB:tok1",
		} {
			s := classify(base.DefaultMetadataID, k)
			assert.Equal(t, int64(1), s.DocsOutOfScope.Load(), "%q is a sibling-DB inverted-form key — must remain out-of-scope", k)
		}
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

	stats := &MigrationStats{}
	remaining, err := MigrateMetadata(ctx, ms, metadataID, stats)
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
	remaining, err := MigrateMetadata(ctx, ms, base.DefaultMetadataID, stats)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, int64(1), stats.DocsMigrated.Load(), "m_alice user in default mode must be migrated, not skipped as a sibling-DB shape")

	got, _, getErr := ms.Primary().GetRaw(ctx, mUserKey)
	require.NoError(t, getErr)
	assert.Equal(t, body, got)
	_, _, getErr = ms.Fallback().GetRaw(ctx, mUserKey)
	assert.True(t, base.IsDocNotFoundError(getErr))
}
