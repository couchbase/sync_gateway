// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestMetadataStore returns a MetadataStore backed by two distinct DataStores in the
// supplied TestBucket: the default collection as fallback (matching legacy SG layout) and a
// named collection as primary. Both are supported by Rosmar so these tests run by default.
func newTestMetadataStore(t *testing.T, bucket *TestBucket) *MetadataStore {
	t.Helper()
	primary, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	fallback := bucket.DefaultDataStore(TestCtx(t))
	return NewMetadataStore(primary, fallback)
}

// TestMetadataStoreIncrLegacyFallbackPassthrough verifies that when primary is empty and
// fallback holds a legacy numeric counter (no migration pill), the wrapper passes Incrs
// through to the fallback rather than migrating. Migration only happens once the migration
// manager writes the poison pill.
func TestMetadataStoreIncrLegacyFallbackPassthrough(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testIncr:seq"

	_, err := ms.Fallback().Incr(ctx, key, 10, 10, 0)
	require.NoError(t, err)

	result, err := ms.Incr(ctx, key, 5, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), result, "expected legacy fallback Incr to flow through")

	fbVal, err := GetCounter(ctx, ms.Fallback(), key)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), fbVal, "wrapper must not move legacy counters without a pill")

	primaryExists, err := ms.Primary().Exists(ctx, key)
	require.NoError(t, err)
	assert.False(t, primaryExists, "primary should remain untouched in the legacy passthrough case")
}

// TestMetadataStoreIncrSelfHealsPill verifies the wrapper recognises a
// SyncSeqMigrationPill body on the fallback, extracts LastSeq, bootstraps primary, and
// clears the pill.
func TestMetadataStoreIncrSelfHealsPill(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testPill:seq"

	pill := SyncSeqMigrationPill{LastSeq: 42, SGMetadataMigrationPill: true, PilledAt: "2026-05-27T00:00:00Z"}
	payload, err := JSONMarshal(&pill)
	require.NoError(t, err)
	_, err = ms.Fallback().AddRaw(ctx, key, 0, payload)
	require.NoError(t, err)

	// Wrapper Incr: bootstrap primary at LastSeq=42, then add the requested amount.
	result, err := ms.Incr(ctx, key, 8, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(50), result, "expected primary counter to be seeded from pill.LastSeq")

	_, _, err = ms.Fallback().GetRaw(ctx, key)
	assert.True(t, IsDocNotFoundError(err), "expected pill to be removed after self-heal, got %v", err)

	// Regression check: the seeded counter must be readable via GetCounter, which goes
	// through SGJSONTranscoder. If the seed wrote with the Binary datatype flag instead
	// of as a counter, this fails on Couchbase Server with "you must encode raw JSON
	// data in a byte array or string" — exactly the failure mode the migration manager
	// hit before switching from AddRaw to Incr for the post-pill seed.
	counterValue, err := GetCounter(ctx, ms.Primary(), key)
	require.NoError(t, err, "post-self-heal counter must be readable via GetCounter")
	assert.Equal(t, uint64(50), counterValue)
}

// TestGetCounterHealsSeqMigrationPill is the regression for the crash-restart wedge
// where a pill on the fallback seq counter outlives the wrapper's Incr self-heal. At
// DB startup sequenceAllocator.lastSequence -> base.GetCounter is the first read of
// the seq counter; before GetCounter routed through Incr, a fallback pill body
// returned an UnmarshalTypeError into *uint64 and the database failed to start.
// Routing through Incr(0,0,0) lets the wrapper self-heal transparently.
func TestGetCounterHealsSeqMigrationPill(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testGetCounterPill:seq"

	pill := SyncSeqMigrationPill{LastSeq: 100, SGMetadataMigrationPill: true, PilledAt: "2026-05-27T00:00:00Z"}
	payload, err := JSONMarshal(&pill)
	require.NoError(t, err)
	_, err = ms.Fallback().AddRaw(ctx, key, 0, payload)
	require.NoError(t, err)

	value, err := GetCounter(ctx, ms, key)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), value, "GetCounter must return pill.LastSeq via wrapper.Incr self-heal")

	_, _, err = ms.Fallback().GetRaw(ctx, key)
	assert.True(t, IsDocNotFoundError(err), "fallback pill must be cleared after self-heal, got %v", err)

	primaryValue, err := GetCounter(ctx, ms.Primary(), key)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), primaryValue, "primary must hold the seeded counter")

	// Idempotence: second invocation should return the same value with no further side
	// effects (no second pill required, no error on the already-migrated counter).
	again, err := GetCounter(ctx, ms, key)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), again)
}

// TestPrimaryAddRawDuplicateContract pins the AddRaw-on-duplicate contract that the
// wrapper's Incr self-heal branch relies on: returning (added=false, err=nil) rather than
// surfacing a duplicate-key error. Both the gocb path (collection_gocb.go) and the
// rosmar path (rosmar.Collection.add) honour this today. A future backend regression that
// starts surfacing sgbucket.ErrKeyExists or similar would break the wrapper's race
// tolerance, so we lock the contract here.
func TestPrimaryAddRawDuplicateContract(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testAddRawDup:seq"

	added, err := ms.Primary().AddRaw(ctx, key, 0, []byte("42"))
	require.NoError(t, err)
	require.True(t, added, "first AddRaw should succeed")

	added, err = ms.Primary().AddRaw(ctx, key, 0, []byte("99"))
	require.NoError(t, err, "duplicate AddRaw must not surface an error — wrapper's self-heal relies on this")
	require.False(t, added, "duplicate AddRaw must return added=false")
}

// TestMetadataStoreIncrAfterMigrationCompleteBypassesFallback verifies that once

// TestMetadataStoreIncrAfterMigrationCompleteBypassesFallback verifies that once
// SetMigrationComplete has been called the wrapper short-circuits to primary, even if a
// fallback doc still exists (it should be invisible).
func TestMetadataStoreIncrAfterMigrationCompleteBypassesFallback(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testComplete:seq"

	// Stale fallback value present.
	_, err := ms.Fallback().Incr(ctx, key, 99, 99, 0)
	require.NoError(t, err)

	ms.SetMigrationComplete()

	// Wrapper should not consult fallback now; primary doc gets created at the supplied
	// default value (1) and incremented by amt (0), returning the default value.
	result, err := ms.Incr(ctx, key, 0, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), result, "expected primary auto-create at def, fallback should be invisible")
}

// TestMetadataStoreIncrUnknownFallbackBodyErrors confirms that a non-pill, non-numeric
// fallback body produces an explicit error rather than silently treating the doc as zero
// (which would lose sequences).
func TestMetadataStoreIncrUnknownFallbackBodyErrors(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testGarbage:seq"

	garbage := []byte(`{"unrecognised":"shape"}`)
	_, err := ms.Fallback().AddRaw(ctx, key, 0, garbage)
	require.NoError(t, err)

	_, err = ms.Incr(ctx, key, 1, 1, 0)
	require.Error(t, err, "expected an explicit error when the fallback body is neither a counter nor a pill")
}

// TestMetadataStoreIncrEmptyFallbackCreatesOnPrimary covers the new-database path: the
// fallback has nothing for the key, so Incr should fall through to primary's auto-create.
func TestMetadataStoreIncrEmptyFallbackCreatesOnPrimary(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ms := newTestMetadataStore(t, bucket)
	const key = "_sync:m_testNewDB:seq"

	result, err := ms.Incr(ctx, key, 0, 100, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), result)

	primaryExists, getErr := ms.Primary().Exists(ctx, key)
	require.NoError(t, getErr)
	assert.True(t, primaryExists, "expected primary to hold the new counter")
}

// TestIsCounterNonNumeric verifies the predicate recognises both the gocb
// StatusBadDelta (DELTA_BADVAL) shape and the rosmar json.UnmarshalTypeError shape so the
// wrapper's self-heal branch trips uniformly across backends.
func TestIsCounterNonNumeric(t *testing.T) {
	assert.False(t, IsCounterNonNumeric(nil))

	// Rosmar surfaces a *json.UnmarshalTypeError when Incr decodes a JSON-object body
	// into a uint64. Manufacture the same error here so the predicate is exercised
	// without needing a live backend.
	var dest uint64
	rosmarShape := json.Unmarshal([]byte(`{"last_seq":42}`), &dest)
	require.Error(t, rosmarShape)
	assert.True(t, IsCounterNonNumeric(rosmarShape))

	// Sanity: an unrelated error should NOT be classified as non-numeric.
	assert.False(t, IsCounterNonNumeric(errors.New("unrelated")))
}
