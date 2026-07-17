/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestCbgtCheckpointWriterMarshalJSON covers persisting different shapes of cbgt-supplied checkpoint data via
// cbgtCheckpointWriter, in particular that fields cbgt owns but SG doesn't otherwise model survive being persisted.
func TestCbgtCheckpointWriterMarshalJSON(t *testing.T) {
	testCases := []struct {
		name     string
		raw      string
		lastSeq  uint64
		expected map[string]any
	}{
		{
			name:     "typical cbgt checkpoint",
			raw:      `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20}`,
			lastSeq:  18,
			expected: map[string]any{"failOverLog": []any{[]any{float64(123), float64(0)}}, "seqStart": float64(10), "seqEnd": float64(20), "snapStart": float64(15), "snapEnd": float64(20), "lastSeq": float64(18)},
		},
		{
			name:     "already has a stale lastSeq value, gets overwritten",
			raw:      `{"snapStart":15,"snapEnd":20,"lastSeq":1}`,
			lastSeq:  42,
			expected: map[string]any{"snapStart": float64(15), "snapEnd": float64(20), "lastSeq": float64(42)},
		},
		{
			name:     "unknown future cbgt fields are preserved",
			raw:      `{"snapStart":5,"snapEnd":10,"newCbgtField":"someValue","nested":{"a":1}}`,
			lastSeq:  7,
			expected: map[string]any{"snapStart": float64(5), "snapEnd": float64(10), "newCbgtField": "someValue", "nested": map[string]any{"a": float64(1)}, "lastSeq": float64(7)},
		},
		{
			name:     "empty object",
			raw:      `{}`,
			lastSeq:  0,
			expected: map[string]any{"lastSeq": float64(0)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			writer, err := newCbgtCheckpointWriter([]byte(testCase.raw), testCase.lastSeq)
			require.NoError(t, err)

			persisted, err := JSONMarshal(writer)
			require.NoError(t, err)

			var persistedFields map[string]any
			require.NoError(t, JSONUnmarshal(persisted, &persistedFields))
			assert.Equal(t, testCase.expected, persistedFields)
		})
	}
}

// TestCbgtCheckpointReaderExtractLastSeq covers extracting lastSeq from different shapes of persisted checkpoint
// data via cbgtCheckpointReader, including the snapStart fallback used for checkpoints persisted before lastSeq
// was tracked, and that fields cbgt owns but SG doesn't otherwise model survive being handed back to cbgt.
func TestCbgtCheckpointReaderExtractLastSeq(t *testing.T) {
	testCases := []struct {
		name            string
		raw             string
		expectedLastSeq uint64
		expectedRemains map[string]any
	}{
		{
			name:            "no lastSeq falls back to snapStart",
			raw:             `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20}`,
			expectedLastSeq: 15,
			expectedRemains: map[string]any{"failOverLog": []any{[]any{float64(123), float64(0)}}, "seqStart": float64(10), "seqEnd": float64(20), "snapStart": float64(15), "snapEnd": float64(20)},
		},
		{
			name:            "lastSeq present takes priority over snapStart",
			raw:             `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20,"lastSeq":18}`,
			expectedLastSeq: 18,
			expectedRemains: map[string]any{"failOverLog": []any{[]any{float64(123), float64(0)}}, "seqStart": float64(10), "seqEnd": float64(20), "snapStart": float64(15), "snapEnd": float64(20)},
		},
		{
			name:            "unknown future cbgt fields are preserved",
			raw:             `{"snapStart":5,"snapEnd":10,"newCbgtField":"someValue","nested":{"a":1}}`,
			expectedLastSeq: 5,
			expectedRemains: map[string]any{"snapStart": float64(5), "snapEnd": float64(10), "newCbgtField": "someValue", "nested": map[string]any{"a": float64(1)}},
		},
		{
			name:            "empty object has no snapStart to fall back to",
			raw:             `{}`,
			expectedLastSeq: 0,
			expectedRemains: map[string]any{},
		},
		{
			name:            "persisted lastSeq of zero is used as-is, not treated as absent",
			raw:             `{"snapStart":15,"snapEnd":20,"lastSeq":0}`,
			expectedLastSeq: 0,
			expectedRemains: map[string]any{"snapStart": float64(15), "snapEnd": float64(20)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader, err := newCbgtCheckpointReader([]byte(testCase.raw))
			require.NoError(t, err)

			lastSeq, rawMetadata, err := reader.extractLastSeq()
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedLastSeq, lastSeq)

			var remains map[string]any
			require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
			assert.Equal(t, testCase.expectedRemains, remains)
			_, hasLastSeq := remains["lastSeq"]
			assert.False(t, hasLastSeq)
		})
	}
}

// checkpointShapeTestCases enumerates different shapes of cbgt checkpoint data used to exercise round-tripping:
// the full set of known (CbgtOpaqueMetadata) fields, a partial subset of them, fields SG doesn't model at
// all, and combinations of the two.
var checkpointShapeTestCases = []struct {
	name string
	raw  string
}{
	{name: "full known fields", raw: `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20}`},
	{name: "partial known fields (snapStart+snapEnd only)", raw: `{"snapStart":15,"snapEnd":20}`},
	{name: "partial known fields (failOverLog only)", raw: `{"failOverLog":[[123,0]]}`},
	{name: "full known fields plus extra opaque fields", raw: `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20,"newCbgtField":"someValue","nested":{"a":1}}`},
	{name: "partial known fields plus extra opaque fields", raw: `{"snapStart":8,"extraField":"value","nested":{"x":1}}`},
	{name: "only extra opaque fields, no known fields at all", raw: `{"onlyUnknown":"value"}`},
	{name: "empty object", raw: `{}`},
}

// TestCbgtCheckpointWriterReaderRoundTrip chains cbgtCheckpointWriter -> cbgtCheckpointReader directly (i.e.
// without going through persistCheckpoint/loadCheckpoint or a metadata bucket), across checkpoint shapes with
// partial known fields and/or extra fields SG doesn't model, verifying every original field survives exactly.
func TestCbgtCheckpointWriterReaderRoundTrip(t *testing.T) {
	const lastSeq = uint64(42)
	for _, testCase := range checkpointShapeTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			writer, err := newCbgtCheckpointWriter([]byte(testCase.raw), lastSeq)
			require.NoError(t, err)

			persisted, err := JSONMarshal(writer)
			require.NoError(t, err)

			reader, err := newCbgtCheckpointReader(persisted)
			require.NoError(t, err)

			extractedLastSeq, rawMetadata, err := reader.extractLastSeq()
			require.NoError(t, err)
			assert.Equal(t, lastSeq, extractedLastSeq)

			var remains map[string]any
			require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
			var original map[string]any
			require.NoError(t, JSONUnmarshal([]byte(testCase.raw), &original))
			assert.Equal(t, original, remains)
		})
	}
}

// TestDCPCommonCheckpointRoundTripShapes exercises persistCheckpoint followed by loadCheckpoint on a *DCPCommon -
// i.e. through an actual metadata bucket, not just the cbgtCheckpointWriter/cbgtCheckpointReader types directly -
// across the same checkpoint shapes as TestCbgtCheckpointWriterReaderRoundTrip, verifying every original field
// survives exactly.
func TestDCPCommonCheckpointRoundTripShapes(t *testing.T) {
	const lastSeq = uint64(42)
	for _, testCase := range checkpointShapeTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

			dcpCommon, err := NewDCPCommon(ctx, DCPDestOptions{
				MetadataStore:      bucket.GetSingleDataStore(),
				MaxVbNo:            1,
				PersistCheckpoints: true,
				CheckpointPrefix:   "test_dcp_checkpoint_round_trip_shapes_" + testCase.name + "_",
			})
			require.NoError(t, err)

			require.NoError(t, dcpCommon.persistCheckpoint(0, []byte(testCase.raw), lastSeq))

			rawMetadata, extractedLastSeq, err := dcpCommon.loadCheckpoint(0)
			require.NoError(t, err)
			assert.Equal(t, lastSeq, extractedLastSeq)

			var remains map[string]any
			require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
			var original map[string]any
			require.NoError(t, JSONUnmarshal([]byte(testCase.raw), &original))
			assert.Equal(t, original, remains)
		})
	}
}

// TestCbgtCheckpointPreservesLargeSequenceNumberPrecision verifies that sequence numbers beyond float64's safe
// integer range (2^53) survive both cbgtCheckpointWriter and cbgtCheckpointReader intact - CbgtOpaqueMetadata
// and LastSeq are typed uint64 fields, decoded directly rather than via interface{} (which would go through
// float64 and lose precision).
func TestCbgtCheckpointPreservesLargeSequenceNumberPrecision(t *testing.T) {
	const largeSeq uint64 = 1<<53 + 1 // smallest uint64 not exactly representable as a float64

	raw := fmt.Sprintf(`{"snapStart":%d,"snapEnd":%d}`, largeSeq, largeSeq)

	writer, err := newCbgtCheckpointWriter([]byte(raw), largeSeq)
	require.NoError(t, err)
	persisted, err := JSONMarshal(writer)
	require.NoError(t, err)

	reader, err := newCbgtCheckpointReader(persisted)
	require.NoError(t, err)
	assert.Equal(t, largeSeq, reader.LastSeq)
	assert.Equal(t, largeSeq, reader.SnapEnd)

	lastSeq, rawMetadata, err := reader.extractLastSeq()
	require.NoError(t, err)
	assert.Equal(t, largeSeq, lastSeq)

	var remains CbgtOpaqueMetadata
	require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
	assert.Equal(t, largeSeq, remains.SnapEnd)
}

// TestDCPCommonCheckpointRoundTrip verifies that persistCheckpoint followed by loadCheckpoint on a *DCPCommon
// correctly extracts lastSeq (falling back to snapStart for pre-existing checkpoints without it) and preserves
// cbgt's own checkpoint fields - including ones SG doesn't otherwise model - across different shapes of underlying
// cbgt checkpoint data.
func TestDCPCommonCheckpointRoundTrip(t *testing.T) {
	testCases := []struct {
		name             string
		value            string
		persistedLastSeq uint64
		endSeqNos        map[uint16]uint64
		expectedLastSeq  uint64
	}{
		{
			name:             "lastSeq persisted directly",
			value:            `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20}`,
			persistedLastSeq: 18,
			expectedLastSeq:  18,
		},
		{
			name:             "endSeqNos caps lastSeq when snapStart is beyond the expected end",
			value:            `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":9000,"snapEnd":9000}`,
			persistedLastSeq: 9000,
			endSeqNos:        map[uint16]uint64{0: 100},
			expectedLastSeq:  100,
		},
		{
			name:             "unknown cbgt fields survive the round trip",
			value:            `{"snapStart":5,"snapEnd":10,"newCbgtField":"someValue"}`,
			persistedLastSeq: 7,
			expectedLastSeq:  7,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

			dcpCommon, err := NewDCPCommon(ctx, DCPDestOptions{
				MetadataStore:      bucket.GetSingleDataStore(),
				MaxVbNo:            1,
				PersistCheckpoints: true,
				CheckpointPrefix:   "test_dcp_checkpoint_round_trip_" + testCase.name + "_",
				EndSeqNos:          testCase.endSeqNos,
			})
			require.NoError(t, err)

			require.NoError(t, dcpCommon.persistCheckpoint(0, []byte(testCase.value), testCase.persistedLastSeq))

			rawMetadata, lastSeq, err := dcpCommon.loadCheckpoint(0)
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedLastSeq, lastSeq)

			var remains map[string]any
			require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
			_, hasLastSeq := remains["lastSeq"]
			assert.False(t, hasLastSeq)

			var originalFields map[string]any
			require.NoError(t, JSONUnmarshal([]byte(testCase.value), &originalFields))
			assert.Equal(t, originalFields, remains)
		})
	}
}

// TestDCPCommonLoadLegacyCheckpointFallsBackToSnapStart verifies that a checkpoint persisted by an older version of
// Sync Gateway (i.e. before lastSeq was tracked, so the raw value has no lastSeq field) still loads correctly,
// falling back to the checkpoint's snapStart.
func TestDCPCommonLoadLegacyCheckpointFallsBackToSnapStart(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	const checkpointPrefix = "test_dcp_legacy_checkpoint_"
	dcpCommon, err := NewDCPCommon(ctx, DCPDestOptions{
		MetadataStore:      bucket.GetSingleDataStore(),
		MaxVbNo:            1,
		PersistCheckpoints: true,
		CheckpointPrefix:   checkpointPrefix,
	})
	require.NoError(t, err)

	// Simulate a checkpoint written before lastSeq was tracked, by writing directly rather than via
	// persistCheckpoint (which always injects lastSeq).
	legacyValue := []byte(`{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20}`)
	require.NoError(t, bucket.GetSingleDataStore().SetRaw(ctx, fmt.Sprintf("%s%d", checkpointPrefix, 0), 0, nil, legacyValue))

	rawMetadata, lastSeq, err := dcpCommon.loadCheckpoint(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), lastSeq)

	var remains map[string]any
	require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
	var originalFields map[string]any
	require.NoError(t, JSONUnmarshal(legacyValue, &originalFields))
	assert.Equal(t, originalFields, remains)
}

// TestDCPCommonPersistCheckpointNilValue verifies that persistCheckpoint succeeds when passed a nil value, as
// happens when ForceCheckpointWrite runs for a vbucket that completed InitVbMeta but has no prior cbgt checkpoint
// metadata yet (meta[vbNo] is still nil).
func TestDCPCommonPersistCheckpointNilValue(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	const checkpointPrefix = "test_dcp_persist_checkpoint_nil_value_"
	dcpCommon, err := NewDCPCommon(ctx, DCPDestOptions{
		MetadataStore:      bucket.GetSingleDataStore(),
		MaxVbNo:            1,
		PersistCheckpoints: true,
		CheckpointPrefix:   checkpointPrefix,
	})
	require.NoError(t, err)

	require.NoError(t, dcpCommon.persistCheckpoint(0, nil, 42))

	rawMetadata, lastSeq, err := dcpCommon.loadCheckpoint(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), lastSeq)

	var remains map[string]any
	require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
	assert.Equal(t, map[string]any{}, remains)
}
