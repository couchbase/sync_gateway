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
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

const cbgtModulePath = "github.com/couchbase/cbgt"

// fieldSummary is the subset of a struct field's shape that matters for JSON (de)serialization.
type fieldSummary struct {
	Type    string
	JSONTag string
}

// TestCbgtOpaqueMetadataMatchesCbgtMetaData verifies that cbgtOpaqueCheckpoint has the same fields (name, type,
// and json tag) as cbgt's own unexported metaData struct (feed_dcp_gocbcore.go), which cbgt's gocbcore DCP
// feed uses to marshal/unmarshal checkpoint metadata. cbgt.metaData is unexported, so it can't be referenced
// directly from this package - instead this locates and parses the actual cbgt source file, at the version
// resolved into this build, out of the local module cache, so the check stays correct across cbgt upgrades
// without a hand-maintained mirror struct.
func TestCbgtOpaqueMetadataMatchesCbgtMetaData(t *testing.T) {
	cbgtFields := parseCbgtMetaDataFields(t)
	sgFields := reflectedFields(reflect.TypeFor[cbgtOpaqueCheckpoint]())
	assert.Equal(t, cbgtFields, sgFields)
}

// parseCbgtMetaDataFields locates cbgt's feed_dcp_gocbcore.go in the module cache (at the version this build
// resolved) and parses out the fields of its private metaData struct.
func parseCbgtMetaDataFields(t *testing.T) map[string]fieldSummary {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", cbgtModulePath).Output()
	require.NoError(t, err, "failed to locate %s module directory", cbgtModulePath)
	sourceFile := filepath.Join(strings.TrimSpace(string(out)), "feed_dcp_gocbcore.go")

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, 0)
	require.NoError(t, err)

	var structType *ast.StructType
	ast.Inspect(file, func(n ast.Node) bool {
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok || typeSpec.Name.Name != "metaData" {
			return true
		}
		structType, ok = typeSpec.Type.(*ast.StructType)
		require.True(t, ok, "expected cbgt.metaData to be declared as a struct type")
		return false
	})
	require.NotNil(t, structType, "could not find cbgt.metaData struct in %s", sourceFile)

	fields := make(map[string]fieldSummary, len(structType.Fields.List))
	for _, field := range structType.Fields.List {
		require.Len(t, field.Names, 1, "expected each cbgt.metaData field to have exactly one name")
		name := field.Names[0].Name

		var jsonTag string
		if field.Tag != nil {
			unquoted, err := strconv.Unquote(field.Tag.Value)
			require.NoError(t, err)
			jsonTag = reflect.StructTag(unquoted).Get("json")
		}
		fields[name] = fieldSummary{Type: types.ExprString(field.Type), JSONTag: jsonTag}
	}
	return fields
}

// reflectedFields returns a struct type's fields keyed by name, capturing only the parts of each field
// relevant to JSON (de)serialization, so it can be compared against parseCbgtMetaDataFields regardless of
// field ordering.
func reflectedFields(t reflect.Type) map[string]fieldSummary {
	fields := make(map[string]fieldSummary, t.NumField())
	for field := range t.Fields() {
		fields[field.Name] = fieldSummary{Type: field.Type.String(), JSONTag: field.Tag.Get("json")}
	}
	return fields
}

// checkpointShapeTestCases enumerates the shapes of cbgt checkpoint data used to exercise round-tripping: cbgt
// always marshals its metaData struct in full (no omitempty), so the only shapes a real cbgt checkpoint takes are
// all cbgtOpaqueCheckpoint fields present, or no cbgt checkpoint at all yet (nil/empty).
var checkpointShapeTestCases = []struct {
	name string
	raw  string
}{
	{name: "full known fields", raw: `{"failOverLog":[[123,0]],"seqStart":10,"seqEnd":20,"snapStart":15,"snapEnd":20}`},
	{name: "empty object", raw: `{}`},
}

// TestCbgtCheckpointRoundTrip chains createCbgtCheckpoint -> readCbgtCheckpoint directly (i.e. without going
// through persistCheckpoint/loadCheckpoint or a metadata bucket), across checkpoint shapes, verifying every field
// survives exactly.
func TestCbgtCheckpointRoundTrip(t *testing.T) {
	const lastSeq = uint64(42)
	for _, testCase := range checkpointShapeTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			persisted, err := createCbgtCheckpoint([]byte(testCase.raw), lastSeq)
			require.NoError(t, err)

			extractedLastSeq, rawMetadata, err := readCbgtCheckpoint(persisted)
			require.NoError(t, err)
			assert.Equal(t, lastSeq, extractedLastSeq)

			var original map[string]any
			require.NoError(t, JSONUnmarshal([]byte(testCase.raw), &original))
			if len(original) == 0 {
				assert.Len(t, rawMetadata, 0)
				return
			}

			var remains map[string]any
			require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
			assert.Equal(t, original, remains)
		})
	}
}

// TestDCPCommonCheckpointRoundTripShapes exercises persistCheckpoint followed by loadCheckpoint on a *DCPCommon -
// i.e. through an actual metadata bucket, not just createCbgtCheckpoint/readCbgtCheckpoint directly - across the
// same checkpoint shapes as TestCbgtCheckpointRoundTrip, verifying every field survives exactly.
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

			var original map[string]any
			require.NoError(t, JSONUnmarshal([]byte(testCase.raw), &original))
			if len(original) == 0 {
				assert.Len(t, rawMetadata, 0)
				return
			}

			var remains map[string]any
			require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
			assert.Equal(t, original, remains)
		})
	}
}

// TestCbgtCheckpointPreservesLargeSequenceNumberPrecision verifies that sequence numbers beyond float64's safe
// integer range (2^53) survive both createCbgtCheckpoint and readCbgtCheckpoint intact - CbgtCheckpoint's fields
// are typed uint64, decoded directly rather than via interface{} (which would go through float64 and lose
// precision).
func TestCbgtCheckpointPreservesLargeSequenceNumberPrecision(t *testing.T) {
	const largeSeq uint64 = 1<<53 + 1 // smallest uint64 not exactly representable as a float64

	raw := fmt.Sprintf(`{"snapStart":%d,"snapEnd":%d}`, largeSeq, largeSeq)

	persisted, err := createCbgtCheckpoint([]byte(raw), largeSeq)
	require.NoError(t, err)

	lastSeq, rawMetadata, err := readCbgtCheckpoint(persisted)
	require.NoError(t, err)
	assert.Equal(t, largeSeq, lastSeq)

	var remains cbgtOpaqueCheckpoint
	require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
	assert.Equal(t, largeSeq, remains.SnapEnd)
}

// TestDCPCommonCheckpointRoundTrip verifies that persistCheckpoint followed by loadCheckpoint on a *DCPCommon
// correctly extracts lastSeq (falling back to snapStart for pre-existing checkpoints without it) and preserves
// cbgt's own checkpoint fields across different shapes of underlying cbgt checkpoint data.
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
	assert.Len(t, rawMetadata, 0)
}

// TestMakeVbucketMetadataIncludesLastSeq verifies that makeVbucketMetadataForSequence builds a full CbgtCheckpoint
// (cbgt's own opaque fields plus SG's lastSeq), not just the bare cbgt fields. This is a change in behavior -
// previously this returned only cbgt's fields, with no lastSeq at all - so that whatever's cached as the
// vbucket's in-memory metadata immediately after a rollback (before the next persist/load cycle through
// createCbgtCheckpoint/readCbgtCheckpoint) already carries a correct, consistent lastSeq value. It also covers
// seqEnd: unbounded (max uint64) by default, but capped to the vbucket's endSeqNos entry for one-shot feeds.
func TestMakeVbucketMetadataIncludesLastSeq(t *testing.T) {
	const (
		vbucketUUID = uint64(1234)
		sequence    = uint64(5678)
	)

	testCases := []struct {
		name           string
		endSeqNos      map[uint16]uint64
		expectedSeqEnd uint64
	}{
		{name: "no endSeqNos leaves seqEnd unbounded", endSeqNos: nil, expectedSeqEnd: 0xFFFFFFFFFFFFFFFF},
		{name: "endSeqNos caps seqEnd for this vbucket", endSeqNos: map[uint16]uint64{0: 9000}, expectedSeqEnd: 9000},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

			dcpCommon, err := NewDCPCommon(ctx, DCPDestOptions{
				MetadataStore: bucket.GetSingleDataStore(),
				MaxVbNo:       1,
				EndSeqNos:     testCase.endSeqNos,
			})
			require.NoError(t, err)

			raw, err := dcpCommon.makeVbucketMetadataForSequence(0, vbucketUUID, sequence)
			require.NoError(t, err)

			var checkpoint CbgtCheckpoint
			require.NoError(t, JSONUnmarshal(raw, &checkpoint))
			assert.Equal(t, sequence, checkpoint.LastSeq)
			assert.Equal(t, sequence, checkpoint.SeqStart)
			assert.Equal(t, sequence, checkpoint.SnapStart)
			assert.Equal(t, sequence, checkpoint.SnapEnd)
			assert.Equal(t, testCase.expectedSeqEnd, checkpoint.SeqEnd)
			assert.Equal(t, [][]uint64{{vbucketUUID, 0}}, checkpoint.FailOverLog)

			var fields map[string]any
			require.NoError(t, JSONUnmarshal(raw, &fields))
			_, hasLastSeq := fields["lastSeq"]
			assert.True(t, hasLastSeq, "expected makeVbucketMetadataForSequence's output to include a lastSeq field")
		})
	}
}

// TestDCPCommonRollbackExPersistsLastSeq is a regression test for makeVbucketMetadataForSequence now building a
// full CbgtCheckpoint (including lastSeq) rather than just cbgt's bare opaque fields. It exercises rollbackEx
// end-to-end through a real metadata bucket - the same path DCPDest.RollbackEx uses - verifying that the
// persisted checkpoint's lastSeq (read back via loadCheckpoint) reflects the rollback sequence, and that cbgt's
// own fields still round-trip correctly, i.e. makeVbucketMetadataForSequence's extra lastSeq field doesn't
// confuse createCbgtCheckpoint (which always derives lastSeq from its own parameter, not from opaqueValue).
func TestDCPCommonRollbackExPersistsLastSeq(t *testing.T) {
	const (
		vbucketUUID      = uint64(1234)
		rollbackSeq      = uint64(100)
		checkpointPrefix = "test_dcp_rollback_ex_persists_lastseq_"
	)

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dcpCommon, err := NewDCPCommon(ctx, DCPDestOptions{
		MetadataStore:      bucket.GetSingleDataStore(),
		MaxVbNo:            1,
		PersistCheckpoints: true,
		CheckpointPrefix:   checkpointPrefix,
	})
	require.NoError(t, err)
	dcpCommon.InitVbMeta(0)

	rollbackMetadata, err := dcpCommon.makeVbucketMetadataForSequence(0, vbucketUUID, rollbackSeq)
	require.NoError(t, err)
	require.NoError(t, dcpCommon.rollbackEx(0, vbucketUUID, rollbackSeq, rollbackMetadata))

	rawMetadata, lastSeq, err := dcpCommon.loadCheckpoint(0)
	require.NoError(t, err)
	assert.Equal(t, rollbackSeq, lastSeq)

	var remains cbgtOpaqueCheckpoint
	require.NoError(t, JSONUnmarshal(rawMetadata, &remains))
	assert.Equal(t, rollbackSeq, remains.SeqStart)
	assert.Equal(t, rollbackSeq, remains.SnapStart)
	assert.Equal(t, rollbackSeq, remains.SnapEnd)
	assert.Equal(t, [][]uint64{{vbucketUUID, 0}}, remains.FailOverLog)

	// setMetaData caches rollbackMetadata verbatim as the vbucket's in-memory metadata (what OpaqueGet hands back
	// to cbgt) until the next persist/load cycle - so it's expected to carry the extra lastSeq field makeVbucketMetadata
	// now includes. That's harmless: cbgt's own json.Unmarshal into its metaData struct silently ignores unknown
	// fields, and createCbgtCheckpoint always derives lastSeq from its own parameter, never from opaqueValue.
	cachedMetadata, cachedLastSeq, err := dcpCommon.getMetaData(0)
	require.NoError(t, err)
	assert.Equal(t, rollbackSeq, cachedLastSeq)
	assert.Equal(t, rollbackMetadata, cachedMetadata)
}
