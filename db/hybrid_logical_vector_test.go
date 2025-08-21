// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternalHLVFunctions:
//   - Tests internal api methods on the HLV work as expected
//   - Tests methods GetCurrentVersion, AddVersion and Remove
func TestInternalHLVFunctions(t *testing.T) {
	expectedPV := make(HLVVersions)
	currSourceID := "5pRi8Piv1yLcLJ1iVNJIsA"
	currVersion := uint64(12345678)
	pvSourceID := "YZvBpEaztom9z5V/hDoeIw"
	pvVersion := uint64(64463204720)
	expectedPV[pvSourceID] = pvVersion
	mvSourceID := "NqiIe0LekFPLeX4JvTO6Iw"
	mvVersion := uint64(345454)

	hlv := HybridLogicalVector{
		SourceID: currSourceID,
		Version:  12345678,
		MergeVersions: map[string]uint64{
			mvSourceID: mvVersion,
		},
		PreviousVersions: map[string]uint64{
			pvSourceID: pvVersion,
		},
	}

	newCAS := uint64(123456789)
	const newSource = "s_testsource"

	// create a new version vector entry that will error method AddVersion
	badNewVector := Version{
		Value:    123345,
		SourceID: currSourceID,
	}
	// create a new version vector entry that should be added to HLV successfully
	newVersionVector := Version{
		Value:    newCAS,
		SourceID: currSourceID,
	}

	// Get current version vector, sourceID and CAS pair
	source, version := hlv.GetCurrentVersion()
	assert.Equal(t, currSourceID, source)
	assert.Equal(t, currVersion, version)

	// add new version vector with same sourceID as current sourceID.  MV will move to PV but PV should not otherwise change
	require.NoError(t, hlv.AddVersion(newVersionVector))
	assert.Len(t, hlv.MergeVersions, 0)
	assert.Len(t, hlv.PreviousVersions, 2)
	hlv.Version = currVersion
	expectedPV[mvSourceID] = mvVersion

	// attempt to add new version vector to HLV that has a CAS value less than the current CAS value
	require.Error(t, hlv.AddVersion(badNewVector))

	// add current version and sourceID of HLV to pv map for assertions
	expectedPV[currSourceID] = currVersion
	// Add a new version vector pair to the HLV structure and assert that it moves the current version vector pair to the previous versions section
	newVersionVector.SourceID = newSource
	require.NoError(t, hlv.AddVersion(newVersionVector))
	assert.Equal(t, newCAS, hlv.Version)
	assert.Equal(t, newSource, hlv.SourceID)
	require.Equal(t, hlv.PreviousVersions, expectedPV)

	// remove garbage sourceID from PV and assert we get error
	require.Error(t, hlv.Remove("testing"))
	// Remove a sourceID CAS pair from previous versions section of the HLV structure (for compaction)
	require.NoError(t, hlv.Remove(currSourceID))
	delete(expectedPV, currSourceID)
	require.Equal(t, hlv.PreviousVersions, expectedPV)
}

// TestHLVIsDominating:
//   - Tests cases where one HLV is said to be 'dominating' over another
//   - Assert that all scenarios returns false from IsInConflict method, as we have a HLV that is dominating in each case
func TestHLVIsDominating(t *testing.T) {
	testCases := []struct {
		name           string
		HLVA           string
		HLVB           string
		expectedResult bool
	}{
		{
			name:           "Matching current source, newer version",
			HLVA:           "20@cluster1;2@cluster2",
			HLVB:           "10@cluster1;1@cluster2",
			expectedResult: true,
		}, {
			name:           "Matching current source and version",
			HLVA:           "20@cluster1;2@cluster2",
			HLVB:           "20@cluster1;2@cluster2",
			expectedResult: true,
		},
		{
			name:           "B CV found in A's PV",
			HLVA:           "20@cluster1;10@cluster2",
			HLVB:           "10@cluster2;15@cluster1",
			expectedResult: true,
		},
		{
			name:           "B CV older than A's PV for same source",
			HLVA:           "20@cluster1;15@cluster2",
			HLVB:           "10@cluster2;15@cluster1",
			expectedResult: true,
		},
		{
			name:           "Unique sources in A",
			HLVA:           "20@cluster1;15@cluster2,3@cluster3",
			HLVB:           "10@cluster2;10@cluster1",
			expectedResult: true,
		},
		{
			name:           "Unique sources in B",
			HLVA:           "20@cluster1",
			HLVB:           "15@cluster1;3@cluster3",
			expectedResult: true,
		},
		{
			name:           "B has newer cv",
			HLVA:           "10@cluster1",
			HLVB:           "15@cluster1",
			expectedResult: false,
		},
		{
			name:           "B has newer cv than A pv",
			HLVA:           "20@cluster2;10@cluster1",
			HLVB:           "15@cluster1;20@cluster2",
			expectedResult: false,
		},
		{
			name:           "B's cv not found in A",
			HLVA:           "20@cluster2;10@cluster1",
			HLVB:           "5@cluster3",
			expectedResult: false,
		},
		{
			name:           "a.MV dominates B.CV",
			HLVA:           "20@cluster1,20@cluster2,5@cluster3",
			HLVB:           "10@cluster2",
			expectedResult: true,
		},
		{
			name:           "a.MV doesn't dominate B.CV",
			HLVA:           "20@cluster1,5@cluster2,5@cluster3",
			HLVB:           "10@cluster2",
			expectedResult: false,
		},
		{
			name:           "b.CV.source occurs in both a.CV and a.MV, dominates both",
			HLVA:           "2@cluster1,1@cluster1,3@cluster2",
			HLVB:           "4@cluster1",
			expectedResult: false,
		},
		{
			name:           "b.CV.source occurs in both a.CV and a.MV, dominates only a.MV",
			HLVA:           "4@cluster1,1@cluster1,2@cluster2",
			HLVB:           "3@cluster1",
			expectedResult: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			hlvA := createHLVForTest(t, testCase.HLVA)
			hlvB := createHLVForTest(t, testCase.HLVB)
			if testCase.expectedResult {
				require.True(t, hlvA.isDominating(hlvB), "Expected %s to dominate %s", testCase.HLVA, testCase.HLVB)
			} else {
				require.False(t, hlvA.isDominating(hlvB), "Expected %s not to dominate %s", testCase.HLVA, testCase.HLVB)

			}
		})
	}
}

// createHLVForTest is a helper function to create a HLV for use in a test. This uses the CBL wire format.
func createHLVForTest(tb *testing.T, input string) *HybridLogicalVector {
	if input == "" {
		return NewHybridLogicalVector()
	}
	hlv, _, err := extractHLVFromBlipString(input)
	require.NoError(tb, err)
	return hlv
}

// Tests import of server-side mutations made by HLV-aware and non-HLV-aware peers
func TestHLVImport(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyMigrate, base.KeyImport)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	type outputData struct {
		docID             string
		preImportHLV      *HybridLogicalVector
		preImportCas      uint64
		preImportMou      *MetadataOnlyUpdate
		postImportCas     uint64
		preImportRevSeqNo uint64
	}

	var standardBody = []byte(`{"prop":"value"}`)
	otherSource := "otherSource"
	var testCases = []struct {
		name        string
		preFunc     func(t *testing.T, collection *DatabaseCollectionWithUser, docID string)
		expectedMou func(output *outputData) *MetadataOnlyUpdate
		expectedHLV func(output *outputData) *HybridLogicalVector
	}{
		{
			name: "SDK write, no existing doc",
			preFunc: func(t *testing.T, collection *DatabaseCollectionWithUser, docID string) {
				_, err := collection.dataStore.WriteCas(docID, 0, 0, standardBody, sgbucket.Raw)
				require.NoError(t, err, "write error")
			},
			expectedMou: func(output *outputData) *MetadataOnlyUpdate {
				return &MetadataOnlyUpdate{
					HexCAS:           string(base.Uint64CASToLittleEndianHex(output.postImportCas)),
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(output.preImportCas)),
					PreviousRevSeqNo: output.preImportRevSeqNo,
				}
			},
			expectedHLV: func(output *outputData) *HybridLogicalVector {
				return &HybridLogicalVector{
					SourceID:          db.EncodedSourceID,
					Version:           output.preImportCas,
					CurrentVersionCAS: output.preImportCas,
				}
			},
		},
		{
			name: "SDK write, existing doc",
			preFunc: func(t *testing.T, collection *DatabaseCollectionWithUser, docID string) {
				_, doc, err := collection.Put(ctx, docID, Body{"foo": "bar"})
				require.NoError(t, err)
				_, err = collection.dataStore.WriteCas(docID, 0, doc.Cas, standardBody, sgbucket.Raw)
				require.NoError(t, err, "write error")
			},
			expectedMou: func(output *outputData) *MetadataOnlyUpdate {
				return &MetadataOnlyUpdate{
					HexCAS:           string(base.Uint64CASToLittleEndianHex(output.postImportCas)),
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(output.preImportCas)),
					PreviousRevSeqNo: output.preImportRevSeqNo,
				}
			},
			expectedHLV: func(output *outputData) *HybridLogicalVector {
				return &HybridLogicalVector{
					SourceID:          db.EncodedSourceID,
					Version:           output.preImportCas,
					CurrentVersionCAS: output.preImportCas,
				}
			},
		},
		{
			name: "HLV write from without mou",
			preFunc: func(t *testing.T, collection *DatabaseCollectionWithUser, docID string) {
				hlvHelper := NewHLVAgent(t, collection.dataStore, otherSource, "_vv")
				_ = hlvHelper.InsertWithHLV(ctx, docID)
			},
			expectedMou: func(output *outputData) *MetadataOnlyUpdate {
				return &MetadataOnlyUpdate{
					HexCAS:           string(base.Uint64CASToLittleEndianHex(output.postImportCas)),
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(output.preImportCas)),
					PreviousRevSeqNo: output.preImportRevSeqNo,
				}
			},
			expectedHLV: func(output *outputData) *HybridLogicalVector {
				return &HybridLogicalVector{
					SourceID:          EncodeSource(otherSource),
					Version:           output.preImportCas,
					CurrentVersionCAS: output.preImportCas,
				}
			},
		},
		{
			name: "XDCR stamped with _mou",
			preFunc: func(t *testing.T, collection *DatabaseCollectionWithUser, docID string) {
				hlvHelper := NewHLVAgent(t, collection.dataStore, otherSource, "_vv")
				cas := hlvHelper.InsertWithHLV(ctx, docID)

				_, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, docID, []string{base.VirtualXattrRevSeqNo})
				require.NoError(t, err)
				mou := &MetadataOnlyUpdate{
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(cas)),
					PreviousRevSeqNo: RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo]),
				}
				opts := &sgbucket.MutateInOptions{
					MacroExpansion: []sgbucket.MacroExpansionSpec{
						sgbucket.NewMacroExpansionSpec(XattrMouCasPath(), sgbucket.MacroCas),
					},
				}
				_, err = collection.dataStore.UpdateXattrs(ctx, docID, 0, cas, map[string][]byte{base.MouXattrName: base.MustJSONMarshal(t, mou)}, opts)
				require.NoError(t, err)
			},
			expectedMou: func(output *outputData) *MetadataOnlyUpdate {
				return &MetadataOnlyUpdate{
					HexCAS:           string(base.Uint64CASToLittleEndianHex(output.postImportCas)),
					PreviousHexCAS:   output.preImportMou.PreviousHexCAS,
					PreviousRevSeqNo: output.preImportRevSeqNo,
				}
			},
			expectedHLV: func(output *outputData) *HybridLogicalVector {
				return output.preImportHLV
			},
		},
		{
			name: "invalid _mou, but valid hlv",
			preFunc: func(t *testing.T, collection *DatabaseCollectionWithUser, docID string) {
				hlvHelper := NewHLVAgent(t, collection.dataStore, otherSource, "_vv")
				cas := hlvHelper.InsertWithHLV(ctx, docID)

				_, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, docID, []string{base.VirtualXattrRevSeqNo})
				require.NoError(t, err)
				mou := &MetadataOnlyUpdate{
					HexCAS:           "invalid",
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(cas)),
					PreviousRevSeqNo: RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo]),
				}
				_, err = collection.dataStore.UpdateXattrs(ctx, docID, 0, cas, map[string][]byte{base.MouXattrName: base.MustJSONMarshal(t, mou)}, nil)
				require.NoError(t, err)
			},
			expectedMou: func(output *outputData) *MetadataOnlyUpdate {
				return &MetadataOnlyUpdate{
					HexCAS:           string(base.Uint64CASToLittleEndianHex(output.postImportCas)),
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(output.preImportCas)),
					PreviousRevSeqNo: output.preImportRevSeqNo,
				}
			},
			expectedHLV: func(output *outputData) *HybridLogicalVector {
				return &HybridLogicalVector{
					SourceID:          db.EncodedSourceID,
					Version:           output.preImportCas,
					CurrentVersionCAS: output.preImportCas,
					PreviousVersions: map[string]uint64{
						EncodeSource(otherSource): output.preImportHLV.CurrentVersionCAS,
					},
				}
			},
		},
		{
			name: "SDK write with valid _mou, but no HLV",
			preFunc: func(t *testing.T, collection *DatabaseCollectionWithUser, docID string) {
				cas, err := collection.dataStore.WriteCas(docID, 0, 0, standardBody, sgbucket.Raw)
				require.NoError(t, err)
				_, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, docID, []string{base.VirtualXattrRevSeqNo})
				require.NoError(t, err)

				mou := &MetadataOnlyUpdate{
					PreviousHexCAS:   string(base.Uint64CASToLittleEndianHex(cas)),
					PreviousRevSeqNo: RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo]),
				}
				opts := &sgbucket.MutateInOptions{
					MacroExpansion: []sgbucket.MacroExpansionSpec{
						sgbucket.NewMacroExpansionSpec(XattrMouCasPath(), sgbucket.MacroCas),
					},
				}
				_, err = collection.dataStore.UpdateXattrs(ctx, docID, 0, cas, map[string][]byte{base.MouXattrName: base.MustJSONMarshal(t, mou)}, opts)
				require.NoError(t, err)
			},
			expectedMou: func(output *outputData) *MetadataOnlyUpdate {
				return &MetadataOnlyUpdate{
					HexCAS:           string(base.Uint64CASToLittleEndianHex(output.postImportCas)),
					PreviousHexCAS:   output.preImportMou.PreviousHexCAS,
					PreviousRevSeqNo: output.preImportRevSeqNo,
				}
			},
			expectedHLV: func(output *outputData) *HybridLogicalVector {
				return &HybridLogicalVector{
					SourceID:          db.EncodedSourceID,
					Version:           output.preImportCas,
					CurrentVersionCAS: output.preImportCas,
				}
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			docID := strings.ToLower(testCase.name)
			testCase.preFunc(t, collection, docID)

			xattrNames := []string{base.SyncXattrName, base.VvXattrName, base.MouXattrName, base.VirtualXattrRevSeqNo}
			_, existingXattrs, preImportCas, err := collection.dataStore.GetWithXattrs(ctx, docID, xattrNames)
			require.NoError(t, err)
			revSeqNo := RetrieveDocRevSeqNo(t, existingXattrs[base.VirtualXattrRevSeqNo])

			var preImportMou *MetadataOnlyUpdate
			if mouBytes, ok := existingXattrs[base.MouXattrName]; ok && mouBytes != nil {
				require.NoError(t, base.JSONUnmarshal(mouBytes, &preImportMou))
			}
			importOpts := importDocOptions{
				isDelete: false,
				expiry:   nil,
				mode:     ImportFromFeed,
				revSeqNo: revSeqNo,
			}
			_, err = collection.ImportDocRaw(ctx, docID, standardBody, existingXattrs, importOpts, preImportCas)
			require.NoError(t, err, "import error")

			xattrs, finalCas, err := collection.dataStore.GetXattrs(ctx, docID, xattrNames)
			require.NoError(t, err)
			require.NotEqual(t, preImportCas, finalCas)

			// validate _sync.cas was expanded to document cas
			require.Contains(t, xattrs, base.SyncXattrName)
			var syncData *SyncData
			require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
			require.Equal(t, finalCas, base.HexCasToUint64(syncData.Cas))

			output := outputData{
				docID:             docID,
				preImportCas:      preImportCas,
				preImportMou:      preImportMou,
				postImportCas:     finalCas,
				preImportRevSeqNo: revSeqNo,
			}
			if existingHLV, ok := existingXattrs[base.VvXattrName]; ok {

				require.NoError(t, base.JSONUnmarshal(existingHLV, &output.preImportHLV))
			}

			if testCase.expectedMou != nil {
				require.Contains(t, xattrs, base.MouXattrName)
				var mou *MetadataOnlyUpdate
				require.NoError(t, base.JSONUnmarshal(xattrs[base.MouXattrName], &mou))
				require.Contains(t, xattrs, base.MouXattrName)
				require.Equal(t, *testCase.expectedMou(&output), *mou)
			}
			var hlv *HybridLogicalVector
			require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
			require.Equal(t, *testCase.expectedHLV(&output), *hlv)
		})
	}

}

// TestHLVMapToCBLString:
//   - Purpose is to test the ability to extract from HLV maps in CBL replication format
//   - Three test cases, both MV and PV defined, only PV defined and only MV defined
//   - To protect against flake added some splitting of the result string in test case 1 as we cannot guarantee the
//     order the string will be made in given map iteration is random
func TestHLVMapToCBLString(t *testing.T) {
	testCases := []struct {
		name           string
		hlv            HybridLogicalVector
		possibleOutput []string // more than one is possible since order of mv/pv not defined
	}{
		{
			name: "Both PV and mv",
			hlv: HybridLogicalVector{
				PreviousVersions: map[string]uint64{
					"a": 1,
					"b": 2,
				},
				MergeVersions: map[string]uint64{
					"c": 3,
					"d": 4,
				},
			},
			possibleOutput: []string{
				"3@c,4@d;1@a,2@b",
				"3@c,4@d;2@b,1@a",
				"4@d,3@c;1@a,2@b",
				"4@d,3@c;2@b,1@a",
			},
		},
		{
			name: "Single PV",
			hlv: HybridLogicalVector{
				PreviousVersions: map[string]uint64{
					"a": 1,
				},
			},
			possibleOutput: []string{
				"1@a",
			},
		},
		{
			name: "Multiple PV",
			hlv: HybridLogicalVector{
				PreviousVersions: map[string]uint64{
					"a": 1,
					"b": 2,
				},
			},
			possibleOutput: []string{
				"1@a,2@b",
				"2@b,1@a",
			},
		},
		{
			name: "Single MV only",
			hlv: HybridLogicalVector{
				MergeVersions: map[string]uint64{
					"a": 1,
				},
			},
			possibleOutput: []string{
				"1@a;",
			},
		},
		{
			name: "MV only",
			hlv: HybridLogicalVector{
				MergeVersions: map[string]uint64{
					"a": 1,
					"b": 2,
				},
			},
			possibleOutput: []string{
				"1@a,2@b;",
				"2@b,1@a;",
			},
		},
		{
			name: "Long MV only",
			hlv: HybridLogicalVector{
				MergeVersions: map[string]uint64{
					"a": 1,
					"b": 2,
					"c": 3,
				},
			},
			possibleOutput: []string{
				"1@a,2@b,3@c;",
				"1@a,3@c,2@b;",
				"2@b,1@a,3@c;",
				"2@b,3@c,1@a;",
				"3@c,1@a,2@b;",
				"3@c,2@b,1@a;",
			},
		},
		{
			name: "CV only",
			hlv: HybridLogicalVector{
				SourceID: "a",
				Version:  1,
			},
			possibleOutput: []string{
				"",
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// there is no guarantee of order in the map so all possible strings are listed
			require.Contains(t, test.possibleOutput, test.hlv.ToHistoryForHLV(), "expected string not as expected for %#+v", test.hlv)
		})
	}
}

// TestInvalidHLVOverChangesMessage:
//   - Test hlv string that has too many sections to it (parts delimited by ;)
//   - Test hlv string that is empty
//   - Assert that ExtractHLVFromBlipMessage will return error in both cases
func TestInvalidHLVInBlipMessageForm(t *testing.T) {
	testCases := []struct {
		name   string
		hlv    string
		errMsg string
	}{
		{
			name:   "Too many sections",
			hlv:    "25@def; 22@def,21@eff; 20@abc,18@hij; 222@hiowdwdew, 5555@dhsajidfgd",
			errMsg: "invalid hlv in changes message received",
		},
		{
			name:   "empty",
			hlv:    "",
			errMsg: "empty hlv",
		},
		{
			name:   "cv,mv,mv (duplicate in mv)",
			hlv:    "1@abc,1@abc,2@def",
			errMsg: "cv exists in mv",
		},
		{
			name:   "cv,mv,mv,pv (duplicate in mv)",
			hlv:    "2@abc,1@abc,2@def;1@abc",
			errMsg: "found in pv and mv",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.NotEmpty(t, testCase.errMsg) // make sure err msg is specified
			hlv, legacyRevs, err := extractHLVFromBlipString(testCase.hlv)
			require.ErrorContains(t, err, testCase.errMsg, "expected err for %s", testCase.hlv)
			require.Nil(t, hlv)
			require.Nil(t, legacyRevs)
		})
	}
}

type extractHLVFromBlipMsgBMarkCases = struct {
	name        string
	hlvString   string
	expectedHLV HybridLogicalVector
	legacyRevs  []string
}

func getHLVTestCases(t testing.TB) []extractHLVFromBlipMsgBMarkCases {
	return []extractHLVFromBlipMsgBMarkCases{
		{
			name:      "cv,mv,mv",
			hlvString: "25@def, 22@def, 21@eff",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				MergeVersions: map[string]uint64{
					"def": stringHexToUint(t, "22"),
					"eff": stringHexToUint(t, "21"),
				},
			},
		},
		{
			name:      "cv, mv and pv, leading spaces",                                                                                                                                                           // with spaces
			hlvString: "25@def, 22@def, 21@eff, 500@x, 501@xx, 4000@xxx, 700@y, 701@yy, 702@yyy; 20@abc, 18@hij, 3@x2, 4@xx2, 5@xxx2, 6@xxxx, 7@xxxxx, 3@y2, 4@yy2, 5@yyy2, 6@yyyy, 7@yyyyy, 2@xy, 3@xyy, 4@xxy", // 15 pv 8 mv
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				MergeVersions: map[string]uint64{
					"def": stringHexToUint(t, "22"),
					"eff": stringHexToUint(t, "21"),
					"x":   stringHexToUint(t, "500"),
					"xx":  stringHexToUint(t, "501"),
					"xxx": stringHexToUint(t, "4000"),
					"y":   stringHexToUint(t, "700"),
					"yy":  stringHexToUint(t, "701"),
					"yyy": stringHexToUint(t, "702"),
				},
				PreviousVersions: map[string]uint64{
					"abc":   stringHexToUint(t, "20"),
					"hij":   stringHexToUint(t, "18"),
					"x2":    stringHexToUint(t, "3"),
					"xx2":   stringHexToUint(t, "4"),
					"xxx2":  stringHexToUint(t, "5"),
					"xxxx":  stringHexToUint(t, "6"),
					"xxxxx": stringHexToUint(t, "7"),
					"y2":    stringHexToUint(t, "3"),
					"yy2":   stringHexToUint(t, "4"),
					"yyy2":  stringHexToUint(t, "5"),
					"yyyy":  stringHexToUint(t, "6"),
					"yyyyy": stringHexToUint(t, "7"),
					"xy":    stringHexToUint(t, "2"),
					"xyy":   stringHexToUint(t, "3"),
					"xxy":   stringHexToUint(t, "4"),
				},
			},
		},
		{
			name:      "cv mv and pv, no spaces",                                                                                                                                          // without spaces
			hlvString: "25@def,22@def,21@eff,500@x,501@xx,4000@xxx,700@y,701@yy,702@yyy;20@abc,18@hij,3@x2,4@xx2,5@xxx2,6@xxxx,7@xxxxx,3@y2,4@yy2,5@yyy2,6@yyyy,7@yyyyy,2@xy,3@xyy,4@xxy", // 15 pv 8 mv
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				MergeVersions: map[string]uint64{
					"def": stringHexToUint(t, "22"),
					"eff": stringHexToUint(t, "21"),
					"x":   stringHexToUint(t, "500"),
					"xx":  stringHexToUint(t, "501"),
					"xxx": stringHexToUint(t, "4000"),
					"y":   stringHexToUint(t, "700"),
					"yy":  stringHexToUint(t, "701"),
					"yyy": stringHexToUint(t, "702"),
				},
				PreviousVersions: map[string]uint64{
					"abc":   stringHexToUint(t, "20"),
					"hij":   stringHexToUint(t, "18"),
					"x2":    stringHexToUint(t, "3"),
					"xx2":   stringHexToUint(t, "4"),
					"xxx2":  stringHexToUint(t, "5"),
					"xxxx":  stringHexToUint(t, "6"),
					"xxxxx": stringHexToUint(t, "7"),
					"y2":    stringHexToUint(t, "3"),
					"yy2":   stringHexToUint(t, "4"),
					"yyy2":  stringHexToUint(t, "5"),
					"yyyy":  stringHexToUint(t, "6"),
					"yyyyy": stringHexToUint(t, "7"),
					"xy":    stringHexToUint(t, "2"),
					"xyy":   stringHexToUint(t, "3"),
					"xxy":   stringHexToUint(t, "4"),
				},
			},
		},
		{
			name:      "cv; pv,pv",
			hlvString: "25@def; 20@abc,18@hij",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				PreviousVersions: map[string]uint64{
					"abc": stringHexToUint(t, "20"),
					"hij": stringHexToUint(t, "18"),
				},
			},
		},
		{
			name:      "cv,mv ;pv, mixed spacing",
			hlvString: "25@def, 22@def,21@eff; 20@abc,18@hij,3@x,4@xx,5@xxx,6@xxxx,7@xxxxx,3@y,4@yy,5@yyy,6@yyyy,7@yyyyy,2@xy,3@xyy,4@xxy", // 15
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				MergeVersions: map[string]uint64{
					"def": stringHexToUint(t, "22"),
					"eff": stringHexToUint(t, "21"),
				},
				PreviousVersions: map[string]uint64{
					"abc":   stringHexToUint(t, "20"),
					"hij":   stringHexToUint(t, "18"),
					"x":     stringHexToUint(t, "3"),
					"xx":    stringHexToUint(t, "4"),
					"xxx":   stringHexToUint(t, "5"),
					"xxxx":  stringHexToUint(t, "6"),
					"xxxxx": stringHexToUint(t, "7"),
					"y":     stringHexToUint(t, "3"),
					"yy":    stringHexToUint(t, "4"),
					"yyy":   stringHexToUint(t, "5"),
					"yyyy":  stringHexToUint(t, "6"),
					"yyyyy": stringHexToUint(t, "7"),
					"xy":    stringHexToUint(t, "2"),
					"xyy":   stringHexToUint(t, "3"),
					"xxy":   stringHexToUint(t, "4"),
				},
			},
		},
		{
			name:      "cv only",
			hlvString: "24@def",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "24"),
				SourceID:          "def",
			},
		},
		{
			name:      "cv; pv base64 encoded",
			hlvString: "1@Hell0CA; 1@1Hr0k43xS662TToxODDAxQ",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "1"),
				SourceID:          "Hell0CA",
				PreviousVersions: map[string]uint64{
					"1Hr0k43xS662TToxODDAxQ": stringHexToUint(t, "1"),
				},
			},
		},
		{
			name:      "cv,mv,mv;pv,pv - small",
			hlvString: "25@def, 22@def,21@eff; 20@abc,18@hij",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				MergeVersions: map[string]uint64{
					"def": stringHexToUint(t, "22"),
					"eff": stringHexToUint(t, "21"),
				},
				PreviousVersions: map[string]uint64{
					"abc": stringHexToUint(t, "20"),
					"hij": stringHexToUint(t, "18"),
				},
			},
		},
		{
			name:      "cv,mv,mv;pv,pv,legacyrev",
			hlvString: "25@def, 22@def,21@eff; 20@abc,18@hij,1-abc",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
				MergeVersions: map[string]uint64{
					"def": stringHexToUint(t, "22"),
					"eff": stringHexToUint(t, "21"),
				},
				PreviousVersions: map[string]uint64{
					"abc": stringHexToUint(t, "20"),
					"hij": stringHexToUint(t, "18"),
				},
			},
			legacyRevs: []string{"1-abc"},
		},
		{
			name:      "cv; trailing semicolon",
			hlvString: "25@def;",
			expectedHLV: HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           stringHexToUint(t, "25"),
				SourceID:          "def",
			},
		},
	}
}

// TestExtractHLVFromChangesMessage:
//   - Each test case gets run through extractHLVFromBlipString and assert that the resulting HLV
//     is correct to what is expected
func TestExtractHLVFromChangesMessage(t *testing.T) {
	for _, test := range getHLVTestCases(t) {
		t.Run(test.name, func(t *testing.T) {
			hlv, legacyRevs, err := extractHLVFromBlipString(test.hlvString)
			require.NoError(t, err)

			require.Equal(t, test.expectedHLV, *hlv, "HLV not parsed correctly for %s", test.hlvString)
			require.Equal(t, test.legacyRevs, legacyRevs)
		})
	}
}

// TestExtractCVFromProposeChangesRev
func TestExtractCVFromProposeChangesRev(t *testing.T) {

	var testCases = []struct {
		name       string
		inputRev   string
		expectedCV string
	}{
		{
			name:       "cv only",
			inputRev:   "1000@abc",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv trailing whitespace",
			inputRev:   "1000@abc ",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv and mv",
			inputRev:   "1000@abc,900@abc,900@def",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv and mv trailing whitespace",
			inputRev:   "1000@abc ,900@abc,900@def",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv trailing whitespace",
			inputRev:   "1000@abc ",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv and pv",
			inputRev:   "1000@abc;900@def,800@ghi",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv, mv, pv",
			inputRev:   "1000@abc,900@abc,900@def;900@def,800@ghi",
			expectedCV: "1000@abc",
		},
		{
			name:       "cv, mv, pv, creative whitespace",
			inputRev:   " 1000@abc ,900@abc, 900@def; 900@def,800@ghi  ",
			expectedCV: "1000@abc",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			cv := ExtractCVFromProposeChangesRev(test.inputRev)
			assert.Equal(t, test.expectedCV, cv)
		})
	}
}

func BenchmarkExtractHLVFromBlipMessage(b *testing.B) {
	for _, bm := range getHLVTestCases(b) {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, _ = extractHLVFromBlipString(bm.hlvString)
			}
		})
	}
}

func TestParseCBLVersion(t *testing.T) {
	vrsString := "19@YWJj"

	vrs, err := ParseVersion(vrsString)
	require.NoError(t, err)
	assert.Equal(t, "YWJj", vrs.SourceID)
	assert.Equal(t, uint64(25), vrs.Value)

	cblString := vrs.String()
	assert.Equal(t, vrsString, cblString)
}

// TestVersionDeltaCalculation:
//   - Create some random versions and assign to a source/version map
//   - Convert the map to deltas and assert that first item in list is greater than all other elements
//   - Create a test HLV and convert it to persisted format in bytes
//   - Convert this back to in memory format, assert each elem of in memory format previous versions map is the same as
//     the corresponding element in the original pvMap
//   - Do the same for a pv map that will have two entries with the same version value
//   - Do the same as above but for nil maps
func TestVersionDeltaCalculation(t *testing.T) {
	src1 := "src1"
	src2 := "src2"
	src3 := "src3"
	src4 := "src4"
	src5 := "src5"

	timeNow := time.Now().UnixNano()
	// make some version deltas
	v1 := uint64(timeNow - rand.Int64N(1000000000000))
	v2 := uint64(timeNow - rand.Int64N(1000000000000))
	v3 := uint64(timeNow - rand.Int64N(1000000000000))
	v4 := uint64(timeNow - rand.Int64N(1000000000000))
	v5 := uint64(timeNow - rand.Int64N(1000000000000))

	// make map of source to version
	pvMap := make(HLVVersions)
	pvMap[src1] = v1
	pvMap[src2] = v2
	pvMap[src3] = v3
	pvMap[src4] = v4
	pvMap[src5] = v5

	// convert to version delta map assert that first element is larger than all other elements
	deltas := VersionDeltas(pvMap)
	assert.Greater(t, deltas[0].Value, deltas[1].Value)
	assert.Greater(t, deltas[0].Value, deltas[2].Value)
	assert.Greater(t, deltas[0].Value, deltas[3].Value)
	assert.Greater(t, deltas[0].Value, deltas[4].Value)

	// create a test hlv
	inputHLVA := "2@cluster3"
	hlv := createHLVForTest(t, inputHLVA)
	hlv.PreviousVersions = pvMap
	expSrc := hlv.SourceID
	expVal := hlv.Version
	expCas := hlv.CurrentVersionCAS

	// convert hlv to persisted format
	vvXattr, err := base.JSONMarshal(&hlv)
	require.NoError(t, err)

	// convert the bytes back to an in memory format of hlv
	memHLV := NewHybridLogicalVector()
	err = base.JSONUnmarshal(vvXattr, &memHLV)
	require.NoError(t, err)

	assert.Equal(t, pvMap[src1], memHLV.PreviousVersions[src1])
	assert.Equal(t, pvMap[src2], memHLV.PreviousVersions[src2])
	assert.Equal(t, pvMap[src3], memHLV.PreviousVersions[src3])
	assert.Equal(t, pvMap[src4], memHLV.PreviousVersions[src4])
	assert.Equal(t, pvMap[src5], memHLV.PreviousVersions[src5])

	// assert that the other elements are as expected
	assert.Equal(t, expSrc, memHLV.SourceID)
	assert.Equal(t, expVal, memHLV.Version)
	assert.Equal(t, expCas, memHLV.CurrentVersionCAS)
	assert.Len(t, memHLV.MergeVersions, 0)

	// test hlv with two pv version entries that are equal to each other
	hlv = createHLVForTest(t, inputHLVA)
	// make src3 have the same version value as src2
	pvMap[src3] = pvMap[src2]
	hlv.PreviousVersions = pvMap

	// convert hlv to persisted format
	vvXattr, err = base.JSONMarshal(&hlv)
	require.NoError(t, err)

	// convert the bytes back to an in memory format of hlv
	memHLV = NewHybridLogicalVector()
	err = base.JSONUnmarshal(vvXattr, &memHLV)
	require.NoError(t, err)

	assert.Equal(t, pvMap[src1], memHLV.PreviousVersions[src1])
	assert.Equal(t, pvMap[src2], memHLV.PreviousVersions[src2])
	assert.Equal(t, pvMap[src3], memHLV.PreviousVersions[src3])
	assert.Equal(t, pvMap[src4], memHLV.PreviousVersions[src4])
	assert.Equal(t, pvMap[src5], memHLV.PreviousVersions[src5])

	// assert that the other elements are as expected
	assert.Equal(t, expSrc, memHLV.SourceID)
	assert.Equal(t, expVal, memHLV.Version)
	assert.Equal(t, expCas, memHLV.CurrentVersionCAS)
	assert.Len(t, memHLV.MergeVersions, 0)

	// test hlv with nil merge versions and nil previous versions to test panic safe
	pvMap = nil
	hlv2 := createHLVForTest(t, inputHLVA)
	hlv2.PreviousVersions = pvMap
	hlv2.MergeVersions = nil
	deltas = VersionDeltas(pvMap)
	assert.Nil(t, deltas)

	// construct byte array from hlv
	vvXattr, err = base.JSONMarshal(&hlv2)
	require.NoError(t, err)
	// convert the bytes back to an in memory format of hlv
	memHLV = &HybridLogicalVector{}
	err = base.JSONUnmarshal(vvXattr, &memHLV)
	require.NoError(t, err)

	// assert in memory hlv is as expected
	assert.Equal(t, expSrc, memHLV.SourceID)
	assert.Equal(t, expVal, memHLV.Version)
	assert.Equal(t, expCas, memHLV.CurrentVersionCAS)
	assert.Len(t, memHLV.PreviousVersions, 0)
	assert.Len(t, memHLV.MergeVersions, 0)
}

func stringHexToUint(t testing.TB, value string) uint64 {
	intValue, err := strconv.ParseUint(value, 16, 64)
	require.NoError(t, err)
	return intValue
}

func TestHLVInvalidateMV(t *testing.T) {
	testCases := []struct {
		name        string
		existingHLV string
		expectedHLV string
	}{
		{
			name:        "cv",
			existingHLV: "2@a",
			expectedHLV: "2@a",
		},
		{
			name:        "cv mv, unique source",
			existingHLV: "2@a,2@b,3@c",
			expectedHLV: "2@a;2@b,3@c",
		},
		{
			name:        "cv mv, duplicate source",
			existingHLV: "2@a,1@a,2@b",
			expectedHLV: "2@a;2@b",
		},
		{
			name:        "cv pv",
			existingHLV: "2@a;1@b,2@c",
			expectedHLV: "2@a;1@b,2@c",
		},
		{
			name:        "cv mv pv, unique sources",
			existingHLV: "2@a,1@b,2@c;1@d,2@e",
			expectedHLV: "2@a;1@b,2@c,1@d,2@e",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			inputHLV := createHLVForTest(t, testCase.existingHLV)
			inputHLV.InvalidateMV()
			expectedHLV := createHLVForTest(t, testCase.expectedHLV)
			require.True(t, expectedHLV.Equal(inputHLV), "Expected %#v but found %#v", expectedHLV, inputHLV)
		})
	}
}

func TestAddVersion(t *testing.T) {
	testCases := []struct {
		name        string
		initialHLV  string
		newVersion  string
		expectedHLV string
	}{
		{
			name:        "cv only, same source",
			initialHLV:  "100@a",
			newVersion:  "110@a",
			expectedHLV: "110@a",
		},
		{
			name:        "cv only, different source",
			initialHLV:  "100@a",
			newVersion:  "110@b",
			expectedHLV: "110@b;100@a",
		},
		{
			name:        "single pv, cv source update",
			initialHLV:  "100@b;90@a",
			newVersion:  "110@b",
			expectedHLV: "110@b;90@a",
		},
		{
			name:        "single pv, pv source update",
			initialHLV:  "100@b;90@a",
			newVersion:  "110@a",
			expectedHLV: "110@a;100@b",
		},
		{
			name:        "single pv, new source update",
			initialHLV:  "100@b;90@a",
			newVersion:  "110@c",
			expectedHLV: "110@c;90@a,100@b",
		},
		{
			name:        "multiple pv, cv source update",
			initialHLV:  "100@b;90@a,80@c",
			newVersion:  "110@b",
			expectedHLV: "110@b;90@a,80@c",
		},
		{
			name:        "multiple pv, pv source update",
			initialHLV:  "100@b;90@a,80@c",
			newVersion:  "110@a",
			expectedHLV: "110@a;100@b,80@c",
		},
		{
			name:        "multiple pv, new source update",
			initialHLV:  "100@b;90@a,80@c",
			newVersion:  "110@d",
			expectedHLV: "110@d;90@a,100@b,80@c",
		},
		{
			name:        "mv only, cv source update",
			initialHLV:  "100@b,90@b,80@a",
			newVersion:  "110@b",
			expectedHLV: "110@b;80@a",
		},
		{
			name:        "mv only, mv source update",
			initialHLV:  "100@b,90@b,80@a",
			newVersion:  "110@a",
			expectedHLV: "110@a;100@b",
		},
		{
			name:        "mv only, cv not in mv, cv source update",
			initialHLV:  "100@c,90@b,80@a",
			newVersion:  "110@c",
			expectedHLV: "110@c;90@b,80@a",
		},
		{
			name:        "mv only, cv not in mv, mv source update",
			initialHLV:  "100@c,90@b,80@a",
			newVersion:  "110@b",
			expectedHLV: "110@b;100@c,80@a",
		},
		{
			name:        "mv only, cv not in mv, new source update",
			initialHLV:  "100@c,90@b,80@a",
			newVersion:  "110@d",
			expectedHLV: "110@d;100@c,90@b,80@a",
		},
		{
			name:        "mv and pv, cv source update",
			initialHLV:  "100@c,90@b,80@a;70@d",
			newVersion:  "110@c",
			expectedHLV: "110@c;90@b,80@a,70@d",
		},
		{
			name:        "mv and pv, mv source update",
			initialHLV:  "100@c,90@b,80@a;70@d",
			newVersion:  "110@b",
			expectedHLV: "110@b;100@c,80@a,70@d",
		},
		{
			name:        "mv and pv, pv source update",
			initialHLV:  "100@c,90@b,80@a;70@d",
			newVersion:  "110@d",
			expectedHLV: "110@d;100@c,90@b,80@a",
		},
		{
			name:        "mv and pv, new source update",
			initialHLV:  "100@c,90@b,80@a;70@d",
			newVersion:  "110@e",
			expectedHLV: "110@e;100@c,90@b,80@a,70@d",
		},
		{
			name:        "mv (with cv source) and pv, cv source update",
			initialHLV:  "100@b,90@b,80@a;70@c",
			newVersion:  "110@b",
			expectedHLV: "110@b;80@a,70@c",
		},
		{
			name:        "mv (with cv source) and pv, mv source update",
			initialHLV:  "100@b,90@b,80@a;70@c",
			newVersion:  "110@a",
			expectedHLV: "110@a;100@b,70@c",
		},
		{
			name:        "mv (with cv source) and pv, pv source update",
			initialHLV:  "100@b,90@b,80@a;70@c",
			newVersion:  "110@c",
			expectedHLV: "110@c;100@b,80@a",
		},
		{
			name:        "mv (with cv source) and pv, new source update",
			initialHLV:  "100@b,90@b,80@a;70@c",
			newVersion:  "110@d",
			expectedHLV: "110@d;100@b,80@a,70@c",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			hlv, _, err := extractHLVFromBlipString(tc.initialHLV)
			require.NoError(t, err, "unable to parse initialHLV")
			newVersion, err := ParseVersion(tc.newVersion)
			require.NoError(t, err)

			err = hlv.AddVersion(newVersion)
			require.NoError(t, err)

			expectedHLV, _, err := extractHLVFromBlipString(tc.expectedHLV)
			require.NoError(t, err)
			require.True(t, hlv.Equal(expectedHLV), "expected %#v does not match actual %#v", expectedHLV, hlv)

		})
	}
}

func TestIsInConflict(t *testing.T) {
	testCases := []struct {
		name        string
		localHLV    string
		incomingHLV string
		conflict    HLVConflictStatus
	}{
		{
			name:        "CV equal",
			localHLV:    "111@abc;123@def",
			incomingHLV: "111@abc;123@ghi",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "no conflict case",
			localHLV:    "111@abc;123@def",
			incomingHLV: "112@abc;123@ghi",
			conflict:    HLVNoConflict,
		},
		{
			name:        "local revision is newer",
			localHLV:    "111@abc;123@def",
			incomingHLV: "100@abc;123@ghi",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "merge versions match",
			localHLV:    "130@abc,123@def,100@ghi;50@jkl",
			incomingHLV: "150@mno,123@def,100@ghi;50@jkl",
			conflict:    HLVNoConflict,
		},
		{
			name:        "cv conflict",
			localHLV:    "1@abc",
			incomingHLV: "1@def",
			conflict:    HLVConflict,
		},
		{
			name:        "Matching current source, newer version",
			localHLV:    "20@cluster1;2@cluster2",
			incomingHLV: "10@cluster1;1@cluster2",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "Matching current source and version",
			localHLV:    "20@cluster1;2@cluster2",
			incomingHLV: "20@cluster1;2@cluster2",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "B CV found in A's PV",
			localHLV:    "20@cluster1;10@cluster2",
			incomingHLV: "10@cluster2;15@cluster1",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "B CV older than A's PV for same source",
			localHLV:    "20@cluster1;15@cluster2",
			incomingHLV: "10@cluster2;15@cluster1",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "Unique sources in A",
			localHLV:    "20@cluster1;15@cluster2,3@cluster3",
			incomingHLV: "10@cluster2;10@cluster1",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "Unique sources in B",
			localHLV:    "20@cluster1",
			incomingHLV: "15@cluster1;3@cluster3",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "B has newer cv than A pv",
			localHLV:    "20@cluster2;10@cluster1",
			incomingHLV: "15@cluster1;20@cluster2",
			conflict:    HLVNoConflict,
		},
		{
			name:        "B's cv not found in A",
			localHLV:    "20@cluster2;10@cluster1",
			incomingHLV: "5@cluster3",
			conflict:    HLVConflict,
		},
		{
			name:        "a.MV dominates B.CV",
			localHLV:    "20@cluster1,20@cluster2,5@cluster3",
			incomingHLV: "10@cluster2",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
		{
			name:        "a.MV doesn't dominate B.CV",
			localHLV:    "20@cluster1,5@cluster2,5@cluster3",
			incomingHLV: "10@cluster2",
			conflict:    HLVConflict, // conflict since mv doesn't match
		},
		{
			name:        "b.CV.source occurs in both a.CV and a.MV, dominates both",
			localHLV:    "2@cluster1,1@cluster1,3@cluster2",
			incomingHLV: "4@cluster1",
			conflict:    HLVNoConflict,
		},
		{
			name:        "b.CV.source occurs in both a.CV and a.MV, dominates only a.MV",
			localHLV:    "4@cluster1,1@cluster1,2@cluster2",
			incomingHLV: "3@cluster1",
			conflict:    HLVNoConflictRevAlreadyPresent,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			localHLV, _, err := extractHLVFromBlipString(tc.localHLV)
			require.NoError(t, err)
			incomingHLV, _, err := extractHLVFromBlipString(tc.incomingHLV)
			require.NoError(t, err)

			require.Equal(t, tc.conflict, IsInConflict(t.Context(), localHLV, incomingHLV))
		})
	}
}

func TestHLVUpddateFromIncomingRemoteWins(t *testing.T) {
	testCases := []struct {
		name        string
		existingHLV string
		incomingHLV string
		finalHLV    string
	}{
		{
			name:        "update cv and add pv",
			existingHLV: "15@abc",
			incomingHLV: "25@def;20@abc",
			finalHLV:    "25@def;20@abc",
		},
		{
			name:        "update cv, move cv to pv",
			existingHLV: "15@abc;30@def",
			incomingHLV: "35@def;15@abc",
			finalHLV:    "35@def;15@abc",
		},
		{
			name:        "Add new MV",
			existingHLV: "",
			incomingHLV: "1@b,1@a,2@c",
			finalHLV:    "1@b,1@a,2@c",
		},
		{
			name:        "existing mv, move to pv",
			existingHLV: "3@c,2@b,1@a",
			incomingHLV: "4@c",
			finalHLV:    "4@c;2@b,1@a",
		},
		{
			name:        "incoming pv overwrite mv, equal values",
			existingHLV: "3@c,2@b,1@a",
			incomingHLV: "4@c;2@b,1@a",
			finalHLV:    "4@c;2@b,1@a",
		},
		{
			name:        "incoming mv overwrite pv, equal values",
			existingHLV: "3@c;2@b,1@a",
			incomingHLV: "4@c,2@b,1@a",
			finalHLV:    "4@c,2@b,1@a",
		},
		{
			name:        "incoming mv overwrite pv, greater values",
			existingHLV: "3@c;2@b,1@a",
			incomingHLV: "4@c,5@b,6@a",
			finalHLV:    "4@c,5@b,6@a",
		},
		// Invalid MV cleanup cases should preserve any conflicting versions from incoming HLV
		{
			// Invalid since MV should always have two values.
			name:        "Add single value MV",
			existingHLV: "",
			incomingHLV: "1@b,1@a",
			finalHLV:    "1@b,1@a",
		},
		{
			// Invalid since there should not be able to be an incoming merge conflict where a different newer version exists.
			name:        "incoming mv partially overlaps with pv",
			existingHLV: "3@c;2@b,6@a",
			incomingHLV: "4@c,2@b,1@a",
			finalHLV:    "4@c,2@b,1@a",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			localHLV := createHLVForTest(t, test.existingHLV)
			incomingHLV := createHLVForTest(t, test.incomingHLV)
			expectedHLV := createHLVForTest(t, test.finalHLV)

			localHLV.UpdateWithIncomingHLV(incomingHLV)
			require.True(t, localHLV.Equal(expectedHLV), "Expected HLV %s, actual HLV %s", test.finalHLV, hlvAsBlipString(t, localHLV))
		})
	}
}

func TestHLVUpdateFromIncomingNewCV(t *testing.T) {
	testCases := []struct {
		name        string
		existingHLV string
		incomingHLV string
		newCV       Version
		finalHLV    string
	}{
		{
			name:        "simple merge",
			existingHLV: "1@a",
			incomingHLV: "2@b",
			newCV:       Version{SourceID: "c", Value: 3},
			finalHLV:    "3@c,2@b,1@a",
		},
		{
			name:        "existing mv",
			existingHLV: "1@a,3@d,4@e",
			incomingHLV: "2@b",
			newCV:       Version{SourceID: "c", Value: 5},
			finalHLV:    "5@c,2@b,1@a;4@e,3@d",
		},
		{
			name:        "existing pv",
			existingHLV: "1@a;3@d,4@e",
			incomingHLV: "2@b",
			newCV:       Version{SourceID: "c", Value: 5},
			finalHLV:    "5@c,2@b,1@a;4@e,3@d",
		},
		{
			name:        "incoming mv",
			existingHLV: "1@a",
			incomingHLV: "4@b,3@d,2@e",
			newCV:       Version{SourceID: "c", Value: 5},
			finalHLV:    "5@c,4@b,1@a;3@d,2@e",
		},
		{
			name:        "incoming pv",
			existingHLV: "1@a",
			incomingHLV: "2@b;4@d,3@e",
			newCV:       Version{SourceID: "c", Value: 5},
			finalHLV:    "5@c,2@b,1@a;4@d,3@e",
		},
		{
			name:        "both mv",
			existingHLV: "1@a,3@d,4@e",
			incomingHLV: "6@b,5@f,2@g",
			newCV:       Version{SourceID: "c", Value: 7},
			finalHLV:    "7@c,6@b,1@a;5@f,4@e,3@d,2@g",
		},
		{
			name:        "both pv",
			existingHLV: "1@a;3@d,4@e",
			incomingHLV: "2@b;6@f,5@g",
			newCV:       Version{SourceID: "c", Value: 7},
			finalHLV:    "7@c,2@b,1@a;6@f,5@g,4@e,3@d",
		},
		{
			name:        "existing mv and incoming pv",
			existingHLV: "1@a,3@d,4@e",
			incomingHLV: "2@b;6@f,5@g",
			newCV:       Version{SourceID: "c", Value: 7},
			finalHLV:    "7@c,2@b,1@a;6@f,5@g,4@e,3@d",
		},
		{
			name:        "existing pv and incoming mv",
			existingHLV: "1@a;3@d,4@e",
			incomingHLV: "6@b,5@f,2@g",
			newCV:       Version{SourceID: "c", Value: 7},
			finalHLV:    "7@c,6@b,1@a;5@f,4@e,3@d,2@g",
		},
		{
			name:        "existing mv,pv, incoming mv",
			existingHLV: "1@a,3@d,4@e;8@h,7@g",
			incomingHLV: "6@b,5@f,2@c",
			newCV:       Version{SourceID: "i", Value: 9},
			finalHLV:    "9@i,6@b,1@a;8@h,7@g,5@f,4@e,3@d,2@c",
		},
		{
			name:        "existing mv,pv, incoming pv",
			existingHLV: "1@a,3@d,4@e;8@h,7@g",
			incomingHLV: "6@b;5@f,2@c",
			newCV:       Version{SourceID: "i", Value: 9},
			finalHLV:    "9@i,6@b,1@a;8@h,7@g,5@f,4@e,3@d,2@c",
		},
		{
			name:        "existing mv,pv, incoming mv,pv",
			existingHLV: "1@a,3@d,4@e;8@h,7@g",
			incomingHLV: "6@b,5@f,2@c;9@i,10@j",
			newCV:       Version{SourceID: "k", Value: 11},
			// note newCV is b@k because SourceID is always encoded
			finalHLV: "b@k,6@b,1@a;10@j,9@i,8@h,7@g,5@f,4@e,3@d,2@c",
		},
		{
			name:        "existing mv duplicates value with existing cv",
			existingHLV: "3@a,2@b,1@a",
			incomingHLV: "4@d",
			newCV:       Version{SourceID: "e", Value: 5},
			finalHLV:    "5@e,4@d,3@a;2@b",
		},
		{
			name:        "incoming mv duplicates value with incoming cv",
			existingHLV: "1@a",
			incomingHLV: "4@c,3@b,2@c",
			newCV:       Version{SourceID: "d", Value: 5},
			finalHLV:    "5@d,4@c,1@a;3@b",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			localHLV := createHLVForTest(t, test.existingHLV)
			incomingHLV := createHLVForTest(t, test.incomingHLV)
			expectedHLV := createHLVForTest(t, test.finalHLV)

			require.Equal(t, HLVConflict, IsInConflict(t.Context(), localHLV, incomingHLV))

			require.NoError(t, localHLV.MergeWithIncomingHLV(test.newCV, incomingHLV))
			require.True(t, localHLV.Equal(expectedHLV), "Expected HLV %s, actual HLV %s", test.finalHLV, hlvAsBlipString(t, localHLV))
		})
	}
}

func TestAddNewerVersionsTMP(t *testing.T) {
	testCases := []struct {
		name          string
		newHLV        string // new hlv init in step 1 of local/remote wins
		nonWinningHLV string // is local/remote hlv
		expectedHLV   string
	}{
		{
			name:          "MV has entry higher than non winning HLV",
			newHLV:        "10@abc,8@def,9@efg;1@foo",
			nonWinningHLV: "10@abc,8@def,2@efg;1@foo,2@bar",
			expectedHLV:   "10@abc,8@def,9@efg;1@foo,2@bar",
		},
		{
			name:          "winning MV has entry lower than incoming HLV",
			newHLV:        "20@abc,2@def,3@efg;1@foo",
			nonWinningHLV: "20@abc,10@def,9@efg;2@foo,4@bar",
			expectedHLV:   "20@abc;10@def,9@efg,2@foo,4@bar",
		},
		{
			name:          "No common elems in MV",
			newHLV:        "20@abc,10@def,11@efg;5@foo",
			nonWinningHLV: "20@abc,9@xyz,8@lmn;2@foo,4@bar",
			expectedHLV:   "20@abc,10@def,11@efg;9@xyz,8@lmn,5@foo,4@bar",
		},
		{
			name:          "newHLV had MV and non winning HLV does not",
			newHLV:        "20@abc,10@def,11@efg;5@foo",
			nonWinningHLV: "20@abc;9@xyz,8@lmn,2@foo,4@bar",
			expectedHLV:   "20@abc,10@def,11@efg;9@xyz,8@lmn,5@foo,4@bar",
		},
		{
			name:          "non winning HLV has MV and newHLV does not",
			newHLV:        "20@abc;9@xyz,8@lmn,2@foo,4@bar",
			nonWinningHLV: "20@abc,10@def,11@efg;5@foo",
			expectedHLV:   "20@abc;10@def,11@efg,9@xyz,8@lmn,5@foo,4@bar",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			localHLV, _, err := extractHLVFromBlipString(tc.newHLV)
			require.NoError(t, err)
			incomingHLV, _, err := extractHLVFromBlipString(tc.nonWinningHLV)
			require.NoError(t, err)

			localHLV.addNewerVersionTmp(incomingHLV)

			expectedHLV, _, err := extractHLVFromBlipString(tc.expectedHLV)
			require.NoError(t, err)

			require.Equal(t, expectedHLV, localHLV)
		})
	}
}

func TestLocalWinsConflictResolutionForHLV(t *testing.T) {
	testCases := []struct {
		name        string
		localHLV    string
		remoteHLV   string
		expectedHLV string
	}{
		{
			name:        "local doc has MV remote does not",
			localHLV:    "20@abc,10@def,11@efg;5@foo",
			remoteHLV:   "10@xyz;8@foo,9@bar",
			expectedHLV: "20@abc,10@def,11@efg;10@xyz,8@foo,9@bar",
		},
		{
			name:        "local doc has no MV and remote does",
			localHLV:    "20@abc;5@foo",
			remoteHLV:   "11@xyz,10@def,11@efg;7@foo",
			expectedHLV: "20@abc;10@def,11@xyz,11@efg,7@foo",
		},
		{
			name:        "local hlv has MV entry less than remote hlv",
			localHLV:    "10@abc,1@def,3@efg;1@foo",
			remoteHLV:   "2@xyz,8@def,9@efg;1@foo",
			expectedHLV: "10@abc;8@def,9@efg,1@foo,2@xyz",
		},
		{
			name:        "local hlv has MV entry greater than remote hlv",
			localHLV:    "10@abc,10@def,11@efg;1@foo",
			remoteHLV:   "2@xyz,8@def,9@efg;1@bar",
			expectedHLV: "10@abc,10@def,11@efg;1@bar,1@foo,2@xyz",
		},
		{
			name:        "both local and remote have mv and no common sources",
			localHLV:    "10@abc,10@def,11@efg;1@foo",
			remoteHLV:   "2@xyz,8@lmn,9@pqr;1@bar",
			expectedHLV: "10@abc,10@def,11@efg;2@xyz,8@lmn,9@pqr,1@bar,1@foo",
		},
		{
			name:        "local hlv has cv less than remote hlv",
			localHLV:    "10@abc;1@foo",
			remoteHLV:   "20@xyz;2@bar",
			expectedHLV: "FFFFFFFFFFFFFFFF@testSource,10@abc,20@xyz;1@foo,2@bar",
		},
		{
			name:        "local hlv has cv greater than remote hlv",
			localHLV:    "20@abc;1@foo",
			remoteHLV:   "10@xyz;2@bar",
			expectedHLV: "20@abc;1@foo,10@xyz,2@bar",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			localHLV, _, err := extractHLVFromBlipString(tc.localHLV)
			require.NoError(t, err)
			remoteHLV, _, err := extractHLVFromBlipString(tc.remoteHLV)
			require.NoError(t, err)

			localWinsHLV, err := localWinsConflictResolutionForHLV(t.Context(), localHLV, remoteHLV, "testDoc", "testSource")
			require.NoError(t, err)

			expectedHLV, _, err := extractHLVFromBlipString(tc.expectedHLV)
			require.NoError(t, err)
			if expectedHLV.Version == math.MaxUint64 {
				expectedHLV.CurrentVersionCAS = math.MaxUint64
			}

			require.Equal(t, expectedHLV, localWinsHLV)
		})
	}
}

func TestRemoteWinsConflictResolutionForHLV(t *testing.T) {
	testCases := []struct {
		name        string
		localHLV    string
		remoteHLV   string
		expectedHLV string
	}{
		{
			name:        "remote hlv has mv and local does not",
			localHLV:    "10@abc;1@foo",
			remoteHLV:   "20@xyz,10@def,11@efg;5@bar",
			expectedHLV: "20@xyz,10@def,11@efg;1@foo,5@bar,10@abc",
		},
		{
			name:        "remote hlv has no mv and local does",
			localHLV:    "20@abc,10@def,11@efg;5@foo",
			remoteHLV:   "10@xyz;8@bar,9@baz",
			expectedHLV: "10@xyz;10@def,11@efg,8@bar,9@baz,5@foo,20@abc",
		},
		{
			name:        "remote hlv has mv entry less than local hlv",
			localHLV:    "10@abc,8@def,9@efg;1@foo",
			remoteHLV:   "2@xyz,1@def,3@efg;1@bar",
			expectedHLV: "2@xyz;8@def,9@efg,1@foo,1@bar,10@abc",
		},
		{
			name:        "remote hlv has mv entry greater than local hlv",
			localHLV:    "10@abc,4@def,5@efg;1@foo",
			remoteHLV:   "2@xyz,8@def,9@efg;1@bar,2@foo",
			expectedHLV: "2@xyz,8@def,9@efg;10@abc,1@bar,2@foo",
		},
		{
			name:        "remote hlv has mv and no common sources with local",
			localHLV:    "10@abc,10@def,11@efg;1@foo",
			remoteHLV:   "2@xyz,8@lmn,9@pqr;1@bar",
			expectedHLV: "2@xyz,8@lmn,9@pqr;10@abc,10@def,11@efg,1@bar,1@foo",
		},
		{
			name:        "remote and local have no mv",
			localHLV:    "10@abc;1@foo",
			remoteHLV:   "20@xyz;2@bar",
			expectedHLV: "20@xyz;1@foo,2@bar,10@abc",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			localHLV, _, err := extractHLVFromBlipString(tc.localHLV)
			require.NoError(t, err)
			remoteHLV, _, err := extractHLVFromBlipString(tc.remoteHLV)
			require.NoError(t, err)

			remoteWinsHLV, err := remoteWinsConflictResolutionForHLV(t.Context(), "testDoc", localHLV, remoteHLV)
			require.NoError(t, err)

			expectedHLV, _, err := extractHLVFromBlipString(tc.expectedHLV)
			require.NoError(t, err)

			require.Equal(t, expectedHLV, remoteWinsHLV)
		})
	}

}
