// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"encoding/base64"
	"math/rand/v2"
	"reflect"
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
	pv := make(HLVVersions)
	currSourceId := "5pRi8Piv1yLcLJ1iVNJIsA"
	currVersion := uint64(12345678)
	pvSourceID := "YZvBpEaztom9z5V/hDoeIw"
	pv[pvSourceID] = 64463204720

	hlv := HybridLogicalVector{
		SourceID: currSourceId,
		Version:  12345678,
		MergeVersions: map[string]uint64{
			"NqiIe0LekFPLeX4JvTO6Iw": 345454,
		},
		PreviousVersions: map[string]uint64{
			"YZvBpEaztom9z5V/hDoeIw": 64463204720,
		},
	}

	newCAS := uint64(123456789)
	const newSource = "s_testsource"

	// create a new version vector entry that will error method AddVersion
	badNewVector := Version{
		Value:    123345,
		SourceID: currSourceId,
	}
	// create a new version vector entry that should be added to HLV successfully
	newVersionVector := Version{
		Value:    newCAS,
		SourceID: currSourceId,
	}

	// Get current version vector, sourceID and CAS pair
	source, version := hlv.GetCurrentVersion()
	assert.Equal(t, currSourceId, source)
	assert.Equal(t, currVersion, version)

	// add new version vector with same sourceID as current sourceID and assert it doesn't add to previous versions then restore HLV to previous state
	require.NoError(t, hlv.AddVersion(newVersionVector))
	assert.Len(t, hlv.PreviousVersions, 1)
	hlv.Version = currVersion

	// attempt to add new version vector to HLV that has a CAS value less than the current CAS value
	require.Error(t, hlv.AddVersion(badNewVector))

	// add current version and sourceID of HLV to pv map for assertions
	pv[currSourceId] = currVersion
	// Add a new version vector pair to the HLV structure and assert that it moves the current version vector pair to the previous versions section
	newVersionVector.SourceID = newSource
	require.NoError(t, hlv.AddVersion(newVersionVector))
	assert.Equal(t, newCAS, hlv.Version)
	assert.Equal(t, newSource, hlv.SourceID)
	require.Equal(t, hlv.PreviousVersions, pv)

	// remove garbage sourceID from PV and assert we get error
	require.Error(t, hlv.Remove("testing"))
	// Remove a sourceID CAS pair from previous versions section of the HLV structure (for compaction)
	require.NoError(t, hlv.Remove(currSourceId))
	delete(pv, currSourceId)
	require.Equal(t, hlv.PreviousVersions, pv)
}

// TestConflictDetectionDominating:
//   - Tests cases where one HLV's is said to be 'dominating' over another
//   - Test case 1: where sourceID is the same between HLV's but HLV(A) has higher version CAS than HLV(B) thus A dominates
//   - Test case 2: where sourceID is different and HLV(A) sourceID is present in HLV(B) PV and HLV(A) has dominating version
//   - Test case 3: where sourceID is different and HLV(A) sourceID is present in HLV(B) MV and HLV(A) has dominating version
//   - Test case 4: where sourceID is test case 2 but flipped to show the code checks for dominating versions both sides
//   - Assert that all scenarios returns false from IsInConflict method, as we have a HLV that is dominating in each case
func TestConflictDetectionDominating(t *testing.T) {
	testCases := []struct {
		name           string
		inputListHLVA  []string
		inputListHLVB  []string
		expectedResult bool
	}{
		{
			name:           "Matching current source, newer version",
			inputListHLVA:  []string{"cluster1@20", "cluster2@2"},
			inputListHLVB:  []string{"cluster1@10", "cluster2@1"},
			expectedResult: true,
		}, {
			name:           "Matching current source and version",
			inputListHLVA:  []string{"cluster1@20", "cluster2@2"},
			inputListHLVB:  []string{"cluster1@20", "cluster2@1"},
			expectedResult: true,
		},
		{
			name:           "B CV found in A's PV",
			inputListHLVA:  []string{"cluster1@20", "cluster2@10"},
			inputListHLVB:  []string{"cluster2@10", "cluster1@15"},
			expectedResult: true,
		},
		{
			name:           "B CV older than A's PV for same source",
			inputListHLVA:  []string{"cluster1@20", "cluster2@10"},
			inputListHLVB:  []string{"cluster2@10", "cluster1@15"},
			expectedResult: true,
		},
		{
			name:           "Unique sources in A",
			inputListHLVA:  []string{"cluster1@20", "cluster2@15", "cluster3@3"},
			inputListHLVB:  []string{"cluster2@10", "cluster1@10"},
			expectedResult: true,
		},
		{
			name:           "Unique sources in B",
			inputListHLVA:  []string{"cluster1@20"},
			inputListHLVB:  []string{"cluster1@15", "cluster3@3"},
			expectedResult: true,
		},
		{
			name:           "B has newer cv",
			inputListHLVA:  []string{"cluster1@10"},
			inputListHLVB:  []string{"cluster1@15"},
			expectedResult: false,
		},
		{
			name:           "B has newer cv than A pv",
			inputListHLVA:  []string{"cluster2@20", "cluster1@10"},
			inputListHLVB:  []string{"cluster1@15", "cluster2@20"},
			expectedResult: false,
		},
		{
			name:           "B's cv not found in A",
			inputListHLVA:  []string{"cluster2@20", "cluster1@10"},
			inputListHLVB:  []string{"cluster3@5"},
			expectedResult: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			hlvA := createHLVForTest(t, testCase.inputListHLVA)
			hlvB := createHLVForTest(t, testCase.inputListHLVB)
			require.True(t, hlvA.isDominating(hlvB) == testCase.expectedResult)

		})
	}
}

// createHLVForTest is a helper function to create a HLV for use in a test. Takes a list of strings in the format of <sourceID@version> and assumes
// first entry is current version. For merge version entries you must specify 'm_' as a prefix to sourceID NOTE: it also sets cvCAS to the current version
func createHLVForTest(tb *testing.T, inputList []string) *HybridLogicalVector {
	hlvOutput := NewHybridLogicalVector()

	// first element will be current version and source pair
	currentVersionPair := strings.Split(inputList[0], "@")
	hlvOutput.SourceID = base64.StdEncoding.EncodeToString([]byte(currentVersionPair[0]))
	value, err := strconv.ParseUint(currentVersionPair[1], 10, 64)
	require.NoError(tb, err)
	hlvOutput.Version = value
	hlvOutput.CurrentVersionCAS = value

	// remove current version entry in list now we have parsed it into the HLV
	inputList = inputList[1:]

	for _, version := range inputList {
		currentVersionPair = strings.Split(version, "@")
		value, err = strconv.ParseUint(currentVersionPair[1], 10, 64)
		require.NoError(tb, err)
		if strings.HasPrefix(currentVersionPair[0], "m_") {
			// add entry to merge version removing the leading prefix for sourceID
			hlvOutput.MergeVersions[EncodeSource(currentVersionPair[0][2:])] = value
		} else {
			// if it's not got the prefix we assume it's a previous version entry
			hlvOutput.PreviousVersions[EncodeSource(currentVersionPair[0])] = value
		}
	}
	return hlvOutput
}

func TestAddNewerVersionsBetweenTwoVectorsWhenNotInConflict(t *testing.T) {
	testCases := []struct {
		name          string
		localInput    []string
		incomingInput []string
		expected      []string
	}{
		{
			name:          "testcase1",
			localInput:    []string{"abc@15"},
			incomingInput: []string{"def@25", "abc@20"},
			expected:      []string{"def@25", "abc@20"},
		},
		{
			name:          "testcase2",
			localInput:    []string{"abc@15", "def@30"},
			incomingInput: []string{"def@35", "abc@15"},
			expected:      []string{"def@35", "abc@15"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			localHLV := createHLVForTest(t, test.localInput)
			incomingHLV := createHLVForTest(t, test.incomingInput)
			expectedHLV := createHLVForTest(t, test.expected)

			_ = localHLV.AddNewerVersions(incomingHLV)
			// assert on expected values
			assert.Equal(t, expectedHLV.SourceID, localHLV.SourceID)
			assert.Equal(t, expectedHLV.Version, localHLV.Version)
			assert.True(t, reflect.DeepEqual(expectedHLV.PreviousVersions, localHLV.PreviousVersions))
		})
	}
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
			name: "Just PV",
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
			name: "Just MV",
			hlv: HybridLogicalVector{
				MergeVersions: map[string]uint64{
					"a": 1,
				},
			},
			possibleOutput: []string{
				"1@a;",
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
			hlv, legacyRevs, err := ExtractHLVFromBlipMessage(testCase.hlv)
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
//   - Each test case gets run through ExtractHLVFromBlipMessage and assert that the resulting HLV
//     is correct to what is expected
func TestExtractHLVFromChangesMessage(t *testing.T) {
	for _, test := range getHLVTestCases(t) {
		t.Run(test.name, func(t *testing.T) {
			hlv, legacyRevs, err := ExtractHLVFromBlipMessage(test.hlvString)
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
				_, _, _ = ExtractHLVFromBlipMessage(bm.hlvString)
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
	inputHLVA := []string{"cluster3@2"}
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

func TestHLVUpdateMV(t *testing.T) {
	testCases := []struct {
		name        string
		existingHLV HybridLogicalVector
		incomingHLV HybridLogicalVector
		finalHLV    HybridLogicalVector
	}{
		{
			name:        "Add new MV",
			existingHLV: HybridLogicalVector{},
			incomingHLV: HybridLogicalVector{
				MergeVersions: map[string]uint64{
					"a": 1,
				},
			},
			// does not update with incoming versions?
			finalHLV: HybridLogicalVector{},
		},
		{
			name: "existing mv, move to pv",
			existingHLV: HybridLogicalVector{
				Version:  3,
				SourceID: "c",
				MergeVersions: map[string]uint64{
					"a": 1,
					"b": 2,
				},
			},
			incomingHLV: HybridLogicalVector{
				Version:  4,
				SourceID: "c",
			},
			// does not update with incoming versions?
			finalHLV: HybridLogicalVector{
				Version:  4,
				SourceID: "c",
				PreviousVersions: map[string]uint64{
					"a": 1,
					"b": 2,
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			inputHLV := testCase.existingHLV // note this is destructive to input
			require.NoError(t, inputHLV.AddNewerVersions(&testCase.incomingHLV))
			require.Equal(t, testCase.finalHLV, inputHLV)
		})
	}
}
