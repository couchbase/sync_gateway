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
	"reflect"
	"strconv"
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternalHLVFunctions:
//   - Tests internal api methods on the HLV work as expected
//   - Tests methods GetCurrentVersion, AddVersion and Remove
func TestInternalHLVFunctions(t *testing.T) {
	pv := make(map[string]string)
	currSourceId := EncodeSource("5pRi8Piv1yLcLJ1iVNJIsA")
	currVersion := EncodeValue(12345678)
	pv[EncodeSource("YZvBpEaztom9z5V/hDoeIw")] = EncodeValue(64463204720)

	inputHLV := []string{"5pRi8Piv1yLcLJ1iVNJIsA@12345678", "YZvBpEaztom9z5V/hDoeIw@64463204720", "m_NqiIe0LekFPLeX4JvTO6Iw@345454"}
	hlv := createHLVForTest(t, inputHLV)

	newCAS := EncodeValue(123456789)
	const newSource = "s_testsource"

	// create a new version vector entry that will error method AddVersion
	badNewVector := Version{
		Value:    EncodeValue(123345),
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
	assert.True(t, reflect.DeepEqual(hlv.PreviousVersions, pv))

	// remove garbage sourceID from PV and assert we get error
	require.Error(t, hlv.Remove("testing"))
	// Remove a sourceID CAS pair from previous versions section of the HLV structure (for compaction)
	require.NoError(t, hlv.Remove(currSourceId))
	delete(pv, currSourceId)
	assert.True(t, reflect.DeepEqual(hlv.PreviousVersions, pv))
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

// TestConflictEqualHLV:
//   - Creates two 'equal' HLV's and asserts we identify them as equal
//   - Then tests other code path in event source ID differs and current CAS differs but with identical merge versions
//     that we identify they are in fact 'equal'
//   - Then test the same but for previous versions
func TestConflictEqualHLV(t *testing.T) {
	// two vectors with the same sourceID and version pair as the current vector
	inputHLVA := []string{"cluster1@10", "cluster2@3"}
	inputHLVB := []string{"cluster1@10", "cluster2@4"}
	hlvA := createHLVForTest(t, inputHLVA)
	hlvB := createHLVForTest(t, inputHLVB)
	decHLVA := hlvA.ToDecodedHybridLogicalVector()
	decHLVB := hlvB.ToDecodedHybridLogicalVector()
	require.True(t, decHLVA.isEqual(decHLVB))

	// test conflict detection with different version CAS but same merge versions
	inputHLVA = []string{"cluster2@12", "cluster3@3", "cluster4@2"}
	inputHLVB = []string{"cluster1@10", "cluster3@3", "cluster4@2"}
	hlvA = createHLVForTest(t, inputHLVA)
	hlvB = createHLVForTest(t, inputHLVB)
	decHLVA = hlvA.ToDecodedHybridLogicalVector()
	decHLVB = hlvB.ToDecodedHybridLogicalVector()
	require.True(t, decHLVA.isEqual(decHLVB))

	// test conflict detection with different version CAS but same previous version vectors
	inputHLVA = []string{"cluster3@2", "cluster1@3", "cluster2@5"}
	hlvA = createHLVForTest(t, inputHLVA)
	inputHLVB = []string{"cluster4@7", "cluster1@3", "cluster2@5"}
	hlvB = createHLVForTest(t, inputHLVB)
	decHLVA = hlvA.ToDecodedHybridLogicalVector()
	decHLVB = hlvB.ToDecodedHybridLogicalVector()
	require.True(t, decHLVA.isEqual(decHLVB))

	cluster1Encoded := base64.StdEncoding.EncodeToString([]byte("cluster1"))
	// remove an entry from one of the HLV PVs to assert we get false returned from isEqual
	require.NoError(t, hlvA.Remove(cluster1Encoded))
	decHLVA = hlvA.ToDecodedHybridLogicalVector()
	require.False(t, decHLVA.isEqual(decHLVB))
}

// createHLVForTest is a helper function to create a HLV for use in a test. Takes a list of strings in the format of <sourceID@version> and assumes
// first entry is current version. For merge version entries you must specify 'm_' as a prefix to sourceID NOTE: it also sets cvCAS to the current version
func createHLVForTest(tb *testing.T, inputList []string) HybridLogicalVector {
	hlvOutput := NewHybridLogicalVector()

	// first element will be current version and source pair
	currentVersionPair := strings.Split(inputList[0], "@")
	hlvOutput.SourceID = base64.StdEncoding.EncodeToString([]byte(currentVersionPair[0]))
	value, err := strconv.ParseUint(currentVersionPair[1], 10, 64)
	require.NoError(tb, err)
	vrsEncoded := EncodeValue(value)
	hlvOutput.Version = vrsEncoded
	hlvOutput.CurrentVersionCAS = vrsEncoded

	// remove current version entry in list now we have parsed it into the HLV
	inputList = inputList[1:]

	for _, version := range inputList {
		currentVersionPair = strings.Split(version, "@")
		value, err = strconv.ParseUint(currentVersionPair[1], 10, 64)
		require.NoError(tb, err)
		if strings.HasPrefix(currentVersionPair[0], "m_") {
			// add entry to merge version removing the leading prefix for sourceID
			hlvOutput.MergeVersions[EncodeSource(currentVersionPair[0][2:])] = EncodeValue(value)
		} else {
			// if it's not got the prefix we assume it's a previous version entry
			hlvOutput.PreviousVersions[EncodeSource(currentVersionPair[0])] = EncodeValue(value)
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

	collection := GetSingleDatabaseCollectionWithUser(t, db)
	localSource := db.EncodedBucketUUID

	// 1. Test standard import of an SDK write
	standardImportKey := "standardImport_" + t.Name()
	standardImportBody := []byte(`{"prop":"value"}`)
	cas, err := collection.dataStore.WriteCas(standardImportKey, 0, 0, standardImportBody, sgbucket.Raw)
	require.NoError(t, err, "write error")
	_, err = collection.ImportDocRaw(ctx, standardImportKey, standardImportBody, nil, false, cas, nil, ImportFromFeed)
	require.NoError(t, err, "import error")

	importedDoc, _, err := collection.GetDocWithXattrs(ctx, standardImportKey, DocUnmarshalAll)
	require.NoError(t, err)
	importedHLV := importedDoc.HLV
	encodedCAS := string(base.Uint64CASToLittleEndianHex(cas))
	require.Equal(t, encodedCAS, importedHLV.ImportCAS)
	require.Equal(t, importedDoc.SyncData.Cas, importedHLV.CurrentVersionCAS)
	require.Equal(t, importedDoc.SyncData.Cas, importedHLV.Version)
	require.Equal(t, localSource, importedHLV.SourceID)

	// 2. Test import of write by HLV-aware peer (HLV is already updated, sync metadata is not).
	otherSource := "otherSource"
	hlvHelper := NewHLVAgent(t, collection.dataStore, otherSource, "_vv")
	existingHLVKey := "existingHLV_" + t.Name()
	_ = hlvHelper.InsertWithHLV(ctx, existingHLVKey)

	existingBody, existingXattrs, cas, err := collection.dataStore.GetWithXattrs(ctx, existingHLVKey, []string{base.SyncXattrName, base.VvXattrName})
	require.NoError(t, err)
	encodedCAS = EncodeValue(cas)

	_, err = collection.ImportDocRaw(ctx, existingHLVKey, existingBody, existingXattrs, false, cas, nil, ImportFromFeed)
	require.NoError(t, err, "import error")

	importedDoc, _, err = collection.GetDocWithXattrs(ctx, existingHLVKey, DocUnmarshalAll)
	require.NoError(t, err)
	importedHLV = importedDoc.HLV
	// cas in the HLV's current version and cvCAS should not have changed, and should match importCAS
	require.Equal(t, encodedCAS, importedHLV.ImportCAS)
	require.Equal(t, encodedCAS, importedHLV.CurrentVersionCAS)
	require.Equal(t, encodedCAS, importedHLV.Version)
	require.Equal(t, hlvHelper.Source, importedHLV.SourceID)
}

// TestHLVMapToCBLString:
//   - Purpose is to test the ability to extract from HLV maps in CBL replication format
//   - Three test cases, both MV and PV defined, only PV defined and only MV defined
//   - To protect against flake added some splitting of the result string in test case 1 as we cannot guarantee the
//     order the string will be made in given map iteration is random
func TestHLVMapToCBLString(t *testing.T) {

	testCases := []struct {
		name        string
		inputHLV    []string
		expectedStr string
		both        bool
	}{
		{
			name: "Both PV and mv",
			inputHLV: []string{"cb06dc003846116d9b66d2ab23887a96@123456", "YZvBpEaztom9z5V/hDoeIw@1628620455135215600", "m_NqiIe0LekFPLeX4JvTO6Iw@1628620455139868700",
				"m_LhRPsa7CpjEvP5zeXTXEBA@1628620455147864000"},
			expectedStr: "0x1c008cd6ac059a16@TnFpSWUwTGVrRlBMZVg0SnZUTzZJdw==,0xc0ff05d7ac059a16@TGhSUHNhN0NwakV2UDV6ZVhUWEVCQQ==;0xf0ff44d6ac059a16@WVp2QnBFYXp0b205ejVWL2hEb2VJdw==",
			both:        true,
		},
		{
			name:        "Just PV",
			inputHLV:    []string{"cb06dc003846116d9b66d2ab23887a96@123456", "YZvBpEaztom9z5V/hDoeIw@1628620455135215600"},
			expectedStr: "0xf0ff44d6ac059a16@WVp2QnBFYXp0b205ejVWL2hEb2VJdw==",
		},
		{
			name:        "Just MV",
			inputHLV:    []string{"cb06dc003846116d9b66d2ab23887a96@123456", "m_NqiIe0LekFPLeX4JvTO6Iw@1628620455139868700"},
			expectedStr: "0x1c008cd6ac059a16@TnFpSWUwTGVrRlBMZVg0SnZUTzZJdw==",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			hlv := createHLVForTest(t, test.inputHLV)
			historyStr := hlv.ToHistoryForHLV()

			if test.both {
				initial := strings.Split(historyStr, ";")
				mvSide := strings.Split(initial[0], ",")
				assert.Contains(t, test.expectedStr, initial[1])
				for _, v := range mvSide {
					assert.Contains(t, test.expectedStr, v)
				}
			} else {
				assert.Equal(t, test.expectedStr, historyStr)
			}
		})
	}
}

// TestInvalidHLVOverChangesMessage:
//   - Test hlv string that has too many sections to it (parts delimited by ;)
//   - Test hlv string that is empty
//   - Assert that extractHLVFromBlipMessage will return error in both cases
func TestInvalidHLVInBlipMessageForm(t *testing.T) {
	hlvStr := "25@def; 22@def,21@eff; 20@abc,18@hij; 222@hiowdwdew, 5555@dhsajidfgd"

	hlv, err := extractHLVFromBlipMessage(hlvStr)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid hlv in changes message received")
	assert.Equal(t, HybridLogicalVector{}, hlv)

	hlvStr = ""
	hlv, err = extractHLVFromBlipMessage(hlvStr)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid hlv in changes message received")
	assert.Equal(t, HybridLogicalVector{}, hlv)
}

var extractHLVFromBlipMsgBMarkCases = []struct {
	name             string
	hlvString        string
	expectedHLV      []string
	mergeVersions    bool
	previousVersions bool
}{
	{
		name:             "mv and pv, leading spaces",                                                                                                                                                         // with spaces
		hlvString:        "25@def; 22@def, 21@eff, 500@x, 501@xx, 4000@xxx, 700@y, 701@yy, 702@yyy; 20@abc, 18@hij, 3@x, 4@xx, 5@xxx, 6@xxxx, 7@xxxxx, 3@y, 4@yy, 5@yyy, 6@yyyy, 7@yyyyy, 2@xy, 3@xyy, 4@xxy", // 15 pv 8 mv
		expectedHLV:      []string{"def@25", "abc@20", "hij@18", "x@3", "xx@4", "xxx@5", "xxxx@6", "xxxxx@7", "y@3", "yy@4", "yyy@5", "yyyy@6", "yyyyy@7", "xy@2", "xyy@3", "xxy@4", "m_def@22", "m_eff@21", "m_x@500", "m_xx@501", "m_xxx@4000", "m_y@700", "m_yy@701", "m_yyy@702"},
		previousVersions: true,
		mergeVersions:    true,
	},
	{
		name:             "mv and pv, no spaces",                                                                                                                                       // without spaces
		hlvString:        "25@def;22@def,21@eff,500@x,501@xx,4000@xxx,700@y,701@yy,702@yyy;20@abc,18@hij,3@x,4@xx,5@xxx,6@xxxx,7@xxxxx,3@y,4@yy,5@yyy,6@yyyy,7@yyyyy,2@xy,3@xyy,4@xxy", // 15 pv 8 mv
		expectedHLV:      []string{"def@25", "abc@20", "hij@18", "x@3", "xx@4", "xxx@5", "xxxx@6", "xxxxx@7", "y@3", "yy@4", "yyy@5", "yyyy@6", "yyyyy@7", "xy@2", "xyy@3", "xxy@4", "m_def@22", "m_eff@21", "m_x@500", "m_xx@501", "m_xxx@4000", "m_y@700", "m_yy@701", "m_yyy@702"},
		previousVersions: true,
		mergeVersions:    true,
	},
	{
		name:             "pv only",
		hlvString:        "25@def; 20@abc,18@hij",
		expectedHLV:      []string{"def@25", "abc@20", "hij@18"},
		previousVersions: true,
	},
	{
		name:             "mv and pv, mixed spacing",
		hlvString:        "25@def; 22@def,21@eff; 20@abc,18@hij,3@x,4@xx,5@xxx,6@xxxx,7@xxxxx,3@y,4@yy,5@yyy,6@yyyy,7@yyyyy,2@xy,3@xyy,4@xxy", // 15
		expectedHLV:      []string{"def@25", "abc@20", "hij@18", "x@3", "xx@4", "xxx@5", "xxxx@6", "xxxxx@7", "y@3", "yy@4", "yyy@5", "yyyy@6", "yyyyy@7", "xy@2", "xyy@3", "xxy@4", "m_def@22", "m_eff@21"},
		mergeVersions:    true,
		previousVersions: true,
	},
	{
		name:        "cv only",
		hlvString:   "24@def",
		expectedHLV: []string{"def@24"},
	},
	{
		name:             "cv and mv,base64 encoded",
		hlvString:        "1@Hell0CA; 1@1Hr0k43xS662TToxODDAxQ",
		expectedHLV:      []string{"Hell0CA@1", "1Hr0k43xS662TToxODDAxQ@1"},
		previousVersions: true,
	},
	{
		name:             "cv and mv - small",
		hlvString:        "25@def; 22@def,21@eff; 20@abc,18@hij",
		expectedHLV:      []string{"def@25", "abc@20", "hij@18", "m_def@22", "m_eff@21"},
		mergeVersions:    true,
		previousVersions: true,
	},
}

// TestExtractHLVFromChangesMessage:
//   - Test case 1: CV entry and 1 PV entry
//   - Test case 2: CV entry and 2 PV entries
//   - Test case 3: CV entry, 2 MV entries and 2 PV entries
//   - Test case 4: just CV entry
//   - Each test case gets run through extractHLVFromBlipMessage and assert that the resulting HLV
//     is correct to what is expected
func TestExtractHLVFromChangesMessage(t *testing.T) {
	for _, test := range extractHLVFromBlipMsgBMarkCases {
		t.Run(test.name, func(t *testing.T) {
			expectedVector := createHLVForTest(t, test.expectedHLV)

			// TODO: When CBG-3662 is done, should be able to simplify base64 handling to treat source as a string
			//       that may represent a base64 encoding
			base64EncodedHlvString := cblEncodeTestSources(test.hlvString)
			hlv, err := extractHLVFromBlipMessage(base64EncodedHlvString)
			require.NoError(t, err)

			assert.Equal(t, expectedVector.SourceID, hlv.SourceID)
			assert.Equal(t, expectedVector.Version, hlv.Version)
			if test.previousVersions {
				assert.True(t, reflect.DeepEqual(expectedVector.PreviousVersions, hlv.PreviousVersions))
			}
			if test.mergeVersions {
				assert.True(t, reflect.DeepEqual(expectedVector.MergeVersions, hlv.MergeVersions))
			}
		})
	}
}

func BenchmarkExtractHLVFromBlipMessage(b *testing.B) {
	for _, bm := range extractHLVFromBlipMsgBMarkCases {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = extractHLVFromBlipMessage(bm.hlvString)
			}
		})
	}
}

// cblEncodeTestSources converts the simplified versions in test data to CBL-style encoding
func cblEncodeTestSources(hlvString string) (base64HLVString string) {

	vectorFields := strings.Split(hlvString, ";")
	vectorLength := len(vectorFields)
	if vectorLength == 0 {
		return hlvString
	}

	// first vector field is single vector, cv
	base64HLVString += EncodeTestVersion(vectorFields[0])
	for _, field := range vectorFields[1:] {
		base64HLVString += ";"
		versions := strings.Split(field, ",")
		if len(versions) == 0 {
			continue
		}
		base64HLVString += EncodeTestVersion(versions[0])
		for _, version := range versions[1:] {
			base64HLVString += ","
			base64HLVString += EncodeTestVersion(version)
		}
	}
	return base64HLVString
}
