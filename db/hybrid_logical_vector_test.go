// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternalHLVFunctions:
//   - Tests internal api methods on the HLV work as expected
//   - Tests methods GetCurrentVersion, AddVersion and Remove
func TestInternalHLVFunctions(t *testing.T) {
	pv := make(map[string]uint64)
	currSourceId := "s_5pRi8Piv1yLcLJ1iVNJIsA"
	const currVersion = 12345678
	pv["s_YZvBpEaztom9z5V/hDoeIw"] = 64463204720

	inputHLV := []string{"s_5pRi8Piv1yLcLJ1iVNJIsA@12345678", "s_YZvBpEaztom9z5V/hDoeIw@64463204720", "m_s_NqiIe0LekFPLeX4JvTO6Iw@345454"}
	hlv := createHLVForTest(t, inputHLV)

	const newCAS = 123456789
	const newSource = "s_testsource"

	// create a new version vector entry that will error method AddVersion
	badNewVector := CurrentVersionVector{
		VersionCAS: 123345,
		SourceID:   currSourceId,
	}
	// create a new version vector entry that should be added to HLV successfully
	newVersionVector := CurrentVersionVector{
		VersionCAS: newCAS,
		SourceID:   currSourceId,
	}

	// Get current version vector, sourceID and CAS pair
	source, version := hlv.GetCurrentVersion()
	assert.Equal(t, currSourceId, source)
	assert.Equal(t, uint64(currVersion), version)

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
	assert.Equal(t, uint64(newCAS), hlv.Version)
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
//   - Tests two cases where one HLV's is said to be 'dominating' over another and thus not in conflict
//   - Test case 1: where sourceID is the same between HLV's but HLV(A) has higher version CAS than HLV(B) thus A dominates
//   - Test case 2: where sourceID is different and HLV(A) sourceID is present in HLV(B) PV and HLV(A) has dominating version
//   - Test case 3: where sourceID is different and HLV(A) sourceID is present in HLV(B) MV and HLV(A) has dominating version
//   - Test case 4: where sourceID is test case 2 but flipped to show the code checks for dominating versions both sides
//   - Assert that all scenarios returns false from IsInConflict method, as we have a HLV that is dominating in each case
func TestConflictDetectionDominating(t *testing.T) {
	testCases := []struct {
		name          string
		inputListHLVA []string
		inputListHLVB []string
	}{
		{
			name:          "Test case 1",
			inputListHLVA: []string{"cluster1@20", "cluster2@2"},
			inputListHLVB: []string{"cluster1@10", "cluster2@1"},
		},
		{
			name:          "Test case 2",
			inputListHLVA: []string{"cluster1@20", "cluster3@3"},
			inputListHLVB: []string{"cluster2@10", "cluster1@15"},
		},
		{
			name:          "Test case 3",
			inputListHLVA: []string{"cluster1@20", "cluster3@3"},
			inputListHLVB: []string{"cluster2@10", "m_cluster1@12", "m_cluster2@11"},
		},
		{
			name:          "Test case 4",
			inputListHLVA: []string{"cluster2@10", "cluster1@15"},

			inputListHLVB: []string{"cluster1@20", "cluster3@3"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			hlvA := createHLVForTest(t, testCase.inputListHLVA)
			hlvB := createHLVForTest(t, testCase.inputListHLVB)
			require.False(t, hlvA.IsInConflict(hlvB))
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
	require.True(t, hlvA.isEqual(hlvB))

	// test conflict detection with different version CAS but same merge versions
	inputHLVA = []string{"cluster2@12", "cluster3@3", "cluster4@2"}
	inputHLVB = []string{"cluster1@10", "cluster3@3", "cluster4@2"}
	hlvA = createHLVForTest(t, inputHLVA)
	hlvB = createHLVForTest(t, inputHLVB)
	require.True(t, hlvA.isEqual(hlvB))

	// test conflict detection with different version CAS but same previous version vectors
	inputHLVA = []string{"cluster3@2", "cluster1@3", "cluster2@5"}
	hlvA = createHLVForTest(t, inputHLVA)
	inputHLVB = []string{"cluster4@7", "cluster1@3", "cluster2@5"}
	hlvB = createHLVForTest(t, inputHLVB)
	require.True(t, hlvA.isEqual(hlvB))

	// remove an entry from one of the HLV PVs to assert we get false returned from isEqual
	require.NoError(t, hlvA.Remove("cluster1"))
	require.False(t, hlvA.isEqual(hlvB))
}

// TestConflictExample:
//   - Takes example conflict scenario from PRD to see if we correctly identify conflict in that scenario
//   - Creates two HLV's similar to ones in example and calls IsInConflict to assert it returns true
func TestConflictExample(t *testing.T) {
	input := []string{"cluster1@11", "cluster3@2", "cluster2@4"}
	inMemoryHLV := createHLVForTest(t, input)

	input = []string{"cluster2@2", "cluster3@3"}
	otherVector := createHLVForTest(t, input)
	require.True(t, inMemoryHLV.IsInConflict(otherVector))
}

// createHLVForTest is a helper function to create a HLV for use in a test. Takes a list of strings in the format of <sourceID@version> and assumes
// first entry is current version. For merge version entries you must specify 'm_' as a prefix to sourceID NOTE: it also sets cvCAS to the current version
func createHLVForTest(tb *testing.T, inputList []string) HybridLogicalVector {
	hlvOutput := NewHybridLogicalVector()

	// first element will be current version and source pair
	currentVersionPair := strings.Split(inputList[0], "@")
	hlvOutput.SourceID = currentVersionPair[0]
	version, err := strconv.Atoi(currentVersionPair[1])
	require.NoError(tb, err)
	hlvOutput.Version = uint64(version)
	hlvOutput.CurrentVersionCAS = uint64(version)

	// remove current version entry in list now we have parsed it into the HLV
	inputList = inputList[1:]

	for _, value := range inputList {
		currentVersionPair = strings.Split(value, "@")
		version, err = strconv.Atoi(currentVersionPair[1])
		require.NoError(tb, err)
		if strings.HasPrefix(currentVersionPair[0], "m_") {
			// add entry to merge version removing the leading prefix for sourceID
			hlvOutput.MergeVersions[currentVersionPair[0][2:]] = uint64(version)
		} else {
			// if its not got the prefix we assume its a previous version entry
			hlvOutput.PreviousVersions[currentVersionPair[0]] = uint64(version)
		}
	}
	return hlvOutput
}

// TestHybridLogicalVectorPersistence:
//   - Tests the process of constructing in memory HLV and marshaling it to persisted format
//   - Asserts on the format
//   - Unmarshal the HLV and assert that the process works as expected
func TestHybridLogicalVectorPersistence(t *testing.T) {
	// create HLV
	inputHLV := []string{"cb06dc003846116d9b66d2ab23887a96@123456", "s_YZvBpEaztom9z5V/hDoeIw@1628620455135215600", "m_s_NqiIe0LekFPLeX4JvTO6Iw@1628620455139868700",
		"m_s_LhRPsa7CpjEvP5zeXTXEBA@1628620455147864000"}
	inMemoryHLV := createHLVForTest(t, inputHLV)

	// marshal in memory hlv into persisted form
	byteArray, err := inMemoryHLV.MarshalJSON()
	require.NoError(t, err)

	// convert to string and assert the in memory struct is converted to persisted form correctly
	// no guarantee the order of the marshaling of the mv part so just assert on the values
	strHLV := string(byteArray)
	assert.Contains(t, strHLV, `"cvCas":"0x40e2010000000000`)
	assert.Contains(t, strHLV, `"src":"cb06dc003846116d9b66d2ab23887a96"`)
	assert.Contains(t, strHLV, `"vrs":"0x40e2010000000000"`)
	assert.Contains(t, strHLV, `"s_LhRPsa7CpjEvP5zeXTXEBA":"c0ff05d7ac059a16"`)
	assert.Contains(t, strHLV, `"s_NqiIe0LekFPLeX4JvTO6Iw":"1c008cd6ac059a16"`)
	assert.Contains(t, strHLV, `"pv":{"s_YZvBpEaztom9z5V/hDoeIw":"f0ff44d6ac059a16"}`)

	// Unmarshal the in memory constructed HLV above
	hlvFromPersistance := HybridLogicalVector{}
	err = hlvFromPersistance.UnmarshalJSON(byteArray)
	require.NoError(t, err)

	// assertions on values of unmarshaled HLV
	assert.Equal(t, inMemoryHLV.CurrentVersionCAS, hlvFromPersistance.CurrentVersionCAS)
	assert.Equal(t, inMemoryHLV.SourceID, hlvFromPersistance.SourceID)
	assert.Equal(t, inMemoryHLV.Version, hlvFromPersistance.Version)
	assert.Equal(t, inMemoryHLV.PreviousVersions, hlvFromPersistance.PreviousVersions)
	assert.Equal(t, inMemoryHLV.MergeVersions, hlvFromPersistance.MergeVersions)
}
