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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternalHLVFunctions:
//   - Tests internal api methods on the HLV work as expected
//   - Tests methods GetCurrentVersion, AddVersion and Remove
func TestInternalHLVFunctions(t *testing.T) {
	mv := make(map[string]uint64)
	pv := make(map[string]uint64)
	currSourceId := "s_5pRi8Piv1yLcLJ1iVNJIsA"
	const currVersion = 12345678

	mv["s_NqiIe0LekFPLeX4JvTO6Iw"] = 345454
	pv["s_YZvBpEaztom9z5V/hDoeIw"] = 64463204720

	hlv := HybridLogicalVector{
		CurrentVersionCAS: currVersion,
		SourceID:          currSourceId,
		Version:           currVersion,
		MergeVersions:     mv,
		PreviousVersions:  pv,
	}

	const newCAS = 123456789
	const newSource = "s_testsource"
	currVersionVector := CurrentVersionVector{
		VersionCAS: 123456789,
		SourceID:   newSource,
	}

	pv[currSourceId] = currVersion

	// Get current version vector, sourceID and CAS pair
	source, version := hlv.GetCurrentVersion()
	assert.Equal(t, currSourceId, source)
	assert.Equal(t, uint64(currVersion), version)

	// Add a new version vector pair to the HLV structure and assert that it moves the current version vector pair to the previous versions section
	hlv.AddVersion(currVersionVector)
	assert.Equal(t, uint64(newCAS), hlv.Version)
	assert.Equal(t, newSource, hlv.SourceID)
	assert.True(t, reflect.DeepEqual(hlv.PreviousVersions, pv))

	// Remove a sourceID CAS pair from previous versions section of the HLV structure (for compaction)
	hlv.Remove(currSourceId)
	delete(pv, currSourceId)
	assert.True(t, reflect.DeepEqual(hlv.PreviousVersions, pv))
}

// TestConflictDetectionDominating:
//   - Tests two cases where one HLV's is said to be 'dominating' over another and thus not in conflict
//   - Test case 1: where sourceID is the same between HLV's but the in memory representation has higher version CAS
//   - Test case 2: where sourceID is different and the in memory HLV has higher CAS than other HLV but the other HLV's
//     sourceID:CAS pair is present in the in memory HLV's previous versions
//   - Assert that both scenarios returns false from IsInConflict method, as we have a HLV that is dominating in each case
func TestConflictDetectionDominating(t *testing.T) {
	testCases := []struct {
		name          string
		sourceID      string
		version       uint64
		otherSourceID string
		otherVersion  uint64
	}{
		{
			name:          "dominating case 1",
			sourceID:      "cluster1",
			version:       2,
			otherSourceID: "cluster1",
			otherVersion:  1,
		},
		{
			name:          "dominating case 2",
			sourceID:      "cluster1",
			version:       2,
			otherSourceID: "cluster2",
			otherVersion:  1,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.name == "dominating case 1" {
				inMemoryHLV := HybridLogicalVector{
					CurrentVersionCAS: testCase.version,
					SourceID:          testCase.sourceID,
					Version:           testCase.version,
				}
				otherHLV := HybridLogicalVector{
					CurrentVersionCAS: testCase.otherVersion,
					SourceID:          testCase.otherSourceID,
					Version:           testCase.otherVersion,
				}
				require.False(t, inMemoryHLV.IsInConflict(otherHLV))
			} else {
				inMemoryPV := make(map[string]uint64)
				inMemoryHLV := HybridLogicalVector{
					CurrentVersionCAS: testCase.version,
					SourceID:          testCase.sourceID,
					Version:           testCase.version,
				}
				otherHLV := HybridLogicalVector{
					CurrentVersionCAS: testCase.otherVersion,
					SourceID:          testCase.otherSourceID,
					Version:           testCase.otherVersion,
				}
				inMemoryPV["cluster2"] = 1
				inMemoryHLV.PreviousVersions = inMemoryPV
				require.False(t, inMemoryHLV.IsInConflict(otherHLV))
			}
		})
	}
}

// TestConflictEqualHLV:
//   - Creates two 'equal' HLV's and asserts that they are not in conflict
//   - Then tests other code path in event source ID differs and current CAS differs but with identical merge versions
//     that we identify they are not in conflict and are in fact 'equal'
func TestConflictEqualHLV(t *testing.T) {
	inMemoryMV := make(map[string]uint64)
	inMemoryPV := make(map[string]uint64)

	inMemoryHLV := HybridLogicalVector{
		CurrentVersionCAS: 1,
		SourceID:          "cluster1",
		Version:           1,
		MergeVersions:     inMemoryMV,
		PreviousVersions:  inMemoryPV,
	}

	otherVectorMV := make(map[string]uint64)
	otherVectorPV := make(map[string]uint64)
	otherVector := HybridLogicalVector{
		CurrentVersionCAS: 1,
		SourceID:          "cluster1",
		Version:           1,
		MergeVersions:     otherVectorMV,
		PreviousVersions:  otherVectorPV,
	}
	require.False(t, inMemoryHLV.IsInConflict(otherVector))

	// test conflict detection with different version CAS but same merge versions (force that code path)
	inMemoryHLV.Version = 12
	inMemoryHLV.SourceID = "cluster2"
	inMemoryHLV.MergeVersions["test1"] = 123
	otherVector.MergeVersions["test1"] = 123
	require.False(t, inMemoryHLV.IsInConflict(otherVector))
}

// TestConflictExample:
//   - Takes example conflict scenario from PRD to see if we correctly identify conflict in that scenario
//   - Creates two HLV's similar to ones in example and calls IsInConflict to assert it returns true
func TestConflictExample(t *testing.T) {
	inMemoryMV := make(map[string]uint64)
	inMemoryPV := make(map[string]uint64)
	inMemoryHLV := HybridLogicalVector{
		CurrentVersionCAS: 1,
		SourceID:          "cluster1",
		Version:           3,
		MergeVersions:     inMemoryMV,
		PreviousVersions:  inMemoryPV,
	}

	otherVectorMV := make(map[string]uint64)
	otherVectorPV := make(map[string]uint64)
	otherVector := HybridLogicalVector{
		CurrentVersionCAS: 1,
		SourceID:          "cluster2",
		Version:           2,
		MergeVersions:     otherVectorMV,
		PreviousVersions:  otherVectorPV,
	}
	require.True(t, inMemoryHLV.IsInConflict(otherVector))
}
