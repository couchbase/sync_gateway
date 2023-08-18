// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

type HybridLogicalVector struct {
	CurrentVersionCAS uint64            // current version cas (or cvCAS) stores the current CAS at the time of replication
	SourceID          string            // source bucket uuid of where this entry originated from
	Version           uint64            // current cas of the current version on the version vector
	MergeVersions     map[string]uint64 // map of merge versions for fast efficient lookup
	PreviousVersions  map[string]uint64 // map of previous versions for fast efficient lookup
}

// CurrentVersionVector is a structure used to add a new sourceID:CAS entry to a HLV
type CurrentVersionVector struct {
	VersionCAS uint64
	SourceID   string
}

type PersistedHybridLogicalVector struct {
	CurrentVersionCAS string            `json:"cvCas,omitempty"`
	SourceID          string            `json:"src,omitempty"`
	Version           string            `json:"vrs,omitempty"`
	MergeVersions     map[string]string `json:"mv,omitempty"`
	PreviousVersions  map[string]string `json:"pv,omitempty"`
}

type PersistedVersionVector struct {
	PersistedHybridLogicalVector `json:"_vv"`
}

// NewHybridLogicalVector returns a HybridLogicalVector struct with maps initialised in the struct
func NewHybridLogicalVector() HybridLogicalVector {
	return HybridLogicalVector{
		PreviousVersions: make(map[string]uint64),
		MergeVersions:    make(map[string]uint64),
	}
}

// GetCurrentVersion return the current version vector from the HLV in memory
func (hlv *HybridLogicalVector) GetCurrentVersion() (string, uint64) {
	return hlv.SourceID, hlv.Version
}

// IsInConflict tests to see if in memory HLV is conflicting with another HLV
func (hlv *HybridLogicalVector) IsInConflict(otherVector HybridLogicalVector) bool {
	// test if either HLV(A) or HLV(B) are dominating over each other. If so they are not in conflict
	if hlv.isDominating(otherVector) || otherVector.isDominating(*hlv) {
		return false
	}
	// if the version vectors aren't dominating over one another then conflict is present
	return true
}

// AddVersion adds a version vector to the in memory representation of a HLV and moves current version vector to
// previous versions on the HLV if needed
func (hlv *HybridLogicalVector) AddVersion(newVersion CurrentVersionVector) error {
	if newVersion.VersionCAS < hlv.Version {
		return fmt.Errorf("attempting to add new verison vector entry with a CAS that is less than the current version CAS value")
	}
	// if new entry has the same source we simple just update the version
	if newVersion.SourceID == hlv.SourceID {
		hlv.Version = newVersion.VersionCAS
		return nil
	}
	// if we get here this is a new version from a different sourceID thus need to move current sourceID to previous versions and update current version
	hlv.PreviousVersions[hlv.SourceID] = hlv.Version
	hlv.Version = newVersion.VersionCAS
	hlv.SourceID = newVersion.SourceID
	return nil
}

// Remove removes a vector from previous versions section of in memory HLV
func (hlv *HybridLogicalVector) Remove(source string) error {
	// if entry is not found in previous versions we return error
	if hlv.PreviousVersions[source] == 0 {
		return base.ErrNotFound
	}
	delete(hlv.PreviousVersions, source)
	return nil
}

// isDominating tests if in memory HLV is dominating over another
func (hlv *HybridLogicalVector) isDominating(otherVector HybridLogicalVector) bool {
	// Dominating Criteria:
	// HLV A dominates HLV B if source(A) == source(B) and version(A) > version(B)
	// If there is an entry in pv(B) for A's current source and version(A) > B's version for that pv entry then A is dominating
	// if there is an entry in mv(B) for A's current source and version(A) > B's version for that pv entry then A is dominating

	// Grab the latest CAS version for HLV(A)'s sourceID in HLV(B), if HLV(A) version CAS is > HLV(B)'s then it is dominating
	// If 0 CAS is returned then the sourceID does not exist on HLV(B)
	if latestCAS := otherVector.GetVersion(hlv.SourceID); latestCAS != 0 && hlv.Version > latestCAS {
		return true
	}
	// HLV A is not dominating over HLV B
	return false
}

// isEqual tests if in memory HLV is equal to another
func (hlv *HybridLogicalVector) isEqual(otherVector HybridLogicalVector) bool {
	// if in HLV(A) sourceID the same as HLV(B) sourceID and HLV(A) CAS is equal to HLV(B) CAS then the two HLV's are equal
	if hlv.SourceID == otherVector.SourceID && hlv.Version == otherVector.Version {
		return true
	}
	// if the HLV(A) merge versions isn't empty and HLV(B) merge versions isn't empty AND if
	// merge versions between the two HLV's are the same, they are equal
	if len(hlv.MergeVersions) != 0 && len(otherVector.MergeVersions) != 0 {
		if hlv.equalMergeVectors(otherVector) {
			return true
		}
	}
	if len(hlv.PreviousVersions) != 0 && len(otherVector.PreviousVersions) != 0 {
		if hlv.equalPreviousVectors(otherVector) {
			return true
		}
	}
	// they aren't equal
	return false
}

// equalMergeVectors tests if two merge vectors between HLV's are equal or not
func (hlv *HybridLogicalVector) equalMergeVectors(otherVector HybridLogicalVector) bool {
	if len(hlv.MergeVersions) != len(otherVector.MergeVersions) {
		return false
	}
	for k, v := range hlv.MergeVersions {
		if v != otherVector.MergeVersions[k] {
			return false
		}
	}
	return true
}

// equalPreviousVectors tests if two previous versions vectors between two HLV's are equal or not
func (hlv *HybridLogicalVector) equalPreviousVectors(otherVector HybridLogicalVector) bool {
	if len(hlv.PreviousVersions) != len(otherVector.PreviousVersions) {
		return false
	}
	for k, v := range hlv.PreviousVersions {
		if v != otherVector.PreviousVersions[k] {
			return false
		}
	}
	return true
}

// GetVersion returns the latest CAS value in the HLV for a given sourceID, if the sourceID is not present in the HLV it will return 0 CAS value
func (hlv *HybridLogicalVector) GetVersion(sourceID string) uint64 {
	var latestVersion uint64
	if sourceID == hlv.SourceID {
		latestVersion = hlv.Version
	}
	if pvEntry := hlv.PreviousVersions[sourceID]; pvEntry > latestVersion {
		latestVersion = pvEntry
	}
	if mvEntry := hlv.MergeVersions[sourceID]; mvEntry > latestVersion {
		latestVersion = mvEntry
	}
	return latestVersion
}

func (hlv *HybridLogicalVector) MarshalJSON() ([]byte, error) {

	persistedHLV, err := hlv.convertHLVToPersistedFormat()
	if err != nil {
		return nil, err
	}

	return base.JSONMarshal(*persistedHLV)
}

func (hlv *HybridLogicalVector) UnmarshalJSON(inputjson []byte) error {
	persistedJSON := PersistedVersionVector{}
	err := base.JSONUnmarshal(inputjson, &persistedJSON)
	if err != nil {
		return err
	}
	// convert the data to in memory format
	hlv.convertPersistedHLVToInMemoryHLV(persistedJSON)
	return nil
}

func (hlv *HybridLogicalVector) convertHLVToPersistedFormat() (*PersistedVersionVector, error) {
	persistedHLV := PersistedVersionVector{}
	var cvCasByteArray []byte
	if hlv.CurrentVersionCAS != 0 {
		cvCasByteArray = base.Uint64CASToLittleEndianHex(hlv.CurrentVersionCAS)
	}
	vrsCasByteArray := base.Uint64CASToLittleEndianHex(hlv.Version)

	pvPersistedFormat, err := convertMapToPersistedFormat(hlv.PreviousVersions)
	if err != nil {
		return nil, err
	}
	mvPersistedFormat, err := convertMapToPersistedFormat(hlv.MergeVersions)
	if err != nil {
		return nil, err
	}

	persistedHLV.CurrentVersionCAS = string(cvCasByteArray)
	persistedHLV.SourceID = hlv.SourceID
	persistedHLV.Version = string(vrsCasByteArray)
	persistedHLV.PreviousVersions = pvPersistedFormat
	persistedHLV.MergeVersions = mvPersistedFormat
	return &persistedHLV, nil
}

func (hlv *HybridLogicalVector) convertPersistedHLVToInMemoryHLV(persistedJSON PersistedVersionVector) {
	hlv.CurrentVersionCAS = base.HexCasToUint64(persistedJSON.CurrentVersionCAS)
	hlv.SourceID = persistedJSON.SourceID
	// convert the hex cas to uint64 cas
	hlv.Version = base.HexCasToUint64(persistedJSON.Version)
	// convert the maps form persisted format to the in memory format
	hlv.PreviousVersions = convertMapToInMemoryFormat(persistedJSON.PreviousVersions)
	hlv.MergeVersions = convertMapToInMemoryFormat(persistedJSON.MergeVersions)
}

// convertMapToPersistedFormat will convert in memory map of previous versions or merge versions into the persisted format map
func convertMapToPersistedFormat(memoryMap map[string]uint64) (map[string]string, error) {
	if memoryMap == nil {
		return nil, nil
	}
	returnedMap := make(map[string]string)
	var persistedCAS string
	for source, cas := range memoryMap {
		casByteArray := base.Uint64CASToLittleEndianHex(cas)
		persistedCAS = string(casByteArray)
		// remove the leading '0x' from the CAS value
		persistedCAS = persistedCAS[2:]
		returnedMap[source] = persistedCAS
	}
	return returnedMap, nil
}

// convertMapToInMemoryFormat will convert the persisted format map to an in memory format of that map.
// Used for previous versions and merge versions maps on HLV
func convertMapToInMemoryFormat(persistedMap map[string]string) map[string]uint64 {
	if persistedMap == nil {
		return nil
	}
	returnedMap := make(map[string]uint64)
	// convert each CAS entry from little endian hex to Uint64
	for key, value := range persistedMap {
		returnedMap[key] = base.HexCasToUint64(value)
	}
	return returnedMap
}
