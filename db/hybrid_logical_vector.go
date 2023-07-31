// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

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

// GetCurrentVersion return the current version vector from the HLV in memory
func (hlv *HybridLogicalVector) GetCurrentVersion() (string, uint64) {
	return hlv.SourceID, hlv.Version
}

// IsInConflict tests to see if in memory HLV is conflicting with another HLV
func (hlv *HybridLogicalVector) IsInConflict(otherVector HybridLogicalVector) bool {
	// test if in memory version vector is dominating or if they are equal. If so no conflict is present
	if hlv.isDominating(otherVector) || hlv.isEqual(otherVector) {
		return false
	}
	// if the version vectors aren't dominating or equal then conflict is present
	return true
}

// AddVersion adds a version vector to the in memory representation of a HLV and moves current version vector to
// previous versions on the HLV
func (hlv *HybridLogicalVector) AddVersion(version CurrentVersionVector) {
	hlv.PreviousVersions[hlv.SourceID] = hlv.Version
	hlv.Version = version.VersionCAS
	hlv.SourceID = version.SourceID
}

// Remove removes a vector from previous versions section of in memory HLV
func (hlv *HybridLogicalVector) Remove(source string) {
	delete(hlv.PreviousVersions, source)
}

// isDominating tests if in memory HLV is dominating over another
func (hlv *HybridLogicalVector) isDominating(otherVector HybridLogicalVector) bool {
	// in memory hlv is dominating if source ID matches other sourceID but with in memory CAS greater than other vector
	if hlv.SourceID == otherVector.SourceID && hlv.Version > otherVector.Version {
		return true
	}
	// if im memory CAS is greater than other and other sourceID:CAS pair is inside previous versions or
	// merge versions then in memory HLV is dominating
	if hlv.Version > otherVector.Version {
		if hlv.PreviousVersions[otherVector.SourceID] == otherVector.Version {
			return true
		}
		if hlv.MergeVersions[otherVector.SourceID] == otherVector.Version {
			return true
		}
	}
	return false
}

// isEqual tests if in memory HLV is equal to another
func (hlv *HybridLogicalVector) isEqual(otherVector HybridLogicalVector) bool {
	// if in memory hlv sourceID the sae as other sourceID and in memory CAS is equal to other CAS then HLV's are equal
	if hlv.SourceID == otherVector.SourceID && hlv.Version == otherVector.Version {
		return true
	}
	// if the in memory HLV merge versions isn't empty and the other HLV merge versions isn't empty AND if
	// merge versions between the two HLV's are the same, they are equal
	if len(hlv.MergeVersions) != 0 && len(otherVector.MergeVersions) != 0 {
		if hlv.equalMergeVectors(otherVector) {
			return true
		}
	}
	// they aren't equal
	return false
}

// equalMergeVectors tests if two merge vectors between HLV's are equal or not
func (hlv *HybridLogicalVector) equalMergeVectors(otherVector HybridLogicalVector) bool {
	for k, v := range hlv.MergeVersions {
		if v != otherVector.MergeVersions[k] {
			return false
		}
	}
	return true
}
