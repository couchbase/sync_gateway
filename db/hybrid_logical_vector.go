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
	"fmt"
	"math"
	"strconv"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// hlvExpandMacroCASValue causes the field to be populated by CAS value by macro expansion
const hlvExpandMacroCASValue = math.MaxUint64

// HybridLogicalVector (HLV) is a type that represents a vector of Hybrid Logical Clocks.
type HybridLogicalVector struct {
	CurrentVersionCAS uint64            // current version cas (or cvCAS) stores the current CAS at the time of replication
	ImportCAS         uint64            // Set when an import modifies the document CAS but preserves the HLV (import of a version replicated by XDCR)
	SourceID          string            // source bucket uuid of where this entry originated from
	Version           uint64            // current cas of the current version on the version vector
	MergeVersions     map[string]uint64 // map of merge versions for fast efficient lookup
	PreviousVersions  map[string]uint64 // map of previous versions for fast efficient lookup
}

// Version is representative of a single entry in a HybridLogicalVector.
type Version struct {
	// SourceID is an ID representing the source of the value (e.g. Couchbase Lite ID)
	SourceID string `json:"source_id"`
	// Value is a Hybrid Logical Clock value (In Couchbase Server, CAS is a HLC)
	Value uint64 `json:"version"`
}

func CreateVersion(source string, version uint64) Version {
	return Version{
		SourceID: source,
		Value:    version,
	}
}

func CreateVersionFromString(versionString string) (version Version, err error) {
	timestampString, sourceBase64, found := strings.Cut(versionString, "@")
	if !found {
		return version, fmt.Errorf("Malformed version string %s, delimiter not found", versionString)
	}
	sourceBytes, err := base64.StdEncoding.DecodeString(sourceBase64)
	if err != nil {
		return version, fmt.Errorf("Unable to decode sourceID for version %s: %w", versionString, err)
	}
	version.SourceID = string(sourceBytes)
	version.Value = base.HexCasToUint64(timestampString)
	return version, nil
}

// String returns a Couchbase Lite-compatible string representation of the version.
func (v Version) String() string {
	timestamp := string(base.Uint64CASToLittleEndianHex(v.Value))
	source := base64.StdEncoding.EncodeToString([]byte(v.SourceID))
	return timestamp + "@" + source
}

// PersistedHybridLogicalVector is the marshalled format of HybridLogicalVector.
// This representation needs to be kept in sync with XDCR.
type PersistedHybridLogicalVector struct {
	CurrentVersionCAS string            `json:"cvCas,omitempty"`
	ImportCAS         string            `json:"importCAS,omitempty"`
	SourceID          string            `json:"src"`
	Version           string            `json:"vrs"`
	MergeVersions     map[string]string `json:"mv,omitempty"`
	PreviousVersions  map[string]string `json:"pv,omitempty"`
}

// NewHybridLogicalVector returns an initialised HybridLogicalVector.
func NewHybridLogicalVector() HybridLogicalVector {
	return HybridLogicalVector{
		PreviousVersions: make(map[string]uint64),
		MergeVersions:    make(map[string]uint64),
	}
}

// GetCurrentVersion returns the current version from the HLV in memory.
func (hlv *HybridLogicalVector) GetCurrentVersion() (string, uint64) {
	return hlv.SourceID, hlv.Version
}

// GetCurrentVersion returns the current version in transport format
func (hlv *HybridLogicalVector) GetCurrentVersionString() string {
	if hlv == nil || hlv.SourceID == "" {
		return ""
	}
	version := Version{
		SourceID: hlv.SourceID,
		Value:    hlv.Version,
	}
	return version.String()
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

// AddVersion adds newVersion to the in memory representation of the HLV.
func (hlv *HybridLogicalVector) AddVersion(newVersion Version) error {
	if newVersion.Value < hlv.Version {
		return fmt.Errorf("attempting to add new version vector entry with a CAS that is less than the current version CAS value. Current cas: %d new cas %d", hlv.Version, newVersion.Value)
	}
	// check if this is the first time we're adding a source - version pair
	if hlv.SourceID == "" {
		hlv.Version = newVersion.Value
		hlv.SourceID = newVersion.SourceID
		return nil
	}
	// if new entry has the same source we simple just update the version
	if newVersion.SourceID == hlv.SourceID {
		hlv.Version = newVersion.Value
		return nil
	}
	// if we get here this is a new version from a different sourceID thus need to move current sourceID to previous versions and update current version
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(map[string]uint64)
	}
	// we need to check if source ID already exists in PV, if so we need to ensure we are only updating with the
	// sourceID-version pair if incoming version is greater than version already there
	if currPVVersion, ok := hlv.PreviousVersions[hlv.SourceID]; ok {
		// if we get here source ID exists in PV, only replace version if it is less than the incoming version
		if currPVVersion < hlv.Version {
			hlv.PreviousVersions[hlv.SourceID] = hlv.Version
		} else {
			return fmt.Errorf("local hlv has current source in previous versiosn with version greater than current version. Current CAS: %d, PV CAS %d", hlv.Version, currPVVersion)
		}
	} else {
		// source doesn't exist in PV so add
		hlv.PreviousVersions[hlv.SourceID] = hlv.Version
	}
	hlv.Version = newVersion.Value
	hlv.SourceID = newVersion.SourceID
	return nil
}

// Remove removes a source from previous versions of the HLV.
// TODO: Does this need to remove source from current version as well? Merge Versions?
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
	if latestCAS, found := otherVector.GetVersion(hlv.SourceID); found && hlv.Version > latestCAS {
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

// GetVersion returns the latest CAS value in the HLV for a given sourceID along with boolean value to
// indicate if sourceID is found in the HLV, if the sourceID is not present in the HLV it will return 0 CAS value and false
func (hlv *HybridLogicalVector) GetVersion(sourceID string) (uint64, bool) {
	if sourceID == "" {
		return 0, false
	}
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
	// if we have 0 cas value, there is no entry for this source ID in the HLV
	if latestVersion == 0 {
		return latestVersion, false
	}
	return latestVersion, true
}

// AddNewerVersions will take a hlv and add any newer source/version pairs found across CV and PV found in the other HLV taken as parameter
// when both HLV
func (hlv *HybridLogicalVector) AddNewerVersions(otherVector HybridLogicalVector) error {

	// create current version for incoming vector and attempt to add it to the local HLV, AddVersion will handle if attempting to add older
	// version than local HLVs CV pair
	otherVectorCV := Version{SourceID: otherVector.SourceID, Value: otherVector.Version}
	err := hlv.AddVersion(otherVectorCV)
	if err != nil {
		return err
	}

	if otherVector.PreviousVersions != nil || len(otherVector.PreviousVersions) != 0 {
		// Iterate through incoming vector previous versions, update with the version from other vector
		// for source if the local version for that source is lower
		for i, v := range otherVector.PreviousVersions {
			if hlv.PreviousVersions[i] == 0 || hlv.PreviousVersions[i] < v {
				hlv.setPreviousVersion(i, v)
			}
		}
	}
	// if current source exists in PV, delete it.
	if _, ok := hlv.PreviousVersions[hlv.SourceID]; ok {
		delete(hlv.PreviousVersions, hlv.SourceID)
	}
	return nil
}

func (hlv HybridLogicalVector) MarshalJSON() ([]byte, error) {

	persistedHLV, err := hlv.convertHLVToPersistedFormat()
	if err != nil {
		return nil, err
	}

	return base.JSONMarshal(*persistedHLV)
}

func (hlv *HybridLogicalVector) UnmarshalJSON(inputjson []byte) error {
	persistedJSON := PersistedHybridLogicalVector{}
	err := base.JSONUnmarshal(inputjson, &persistedJSON)
	if err != nil {
		return err
	}
	// convert the data to in memory format
	hlv.convertPersistedHLVToInMemoryHLV(persistedJSON)
	return nil
}

func (hlv *HybridLogicalVector) convertHLVToPersistedFormat() (*PersistedHybridLogicalVector, error) {
	persistedHLV := PersistedHybridLogicalVector{}
	var cvCasByteArray []byte
	var importCASBytes []byte
	var vrsCasByteArray []byte
	if hlv.CurrentVersionCAS != 0 {
		cvCasByteArray = base.Uint64CASToLittleEndianHex(hlv.CurrentVersionCAS)
	}
	if hlv.ImportCAS != 0 {
		importCASBytes = base.Uint64CASToLittleEndianHex(hlv.ImportCAS)
	}
	if hlv.Version != 0 {
		vrsCasByteArray = base.Uint64CASToLittleEndianHex(hlv.Version)
	}

	pvPersistedFormat, err := convertMapToPersistedFormat(hlv.PreviousVersions)
	if err != nil {
		return nil, err
	}
	mvPersistedFormat, err := convertMapToPersistedFormat(hlv.MergeVersions)
	if err != nil {
		return nil, err
	}

	persistedHLV.CurrentVersionCAS = string(cvCasByteArray)
	persistedHLV.ImportCAS = string(importCASBytes)
	persistedHLV.SourceID = hlv.SourceID
	persistedHLV.Version = string(vrsCasByteArray)
	persistedHLV.PreviousVersions = pvPersistedFormat
	persistedHLV.MergeVersions = mvPersistedFormat
	return &persistedHLV, nil
}

func (hlv *HybridLogicalVector) convertPersistedHLVToInMemoryHLV(persistedJSON PersistedHybridLogicalVector) {
	hlv.CurrentVersionCAS = base.HexCasToUint64(persistedJSON.CurrentVersionCAS)
	if persistedJSON.ImportCAS != "" {
		hlv.ImportCAS = base.HexCasToUint64(persistedJSON.ImportCAS)
	}
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

// computeMacroExpansions returns the mutate in spec needed for the document update based off the outcome in updateHLV
func (hlv *HybridLogicalVector) computeMacroExpansions() []sgbucket.MacroExpansionSpec {
	var outputSpec []sgbucket.MacroExpansionSpec
	if hlv.Version == hlvExpandMacroCASValue {
		spec := sgbucket.NewMacroExpansionSpec(xattrCurrentVersionPath(base.SyncXattrName), sgbucket.MacroCas)
		outputSpec = append(outputSpec, spec)
		// If version is being expanded, we need to also specify the macro expansion for the expanded rev property
		currentRevSpec := sgbucket.NewMacroExpansionSpec(xattrCurrentRevVersionPath(base.SyncXattrName), sgbucket.MacroCas)
		outputSpec = append(outputSpec, currentRevSpec)
	}
	if hlv.CurrentVersionCAS == hlvExpandMacroCASValue {
		spec := sgbucket.NewMacroExpansionSpec(xattrCurrentVersionCASPath(base.SyncXattrName), sgbucket.MacroCas)
		outputSpec = append(outputSpec, spec)
	}
	return outputSpec
}

// setPreviousVersion will take a source/version pair and add it to the HLV previous versions map
func (hlv *HybridLogicalVector) setPreviousVersion(source string, version uint64) {
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(map[string]uint64)
	}
	hlv.PreviousVersions[source] = version
}

// extractHLVFromBlipMessage extracts the full HLV a string in the format seen over Blip
func extractHLVFromBlipMessage(versionVectorStr string) (HybridLogicalVector, error) {
	hlv := HybridLogicalVector{}

	vectorFields := strings.Split(versionVectorStr, ";")
	vectorLength := len(vectorFields)
	if vectorLength == 1 && vectorFields[0] == "" {
		return HybridLogicalVector{}, fmt.Errorf("invalid hlv in changes message received")
	}

	// add current version (should always be present)
	cvStr := vectorFields[0]
	version := strings.Split(cvStr, "@")

	vrs, err := strconv.ParseUint(version[0], 10, 64)
	if err != nil {
		return HybridLogicalVector{}, err
	}
	err = hlv.AddVersion(Version{SourceID: version[1], Value: vrs})
	if err != nil {
		return HybridLogicalVector{}, err
	}

	switch vectorLength {
	case 1:
		return hlv, nil
	case 2:
		// only cv and pv present
		sourceVersionListPV, err := parseVectorValues(vectorFields[1])
		if err != nil {
			return HybridLogicalVector{}, err
		}
		hlv.PreviousVersions = make(map[string]uint64)
		for _, v := range sourceVersionListPV {
			hlv.PreviousVersions[v.SourceID] = v.Value
		}
		return hlv, nil
	case 3:
		// cv, mv and pv present
		hlv.PreviousVersions = make(map[string]uint64)
		hlv.MergeVersions = make(map[string]uint64)
		sourceVersionListPV, err := parseVectorValues(vectorFields[2])
		if err != nil {
			return HybridLogicalVector{}, err
		}
		for _, pv := range sourceVersionListPV {
			hlv.PreviousVersions[pv.SourceID] = pv.Value
		}
		sourceVersionListMV, err := parseVectorValues(vectorFields[1])
		if err != nil {
			return HybridLogicalVector{}, err
		}
		for _, mv := range sourceVersionListMV {
			hlv.MergeVersions[mv.SourceID] = mv.Value
		}
		return hlv, nil
	default:
		return HybridLogicalVector{}, fmt.Errorf("invalid hlv in changes message received")
	}
}

// parseVectorValues takes a HLV section (for example previous versions) in Blip string form and split that into
// source and version pairs
func parseVectorValues(vectorStr string) (sourceVersionList []Version, err error) {
	vectorPairs := strings.Split(vectorStr, ",")

	// remove any leading whitespace form the string value
	for i, v := range vectorPairs {
		if v[0] == ' ' {
			v = v[1:]
			vectorPairs[i] = v
		}
	}
	for _, v := range vectorPairs {
		sourceAndVersion := strings.Split(v, "@")
		vrs, err := strconv.ParseUint(sourceAndVersion[0], 10, 64)
		if err != nil {
			return nil, err
		}
		sourceVersionList = append(sourceVersionList, Version{SourceID: sourceAndVersion[1], Value: vrs})
	}
	return sourceVersionList, nil
}
