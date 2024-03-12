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
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// hlvExpandMacroCASValue causes the field to be populated by CAS value by macro expansion
const hlvExpandMacroCASValue = "expand"

// HybridLogicalVectorInterface is an interface to contain methods that will operate on both a decoded HLV and encoded HLV
type HybridLogicalVectorInterface interface {
	GetValue(sourceID string) (uint64, bool)
}

var _ HybridLogicalVectorInterface = &HybridLogicalVector{}
var _ HybridLogicalVectorInterface = &DecodedHybridLogicalVector{}

// DecodedHybridLogicalVector (HLV) is a type that represents a decoded vector of Hybrid Logical Clocks.
type DecodedHybridLogicalVector struct {
	CurrentVersionCAS uint64            // current version cas (or cvCAS) stores the current CAS in uint64 type at the time of replication
	ImportCAS         uint64            // Set when an import modifies the document CAS but preserves the HLV (import of a version replicated by XDCR)
	SourceID          string            // source bucket uuid in (base64 encoded format) of where this entry originated from
	Version           uint64            // current cas in uint64 format of the current version on the version vector
	MergeVersions     map[string]uint64 // map of merge versions for fast efficient lookup
	PreviousVersions  map[string]uint64 // map of previous versions for fast efficient lookup
}

// Version is representative of a single entry in a HybridLogicalVector.
type Version struct {
	// SourceID is an ID representing the source of the value (e.g. Couchbase Lite ID)
	SourceID string `json:"source_id"`
	// Value is a Hybrid Logical Clock value (In Couchbase Server, CAS is a HLC)
	Value string `json:"version"`
}

// DecodedVersion is a sourceID and version pair in string/uint64 format for use in conflict detection
type DecodedVersion struct {
	// SourceID is an ID representing the source of the value (e.g. Couchbase Lite ID)
	SourceID string `json:"source_id"`
	// Value is a Hybrid Logical Clock value (In Couchbase Server, CAS is a HLC)
	Value uint64 `json:"version"`
}

// CreateDecodedVersion creates a sourceID and version pair in string/uint64 format
func CreateDecodedVersion(source string, version uint64) DecodedVersion {
	return DecodedVersion{
		SourceID: source,
		Value:    version,
	}
}

// CreateVersion creates an encoded sourceID and version pair
func CreateVersion(source, version string) Version {
	return Version{
		SourceID: source,
		Value:    version,
	}
}

func ParseVersion(versionString string) (version Version, err error) {
	timestampString, sourceBase64, found := strings.Cut(versionString, "@")
	if !found {
		return version, fmt.Errorf("Malformed version string %s, delimiter not found", versionString)
	}
	version.SourceID = sourceBase64
	version.Value = timestampString
	return version, nil
}

func ParseDecodedVersion(versionString string) (version DecodedVersion, err error) {
	timestampString, sourceBase64, found := strings.Cut(versionString, "@")
	if !found {
		return version, fmt.Errorf("Malformed version string %s, delimiter not found", versionString)
	}
	version.SourceID = sourceBase64
	version.Value = base.HexCasToUint64(timestampString)
	return version, nil
}

// String returns a Couchbase Lite-compatible string representation of the version.
func (v DecodedVersion) String() string {
	timestamp := string(base.Uint64CASToLittleEndianHex(v.Value))
	source := base64.StdEncoding.EncodeToString([]byte(v.SourceID))
	return timestamp + "@" + source
}

// String returns a version/sourceID pair in CBL string format
func (v Version) String() string {
	return v.Value + "@" + v.SourceID
}

// ExtractCurrentVersionFromHLV will take the current version form the HLV struct and return it in the Version struct
func (hlv *HybridLogicalVector) ExtractCurrentVersionFromHLV() *Version {
	src, vrs := hlv.GetCurrentVersion()
	currVersion := CreateVersion(src, vrs)
	return &currVersion
}

// PersistedHybridLogicalVector is the marshalled format of HybridLogicalVector.
// This representation needs to be kept in sync with XDCR.
type HybridLogicalVector struct {
	CurrentVersionCAS string            `json:"cvCas,omitempty"`     // current version cas (or cvCAS) stores the current CAS in little endian hex format at the time of replication
	ImportCAS         string            `json:"importCAS,omitempty"` // Set when an import modifies the document CAS but preserves the HLV (import of a version replicated by XDCR)
	SourceID          string            `json:"src"`                 // source bucket uuid in (base64 encoded format) of where this entry originated from
	Version           string            `json:"ver"`                 // current cas in little endian hex format of the current version on the version vector
	MergeVersions     map[string]string `json:"mv,omitempty"`        // map of merge versions for fast efficient lookup
	PreviousVersions  map[string]string `json:"pv,omitempty"`        // map of previous versions for fast efficient lookup
}

// NewHybridLogicalVector returns an initialised HybridLogicalVector.
func NewHybridLogicalVector() HybridLogicalVector {
	return HybridLogicalVector{
		PreviousVersions: make(map[string]string),
		MergeVersions:    make(map[string]string),
	}
}

// GetCurrentVersion returns the current version from the HLV in memory.
func (hlv *HybridLogicalVector) GetCurrentVersion() (string, string) {
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

// IsVersionInConflict tests to see if a given version would be in conflict with the in memory HLV.
func (hlv *HybridLogicalVector) IsVersionInConflict(version Version) bool {
	v1 := Version{hlv.SourceID, hlv.Version}
	if v1.isVersionDominating(version) || version.isVersionDominating(v1) {
		return false
	}
	return true
}

// IsVersionKnown checks to see whether the HLV already contains a Version for the provided
// source with a matching or newer value
func (hlv *HybridLogicalVector) DominatesSource(version Version) bool {
	existingValueForSource, found := hlv.GetValue(version.SourceID)
	if !found {
		return false
	}
	return existingValueForSource >= base.HexCasToUint64(version.Value)

}

// AddVersion adds newVersion to the in memory representation of the HLV.
func (hlv *HybridLogicalVector) AddVersion(newVersion Version) error {
	var newVersionCAS uint64
	hlvVersionCAS := base.HexCasToUint64(hlv.Version)
	if newVersion.Value != hlvExpandMacroCASValue {
		newVersionCAS = base.HexCasToUint64(newVersion.Value)
	}
	// check if this is the first time we're adding a source - version pair
	if hlv.SourceID == "" {
		hlv.Version = newVersion.Value
		hlv.SourceID = newVersion.SourceID
		return nil
	}
	// if new entry has the same source we simple just update the version
	if newVersion.SourceID == hlv.SourceID {
		if newVersion.Value != hlvExpandMacroCASValue && newVersionCAS < hlvVersionCAS {
			return fmt.Errorf("attempting to add new version vector entry with a CAS that is less than the current version CAS value for the same source. Current cas: %s new cas %s", hlv.Version, newVersion.Value)
		}
		hlv.Version = newVersion.Value
		return nil
	}
	// if we get here this is a new version from a different sourceID thus need to move current sourceID to previous versions and update current version
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(map[string]string)
	}
	// we need to check if source ID already exists in PV, if so we need to ensure we are only updating with the
	// sourceID-version pair if incoming version is greater than version already there
	if currPVVersion, ok := hlv.PreviousVersions[hlv.SourceID]; ok {
		// if we get here source ID exists in PV, only replace version if it is less than the incoming version
		currPVVersionCAS := base.HexCasToUint64(currPVVersion)
		if currPVVersionCAS < hlvVersionCAS {
			hlv.PreviousVersions[hlv.SourceID] = hlv.Version
		} else {
			return fmt.Errorf("local hlv has current source in previous version with version greater than current version. Current CAS: %s, PV CAS %s", hlv.Version, currPVVersion)
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
	if hlv.PreviousVersions[source] == "" {
		return base.ErrNotFound
	}
	delete(hlv.PreviousVersions, source)
	return nil
}

// isDominating tests if in memory HLV is dominating over another.
// If HLV A dominates CV of HLV B, it can be assumed to dominate the entire HLV, since
// CV dominates PV for a given HLV.  Given this, it's sufficient to check whether HLV A
// has a version for HLV B's current source that's greater than or equal to HLV B's current version.
func (hlv *HybridLogicalVector) isDominating(otherVector HybridLogicalVector) bool {
	return hlv.DominatesSource(Version{otherVector.SourceID, otherVector.Version})
}

// isVersionDominating tests if v2 is dominating v1
func (v1 *Version) isVersionDominating(v2 Version) bool {
	if v1.SourceID != v2.SourceID {
		return false
	}
	if v1.Value > v2.Value {
		return true
	}
	return false
}

// isEqual tests if in memory HLV is equal to another
func (hlv *DecodedHybridLogicalVector) isEqual(otherVector DecodedHybridLogicalVector) bool {
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
func (hlv *DecodedHybridLogicalVector) equalMergeVectors(otherVector DecodedHybridLogicalVector) bool {
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
func (hlv *DecodedHybridLogicalVector) equalPreviousVectors(otherVector DecodedHybridLogicalVector) bool {
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

// GetValue returns the latest CAS value in the HLV for a given sourceID along with boolean value to
// indicate if sourceID is found in the HLV, if the sourceID is not present in the HLV it will return 0 CAS value and false
func (hlv *DecodedHybridLogicalVector) GetValue(sourceID string) (uint64, bool) {
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

// GetVersion returns the latest decoded CAS value in the HLV for a given sourceID
func (hlv *HybridLogicalVector) GetValue(sourceID string) (uint64, bool) {
	if sourceID == "" {
		return 0, false
	}
	var latestVersion uint64
	if sourceID == hlv.SourceID {
		latestVersion = base.HexCasToUint64(hlv.Version)
	}
	if pvEntry, ok := hlv.PreviousVersions[sourceID]; ok {
		entry := base.HexCasToUint64(pvEntry)
		if entry > latestVersion {
			latestVersion = entry
		}
	}
	if mvEntry, ok := hlv.MergeVersions[sourceID]; ok {
		entry := base.HexCasToUint64(mvEntry)
		if entry > latestVersion {
			latestVersion = entry
		}
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
			if hlv.PreviousVersions[i] == "" {
				hlv.setPreviousVersion(i, v)
			} else {
				// if we get here then there is entry for this source in PV so we must check if its newer or not
				otherHLVPVValue := base.HexCasToUint64(v)
				localHLVPVValue := base.HexCasToUint64(hlv.PreviousVersions[i])
				if localHLVPVValue < otherHLVPVValue {
					hlv.setPreviousVersion(i, v)
				}
			}
		}
	}
	// if current source exists in PV, delete it.
	if _, ok := hlv.PreviousVersions[hlv.SourceID]; ok {
		delete(hlv.PreviousVersions, hlv.SourceID)
	}
	return nil
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
func (hlv *HybridLogicalVector) setPreviousVersion(source string, version string) {
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(map[string]string)
	}
	hlv.PreviousVersions[source] = version
}

func (hlv *HybridLogicalVector) IsVersionKnown(otherVersion Version) bool {
	value, found := hlv.GetValue(otherVersion.SourceID)
	if !found {
		return false
	}
	return value >= base.HexCasToUint64(otherVersion.Value)
}

// toHistoryForHLV formats blip History property for V4 replication and above
func (hlv *HybridLogicalVector) ToHistoryForHLV() string {
	// take pv and mv from hlv if defined and add to history
	var s strings.Builder
	// Merge versions must be defined first if they exist
	if hlv.MergeVersions != nil {
		// We need to keep track of where we are in the map, so we don't add a trailing ',' to end of string
		itemNo := 1
		for key, value := range hlv.MergeVersions {
			vrs := Version{SourceID: key, Value: value}
			s.WriteString(vrs.String())
			if itemNo < len(hlv.MergeVersions) {
				s.WriteString(",")
			}
			itemNo++
		}
	}
	if hlv.PreviousVersions != nil {
		// We need to keep track of where we are in the map, so we don't add a trailing ',' to end of string
		itemNo := 1
		// only need ';' if we have MV and PV both defined
		if len(hlv.MergeVersions) > 0 && len(hlv.PreviousVersions) > 0 {
			s.WriteString(";")
		}
		for key, value := range hlv.PreviousVersions {
			vrs := Version{SourceID: key, Value: value}
			s.WriteString(vrs.String())
			if itemNo < len(hlv.PreviousVersions) {
				s.WriteString(",")
			}
			itemNo++
		}
	}
	return s.String()
}

// ToDecodedHybridLogicalVector converts the little endian hex values of a HLV to uint64 values
func (hlv *HybridLogicalVector) ToDecodedHybridLogicalVector() DecodedHybridLogicalVector {
	var decodedVersion, decodedCVCAS, decodedImportCAS uint64
	if hlv.Version != "" {
		decodedVersion = base.HexCasToUint64(hlv.Version)
	}
	if hlv.ImportCAS != "" {
		decodedImportCAS = base.HexCasToUint64(hlv.ImportCAS)
	}
	if hlv.CurrentVersionCAS != "" {
		decodedCVCAS = base.HexCasToUint64(hlv.CurrentVersionCAS)
	}
	decodedHLV := DecodedHybridLogicalVector{
		CurrentVersionCAS: decodedCVCAS,
		Version:           decodedVersion,
		ImportCAS:         decodedImportCAS,
		SourceID:          hlv.SourceID,
		PreviousVersions:  make(map[string]uint64, len(hlv.PreviousVersions)),
		MergeVersions:     make(map[string]uint64, len(hlv.MergeVersions)),
	}

	for i, v := range hlv.PreviousVersions {
		decodedHLV.PreviousVersions[i] = base.HexCasToUint64(v)
	}
	for i, v := range hlv.MergeVersions {
		decodedHLV.MergeVersions[i] = base.HexCasToUint64(v)
	}
	return decodedHLV
}

// appendRevocationMacroExpansions adds macro expansions for the channel map.  Not strictly an HLV operation
// but putting the function here as it's required when the HLV's current version is being macro expanded
func appendRevocationMacroExpansions(currentSpec []sgbucket.MacroExpansionSpec, channelNames []string) (updatedSpec []sgbucket.MacroExpansionSpec) {
	for _, channelName := range channelNames {
		spec := sgbucket.NewMacroExpansionSpec(xattrRevokedChannelVersionPath(base.SyncXattrName, channelName), sgbucket.MacroCas)
		currentSpec = append(currentSpec, spec)
	}
	return currentSpec

}

// extractHLVFromBlipMessage extracts the full HLV a string in the format seen over Blip
// blip string may be the following formats
//  1. cv only:    		cv
//  2. cv and pv:  		cv;pv
//  3. cv, pv, and mv: 	cv;mv;pv
//
// TODO: CBG-3662 - Optimise once we've settled on and tested the format with CBL
func extractHLVFromBlipMessage(versionVectorStr string) (HybridLogicalVector, error) {
	hlv := HybridLogicalVector{}

	vectorFields := strings.Split(versionVectorStr, ";")
	vectorLength := len(vectorFields)
	if (vectorLength == 1 && vectorFields[0] == "") || vectorLength > 3 {
		return HybridLogicalVector{}, fmt.Errorf("invalid hlv in changes message received")
	}

	// add current version (should always be present)
	cvStr := vectorFields[0]
	version := strings.Split(cvStr, "@")
	if len(version) < 2 {
		return HybridLogicalVector{}, fmt.Errorf("invalid version in changes message received")
	}

	err := hlv.AddVersion(Version{SourceID: version[1], Value: version[0]})
	if err != nil {
		return HybridLogicalVector{}, err
	}

	switch vectorLength {
	case 1:
		// cv only
		return hlv, nil
	case 2:
		// only cv and pv present
		sourceVersionListPV, err := parseVectorValues(vectorFields[1])
		if err != nil {
			return HybridLogicalVector{}, err
		}
		hlv.PreviousVersions = make(map[string]string)
		for _, v := range sourceVersionListPV {
			hlv.PreviousVersions[v.SourceID] = v.Value
		}
		return hlv, nil
	case 3:
		// cv, mv and pv present
		sourceVersionListPV, err := parseVectorValues(vectorFields[2])
		hlv.PreviousVersions = make(map[string]string)
		if err != nil {
			return HybridLogicalVector{}, err
		}
		for _, pv := range sourceVersionListPV {
			hlv.PreviousVersions[pv.SourceID] = pv.Value
		}

		sourceVersionListMV, err := parseVectorValues(vectorFields[1])
		hlv.MergeVersions = make(map[string]string)
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

// parseVectorValues takes an HLV section (cv, pv or mv) in string form and splits into
// source and version pairs
func parseVectorValues(vectorStr string) (versions []Version, err error) {
	versionsStr := strings.Split(vectorStr, ",")
	versions = make([]Version, 0, len(versionsStr))

	for _, v := range versionsStr {
		// remove any leading whitespace form the string value
		// TODO: Can avoid by restricting spec
		if len(v) > 0 && v[0] == ' ' {
			v = v[1:]
		}
		version, err := ParseVersion(v)
		if err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}

	return versions, nil
}

// Helper functions for version source and value encoding
func EncodeSource(source string) string {
	return base64.StdEncoding.EncodeToString([]byte(source))
}

func EncodeValue(value uint64) string {
	return base.CasToString(value)
}

// EncodeValueStr converts a simplified number ("1") to a hex-encoded string
func EncodeValueStr(value string) (string, error) {
	return base.StringDecimalToLittleEndianHex(strings.TrimSpace(value))
}
