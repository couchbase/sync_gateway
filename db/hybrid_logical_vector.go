// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

type HLVVersions map[string]uint64 // map of source ID to version uint64 version value

// Version is representative of a single entry in a HybridLogicalVector.
type Version struct {
	// SourceID is an ID representing the source of the value (e.g. Couchbase Lite ID)
	SourceID string `json:"source_id"`
	// Value is a Hybrid Logical Clock value (In Couchbase Server, CAS is a HLC)
	Value uint64 `json:"version"`
}

// VersionsDeltas will be sorted by version, first entry will be fill version then after that will be calculated deltas
type VersionsDeltas []Version

func (vde VersionsDeltas) Len() int { return len(vde) }

func (vde VersionsDeltas) Swap(i, j int) {
	vde[i], vde[j] = vde[j], vde[i]
}

func (vde VersionsDeltas) Less(i, j int) bool {
	if vde[i].Value == vde[j].Value {
		return false
	}
	return vde[i].Value < vde[j].Value
}

// VersionDeltas calculate the deltas of input map
func VersionDeltas(versions map[string]uint64) VersionsDeltas {
	if versions == nil {
		return nil
	}

	vdm := make(VersionsDeltas, 0, len(versions))
	for src, vrs := range versions {
		vdm = append(vdm, CreateVersion(src, vrs))
	}

	// return early for single entry
	if len(vdm) == 1 {
		return vdm
	}

	// sort the list
	sort.Sort(vdm)

	// traverse in reverse order and calculate delta between versions, leaving the first element as is
	for i := len(vdm) - 1; i >= 1; i-- {
		vdm[i].Value = vdm[i].Value - vdm[i-1].Value
	}
	return vdm
}

// VersionsToDeltas will calculate deltas from the input map (pv or mv). Then will return the deltas in persisted format
func VersionsToDeltas(m map[string]uint64) []string {
	if len(m) == 0 {
		return nil
	}

	var vrsList []string
	deltas := VersionDeltas(m)
	for _, delta := range deltas {
		listItem := delta.StringForVersionDelta()
		vrsList = append(vrsList, listItem)
	}

	return vrsList
}

// PersistedDeltasToMap converts the list of deltas in pv or mv from the bucket back from deltas into full versions in map format
func PersistedDeltasToMap(vvList []string) (map[string]uint64, error) {
	vv := make(map[string]uint64)
	if len(vvList) == 0 {
		return vv, nil
	}

	var lastEntryVersion uint64
	for _, v := range vvList {
		timestampString, sourceBase64, found := strings.Cut(v, "@")
		if !found {
			return nil, fmt.Errorf("Malformed version string %s, delimiter not found", v)
		}
		ver, err := base.HexCasToUint64ForDelta([]byte(timestampString))
		if err != nil {
			return nil, err
		}
		lastEntryVersion = ver + lastEntryVersion
		vv[sourceBase64] = lastEntryVersion
	}
	return vv, nil
}

// CreateVersion creates an encoded sourceID and version pair
func CreateVersion(source string, version uint64) Version {
	return Version{
		SourceID: source,
		Value:    version,
	}
}

// ParseVersion will parse source version pair from string format
func ParseVersion(versionString string) (version Version, err error) {
	timestampString, sourceBase64, found := strings.Cut(versionString, "@")
	if !found {
		return version, fmt.Errorf("Malformed version string %s, delimiter not found", versionString)
	}
	version.SourceID = sourceBase64
	// remove any leading whitespace, this should be addressed in CBG-3662
	if len(timestampString) > 0 && timestampString[0] == ' ' {
		timestampString = timestampString[1:]
	}
	vrs, err := strconv.ParseUint(timestampString, 16, 64)
	if err != nil {
		return version, err
	}
	version.Value = vrs
	return version, nil
}

// String returns a version/sourceID pair in CBL string format. This does not match the format serialized on CBS, which will be in 0x0 format.
func (v Version) String() string {
	return strconv.FormatUint(v.Value, 16) + "@" + v.SourceID
}

// IsEmpty returns true if the version is empty/zero value.
func (v Version) IsEmpty() bool {
	return v.SourceID == "" && v.Value == 0
}

// StringForVersionDelta will take a version struct and convert the value to delta format
// (encoding it to LE hex, stripping any 0's off the end and stripping leading 0x)
func (v Version) StringForVersionDelta() string {
	encodedVal := base.Uint64ToLittleEndianHexAndStripZeros(v.Value)
	return encodedVal + "@" + v.SourceID
}

// ExtractCurrentVersionFromHLV will take the current version form the HLV struct and return it in the Version struct
func (hlv *HybridLogicalVector) ExtractCurrentVersionFromHLV() *Version {
	src, vrs := hlv.GetCurrentVersion()
	currVersion := CreateVersion(src, vrs)
	return &currVersion
}

// HybridLogicalVector is the in memory format for the hLv.
type HybridLogicalVector struct {
	CurrentVersionCAS uint64      // current version cas (or cvCAS) stores the current CAS in little endian hex format at the time of replication
	SourceID          string      // source bucket uuid in (base64 encoded format) of where this entry originated from
	Version           uint64      // current cas in little endian hex format of the current version on the version vector
	MergeVersions     HLVVersions // map of merge versions for fast efficient lookup
	PreviousVersions  HLVVersions // map of previous versions for fast efficient lookup
}

// NewHybridLogicalVector returns an initialised HybridLogicalVector.
func NewHybridLogicalVector() *HybridLogicalVector {
	return &HybridLogicalVector{
		PreviousVersions: make(HLVVersions),
		MergeVersions:    make(HLVVersions),
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

// IsVersionKnown checks to see whether the HLV already contains a Version for the provided
// source with a matching or newer value
func (hlv *HybridLogicalVector) DominatesSource(version Version) bool {
	existingValueForSource, found := hlv.GetValue(version.SourceID)
	if !found {
		return false
	}
	return existingValueForSource >= version.Value

}

// AddVersion adds newVersion to the in memory representation of the HLV.
func (hlv *HybridLogicalVector) AddVersion(newVersion Version) error {
	var newVersionCAS uint64
	hlvVersionCAS := hlv.Version
	if newVersion.Value != expandMacroCASValueUint64 {
		newVersionCAS = newVersion.Value
	}
	// check if this is the first time we're adding a source - version pair
	if hlv.SourceID == "" {
		hlv.Version = newVersion.Value
		hlv.SourceID = newVersion.SourceID
		return nil
	}
	// if new entry has the same source we simple just update the version
	if newVersion.SourceID == hlv.SourceID {
		if newVersion.Value != expandMacroCASValueUint64 && newVersionCAS < hlvVersionCAS {
			return fmt.Errorf("attempting to add new version vector entry with a CAS that is less than the current version CAS value for the same source. Current cas: %d new cas %d", hlv.Version, newVersion.Value)
		}
		hlv.Version = newVersion.Value
		return nil
	}
	// if we get here this is a new version from a different sourceID thus need to move current sourceID to previous versions and update current version
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(HLVVersions)
	}
	// we need to check if source ID already exists in PV, if so we need to ensure we are only updating with the
	// sourceID-version pair if incoming version is greater than version already there
	if currPVVersion, ok := hlv.PreviousVersions[hlv.SourceID]; ok {
		// if we get here source ID exists in PV, only replace version if it is less than the incoming version
		currPVVersionCAS := currPVVersion
		if currPVVersionCAS < hlvVersionCAS {
			hlv.PreviousVersions[hlv.SourceID] = hlv.Version
		} else {
			return fmt.Errorf("local hlv has current source in previous version with version greater than current version. Current CAS: %d, PV CAS %d", hlv.Version, currPVVersion)
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

// isDominating tests if in memory HLV is dominating over another.
// If HLV A dominates CV of HLV B, it can be assumed to dominate the entire HLV, since
// CV dominates PV for a given HLV.  Given this, it's sufficient to check whether HLV A
// has a version for HLV B's current source that's greater than or equal to HLV B's current version.
func (hlv *HybridLogicalVector) isDominating(otherVector *HybridLogicalVector) bool {
	return hlv.DominatesSource(Version{otherVector.SourceID, otherVector.Version})
}

// GetVersion returns the latest decoded CAS value in the HLV for a given sourceID
func (hlv *HybridLogicalVector) GetValue(sourceID string) (uint64, bool) {
	if sourceID == "" {
		return 0, false
	}
	var latestVersion uint64
	if sourceID == hlv.SourceID {
		latestVersion = hlv.Version
	}
	if pvEntry, ok := hlv.PreviousVersions[sourceID]; ok {
		entry := pvEntry
		if entry > latestVersion {
			latestVersion = entry
		}
	}
	if mvEntry, ok := hlv.MergeVersions[sourceID]; ok {
		entry := mvEntry
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
func (hlv *HybridLogicalVector) AddNewerVersions(otherVector *HybridLogicalVector) error {

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
			if hlv.PreviousVersions[i] == 0 {
				hlv.setPreviousVersion(i, v)
			} else {
				// if we get here then there is entry for this source in PV so we must check if its newer or not
				otherHLVPVValue := v
				localHLVPVValue := hlv.PreviousVersions[i]
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
	if hlv.Version == expandMacroCASValueUint64 {
		spec := sgbucket.NewMacroExpansionSpec(xattrCurrentVersionPath(base.VvXattrName), sgbucket.MacroCas)
		outputSpec = append(outputSpec, spec)
		// If version is being expanded, we need to also specify the macro expansion for the expanded rev property
		currentRevSpec := sgbucket.NewMacroExpansionSpec(xattrCurrentRevVersionPath(base.SyncXattrName), sgbucket.MacroCas)
		outputSpec = append(outputSpec, currentRevSpec)
	}
	if hlv.CurrentVersionCAS == expandMacroCASValueUint64 {
		spec := sgbucket.NewMacroExpansionSpec(xattrCurrentVersionCASPath(base.VvXattrName), sgbucket.MacroCas)
		outputSpec = append(outputSpec, spec)
	}
	return outputSpec
}

// setPreviousVersion will take a source/version pair and add it to the HLV previous versions map
func (hlv *HybridLogicalVector) setPreviousVersion(source string, version uint64) {
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(HLVVersions)
	}
	hlv.PreviousVersions[source] = version
}

func (hlv *HybridLogicalVector) IsVersionKnown(otherVersion Version) bool {
	value, found := hlv.GetValue(otherVersion.SourceID)
	if !found {
		return false
	}
	return value >= otherVersion.Value
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

// appendRevocationMacroExpansions adds macro expansions for the channel map.  Not strictly an HLV operation
// but putting the function here as it's required when the HLV's current version is being macro expanded
func appendRevocationMacroExpansions(currentSpec []sgbucket.MacroExpansionSpec, channelNames []string) (updatedSpec []sgbucket.MacroExpansionSpec) {
	for _, channelName := range channelNames {
		spec := sgbucket.NewMacroExpansionSpec(xattrRevokedChannelVersionPath(base.SyncXattrName, channelName), sgbucket.MacroCas)
		currentSpec = append(currentSpec, spec)
	}
	return currentSpec

}

// ExtractHLVFromBlipMessage extracts the full HLV a string in the format seen over Blip
// blip string may be the following formats
//  1. cv only:    		cv
//  2. cv and pv:  		cv;pv
//  3. cv, pv, and mv: 	cv;mv;pv
//
// Function will return list of revIDs if legacy rev ID was found in the HLV history section (PV)
// TODO: CBG-3662 - Optimise once we've settled on and tested the format with CBL
func ExtractHLVFromBlipMessage(versionVectorStr string) (*HybridLogicalVector, []string, error) {
	hlv := &HybridLogicalVector{}

	vectorFields := strings.Split(versionVectorStr, ";")
	vectorLength := len(vectorFields)
	if (vectorLength == 1 && vectorFields[0] == "") || vectorLength > 3 {
		return &HybridLogicalVector{}, nil, fmt.Errorf("invalid hlv in changes message received")
	}

	// add current version (should always be present)
	cvStr := vectorFields[0]
	version := strings.Split(cvStr, "@")
	if len(version) < 2 {
		return &HybridLogicalVector{}, nil, fmt.Errorf("invalid version in changes message received")
	}

	vrs, err := strconv.ParseUint(version[0], 16, 64)
	if err != nil {
		return &HybridLogicalVector{}, nil, err
	}
	err = hlv.AddVersion(Version{SourceID: version[1], Value: vrs})
	if err != nil {
		return &HybridLogicalVector{}, nil, err
	}

	switch vectorLength {
	case 1:
		// cv only
		return hlv, nil, nil
	case 2:
		// only cv and pv present
		sourceVersionListPV, legacyRev, err := parseVectorValues(vectorFields[1])
		if err != nil {
			return &HybridLogicalVector{}, nil, err
		}
		hlv.PreviousVersions = make(HLVVersions)
		for _, v := range sourceVersionListPV {
			hlv.PreviousVersions[v.SourceID] = v.Value
		}
		return hlv, legacyRev, nil
	case 3:
		// cv, mv and pv present
		sourceVersionListPV, legacyRev, err := parseVectorValues(vectorFields[2])
		hlv.PreviousVersions = make(HLVVersions)
		if err != nil {
			return &HybridLogicalVector{}, nil, err
		}
		for _, pv := range sourceVersionListPV {
			hlv.PreviousVersions[pv.SourceID] = pv.Value
		}

		sourceVersionListMV, _, err := parseVectorValues(vectorFields[1])
		hlv.MergeVersions = make(HLVVersions)
		if err != nil {
			return &HybridLogicalVector{}, nil, err
		}
		for _, mv := range sourceVersionListMV {
			hlv.MergeVersions[mv.SourceID] = mv.Value
		}
		return hlv, legacyRev, nil
	default:
		return &HybridLogicalVector{}, nil, fmt.Errorf("invalid hlv in changes message received")
	}
}

// parseVectorValues takes an HLV section (cv, pv or mv) in string form and splits into
// source and version pairs. Also returns legacyRev list if legacy revID's are found in the input string.
func parseVectorValues(vectorStr string) (versions []Version, legacyRevList []string, err error) {
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
			// If v is a legacy rev ID, ignore when constructing the HLV.
			if isLegacyRev(v) {
				legacyRevList = append(legacyRevList, v)
				continue
			}
			return nil, nil, err
		}
		versions = append(versions, version)
	}

	return versions, legacyRevList, nil
}

// isLegacyRev returns true if the given string is a revID, false otherwise. Has the same functionality as ParseRevID
// but doesn't warn for malformed revIDs
func isLegacyRev(rev string) bool {
	if rev == "" {
		return false
	}

	idx := strings.Index(rev, "-")
	if idx == -1 {
		return false
	}

	gen, err := strconv.Atoi(rev[:idx])
	if err != nil {
		return false
	} else if gen < 1 {
		return false
	}
	return true
}

// Helper functions for version source and value encoding
func EncodeSource(source string) string {
	return base64.StdEncoding.EncodeToString([]byte(source))
}

// EncodeValueStr converts a simplified number ("1") to a hex-encoded string
func EncodeValueStr(value string) (string, error) {
	return base.StringDecimalToLittleEndianHex(strings.TrimSpace(value))
}

// CreateEncodedSourceID will hash the bucket UUID and cluster UUID using md5 hash function then will base64 encode it
// This function is in sync with xdcr implementation of UUIDstoDocumentSource https://github.com/couchbase/goxdcr/blob/dfba7a5b4251d93db46e2b0b4b55ea014218931b/hlv/hlv.go#L51
func CreateEncodedSourceID(bucketUUID, clusterUUID string) (string, error) {
	md5Hash := md5.Sum([]byte(bucketUUID + clusterUUID))
	hexStr := hex.EncodeToString(md5Hash[:])
	source, err := base.HexToBase64(hexStr)
	if err != nil {
		return "", err
	}
	return string(source), nil
}

func (hlv HybridLogicalVector) MarshalJSON() ([]byte, error) {
	type BucketVector struct {
		CurrentVersionCAS string    `json:"cvCas,omitempty"`
		SourceID          string    `json:"src"`
		Version           string    `json:"ver"`
		PV                *[]string `json:"pv,omitempty"`
		MV                *[]string `json:"mv,omitempty"`
	}
	var cvCas string
	var vrsCas string

	var bucketHLV = BucketVector{}
	if hlv.CurrentVersionCAS != 0 {
		cvCas = base.CasToString(hlv.CurrentVersionCAS)
		bucketHLV.CurrentVersionCAS = cvCas
	}
	vrsCas = base.CasToString(hlv.Version)
	bucketHLV.Version = vrsCas
	bucketHLV.SourceID = hlv.SourceID

	pvPersistedFormat := VersionsToDeltas(hlv.PreviousVersions)
	if len(pvPersistedFormat) > 0 {
		bucketHLV.PV = &pvPersistedFormat
	}
	mvPersistedFormat := VersionsToDeltas(hlv.MergeVersions)
	if len(mvPersistedFormat) > 0 {
		bucketHLV.MV = &mvPersistedFormat
	}

	return base.JSONMarshal(&bucketHLV)
}

func (hlv *HybridLogicalVector) UnmarshalJSON(inputjson []byte) error {
	type BucketVector struct {
		CurrentVersionCAS string    `json:"cvCas,omitempty"`
		SourceID          string    `json:"src"`
		Version           string    `json:"ver"`
		PV                *[]string `json:"pv,omitempty"`
		MV                *[]string `json:"mv,omitempty"`
	}
	var bucketDeltas BucketVector
	err := base.JSONUnmarshal(inputjson, &bucketDeltas)
	if err != nil {
		return err
	}
	if bucketDeltas.CurrentVersionCAS != "" {
		hlv.CurrentVersionCAS = base.HexCasToUint64(bucketDeltas.CurrentVersionCAS)
	}

	hlv.SourceID = bucketDeltas.SourceID
	hlv.Version = base.HexCasToUint64(bucketDeltas.Version)
	if bucketDeltas.PV != nil {
		prevVersion, err := PersistedDeltasToMap(*bucketDeltas.PV)
		if err != nil {
			return err
		}
		hlv.PreviousVersions = prevVersion
	}
	if bucketDeltas.MV != nil {
		mergeVersion, err := PersistedDeltasToMap(*bucketDeltas.MV)
		if err != nil {
			return err
		}
		hlv.MergeVersions = mergeVersion
	}
	return nil
}
