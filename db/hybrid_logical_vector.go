// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
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

func (v Version) GoString() string {
	return fmt.Sprintf("Version{SourceID:%s, Value:%d}", v.SourceID, v.Value)
}

// IsEmpty returns true if the version is empty/zero value.
func (v Version) IsEmpty() bool {
	return v.SourceID == "" && v.Value == 0
}

// Equal returns true if sourceID and value of the two versions are equal.
func (v Version) Equal(other Version) bool {
	return v.SourceID == other.SourceID && v.Value == other.Value
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

// Equal compares the full HLV to another HLV.
func (hlv *HybridLogicalVector) Equal(other *HybridLogicalVector) bool {
	if hlv.SourceID != other.SourceID {
		return false
	}
	if hlv.Version != other.Version {
		return false
	}

	if !maps.Equal(hlv.PreviousVersions, other.PreviousVersions) {
		return false
	}

	if !maps.Equal(hlv.MergeVersions, other.MergeVersions) {
		return false
	}

	return true
}

func (hlv *HybridLogicalVector) Copy() *HybridLogicalVector {
	if hlv == nil {
		return nil
	}
	return &HybridLogicalVector{
		CurrentVersionCAS: hlv.CurrentVersionCAS,
		SourceID:          hlv.SourceID,
		Version:           hlv.Version,
		MergeVersions:     maps.Clone(hlv.MergeVersions),
		PreviousVersions:  maps.Clone(hlv.PreviousVersions),
	}
}

// GetCurrentVersion returns the current version from the HLV in memory.
func (hlv *HybridLogicalVector) GetCurrentVersion() (string, uint64) {
	return hlv.SourceID, hlv.Version
}

// GetCurrentVersionString returns the current version in transport format
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

// DominatesSource checks to see whether the HLV already contains a Version for the provided
// source with a matching or newer value
func (hlv *HybridLogicalVector) DominatesSource(version Version) bool {
	existingValueForSource, found := hlv.GetValue(version.SourceID)
	if !found {
		return false
	}
	return existingValueForSource >= version.Value

}

// AddVersion adds newVersion as the current version to the in memory representation of the HLV.
func (hlv *HybridLogicalVector) AddVersion(newVersion Version) error {

	// check if this is the first time we're adding a source - version pair
	if hlv.SourceID == "" {
		hlv.Version = newVersion.Value
		hlv.SourceID = newVersion.SourceID
		return nil
	}

	// If the new version is older than an existing version for the same source, return error
	existingValueForSource, found := hlv.GetValue(newVersion.SourceID)
	if found && existingValueForSource > newVersion.Value {
		return fmt.Errorf("attempting to add new version vector entry with a value that is less than the existing value for the same source. New version: %v, Existing HLV: %v", newVersion, hlv)
	}

	// Move existing mv to pv before adding the new version
	hlv.InvalidateMV()

	// If the new version has the same source as existing cv, we just update the cv value
	if newVersion.SourceID == hlv.SourceID {
		hlv.Version = newVersion.Value
		return nil
	}

	// If we get here this is a new version from a different sourceID.  Need to move existing cv to pv and update cv
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(HLVVersions)
	}

	hlv.PreviousVersions[hlv.SourceID] = hlv.Version

	// If new version source already existed in PV, need to remove it
	delete(hlv.PreviousVersions, newVersion.SourceID)
	hlv.Version = newVersion.Value
	hlv.SourceID = newVersion.SourceID
	return nil
}

// InvalidateMV will move all merge versions to PV, except merge version entries that share a source with cv
func (hlv *HybridLogicalVector) InvalidateMV() {
	for source, value := range hlv.MergeVersions {
		if source == hlv.SourceID {
			continue
		}
		hlv.SetPreviousVersion(source, value)
	}
	hlv.MergeVersions = nil
}

// Remove removes a source from previous versions of the HLV.
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

	// Copy incoming merge versions (previously existing merge versions will have been moved to pv by AddVersion)
	for i, v := range otherVector.MergeVersions {
		hlv.SetMergeVersion(i, v)
	}

	if len(otherVector.PreviousVersions) != 0 {
		// Iterate through incoming vector previous versions, update with the version from other vector
		// for source if the local version for that source is lower
		for i, v := range otherVector.PreviousVersions {
			if hlv.PreviousVersions[i] == 0 {
				hlv.SetPreviousVersion(i, v)
			} else {
				// if we get here then there is entry for this source in PV so we must check if its newer or not
				otherHLVPVValue := v
				localHLVPVValue := hlv.PreviousVersions[i]
				if localHLVPVValue < otherHLVPVValue {
					hlv.SetPreviousVersion(i, v)
				}
			}
		}
	}
	// ensure no duplicates of cv, mv in pv
	delete(hlv.PreviousVersions, hlv.SourceID)

	for source := range hlv.MergeVersions {
		delete(hlv.PreviousVersions, source)
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

// SetPreviousVersion will take a source/version pair and add it to the HLV previous versions map
func (hlv *HybridLogicalVector) SetPreviousVersion(source string, version uint64) {
	if hlv.PreviousVersions == nil {
		hlv.PreviousVersions = make(HLVVersions)
	}
	hlv.PreviousVersions[source] = version
}

// SetMergeVersion will take a source/version pair and add it to the HLV merge versions map
func (hlv *HybridLogicalVector) SetMergeVersion(source string, version uint64) {
	if hlv.MergeVersions == nil {
		hlv.MergeVersions = make(HLVVersions)
	}
	hlv.MergeVersions[source] = version
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
		if itemNo > 1 {
			s.WriteString(";")
		}
	}
	if hlv.PreviousVersions != nil {
		// We need to keep track of where we are in the map, so we don't add a trailing ',' to end of string
		itemNo := 1
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

// extractHLVFromBlipMessage extracts the full HLV a string in the format seen over Blip
// blip string may be the following formats
//  1. cv only:    		cv
//  2. cv and pv:  		cv;pv
//  3. cv, pv, and mv: 	cv;mv;pv
//
// Function will return list of revIDs if legacy rev ID was found in the HLV history section (PV)
// TODO: CBG-3662 - Optimise once we've settled on and tested the format with CBL
func extractHLVFromBlipString(versionVectorStr string) (*HybridLogicalVector, []string, error) {
	hlv := &HybridLogicalVector{}

	vectorFields := strings.Split(versionVectorStr, ";")
	vectorLength := len(vectorFields)
	if vectorLength == 1 && vectorFields[0] == "" {
		return nil, nil, fmt.Errorf("invalid empty hlv in changes message received: %q", versionVectorStr)
	}
	if vectorLength > 2 {
		return nil, nil, fmt.Errorf("invalid hlv in changes message received, more than one semi-colon: %q", versionVectorStr)
	}

	cvmvList, legacyRevs, err := parseVectorValues(vectorFields[0])
	if err != nil {
		return nil, nil, err
	}
	if legacyRevs != nil {
		return nil, nil, fmt.Errorf("invalid hlv in changes message received, legacy revID found in cv: %q", vectorFields[0])
	}
	for i, v := range cvmvList {
		switch i {
		case 0:
			err := hlv.AddVersion(v)
			if err != nil {
				return nil, nil, err
			}
			continue
		case 1:
			hlv.MergeVersions = make(HLVVersions)
		}
		if _, ok := hlv.MergeVersions[v.SourceID]; ok {
			return nil, nil, fmt.Errorf("SourceID %q found multiple times in mv for %q", v.SourceID, versionVectorStr)
		}
		if v.SourceID == hlv.SourceID && v.Value == hlv.Version {
			return nil, nil, fmt.Errorf("cv exists in mv for %q", versionVectorStr)
		}
		hlv.MergeVersions[v.SourceID] = v.Value
	}
	// no pv
	if vectorLength == 1 {
		return hlv, nil, nil
	} else if vectorFields[1] == "" { // trailing semi-colon
		return hlv, nil, nil
	}
	pvList, legacyRevs, err := parseVectorValues(vectorFields[1])
	if err != nil {
		return nil, nil, err
	}
	for i, v := range pvList {
		if i == 0 {
			hlv.PreviousVersions = make(HLVVersions)
		}
		if _, ok := hlv.PreviousVersions[v.SourceID]; ok {
			return nil, nil, fmt.Errorf("SourceID %q found multiple times in pv for %q", v.SourceID, versionVectorStr)
		}
		if _, ok := hlv.MergeVersions[v.SourceID]; ok {
			return nil, nil, fmt.Errorf("SourceID %q found in pv and mv for %q", v.SourceID, versionVectorStr)
		}
		hlv.PreviousVersions[v.SourceID] = v.Value
	}
	return hlv, legacyRevs, nil
}

// ExtractCVFromProposeChangesRev strips any trailing HLV content from proposeChanges rev property(CBG-4460)
func ExtractCVFromProposeChangesRev(rev string) string {
	pvDelimiter := strings.Index(rev, ";")
	if pvDelimiter > 0 {
		rev = rev[:pvDelimiter]
	}
	mvDelimiter := strings.Index(rev, ",")
	if mvDelimiter > 0 {
		rev = rev[:mvDelimiter]
	}

	return strings.TrimSpace(rev)
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

func (hlv HybridLogicalVector) GoString() string {
	return fmt.Sprintf("HybridLogicalVector{CurrentVersionCAS:%d, SourceID:%s, Version:%d, PreviousVersions:%#+v, MergeVersions:%#+v}", hlv.CurrentVersionCAS, hlv.SourceID, hlv.Version, hlv.PreviousVersions, hlv.MergeVersions)
}

// ErrNoNewVersionsToAdd will be thrown when there are no new versions from incoming HLV to be added to HLV that is local
var ErrNoNewVersionsToAdd = errors.New("no new versions to add to HLV")

// IsInConflict is used to identify if two HLV's are in conflict or not. Will return boolean to indicate if in conflict
// or not and will error for the following cases:
//   - Local HLV dominates incoming HLV (meaning local version is a newer version that the incoming one)
//   - Local CV matches incoming CV, so no new versions to add
func IsInConflict(ctx context.Context, localHLV, incomingHLV *HybridLogicalVector) (bool, error) {
	incomingCV := incomingHLV.ExtractCurrentVersionFromHLV()
	localCV := localHLV.ExtractCurrentVersionFromHLV()

	// check if incoming CV and local CV are the same. This is needed here given that if both CV's are the same the
	// below check to check local revision is newer than incoming revision will pass given the check and will add the
	// incoming versions even if CVs are the same
	if localCV.Equal(*incomingCV) {
		base.TracefCtx(ctx, base.KeyVV, "incoming CV %#+v is equal to local revision %#+v", incomingCV, localCV)
		return false, ErrNoNewVersionsToAdd
	}

	// standard no conflict case. In the simple case, this happens when:
	//  - Client A writes document 1@srcA
	//  - Client B pulls document 1@srcA from Client A
	//  - Client A writes document 2@srcA
	//	- Client B pulls document 2@srcA from Client A
	if incomingHLV.DominatesSource(*localCV) {
		return false, nil
	}

	// local revision is newer than incoming revision. Common case:
	// - Client A writes document 1@srcA
	// - Client A pushes to Client B as 1@srcA
	// - Client A pulls document 1@srcA from Client B
	//
	// NOTE: without P2P replication, this should not be the case and we would not get this revision, since Client A
	// would respond to a Client B changes message that Client A does not need this revision
	if localHLV.DominatesSource(*incomingCV) {
		return false, ErrNoNewVersionsToAdd
	}
	// Check if conflict has been previously resolved.
	// - If merge versions are empty, then it has not be resolved.
	// - If merge versions do not match, then it has not been resolved.
	if len(incomingHLV.MergeVersions) != 0 && len(localHLV.MergeVersions) != 0 && maps.Equal(incomingHLV.MergeVersions, localHLV.MergeVersions) {
		base.DebugfCtx(ctx, base.KeyVV, "merge versions match between local HLV %#v and incoming HLV %#v, conflict previously resolved", localHLV, incomingCV)
		return false, nil
	}
	return true, nil
}
