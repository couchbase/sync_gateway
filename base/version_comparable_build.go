// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// comparableBuildVersionEpoch can be incremented when the versioning system or string format changes, whilst maintaining ordering.
	// i.e. It's a version number version
	// e.g: version system change from semver to dates: 0:30.2.1@45-EE < 1:22-3-25@33-EE
	comparableBuildVersionEpoch = 0
)

// ComparableBuildVersion is an [epoch:]major.minor.patch[.other][@build][-edition] version that has methods to reliably extract information.
type ComparableBuildVersion struct {
	epoch, major, minor, patch, other uint8
	build                             uint16
	edition                           productEdition
	str                               string
}

func zeroComparableBuildVersion() *ComparableBuildVersion {
	v := &ComparableBuildVersion{
		epoch:   0,
		major:   0,
		minor:   0,
		patch:   0,
		other:   0,
		build:   0,
		edition: "",
	}
	v.str = v.formatComparableBuildVersion()
	return v
}

// NewComparableBuildVersionFromString parses a ComparableBuildVersion from the given version string.
// Expected format: `[epoch:]major.minor.patch[.other][@build][-edition]`
func NewComparableBuildVersionFromString(version string) (*ComparableBuildVersion, error) {
	epoch, major, minor, patch, other, build, edition, err := parseComparableBuildVersion(version)
	if err != nil {
		return nil, err
	}
	v := &ComparableBuildVersion{
		epoch:   epoch,
		major:   major,
		minor:   minor,
		patch:   patch,
		other:   other,
		build:   build,
		edition: edition,
	}
	v.str = v.formatComparableBuildVersion()
	if v.str != version {
		return nil, fmt.Errorf("version string %q is not equal to formatted version string %q", version, v.str)
	}
	return v, nil
}

func NewComparableBuildVersion(majorStr, minorStr, patchStr, otherStr, buildStr, editionStr string) (*ComparableBuildVersion, error) {
	_, major, minor, patch, other, build, edition, err := parseComparableBuildVersionComponents("", majorStr, minorStr, patchStr, otherStr, buildStr, editionStr)
	if err != nil {
		return nil, err
	}
	v := &ComparableBuildVersion{
		epoch:   comparableBuildVersionEpoch,
		major:   major,
		minor:   minor,
		patch:   patch,
		other:   other,
		build:   build,
		edition: edition,
	}
	v.str = v.formatComparableBuildVersion()
	return v, nil
}

// Equal returns true if pv is equal to b
func (pv *ComparableBuildVersion) Equal(b *ComparableBuildVersion) bool {
	return pv.epoch == b.epoch &&
		pv.major == b.major &&
		pv.minor == b.minor &&
		pv.patch == b.patch &&
		pv.other == b.other &&
		pv.build == b.build &&
		pv.edition == b.edition
}

// Less returns true if a is less than b
func (a *ComparableBuildVersion) Less(b *ComparableBuildVersion) bool {
	if a.epoch < b.epoch {
		return true
	} else if a.epoch > b.epoch {
		return false
	}
	if a.major < b.major {
		return true
	} else if a.major > b.major {
		return false
	}
	if a.minor < b.minor {
		return true
	} else if a.minor > b.minor {
		return false
	}
	if a.patch < b.patch {
		return true
	} else if a.patch > b.patch {
		return false
	}
	if a.other < b.other {
		return true
	} else if a.other > b.other {
		return false
	}
	if a.build < b.build {
		return true
	} else if a.build > b.build {
		return false
	}
	if a.edition < b.edition {
		return true
	} else if a.edition > b.edition {
		return false
	}

	// versions are equal
	return false
}

// AtLeastMinorDowngrade returns true there is a major or minor downgrade from a to b.
func (a *ComparableBuildVersion) AtLeastMinorDowngrade(b *ComparableBuildVersion) bool {
	if a.epoch != b.epoch {
		return a.epoch > b.epoch
	}
	if a.major != b.major {
		return a.major > b.major
	}
	return a.minor > b.minor
}

func (pv ComparableBuildVersion) String() string {
	return pv.str
}

// MarshalJSON implements json.Marshaler for ComparableBuildVersion. The JSON representation is the version string.
func (pv *ComparableBuildVersion) MarshalJSON() ([]byte, error) {
	return JSONMarshal(pv.String())
}

func (pv *ComparableBuildVersion) UnmarshalJSON(val []byte) error {
	var strVal string
	err := JSONUnmarshal(val, &strVal)
	if err != nil {
		return err
	}
	if strVal != "" {
		pv.epoch, pv.major, pv.minor, pv.patch, pv.other, pv.build, pv.edition, err = parseComparableBuildVersion(strVal)
	}

	pv.str = pv.formatComparableBuildVersion()
	return err
}

const (
	comparableBuildVersionSep        = '.'
	comparableBuildVersionSepEpoch   = ':'
	comparableBuildVersionSepBuild   = '@'
	comparableBuildVersionSepEdition = '-'
)

// formatComparableBuildVersion returns the string representation of the given version.
// format: `[epoch:]major.minor.patch[.other][@build][-edition]`
func (pv *ComparableBuildVersion) formatComparableBuildVersion() string {
	if pv == nil {
		return "0.0.0"
	}

	epochStr := ""
	if pv.epoch > 0 {
		epochStr = strconv.FormatUint(uint64(pv.epoch), 10) + string(comparableBuildVersionSepEpoch)
	}

	semverStr := strconv.FormatUint(uint64(pv.major), 10) +
		string(comparableBuildVersionSep) +
		strconv.FormatUint(uint64(pv.minor), 10) +
		string(comparableBuildVersionSep) +
		strconv.FormatUint(uint64(pv.patch), 10)

	otherStr := ""
	if pv.other > 0 {
		otherStr = string(comparableBuildVersionSep) +
			strconv.FormatUint(uint64(pv.other), 10)
	}

	buildStr := ""
	if pv.build > 0 {
		buildStr = string(comparableBuildVersionSepBuild) + strconv.FormatUint(uint64(pv.build), 10)
	}

	editionStr := ""
	if ed := pv.edition.String(); ed != "" {
		editionStr = string(comparableBuildVersionSepEdition) + ed
	}

	return epochStr + semverStr + otherStr + buildStr + editionStr
}

func parseComparableBuildVersion(version string) (epoch, major, minor, patch, other uint8, build uint16, edition productEdition, err error) {
	epochStr, majorStr, minorStr, patchStr, otherStr, buildStr, edtionStr, err := extractComparableBuildVersionComponents(version)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, "", err
	}
	return parseComparableBuildVersionComponents(epochStr, majorStr, minorStr, patchStr, otherStr, buildStr, edtionStr)
}

func parseComparableBuildVersionComponents(epochStr, majorStr, minorStr, patchStr, otherStr, buildStr, editionStr string) (epoch, major, minor, patch, other uint8, build uint16, edition productEdition, err error) {
	if epochStr != "" {
		tmp, err := strconv.ParseUint(epochStr, 10, 8)
		if err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version epoch: %q: %w", epochStr, err)
		}
		epoch = uint8(tmp)
	}

	if majorStr != "" {
		tmp, err := strconv.ParseUint(majorStr, 10, 8)
		if err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version major: %q: %w", majorStr, err)
		}
		major = uint8(tmp)
	}

	if minorStr != "" {
		tmp, err := strconv.ParseUint(minorStr, 10, 8)
		if err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version minor: %q: %w", minorStr, err)
		}
		minor = uint8(tmp)
	}

	if patchStr != "" {
		tmp, err := strconv.ParseUint(patchStr, 10, 8)
		if err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version patch: %q: %w", patchStr, err)
		}
		patch = uint8(tmp)
	}

	if otherStr != "" {
		tmp, err := strconv.ParseUint(otherStr, 10, 8)
		if err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version other: %q: %w", otherStr, err)
		}
		other = uint8(tmp)
	}

	if buildStr != "" {
		tmp, err := strconv.ParseUint(buildStr, 10, 16)
		if err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version build: %q: %w", buildStr, err)
		}
		build = uint16(tmp)
	}

	if editionStr != "" {
		if err := isValidProductEdition(editionStr); err != nil {
			return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("couldn't parse version edition: %q: %w", editionStr, err)
		}
		edition = productEdition(editionStr)
	}

	return epoch, major, minor, patch, other, build, edition, nil
}

// extractComparableBuildVersionComponents takes a version string and returns each component as a string
func extractComparableBuildVersionComponents(version string) (epoch, major, minor, patch, other, build, edition string, err error) {

	var remainder string

	// The repeated Cuts look inefficient, but is faster and lower alloc than something like strings.Split,
	// and still iterating over the entire string only once, albeit in small chunks.

	// prefixes
	epoch, remainder = safeCutBefore(version, string(comparableBuildVersionSepEpoch))

	// suffixes
	edition, remainder = safeCutAfter(remainder, string(comparableBuildVersionSepEdition))
	build, remainder = safeCutAfter(remainder, string(comparableBuildVersionSepBuild))

	// major.minor.patch[.other]
	major, remainder = safeCutBefore(remainder, string(comparableBuildVersionSep))
	minor, remainder = safeCutBefore(remainder, string(comparableBuildVersionSep))

	// handle optional [.other]
	if before, after, ok := strings.Cut(remainder, string(comparableBuildVersionSep)); !ok {
		patch = remainder
	} else {
		patch = before
		other = after
	}

	if major == "" || minor == "" || patch == "" {
		return "", "", "", "", "", "", "", fmt.Errorf("version %q requires at least major.minor.patch components", version)
	}

	return epoch, major, minor, patch, other, build, edition, nil
}

type productEdition string

const (
	ProductEditionEE = "EE" // Enterprise Edition
	ProductEditionCE = "CE" // Community Edition
)

func isValidProductEdition(s string) error {
	switch s {
	case ProductEditionEE, ProductEditionCE:
		return nil
	}
	return fmt.Errorf("unknown edition: %q", s)
}

func (pe *productEdition) String() string {
	if pe == nil {
		return ""
	}
	return string(*pe)
}
