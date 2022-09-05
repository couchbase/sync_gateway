// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const (
	// comparableVersionEpoch can be incremented when the versioning system or string format changes, whilst maintaining ordering.
	// i.e. It's a version number version
	// e.g: version system change from semver to dates: 0:30.2.1@45-EE < 1:22-3-25@33-EE
	comparableVersionEpoch = 0
)

// ComparableVersion is an [epoch:]major.minor.patch[.other][@build][-edition] version that has methods to reliably extract information.
type ComparableVersion struct {
	epoch, major, minor, patch, other uint8
	build                             uint16
	edition                           productEdition
	strOnce                           sync.Once
	str                               string
}

var zeroComparableVersion = &ComparableVersion{
	epoch:   0,
	major:   0,
	minor:   0,
	patch:   0,
	other:   0,
	build:   0,
	edition: "",
}

// NewComparableVersionFromString parses a ComparableVersion from the given version string.
// Expected format: `[epoch:]major.minor.patch[.other][@build][-edition]`
func NewComparableVersionFromString(version string) (*ComparableVersion, error) {
	epoch, major, minor, patch, other, build, edition, err := parseComparableVersion(version)
	if err != nil {
		return nil, err
	}
	return &ComparableVersion{
		epoch:   epoch,
		major:   major,
		minor:   minor,
		patch:   patch,
		other:   other,
		build:   build,
		edition: edition,
	}, nil
}

func NewComparableVersion(majorStr, minorStr, patchStr, otherStr, buildStr, editionStr string) (*ComparableVersion, error) {
	_, major, minor, patch, other, build, edition, err := parseComparableVersionComponents("", majorStr, minorStr, patchStr, otherStr, buildStr, editionStr)
	if err != nil {
		return nil, err
	}
	return &ComparableVersion{
		epoch:   comparableVersionEpoch,
		major:   major,
		minor:   minor,
		patch:   patch,
		other:   other,
		build:   build,
		edition: edition,
	}, nil
}

// Equal returns true if pv is equal to b
func (pv *ComparableVersion) Equal(b *ComparableVersion) bool {
	return pv.epoch == b.epoch &&
		pv.major == b.major &&
		pv.minor == b.minor &&
		pv.patch == b.patch &&
		pv.other == b.other &&
		pv.build == b.build &&
		pv.edition == b.edition
}

// Less returns true if a is less than b
func (a *ComparableVersion) Less(b *ComparableVersion) bool {
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

func (pv *ComparableVersion) String() string {
	pv.strOnce.Do(func() {
		pv.str = pv.formatComparableVersion()
	})
	return pv.str
}

// MarshalJSON implements json.Marshaler for ComparableVersion. The JSON representation is the version string.
func (pv *ComparableVersion) MarshalJSON() ([]byte, error) {
	return JSONMarshal(pv.String())
}

func (pv *ComparableVersion) UnmarshalJSON(val []byte) error {
	var strVal string
	err := JSONUnmarshal(val, &strVal)
	if err != nil {
		return err
	}
	pv.epoch, pv.major, pv.minor, pv.patch, pv.other, pv.build, pv.edition, err = parseComparableVersion(strVal)
	return err
}

const (
	comparableVersionSep        = '.'
	comparableVersionSepEpoch   = ':'
	comparableVersionSepBuild   = '@'
	comparableVersionSepEdition = '-'
)

// formatComparableVersion returns the string representation of the given version.
// format: `[epoch:]major.minor.patch[.other][@build][-edition]`
func (pv *ComparableVersion) formatComparableVersion() string {
	if pv == nil {
		return "0.0.0"
	}

	epochStr := ""
	if pv.epoch > 0 {
		epochStr = strconv.FormatUint(uint64(pv.epoch), 10) + string(comparableVersionSepEpoch)
	}

	semverStr := strconv.FormatUint(uint64(pv.major), 10) +
		string(comparableVersionSep) +
		strconv.FormatUint(uint64(pv.minor), 10) +
		string(comparableVersionSep) +
		strconv.FormatUint(uint64(pv.patch), 10)

	otherStr := ""
	if pv.other > 0 {
		otherStr = string(comparableVersionSep) +
			strconv.FormatUint(uint64(pv.other), 10)
	}

	buildStr := ""
	if pv.build > 0 {
		buildStr = string(comparableVersionSepBuild) + strconv.FormatUint(uint64(pv.build), 10)
	}

	editionStr := ""
	if ed := pv.edition.String(); ed != "" {
		editionStr = string(comparableVersionSepEdition) + ed
	}

	return epochStr + semverStr + otherStr + buildStr + editionStr
}

func parseComparableVersion(version string) (epoch, major, minor, patch, other uint8, build uint16, edition productEdition, err error) {
	epochStr, majorStr, minorStr, patchStr, otherStr, buildStr, edtionStr, err := extractComparableVersionComponents(version)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, "", err
	}
	return parseComparableVersionComponents(epochStr, majorStr, minorStr, patchStr, otherStr, buildStr, edtionStr)
}

func parseComparableVersionComponents(epochStr, majorStr, minorStr, patchStr, otherStr, buildStr, editionStr string) (epoch, major, minor, patch, other uint8, build uint16, edition productEdition, err error) {
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

// extractComparableVersionComponents takes a version string and returns each component as a string
func extractComparableVersionComponents(version string) (epoch, major, minor, patch, other, build, edition string, err error) {

	var remainder string

	// The repeated Cuts look inefficient, but is faster and lower alloc than something like strings.Split,
	// and still iterating over the entire string only once, albeit in small chunks.

	// prefixes
	epoch, remainder = safeCutBefore(version, string(comparableVersionSepEpoch))

	// suffixes
	edition, remainder = safeCutAfter(remainder, string(comparableVersionSepEdition))
	build, remainder = safeCutAfter(remainder, string(comparableVersionSepBuild))

	// major.minor.patch[.other]
	major, remainder = safeCutBefore(remainder, string(comparableVersionSep))
	minor, remainder = safeCutBefore(remainder, string(comparableVersionSep))

	// handle optional [.other]
	if before, after, ok := strings.Cut(remainder, string(comparableVersionSep)); !ok {
		patch = remainder
	} else {
		patch = before
		other = after
	}

	if major == "" || minor == "" || patch == "" {
		return "", "", "", "", "", "", "", errors.New("version requires at least major.minor.patch components")
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
