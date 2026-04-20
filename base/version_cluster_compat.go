// Copyright 2026-Present Couchbase, Inc.
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

// ClusterCompatVersion represents a major.minor version used for cluster compatibility gating.
// Only major and minor components are tracked — patch, build, and edition are intentionally
// omitted since cluster compat decisions only care about feature-level version boundaries.
type ClusterCompatVersion struct {
	Major uint8 `json:"major"`
	Minor uint8 `json:"minor"`
}

// NewClusterCompatVersion creates a ClusterCompatVersion from major and minor components.
func NewClusterCompatVersion(major, minor uint8) ClusterCompatVersion {
	return ClusterCompatVersion{Major: major, Minor: minor}
}

// AtLeast returns true if this version is greater than or equal to the given major.minor.
func (v ClusterCompatVersion) AtLeast(major, minor uint8) bool {
	if v.Major != major {
		return v.Major > major
	}
	return v.Minor >= minor
}

// MinClusterCompatVersion returns the minimum version from the given versions.
// Returns the zero value if no versions are provided.
func MinClusterCompatVersion(versions ...ClusterCompatVersion) ClusterCompatVersion {
	if len(versions) == 0 {
		return ClusterCompatVersion{}
	}
	min := versions[0]
	for _, v := range versions[1:] {
		if clusterCompatVersionLess(v, min) {
			min = v
		}
	}
	return min
}

// clusterCompatVersionLess returns true if a is strictly less than b.
func clusterCompatVersionLess(a, b ClusterCompatVersion) bool {
	if a.Major != b.Major {
		return a.Major < b.Major
	}
	return a.Minor < b.Minor
}

// String returns the "major.minor" string representation.
func (v ClusterCompatVersion) String() string {
	return strconv.FormatUint(uint64(v.Major), 10) + "." + strconv.FormatUint(uint64(v.Minor), 10)
}

// MarshalJSON serializes ClusterCompatVersion as a "major.minor" string.
func (v ClusterCompatVersion) MarshalJSON() ([]byte, error) {
	return JSONMarshal(v.String())
}

// UnmarshalJSON deserializes ClusterCompatVersion from a "major.minor" string.
func (v *ClusterCompatVersion) UnmarshalJSON(data []byte) error {
	var s string
	if err := JSONUnmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := ParseClusterCompatVersion(s)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}

// ClusterCompatChecker provides read-only access to the cluster compatibility version.
type ClusterCompatChecker interface {
	ClusterCompatVersion() *ClusterCompatVersion
	ClusterIsAtLeast(major, minor uint8) bool
	// NodeVersions returns the cluster compat version of each node in the cluster, keyed by node UUID.
	// This is the union of nodes across all bucket registries.
	NodeVersions() map[string]ClusterCompatVersion
}

// ParseClusterCompatVersion parses a "major.minor" string into a ClusterCompatVersion.
func ParseClusterCompatVersion(s string) (ClusterCompatVersion, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return ClusterCompatVersion{}, fmt.Errorf("invalid cluster compat version %q: expected major.minor", s)
	}
	major, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil {
		return ClusterCompatVersion{}, fmt.Errorf("invalid major version in %q: %w", s, err)
	}
	minor, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return ClusterCompatVersion{}, fmt.Errorf("invalid minor version in %q: %w", s, err)
	}
	return ClusterCompatVersion{Major: uint8(major), Minor: uint8(minor)}, nil
}
