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
	"time"
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

// GreaterThan returns true if v is strictly greater than other.
func (v ClusterCompatVersion) GreaterThan(other ClusterCompatVersion) bool {
	return clusterCompatVersionLess(other, v)
}

// IsZero reports whether v is the zero value (Major == 0 && Minor == 0), which is how an
// unset / never-ratcheted version is represented (e.g. a fresh GatewayRegistry before any
// node has registered).
func (v ClusterCompatVersion) IsZero() bool {
	return v.Major == 0 && v.Minor == 0
}

// MinClusterCompatVersion returns the minimum version from the given versions.
// Returns the zero value if no versions are provided.
func MinClusterCompatVersion(versions ...ClusterCompatVersion) ClusterCompatVersion {
	if len(versions) == 0 {
		return ClusterCompatVersion{}
	}
	minVersion := versions[0]
	for _, v := range versions[1:] {
		if clusterCompatVersionLess(v, minVersion) {
			minVersion = v
		}
	}
	return minVersion
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

// RegistryNode represents a single Sync Gateway node's version registration in the cluster.
// HeartbeatAt is rewritten on every register call; entries whose HeartbeatAt is older than
// the configured node heartbeat expiry are pruned by RegisterNodeVersion.
// ConfigGroupID each SGW node belongs to one config group, single valued per RegistryNode
// Databases keyed by db name, value is the applied DatabaseConfig.Version. Stamped on every RegisterNodeVersion call from
// the writing node's appliedDBVersions map
type RegistryNode struct {
	Version       ClusterCompatVersion `json:"version"`
	HeartbeatAt   time.Time            `json:"heartbeat_at"`
	ConfigGroupID string               `json:"config_group_id,omitempty"`
	Databases     map[string]string    `json:"databases,omitempty"`
}

// RegistryFreeze records an admin-issued cluster compatibility version freeze stored in a
// bucket registry. When present, the cluster compatibility version reported by Sync Gateway is
// pinned to Version, preventing it from advancing as nodes are upgraded.
type RegistryFreeze struct {
	Version  ClusterCompatVersion `json:"version"`
	FrozenAt time.Time            `json:"frozen_at"`
}

// PreSGNodeVersionFallback is the cluster compat version stamped onto an observation of a
// peer found in SGRCluster.Nodes whose SGNode entry doesn't carry a Version field — i.e. a
// pre-4.1 peer (SGNode.Version was introduced in 4.1). Capping at 4.0 reflects what we know:
// the peer participates in ISGR but predates the version-stamping field. When the side-channel
// does carry a parseable version, that observed version is used directly instead.
//
// Different from preCbgtExtrasVersion (3.0) because each side-channel started carrying
// version info in a different release: cbgt extras since 3.1, SGNode.Version since 4.1.
var PreSGNodeVersionFallback = NewClusterCompatVersion(4, 0)

// RegistryPreCCVAwareNode represents an observation of a pre-CCV-aware Sync Gateway peer by a CCV-
// aware node. Unlike RegistryNode, these entries are written by an observing peer, not by the
// pre-CCV-aware peer itself, and are keyed in GatewayRegistry.PreCCVAwareNodes by the pre-CCV-aware peer's
// UUID (which is therefore not duplicated inside the entry body). Observations of the same
// peer through multiple side-channels collapse onto a single entry with the lower-versioned
// reading winning. LastObservedAt plays the same role as RegistryNode.HeartbeatAt: entries
// older than the configured heartbeat expiry are pruned by RegisterNodeVersion.
//
// Version is the pre-CCV-aware peer's cluster compat version (major.minor). The observer parses
// the side-channel's full build version at observation time and stores only the major.minor —
// CCV decisions don't need higher precision and storing the build string would imply
// precision the observation can't guarantee. When the side-channel carried no version,
// Version is set to PreSGNodeVersionFallback as a conservative fallback at observation time.
type RegistryPreCCVAwareNode struct {
	Version        ClusterCompatVersion `json:"version"`
	LastObservedAt time.Time            `json:"last_observed_at"`
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
