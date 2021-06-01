/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"regexp"
	"strconv"
)

// userAgentRegexp is a regular expression that matches a CBL user-agent
// A word
// A forward slash
// A (captured) series of numbers -- major version
// A dot (needs to be escaped)
// A (captured) series of numbers -- minor version
// A bunch of whatever (.*)
var userAgentRegexp = regexp.MustCompile(`CouchbaseLite/([0-9]*)\.([0-9]*).*`)

type UserAgentVersion struct {
	requestHeader string
	majorVersion  int
	minorVersion  int
}

func NewUserAgentVersion(requestHeader string) *UserAgentVersion {
	userAgentVersion := UserAgentVersion{
		requestHeader: requestHeader,
	}
	userAgentVersion.parse()
	return &userAgentVersion
}

// Extract the major and minor version from the request header string
func (uav *UserAgentVersion) parse() {

	if uav.requestHeader == "" {
		return
	}

	// Find match and extract groups
	result := userAgentRegexp.FindStringSubmatch(uav.requestHeader)

	// We should have at least three things captured
	if len(result) < 3 {
		return
	}

	// The first result is the entire string that matched, ignore it
	_ = result[0]

	// Get the first group
	majorVersionStr := result[1]
	if majorVersionStr == "" {
		return
	}
	majorVersion, err := strconv.Atoi(majorVersionStr)
	if err != nil {
		return
	}
	uav.majorVersion = majorVersion

	// Get the second group
	minorVersionStr := result[2]
	if minorVersionStr == "" {
		return
	}
	minorVersion, err := strconv.Atoi(minorVersionStr)
	if err != nil {
		return
	}
	uav.minorVersion = minorVersion

}

func (uav UserAgentVersion) MajorVersion() int {
	return uav.majorVersion
}

func (uav UserAgentVersion) MinorVersion() int {
	return uav.minorVersion
}

func (uav UserAgentVersion) IsEqualToOrAfter(otherMajorVersion, otherMinorVersion int) bool {

	// if our major version is less than their major version, then there's no
	// way we could be after their overall version, so return false
	if uav.MajorVersion() < otherMajorVersion {
		return false
	}

	// if our major version is greater than their major version, we're definitely
	// after so return true
	if uav.MajorVersion() > otherMajorVersion {
		return true
	}

	// the major versions are equal, so it's down to minor versions.
	// if we're strictly after their minor version, return true
	if uav.MinorVersion() >= otherMinorVersion {
		return true
	}

	// looks like we're less than their minor version, so return false
	return false

}

func (uav UserAgentVersion) IsBefore(otherMajorVersion, otherMinorVersion int) bool {
	return !uav.IsEqualToOrAfter(otherMajorVersion, otherMinorVersion)
}
