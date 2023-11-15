/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"
	"strings"
)

const (
	ProductName = "Couchbase Sync Gateway"

	ProductAPIVersionMajor = "4"
	ProductAPIVersionMinor = "0"
	ProductAPIVersion      = ProductAPIVersionMajor + "." + ProductAPIVersionMinor
)

// populated via init() below
var (
	// ProductVersion describes the specific version information of the build.
	ProductVersion *ComparableBuildVersion

	// VersionString appears in the "Server:" header of HTTP responses.
	// CBL 1.x parses the header to determine whether it's talking to Sync Gateway (vs. CouchDB) and what version.
	// This determines what replication API features it will use (E.g: bulk get and POST _changes that are CB-Mobile only features.)
	VersionString string

	// LongVersionString includes build number; appears in the response of "GET /" and the initial log message
	LongVersionString string

	// ProductNameString comes from Gerrit (jenkins builds) or Git (dev builds); is probably "Couchbase Sync Gateway"
	ProductNameString string
)

// substituted by Jenkins build scripts
const (
	buildPlaceholderServerName               = "@PRODUCT_NAME@"    // e.g: Couchbase Sync Gateway
	buildPlaceholderVersionBuildNumberString = "@PRODUCT_VERSION@" // e.g: 2.8.2-1
	buildPlaceholderVersionCommitSHA         = "@COMMIT_SHA@"      // e.g: 4df7a2d
)

func init() {

	var (
		majorStr, minorStr, patchStr, otherStr, buildStr, editionStr string
	)

	// Use build info if available, otherwise use git info to populate instead.
	if buildPlaceholderVersionBuildNumberString[0] != '@' {
		var versionString string
		versionString, majorStr, minorStr, patchStr, otherStr, buildStr = parseBuildPlaceholderVersionBuildNumberString(buildPlaceholderVersionBuildNumberString)
		var formattedBuildStr string
		if buildStr != "" {
			formattedBuildStr = buildStr + ";"
		}

		productName := buildPlaceholderServerName

		// E.g: Couchbase Sync Gateway/2.8.2(1;4df7a2d) EE
		LongVersionString = fmt.Sprintf("%s/%s(%s%.7s) %s", productName, versionString, formattedBuildStr, buildPlaceholderVersionCommitSHA, productEditionShortName)
		VersionString = fmt.Sprintf("%s/%s %s", productName, versionString, productEditionShortName)
		ProductNameString = productName
	} else {
		// no patch version available for git-based version info
		majorStr = ProductAPIVersionMajor
		minorStr = ProductAPIVersionMinor

		// GitProductName is set via the build script, but may not be set when unit testing.
		productName := GitProductName
		if productName == "" {
			productName = ProductName
		}

		// E.g: Couchbase Sync Gateway/CBG-1914(6282c1c+CHANGES) CE
		LongVersionString = fmt.Sprintf("%s/%s(%.7s%s) %s", productName, GitBranch, GitCommit, GitDirty, productEditionShortName)
		VersionString = fmt.Sprintf("%s/%s branch/%s commit/%.7s%s %s", productName, ProductAPIVersion, GitBranch, GitCommit, GitDirty, productEditionShortName)
		ProductNameString = productName
	}

	editionStr = productEditionShortName

	var err error
	ProductVersion, err = NewComparableBuildVersion(majorStr, minorStr, patchStr, otherStr, buildStr, editionStr)
	if err != nil {
		panic(err)
	}
}

func parseBuildPlaceholderVersionBuildNumberString(s string) (rawVersion, major, minor, patch, other, build string) {
	// Split version number and build number (optional)
	versionAndBuild := strings.Split(s, "-")

	rawVersion = versionAndBuild[0]
	versions := strings.Split(rawVersion, ".")
	if len(versions) == 0 || len(versions) > 4 {
		PanicfCtx(context.Background(), "unknown version format (expected major.minor.patch[.other]) got %v", rawVersion)
	}

	if len(versions) > 0 {
		major = versions[0]
	}
	if len(versions) > 1 {
		minor = versions[1]
	}
	if len(versions) > 2 {
		patch = versions[2]
	}
	if len(versions) > 3 {
		other = versions[3]
	}

	if len(versionAndBuild) > 1 {
		build = versionAndBuild[1]
	}

	return rawVersion, major, minor, patch, other, build
}

// IsEnterpriseEdition returns true if this Sync Gateway node is enterprise edition.
// This can be used to restrict config options, etc. at runtime. This should not be
// used as a conditional around private/EE-only code, as CE builds will fail to compile.
// Use the cb_sg_enterprise build tag for conditional compilation instead.
func IsEnterpriseEdition() bool {
	return productEditionEnterprise == true
}
