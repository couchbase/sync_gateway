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
	"fmt"
	"strings"
)

const (
	ProductName          = "Couchbase Sync Gateway"
	ProductVersionNumber = "3.1" // API/feature level
)

var (
	// VersionString appears in the "Server:" header of HTTP responses.
	// CBL 1.x parses the header to determine whether it's talking to Sync Gateway (vs. CouchDB) and what version.
	// This determines what replication API features it will use (E.g: bulk get and POST _changes that are CB-Mobile only features.)
	VersionString string

	// LongVersionString includes build number; appears in the response of "GET /" and the initial log message
	LongVersionString string

	// ProductNameString comes from Gerrit (jenkins builds) or Git (dev builds)
	ProductNameString string
)

func init() {
	// Placeholders substituted by Jenkins build
	const (
		buildPlaceholderServerName               = "@PRODUCT_NAME@"
		buildPlaceholderVersionBuildNumberString = "@PRODUCT_VERSION@"
		buildPlaceholderVersionCommitSHA         = "@COMMIT_SHA@"
	)

	// Use build info if available, otherwise use git info to populate instead.
	if buildPlaceholderVersionBuildNumberString[0] != '@' {
		// Split version number and build number (optional)
		versionTokens := strings.Split(buildPlaceholderVersionBuildNumberString, "-")
		BuildVersionString := versionTokens[0]
		var BuildNumberString string
		if len(versionTokens) > 1 {
			BuildNumberString = fmt.Sprintf("%s;", versionTokens[1])
		}
		LongVersionString = fmt.Sprintf("%s/%s(%s%.7s) %s", buildPlaceholderServerName, BuildVersionString, BuildNumberString, buildPlaceholderVersionCommitSHA, productEditionShortName)
		VersionString = fmt.Sprintf("%s/%s %s", buildPlaceholderServerName, BuildVersionString, productEditionShortName)
		ProductNameString = buildPlaceholderServerName
	} else {
		// GitProductName is set via the build script, but may not be set when unit testing.
		productName := GitProductName
		if productName == "" {
			productName = ProductName
		}
		LongVersionString = fmt.Sprintf("%s/%s(%.7s%s) %s", productName, GitBranch, GitCommit, GitDirty, productEditionShortName)
		VersionString = fmt.Sprintf("%s/%s branch/%s commit/%.7s%s %s", productName, ProductVersionNumber, GitBranch, GitCommit, GitDirty, productEditionShortName)
		ProductNameString = productName
	}
}

// IsEnterpriseEdition returns true if this Sync Gateway node is enterprise edition.
// This can be used to restrict config options, etc. at runtime. This should not be
// used as a conditional around private/EE-only code, as CE builds will fail to compile.
// Use the cb_sg_enterprise build tag for conditional compilation instead.
func IsEnterpriseEdition() bool {
	return productEditionEnterprise == true
}
