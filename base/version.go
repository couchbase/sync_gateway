package base

import (
	"fmt"
	"strings"
)

const ServerName = "@PRODUCT_NAME@"                  // DO NOT CHANGE; clients check this
const VersionNumber = "2.5"                          // API/feature level
const VersionBuildNumberString = "@PRODUCT_VERSION@" // Real string substituted by Jenkins build
const VersionCommitSHA = "@COMMIT_SHA@"              // Real string substituted by Jenkins build

// This appears in the "Server:" header of HTTP responses.
// This should be changed only very cautiously, because Couchbase Lite parses the header value
// to determine whether it's talking to Sync Gateway (vs. CouchDB) and what version. This in turn
// determines what replication API features it will use.
var VersionString string

// This includes build number; appears in the response of "GET /" and the initial log message
var LongVersionString string

// Either comes from Gerrit (jenkins builds) or Git (dev builds)
var ProductName string

func init() {
	if VersionBuildNumberString[0] != '@' {
		//Split version number and build number (optional)
		versionTokens := strings.Split(VersionBuildNumberString, "-")
		BuildVersionString := versionTokens[0]
		var BuildNumberString string
		if len(versionTokens) > 1 {
			BuildNumberString = fmt.Sprintf("%s;", versionTokens[1])
		}
		LongVersionString = fmt.Sprintf("%s %s/%s(%s%.7s)",
			ServerName, productEditionShortName, BuildVersionString, BuildNumberString, VersionCommitSHA)

		VersionString = fmt.Sprintf("%s %s/%s", ServerName, productEditionShortName, BuildVersionString)
		ProductName = ServerName
	} else {
		LongVersionString = fmt.Sprintf("%s %s/%s(%.7s%s)", GitProductName, productEditionShortName, GitBranch, GitCommit, GitDirty)
		VersionString = fmt.Sprintf("%s %s/%s branch/%s commit/%.7s%s", GitProductName, productEditionShortName, VersionNumber, GitBranch, GitCommit, GitDirty)
		ProductName = GitProductName
	}
}

// IsEnterpriseEdition returns true if this Sync Gateway node is enterprise edition.
func IsEnterpriseEdition() bool {
	return productEditionEnterprise == true
}
