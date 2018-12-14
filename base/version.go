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
		LongVersionString = fmt.Sprintf("%s/%s(%s%.7s) %s",
			ServerName, BuildVersionString, BuildNumberString, VersionCommitSHA, productEditionShortName)

		VersionString = fmt.Sprintf("%s/%s %s", ServerName, BuildVersionString, productEditionShortName)
		ProductName = ServerName
	} else {
		LongVersionString = fmt.Sprintf("%s/%s(%.7s%s) %s", GitProductName, GitBranch, GitCommit, GitDirty, productEditionShortName)
		VersionString = fmt.Sprintf("%s/%s branch/%s commit/%.7s%s %s", GitProductName, VersionNumber, GitBranch, GitCommit, GitDirty, productEditionShortName)
		ProductName = GitProductName
	}
}

// IsEnterpriseEdition returns true if this Sync Gateway node is enterprise edition. This can be used to restrict config options, etc. at runtime.
// This should not be used as a condtional around private/EE-only code, as CE builds will fail to compile. Use the build tag for conditional compilation instead.
func IsEnterpriseEdition() bool {
	return productEditionEnterprise == true
}
