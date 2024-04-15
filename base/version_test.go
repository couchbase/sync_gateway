// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPRODUCT_VERSIONAPIEquality tests that the stamped "PRODUCT_VERSION" is equal to the manually-specified API version constants.
func TestPRODUCT_VERSIONAPIEquality(t *testing.T) {
	printVersionInformation(t)

	// don't bother checking if this checkout hasn't been stamped with a version (only the build server does this as it needs to know the job/build number)
	if hasStampedVersion := buildPlaceholderVersionBuildNumberString[0] != '@'; !hasStampedVersion {
		t.Skipf("This build has not been stamped with a version - skipping test")
	}

	_, major, minor, _, _, _ := parseBuildPlaceholderVersionBuildNumberString(buildPlaceholderVersionBuildNumberString)
	assert.Equal(t, ProductAPIVersionMajor, major, "buildPlaceholderVersionBuildNumberString should match ProductAPIVersion const")
	assert.Equal(t, ProductAPIVersionMinor, minor, "buildPlaceholderVersionBuildNumberString should match ProductAPIVersion const")
}

func printVersionInformation(tb testing.TB) {
	tb.Logf("ProductName: %s", ProductName)
	tb.Logf("ProductVersion: %v", ProductVersion)
	tb.Logf("---")
	tb.Logf("ProductAPIVersion: %s", ProductAPIVersion)
	tb.Logf("---")
	tb.Logf("VersionString: %s", VersionString)
	tb.Logf("LongVersionString: %s", LongVersionString)
	tb.Logf("ProductNameString: %s", ProductNameString)
	tb.Logf("---")
	tb.Logf("buildPlaceholderServerName: %s", buildPlaceholderServerName)
	tb.Logf("buildPlaceholderVersionBuildNumberString: %s", buildPlaceholderVersionBuildNumberString)
	tb.Logf("buildPlaceholderVersionCommitSHA: %s", buildPlaceholderVersionCommitSHA)
}
