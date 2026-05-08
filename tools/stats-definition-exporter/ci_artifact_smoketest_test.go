// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included
// in the file licenses/APL2.txt.

package main

import "testing"

// TestCIArtifactUploadSmoke is an intentional failure used to verify that
// the new "Upload test logs artifact" CI step produces a downloadable
// test.json artifact on failure. REVERT BEFORE MERGE.
func TestCIArtifactUploadSmoke(t *testing.T) {
	t.Fatal("intentional failure to verify CI artifact upload — see .github/workflows/ci.yml 'Upload test logs artifact' step")
}
