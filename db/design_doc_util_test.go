// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
)

// setDesignDocPreviousVersionsForTest sets the previous versions of the design docs for testing purposes and reverts to the original set once the test is done.
func setDesignDocPreviousVersionsForTest(t testing.TB, versions ...string) {
	original := DesignDocPreviousVersions
	t.Cleanup(func() {
		DesignDocPreviousVersions = original
	})
	DesignDocPreviousVersions = versions
}

// assertDesignDocExists ensures that the design doc exists in the dataStore.
func assertDesignDocExists(t testing.TB, viewStore sgbucket.ViewStore, ddocName string) bool {
	_, err := viewStore.GetDDoc(ddocName)
	return assert.NoErrorf(t, err, "Design doc %s should exist but got an error fetching it: %v", ddocName, err)
}

// assertDesignDocDoesNotExist ensures that the design doc does not exist in the dataStore.
func assertDesignDocNotExists(t testing.TB, viewStore sgbucket.ViewStore, ddocName string) bool {
	ddoc, err := viewStore.GetDDoc(ddocName)
	if err == nil {
		return assert.Failf(t, "Design doc %s should not exist but but it did: %v", ddocName, ddoc)
	}
	return assert.Truef(t, IsMissingDDocError(err), "Design doc %s should not exist but got a different error fetching it: %v", ddocName, err)
}
