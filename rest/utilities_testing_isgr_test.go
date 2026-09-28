// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestHLVEqualAllowingEncodedRevs(t *testing.T) {
	const chainRev, otherRev = "2-abcdef1234", "1-9876543210"
	// "1-initial" is a fake revTreeID with no hex digest, which has no encoded version and must not stop the comparison
	revChain := []string{chainRev, "1-initial"}

	encoded := func(revID string) db.Version {
		v, err := db.LegacyRevToRevTreeEncodedVersion(revID)
		require.NoError(t, err)
		return v
	}
	newHLV := func(pv ...db.Version) *db.HybridLogicalVector {
		hlv := db.NewHybridLogicalVector()
		hlv.SourceID, hlv.Version = "cv", 100
		for _, v := range pv {
			hlv.PreviousVersions[v.SourceID] = v.Value
		}
		return hlv
	}

	testCases := []struct {
		name     string
		expected *db.HybridLogicalVector
		actual   *db.HybridLogicalVector
		equal    bool
	}{
		{
			name:     "identical",
			expected: newHLV(db.Version{SourceID: "a", Value: 1}),
			actual:   newHLV(db.Version{SourceID: "a", Value: 1}),
			equal:    true,
		},
		{
			name:     "extra encoded version of a revision in the chain",
			expected: newHLV(),
			actual:   newHLV(encoded(chainRev)),
			equal:    true,
		},
		{
			name:     "extra encoded version of a revision outside the chain",
			expected: newHLV(),
			actual:   newHLV(encoded(otherRev)),
			equal:    false,
		},
		{
			name:     "extra version that isn't revTreeID encoded",
			expected: newHLV(),
			actual:   newHLV(db.Version{SourceID: "a", Value: 1}),
			equal:    false,
		},
		{
			name:     "missing expected pv entry",
			expected: newHLV(encoded(chainRev)),
			actual:   newHLV(),
			equal:    false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.equal, hlvEqualAllowingEncodedRevs(t, tc.expected, tc.actual, revChain))
		})
	}
}
