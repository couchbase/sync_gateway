// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// TestGetOldRevisionBodyByRevTreeID ensures that fetching an older revision body using a legacy RevTree ID after flushing the revision cache works with store_legacy_revtree_data set.
func TestGetOldRevisionBodyByRevTreeID(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)

	tests := []struct {
		name                   string
		storeLegacyRevTreeData bool
		deltaSyncEnabled       bool
		expectedToFindOldRev   bool
	}{
		{
			name:                   "store_legacy_revtree_data=true, delta_sync=true",
			storeLegacyRevTreeData: true,
			deltaSyncEnabled:       true,
			expectedToFindOldRev:   true,
		},
		{
			name:                   "store_legacy_revtree_data=true, delta_sync=false",
			storeLegacyRevTreeData: true,
			deltaSyncEnabled:       false,
			expectedToFindOldRev:   true,
		},
		{
			name:                   "store_legacy_revtree_data=false, delta_sync=true",
			storeLegacyRevTreeData: false,
			deltaSyncEnabled:       true,
			expectedToFindOldRev:   false,
		},
		{
			name:                   "store_legacy_revtree_data=false, delta_sync=false",
			storeLegacyRevTreeData: false,
			deltaSyncEnabled:       false,
			expectedToFindOldRev:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					DeltaSync:              &DeltaSyncConfig{Enabled: base.Ptr(tc.deltaSyncEnabled)},
					StoreLegacyRevTreeData: base.Ptr(tc.storeLegacyRevTreeData),
				}},
			})
			defer rt.Close()

			// Create and update a document to generate an old revision.
			const docID = "doc1"
			v1 := rt.PutDoc(docID, `{"v":1}`)
			v2 := rt.UpdateDoc(docID, v1, `{"v":2}`)
			_ = v2

			// Flush the revision cache to force retrieval from bucket-stored old revision docs.
			rt.GetDatabase().FlushRevisionCacheForTest()

			resp := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", docID, v1.RevTreeID), "")
			// Attempt to fetch the old revision body using its RevTree ID.
			if tc.expectedToFindOldRev {
				RequireStatus(t, resp, http.StatusOK)
				assert.Contains(t, resp.BodyString(), `"v":1`)
			} else {
				RequireStatus(t, resp, http.StatusNotFound)
			}
		})
	}
}
