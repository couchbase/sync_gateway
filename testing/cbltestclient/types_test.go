// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestDocumentProperties(t *testing.T) {
	doc := Document{DocumentIDProperty: "doc1", DocumentRevsProperty: "1@src", "foo": "bar"}
	assert.Equal(t, "doc1", doc.ID())
	assert.Equal(t, "1@src", doc.Revs())
	assert.Equal(t, map[string]any{"foo": "bar"}, doc.Properties())
}

func TestDocumentHLV(t *testing.T) {
	doc := Document{DocumentRevsProperty: "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w;17f4ed7b42a70000@ScJAVJf3TdOUanAcByIcXg"}
	hlv, legacyRevs, err := doc.HLV()
	require.NoError(t, err)
	assert.Empty(t, legacyRevs)
	assert.Equal(t, "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w", hlv.GetCurrentVersionString())
	assert.Len(t, hlv.PreviousVersions, 1)

	_, _, err = Document{}.HLV()
	require.Error(t, err)
}
