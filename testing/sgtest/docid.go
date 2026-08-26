// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/testing/require"
)

// SafeDocumentName returns a document name free of any special characters for use in tests.
func SafeDocumentName(t testing.TB, name string) string {
	t.Helper()
	docName := strings.ToLower(name)
	for _, c := range []string{" ", "<", ">", "/", "="} {
		docName = strings.ReplaceAll(docName, c, "_")
	}
	require.Less(t, len(docName), 251, "Document name %s is too long, must be less than 251 characters", name)
	return docName
}
