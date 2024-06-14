// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package audit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenerateAuditdModuleDescriptor outputs a generated auditd module descriptor for SGAuditEvents.
func TestGenerateAuditdModuleDescriptor(t *testing.T) {
	b, err := generateAuditdModuleDescriptor(SGAuditEvents)
	require.NoError(t, err)
	t.Log(string(b))
}
