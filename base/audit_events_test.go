// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAuditEvents(t *testing.T) {
	// Ensures that the above audit event IDs are within the allocated range and are valid.
	require.NoError(t, validateAuditEvents(AuditEvents))
}

func validateAuditEvents(e events) error {
	for id, descriptor := range e {
		if id < auditdSyncGatewayStartID || id > auditdSyncGatewayEndID {
			return fmt.Errorf("invalid audit event ID: %d %q (allowed range: %d-%d)",
				id, descriptor.Name, auditdSyncGatewayStartID, auditdSyncGatewayEndID)
		}
	}
	return nil
}
