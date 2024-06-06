// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package audit

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	// auditdSyncGatewayEndID is the maximum ID that can be allocated to a Sync Gateway audit descriptor.
	auditdSyncGatewayEndID = auditdSyncGatewayStartID + auditdIDBlockSize // 57343

	// auditdIDBlockSize is the number of IDs allocated to each module in auditd.
	auditdIDBlockSize = 0xFFF
)

func TestValidateAuditEvents(t *testing.T) {
	// Ensures that the above audit event IDs are within the allocated range and are valid.
	require.NoError(t, validateAuditEvents(sgAuditEvents))
}

func validateAuditEvents(e events) error {
	for id, descriptor := range e {
		if id < auditdSyncGatewayStartID || id > auditdSyncGatewayEndID {
			return fmt.Errorf("invalid audit event ID: %d %q (allowed range: %d-%d)",
				id, descriptor.name, auditdSyncGatewayStartID, auditdSyncGatewayEndID)
		}
	}
	return nil
}
