// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

const (
	// auditdSyncGatewayStartID is the start of an ID range allocated for Sync Gateway by auditd
	auditdSyncGatewayStartID AuditID = 53248

	AuditIDPlaceholder AuditID = 54000
)

// AuditEvents is a table of audit events created by Sync Gateway.
//
// This is used to generate:
//   - events themselves
//   - a kv-auditd-compatible descriptor with TestGenerateAuditdModuleDescriptor
//   - CSV output for each event to be used to document
var AuditEvents = events{
	AuditIDPlaceholder: {
		Name:        "Placeholder audit event",
		Description: "This is a placeholder.",
		MandatoryFields: map[string]any{
			"context": map[string]any{
				"provider": "example provider",
				"username": "alice",
			},
		},
		OptionalFields: map[string]any{
			"operationID": 123,
			"isSomething": false,
		},
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
}
