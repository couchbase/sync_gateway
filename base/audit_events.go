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
	// auditdSyncGatewayEndID is the maximum ID that can be allocated to a Sync Gateway audit descriptor.
	auditdSyncGatewayEndID = auditdSyncGatewayStartID + auditdIDBlockSize // 57343

	// auditdIDBlockSize is the number of IDs allocated to each module in auditd.
	auditdIDBlockSize = 0xFFF
)

const (
	AuditIDAuditEnabled AuditID = 53248

	AuditIDPublicUserAuthenticated        AuditID = 53260
	AuditIDPublicUserAuthenticationFailed AuditID = 53261

	AuditIDReadDatabase AuditID = 53301
)

// AuditEvents is a table of audit events created by Sync Gateway.
//
// This is used to generate:
//   - events themselves
//   - a kv-auditd-compatible descriptor with TestGenerateAuditdModuleDescriptor
//   - CSV output for each event to be used to document
var AuditEvents = events{
	AuditIDAuditEnabled: {}, // TODO: unreferenced event - somehow find this mistake via test or lint!
	AuditIDReadDatabase: {
		Name:        "Read database",
		Description: "Information about this database was read.",
		MandatoryFields: AuditFields{
			"db": "database_name",
		},
		OptionalFields:     AuditFields{},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserAuthenticated: {
		Name:        "User authenticated",
		Description: "User successfully authenticated",
		MandatoryFields: AuditFields{
			"method": "auth_method", // e.g. "basic", "oidc", "cookie", ...
		},
		OptionalFields: AuditFields{
			"oidc_issuer": "issuer",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserAuthenticationFailed: {
		Name:        "User authentication failed",
		Description: "User authentication failed",
		MandatoryFields: AuditFields{
			"method": "auth_method", // e.g. "basic", "oidc", "cookie", ...
		},
		OptionalFields: AuditFields{
			"username": "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
}

// DefaultAuditEventIDs is a list of audit event IDs that are enabled by default.
var DefaultAuditEventIDs = buildDefaultAuditIDList(AuditEvents)

func buildDefaultAuditIDList(e events) (ids []uint) {
	for k, v := range e {
		if v.EnabledByDefault {
			ids = append(ids, uint(k))
		}
	}
	return ids
}

// NonFilterableEvents is a map of audit events that are not permitted to be filtered.
var NonFilterableEvents = buildNonFilterableEvents(AuditEvents)

func buildNonFilterableEvents(e events) events {
	nonFilterable := make(events)
	for k, v := range e {
		if !v.FilteringPermitted {
			nonFilterable[k] = v
		}
	}
	return nonFilterable
}
