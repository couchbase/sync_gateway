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

// Audit IDs for Sync Gateway.
// The grouping is arbitrary, but are spaced out enough to allow for some future expansion in each area.
const (
	AuditIDAuditEnabled       AuditID = 53248
	AuditIDAuditDisabled      AuditID = 53249
	AuditIDAuditConfigChanged AuditID = 53250

	AuditIDSyncGatewayStartup AuditID = 0

	AuditIDSyncGatewayCollectInfoStart  AuditID = 0
	AuditIDSyncGatewayCollectInfoStop   AuditID = 0
	AuditIDSyncGatewayCollectInfoStatus AuditID = 0

	AuditIDPublicUserAuthenticated        AuditID = 0
	AuditIDPublicUserAuthenticationFailed AuditID = 0

	AuditIDAdminUserAuthenticated        AuditID = 0
	AuditIDAdminUserAuthenticationFailed AuditID = 0
	AuditIDAdminUserAuthorizationFailed  AuditID = 0

	AuditIDPublicAPIRequest  AuditID = 0
	AuditIDAdminAPIRequest   AuditID = 0
	AuditIDMetricsAPIRequest AuditID = 0
	AuditIDBLIPMessage       AuditID = 0

	AuditIDCreateDatabase       AuditID = 0
	AuditIDReadDatabase         AuditID = 0
	AuditIDDeleteDatabase       AuditID = 0
	AuditIDDatabaseOffline      AuditID = 0
	AuditIDDatabaseOnline       AuditID = 0
	AuditIDDatabaseViewCompact  AuditID = 0
	AuditIDDatabaseStartCompact AuditID = 0
	AuditIDDatabaseStopCompact  AuditID = 0
	AuditIDDatabaseViewResync   AuditID = 0
	AuditIDDatabaseStartResync  AuditID = 0
	AuditIDDatabaseStopResync   AuditID = 0

	AuditIDReadDatabaseConfig   AuditID = 0
	AuditIDUpdateDatabaseConfig AuditID = 0

	AuditIDUserCreate AuditID = 0
	AuditIDUserRead   AuditID = 0
	AuditIDUserUpdate AuditID = 0
	AuditIDUserDelete AuditID = 0

	AuditIDRoleCreate AuditID = 0
	AuditIDRoleRead   AuditID = 0
	AuditIDRoleUpdate AuditID = 0
	AuditIDRoleDelete AuditID = 0

	AuditIDDocumentCreate AuditID = 0
	AuditIDDocumentRead   AuditID = 0
	AuditIDDocumentUpdate AuditID = 0
	AuditIDDocumentDelete AuditID = 0

	AuditIDAttachmentCreate AuditID = 0
	AuditIDAttachmentRead   AuditID = 0
	AuditIDAttachmentUpdate AuditID = 0
	AuditIDAttachmentDelete AuditID = 0

	AuditIDReplicationConnect    AuditID = 0
	AuditIDReplicationDisconnect AuditID = 0
	AuditIDReplicationStartPull  AuditID = 0

	AuditIDISGRCreate     AuditID = 0
	AuditIDISGRRead       AuditID = 0
	AuditIDISGRUpdate     AuditID = 0
	AuditIDISGRDelete     AuditID = 0
	AuditIDISGRStart      AuditID = 0
	AuditIDISGRStop       AuditID = 0
	AuditIDISGRReset      AuditID = 0
	AuditIDISGRStatusRead AuditID = 0

	AuditIDISGRReadAll       AuditID = 0
	AuditIDISGRReadStatusAll AuditID = 0
)

// AuditEvents is a table of audit events created by Sync Gateway.
//
// This is used to generate:
//   - events themselves
//   - a kv-auditd-compatible descriptor with TestGenerateAuditdModuleDescriptor
//   - CSV output for each event to be used to document
var AuditEvents = events{
	AuditIDAuditEnabled: {
		Name:               "Audit logging enabled",
		Description:        "Audit logging was enabled",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"audit_scope": "global or db",
		},
		OptionalFields: AuditFields{
			"db": "database_name",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDAuditDisabled: {
		Name:               "Audit logging disabled",
		Description:        "Audit logging was disabled",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"audit_scope": "global or db",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDAuditConfigChanged: {
		Name:               "Audit configuration changed",
		Description:        "Audit configuration was changed",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"db":         "database_name",
			"new_config": "JSON representation of new db audit config",
		},
		EventType: eventTypeAdmin,
	},
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
		Name:        "Public API user authenticated",
		Description: "Public API user successfully authenticated",
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
		Name:        "Public API user authentication failed",
		Description: "Public API user failed to authenticate",
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
