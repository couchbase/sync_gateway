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

	// BLIP Replication events
	AuditIDReplicationConnect    AuditID = 54300
	AuditIDReplicationDisconnect AuditID = 54301
	// ISGR events
	AuditIDISGRCreate    AuditID = 54400
	AuditIDISGRRead      AuditID = 54401
	AuditIDISGRUpdate    AuditID = 54402
	AuditIDISGRDelete    AuditID = 54403
	AuditIDISGRStatus    AuditID = 54410
	AuditIDISGRStart     AuditID = 54411
	AuditIDISGRStop      AuditID = 54412
	AuditIDISGRReset     AuditID = 54413
	AuditIDISGRAllStatus AuditID = 54420
	AuditIDISGRAllRead   AuditID = 54421
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
	AuditIDReplicationConnect: {
		Name:        "Replication connected",
		Description: "Replication connected (Inter-Sync Gateway or Couchbase Lite)",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication_correlation_id", // don't collide with http correlation id, or ISGR replication ID
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDReplicationDisconnect: {
		Name:        "Replication disconnected",
		Description: "Replication disconnected (Inter-Sync Gateway or Couchbase Lite)",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication_correlation_id", // don't collide with http correlation ID, or ISGR replication ID
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDISGRCreate: {
		Name:        "ISGR replication created",
		Description: "Inter-Sync Gateway Replication created",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRRead: {
		Name:        "ISGR replication read",
		Description: "Inter-Sync Gateway Replication read single replication",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRUpdate: {
		Name:        "ISGR replication updated",
		Description: "Inter-Sync Gateway Replication updated",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRDelete: {
		Name:        "ISGR replication deleted",
		Description: "Inter-Sync Gateway Replication deleted",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStatus: {
		Name:        "ISGR replication status",
		Description: "Inter-Sync Gateway Replication status",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStart: {
		Name:        "ISGR replication started",
		Description: "Inter-Sync Gateway Replication started",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStop: {
		Name:        "ISGR replication stopped",
		Description: "Inter-Sync Gateway Replication stopped",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRReset: {
		Name:        "ISGR replication reset",
		Description: "Inter-Sync Gateway Replication reset",
		MandatoryFields: AuditFields{
			AuditFieldISGRReplicationID: "replication_name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRAllStatus: {
		Name:               "ISGR all replications status",
		Description:        "Inter-Sync Gateway get all replication statuses",
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRAllRead: {
		Name:               "ISGR read all replications",
		Description:        "Inter-Sync Gateway read all replications",
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
}

// DefaultAuditEventIDs is a list of audit event IDs that are enabled by default.
var DefaultAuditEventIDs = buildDefaultAuditIDList(AuditEvents)

func buildDefaultAuditIDList(e events) []uint {
	var ids []uint
	for id, event := range e {
		if event.EnabledByDefault {
			ids = append(ids, uint(id))
		}
	}
	return ids
}
