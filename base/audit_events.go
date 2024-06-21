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
	AuditIDAuditEnabled  AuditID = 53248 // audit enabled
	AuditIDAuditDisabled AuditID = 53249 // audit disabled
	AuditIDAuditChanged  AuditID = 53250 // audit configuration changed

	AuditIDUserAuthenticated        AuditID = 53260 // successful login
	AuditIDUserAuthenticationFailed AuditID = 53261 // incorrect login
	AuditIDUserAuthorizationFailed  AuditID = 53262 // correct login but incorrect permissions

	AuditIDCreateDatabase       AuditID = 53300 // create database
	AuditIDReadDatabase         AuditID = 53301 // view database information
	AuditIDReadDatabaseConfig   AuditID = 53302 // view database config
	AuditIDUpdateDatabaseConfig AuditID = 53303 // update database config
	AuditIDDeleteDatabase       AuditID = 53304 // delete database

	AuditIDDocumentCreate AuditID = 54300 // create document
	AuditIDDocumentRead   AuditID = 54301 // read document
	AuditIDDocumentUpdate AuditID = 54302 // update document
	AuditIDDocumentDelete AuditID = 54303 // delete document

	AuditIDReplicationConnect    AuditID = 55300 // new replication connection
	AuditIDReplicationDisconnect AuditID = 55301 // replication connection closed
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
	AuditIDUserAuthenticated: {
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
	AuditIDUserAuthenticationFailed: {
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
