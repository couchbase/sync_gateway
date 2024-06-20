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

	AuditIDCreateDatabase       AuditID = 53300 // create database
	AuditIDReadDatabase         AuditID = 53301 // view database information
	AuditIDReadDatabaseConfig   AuditID = 53302 // view database config
	AuditIDUpdateDatabaseConfig AuditID = 53303 // update database config
	AuditIDDeleteDatabase       AuditID = 53304 // delete database

	AuditIDUserAuthenticated        AuditID = 53400 // successful login
	AuditIDUserAuthenticationFailed AuditID = 53401 // incorrect login
	AuditIDUserAuthorizationFailed  AuditID = 53402 // correct login but incorrect permissions

	AuditIDDocumentCreate AuditID = 53500 // create document
	AuditIDDocumentRead   AuditID = 53501 // read document
	AuditIDDocumentUpdate AuditID = 53502 // update document
	AuditIDDocumentDelete AuditID = 53503 // delete document

	AuditIDReplicationConnect    AuditID = 53600 // new replication connection
	AuditIDReplicationDisconnect AuditID = 53601 // replication connection closed
)

// AuditEvents is a table of audit events created by Sync Gateway.
//
// This is used to generate:
//   - events themselves
//   - a kv-auditd-compatible descriptor with TestGenerateAuditdModuleDescriptor
//   - CSV output for each event to be used to document
var AuditEvents = events{
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
}
