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
// The IDs and grouping is arbitrary, but are spaced to allow for some additions in each area.
const (
	// Audit events
	AuditIDAuditEnabled       AuditID = 53248
	AuditIDAuditDisabled      AuditID = 53249
	AuditIDAuditConfigChanged AuditID = 53250

	// SG events
	AuditIDSyncGatewayStartup AuditID = 53260

	// API events
	AuditIDPublicHTTPAPIRequest  AuditID = 53270
	AuditIDAdminHTTPAPIRequest   AuditID = 53271
	AuditIDMetricsHTTPAPIRequest AuditID = 53272
	//AuditIDBLIPMessage           AuditID = 0 // TODO: Is this required? What info should the event contain?

	// Auth (public) events
	AuditIDPublicUserAuthenticated        AuditID = 53280
	AuditIDPublicUserAuthenticationFailed AuditID = 53281
	// Auth (admin) events
	AuditIDAdminUserAuthenticated        AuditID = 53290
	AuditIDAdminUserAuthenticationFailed AuditID = 53291
	AuditIDAdminUserAuthorizationFailed  AuditID = 53292

	// SG node events
	AuditIDSyncGatewayCollectInfoStatus AuditID = 53300
	AuditIDSyncGatewayCollectInfoStart  AuditID = 53301
	AuditIDSyncGatewayCollectInfoStop   AuditID = 53302
	// TODO: Check API routes for more events

	// SG cluster events
	// TODO: Check API routes for more events

	// Database events
	AuditIDCreateDatabase AuditID = 54000
	AuditIDReadDatabase   AuditID = 54001
	AuditIDDeleteDatabase AuditID = 54002
	// Database config events
	AuditIDReadDatabaseConfig   AuditID = 54010
	AuditIDUpdateDatabaseConfig AuditID = 54011
	// Database operation events
	AuditIDDatabaseOffline       AuditID = 54020
	AuditIDDatabaseOnline        AuditID = 54021
	AuditIDDatabaseCompactStatus AuditID = 54030
	AuditIDDatabaseCompactStart  AuditID = 54031
	AuditIDDatabaseCompactStop   AuditID = 54032
	AuditIDDatabaseResyncStatus  AuditID = 54040
	AuditIDDatabaseResyncStart   AuditID = 54041
	AuditIDDatabaseResyncStop    AuditID = 54042
	// TODO: Check API routes for more events

	// User principal events
	AuditIDUserCreate AuditID = 54100
	AuditIDUserRead   AuditID = 54101
	AuditIDUserUpdate AuditID = 54102
	AuditIDUserDelete AuditID = 54103
	// Role principal events
	AuditIDRoleCreate AuditID = 54110
	AuditIDRoleRead   AuditID = 54111
	AuditIDRoleUpdate AuditID = 54112
	AuditIDRoleDelete AuditID = 54113

	// Changes feeds events

	// BLIP Replication events
	AuditIDReplicationConnect    AuditID = 55000
	AuditIDReplicationDisconnect AuditID = 55001

	// ISGR events
	AuditIDISGRCreate    AuditID = 55100
	AuditIDISGRRead      AuditID = 55101
	AuditIDISGRUpdate    AuditID = 55102
	AuditIDISGRDelete    AuditID = 55103
	AuditIDISGRStatus    AuditID = 55110
	AuditIDISGRStart     AuditID = 55111
	AuditIDISGRStop      AuditID = 55112
	AuditIDISGRReset     AuditID = 55113
	AuditIDISGRAllStatus AuditID = 55120
	AuditIDISGRAllRead   AuditID = 55121

	// Documents events
	AuditIDDocumentCreate AuditID = 56000
	AuditIDDocumentRead   AuditID = 56001
	AuditIDDocumentUpdate AuditID = 56002
	AuditIDDocumentDelete AuditID = 56003
	// Document attachments events
	AuditIDAttachmentCreate AuditID = 56010
	AuditIDAttachmentRead   AuditID = 56011
	AuditIDAttachmentUpdate AuditID = 56012
	AuditIDAttachmentDelete AuditID = 56013
)

// AuditEvents is a table of audit events created by Sync Gateway.
//
// This is used to generate:
//   - events themselves
//   - a kv-auditd-compatible descriptor with TestGenerateAuditdModuleDescriptor
//   - CSV output for each event to be used to document
var AuditEvents = events{
	AuditIDAuditEnabled: {
		Name:               "Auditing enabled",
		Description:        "Audit logging was enabled",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"audit_scope": "global or db",
		},
		OptionalFields: AuditFields{
			"db": "database name",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDAuditDisabled: {
		Name:               "Auditing disabled",
		Description:        "Audit logging was disabled",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"audit_scope": "global or db",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDAuditConfigChanged: {
		Name:               "Auditing configuration changed",
		Description:        "Audit logging configuration was changed",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"db":         "database name",
			"new_config": "JSON representation of new db audit config",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayStartup: {
		Name:               "Sync Gateway startup",
		Description:        "Sync Gateway started",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"sg_version": "version string",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDPublicHTTPAPIRequest: {
		Name:        "Public HTTP API request",
		Description: "Public HTTP API request was made",
		MandatoryFields: AuditFields{
			"method": "GET, POST, etc.",
			"path":   "request_path",
		},
		OptionalFields: AuditFields{
			"request_body": "request_body",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDAdminHTTPAPIRequest: {
		Name:        "Admin HTTP API request",
		Description: "Admin HTTP API request was made",
		MandatoryFields: AuditFields{
			"method": "GET, POST, etc.",
			"path":   "request_path",
		},
		OptionalFields: AuditFields{
			"request_body": "request_body",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDMetricsHTTPAPIRequest: {
		Name:        "Metrics HTTP API request",
		Description: "Metrics HTTP API request was made",
		MandatoryFields: AuditFields{
			"method": "GET, POST, etc.",
			"path":   "request_path",
		},
		OptionalFields: AuditFields{
			"request_body": "request_body",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDPublicUserAuthenticated: {
		Name:        "Public API user authenticated",
		Description: "Public API user successfully authenticated",
		MandatoryFields: AuditFields{
			"method": "basic, oidc, cookie, etc.",
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
			"method": "basic, oidc, cookie, etc.",
		},
		OptionalFields: AuditFields{
			"username": "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDAdminUserAuthenticated: {
		Name:               "Admin API user authenticated",
		Description:        "Admin API user successfully authenticated",
		MandatoryFields:    AuditFields{},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDAdminUserAuthenticationFailed: {
		Name:        "Admin API user authentication failed",
		Description: "Admin API user failed to authenticate",
		MandatoryFields: AuditFields{
			"username": "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDAdminUserAuthorizationFailed: {
		Name:        "Admin API user authorization failed",
		Description: "Admin API user failed to authorize",
		MandatoryFields: AuditFields{
			"username": "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDSyncGatewayCollectInfoStart: {
		Name:               "sgcollect_info start",
		Description:        "sgcollect_info was started",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"output_dir":   "output_directory",
			"upload_host":  "upload_host",
			"customer":     "customer",
			"ticket":       "ticket",
			"keep_zip":     true,
			"zip_filename": "zip_filename",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayCollectInfoStop: {
		Name:               "sgcollect_info stop",
		Description:        "sgcollect_info was stopped",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields:    AuditFields{},
		EventType:          eventTypeAdmin,
	},
	AuditIDSyncGatewayCollectInfoStatus: {
		Name:               "sgcollect_info status",
		Description:        "sgcollect_info status was viewed",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields:    AuditFields{},
		EventType:          eventTypeAdmin,
	},
	AuditIDCreateDatabase: {
		Name:        "Create database",
		Description: "A new database was created",
		MandatoryFields: AuditFields{
			"db":     "database name",
			"config": "JSON representation of db config",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDReadDatabase: {
		Name:        "Read database",
		Description: "Information about this database was read.",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		OptionalFields:     AuditFields{},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDDeleteDatabase: {
		Name:        "Delete database",
		Description: "A database was deleted",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDReadDatabaseConfig: {
		Name:        "Read database config",
		Description: "Database configuration was read",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDUpdateDatabaseConfig: {
		Name:        "Update database config",
		Description: "Database configuration was updated",
		MandatoryFields: AuditFields{
			"db":     "database name",
			"config": "JSON representation of new db config",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseOffline: {
		Name:        "Database offline",
		Description: "Database was taken offline",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseOnline: {
		Name:        "Database online",
		Description: "Database was brought online",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStatus: {
		Name:        "Database compaction status",
		Description: "Database compaction status was viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStart: {
		Name:        "Database compaction start",
		Description: "Database compaction was started",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStop: {
		Name:        "Database compaction stop",
		Description: "Database compaction was stopped",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStatus: {
		Name:        "Database resync status",
		Description: "Database resync status was viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStart: {
		Name:        "Database resync start",
		Description: "Database resync was started",
		MandatoryFields: AuditFields{
			"db":                   "database name",
			"collections":          []string{"list", "of", "collections"},
			"regenerate_sequences": true,
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStop: {
		Name:        "Database resync stop",
		Description: "Database resync was stopped",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserCreate: {
		Name:        "Create user",
		Description: "A new user was created",
		MandatoryFields: AuditFields{
			"username": "username",
			"db":       "database name",
			"roles":    []string{"list", "of", "roles"},
			"channels": []string{"list", "of", "channels"},
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserRead: {
		Name:        "Read user",
		Description: "Information about this user was read",
		MandatoryFields: AuditFields{
			"username": "username",
			"db":       "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserUpdate: {
		Name:        "Update user",
		Description: "User was updated",
		MandatoryFields: AuditFields{
			"username": "username",
			"db":       "database name",
			"roles":    []string{"list", "of", "roles"},
			"channels": []string{"list", "of", "channels"},
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserDelete: {
		Name:        "Delete user",
		Description: "User was deleted",
		MandatoryFields: AuditFields{
			"username": "username",
			"db":       "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDRoleCreate: {
		Name:        "Create role",
		Description: "A new role was created",
		MandatoryFields: AuditFields{
			"role":           "role_name",
			"db":             "database name",
			"admin_channels": []string{"list", "of", "channels"},
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDRoleRead: {
		Name:        "Read role",
		Description: "Information about this role was read",

		MandatoryFields: AuditFields{
			"role": "role_name",
			"db":   "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDRoleUpdate: {
		Name:        "Update role",
		Description: "Role was updated",

		MandatoryFields: AuditFields{
			"role":           "role_name",
			"db":             "database name",
			"admin_channels": []string{"list", "of", "channels"},
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDRoleDelete: {
		Name:        "Delete role",
		Description: "Role was deleted",
		MandatoryFields: AuditFields{
			"role": "role_name",
			"db":   "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDReplicationConnect: {
		Name:        "Replication connect",
		Description: "A client connected using WebSocket/BLIP for replication",
		MandatoryFields: AuditFields{
			"db":  "database name",
			"cid": "context id",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDReplicationDisconnect: {
		Name:        "Replication disconnect",
		Description: "A client disconnected from WebSocket/BLIP replication",
		MandatoryFields: AuditFields{
			"db":  "database name",
			"cid": "context id",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDISGRCreate: {
		Name:        "Create Inter-Sync Gateway Replication",
		Description: "A new Inter-Sync Gateway Replication was created",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
			"config":         "JSON representation of replication config",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRRead: {
		Name:        "Read Inter-Sync Gateway Replication",
		Description: "Information about this Inter-Sync Gateway Replication was read",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRUpdate: {
		Name:        "Update Inter-Sync Gateway Replication",
		Description: "Inter-Sync Gateway Replication was updated",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
			"config":         "JSON representation of new replication config",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
	},
	AuditIDISGRDelete: {
		Name:        "Delete Inter-Sync Gateway Replication",
		Description: "Inter-Sync Gateway Replication was deleted",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStatus: {
		Name:        "Inter-Sync Gateway Replication status",
		Description: "Inter-Sync Gateway Replication status was document viewed",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
	},
	AuditIDISGRStart: {
		Name:        "Inter-Sync Gateway Replication start",
		Description: "Inter-Sync Gateway Replication was started",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
	},
	AuditIDISGRStop: {
		Name:        "Inter-Sync Gateway Replication stop",
		Description: "Inter-Sync Gateway Replication was stopped",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
	},
	AuditIDISGRReset: {
		Name:        "Inter-Sync Gateway Replication reset",
		Description: "Inter-Sync Gateway Replication was reset",
		MandatoryFields: AuditFields{
			"db":             "database name",
			"replication_id": "replication id",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
	},
	AuditIDISGRAllStatus: {
		Name:        "All Inter-Sync Gateway Replication status",
		Description: "All Inter-Sync Gateway Replication statuses were viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
	},
	AuditIDISGRAllRead: {
		Name:        "Read all Inter-Sync Gateway Replications",
		Description: "All Inter-Sync Gateway Replications were viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
	},
	AuditIDDocumentCreate: {
		Name:        "Create document",
		Description: "A new document was created",
		MandatoryFields: AuditFields{
			"db":          "database name",
			"ks":          "keyspace",
			"doc_id":      "document id",
			"doc_version": "revision ID or version",
			"channels":    []string{"list", "of", "channels"},
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentRead: {
		Name:        "Read document",
		Description: "A document was read",
		MandatoryFields: AuditFields{
			"db":          "database name",
			"ks":          "keyspace",
			"doc_id":      "document id",
			"doc_version": "revision ID or version",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentUpdate: {
		Name:        "Update document",
		Description: "A document was updated",
		MandatoryFields: AuditFields{
			"db":          "database name",
			"ks":          "keyspace",
			"doc_id":      "document id",
			"doc_version": "revision ID or version",
			"channels":    []string{"list", "of", "channels"},
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentDelete: {
		Name:        "Delete document",
		Description: "A document was deleted",
		MandatoryFields: AuditFields{
			"db":          "database name",
			"ks":          "keyspace",
			"doc_id":      "document id",
			"doc_version": "revision ID or version",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentCreate: {
		Name:        "Create attachment",
		Description: "A new attachment was created",
		MandatoryFields: AuditFields{
			"db":            "database name",
			"ks":            "keyspace",
			"doc_id":        "document id",
			"doc_version":   "revision ID or version",
			"attachment_id": "attachment name",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentRead: {

		Name:        "Read attachment",
		Description: "An attachment was read",
		MandatoryFields: AuditFields{
			"db":            "database name",
			"ks":            "keyspace",
			"doc_id":        "document id",
			"doc_version":   "revision ID or version",
			"attachment_id": "attachment name",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,

		EventType: eventTypeData,
	},
	AuditIDAttachmentUpdate: {
		Name:        "Update attachment",
		Description: "An attachment was updated",
		MandatoryFields: AuditFields{
			"db":            "database name",
			"ks":            "keyspace",
			"doc_id":        "document id",
			"doc_version":   "revision ID or version",
			"attachment_id": "attachment name",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentDelete: {
		Name:        "Delete attachment",
		Description: "An attachment was deleted",

		MandatoryFields: AuditFields{
			"db":          "database name",
			"ks":          "keyspace",
			"doc_id":      "document id",
			"doc_version": "revision ID or version",
			"attachment":  "attachment_name",
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
}
