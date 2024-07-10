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
	AuditIDPublicUserSessionCreated       AuditID = 53282
	AuditIDPublicUserSessionDeleted       AuditID = 53283
	// Auth (admin) events
	AuditIDAdminUserAuthenticated        AuditID = 53290
	AuditIDAdminUserAuthenticationFailed AuditID = 53291
	AuditIDAdminUserAuthorizationFailed  AuditID = 53292

	// SG node events
	AuditIDSyncGatewayCollectInfoStatus AuditID = 53300
	AuditIDSyncGatewayCollectInfoStart  AuditID = 53301
	AuditIDSyncGatewayCollectInfoStop   AuditID = 53302
	AuditIDSyncGatewayStats             AuditID = 53303
	AuditIDSyncGatewayProfiling         AuditID = 53304

	// SG cluster events
	AuditIDClusterInfoRead AuditID = 53350
	AuditIDPostUpgrade     AuditID = 54043

	// Database events
	AuditIDCreateDatabase  AuditID = 54000
	AuditIDReadDatabase    AuditID = 54001
	AuditIDDeleteDatabase  AuditID = 54002
	AuditIDDatabaseAllRead AuditID = 54003
	// Database config events
	AuditIDReadDatabaseConfig   AuditID = 54010
	AuditIDUpdateDatabaseConfig AuditID = 54011
	// Database operation events
	AuditIDDatabaseOffline                 AuditID = 54020
	AuditIDDatabaseOnline                  AuditID = 54021
	AuditIDDatabaseAttachmentCompactStatus AuditID = 54030
	AuditIDDatabaseAttachmentCompactStart  AuditID = 54031
	AuditIDDatabaseAttachmentCompactStop   AuditID = 54032
	AuditIDDatabaseTombstoneCompactStatus  AuditID = 54033
	AuditIDDatabaseTombstoneCompactStart   AuditID = 54034
	AuditIDDatabaseTombstoneCompactStop    AuditID = 54035

	AuditIDDatabaseResyncStatus AuditID = 54040
	AuditIDDatabaseResyncStart  AuditID = 54041
	AuditIDDatabaseResyncStop   AuditID = 54042
	AuditIDDatabaseRepair       AuditID = 54044
	AuditIDDatabaseFlush        AuditID = 54045

	// User principal events
	AuditIDUserCreate AuditID = 54100
	AuditIDUserRead   AuditID = 54101
	AuditIDUserUpdate AuditID = 54102
	AuditIDUserDelete AuditID = 54103
	AuditIDUsersAll           = 54110
	// Role principal events
	AuditIDRoleCreate AuditID = 54110
	AuditIDRoleRead   AuditID = 54111
	AuditIDRoleUpdate AuditID = 54112
	AuditIDRoleDelete AuditID = 54113
	AuditIDRolesAll           = 54120

	// Changes feeds events
	AuditIDChangesFeedStarted AuditID = 54200

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

	// Documents events
	AuditIDDocumentCreate       AuditID = 55000
	AuditIDDocumentRead         AuditID = 55001
	AuditIDDocumentUpdate       AuditID = 55002
	AuditIDDocumentDelete       AuditID = 55003
	AuditIDDocumentMetadataRead AuditID = 55004
	// Document attachments events
	AuditIDAttachmentCreate AuditID = 55010
	AuditIDAttachmentRead   AuditID = 55011
	AuditIDAttachmentUpdate AuditID = 55012
	AuditIDAttachmentDelete AuditID = 55013
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
			"db":          "database name",
			"audit_scope": "global or db",
			"new_config":  "JSON representation of new db audit config",
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
			"config":     "JSON representation of startup config",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDPublicHTTPAPIRequest: {
		Name:        "Public HTTP API request",
		Description: "Public HTTP API request was made",
		MandatoryFields: AuditFields{
			"http_method": "GET, POST, etc.",
			"http_path":   "request_path",
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
			"http_method": "GET, POST, etc.",
			"http_path":   "request_path",
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
			"http_method": "GET, POST, etc.",
			"http_path":   "request_path",
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
			"auth_method": "basic, oidc, cookie, etc.",
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
			"auth_method": "basic, oidc, cookie, etc.",
		},
		OptionalFields: AuditFields{
			"username": "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserSessionCreated: {
		Name:        "Public API user session created",
		Description: "Public API user session was created",
		MandatoryFields: AuditFields{
			"session_id": "session_id",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserSessionDeleted: {
		Name:        "Public API user session deleted",
		Description: "Public API user session was deleted",
		MandatoryFields: AuditFields{
			"session_id": "session_id",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
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
	AuditIDSyncGatewayCollectInfoStatus: {
		Name:               "sgcollect_info status",
		Description:        "sgcollect_info status was viewed",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields:    AuditFields{},
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
	AuditIDSyncGatewayStats: {
		Name:               "stats requested",
		Description:        "stats were requested",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			"stats_format": "expvar, prometheus, etc.",
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayProfiling: {
		Name:        "profiling requested",
		Description: "profiling was requested",
		MandatoryFields: AuditFields{
			"profile_type": "cpu, memory, etc.",
		},
		OptionalFields: AuditFields{
			"filename": "filename",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDClusterInfoRead: {
		Name:               "Sync Gateway cluster info read",
		Description:        "Sync Gateway cluster info was viewed",
		EnabledByDefault:   true,
		FilteringPermitted: false,
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
		Description: "Information about this database was viewed.",
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
		Description: "Database configuration was viewed",
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
	AuditIDDatabaseAttachmentCompactStatus: {
		Name:        "Database attachment compaction status",
		Description: "Database attachment compaction status was viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseAttachmentCompactStart: {
		Name:        "Database attachment compaction start",
		Description: "Database attachment compaction was started",
		MandatoryFields: AuditFields{
			"db":      "database name",
			"dry_run": false,
			"reset":   false,
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseAttachmentCompactStop: {
		Name:        "Database attachment compaction stop",
		Description: "Database attachment compaction was stopped",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseTombstoneCompactStatus: {
		Name:        "Database tombstone compaction status",
		Description: "Database tombstone compaction status was viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseTombstoneCompactStart: {
		Name:        "Database tombstone compaction start",
		Description: "Database tombstone compaction was started from REST",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseTombstoneCompactStop: {
		Name:        "Database tombstone compaction stop",
		Description: "Database tombstone compaction was stopped from REST",
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
			"collections":          map[string][]string{"scopeName": {"collectionA", "collectionB"}},
			"regenerate_sequences": true,
			"reset":                true,
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
	AuditIDPostUpgrade: {
		Name:        "Post-upgrade",
		Description: "Post-upgrade was run for Sync Gateway databases",
		MandatoryFields: AuditFields{
			"preview": false,
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseRepair: {
		Name:        "Database repair",
		Description: "Database repair was run",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseFlush: {
		Name:        "Database flush",
		Description: "Database flush was run",
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
		Description: "Information about this user was viewed",
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
		Description: "Information about this role was viewed",

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
	AuditIDChangesFeedStarted: {
		Name:        "Changes feed started",
		Description: "Changes feed was started",
		MandatoryFields: AuditFields{
			"db":    "database name",
			"ks":    "keyspace",
			"since": "since",
		},
		OptionalFields: AuditFields{
			"filter":    "filter",
			"doc_ids":   []string{"list", "of", "doc_ids"},
			"channels":  []string{"list", "of", "channels"},
			"feed_type": "continuous, normal, longpoll, websocket, etc.",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDReplicationConnect: {
		Name:        "Replication connect",
		Description: "A replication client connected",
		MandatoryFields: AuditFields{
			"db":  "database name",
			"cid": "context id",
		},
		OptionalFields: AuditFields{
			"client_type":    "isgr, cbl, other",
			"client_version": "client version",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDReplicationDisconnect: {
		Name:        "Replication disconnect",
		Description: "A replication client disconnected",
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
		Description: "Information about this Inter-Sync Gateway Replication was viewed",
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
		Description: "A document was viewed",
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
	AuditIDDocumentMetadataRead: {
		Name:        "Read document metadata",
		Description: "Document metadata was viewed",
		MandatoryFields: AuditFields{
			"db":     "database name",
			"ks":     "keyspace",
			"doc_id": "document id",
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
		Description: "An attachment was viewed",
		MandatoryFields: AuditFields{
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
			"doc_id":        "document id",
			"doc_version":   "revision ID or version",
			"attachment_id": "attachment name",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupKeyspace,
			fieldGroupRequest,
			fieldGroupAuthenticated,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
}

func init() {
	AuditEvents.expandMandatoryFieldGroups()
}

func (e events) expandMandatoryFieldGroups() {
	for _, descriptor := range e {
		descriptor.MandatoryFields.expandMandatoryFieldGroups(descriptor.mandatoryFieldGroups)
	}
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
