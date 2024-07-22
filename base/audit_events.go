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

	// Auth (public) events
	AuditIDPublicUserAuthenticated        AuditID = 53280
	AuditIDPublicUserAuthenticationFailed AuditID = 53281
	AuditIDPublicUserSessionCreated       AuditID = 53282
	AuditIDPublicUserSessionDeleted       AuditID = 53283
	AuditIDPublicUserSessionDeleteAll     AuditID = 53284

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
	AuditIDDatabaseOffline       AuditID = 54020
	AuditIDDatabaseOnline        AuditID = 54021
	AuditIDDatabaseCompactStatus AuditID = 54030
	AuditIDDatabaseCompactStart  AuditID = 54031
	AuditIDDatabaseCompactStop   AuditID = 54032

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
	AuditIDUsersAll   AuditID = 54104
	// Role principal events
	AuditIDRoleCreate AuditID = 54110
	AuditIDRoleRead   AuditID = 54111
	AuditIDRoleUpdate AuditID = 54112
	AuditIDRoleDelete AuditID = 54113
	AuditIDRolesAll   AuditID = 54114

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
			"audit_scope":     "global or db",
			AuditFieldPayload: "JSON representation of new db audit config",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayStartup: {
		Name:               "Sync Gateway startup",
		Description:        "Sync Gateway started",
		EnabledByDefault:   true,
		FilteringPermitted: false,
		MandatoryFields: AuditFields{
			AuditFieldSGVersion:                      "version string",
			AuditFieldUseTLSServer:                   true,
			AuditFieldServerTLSSkipVerify:            true,
			AuditFieldAdminInterfaceAuthentication:   true,
			AuditFieldMetricsInterfaceAuthentication: true,
			AuditFieldLogFilePath:                    "/log/file/path",
			AuditFieldBcryptCost:                     10,
			AuditFieldDisablePersistentConfig:        false,
		},
		EventType: eventTypeAdmin,
	},
	AuditIDPublicHTTPAPIRequest: {
		Name:        "Public HTTP API request",
		Description: "Public HTTP API request was made",
		MandatoryFields: AuditFields{
			AuditFieldHTTPMethod: "GET, POST, etc.",
			AuditFieldHTTPPath:   "request_path",
		},
		OptionalFields: AuditFields{
			AuditFieldRequestBody: "request_body",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDAdminHTTPAPIRequest: {
		Name:        "Admin HTTP API request",
		Description: "Admin HTTP API request was made",
		MandatoryFields: AuditFields{
			AuditFieldHTTPMethod: "GET, POST, etc.",
			AuditFieldHTTPPath:   "request_path",
		},
		OptionalFields: AuditFields{
			AuditFieldRequestBody: "request_body",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDMetricsHTTPAPIRequest: {
		Name:        "Metrics HTTP API request",
		Description: "Metrics HTTP API request was made",
		MandatoryFields: AuditFields{
			AuditFieldHTTPMethod: "GET, POST, etc.",
			AuditFieldHTTPPath:   "request_path",
		},
		OptionalFields: AuditFields{
			AuditFieldRequestBody: "request_body",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDPublicUserAuthenticated: {
		Name:        "Public API user authenticated",
		Description: "Public API user successfully authenticated",
		MandatoryFields: AuditFields{
			AuditFieldAuthMethod: "basic, oidc, cookie, etc.",
		},
		OptionalFields: AuditFields{
			"oidc_issuer": "issuer",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserAuthenticationFailed: {
		Name:        "Public API user authentication failed",
		Description: "Public API user failed to authenticate",
		MandatoryFields: AuditFields{
			AuditFieldAuthMethod: "basic, oidc, cookie, etc.",
		},
		OptionalFields: AuditFields{
			AuditFieldUserName: "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserSessionCreated: {
		Name:        "Public API user session created",
		Description: "Public API user session was created",
		MandatoryFields: AuditFields{
			AuditFieldSessionID: "session_id",
		},
		OptionalFields: AuditFields{
			AuditFieldUserName: "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserSessionDeleted: {
		Name:        "Public API user session deleted",
		Description: "Public API user session was deleted",
		MandatoryFields: AuditFields{
			AuditFieldSessionID: "session_id",
		},
		OptionalFields: AuditFields{
			AuditFieldUserName: "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserSessionDeleteAll: {
		Name:        "Public API user all sessions deleted",
		Description: "All sessions were deleted for a Public API user",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
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
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDAdminUserAuthenticationFailed: {
		Name:        "Admin API user authentication failed",
		Description: "Admin API user failed to authenticate",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDAdminUserAuthorizationFailed: {
		Name:        "Admin API user authorization failed",
		Description: "Admin API user failed to authorize",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDSyncGatewayCollectInfoStatus: {
		Name:               "sgcollect_info status",
		Description:        "sgcollect_info status was viewed",
		EnabledByDefault:   true,
		FilteringPermitted: true,
		MandatoryFields:    AuditFields{},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayCollectInfoStart: {
		Name:               "sgcollect_info start",
		Description:        "sgcollect_info was started",
		EnabledByDefault:   true,
		FilteringPermitted: true,
		MandatoryFields: AuditFields{
			"output_dir":   "output_directory",
			"upload_host":  "upload_host",
			"customer":     "customer",
			"ticket":       "ticket",
			"keep_zip":     true,
			"zip_filename": "zip_filename",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayCollectInfoStop: {
		Name:               "sgcollect_info stop",
		Description:        "sgcollect_info was stopped",
		EnabledByDefault:   true,
		FilteringPermitted: true,
		MandatoryFields:    AuditFields{},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayStats: {
		Name:               "stats requested",
		Description:        "stats were requested",
		EnabledByDefault:   false, // because low value high volume
		FilteringPermitted: true,
		MandatoryFields: AuditFields{
			AuditFieldStatsFormat: "expvar, prometheus, etc.",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EventType: eventTypeAdmin,
	},
	AuditIDSyncGatewayProfiling: {
		Name:        "profiling requested",
		Description: "profiling was requested",
		MandatoryFields: AuditFields{
			AuditFieldPprofProfileType: "cpu, memory, etc.",
		},
		OptionalFields: AuditFields{
			AuditFieldFileName: "filename",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDClusterInfoRead: {
		Name:               "Sync Gateway cluster info read",
		Description:        "Sync Gateway cluster info was viewed",
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDCreateDatabase: {
		Name:        "Create database",
		Description: "A new database was created",
		MandatoryFields: AuditFields{
			AuditFieldPayload: "JSON representation of db config",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDReadDatabase: {
		Name:        "Read database",
		Description: "Information about this database was viewed",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   false, // because high volume (Capella UI)
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDDeleteDatabase: {
		Name:        "Delete database",
		Description: "A database was deleted",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseAllRead: {
		Name:        "Read all databases",
		Description: "All databases were viewed",
		MandatoryFields: AuditFields{
			AuditFieldDBNames: []string{"list", "of", "db", "names"},
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   false, // because high volume (Capella UI)
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDReadDatabaseConfig: {
		Name:        "Read database config",
		Description: "Database configuration was viewed",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDUpdateDatabaseConfig: {
		Name:        "Update database config",
		Description: "Database configuration was updated",
		MandatoryFields: AuditFields{
			AuditFieldPayload: "payload",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseOffline: {
		Name:        "Database offline",
		Description: "Database was taken offline",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseOnline: {
		Name:        "Database online",
		Description: "Database was brought online",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStart: {
		Name:        "Database attachment compaction start",
		Description: "Database attachment compaction was started",
		MandatoryFields: AuditFields{
			AuditFieldCompactionType: "attachment or tombstone",
		},
		OptionalFields: AuditFields{
			AuditFieldCompactionDryRun: false,
			AuditFieldCompactionReset:  false,
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStop: {
		Name:        "Database compaction stop",
		Description: "Database compaction was stopped",
		MandatoryFields: AuditFields{
			AuditFieldCompactionType: "attachment or tombstone",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStatus: {
		Name:        "Database compaction status",
		Description: "Database compaction status was viewed",
		MandatoryFields: AuditFields{
			AuditFieldCompactionType: "attachment or tombstone",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStatus: {
		Name:        "Database resync status",
		Description: "Database resync status was viewed",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   false, // because low value high volume (Capella UI)
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStart: {
		Name:        "Database resync start",
		Description: "Database resync was started",
		MandatoryFields: AuditFields{
			"collections":          map[string][]string{"scopeName": {"collectionA", "collectionB"}},
			"regenerate_sequences": true,
			"reset":                true,
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStop: {
		Name:        "Database resync stop",
		Description: "Database resync was stopped",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDPostUpgrade: {
		Name:        "Post-upgrade",
		Description: "Post-upgrade was run for Sync Gateway databases",
		MandatoryFields: AuditFields{
			AuditFieldPostUpgradePreview: true,
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseRepair: {
		Name:        "Database repair",
		Description: "Database repair was run",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseFlush: {
		Name:        "Database flush",
		Description: "Database flush was run",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserCreate: {
		Name:        "Create user",
		Description: "A new user was created",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
			AuditFieldDatabase: "database name",
			"roles":            []string{"list", "of", "roles"},
			"channels":         map[string]map[string][]string{"scopeName": {"collectionName": {"list", "of", "channels"}}},
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserRead: {
		Name:        "Read user",
		Description: "Information about this user was viewed",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
			AuditFieldDatabase: "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserUpdate: {
		Name:        "Update user",
		Description: "User was updated",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
			AuditFieldDatabase: "database name",
			"roles":            []string{"list", "of", "roles"},
			"channels":         map[string]map[string][]string{"scopeName": {"collectionName": {"list", "of", "channels"}}},
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDUserDelete: {
		Name:        "Delete user",
		Description: "User was deleted",
		MandatoryFields: AuditFields{
			AuditFieldUserName: "username",
			AuditFieldDatabase: "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDUsersAll: {
		Name:        "Read all users",
		Description: "All users were viewed",
		MandatoryFields: AuditFields{
			"db":        "database name",
			"usernames": []string{"list", "of", "usernames"},
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDRoleCreate: {
		Name:        "Create role",
		Description: "A new role was created",
		MandatoryFields: AuditFields{
			"role":           "role_name",
			"db":             "database name",
			"admin_channels": map[string]map[string][]string{"scopeName": {"collectionName": {"list", "of", "channels"}}},
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
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
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDRoleUpdate: {
		Name:        "Update role",
		Description: "Role was updated",
		MandatoryFields: AuditFields{
			"role":           "role_name",
			"db":             "database name",
			"admin_channels": map[string]map[string][]string{"scopeName": {"collectionName": {"list", "of", "channels"}}},
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
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
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDRolesAll: {
		Name:        "Read all roles",
		Description: "All roles were viewed",
		MandatoryFields: AuditFields{
			"db":    "database name",
			"roles": []string{"list", "of", "roles"},
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDChangesFeedStarted: {
		Name:        "Changes feed started",
		Description: "Changes feed was started",
		MandatoryFields: AuditFields{
			AuditFieldSince: "since",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		OptionalFields: AuditFields{
			AuditFieldFilter:      "filter",
			AuditFieldDocIDs:      []string{"list", "of", "doc_ids"},
			AuditFieldChannels:    []string{"list", "of", "channels"},
			AuditFieldFeedType:    "continuous, normal, longpoll, websocket, etc.",
			AuditFieldIncludeDocs: true,
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDReplicationConnect: {
		Name:        "Replication connect",
		Description: "A replication client connected",
		MandatoryFields: AuditFields{
			"client_type": "isgr, cbl, other",
		},
		OptionalFields: AuditFields{
			"client_version": "client version",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDReplicationDisconnect: {
		Name:        "Replication disconnect",
		Description: "A replication client disconnected",
		MandatoryFields: AuditFields{
			"client_type": "isgr, cbl, other",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeUser,
	},
	AuditIDISGRCreate: {
		Name:        "Create Inter-Sync Gateway Replication",
		Description: "A new Inter-Sync Gateway Replication was created",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
			AuditFieldPayload:       "payload",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRRead: {
		Name:        "Read Inter-Sync Gateway Replication",
		Description: "Information about this Inter-Sync Gateway Replication was viewed",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRUpdate: {
		Name:        "Update Inter-Sync Gateway Replication",
		Description: "Inter-Sync Gateway Replication was updated",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
			AuditFieldPayload:       "payload",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRDelete: {
		Name:        "Delete Inter-Sync Gateway Replication",
		Description: "Inter-Sync Gateway Replication was deleted",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStatus: {
		Name:        "Inter-Sync Gateway Replication status",
		Description: "Inter-Sync Gateway Replication status was document viewed",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStart: {
		Name:        "Inter-Sync Gateway Replication start",
		Description: "Inter-Sync Gateway Replication was started",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRStop: {
		Name:        "Inter-Sync Gateway Replication stop",
		Description: "Inter-Sync Gateway Replication was stopped",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRReset: {
		Name:        "Inter-Sync Gateway Replication reset",
		Description: "Inter-Sync Gateway Replication was reset",
		MandatoryFields: AuditFields{
			AuditFieldReplicationID: "replication id",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRAllStatus: {
		Name:        "All Inter-Sync Gateway Replication status",
		Description: "All Inter-Sync Gateway Replication statuses were viewed",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDISGRAllRead: {
		Name:        "Read all Inter-Sync Gateway Replications",
		Description: "All Inter-Sync Gateway Replications were viewed",
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupDatabase,
			fieldGroupRequest,
			// fieldGroupAuthenticated, // FIXME: CBG-3973
		},
		EnabledByDefault:   true,
		FilteringPermitted: true,
		EventType:          eventTypeAdmin,
	},
	AuditIDDocumentCreate: {
		Name:        "Create document",
		Description: "A new document was created",
		MandatoryFields: AuditFields{
			AuditFieldDocID:      "document id",
			AuditFieldDocVersion: "revision ID",
			AuditFieldChannels:   []string{"list", "of", "channels"},
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentRead: {
		Name:        "Read document",
		Description: "A document was viewed",
		MandatoryFields: AuditFields{
			AuditFieldDocID:      "document id",
			AuditFieldDocVersion: "revision ID",
		},
		mandatoryFieldGroups: []fieldGroup{
			// fieldGroupAuthenticated, // FIXME: CBG-3973
			fieldGroupDatabase,
			fieldGroupKeyspace,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentUpdate: {
		Name:        "Update document",
		Description: "A document was updated",
		MandatoryFields: AuditFields{
			AuditFieldDocID:      "document id",
			AuditFieldDocVersion: "revision ID",
			AuditFieldChannels:   []string{"list", "of", "channels"},
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentMetadataRead: {
		Name:        "Read document metadata",
		Description: "Document metadata was viewed",
		MandatoryFields: AuditFields{
			AuditFieldDocID: "document id",
		},
		mandatoryFieldGroups: []fieldGroup{
			// fieldGroupAuthenticated, // FIXME: CBG-3973
			fieldGroupDatabase,
			fieldGroupKeyspace,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDDocumentDelete: {
		Name:        "Delete document",
		Description: "A document was deleted",
		MandatoryFields: AuditFields{
			AuditFieldDocID: "document id",
		},
		OptionalFields: AuditFields{
			AuditFieldDocVersion: "revision ID",                      // these are set when purged: false
			AuditFieldChannels:   []string{"list", "of", "channels"}, // these are set when purged: false
			AuditFieldPurged:     true,
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentCreate: {
		Name:        "Create attachment",
		Description: "A new attachment was created",
		MandatoryFields: AuditFields{
			AuditFieldDocID:        "document id",
			AuditFieldDocVersion:   "revision ID",
			AuditFieldAttachmentID: "attachment name",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
			fieldGroupRequest,
		},

		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentRead: {

		Name:        "Read attachment",
		Description: "An attachment was viewed",
		MandatoryFields: AuditFields{
			AuditFieldDocID:        "document id",
			AuditFieldDocVersion:   "revision ID",
			AuditFieldAttachmentID: "attachment name",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
			fieldGroupRequest,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentUpdate: {
		Name:        "Update attachment",
		Description: "An attachment was updated",
		MandatoryFields: AuditFields{
			AuditFieldDocID:        "document id",
			AuditFieldDocVersion:   "revision ID",
			AuditFieldAttachmentID: "attachment name",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
			fieldGroupRequest,
		},
		EnabledByDefault:   false,
		FilteringPermitted: true,
		EventType:          eventTypeData,
	},
	AuditIDAttachmentDelete: {
		Name:        "Delete attachment",
		Description: "An attachment was deleted",
		MandatoryFields: AuditFields{
			AuditFieldDocID:        "document id",
			AuditFieldDocVersion:   "revision ID",
			AuditFieldAttachmentID: "attachment name",
		},
		mandatoryFieldGroups: []fieldGroup{
			fieldGroupAuthenticated,
			fieldGroupDatabase,
			fieldGroupKeyspace,
			fieldGroupRequest,
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
		descriptor.expandMandatoryFieldGroups(descriptor.mandatoryFieldGroups)
	}
}

// AllAuditEventIDs is a list of all audit event IDs.
var AllAuditeventIDs = buildAllAuditIDList(AuditEvents)

func buildAllAuditIDList(e events) (ids []uint) {
	ids = make([]uint, 0, len(e))
	for k := range e {
		ids = append(ids, uint(k))
	}
	return ids
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
