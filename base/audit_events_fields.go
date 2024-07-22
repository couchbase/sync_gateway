// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

const (

	// Commonly used audit event fields

	AuditFieldID               = "id"
	AuditFieldTimestamp        = "timestamp"
	AuditFieldName             = "name"
	AuditFieldDescription      = "description"
	AuditFieldRealUserID       = "real_userid"
	AuditFieldRealUserIDDomain = "domain"
	AuditFieldRealUserIDUser   = "user"
	AuditFieldLocal            = "local"
	AuditFieldRemote           = "remote"
	AuditFieldDatabase         = "db"
	AuditFieldCorrelationID    = "cid" // FIXME: how to distinguish between this field (http) and blip id below
	AuditFieldKeyspace         = "ks"
	AuditFieldAuthMethod       = "auth_method"
	AuditFieldUserName         = "username"

	AuditFieldReplicationID      = "replication_id"
	AuditFieldPayload            = "payload"
	AuditFieldCompactionType     = "type"
	AuditFieldCompactionDryRun   = "dry_run"
	AuditFieldCompactionReset    = "reset"
	AuditFieldPostUpgradePreview = "preview"

	AuditEffectiveUserID            = "effective_userid"
	AuditFieldEffectiveUserIDDomain = "domain"
	AuditFieldEffectiveUserIDUser   = "user"
	AuditFieldAuditScope            = "audit_scope"
	AuditFieldFileName              = "filename"
	AuditFieldDBNames               = "db_names"

	// AuditIDSyncGatewayStartup AuditID = 53260
	AuditFieldSGVersion                      = "sg_version"
	AuditFieldUseTLSServer                   = "use_tls_server"
	AuditFieldServerTLSSkipVerify            = "server_tls_skip_verify"
	AuditFieldAdminInterfaceAuthentication   = "admin_interface_authentication"
	AuditFieldMetricsInterfaceAuthentication = "metrics_interface_authentication"
	AuditFieldLogFilePath                    = "log_file_path"
	AuditFieldBcryptCost                     = "bcrypt_cost"
	AuditFieldDisablePersistentConfig        = "disable_persistent_config"

	// AuditIDSyncGatewayStats             AuditID = 53303
	AuditFieldStatsFormat = "stats_format"

	//AuditIDSyncGatewayProfiling         AuditID = 53304
	AuditFieldPprofProfileType = "profile_type"

	// API events  AuditID = 53270, 53271, 53272
	AuditFieldHTTPMethod  = "http_method"
	AuditFieldHTTPPath    = "http_path"
	AuditFieldHTTPStatus  = "http_status"
	AuditFieldRequestBody = "request_body"

	// CRUD events
	AuditFieldAttachmentID = "attachment_id"
	AuditFieldChannels     = "channels"
	AuditFieldDocID        = "doc_id"
	AuditFieldDocVersion   = "doc_version"
	AuditFieldPurged       = "purged"

	// Session events 53282, 53283
	AuditFieldSessionID = "session_id"

	// AuditIDChangesFeedStarted AuditID = 54200
	AuditFieldSince       = "since"
	AuditFieldFilter      = "filter"
	AuditFieldDocIDs      = "doc_ids"
	AuditFieldFeedType    = "feed_type"
	AuditFieldIncludeDocs = "include_docs"
)
