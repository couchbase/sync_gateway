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
	AuditFieldID                 = "id"
	AuditFieldTimestamp          = "timestamp"
	AuditFieldName               = "name"
	AuditFieldDescription        = "description"
	AuditFieldRealUserID         = "real_userid"
	AuditFieldLocal              = "local"
	AuditFieldRemote             = "remote"
	AuditFieldDatabase           = "db"
	AuditFieldCorrelationID      = "cid" // FIXME: how to distinguish between this field (http) and blip id below
	AuditFieldKeyspace           = "ks"
	AuditFieldReplicationID      = "replication_id"
	AuditFieldPayload            = "payload"
	AuditFieldCompactionType     = "type"
	AuditFieldCompactionDryRun   = "dry_run"
	AuditFieldCompactionReset    = "reset"
	AuditFieldPostUpgradePreview = "preview"
	AuditFieldAuthMethod         = "auth_method"
	AuditFieldAuditScope         = "audit_scope"
	AuditFieldFileName           = "filename"

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
	AuditFieldRequestBody = "request_body"
)
