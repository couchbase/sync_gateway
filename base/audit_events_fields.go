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
	auditFieldID                 = "id"
	auditFieldTimestamp          = "timestamp"
	auditFieldName               = "name"
	auditFieldDescription        = "description"
	auditFieldRealUserID         = "real_userid"
	auditFieldLocal              = "local"
	auditFieldRemote             = "remote"
	auditFieldDatabase           = "db"
	auditFieldCorrelationID      = "cid" // FIXME: how to distinguish between this field (http) and blip id below
	auditFieldKeyspace           = "ks"
	AuditFieldReplicationID      = "replication_id"
	AuditFieldPayload            = "payload"
	AuditFieldCompactionType     = "type"
	AuditFieldCompactionDryRun   = "dry_run"
	AuditFieldCompactionReset    = "reset"
	AuditFieldPostUpgradePreview = "preview"

	// AuditIDSyncGatewayStartup AuditID = 53260
	AuditFieldSGVersion                      = "sg_version"
	AuditFieldUseTLSServer                   = "use_tls_server"
	AuditFieldServerTLSSkipVerify            = "server_tls_skip_verify"
	AuditFieldAdminInterfaceAuthentication   = "admin_interface_authentication"
	AuditFieldMetricsInterfaceAuthentication = "metrics_interface_authentication"
	AuditFieldLogFilePath                    = "log_file_path"
	AuditFieldBcryptCost                     = "bcrypt_cost"
	AuditFieldDisablePersistentConfig        = "disable_persistent_config"
)
