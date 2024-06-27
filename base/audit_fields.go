package base

import (
	"context"
	"time"
)

const (
	// fields set on all audit events
	auditFieldID          = "id"
	auditFieldTimestamp   = "timestamp"
	auditFieldName        = "name"
	auditFieldDescription = "description"

	// commonly used fields
	auditFieldRealUserID    = "real_userid"
	auditFieldLocal         = "local"
	auditFieldRemote        = "remote"
	auditFieldDatabase      = "db"
	auditFieldCorrelationID = "cid"
	auditFieldKeyspace      = "ks"
)

func (f AuditFields) withCommonMandatoryFields() {
	if f == nil {
		f = make(AuditFields)
	}
	f[auditFieldID] = 0
	f[auditFieldTimestamp] = "event timestamp"
	f[auditFieldName] = "event name"
	f[auditFieldDescription] = "event description"
}

func (f AuditFields) withCommonOptionalFields() {
	if f == nil {
		f = make(AuditFields)
	}
	f[auditFieldRealUserID] = map[string]any{
		"domain": "user domain",
		"user":   "user id",
	}
	f[auditFieldLocal] = "local ip"
	f[auditFieldRemote] = "remote ip"
	f[auditFieldDatabase] = "database name"
	f[auditFieldCorrelationID] = "correlation id"
	f[auditFieldKeyspace] = "keyspace"
}

// expandFields populates data with information from the id, context and additionalData.
func expandFields(id AuditID, ctx context.Context, additionalData AuditFields) AuditFields {
	var fields AuditFields
	if additionalData != nil {
		fields = additionalData
	} else {
		fields = make(AuditFields)
	}

	// static event data
	fields[auditFieldID] = uint64(id)
	fields[auditFieldName] = AuditEvents[id].Name
	fields[auditFieldDescription] = AuditEvents[id].Description

	// context data
	logCtx := getLogCtx(ctx)
	if logCtx.Database != "" {
		fields[auditFieldDatabase] = logCtx.Database
	}
	if logCtx.CorrelationID != "" {
		fields[auditFieldCorrelationID] = logCtx.CorrelationID
	}
	if logCtx.Bucket != "" && logCtx.Scope != "" && logCtx.Collection != "" {
		fields[auditFieldKeyspace] = FullyQualifiedCollectionName(logCtx.Bucket, logCtx.Scope, logCtx.Collection)
	}
	// TODO: CBG-3973 - Pull fields from ctx
	userDomain := "placeholder"
	userID := "placeholder"
	if userDomain != "" && userID != "" {
		fields[auditFieldRealUserID] = map[string]any{
			"domain": userDomain,
			"user":   userID,
		}
	}
	localIP := "192.0.2.1"
	localPort := "4984"
	if localIP != "" && localPort != "" {
		fields[auditFieldLocal] = map[string]any{
			"ip":   localIP,
			"port": localPort,
		}
	}
	remoteIP := "203.0.113.1"
	remotePort := "12345"
	if remoteIP != "" && remotePort != "" {
		fields[auditFieldRemote] = map[string]any{
			"ip":   remoteIP,
			"port": remotePort,
		}
	}

	fields[auditFieldTimestamp] = time.Now()

	// TODO: CBG-3976 - Inject and merge data from env var
	// TODO: CBG-3977 - Inject and merge data from request header

	return fields
}
