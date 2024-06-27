// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	auditLogName        = "audit"
	defaultAuditEnabled = false
)

// commonly used fields for audit events
const (
	auditFieldID            = "id"
	auditFieldTimestamp     = "timestamp"
	auditFieldName          = "name"
	auditFieldDescription   = "description"
	auditFieldRealUserID    = "real_userid"
	auditFieldLocal         = "local"
	auditFieldRemote        = "remote"
	auditFieldDatabase      = "db"
	auditFieldCorrelationID = "cid"
	auditFieldKeyspace      = "ks"
)

// expandFields populates data with information from the id, context and additionalData.
func expandFields(id AuditID, ctx context.Context, globalFields AuditFields, additionalData AuditFields) AuditFields {
	fields := auditLogger.baseFields()
	if additionalData != nil {
		for k, v := range additionalData {
			fields[k] = v
		}
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

// Audit creates and logs an audit event for the given ID and a set of additional data associated with the request.
func Audit(ctx context.Context, id AuditID, additionalData AuditFields) {
	var fields AuditFields

	if IsDevMode() {
		// NOTE: This check is expensive and indicates a dev-time mistake that needs addressing.
		// Don't bother in production code, but also delay expandFields until we know we will log.
		fields = expandFields(id, ctx, auditLogger.baseFields(), additionalData)
		id.MustValidateFields(fields)
	}

	if !auditLogger.shouldLog(id, ctx) {
		return
	}

	// delayed expansion until after enabled checks in non-dev mode
	if fields == nil {
		fields = expandFields(id, ctx, auditLogger.baseFields(), additionalData)
	}
	fieldsJSON, err := JSONMarshal(fields)
	if err != nil {
		if IsDevMode() {
			panic(fmt.Sprintf("failed to marshal audit fields: %v", err))
		} else {
			WarnfCtx(ctx, "failed to marshal audit fields: %v", err)
			return
		}
	}
	auditLogger.logf(string(fieldsJSON))
}

// AuditLogger is a file logger with audit-specific behaviour.
type AuditLogger struct {
	FileLogger

	// AuditLoggerConfig stores the initial config used to instantiate AuditLogger
	config       AuditLoggerConfig
	globalFields map[string]any
}

func (l *AuditLogger) getAuditLoggerConfig() *AuditLoggerConfig {
	c := AuditLoggerConfig{}
	if l != nil {
		// Copy config struct to avoid mutating running config
		c = l.config
	}

	c.FileLoggerConfig = *l.getFileLoggerConfig()

	return &c
}

// NewAuditLogger returns a new AuditLogger from a config.
func NewAuditLogger(ctx context.Context, config *AuditLoggerConfig, logFilePath string, minAge int, buffer *strings.Builder, globalFields map[string]any) (*AuditLogger, error) {
	if config == nil {
		config = &AuditLoggerConfig{}
	}

	if config.FileLoggerConfig.Enabled == nil {
		config.FileLoggerConfig.Enabled = BoolPtr(defaultAuditEnabled)
	}

	fl, err := NewFileLogger(ctx, &config.FileLoggerConfig, LevelNone, auditLogName, logFilePath, minAge, buffer)
	if err != nil {
		return nil, err
	}

	logger := &AuditLogger{
		FileLogger:   *fl,
		config:       *config,
		globalFields: globalFields,
	}

	return logger, nil
}

func (al *AuditLogger) shouldLog(id AuditID, ctx context.Context) bool {
	if !auditLogger.FileLogger.shouldLog(LevelNone) {
		return false
	}
	logCtx := getLogCtx(ctx)
	if logCtx.DbLogConfig != nil && logCtx.DbLogConfig.Audit != nil {
		if !logCtx.DbLogConfig.Audit.Enabled {
			return false
		}
		if _, ok := logCtx.DbLogConfig.Audit.EnabledEvents[id]; !ok {
			return false
		}
	}
	return true
}

// baseFields returns the fields that are common to all audit events.
func (al *AuditLogger) baseFields() AuditFields {
	if al == nil {
		return make(AuditFields)
	}
	fields := make(AuditFields, len(al.globalFields))
	for k, v := range al.globalFields {
		fields[k] = v
	}
	return fields
}
