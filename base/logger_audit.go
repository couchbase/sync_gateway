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
	"net"
	"strings"
	"time"
)

const (
	auditLogName        = "audit"
	defaultAuditEnabled = false
)

// expandFields populates data with information from the id, context and additionalData.
func expandFields(id AuditID, ctx context.Context, globalFields AuditFields, additionalData AuditFields) AuditFields {
	var fields AuditFields
	if additionalData != nil {
		fields = additionalData
	} else {
		fields = make(AuditFields)
	}

	// static event data
	fields[AuditFieldID] = uint64(id)
	fields[AuditFieldName] = AuditEvents[id].Name
	fields[AuditFieldDescription] = AuditEvents[id].Description

	// context data
	logCtx := getLogCtx(ctx)
	if logCtx.Database != "" {
		fields[AuditFieldDatabase] = logCtx.Database
	}
	if logCtx.CorrelationID != "" {
		fields[AuditFieldCorrelationID] = logCtx.CorrelationID
	}
	if logCtx.Bucket != "" && logCtx.Scope != "" && logCtx.Collection != "" {
		fields[AuditFieldKeyspace] = FullyQualifiedCollectionName(logCtx.Bucket, logCtx.Scope, logCtx.Collection)
	}
	userDomain := logCtx.UserDomain
	userName := logCtx.Username
	if userDomain != "" || userName != "" {
		fields[AuditFieldRealUserID] = map[string]any{
			"domain": userDomain,
			"user":   userName,
		}
	}
	effectiveDomain := logCtx.EffectiveDomain
	effectiveUser := logCtx.EffectiveUserID
	if effectiveDomain != "" || effectiveUser != "" {
		fields[AuditEffectiveUserID] = map[string]any{
			"domain": effectiveDomain,
			"user":   effectiveUser,
		}
	}
	if logCtx.RequestHost != "" {
		host, port, err := net.SplitHostPort(logCtx.RequestHost)
		if err != nil {
			AssertfCtx(ctx, "couldn't parse request host %q: %v", logCtx.RequestHost, err)
		} else {
			fields[AuditFieldLocal] = map[string]any{
				"ip":   host,
				"port": port,
			}
		}
	}

	if logCtx.RequestRemoteAddr != "" {
		host, port, err := net.SplitHostPort(logCtx.RequestRemoteAddr)
		if err != nil {
			AssertfCtx(ctx, "couldn't parse request remote addr %q: %v", logCtx.RequestRemoteAddr, err)
		} else {
			fields[AuditFieldRemote] = map[string]any{
				"ip":   host,
				"port": port,
			}
		}
	}

	fields[AuditFieldTimestamp] = time.Now()

	fields.merge(ctx, globalFields)
	fields.merge(ctx, logCtx.RequestAdditionalAuditFields)

	return fields
}

// Merge will perform a shallow overwrite of the fields in the AuditFields. If there are conflicts, do not overwrite but log a warning. This will panic in dev mode.
func (f *AuditFields) merge(ctx context.Context, overwrites AuditFields) {
	var duplicateFields []string
	for k, v := range overwrites {
		_, ok := (*f)[k]
		if ok {
			duplicateFields = append(duplicateFields, fmt.Sprintf("%q=%q", k, v))
			continue
		}
		(*f)[k] = v
	}
	if duplicateFields != nil {
		WarnfCtx(ctx, "audit fields %s already exist in base audit fields %+v, will not overwrite an audit event", strings.Join(duplicateFields, ","), *f)
	}
}

// Audit creates and logs an audit event for the given ID and a set of additional data associated with the request.
func Audit(ctx context.Context, id AuditID, additionalData AuditFields) {
	var fields AuditFields

	if IsDevMode() {
		// NOTE: This check is expensive and indicates a dev-time mistake that needs addressing.
		// Don't bother in production code, but also delay expandFields until we know we will log.
		fields = expandFields(id, ctx, auditLogger.globalFields, additionalData)
		id.MustValidateFields(fields)
	}

	if !auditLogger.shouldLog(id, ctx) {
		return
	}

	// delayed expansion until after enabled checks in non-dev mode
	if fields == nil {
		fields = expandFields(id, ctx, auditLogger.globalFields, additionalData)
	}
	fieldsJSON, err := JSONMarshalCanonical(fields)
	if err != nil {
		AssertfCtx(ctx, "failed to marshal audit fields: %v", err)
		return
	}
	auditLogger.logf(string(fieldsJSON))
}

// IsAuditEnabled checks if auditing is enabled for the SG node
func IsAuditEnabled() bool {
	return auditLogger.FileLogger.shouldLog(LevelNone)
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

	if *config.FileLoggerConfig.Enabled {
		Audit(ctx, AuditIDAuditEnabled, AuditFields{"audit_scope": "global"})
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
		if !shouldLogAuditEventForUserAndRole(&logCtx) {
			return false
		}
	}

	return true
}

// shouldLogAuditEventForUserAndRole returns true if the request should be logged
func shouldLogAuditEventForUserAndRole(logCtx *LogContext) bool {
	if logCtx.UserDomain == "" && logCtx.Username == "" ||
		len(logCtx.DbLogConfig.Audit.DisabledUsers) == 0 {
		// early return for common cases: no user on context or no disabled users or roles
		return true
	}

	if logCtx.UserDomain != "" && logCtx.Username != "" {
		if _, isDisabled := logCtx.DbLogConfig.Audit.DisabledUsers[AuditLoggingPrincipal{
			Domain: string(logCtx.UserDomain),
			Name:   logCtx.Username,
		}]; isDisabled {
			return false
		}
	}

	return true
}
