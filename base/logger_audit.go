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
)

const (
	auditLogName = "audit"
)

// Audit creates and logs an audit event for the given ID and a set of additional data associated with the request.
func Audit(ctx context.Context, id AuditID, additionalData AuditFields) {
	var fields AuditFields

	if IsDevMode() {
		// NOTE: This check is expensive and indicates a dev-time mistake that needs addressing.
		// Don't bother in production code, but also delay expandFields until we know we will log.
		fields = expandFields(id, ctx, additionalData)
		id.MustValidateFields(fields)
	}

	if !auditLogger.shouldLog(id, ctx) {
		return
	}

	// delayed expansion until after enabled checks in non-dev mode
	if fields == nil {
		fields = expandFields(id, ctx, additionalData)
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
	config AuditLoggerConfig
}

// NewAuditLogger returns a new AuditLogger from a config.
func NewAuditLogger(ctx context.Context, config *AuditLoggerConfig, logFilePath string, minAge int, buffer *strings.Builder) (*AuditLogger, error) {
	if config == nil {
		config = &AuditLoggerConfig{}
	}

	fl, err := NewFileLogger(ctx, &config.FileLoggerConfig, LevelNone, auditLogName, logFilePath, minAge, buffer)
	if err != nil {
		return nil, err
	}

	logger := &AuditLogger{
		FileLogger: *fl,
		config:     *config,
	}

	return logger, nil
}

func (al *AuditLogger) shouldLog(id AuditID, ctx context.Context) bool {
	if !auditLogger.FileLogger.shouldLog(LevelNone) {
		return false
	}
	logCtx := getLogCtx(ctx)
	if logCtx.DbLogConfig != nil && logCtx.DbLogConfig.Audit != nil {
		if _, ok := logCtx.DbLogConfig.Audit.EnabledEvents[id]; !ok {
			return false
		}
	}
	return true
}
