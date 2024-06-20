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
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	auditLogName = "audit"
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

// Audit creates and logs an audit event for the given ID and a set of additional data associated with the request.
func Audit(ctx context.Context, id AuditID, additionalData AuditFields) {
	var fields AuditFields

	if IsDevMode() {
		// NOTE: This check is expensive and indicates a dev-time mistake that needs addressing.
		// Don't bother in production code, but also delay expandFields until we know we will log.
		fields = expandFields(id, ctx, additionalData)
		id.MustValidateFields(fields)
	}

	if auditLogger == nil || !auditLogger.Enabled.IsTrue() {
		return
	}
	logCtx := getLogCtx(ctx)
	if logCtx.DbLogConfig != nil && logCtx.DbLogConfig.Audit != nil {
		if _, ok := logCtx.DbLogConfig.Audit.EnabledEvents[id]; !ok {
			return
		}
	}

	// delayed expansion until after enabled checks in non-dev mode
	if fields == nil {
		fields = expandFields(id, ctx, additionalData)
	}
	attrs := fields.toSlogAttrs()
	auditLogger.sl.LogAttrs(ctx, slog.LevelInfo, "", attrs...)
}

// AuditLogger is a file logger with audit-specific behaviour.
type AuditLogger struct {
	FileLogger

	sl *slog.Logger

	// AuditLoggerConfig stores the initial config used to instantiate AuditLogger
	config AuditLoggerConfig
}

// NewAuditLogger returns a new AuditLogger from a config.
func NewAuditLogger(ctx context.Context, config *AuditLoggerConfig, logFilePath string, minAge int, buffer *strings.Builder) (*AuditLogger, error) {
	if config == nil {
		config = &AuditLoggerConfig{}
	}

	// validate and set defaults
	if err := config.init(ctx, LevelNone, auditLogName, logFilePath, minAge); err != nil {
		return nil, err
	}

	// removeKeys returns a function suitable for HandlerOptions.ReplaceAttr
	// that removes all Attrs with the given keys.
	removeKeys := func(keys ...string) func([]string, slog.Attr) slog.Attr {
		return func(_ []string, a slog.Attr) slog.Attr {
			for _, k := range keys {
				if a.Key == k {
					return slog.Attr{}
				}
			}
			return a
		}
	}

	// TODO: May end up removing slog and doing manual JSON marshalling -> write to file logger.
	h := slog.NewJSONHandler(config.Output, &slog.HandlerOptions{
		// strip all builtin slog keys
		ReplaceAttr: removeKeys(slog.TimeKey, slog.MessageKey, slog.LevelKey, slog.SourceKey),
	})
	sl := slog.New(h)

	logger := &AuditLogger{
		FileLogger: FileLogger{
			Enabled: AtomicBool{},
			level:   LevelNone,
			name:    auditLogName,
			output:  config.Output,
			logger:  log.New(config.Output, "", 0),
		},
		config: *config,
		sl:     sl,
	}
	logger.Enabled.Set(*config.Enabled)

	if buffer != nil {
		logger.buffer = *buffer
	}

	// Only create the collateBuffer channel and worker if required.
	if *config.CollationBufferSize > 1 {
		logger.collateBuffer = make(chan string, *config.CollationBufferSize)
		logger.flushChan = make(chan struct{}, 1)
		logger.collateBufferWg = &sync.WaitGroup{}

		// Start up a single worker to consume messages from the buffer
		go logCollationWorker(logger.collateBuffer, logger.flushChan, logger.collateBufferWg, logger.logger, *config.CollationBufferSize, fileLoggerCollateFlushTimeout)
	}

	return logger, nil
}
