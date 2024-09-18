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
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	auditLogName = "audit"
	// DefaultAuditEnabled is the default value for whether auditing is enabled globally.
	defaultAuditEnabled = false
	// DefaultDbAuditEnabled is the default value for whether database-level auditing is enabled.
	DefaultDbAuditEnabled = false
)

// expandFieldsNumItems is the number of items `expandFields` can add, plus the variable length inputs (global, request, additional).
const expandFieldsNumItems = 11

// expandFields populates data with information from the id, context and additionalData.
func expandFields(id AuditID, ctx context.Context, globalFields AuditFields, additionalData AuditFields) AuditFields {
	logCtx := getLogCtx(ctx)

	// pre-allocate fields to return, allocates once now for the copy but should be big enough for everything we'll add
	expectedLen := expandFieldsNumItems + len(globalFields) + len(additionalData) + len(logCtx.RequestAdditionalAuditFields)
	fields := make(AuditFields, expectedLen)
	if additionalData != nil {
		maps.Copy(fields, additionalData)
	}

	// static event data
	fields[AuditFieldID] = uint32(id)
	fields[AuditFieldName] = AuditEvents[id].Name
	fields[AuditFieldDescription] = AuditEvents[id].Description

	// context data
	if logCtx.Database != "" {
		fields[AuditFieldDatabase] = logCtx.Database
	}
	if logCtx.CorrelationID != "" {
		fields[AuditFieldCorrelationID] = logCtx.CorrelationID
	}
	if logCtx.Database != "" && logCtx.Scope != "" && logCtx.Collection != "" {
		fields[AuditFieldKeyspace] = FullyQualifiedCollectionName(logCtx.Database, logCtx.Scope, logCtx.Collection)
	}
	userDomain := logCtx.UserDomain
	userName := logCtx.Username
	if userDomain != "" || userName != "" {
		fields[AuditFieldRealUserID] = json.RawMessage(`{"` +
			AuditFieldRealUserIDDomain + `":"` + string(userDomain) + `","` +
			AuditFieldRealUserIDUser + `":"` + userName +
			`"}`)
	}
	effectiveDomain := logCtx.EffectiveDomain
	effectiveUser := logCtx.EffectiveUserID
	if effectiveDomain != "" || effectiveUser != "" {
		fields[AuditEffectiveUserID] = json.RawMessage(`{"` +
			AuditFieldEffectiveUserIDDomain + `":"` + effectiveDomain + `","` +
			AuditFieldEffectiveUserIDUser + `":"` + effectiveUser +
			`"}`)
	}
	if logCtx.RequestHost != "" {
		host, port, err := net.SplitHostPort(logCtx.RequestHost)
		if err != nil {
			AssertfCtx(ctx, "couldn't parse request host %q: %v", logCtx.RequestHost, err)
		} else {
			fields[AuditFieldLocal] = json.RawMessage(`{"ip":"` + host + `","port":"` + port + `"}`)
		}
	}

	if logCtx.RequestRemoteAddr != "" {
		host, port, err := net.SplitHostPort(logCtx.RequestRemoteAddr)
		if err != nil {
			AssertfCtx(ctx, "couldn't parse request remote addr %q: %v", logCtx.RequestRemoteAddr, err)
		} else {
			fields[AuditFieldRemote] = json.RawMessage(`{"ip":"` + host + `","port":"` + port + `"}`)
		}
	}

	fields[AuditFieldTimestamp] = time.Now().Format(time.RFC3339)

	fields = fields.merge(ctx, globalFields)
	fields = fields.merge(ctx, logCtx.RequestAdditionalAuditFields)

	return fields
}

// Merge will perform a shallow overwrite of the fields in the AuditFields. If there are conflicts, do not overwrite but log a warning. This will panic in dev mode.
func (f AuditFields) merge(ctx context.Context, overwrites AuditFields) AuditFields {
	var duplicateFields []string
	for k := range overwrites {
		if _, ok := f[k]; ok {
			duplicateFields = append(duplicateFields, fmt.Sprintf("%q='%v'", k, overwrites[k]))
			continue
		}
		f[k] = overwrites[k]
	}
	if duplicateFields != nil {
		WarnfCtx(ctx, "audit fields %s already exist in base audit fields %+v, will not overwrite an audit event", strings.Join(duplicateFields, ","), f)
	}
	return f
}

// Audit creates and logs an audit event for the given ID and a set of additional data associated with the request.
func Audit(ctx context.Context, id AuditID, additionalData AuditFields) {
	var fields AuditFields

	logger := auditLogger.Load()
	if IsDevMode() {
		// NOTE: This check is expensive and indicates a dev-time mistake that needs addressing.
		// Don't bother in production code, but also delay expandFields until we know we will log.
		var globalFields AuditFields
		if logger != nil {
			globalFields = logger.globalFields
		}
		fields = expandFields(id, ctx, globalFields, additionalData)
		id.MustValidateFields(fields)
	}

	if !logger.shouldLog(id, ctx) {
		return
	}

	// delayed expansion until after enabled checks in non-dev mode
	if fields == nil {
		fields = expandFields(id, ctx, logger.globalFields, additionalData)
	}

	fieldsJSON, err := jsoniter.MarshalToString(fields)
	if err != nil {
		AssertfCtx(ctx, "failed to marshal audit fields: %v", err)
		return
	}

	logger.logf(fieldsJSON)
	SyncGatewayStats.GlobalStats.AuditStat.NumAuditsLogged.Add(1)
}

// IsAuditEnabled checks if auditing is enabled for the SG node
func IsAuditEnabled() bool {
	logger := auditLogger.Load()
	if logger == nil {
		return false
	}
	return logger.FileLogger.shouldLog(LevelNone)
}

// AuditLogger is a file logger with audit-specific behaviour.
type AuditLogger struct {
	FileLogger

	// AuditLoggerConfig stores the initial config used to instantiate AuditLogger
	config        AuditLoggerConfig
	globalFields  map[string]any
	enabledEvents map[AuditID]struct{}
}

func (l *AuditLogger) getAuditLoggerConfig() *AuditLoggerConfig {
	c := AuditLoggerConfig{}
	if l != nil {
		// Copy config struct to avoid mutating running config
		c = l.config
		c.FileLoggerConfig = *l.getFileLoggerConfig()
	}

	return &c
}

// NewAuditLogger returns a new AuditLogger from a config.
func NewAuditLogger(ctx context.Context, config *AuditLoggerConfig, logFilePath string, minAge int, buffer *strings.Builder, globalFields map[string]any) (*AuditLogger, error) {
	if config == nil {
		config = &AuditLoggerConfig{}
	}
	if config.EnabledEvents == nil {
		config.EnabledEvents = DefaultGlobalAuditEventIDs
	}

	if config.FileLoggerConfig.Enabled == nil {
		config.FileLoggerConfig.Enabled = BoolPtr(defaultAuditEnabled)
	}

	if config.CollationBufferSize == nil {
		config.CollationBufferSize = IntPtr(defaultFileLoggerCollateBufferSize)
	}

	fl, err := NewFileLogger(ctx, &config.FileLoggerConfig, LevelNone, auditLogName, logFilePath, minAge, buffer)
	if err != nil {
		return nil, err
	}

	var me *MultiError
	enabledEvents := make(map[AuditID]struct{})
	for _, id := range config.EnabledEvents {
		auditID := AuditID(id)
		if e, ok := AuditEvents[auditID]; !ok {
			me = me.Append(fmt.Errorf("unknown audit event ID %d", auditID))
		} else if !e.IsGlobalEvent {
			me = me.Append(fmt.Errorf("audit event ID %d %q can only be configured at the database level", auditID, e.Name))
		} else {
			enabledEvents[auditID] = struct{}{}
		}
	}
	if err := me.ErrorOrNil(); err != nil {
		return nil, err
	}

	for id := range NonFilterableAuditEventsForGlobal {
		// non-filterable events are always enabled by definition
		enabledEvents[id] = struct{}{}
	}

	logger := &AuditLogger{
		FileLogger:    *fl,
		config:        *config,
		globalFields:  globalFields,
		enabledEvents: enabledEvents,
	}

	if *config.FileLoggerConfig.Enabled {
		Audit(ctx, AuditIDAuditEnabled, AuditFields{AuditFieldAuditScope: "global", AuditFieldEnabledEvents: config.EnabledEvents})
	}

	return logger, nil
}

func (al *AuditLogger) shouldLog(id AuditID, ctx context.Context) bool {
	if al == nil {
		return false
	} else if !al.FileLogger.shouldLog(LevelNone) {
		return false
	}

	if !AuditEvents[id].FilteringPermitted {
		return true
	}

	isGlobal := AuditEvents[id].IsGlobalEvent
	if isGlobal {
		if _, ok := al.enabledEvents[id]; !ok {
			return false
		}
	}

	logCtx := getLogCtx(ctx)
	if logCtx.DbLogConfig != nil && logCtx.DbLogConfig.Audit != nil {
		if !logCtx.DbLogConfig.Audit.Enabled {
			return false
		}
		if !isGlobal {
			if _, ok := logCtx.DbLogConfig.Audit.EnabledEvents[id]; !ok {
				return false
			}
		}
		if !shouldLogAuditEventForUserAndRole(&logCtx) {
			return false
		}
	}

	return true
}

// shouldLogAuditEventForUserAndRole returns true if the request should be logged
func shouldLogAuditEventForUserAndRole(logCtx *LogContext) bool {
	if (logCtx.UserDomain == "" && logCtx.Username == "") ||
		len(logCtx.DbLogConfig.Audit.DisabledRoles) == 0 && len(logCtx.DbLogConfig.Audit.DisabledUsers) == 0 {
		// early return for common cases: no user on context or no disabled users or roles
		return true
	}

	if logCtx.UserDomain != "" && logCtx.Username != "" {
		if _, isDisabled := logCtx.DbLogConfig.Audit.DisabledUsers[AuditLoggingPrincipal{
			Domain: string(logCtx.UserDomain),
			Name:   logCtx.Username,
		}]; isDisabled {
			SyncGatewayStats.GlobalStats.AuditStat.NumAuditsFilteredByUser.Add(1)
			return false
		}
	}

	// if any of the user's roles are disabled, then don't log the event
	for role := range logCtx.UserRolesForAuditFiltering {
		if _, isDisabled := logCtx.DbLogConfig.Audit.DisabledRoles[AuditLoggingPrincipal{
			Domain: string(logCtx.UserDomain),
			Name:   role,
		}]; isDisabled {
			SyncGatewayStats.GlobalStats.AuditStat.NumAuditsFilteredByRole.Add(1)
			return false
		}
	}

	return true
}
