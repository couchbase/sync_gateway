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

const (
	AuditIDAuditEnabled AuditID = 53248

	AuditIDPublicUserAuthenticated        AuditID = 53260
	AuditIDPublicUserAuthenticationFailed AuditID = 53261

	AuditIDReadDatabase AuditID = 53301

	// Database operation events
	AuditIDDatabaseOffline       AuditID = 54020
	AuditIDDatabaseOnline        AuditID = 54021
	AuditIDDatabaseCompactStatus AuditID = 54030
	AuditIDDatabaseCompactStart  AuditID = 54031
	AuditIDDatabaseCompactStop   AuditID = 54032
	AuditIDDatabaseResyncStatus  AuditID = 54040
	AuditIDDatabaseResyncStart   AuditID = 54041
	AuditIDDatabaseResyncStop    AuditID = 54042
	AuditIDDatabasePostUpgrade   AuditID = 54043
	AuditIDDatabaseRepair        AuditID = 54044
	AuditIDDatabaseFlush         AuditID = 54045
)

// AuditEvents is a table of audit events created by Sync Gateway.
//
// This is used to generate:
//   - events themselves
//   - a kv-auditd-compatible descriptor with TestGenerateAuditdModuleDescriptor
//   - CSV output for each event to be used to document
var AuditEvents = events{
	AuditIDAuditEnabled: {}, // TODO: unreferenced event - somehow find this mistake via test or lint!
	AuditIDReadDatabase: {
		Name:        "Read database",
		Description: "Information about this database was read.",
		MandatoryFields: AuditFields{
			"db": "database_name",
		},
		OptionalFields:     AuditFields{},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserAuthenticated: {
		Name:        "User authenticated",
		Description: "User successfully authenticated",
		MandatoryFields: AuditFields{
			"method": "auth_method", // e.g. "basic", "oidc", "cookie", ...
		},
		OptionalFields: AuditFields{
			"oidc_issuer": "issuer",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDPublicUserAuthenticationFailed: {
		Name:        "User authentication failed",
		Description: "User authentication failed",
		MandatoryFields: AuditFields{
			"method": "auth_method", // e.g. "basic", "oidc", "cookie", ...
		},
		OptionalFields: AuditFields{
			"username": "username",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeUser,
	},
	AuditIDDatabaseOffline: {
		Name:        "Database offline",
		Description: "Database was taken offline",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseOnline: {
		Name:        "Database online",
		Description: "Database was brought online",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStatus: {
		Name:        "Database compaction status",
		Description: "Database compaction status was viewed",
		MandatoryFields: AuditFields{
			"db":              "database name",
			"compaction_type": "tombstone or attachment",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStart: {
		Name:        "Database compaction start",
		Description: "Database compaction was started",
		MandatoryFields: AuditFields{
			"db":              "database name",
			"compaction_type": "tombstone or attachment",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseCompactStop: {
		Name:        "Database compaction stop",
		Description: "Database compaction was stopped",
		MandatoryFields: AuditFields{
			"db":              "database name",
			"compaction_type": "tombstone or attachment",
		},
		OptionalFields: AuditFields{
			"dry_run": "true or false",
			"reset":   "true, false, or none (resume when able)",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStatus: {
		Name:        "Database resync status",
		Description: "Database resync status was viewed",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStart: {
		Name:        "Database resync start",
		Description: "Database resync was started",
		MandatoryFields: AuditFields{
			"db":                   "database name",
			"collections":          map[string][]string{"scopeName": []string{"list", "of", "collections"}},
			"regenerate_sequences": true,
			"reset":                "true, false, or none (resume when able)",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseResyncStop: {
		Name:        "Database resync stop",
		Description: "Database resync was stopped",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabasePostUpgrade: {
		Name:        "Database post-upgrade",
		Description: "Database post-upgrade was run",
		MandatoryFields: AuditFields{
			"db":      "database name",
			"preview": "true or false",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseRepair: {
		Name:        "Database repair",
		Description: "Database repair was run",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
	AuditIDDatabaseFlush: {
		Name:        "Database flush",
		Description: "Database flush was run",
		MandatoryFields: AuditFields{
			"db": "database name",
		},
		EnabledByDefault:   true,
		FilteringPermitted: false,
		EventType:          eventTypeAdmin,
	},
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
