// Copyright 2024-Pres	ent Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"reflect"
	"strconv"
)

// AuditID is a unique identifier for an audit event.
type AuditID uint

// String implements Stringer for AuditID
func (i AuditID) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

func ParseAuditID(s string) (AuditID, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	return AuditID(id), err
}

// events is a map of audit event IDs to event descriptors.
type events map[AuditID]*EventDescriptor

// EventDescriptor is an audit event. The fields closely (but not exactly) follows kv_engine's auditd descriptor implementation.
type EventDescriptor struct {
	// Name is a short textual Name of the event
	Name string
	// Description is a longer Name / Description of the event
	Description string
	// EnabledByDefault indicates whether the event should be enabled by default
	EnabledByDefault bool
	// FilteringPermitted indicates whether the event can be filtered or not
	FilteringPermitted bool
	// MandatoryFields describe field(s) required for a valid instance of the event
	MandatoryFields AuditFields
	// mandatoryFieldGroups is used to automatically fill MandatoryFields with groups of common fields
	mandatoryFieldGroups []fieldGroup

	// optionalFieldGroups is used to automatically fill OptionalFields with groups of common fields
	optionalFieldGroups []fieldGroup

	// The following fields are for documentation-use only.
	// OptionalFields describe optional field(s) valid in an instance of the event
	OptionalFields AuditFields
	// EventType represents a type of event
	EventType eventType

	// IsGlobalEvent indicates where the event can be enabled
	//  - true  = only via the bootstrap config
	//  - false = only via the database config
	IsGlobalEvent bool
}

type fieldGroup string

const (
	fieldGroupGlobal        fieldGroup = "global"
	fieldGroupRequest       fieldGroup = "request"
	fieldGroupAuthenticated fieldGroup = "authenticated"
	fieldGroupDatabase      fieldGroup = "database"
	fieldGroupKeyspace      fieldGroup = "keyspace"
)

// fieldsByGroup defines which fields are mandatory for each group.
var fieldsByGroup = map[fieldGroup]map[string]any{
	fieldGroupGlobal: {
		AuditFieldTimestamp:   "timestamp",
		AuditFieldID:          uint32(123),
		AuditFieldName:        "event name",
		AuditFieldDescription: "event description",
	},
	fieldGroupRequest: {
		AuditFieldLocal: map[string]any{
			"ip":   "local ip",
			"port": "1234"},
		AuditFieldRemote: map[string]any{
			"ip":   "remote ip",
			"port": "5678",
		},
		AuditFieldCorrelationID: "correlation_id",
	},
	fieldGroupAuthenticated: {
		AuditFieldRealUserID: map[string]any{
			AuditFieldRealUserIDDomain: "user domain",
			AuditFieldRealUserIDUser:   "user name",
		},
	},
	fieldGroupDatabase: {
		AuditFieldDatabase: "database name",
	},
	fieldGroupKeyspace: {
		AuditFieldDatabase: "database name",
		AuditFieldKeyspace: "keyspace",
	},
}

const (
	eventTypeAdmin eventType = "admin"
	eventTypeUser  eventType = "user"
	eventTypeData  eventType = "data"
)

type eventType string

// AuditFields represents additional data associated with a specific audit event invocation.
// E.g. Username, IPs, request parameters, etc.
type AuditFields map[string]any

// expandMandatoryFields adds fields that must be present on events, of the types determined by eventFieldTypes.
func (ed *EventDescriptor) expandMandatoryFieldGroups(groups []fieldGroup) {
	if ed.MandatoryFields == nil {
		ed.MandatoryFields = make(AuditFields)
	}

	// common global fields
	fields := fieldsByGroup[fieldGroupGlobal]
	for k, v := range fields {
		ed.MandatoryFields[k] = v
	}

	// event-specific field groups
	for _, group := range groups {
		groupFields := fieldsByGroup[group]
		for k, v := range groupFields {
			ed.MandatoryFields[k] = v
		}
	}
}

func (ed *EventDescriptor) expandOptionalFieldGroups(groups []fieldGroup) {
	if ed.OptionalFields == nil {
		ed.OptionalFields = make(AuditFields)
	}
	// event-specific field groups
	for _, group := range groups {
		groupFields := fieldsByGroup[group]
		for k, v := range groupFields {
			ed.OptionalFields[k] = v
		}
	}
}

func (i AuditID) MustValidateFields(f AuditFields) {
	if err := i.ValidateFields(f); err != nil {
		panic(fmt.Errorf("audit event %q (%s) invalid:\n%v", i, AuditEvents[i].Name, err))
	}
}

func (i AuditID) ValidateFields(f AuditFields) error {
	if i < auditdSyncGatewayStartID || i > auditdSyncGatewayEndID {
		return fmt.Errorf("invalid audit event ID: %d (allowed range: %d-%d)", i, auditdSyncGatewayStartID, auditdSyncGatewayEndID)
	}
	event, ok := AuditEvents[i]
	if !ok {
		return fmt.Errorf("unknown audit event ID %d", i)
	}
	return mandatoryFieldsPresent(f, event.MandatoryFields, "")
}

func mandatoryFieldsPresent(fields, mandatoryFields AuditFields, baseName string) error {
	me := &MultiError{}
	for k, v := range mandatoryFields {
		if _, ok := fields[k]; !ok {
			me = me.Append(fmt.Errorf("missing mandatory field %s", baseName+k))
			continue
		}
		if !matchingTypes(v, fields[k]) {
			me = me.Append(fmt.Errorf("field value for %s%s must be of type %T but had %T", baseName, k, v, fields[k]))
			continue
		}
		// recurse if map
		if vv, ok := v.(map[string]any); ok {
			if pv, ok := fields[k].(map[string]any); ok {
				me = me.Append(mandatoryFieldsPresent(pv, vv, baseName+k+"."))
			}
		}
	}
	return me.ErrorOrNil()
}

// matchingTypes returns true if the types of a and b are the same.
func matchingTypes(a, b any) bool {
	typeOfA, typeOfB := reflect.TypeOf(a), reflect.TypeOf(b)
	if typeOfA == nil || typeOfB == nil {
		return typeOfA == typeOfB
	}
	// deref
	if typeOfA.Kind() == reflect.Pointer && typeOfB.Kind() != reflect.Pointer {
		typeOfA = typeOfA.Elem()
	} else if typeOfB.Kind() == reflect.Pointer && typeOfA.Kind() != reflect.Pointer {
		typeOfB = typeOfB.Elem()
	}
	if typeOfA.ConvertibleTo(typeOfB) {
		return true
	}
	return typeOfA.Kind() == typeOfB.Kind()
}
