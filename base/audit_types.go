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
type events map[AuditID]EventDescriptor

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

	// The following fields are for documentation-use only.
	// OptionalFields describe optional field(s) valid in an instance of the event
	OptionalFields AuditFields
	// EventType represents a type of event
	EventType eventType
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

func (i AuditID) MustValidateFields(f AuditFields) {
	if err := i.ValidateFields(f); err != nil {
		panic(fmt.Errorf("audit event %s invalid:\n%v", i, err))
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
	return mandatoryFieldsPresent(f, event.MandatoryFields)
}

func mandatoryFieldsPresent(fields, mandatoryFields AuditFields) error {
	me := &MultiError{}
	for k, v := range mandatoryFields {
		// recurse if map
		if vv, ok := v.(map[string]any); ok {
			if pv, ok := fields[k].(map[string]any); ok {
				me = me.Append(mandatoryFieldsPresent(pv, vv))
			}
		}
		if _, ok := fields[k]; !ok {
			me = me.Append(fmt.Errorf("missing mandatory field %s", k))
		}
	}
	return me.ErrorOrNil()
}
