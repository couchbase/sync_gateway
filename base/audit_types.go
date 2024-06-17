// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import "strconv"

// AuditID is a unique identifier for an audit event.
type AuditID uint

// String implements Stringer for AuditID
func (i AuditID) String() string {
	return strconv.FormatUint(uint64(i), 10)
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
	MandatoryFields map[string]any
	// OptionalFields describe optional field(s) valid in an instance of the event
	OptionalFields map[string]any
	// EventType represents a type of event. Used only for documentation categorization.
	EventType eventType
}

const (
	eventTypeAdmin eventType = "admin"
)

type eventType string
