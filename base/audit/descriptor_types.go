// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package audit

// ID is a unique identifier for an audit event.
type ID uint

// events is a map of audit event IDs to event descriptors.
type events map[ID]eventDescriptor

// eventDescriptor is an audit event. The fields closely (but not exactly) follows kv_engine's auditd descriptor implementation.
type eventDescriptor struct {
	// name is a short textual name of the event
	name string
	// description is a longer name / description of the event
	description string
	// enabledByDefault indicates whether the event should be enabled by default
	enabledByDefault bool
	// filteringPermitted indicates whether the event can be filtered or not
	filteringPermitted bool
	// mandatoryFields describe field(s) required for a valid instance of the event
	mandatoryFields map[string]any
	// optionalFields describe optional field(s) valid in an instance of the event
	optionalFields map[string]any
	// eventType represents a type of event. Used only for documentation categorization.
	eventType eventType
}

const (
	eventTypeAdmin eventType = "admin"
)

type eventType string
