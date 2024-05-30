// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package audit

// auditdModuleDescriptor describes an audit module descriptor in the auditd JSON format.
type auditdModuleDescriptor struct {
	Version uint                    `json:"version"`
	Module  string                  `json:"module"`
	Events  []auditdEventDescriptor `json:"events"`
}

// auditdEventDescriptor describes an audit event in the auditd JSON format.
type auditdEventDescriptor struct {
	ID                 ID             `json:"id"`
	Name               string         `json:"name"`
	Description        string         `json:"description"`
	Sync               bool           `json:"sync,omitempty"`
	Enabled            bool           `json:"enabled,omitempty"`
	FilteringPermitted bool           `json:"filtering_permitted,omitempty"`
	MandatoryFields    map[string]any `json:"mandatory_fields"`
	OptionalFields     map[string]any `json:"optional_fields,omitempty"`
}

// toAuditdEventDescriptor converts an eventDescriptor to an auditdEventDescriptor.
// These are _mostly_ the same, but each event holds its own ID in an array in the JSON format.
func toAuditdEventDescriptor(id ID, e eventDescriptor) auditdEventDescriptor {
	return auditdEventDescriptor{
		ID:                 id,
		Name:               e.name,
		Description:        e.description,
		Enabled:            e.enabledByDefault,
		FilteringPermitted: e.filteringPermitted,
		MandatoryFields:    toAuditdFieldType(e.mandatoryFields),
		OptionalFields:     toAuditdFieldType(e.optionalFields),
	}
}

// auditd values that represent each type of field
const (
	auditFieldTypeNumber = 1
	auditFieldTypeString = ""
	auditFieldTypeBool   = true
)

var (
	auditFieldTypeArray = []any{}
)

// toAuditdFieldType converts a map of fields to an auditd-compatible field type (where the value is a fixed representative value of the type)
func toAuditdFieldType(m1 map[string]any) (m2 map[string]any) {
	if m1 == nil {
		return nil
	}

	if len(m1) == 0 {
		return map[string]any{}
	}

	m2 = make(map[string]any, len(m1))
	for k, v := range m1 {
		switch val := v.(type) {
		case map[string]any:
			m2[k] = toAuditdFieldType(val)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			m2[k] = auditFieldTypeNumber
		case string:
			m2[k] = auditFieldTypeString
		case bool:
			m2[k] = auditFieldTypeBool
		case []any:
			m2[k] = auditFieldTypeArray
		}
	}

	return m2
}
