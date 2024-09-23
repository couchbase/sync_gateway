// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

// auditdModuleDescriptor describes an audit module descriptor in the auditd JSON format.
type auditdModuleDescriptor struct {
	Version uint                    `json:"version"`
	Module  string                  `json:"module"`
	Events  []auditdEventDescriptor `json:"events"`
}

// auditdEventDescriptor describes an audit event in the auditd JSON format.
type auditdEventDescriptor struct {
	ID                 AuditID        `json:"id"`
	Name               string         `json:"name"`
	Description        string         `json:"description"`
	Sync               bool           `json:"sync,omitempty"`
	Enabled            bool           `json:"enabled,omitempty"`
	FilteringPermitted bool           `json:"filtering_permitted,omitempty"`
	MandatoryFields    map[string]any `json:"mandatory_fields"`
	OptionalFields     map[string]any `json:"optional_fields,omitempty"`
}

// toAuditdEventDescriptor converts an EventDescriptor to an auditdEventDescriptor.
// These are _mostly_ the same, but each event holds its own ID in an array in the JSON format.
func toAuditdEventDescriptor(id AuditID, e EventDescriptor) auditdEventDescriptor {
	return auditdEventDescriptor{
		ID:                 id,
		Name:               e.Name,
		Description:        e.Description,
		Enabled:            e.EnabledByDefault,
		FilteringPermitted: e.FilteringPermitted,
		MandatoryFields:    toAuditdFieldType(e.MandatoryFields),
		OptionalFields:     toAuditdFieldType(e.OptionalFields),
	}
}

// auditd values that represent each type of field
// ref: https://github.com/couchbase/kv_engine/blob/master/auditd/README.md#defining-the-format-for-mandatory-and-optional-fields
const (
	auditdFieldTypeValueNumber = 1
	auditdFieldTypeValueString = ""
	auditdFieldTypeValueBool   = true
)

var (
	auditdFieldTypeValueArray = []any{}
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
			m2[k] = auditdFieldTypeValueNumber
		case string:
			m2[k] = auditdFieldTypeValueString
		case bool:
			m2[k] = auditdFieldTypeValueBool
		case []any:
			m2[k] = auditdFieldTypeValueArray
		}
	}

	return m2
}
