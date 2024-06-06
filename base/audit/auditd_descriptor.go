// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package audit

import (
	"encoding/json"
)

const (
	// moduleDescriptorName is the name of the module. Must match the name used in the module descriptor file.
	moduleDescriptorName = "sync_gateway"
	// auditdFormatVersion is the version of the auditd format to be used. Only version 2 is supported.
	auditdFormatVersion = 2
)

// generateAuditdModuleDescriptor returns an auditd-compatible module descriptor for the given events.
func generateAuditdModuleDescriptor(e events) ([]byte, error) {
	auditEvents := make([]auditdEventDescriptor, 0, len(e))
	for id, event := range e {
		auditEvents = append(auditEvents, toAuditdEventDescriptor(id, event))
	}
	m := auditdModuleDescriptor{
		Version: auditdFormatVersion,
		Module:  moduleDescriptorName,
		Events:  auditEvents,
	}
	return json.Marshal(m)
}
