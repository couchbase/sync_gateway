// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

func TestValidateAuditEvents(t *testing.T) {
	// Ensures that the above audit event IDs are within the allocated range and are valid.
	require.NoError(t, validateAuditEvents(AuditEvents))
}

func validateAuditEvents(e events) error {
	multiError := &MultiError{}
	seenIDs := make(map[AuditID]struct{})
	for id, descriptor := range e {
		if id < auditdSyncGatewayStartID || id > auditdSyncGatewayEndID {
			multiError = multiError.Append(fmt.Errorf("invalid audit event ID: %d %q (allowed range: %d-%d)",
				id, descriptor.Name, auditdSyncGatewayStartID, auditdSyncGatewayEndID))
		}
		if _, ok := seenIDs[id]; ok {
			multiError = multiError.Append(fmt.Errorf("duplicate audit event ID: %d %q", id, descriptor.Name))
		}
		seenIDs[id] = struct{}{}
	}
	return multiError.ErrorOrNil()
}

// TestGenerateAuditDescriptorCSV outputs a CSV of AuditEvents.
func TestGenerateAuditDescriptorCSV(t *testing.T) {
	b, err := generateCSVModuleDescriptor(AuditEvents)
	require.NoError(t, err)
	fmt.Print(string(b))
}

// generateCSVModuleDescriptor returns a CSV module descriptor for the given events.
func generateCSVModuleDescriptor(e events) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)

	// Write header
	if err := w.Write([]string{"ID", "Name", "Description", "DefaultEnabled", "Filterable", "EventType", "MandatoryFields", "OptionalFields"}); err != nil {
		return nil, err
	}

	keys := make([]AuditID, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, id := range keys {
		event := e[id]

		mandatoryFields := event.MandatoryFields
		mandatoryFields.withCommonMandatoryFields()
		optionalFields := event.OptionalFields
		//optionalFields.withCommonOptionalFields()
		if err := w.Write([]string{
			id.String(),
			event.Name,
			event.Description,
			strconv.FormatBool(event.EnabledByDefault),
			strconv.FormatBool(event.FilteringPermitted),
			string(event.EventType),
			strings.Join(maps.Keys(mandatoryFields), ", "),
			strings.Join(maps.Keys(optionalFields), ", "),
		}); err != nil {
			return nil, err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
