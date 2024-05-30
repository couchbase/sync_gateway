package audit

import (
	"fmt"
)

var sgAuditEvents = events{
	IDPlaceholder: {
		name:        "Placeholder audit event",
		description: "This is a placeholder.",
		mandatoryFields: map[string]any{
			"context": map[string]any{
				"provider": "example provider",
				"username": "alice",
			},
		},
		optionalFields: map[string]any{
			"operationID": 123,
			"isSomething": false,
		},
		filteringPermitted: false,
		eventType:          eventTypeAdmin,
	},
}

func init() {
	// Ensures that the above audit event IDs are within the allocated range and are valid.
	if err := validateAuditEvents(sgAuditEvents); err != nil {
		panic(err)
	}
}

func validateAuditEvents(e events) error {
	for id, descriptor := range e {
		if id < auditdSyncGatewayStartID || id > auditdSyncGatewayEndID {
			return fmt.Errorf("invalid audit event ID: %d %q (allowed range: %d-%d)",
				id, descriptor.name, auditdSyncGatewayStartID, auditdSyncGatewayEndID)
		}
	}
	return nil
}
