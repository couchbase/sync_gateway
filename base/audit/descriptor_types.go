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
