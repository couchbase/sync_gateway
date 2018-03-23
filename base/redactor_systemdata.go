package base

import "fmt"

// SystemData is a type which implements the Redactor interface for logging purposes of system data.
//
// System data is data from other parts of the system Couchbase interacts with over the network
// - IP addresses
// - IP tables
// - Hosts names
// - Ports
// - DNS topology
type SystemData string

// Redact is a stub which will eventaully tag or redact data in logs.
func (sd SystemData) Redact() string {
	// FIXME: Stub (#3370)
	return string(sd)
}

// Compile-time interface check.
var _ Redactor = SystemData("")

// SD returns a SystemData type for any given value.
func SD(i interface{}) SystemData {
	switch v := i.(type) {
	case string:
		return SystemData(v)
	case fmt.Stringer:
		return SystemData(v.String())
	default:
		// Fall back to a slower but safe way of getting a string from any type.
		return SystemData(fmt.Sprintf("%+v", v))
	}
}
