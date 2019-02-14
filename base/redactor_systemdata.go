package base

import (
	"fmt"
	"reflect"

	"github.com/couchbase/clog"
)

// RedactSystemData is a global toggle for system data redaction.
var RedactSystemData = false

// SystemData is a type which implements the Redactor interface for logging purposes of system data.
//
// System data is data from other parts of the system Couchbase interacts with over the network
// - IP addresses
// - IP tables
// - Hosts names
// - Ports
// - DNS topology
type SystemData string

// Redact tags the string with SystemData tags for post-processing.
func (sd SystemData) Redact() string {
	if !RedactSystemData {
		return string(sd)
	}
	return clog.Tag(clog.SystemData, string(sd)).(string)
}

// Compile-time interface check.
var _ Redactor = SystemData("")

// SD returns a SystemData type for any given value.
func SD(i interface{}) Redactor {
	switch v := i.(type) {
	case string:
		return SystemData(v)
	case fmt.Stringer:
		return SystemData(v.String())
	default:
		valueOf := reflect.ValueOf(i)
		if valueOf.Kind() == reflect.Slice {
			return buildRedactorSlice(valueOf, SD)
		}
		// Fall back to a slower but safe way of getting a string from any type.
		return SystemData(fmt.Sprintf("%+v", v))
	}
}
