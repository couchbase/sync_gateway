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
type SystemDataSlice []SystemData

// Redact tags the string with SystemData tags for post-processing.
func (sd SystemData) Redact() string {
	if !RedactSystemData {
		return string(sd)
	}
	return clog.Tag(clog.SystemData, string(sd)).(string)
}

func sds(interfaceSlice interface{}) SystemDataSlice {
	valueOf := reflect.ValueOf(interfaceSlice)

	length := valueOf.Len()
	retVal := make([]SystemData, 0, length)
	for i := 0; i < length; i++ {
		retVal = append(retVal, SD(valueOf.Index(i).Interface()).(SystemData))
	}

	return retVal
}

func (sdSlice SystemDataSlice) Redact() string {
	tmp := []byte{}
	for _, item := range sdSlice {
		tmp = append(tmp, []byte(item.Redact())...)
		tmp = append(tmp, []byte(" ")...)
	}
	return "[ " + string(tmp) + "]"
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
		if reflect.ValueOf(i).Kind() == reflect.Slice {
			return sds(i)
		}
		// Fall back to a slower but safe way of getting a string from any type.
		return SystemData(fmt.Sprintf("%+v", v))
	}
}
