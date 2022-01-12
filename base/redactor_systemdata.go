/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"reflect"
)

const (
	systemDataPrefix = "<sd>"
	systemDataSuffix = "</sd>"
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

func (sd SystemData) String() string {
	return string(sd)
}

// Redact tags the string with SystemData tags for post-processing.
func (sd SystemData) Redact() string {
	if !RedactSystemData {
		return string(sd)
	}
	return systemDataPrefix + string(sd) + systemDataSuffix
}

// Compile-time interface check.
var _ Redactor = SystemData("")

// SD returns a SystemData type for any given value.
func SD(i interface{}) RedactorFunc {
	switch v := i.(type) {
	case string:
		return func() Redactor {
			return SystemData(v)
		}
	case Set:
		return func() Redactor {
			return v.buildRedactorSet(SD)
		}
	case fmt.Stringer:
		return func() Redactor {
			return SystemData(v.String())
		}
	case []byte:
		return func() Redactor {
			return SystemData(string(v))
		}
	default:
		return func() Redactor {
			valueOf := reflect.ValueOf(i)
			if valueOf.Kind() == reflect.Slice {
				return buildRedactorFuncSlice(valueOf, SD)
			}
			// Fall back to a slower but safe way of getting a string from any type.
			return SystemData(fmt.Sprintf("%+v", v))
		}
	}
}
