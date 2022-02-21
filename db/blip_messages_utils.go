/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"strconv"

	"github.com/couchbase/go-blip"
)

// setProperty will set the given property value.
func setProperty(p blip.Properties, k string, v interface{}) {
	switch val := v.(type) {
	case string:
		p[k] = val
	case bool:
		p[k] = strconv.FormatBool(val)
	case uint64:
		p[k] = strconv.FormatUint(val, 10)
	case uint16:
		p[k] = strconv.FormatUint(uint64(val), 10)
	case fmt.Stringer:
		p[k] = val.String()
	default:
		// TODO: CBG-1948
		panic(fmt.Sprintf("unknown setProperty value type: %T", val))
	}
}

// setOptionalProperty will set the given property value, if v is non-zero.
func setOptionalProperty(p blip.Properties, k string, v interface{}) {
	switch val := v.(type) {
	case *string:
		if val != nil {
			p[k] = *val
		}
	case string:
		if val != "" {
			p[k] = val
		}
	case bool:
		if val {
			p[k] = strconv.FormatBool(val)
		}
	case uint64:
		if val != 0 {
			p[k] = strconv.FormatUint(val, 10)
		}
	case uint16:
		if val != 0 {
			p[k] = strconv.FormatUint(uint64(val), 10)
		}
	case fmt.Stringer:
		p[k] = val.String()
	default:
		// TODO: CBG-1948
		panic(fmt.Sprintf("unknown setOptionalProperty value type: %T", val))
	}
}
