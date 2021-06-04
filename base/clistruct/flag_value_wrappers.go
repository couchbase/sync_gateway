/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package clistruct

import (
	"encoding/json"
	"fmt"
	"strings"
)

// stringSliceValue implements flag.Value for []string
type stringSliceValue []string

func (v *stringSliceValue) Set(s string) error {
	if v == nil {
		return fmt.Errorf("nil stringSliceValue reciever")
	}
	*v = strings.Split(s, ",")
	return nil
}

func (v *stringSliceValue) String() string {
	if v == nil {
		return ""
	}
	return strings.Join(*v, ",")
}

// jsonNumberFlagValue implements flag.Value for json.Number
type jsonNumberFlagValue json.Number

func (v *jsonNumberFlagValue) Set(s string) error {
	if v == nil {
		return fmt.Errorf("nil jsonNumberFlagValue reciever")
	}
	*v = jsonNumberFlagValue(s)
	return nil
}

func (v *jsonNumberFlagValue) String() string {
	if v == nil {
		return ""
	}
	return (*json.Number)(v).String()
}
