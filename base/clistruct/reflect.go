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
	"errors"
	"fmt"
	"reflect"
)

// reflectStructValue returns the reflect.Value of the given val struct, also dereferencing pointers.
// Panics if the given val was nil or not a struct.
func reflectStructValue(val interface{}) (refVal reflect.Value, err error) {
	refVal = reflect.ValueOf(val)
	if refVal.IsZero() {
		return refVal, errors.New("can't get reflect.Value of nil")
	}

	derefVal := reflect.Indirect(refVal)
	if derefVal.Kind() != reflect.Struct {
		return refVal, fmt.Errorf("expected val to be a struct, but was %s", derefVal.Kind())
	}

	return derefVal, nil
}
