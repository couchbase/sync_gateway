//go:build !cb_sg_enterprise
// +build !cb_sg_enterprise

/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/json"
	"fmt"
	"io"
)

// ErrDeltasNotSupported is returned when these functions are called in CE
var ErrDeltasNotSupported = fmt.Errorf("Deltas not supported in CE")

// Diff is only implemented in EE, the CE stub always returns an error.
func Diff(old, new map[string]interface{}) (delta []byte, err error) {
	return nil, ErrDeltasNotSupported
}

// Patch is only implemented in EE, the CE stub always returns an error.
func Patch(old *map[string]interface{}, delta map[string]interface{}) (err error) {
	return ErrDeltasNotSupported
}

// JSONUnmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
func JSONUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// JSONMarshal returns the JSON encoding of v.
func JSONMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// JSONMarshalCanonical returns the canonical JSON encoding of v.
// Mostly notably: Ordered properties, in order to generate deterministic Rev IDs.
func JSONMarshalCanonical(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// JSONDecoder returns a new JSON decoder implementing the JSONDecoderI interface
func JSONDecoder(r io.Reader) JSONDecoderI {
	return json.NewDecoder(r)
}

// JSONEncoder returns a new JSON encoder implementing the JSONEncoderI interface
func JSONEncoder(w io.Writer) JSONEncoderI {
	return json.NewEncoder(w)
}

// JSONEncoderCanonical returns a new canonical JSON encoder implementing the JSONEncoderI interface
func JSONEncoderCanonical(w io.Writer) JSONEncoderI {
	return json.NewEncoder(w)
}
