//go:build cb_sg_enterprise
// +build cb_sg_enterprise

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
	"io"
	"time"

	"github.com/couchbaselabs/go-fleecedelta"
	jsoniter "github.com/json-iterator/go"
)

// The reason for these wrappers is to keep go-fleecedelta out of the compiled CE builds by providing no-op versions in deltas_ce.go
func init() {
	fleecedelta.StringDiffEnabled = true             // Global toggle for string diffs
	fleecedelta.StringDiffMinLength = 60             // 60 B min length to match CBL
	fleecedelta.StringDiffMaxLength = 1024 * 1024    // 1 MB max length for string diffs
	fleecedelta.StringDiffTimeout = time.Millisecond // Aggressive string diff timeout
}

// Diff will return the fleece delta between old and new.
func Diff(old, new map[string]interface{}) (delta []byte, err error) {
	delta, err = fleecedelta.DiffJSON(old, new)
	if err != nil {
		return nil, FleeceDeltaError{e: err}
	}
	return delta, nil
}

// Patch attempts to patch old with the given delta passed as a map[string]interface{}
func Patch(old *map[string]interface{}, delta map[string]interface{}) (err error) {
	err = fleecedelta.PatchJSONWithUnmarshalledDelta(old, delta)
	if err != nil {
		return FleeceDeltaError{e: err}
	}
	return nil
}

// JSONUnmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
func JSONUnmarshal(data []byte, v interface{}) (err error) {
	if !UseStdlibJSON {
		err = jsoniter.Unmarshal(data, v)
		if err != nil {
			err = &JSONIterError{E: err}
		}
	} else {
		err = json.Unmarshal(data, v)
	}
	return err
}

// JSONMarshal returns the JSON encoding of v.
func JSONMarshal(v interface{}) (b []byte, err error) {
	if !UseStdlibJSON {
		b, err = jsoniter.Marshal(v)
		if err != nil {
			err = &JSONIterError{E: err}
		}
	} else {
		b, err = json.Marshal(v)
	}
	return b, err
}

// JSONMarshalCanonical returns the canonical JSON encoding of v.
// Mostly notably: Ordered properties, in order to generate deterministic Rev IDs.
func JSONMarshalCanonical(v interface{}) (b []byte, err error) {
	// json.iterator shows performance degradation vs standard library for canonical marshalling,
	// so force the use of the standard library here.
	return json.Marshal(v)
}

// JSONDecoder returns a new JSON decoder implementing the JSONDecoderI interface
func JSONDecoder(r io.Reader) JSONDecoderI {
	if !UseStdlibJSON {
		return jsoniter.NewDecoder(r)
	} else {
		return json.NewDecoder(r)
	}
}

// JSONEncoder returns a new JSON encoder implementing the JSONEncoderI interface
func JSONEncoder(w io.Writer) JSONEncoderI {
	if !UseStdlibJSON {
		return jsoniter.NewEncoder(w)
	} else {
		return json.NewEncoder(w)
	}
}

// JSONEncoderCanonical returns a new canonical JSON encoder implementing the JSONEncoderI interface
func JSONEncoderCanonical(w io.Writer) JSONEncoderI {
	if !UseStdlibJSON {
		return jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(w)
	} else {
		return json.NewEncoder(w)
	}
}
