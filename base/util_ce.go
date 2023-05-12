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
	"bytes"
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

// Incrementally parses a JSON object, passing each top-level key to a callback.
// The callback can choose to unmarshal the value by returning a pointer to a destination value
// (just like the pointer passed to `json.Unmarshal()`), it can return `nil` to skip it, or it
// can return an error to abort the parse.
//
// The function returns a copy of the JSON with all of the unmarshaled key/value pairs removed,
// i.e. containing only the skipped ones.
//
// For compatibility with json.Unmarshal, the input JSON `null` is allowed, and treated as an
// empty object. The output JSON will be nil.
//
// For examples, see TestJSONExtract() in util_test.go.
//
// Note: CE and EE have different implementations of this.
func JSONExtract(input []byte, callback func(string) (any, error)) (output []byte, err error) {
	out := bytes.NewBufferString("{")
	var copyFrom int64 = -1
	keys := Set{}

	reader := bytes.NewReader(input)
	iter := json.NewDecoder(reader)
	// Read the opening of the object:
	if tok, err := iter.Token(); err != nil {
		return nil, err
	} else if tok == nil {
		// For compatibility with json.Unmarshal, allow input `null`, treating it like an empty
		// object but returning `nil`. But EOF must follow:
		if _, err := iter.Token(); err == io.EOF {
			return nil, nil
		} else if err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf("unexpected data after end")
		}
	} else if tok != json.Delim('{') {
		return nil, fmt.Errorf("expected an object")
	}

	for iter.More() {
		// Read key, then colon:
		var key string
		var ok bool
		keyOff := iter.InputOffset()

		if keyTok, err := iter.Token(); err != nil {
			return nil, err
		} else if key, ok = keyTok.(string); !ok {
			return nil, fmt.Errorf("JSON syntax error (expected key)")
		}

		if keys.Contains(key) {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		keys.Add(key)

		if valuep, err := callback(key); err != nil {
			// Callback reported an error:
			return nil, err

		} else if valuep == nil {
			// Non-special property: Remember to copy the key & value:
			if copyFrom < 0 {
				copyFrom = keyOff
			}
			// Skip the value by decoding to a dummy `any` //OPT: Faster to skip tokens
			var dummy any
			if err = iter.Decode(&dummy); err != nil {
				return nil, err
			}

		} else {
			// Special property: First copy any preceding chars to `out`:
			if copyFrom >= 0 {
				if input[copyFrom] == ',' && out.Len() == 1 {
					copyFrom++
				}
				if copyFrom < keyOff {
					out.Write(input[copyFrom:keyOff])
				}
			}
			// Then parse value into `valuep`
			if err = iter.Decode(valuep); err != nil {
				if _, ok := err.(*json.UnmarshalTypeError); ok {
					err = fmt.Errorf("invalid value type for special key %q", key)
				}
				return nil, err
			}
			copyFrom = iter.InputOffset()
		}
	}

	if copyFrom >= 0 {
		if input[copyFrom] == ',' && out.Len() == 1 {
			copyFrom++
		}
		out.Write(input[copyFrom:iter.InputOffset()])
	}
	out.WriteByte('}')

	// Read the closing brace:
	if tok, err := iter.Token(); err != nil {
		return nil, err
	} else if tok != json.Delim('}') {
		return nil, fmt.Errorf("unexpected data at end of object: %v", tok)
	}

	// Make sure there's nothing more:
	if tok, err := iter.Token(); err == nil {
		return nil, fmt.Errorf("unexpected data after end of object: %v", tok)
	} else if err != io.EOF {
		return nil, err
	}
	return out.Bytes(), nil
}
